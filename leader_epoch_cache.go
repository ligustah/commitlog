package commitlog

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	pkgErrors "github.com/pkg/errors"
)

const (
	leaderEpochFileName = "leader-epoch-checkpoint"
	leaderEpochFileV0   = 0
)

// ErrUnknownLeaderEpoch reports a probe that named no epoch. The log refuses it
// rather than answering, because there is no answer that is safe to give: the
// caller of LastOffsetForLeaderEpoch truncates to what it is told, and every
// offset the log could return is an instruction to delete down to it.
var ErrUnknownLeaderEpoch = errors.New("commitlog: leader epoch probe named no epoch")

// Epoch is an optional leader epoch. Its zero value is "no epoch known", which
// is what a caller that has never recorded one has, and AtEpoch(0) is the epoch
// numbered zero — a distinction a uint64 cannot make and a follower's life
// depends on.
//
// It exists because the probe took a bare uint64, and a follower with no epoch
// history had nothing to pass but 0. Zero is also a REAL epoch — ordinary
// Append stamps it, and it is the first epoch of every log that has not failed
// over — so the log answered the question it was asked: where does the epoch
// after 0 begin. On a log whose first recorded epoch is 1, that is offset 0, and
// a follower that truncates to what it is told deletes the log. durable_streams
// lost a node to exactly this, with 450 committed records, and it read as a
// correct answer at every step.
//
// The same shape as Bound, for the same reason and with the same two words. The
// difference is what happens to the unset case: a Bound falls back, because a
// missing compaction ceiling has an obvious safe default. A missing epoch has
// none, so this one is refused.
type Epoch struct {
	epoch uint64
	known bool
}

// AtEpoch returns an Epoch naming leader epoch e. Every uint64 is valid,
// including 0.
func AtEpoch(e uint64) Epoch { return Epoch{epoch: e, known: true} }

// Get returns the epoch and whether one was named.
func (e Epoch) Get() (uint64, bool) { return e.epoch, e.known }

// epochOffset contains the start offset for a given leader epoch.
type epochOffset struct {
	leaderEpoch uint64
	startOffset int64
}

type leaderEpochCache struct {
	epochOffsets   []*epochOffset
	mu             sync.RWMutex
	checkpointFile string
	name           string
}

func newLeaderEpochCache(name, path string) (*leaderEpochCache, error) {
	var (
		file   = filepath.Join(path, leaderEpochFileName)
		epochs = []*epochOffset{}
	)
	// Read the whole file rather than Stat-then-Open. The checkpoint is a few
	// bytes, so there is nothing to stream, and reading it in one call removes
	// both the window between the Stat and the Open and the leaked-handle hazard
	// the old shape had to close around by hand: a handle still open here blocks
	// the atomic checkpoint replace on Windows for the rest of the process.
	//
	// WithRetry for the same reason as the high watermark — this runs while a
	// just-killed previous process may still hold the file open. Absent stays
	// absent: a log that has never had a leader epoch is the normal first-open
	// case, not a race, and ReadFileWithRetry returns that immediately.
	b, err := ReadFileWithRetry(file)
	switch {
	case err == nil:
		epochs, err = readLeaderEpochOffsets(bytes.NewReader(b))
		if err != nil {
			return nil, pkgErrors.Wrap(err, "failed to read leader epoch offsets file")
		}
	case !os.IsNotExist(err):
		return nil, pkgErrors.Wrap(err, "failed to open leader epoch offsets file")
	}
	return &leaderEpochCache{
		epochOffsets:   epochs,
		checkpointFile: file,
		name:           name,
	}, nil
}

// Assign the given leader epoch to the given offset. Once assigned, an epoch
// cannot be reassigned.
func (l *leaderEpochCache) Assign(epoch uint64, offset int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.assign(epoch, offset)
}

// LastOffsetForLeaderEpoch returns the start offset of the first leader epoch
// larger than the provided one or -1 if the current epoch equals the provided
// one.
func (l *leaderEpochCache) LastOffsetForLeaderEpoch(epoch uint64) int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	e := l.findEpoch(epoch + 1)
	if e == nil {
		return -1
	}
	return e.startOffset
}

// LastLeaderEpoch returns the latest leader epoch for the log.
func (l *leaderEpochCache) LastLeaderEpoch() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.latestEpoch()
}

// ClearLatest removes all leader epoch entries from the cache with start
// offsets greater than or equal to the given offset.
func (l *leaderEpochCache) ClearLatest(offset int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if offset > l.latestOffset() {
		return nil
	}
	filtered := make([]*epochOffset, 0, len(l.epochOffsets))
	for _, epoch := range l.epochOffsets {
		if epoch.startOffset < offset {
			filtered = append(filtered, epoch)
		}
	}
	l.epochOffsets = filtered
	err := l.flush()
	return pkgErrors.Wrap(err, "failed to flush epoch offsets")
}

// ClearEarliest searches for the oldest leader epoch < offset, updates the
// saved epoch offset to the given offset, then removes any previous epoch
// entries.
func (l *leaderEpochCache) ClearEarliest(offset int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.earliestOffset() >= offset {
		return nil
	}
	var (
		earliest = make([]*epochOffset, 0, len(l.epochOffsets))
		removed  = 0
	)
	for _, epoch := range l.epochOffsets {
		if epoch.startOffset < offset {
			earliest = append(earliest, epoch)
			removed++
		}
	}
	if len(earliest) == 0 {
		return nil
	}
	l.epochOffsets = l.epochOffsets[removed:]
	// If the offset is less than the earliest offset remaining, add
	// previous epoch back but with an updated offset.
	if offset < l.earliestOffset() || len(l.epochOffsets) == 0 {
		l.epochOffsets = append([]*epochOffset{{
			leaderEpoch: earliest[len(earliest)-1].leaderEpoch,
			startOffset: offset,
		}}, l.epochOffsets...)
	}
	err := l.flush()
	return pkgErrors.Wrap(err, "failed to flush epoch offsets")
}

// There is deliberately no way to overwrite this cache wholesale from another
// one. Compaction used to, with a cache rebuilt from the surviving records'
// epoch stamps, and that silently discarded every epoch no surviving record
// happened to carry — on a leader, all of them. The cache is only ever added to
// (assign) or trimmed at an end (ClearEarliest, ClearLatest), and a trim at the
// earliest end re-anchors rather than drops. See the ClearEarliest call in
// commitLog.CleanWithSpec.

func (l *leaderEpochCache) earliestOffset() int64 {
	if len(l.epochOffsets) == 0 {
		return -1
	}
	return l.epochOffsets[0].startOffset
}

func (l *leaderEpochCache) latestEpoch() uint64 {
	if len(l.epochOffsets) == 0 {
		return 0
	}
	return l.epochOffsets[len(l.epochOffsets)-1].leaderEpoch
}

func (l *leaderEpochCache) latestOffset() int64 {
	if len(l.epochOffsets) == 0 {
		return -1
	}
	return l.epochOffsets[len(l.epochOffsets)-1].startOffset
}

func (l *leaderEpochCache) findEpoch(epoch uint64) *epochOffset {
	i := sort.Search(len(l.epochOffsets), func(i int) bool {
		return l.epochOffsets[i].leaderEpoch >= epoch
	})
	if i < len(l.epochOffsets) {
		return l.epochOffsets[i]
	}
	return nil
}

func (l *leaderEpochCache) assign(epoch uint64, offset int64) error {
	var (
		latestEpoch  = l.latestEpoch()
		latestOffset = l.latestOffset()
	)
	// len, not `epoch > latestEpoch` alone. latestEpoch() answers 0 for an EMPTY
	// cache, which is also a perfectly good epoch — ordinary Append stamps 0 —
	// so the monotonicity check refused the first assignment of epoch 0 on every
	// log, permanently, for the whole of its life before a first failover. A
	// sentinel made of a valid value: 0 meant both "nothing recorded yet" and
	// "the latest epoch is 0", and the gate could not tell which it had.
	//
	// Silently, too. The else branch's warn() fires on `epoch < latestEpoch`
	// (0 < 0) or `offset < latestOffset` (against -1), and neither is true here,
	// so nothing was logged at the call or anywhere after it.
	//
	// Asking the length tests the thing actually in question and leaves the
	// comparison meaning only what it says. An empty cache has no previous epoch
	// to be monotonic against, so any epoch may open the history; every
	// assignment after the first is checked exactly as before.
	if len(l.epochOffsets) == 0 || (epoch > latestEpoch && offset >= latestOffset) {
		l.epochOffsets = append(l.epochOffsets, &epochOffset{
			leaderEpoch: epoch,
			startOffset: offset,
		})
		if err := l.flush(); err != nil {
			return pkgErrors.Wrap(err, "failed to flush epoch offsets")
		}
	} else {
		l.warn(epoch, latestEpoch, offset, latestOffset)
	}
	return nil
}

// flush writes the cached epoch offsets to disk in the following format:
//
// v0:
// version
// num_entries
// leader_epoch start_offset
// leader_epoch start_offset
// ...
//
// Through AtomicWriteFileWithRetry, matching the ReadFileWithRetry above it: the
// read side already waits out the Windows window where a just-killed process's
// handle is still open, and the write side is the half that window was named
// for — an open handle is what makes ReplaceFile fail with "Access is denied",
// and the comment on the read explicitly notes that a handle held here blocks
// this replace. Retrying only the read moved the failure rather than removing
// it.
//
// It also fsyncs the directory. The rename is the commit point for this file
// like every other rename the log finishes with, and an epoch history that
// survived only until the power went out is one a follower then truncates
// against — see LastOffsetForLeaderEpoch, which the caller obeys.
func (l *leaderEpochCache) flush() error {
	if l.checkpointFile == "" {
		return nil
	}
	b := new(bytes.Buffer)
	if _, err := b.WriteString(fmt.Sprintf("%d\n", leaderEpochFileV0)); err != nil {
		return err
	}
	if _, err := b.WriteString(fmt.Sprintf("%d\n", len(l.epochOffsets))); err != nil {
		return err
	}
	for _, epoch := range l.epochOffsets {
		if _, err := b.WriteString(fmt.Sprintf("%d %d\n", epoch.leaderEpoch, epoch.startOffset)); err != nil {
			return err
		}
	}
	return AtomicWriteFileWithRetry(l.checkpointFile, b)
}

func (l *leaderEpochCache) warn(epoch, latestEpoch uint64, offset, latestOffset int64) {
	msg := slog.String("message", l.epochChangeMsg(epoch, latestEpoch, offset, latestOffset))

	// Every refusal logs, including the one no arm anticipated. The two named
	// cases below are strict comparisons, so an assignment of the epoch already
	// latest at an offset at or after its own — a reassignment, which the doc on
	// Assign says is not allowed — fell between them and produced NOTHING: no
	// entry written, no line logged, and a nil error returned.
	//
	// That is indistinguishable from a successful assign at the call site and
	// afterwards, and it cost a downstream team an investigation: their side
	// believed it held epoch history it had never been able to record. A default
	// arm here is not defensive padding; it is the difference between a caller
	// learning immediately and learning from a data loss.
	switch {
	case epoch < latestEpoch:
		slog.Warn("Received log leader epoch assignment for an epoch < latest epoch. "+
			"This implies messages have arrived out of order",
			msg,
		)
	case offset < latestOffset:
		// The trailing "%s" this carried was not a format verb — slog.Warn takes
		// attributes, not a format string — so it was printed literally.
		slog.Warn("Received log leader epoch assignment for an offset < latest offset "+
			"for the most recently stored leader epoch. This implies messages have arrived out of order.",
			msg,
		)
	default:
		slog.Warn("Refused a log leader epoch assignment that would reassign an epoch "+
			"already recorded. An epoch's start offset is fixed once assigned, so "+
			"this assignment was dropped",
			msg,
		)
	}
}

func (l *leaderEpochCache) epochChangeMsg(newEpoch, lastEpoch uint64, newOffset, lastOffset int64) string {
	return fmt.Sprintf("New: {epoch:%d, offset:%d}, Previous: {epoch:%d, offset:%d} for log %s",
		newEpoch, newOffset, lastEpoch, lastOffset, l.name)
}

// readLeaderEpochOffsets reads the contents of the leader epoch checkpoint
// file, which is of the following form:
//
// v0:
// version
// num_entries
// leader_epoch start_offset
// leader_epoch start_offset
// ...
func readLeaderEpochOffsets(file io.Reader) ([]*epochOffset, error) {
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanWords)
	if !scanner.Scan() {
		return nil, errors.New("missing version")
	}
	version, err := strconv.Atoi(scanner.Text())
	if err != nil {
		return nil, pkgErrors.Wrap(err, "invalid file version value")
	}
	// Exact equality, not `> leaderEpochFileV0`. Atoi is SIGNED, and v0 is the
	// first version, so "reject anything newer" also silently meant "accept
	// anything older" — which is not the empty set here: a version line reading
	// "-1" passed and the file was then read as v0. That is the same laundering
	// the epoch parse below refuses, and it matters for the same reason, spelled
	// out there: this file carries no checksum, so its parse is the only
	// integrity check it ever gets.
	if version != leaderEpochFileV0 {
		return nil, fmt.Errorf("unknown version: %d", version)
	}
	if !scanner.Scan() {
		return nil, errors.New("missing number of entries")
	}
	numEntries, err := strconv.Atoi(scanner.Text())
	if err != nil {
		return nil, pkgErrors.Wrap(err, "invalid entries count value")
	}

	var (
		epochs       = make(map[uint64]int64)
		epochOffsets = []*epochOffset{}
	)

	for scanner.Scan() {
		// ParseUint, not ParseInt. An epoch is uint64 in every other line of
		// this file, and this is the one place a value from OUTSIDE the process
		// becomes one. ParseInt accepted "-1" and the conversion to uint64 made
		// it 2^64-1: the highest epoch representable, so a corrupt checkpoint
		// parsed as a valid one that outranks every real epoch and pins
		// latestEpoch() there for good. Nothing downstream can tell that value
		// from a genuine epoch, and the file carries no checksum, so refusing
		// it here is the only chance to notice.
		leaderEpoch, err := strconv.ParseUint(scanner.Text(), 10, 64)
		if err != nil {
			return nil, pkgErrors.Wrap(err, "invalid leader epoch value")
		}
		if !scanner.Scan() {
			return nil, errors.New("missing start offset for epoch")
		}
		startOffset, err := strconv.ParseInt(scanner.Text(), 10, 64)
		if err != nil {
			return nil, pkgErrors.Wrap(err, "invalid epoch start offset value")
		}

		if _, ok := epochs[leaderEpoch]; ok {
			// Duplicate entry.
			return nil, fmt.Errorf("duplicate leader epoch %d", leaderEpoch)
		}
		epochs[leaderEpoch] = startOffset
		epochOffsets = append(epochOffsets, &epochOffset{
			leaderEpoch: leaderEpoch,
			startOffset: startOffset,
		})
	}

	if numEntries != len(epochOffsets) {
		return nil, fmt.Errorf("expected %d entries, got %d",
			numEntries, len(epochOffsets))
	}

	return epochOffsets, nil
}
