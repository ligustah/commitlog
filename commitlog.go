// Package commitlog provides an implementation for a file-backed write-ahead log.
package commitlog

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ligustah/commitlog/compress"
	atomic_file "github.com/natefinch/atomic"
	"github.com/pkg/errors"
)

// ErrSegmentNotFound is returned if the segment could not be found.
var ErrSegmentNotFound = errors.New("segment not found")

// ErrIncorrectOffset is returned if the offset is incorrect. This is used in case Optimistic
// Concurrency Control is activated.
var ErrIncorrectOffset = errors.New("incorrect offset")

// ErrTimestampBeforeLog reports that a timestamp lookup asked for a point
// EARLIER than anything the log still holds — so there is no offset at or
// before it, and none is coming: retention only moves that boundary forward.
//
// A sentinel because the distinction matters to an unattended caller. "The log
// does not go back that far" is a normal answer that such a caller should
// absorb — clamp to the oldest offset and carry on — while a genuine index or
// I/O failure is not. Without something to compare against, the two are one
// opaque error and the safe handling of each is the wrong handling of the
// other.
var ErrTimestampBeforeLog = errors.New("commitlog: timestamp is before the beginning of the log")

// ErrBlockFormat reports a segment written in a block format this build
// does not understand. Callers probe for it at startup (before touching
// anything) so an incompatible store is refused rather than half-read:
// discovering the mismatch mid-replay means state has already been
// mutated under a layout we were guessing at.
var ErrBlockFormat = errors.New("unsupported block format version")

const (
	logFileSuffix   = ".log"
	indexFileSuffix = ".index"
	hwFileName      = "replication-offset-checkpoint"
	// maxSyncWindow caps how long a flush leader waits for others to join it.
	// The window tracks the last flush's duration, so a single pathological
	// fsync would otherwise park every later commit behind its outlier.
	maxSyncWindow = 2 * time.Millisecond
	// minSyncWindow is the floor the window decays to when no one is joining.
	// It stays non-zero so a second committer can still be seen and re-arm the
	// batching, and it is small enough beside an fsync to cost a lone caller
	// nothing measurable.
	minSyncWindow               = 20 * time.Microsecond
	defaultMaxSegmentBytes      = 1073741824
	defaultHWCheckpointInterval = 5 * time.Second
	defaultCleanerInterval      = 5 * time.Minute
)

// commitLog implements the CommitLog interface, which is a durable write-ahead
// log.
type commitLog struct {
	readonly       int32 // Atomic flag
	deleteCleaner  *deleteCleaner
	compactCleaner *compactCleaner
	name           string
	mu             sync.RWMutex
	// cleanMu serializes segment-list maintenance (Clean, Truncate,
	// TruncateBefore). Clean scans and rewrites segments outside mu so reads
	// and appends stay concurrent; without cleanMu a concurrent truncation (or
	// second Clean) can delete segment files mid-rewrite and the final swap
	// resurrects them. Lock order: cleanMu before mu, never the reverse.
	cleanMu sync.Mutex
	// appendMu makes an append atomic from "read the tail" to "write there".
	// An append reads the active segment's next offset and position, encodes a
	// message set stamped with them, and only then takes the segment's write
	// lock — so without this two concurrent appends read the SAME tail, are both
	// stamped with it, and both write over the same byte range. The loser's
	// records are gone and the offset sequence has duplicates, with no error to
	// either caller. Callers that happened to serialize their own writes never
	// saw it; one that stopped doing so lost 29 of 32 records.
	//
	// The encoding sits inside the critical section because the offsets are
	// baked into the encoded bytes, so it cannot be hoisted out without
	// reserving offset ranges and writing out of order. Appends therefore
	// serialize against each other — but not against fsyncs, which is what
	// actually matters for throughput and is why the sync path deliberately
	// runs outside the segment lock.
	//
	// Lock order: appendMu before mu, never the reverse.
	appendMu sync.Mutex
	// The group-commit barrier behind Sync. syncDurable is the highest offset
	// known to be on stable storage; syncFlushing says a flush is in flight and
	// syncDone is closed when it finishes. Concurrent callers whose offset is
	// already covered return without an fsync of their own, which is the whole
	// point: N commits cost one fsync rather than N. Guarded by syncMu, which is
	// held only around the bookkeeping, never across the fsync itself.
	// tierReadOnly withholds this log's right to write to its SegmentStore.
	// Guarded by its own mutex because a pass reads it while the owner may be
	// flipping it.
	tierMu       sync.RWMutex
	tierReadOnly bool
	// reclaim holds store objects a rewrite superseded, waiting for the readers
	// still on them to finish. Drained at the start of a clean pass.
	reclaim []pendingReclaim
	// tierManifestStale records that the last manifest publish did not land, so
	// the manifest may still name a superseded object. Reclamation stops while
	// it is set rather than delete something the manifest points at.
	tierManifestStale bool
	syncMu            sync.Mutex
	syncDurable       int64
	syncFlushing      bool
	syncDone          chan struct{}
	// syncWindow is how long the next flush's leader holds the door open for
	// other committers to join it, set to the previous flush's duration, and
	// syncJoined counts how many joined the flush in flight.
	//
	// The window is what makes the barrier batch at all. Without it — flushing
	// the moment leadership is taken, and letting everyone else queue behind the
	// flush in flight — only callers arriving DURING an fsync are captured, and
	// on a fast disk that is a sliver of the cycle: measured at 64 concurrent
	// committers, 2323 flushes led against 1011 rides. With the window it is 51
	// against 3149.
	syncWindow time.Duration
	syncJoined int
	// Instrumentation for the batching tests: how many Sync calls led a flush
	// versus rode someone else's. Counted under syncMu.
	syncLeaders      int64
	syncFollowers    int64
	hw               int64
	closed           chan struct{}
	closeOnce        sync.Once      // guards close(l.closed)
	bgWG             sync.WaitGroup // tracks the checkpoint + cleaner loops
	segmentsClosed   bool           // guards closeSegments (under mu)
	segments         []*segment
	vActiveSegment   *segment
	hwWaiters        map[contextReader]chan bool
	leaderEpochCache *leaderEpochCache
	deleted          bool
	Options
}

// Options contains settings for configuring a commitLog.
type Options struct {
	Name            string        // commitLog name
	Path            string        // Path to log directory
	MaxSegmentBytes int64         // Max bytes a Segment can contain before creating a new one
	MaxSegmentAge   time.Duration // Max time before a new log segment is rolled out.
	MaxLogBytes     int64         // Retention by bytes
	MaxLogMessages  int64         // Retention by messages
	MaxLogAge       time.Duration // Retention by age
	// MaxTier* bound the segments whose bytes have been offloaded to the
	// SegmentStore, separately from the ones still on local disk. Retention is
	// PER TIER: a segment over the local budget that also exists in a store has
	// left the tier those limits govern rather than being deleted, and the
	// record is gone only when the last tier's limit is reached.
	//
	// The limits above therefore bound LOCAL disk alone and no longer count
	// offloaded segments — counting them would delete records to reclaim space
	// that offloading already reclaimed.
	//
	// Zero keeps everything in the tier, which is what makes this compatible: a
	// log with no SegmentStore has no offloaded segments, so these never apply.
	MaxTierBytes         int64
	MaxTierMessages      int64
	MaxTierAge           time.Duration
	Compact              bool // Run compaction on log clean
	CompactMaxGoroutines int  // Max number of goroutines to use in a log compaction
	// CompactMinAge is a protected compaction horizon: a segment is not eligible
	// for compaction until its most recent write is at least this old, so recent
	// segments keep their full per-record history. Zero disables the lag (any
	// sealed segment may be compacted).
	//
	// It is not a performance knob. It is the bound on HOW FAR A CONSUMER MAY
	// LAG and still see every version of a key rather than only the latest.
	// Compaction is defined to preserve the latest value per key, not the
	// sequence of values, so a reader resuming from an offset older than this
	// horizon finds intermediate versions already gone — the log is intact and
	// its own contract is met, but a consumer maintaining anything derived from
	// the SEQUENCE of changes (an incremental view, a downstream replica, a
	// change feed) has silently missed updates it needed.
	//
	// So size it against the worst lag a dependent consumer may accumulate —
	// including downtime and rebuild time — not against compaction cost. The
	// failure it prevents is invisible at the point it happens: nothing errors,
	// the consumer simply holds a view that no longer matches the log.
	CompactMinAge time.Duration
	// CompactTombstoneRetention enables tombstone GC on plain (spec-less)
	// Clean calls: a latest-per-key record carrying AttrTombstone older than
	// this is removed entirely, so the key vanishes. Intended for
	// NON-transactional compacted logs (transactional layers pass their own
	// CleanSpec instead, with transaction-aware bounds). Zero disables.
	CompactTombstoneRetention time.Duration
	// PrefixReadCoalesceBytes and PrefixReadTierCoalesceBytes are how large a
	// gap between two wanted records ReadKeyPrefix reads THROUGH rather than
	// splitting into a second request — for LOCAL segments and for segments
	// offloaded to the SegmentStore. Zero takes the defaults; NEGATIVE means
	// never coalesce, i.e. one request per isolated record.
	//
	// They are separate settings because the right answer depends on the DEVICE,
	// and the tier is only where the setting can be attached. "Local" is not one
	// kind of storage: on a spinning disk a seek costs milliseconds, so reading
	// through megabytes to avoid one is a bargain and the window should be
	// large; on an NVMe random access is nearly free and the same window is
	// mostly wasted transfer. The local default assumes the unfavourable case
	// (megabytes); lower it, and raise PrefixReadConcurrency, on fast random-
	// access storage.
	//
	// A STORE charges per request and serves many at once, so splitting is what
	// gives the fan-out something to parallelize. The default is far smaller,
	// and where reads are priced per GB the right value is computable rather
	// than guessable: reading through a gap transfers bytes that are discarded,
	// splitting costs one more request, so coalescing pays exactly while
	//
	//	C_req > (gap / 1e9) * C_GB      i.e.      gap < 1e9 * C_req / C_GB
	//
	// At, say, $0.0004/1k GETs and $0.09/GB that breakeven is a few KB. Where
	// bytes are effectively free — a store read from inside the same region —
	// the right-hand side runs away and coalescing always wins on price.
	//
	// Per-request LATENCY is deliberately NOT part of the trade. A round trip is
	// worth a lot of skipped bytes only when requests go out one at a time; with
	// enough in flight it is hidden, and price is what remains.
	// PrefixReadTierConcurrency is what keeps them in flight, so the two work
	// together: a smaller gap means more requests, and the fan-out is what makes
	// that cheap in time.
	PrefixReadCoalesceBytes     int64
	PrefixReadTierCoalesceBytes int64
	// PrefixReadConcurrency and PrefixReadTierConcurrency are how many record
	// reads ReadKeyPrefix keeps in flight against LOCAL segments and against
	// segments offloaded to the SegmentStore. Zero takes the defaults.
	//
	// The unit is a RUN — a span of wanted records read contiguously (see
	// PrefixReadCoalesceBytes) — not a segment, so a prefix whose keys are
	// concentrated in a few segments still fans out.
	//
	// They are enforced INDEPENDENTLY, so a log holding both tiers does not have
	// its store reads throttled behind its disk reads.
	//
	// How wide either should be is a property of the DEVICE. A store serves many
	// requests at once, so keeping them in flight is how its round trips become
	// throughput, and the tier default is high. Local is where it genuinely
	// depends: on a spinning disk concurrent random reads defeat each other,
	// since the queue serializes on one head and parallelism buys seeks rather
	// than bandwidth; on an NVMe a deep queue is exactly how the device is
	// saturated. The local default assumes the unfavourable case, so it is
	// modest — on fast random-access storage there is no reason it should not
	// match or exceed the tier value.
	//
	// Neither is CompactMaxGoroutines: that bounds segment rewrites, which are
	// CPU- and write-bound, not scattered reads that spend their time waiting.
	PrefixReadConcurrency     int
	PrefixReadTierConcurrency int
	// TierReadOnly opens the log without the right to write to its
	// SegmentStore: no offload, no rewrite of a tiered segment, no tier
	// retention, no object deletes. Reads are unaffected.
	//
	// This is what a process runs when it does not own the tier. Flip it with
	// SetTierReadOnly when ownership moves.
	TierReadOnly bool
	// AdoptOptions records THESE options as the log's descriptor instead of
	// checking against the one already on disk. It is the deliberate answer to
	// the two cases New otherwise refuses: retuning an existing log's compaction
	// settings, and opening a log created before descriptors existed (which has
	// none). Requiring an explicit opt-in is the point — an accidentally empty
	// config must not be able to redefine what a log keeps. Ignored for a log
	// being created, which simply records what it was created with.
	AdoptOptions bool
	// DisableAutoClean stops the internal cleaner loop from running Clean.
	// Segment splitting (MaxSegmentAge rolls) keeps running. For logs whose
	// owner drives cleaning explicitly (CleanWithSpec) — an automatic clean
	// has no transaction awareness and must not race the owner's policy.
	DisableAutoClean     bool
	CleanerInterval      time.Duration // Frequency to enforce retention policy
	HWCheckpointInterval time.Duration // Frequency to checkpoint HW to disk
	ConcurrencyControl   bool          // Optimistic Concurrency Control
	// Compression selects the block-compression codec for newly created
	// segments. The zero value (compress.None) disables compression and is
	// byte-for-byte compatible with logs written before compression existed;
	// existing segments keep whatever format they were written in.
	Compression compress.Codec
	// SegmentStore, when set, is the tier below local disk that OffloadBefore
	// moves sealed segments' log bytes into. Reads of an offloaded segment go
	// through the store transparently. Nil disables tiering (the default). The
	// caller scopes the store per-log (e.g. a directory or object-store prefix per
	// stream) since segment keys are the bare base offset.
	SegmentStore SegmentStore
	// RemoteIndexCache, when set (with SegmentStore), enables tiered-storage
	// option 2: OffloadBefore also offloads each sealed segment's index object and
	// drops the local index, so no per-segment index file remains on local disk.
	// Reads fetch the index into this process-wide LRU cache on demand. Nil keeps
	// option 1 (index stays local). Share ONE cache across every log in the
	// process for a single on-disk budget.
	RemoteIndexCache *RemoteIndexCache
}

// New creates a new CommitLog and starts a background goroutine which
// periodically checkpoints the high watermark to disk.
func New(opts Options) (CommitLog, error) {
	if opts.Path == "" {
		return nil, errors.New("path is empty")
	}

	if opts.MaxSegmentBytes == 0 {
		opts.MaxSegmentBytes = defaultMaxSegmentBytes
	}
	if opts.HWCheckpointInterval == 0 {
		opts.HWCheckpointInterval = defaultHWCheckpointInterval
	}
	if opts.CleanerInterval == 0 {
		opts.CleanerInterval = defaultCleanerInterval
	}

	cleanerOpts := deleteCleanerOptions{
		Name: opts.Path,
	}
	cleanerOpts.Retention.Bytes = opts.MaxLogBytes
	cleanerOpts.Retention.Messages = opts.MaxLogMessages
	cleanerOpts.Retention.Age = opts.MaxLogAge
	cleanerOpts.Retention.TierBytes = opts.MaxTierBytes
	cleanerOpts.Retention.TierMessages = opts.MaxTierMessages
	cleanerOpts.Retention.TierAge = opts.MaxTierAge
	cleaner := newDeleteCleaner(cleanerOpts)

	compactCleanerOpts := compactCleanerOptions{
		Name:               opts.Name,
		MaxGoroutines:      opts.CompactMaxGoroutines,
		MinAge:             opts.CompactMinAge,
		TombstoneRetention: opts.CompactTombstoneRetention,
	}
	compactCleaner := newCompactCleaner(compactCleanerOpts)
	compactCleaner.cache = opts.RemoteIndexCache

	path, _ := filepath.Abs(opts.Path)
	epochCache, err := newLeaderEpochCache(opts.Name, path)
	if err != nil {
		return nil, err
	}

	l := &commitLog{
		Options:          opts,
		name:             filepath.Base(path),
		deleteCleaner:    cleaner,
		compactCleaner:   compactCleaner,
		hw:               -1,
		closed:           make(chan struct{}),
		hwWaiters:        make(map[contextReader]chan bool),
		leaderEpochCache: epochCache,
		// -1, not 0: offset 0 is a real record, so a zero value would report the
		// log's very first append as already durable and skip its flush.
		syncDurable:  -1,
		tierReadOnly: opts.TierReadOnly,
	}

	if err := l.init(); err != nil {
		return nil, err
	}

	// Settle what this log IS before opening anything. It has to happen here:
	// once open() runs, the cleaner loop can start applying a retention policy,
	// and the whole point is that a policy the log was not created with never
	// gets applied at all.
	descOpts := opts
	descOpts.Path = path
	isNew, err := logIsNew(path)
	if err != nil {
		return nil, err
	}
	if err := reconcileDescriptor(descOpts, isNew); err != nil {
		return nil, err
	}

	if err := l.open(); err != nil {
		return nil, err
	}

	// After an unclean shutdown, the leader epoch checkpoint file could be
	// ahead of the log (as the log is flushed asynchronously by default). To
	// account for this, remove all entries from the leader epoch checkpoint
	// file where the offset is greater than the log end offset.
	if err := l.leaderEpochCache.ClearLatest(l.activeSegment().NextOffset()); err != nil {
		return nil, err
	}

	// The earliest leader epoch may not be flushed during a hard failure.
	// Recover it here.
	if err := l.leaderEpochCache.ClearEarliest(l.OldestOffset()); err != nil {
		return nil, err
	}

	// Track the background loops so Close/Delete can wait for them to exit before
	// closing segments (otherwise a loop mid-iteration keeps operating on segment
	// files after they are closed, which on Windows holds file handles/mmaps and
	// makes reopening the same path fail).
	l.bgWG.Add(2)
	go func() { defer l.bgWG.Done(); l.checkpointHWLoop() }()
	go func() { defer l.bgWG.Done(); l.cleanerLoop() }()

	return l, nil
}

func (l *commitLog) init() error {
	err := os.MkdirAll(l.Path, 0755)
	if err != nil {
		return errors.Wrap(err, "mkdir failed")
	}
	return nil
}

func (l *commitLog) open() error {
	files, err := ioutil.ReadDir(l.Path)
	if err != nil {
		return errors.Wrap(err, "read dir failed")
	}
	for _, file := range files {
		// If this file is an index file, make sure it has a corresponding .log
		// file OR an .offloaded marker (an offloaded segment keeps its index
		// local but has no local .log). Only a truly orphaned index is removed.
		if strings.HasSuffix(file.Name(), indexFileSuffix) {
			stem := strings.TrimSuffix(file.Name(), indexFileSuffix)
			_, logErr := os.Stat(filepath.Join(l.Path, stem+logFileSuffix))
			_, offErr := os.Stat(filepath.Join(l.Path, stem+offloadedSuffix))
			if os.IsNotExist(logErr) && os.IsNotExist(offErr) {
				if err := os.Remove(filepath.Join(l.Path, file.Name())); err != nil {
					return err
				}
			} else if logErr != nil && !os.IsNotExist(logErr) {
				return errors.Wrap(logErr, "stat file failed")
			}
		} else if strings.HasSuffix(file.Name(), offloadedSuffix) {
			offsetStr := strings.TrimSuffix(file.Name(), offloadedSuffix)
			baseOffset, err := strconv.Atoi(offsetStr)
			if err != nil {
				return err
			}
			if l.SegmentStore == nil {
				return errors.Errorf("commitlog: segment %d is offloaded but no SegmentStore is configured", baseOffset)
			}
			meta, err := readOffloadMarker(filepath.Join(l.Path, file.Name()))
			if err != nil {
				return errors.Wrap(err, "read offload marker failed")
			}
			if meta.IndexKey != "" && l.RemoteIndexCache == nil {
				return errors.Errorf("commitlog: segment %d has an offloaded index but no RemoteIndexCache is configured", baseOffset)
			}
			segment, err := openOffloadedSegment(l.Path, int64(baseOffset), l.MaxSegmentBytes, l.Compression, l.SegmentStore, meta, l.RemoteIndexCache)
			if err != nil {
				return err
			}
			l.segments = append(l.segments, segment)
		} else if strings.HasSuffix(file.Name(), logFileSuffix) {
			offsetStr := strings.TrimSuffix(file.Name(), logFileSuffix)
			baseOffset, err := strconv.Atoi(offsetStr)
			if err != nil {
				return err
			}
			segment, err := newSegment(l.Path, int64(baseOffset), l.MaxSegmentBytes, false, "", l.Compression)
			if err != nil {
				return err
			}
			l.segments = append(l.segments, segment)
		} else if file.Name() == hwFileName {
			// Recover high watermark.
			b, err := ioutil.ReadFile(filepath.Join(l.Path, file.Name()))
			if err != nil {
				return errors.Wrap(err, "read high watermark file failed")
			}
			hw, err := strconv.ParseInt(string(b), 10, 64)
			if err != nil {
				return errors.Wrap(err, "parse high watermark file failed")
			}
			l.hw = hw
		}
	}
	// Take whatever the STORE says its tier holds and this log does not. Local
	// markers have already been read above and win where both describe a
	// segment; this only fills gaps.
	//
	// It is what makes the tier self-describing rather than an appendage of
	// this directory: a process holding the store and an empty or partial log
	// directory opens the log and reaches the offloaded records, without being
	// handed bookkeeping by anyone.
	if l.SegmentStore != nil {
		objs, err := readTierManifest(l.SegmentStore)
		if err != nil {
			return err
		}
		if _, err := l.adoptTierManifestLocked(objs); err != nil {
			return err
		}
	}
	// A log whose newest segment is offloaded has nowhere to append: every
	// offloaded segment is sealed, and the active segment must be local and
	// writable. This is the normal state after adopting a tier into an empty
	// directory, so give it one starting where the tier ends.
	if n := len(l.segments); n > 0 && l.segments[n-1].isOffloaded() {
		next := l.segments[n-1].NextOffset()
		segment, err := newSegment(l.Path, next, l.MaxSegmentBytes, true, "", l.Compression)
		if err != nil {
			return err
		}
		l.segments = append(l.segments, segment)
	}
	if len(l.segments) == 0 {
		segment, err := newSegment(l.Path, 0, l.MaxSegmentBytes, true, "", l.Compression)
		if err != nil {
			return err
		}
		l.segments = append(l.segments, segment)
	}
	activeSegment := l.segments[len(l.segments)-1]
	// A crash can leave the active segment's log physically ahead of its index
	// (the write path appends the log frame before its index entry, and
	// checkpointHW fsyncs only the log). Rebuild the missing index tail so
	// NewestOffset / NextOffset reflect the true physical log — otherwise a seek
	// and a sequential scan disagree on offsets and the next append can collide
	// with an un-indexed record.
	if err := activeSegment.reconcileIndexTail(); err != nil {
		return err
	}
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&l.vActiveSegment)),
		unsafe.Pointer(activeSegment))
	return nil
}

// Append writes the given batch of messages to the log and returns their
// corresponding offsets in the log. This will return ErrCommitLogReadonly if
// the log is in readonly mode.
func (l *commitLog) Append(msgs []*Message) ([]int64, error) {
	if l.IsReadonly() {
		return nil, ErrCommitLogReadonly
	}
	// Stamp append time on messages that carry no timestamp (Kafka's
	// LogAppendTime as the fallback for an unset CreateTime). Every
	// time-based feature — age retention, MaxSegmentAge rolling, the
	// CompactMinAge horizon, the timestamp-search APIs — reads segment
	// write times derived from these; producers that never stamp
	// timestamps would otherwise leave segments looking infinitely old
	// (age retention deletes everything, the compaction horizon protects
	// nothing). AppendMessageSet takes pre-encoded bytes and cannot be
	// stamped; replicating callers are expected to carry source timestamps.
	now := timestamp()
	for _, m := range msgs {
		if m.Timestamp == 0 {
			m.Timestamp = now
		}
	}
	// Reading the tail and writing to it must be one step; see appendMu.
	l.appendMu.Lock()
	defer l.appendMu.Unlock()
	if _, err := l.checkAndPerformSplit(); err != nil {
		return nil, err
	}
	var (
		segment          = l.activeSegment()
		basePosition     = segment.Position()
		baseOffset       = segment.NextOffset()
		ms, entries, err = newMessageSetFromProto(baseOffset, basePosition, msgs, l.IsConcurrencyControlEnabled())
	)
	if err != nil {
		return nil, err
	}
	return l.append(segment, ms, entries)
}

// AppendMessageSet writes the given message set data to the log and returns
// the corresponding offsets in the log. This can be called even if the log is
// in readonly mode to allow for reconciliation, e.g. when replicating from
// another log.
func (l *commitLog) AppendMessageSet(ms []byte) ([]int64, error) {
	// Same atomicity requirement as Append: the entries are derived from the
	// segment's current position, so reading it and writing there cannot be
	// interleaved with another append.
	l.appendMu.Lock()
	defer l.appendMu.Unlock()
	if _, err := l.checkAndPerformSplit(); err != nil {
		return nil, err
	}
	var (
		segment      = l.activeSegment()
		basePosition = segment.Position()
		entries      = entriesForMessageSet(basePosition, ms)
	)
	return l.append(segment, ms, entries)
}

func (l *commitLog) append(segment *segment, ms []byte, entries []*entry) ([]int64, error) {
	if err := segment.WriteMessageSet(ms, entries); err != nil {
		return nil, err
	}
	var (
		lastLeaderEpoch = l.leaderEpochCache.LastLeaderEpoch()
		offsets         = make([]int64, len(entries))
	)
	for i, entry := range entries {
		// Check if message is in a new leader epoch.
		if entry.LeaderEpoch > lastLeaderEpoch {
			// If it is, we need to assign the epoch offset.
			if err := l.leaderEpochCache.Assign(entry.LeaderEpoch, entry.Offset); err != nil {
				return nil, err
			}
			lastLeaderEpoch = entry.LeaderEpoch
		}
		offsets[i] = entry.Offset
	}
	return offsets, nil
}

// tierWritable reports whether this log may write to its SegmentStore.
func (l *commitLog) tierWritable() bool {
	l.tierMu.RLock()
	defer l.tierMu.RUnlock()
	return !l.tierReadOnly
}

// SetTierReadOnly grants or withdraws this log's right to write to its
// SegmentStore. See the interface doc.
func (l *commitLog) SetTierReadOnly(readOnly bool) {
	l.tierMu.Lock()
	l.tierReadOnly = readOnly
	l.tierMu.Unlock()
}

// errTierReadOnly is returned by anything that would write to a store this log
// does not own.
var errTierReadOnly = errors.New(
	"commitlog: this log's tier is read-only — it does not own writes to the store")

// pendingReclaim is one store object a rewrite superseded, waiting to be
// deleted.
//
// pin is the backing that was serving the object at the moment it was
// superseded, or nil where the object had no long-lived holder. The object is
// deletable once nothing still holds the pin: a reader that took the backing
// before the swap is reading the OLD object, and deleting it underneath them
// turns a rewrite into a read error.
type pendingReclaim struct {
	key string
	pin *storeBacking
}

// queueReclaim takes ownership of objects a rewrite superseded. They are
// deleted by a later drainReclaim, never here — see the type doc.
func (l *commitLog) queueReclaim(entries []pendingReclaim) {
	if len(entries) == 0 {
		return
	}
	l.tierMu.Lock()
	l.reclaim = append(l.reclaim, entries...)
	l.tierMu.Unlock()
}

// drainReclaim deletes the superseded objects no reader still holds, and keeps
// the rest for a later pass.
//
// Called at the START of a clean pass, which is what makes it safe on two
// counts that a delete at the point of rewrite cannot satisfy:
//
//   - Readers. A scan that took a backing before the rewrite is still on the old
//     object. By the next pass most are long gone, and one that is not simply
//     waits another pass — the queue is not a deadline.
//   - The manifest. Deleting an object the manifest still names leaves a
//     dangling reference: a reader that trusts the manifest opens a key that is
//     gone. The pass that queued these entries republished the manifest over the
//     NEW keys before returning, so by now nothing names them. If that publish
//     failed the queue is not drained at all (see tierManifestStale) — a crash
//     or an error between the two leaves an orphan, which UnreferencedObjects
//     reports and which costs storage rather than correctness.
//
// Errors are not returned. A store that refuses a delete has cost this log some
// space, not its consistency, and failing a clean — which may have just made a
// retention deadline — because a deletion of already-dead bytes did not land
// would be the worse trade. The entry stays queued and the next pass retries.
func (l *commitLog) drainReclaim() {
	if l.SegmentStore == nil {
		return
	}
	l.tierMu.Lock()
	if l.tierReadOnly || l.tierManifestStale || len(l.reclaim) == 0 {
		l.tierMu.Unlock()
		return
	}
	queued := l.reclaim
	l.reclaim = nil
	l.tierMu.Unlock()

	kept := make([]pendingReclaim, 0, len(queued))
	for _, e := range queued {
		if e.pin != nil && e.pin.referenced() {
			kept = append(kept, e)
			continue
		}
		if err := l.SegmentStore.Delete(e.key); err != nil {
			kept = append(kept, e)
		}
	}

	l.tierMu.Lock()
	l.reclaim = append(kept, l.reclaim...)
	l.tierMu.Unlock()
}

// DeleteStoreObjects removes objects from the SegmentStore. See the interface
// doc.
func (l *commitLog) DeleteStoreObjects(keys []string) ([]string, error) {
	if l.SegmentStore == nil || len(keys) == 0 {
		return nil, nil
	}
	if !l.tierWritable() {
		return nil, errTierReadOnly
	}
	deleted := make([]string, 0, len(keys))
	for _, key := range keys {
		if err := l.SegmentStore.Delete(key); err != nil {
			return deleted, errors.Wrapf(err, "delete %s", key)
		}
		deleted = append(deleted, key)
	}
	return deleted, nil
}

// UnreferencedObjects lists store objects no marker of this log names. See the
// interface doc — in particular, that "unreferenced" is judged only from THIS
// log's markers.
func (l *commitLog) UnreferencedObjects() ([]string, error) {
	if l.SegmentStore == nil {
		return nil, nil
	}
	keys, err := l.SegmentStore.List()
	if err != nil {
		return nil, errors.Wrap(err, "list store")
	}

	// Live means named by the STORE'S MANIFEST or read by one of this log's
	// segments — the union, because each alone is wrong in a different way.
	//
	// The manifest alone would miss an object this log is reading but has not
	// yet republished: a rewrite installs new objects and then writes the
	// manifest, and in between the segment is on a key the manifest does not
	// name yet.
	//
	// This log's segments alone would miss everything ANOTHER process
	// offloaded since this one opened, which on a shared store is exactly the
	// data that must not be collected. That is the difference between
	// "unreferenced by me" and "unreferenced", and getting it wrong deletes
	// data a live peer is serving.
	//
	// The manifest itself is never garbage: it is what makes the tier
	// readable, and nothing references it.
	referenced := make(map[string]bool, len(keys))
	referenced[manifestKey] = true
	if manifest, err := readTierManifest(l.SegmentStore); err == nil {
		for _, o := range manifest {
			if o.LogKey != "" {
				referenced[o.LogKey] = true
			}
			if o.IndexKey != "" {
				referenced[o.IndexKey] = true
			}
		}
	} else {
		// Refuse rather than under-report. A manifest that exists but cannot be
		// read tells us nothing about what is live, and a garbage list built
		// without it would name objects the tier still depends on.
		return nil, errors.Wrap(err, "read tier manifest")
	}
	l.mu.RLock()
	for _, s := range l.segments {
		s.RLock()
		if s.storeKey != "" {
			referenced[s.storeKey] = true
		}
		if s.indexKey != "" {
			referenced[s.indexKey] = true
		}
		s.RUnlock()
	}
	l.mu.RUnlock()

	var orphans []string
	for _, key := range keys {
		if !referenced[key] {
			orphans = append(orphans, key)
		}
	}
	sort.Strings(orphans)
	return orphans, nil
}

// ReadMessageSet returns the log's own framing verbatim, starting at offset.
// See the interface doc for the contract.
func (l *commitLog) ReadMessageSet(offset int64, maxBytes int) ([]byte, error) {
	if maxBytes <= 0 {
		return nil, errors.New("commitlog: maxBytes must be positive")
	}
	l.mu.RLock()
	segments := make([]*segment, len(l.segments))
	copy(segments, l.segments)
	l.mu.RUnlock()

	seg, contains := findSegmentContains(segments, offset)
	if seg == nil {
		return nil, ErrSegmentNotFound
	}
	// Below the oldest surviving record, clamp up to it rather than erroring:
	// a follower resuming from a position retention has since passed should
	// carry on from what is left, exactly as the readers do.
	start := int64(0)
	if contains {
		e, err := seg.findEntry(offset)
		if err != nil {
			return nil, err
		}
		start = e.Position
	}

	// Whole frames only. A partial message set is not something a follower can
	// append, so a maxBytes smaller than the first frame yields that frame
	// rather than a truncation the caller cannot use — starving a follower is
	// worse than overshooting its budget once.
	var (
		out = make([]byte, 0, maxBytes)
		ss  = &segmentScanner{s: seg, pos: start, cache: newBlockCache()}
	)
	for {
		ms, _, err := ss.Scan()
		if err != nil {
			break // end of this segment's readable bytes
		}
		if len(out) > 0 && len(out)+len(ms) > maxBytes {
			break
		}
		out = append(out, ms...)
		if len(out) >= maxBytes {
			break
		}
	}
	return out, nil
}

// NewestOffset returns the offset of the last message in the log or -1 if
// empty.
func (l *commitLog) NewestOffset() int64 {
	return l.activeSegment().NextOffset() - 1
}

// RecoverTail reconciles the high watermark with the log's REAL tail after a
// crash. The HW checkpoint is periodic (≤ HWCheckpointInterval stale), so a
// reopened log can hold committed, previously-SERVED records above it;
// truncating to the checkpoint (the old recovery) retroactively unwrote them
// — re-emission after replay consolidates batches differently, so tailing
// consumers were left holding rows the new history never retracts, and
// offset markers persisted elsewhere (state WALs) overstated the truncated
// tail. Instead, walk the suffix above the checkpoint: every structurally
// valid record is recovered (visibility above the HW stays gated by
// transaction markers — a dangling open tx is aborted by recovery exactly as
// before); only a torn suffix (power loss mid-write) is truncated.
func (l *commitLog) RecoverTail() error {
	hw := l.HighWatermark()
	newest := l.NewestOffset()
	if newest <= hw {
		return nil
	}
	start := hw + 1
	if oldest := l.OldestOffset(); oldest >= 0 && start < oldest {
		start = oldest
	}
	// A non-blocking (no-wait) scan: it returns io.EOF the moment it drains the
	// readable bytes rather than parking for appends that will never arrive.
	// Recovery scans a static tail, so if the reconstructed LEO (newest) ever
	// overshoots the log actually on disk, this terminates instead of hanging.
	r, err := l.newRecoveryReader(start)
	if err != nil {
		// Nothing readable above the checkpoint: keep the old amputation.
		return l.Truncate(hw + 1)
	}
	lastGood := hw
	headers := make([]byte, msgSetHeaderLen)
	ctx := context.Background()
	for {
		_, off, _, _, rerr := r.ReadMessage(ctx, headers)
		if rerr != nil {
			if errors.Is(rerr, ErrCommitLogReadonly) {
				break
			}
			if errors.Is(rerr, io.EOF) {
				// The readable log drained before the reconstructed LEO: the
				// segment metadata claims more records than the log holds on
				// disk (an index-ahead-of-log inconsistency). Truncate the
				// un-backed phantom suffix rather than trusting the index.
				slog.Warn(
					"commitlog: recovery LEO overshoots the log on disk; truncating un-backed tail",
					slog.String("path", l.Path),
					slog.Int64("lastGood", lastGood),
					slog.Int64("expectedNewest", newest),
				)
			}
			// Torn or phantom suffix: keep everything before it, drop the rest.
			if terr := l.Truncate(lastGood + 1); terr != nil {
				return terr
			}
			break
		}
		lastGood = off
		if off >= newest {
			break
		}
	}
	if lastGood > hw {
		l.SetHighWatermark(lastGood)
	}
	return nil
}

// ActiveSegmentBase returns the base offset of the active (unsealed) segment.
// Cleaning passes only rewrite segments sealed before they start, so offsets
// at or above this value are untouched by any concurrently running clean.
func (l *commitLog) ActiveSegmentBase() int64 {
	return l.activeSegment().BaseOffset
}

// OldestOffset returns the offset of the first message in the log or -1 if
// empty.
func (l *commitLog) OldestOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].FirstOffset()
}

// EarliestOffsetAfterTimestamp returns the earliest offset whose timestamp is
// greater than or equal to the given timestamp.
func (l *commitLog) EarliestOffsetAfterTimestamp(timestamp int64) (int64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// Find the first segment whose base timestamp is greater than the given
	// timestamp.
	idx, err := findSegmentIndexByTimestamp(l.segments, timestamp)
	if err == io.EOF {
		// EOF indicates there is no such segment, meaning the timestamp is
		// beyond the end of the log so return the next assignable offset.
		return l.segments[len(l.segments)-1].NextOffset(), nil
	}
	if err != nil {
		return 0, errors.Wrap(err, "failed to find log segment for timestamp")
	}
	// Search the previous segment for the first entry whose timestamp is
	// greater than or equal to the given timestamp. If this is the first
	// segment, just search it.
	var seg *segment
	if idx == 0 {
		seg = l.segments[0]
	} else {
		seg = l.segments[idx-1]
	}
	entry, err := seg.findEntryByTimestamp(timestamp)
	if err == nil {
		return entry.Offset, nil
	}
	if err != ErrEntryNotFound && err != io.EOF {
		return 0, errors.Wrap(err, "failed to find log entry for timestamp")
	}
	// This indicates there are no entries in the segment whose timestamp
	// is greater than or equal to the target timestamp. In this case, search
	// the next segment if there is one. If there isn't, the timestamp is
	// beyond the end of the log so return the next assignable offset.
	if idx < len(l.segments)-1 {
		seg = l.segments[idx]
		entry, err := seg.findEntryByTimestamp(timestamp)
		if err != nil {
			return 0, errors.Wrap(err, "failed to find log entry for timestamp")
		}
		return entry.Offset, nil
	}
	return l.segments[len(l.segments)-1].NextOffset(), nil
}

// LatestOffsetBeforeTimestamp returns the latest offset whose timestamp is less
// than or equal to the given timestamp.
func (l *commitLog) LatestOffsetBeforeTimestamp(timestamp int64) (int64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// Find the first segment whose base timestamp is greater than the given
	// timestamp.
	idx, err := findSegmentIndexByTimestamp(l.segments, timestamp)
	if err != nil {
		return 0, errors.Wrap(err, "failed to find log segment for timestamp")
	}
	// Search the previous segment for the first entry whose timestamp is
	// greater than or equal to the given timestamp. If this is the first
	// segment, just search it.
	var seg *segment
	if idx == 0 {
		seg = l.segments[0]
		// if the given timestamp is before the start of the stream return an
		// error.
		if timestamp < seg.FirstWriteTime() {
			return 0, ErrTimestampBeforeLog
		}
	} else {
		seg = l.segments[idx-1]
	}

	// Find entry equal to or greater than the given timestamp.
	entry, err := seg.findEntryByTimestamp(timestamp)
	if err == nil {
		// If it's an exact match, return the offset.
		if entry.Timestamp == timestamp {
			return entry.Offset, nil
		}

		// Otherwise we want the previous offset.
		return entry.Offset - 1, nil
	}

	if err != ErrEntryNotFound && err != io.EOF {
		return 0, errors.Wrap(err, "failed to find log entry for timestamp")
	}

	// LastOffset(), not the bare field: this runs concurrently with appends,
	// which mutate lastOffset under the segment's write lock. Reading it
	// directly was a data race against every live writer — and this is a path
	// callers run unattended on a timer, so it raced whenever a probe happened
	// to land while a record was being written.
	return seg.LastOffset(), nil
}

// SetHighWatermark sets the high watermark on the log. All messages up to and
// including the high watermark are considered committed.
func (l *commitLog) SetHighWatermark(hw int64) {
	l.mu.Lock()
	if hw > l.hw {
		l.hw = hw
		l.notifyHWChange()
	}
	l.mu.Unlock()
	// TODO: should we flush the HW to disk here?
}

// OverrideHighWatermark sets the high watermark on the log using the given
// value, even if the value is less than the current HW. This is used for unit
// testing purposes.
func (l *commitLog) OverrideHighWatermark(hw int64) {
	l.mu.Lock()
	l.hw = hw
	l.notifyHWChange()
	l.mu.Unlock()
}

// notifyHWChange signals all HW waiters to wake up because the HW has changed.
// This must be called within the log mutex.
func (l *commitLog) notifyHWChange() {
	for r, ch := range l.hwWaiters {
		ch <- false
		delete(l.hwWaiters, r)
	}
}

// notifyReadonly signals all HW waiters to wake up if the HW is caught up to
// the LEO because the log has become readonly. This must be called within the
// log mutex.
func (l *commitLog) notifyReadonly() {
	if l.hw < l.NewestOffset() {
		return
	}
	// HW is caught up to LEO so notify HW waiters.
	for r, ch := range l.hwWaiters {
		ch <- true
		delete(l.hwWaiters, r)
	}
}

// waitForHW registers an HW waiter and returns a channel which will receive a
// bool either when the HW changes (false) or the log has become readonly
// (true).
func (l *commitLog) waitForHW(r contextReader, hw int64) <-chan bool {
	wait := make(chan bool, 1)
	l.mu.Lock()
	if l.hw != hw {
		// HW has changed since reader last checked so they can unblock now.
		wait <- false
	} else if l.hw == l.NewestOffset() && l.IsReadonly() {
		// Log is readonly and HW is caught up to LEO so return an error to reader.
		wait <- true
	} else {
		// Reader needs to wait for HW to advance.
		l.hwWaiters[r] = wait
	}
	l.mu.Unlock()
	return wait
}

func (l *commitLog) removeHWWaiter(r contextReader) {
	l.mu.Lock()
	delete(l.hwWaiters, r)
	l.mu.Unlock()
}

// HighWatermark returns the high watermark for the log.
func (l *commitLog) HighWatermark() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.hw
}

// NewLeaderEpoch indicates the log is entering a new leader epoch.
func (l *commitLog) NewLeaderEpoch(epoch uint64) error {
	return l.leaderEpochCache.Assign(epoch, l.NewestOffset())
}

// LastOffsetForLeaderEpoch returns the start offset of the first leader epoch
// larger than the provided one or the log end offset if the current epoch
// equals the provided one.
func (l *commitLog) LastOffsetForLeaderEpoch(epoch uint64) int64 {
	offset := l.leaderEpochCache.LastOffsetForLeaderEpoch(epoch)
	if offset == -1 {
		offset = l.activeSegment().NextOffset() - 1
	}
	return offset
}

// LastLeaderEpoch returns the latest leader epoch for the log.
func (l *commitLog) LastLeaderEpoch() uint64 {
	return l.leaderEpochCache.LastLeaderEpoch()
}

func (l *commitLog) activeSegment() *segment {
	return (*segment)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.vActiveSegment))))
}

// stopBackgroundLoops signals the checkpoint and cleaner loops to stop and blocks
// until both have returned. It is idempotent and safe for concurrent callers.
//
// It MUST NOT be called while holding l.mu: the loops acquire l.mu mid-iteration
// (checkpointHWLoop RLocks; cleanerLoop's Clean RLocks then Locks), so waiting for
// them to finish while holding l.mu would deadlock.
func (l *commitLog) stopBackgroundLoops() {
	l.closeOnce.Do(func() { close(l.closed) })
	l.bgWG.Wait()
}

// closeSegments checkpoints the high watermark and closes every segment. The
// caller must hold l.mu. Idempotent: a second call is a no-op.
func (l *commitLog) closeSegments() error {
	if l.segmentsClosed {
		return nil
	}
	if err := l.checkpointHW(); err != nil {
		return err
	}
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	l.segmentsClosed = true
	return nil
}

// Close stops the background goroutines (checkpoint + cleaner), then checkpoints
// the high watermark and closes each log segment file. It waits for the
// background loops to exit before touching segments so no goroutine operates on a
// closed segment.
func (l *commitLog) Close() error {
	// Stop and join the background loops without l.mu held (see the doc on
	// stopBackgroundLoops), then close segments under l.mu.
	l.stopBackgroundLoops()

	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closeSegments()
}

// Delete closes the log and removes all data associated with it from the
// filesystem.
func (l *commitLog) Delete() error {
	// Mark deleted before signaling close so a reader that unblocks on l.closed
	// reports ErrCommitLogDeleted rather than ErrCommitLogClosed, and so the
	// checkpoint loop skips writing to a directory about to be removed.
	l.mu.Lock()
	l.deleted = true
	l.mu.Unlock()

	l.stopBackgroundLoops()

	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.closeSegments(); err != nil {
		return err
	}
	return removeAllWithRetry(l.Path)
}

// removeAllWithRetry removes the directory tree, retrying briefly. On Windows a
// concurrent reader's in-flight segment mmap/handle can still block removal for
// a moment after the log's own segments are closed — the reader releases it as
// soon as its read observes the deletion (ReadMessage returns ErrCommitLogDeleted),
// so a short retry covers that window. On Unix the first attempt always succeeds
// (an open file is unlinkable), so there is no added cost there.
func removeAllWithRetry(path string) error {
	var err error
	for i := 0; i < 100; i++ {
		if err = os.RemoveAll(path); err == nil {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return err
}

// Bound for atomicWriteWithRetry. Long enough to cover a handle that is on its
// way out (those clear in milliseconds), short enough that a genuinely
// conflicted write still fails promptly rather than stalling a checkpoint tick.
const (
	atomicWriteRetries    = 25
	atomicWriteRetryDelay = 20 * time.Millisecond
)

// atomicWriteWithRetry writes a file atomically, retrying briefly. On Windows
// the underlying ReplaceFile can transiently fail with "Access is denied" when
// some other handle to the destination has not been released yet — a process
// that just exited, or a scanner that opened the file after the previous write.
// The condition clears in milliseconds, while a real conflict (a second live
// writer, a read-only file) never does, so the bound keeps that case failing
// instead of hiding it. On Unix rename is atomic and the first attempt always
// succeeds, so nothing is added there.
//
// The payload is buffered up front because a retry has to write the SAME bytes
// again: atomic_file.WriteFile consumes the reader, so retrying with the
// original one would replace the file with nothing.
func atomicWriteWithRetry(path string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return errors.Wrap(err, "failed to buffer atomic write payload")
	}
	for i := 0; ; i++ {
		err = atomic_file.WriteFile(path, bytes.NewReader(data))
		if err == nil || i >= atomicWriteRetries {
			return err
		}
		time.Sleep(atomicWriteRetryDelay)
	}
}

// IsDeleted returns true if the commit log has been deleted.
func (l *commitLog) IsDeleted() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.deleted
}

// IsClosed returns true if the commit log was closed.
func (l *commitLog) IsClosed() bool {
	select {
	case <-l.closed:
		return true
	default:
		return false
	}
}

// Truncate removes all messages from the log starting at the given offset.
// Truncate takes appendMu because, like a segment roll, it redefines where an
// append lands — and unlike the write itself, the part that matters here is NOT
// ordered by the segment's own lock.
//
// It is tempting to argue that it is: Truncate calls Delete or Replace on the
// very segment an in-flight appender is holding, and both take that segment's
// write lock, so the two writes are ordered. That argument is wrong, and the
// test that goes with this found it. Before replacing the boundary segment,
// Truncate SCANS it to copy the records below the cut — and that scan runs
// outside the segment lock. An append extending the segment mid-scan leaves the
// copy holding a torn frame, and the rebuilt log then cannot be read end to
// end. Reproduced in roughly one run in eight.
func (l *commitLog) Truncate(offset int64) error {
	// Republish the tier after the segment set changes: dropping segments
	// can remove offloaded ones, and a manifest naming an object that is
	// gone sends a reader at something that will not open.
	defer func() { _ = l.writeTierManifest() }()
	l.cleanMu.Lock()
	defer l.cleanMu.Unlock()
	l.appendMu.Lock()
	defer l.appendMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	seg, idx := findSegment(l.segments, offset)
	if seg == nil {
		// Nothing to truncate.
		return nil
	}

	// Delete all following segments.
	deleted := 0
	for i := idx + 1; i < len(l.segments); i++ {
		if err := l.segments[i].Delete(); err != nil {
			return err
		}
		deleted++
	}

	var replace bool

	// Delete the segment if its base offset is the target offset, provided
	// it's not the first segment.
	if seg.BaseOffset == offset {
		if idx == 0 {
			replace = true
		} else {
			if err := seg.Delete(); err != nil {
				return err
			}
			deleted++
		}
	} else {
		replace = true
	}

	// Retain all preceding segments.
	segments := make([]*segment, len(l.segments)-deleted)
	for i := 0; i < idx; i++ {
		segments[i] = l.segments[i]
	}

	// Replace segment containing offset with truncated segment.
	if replace {
		var (
			ss              = newSegmentScanner(seg)
			newSegment, err = seg.Truncated()
		)
		defer ss.Close()
		if err != nil {
			return err
		}
		for ms, e, err := ss.Scan(); err == nil; ms, e, err = ss.Scan() {
			if ms.Offset() < offset {
				if err := newSegment.WriteMessageSet(ms, []*entry{e}); err != nil {
					return err
				}
			} else {
				break
			}
		}
		if err = newSegment.Replace(seg); err != nil {
			return err
		}
		segments[idx] = newSegment
	}
	activeSegment := segments[len(segments)-1]
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&l.vActiveSegment)),
		unsafe.Pointer(activeSegment))
	l.segments = segments
	return l.leaderEpochCache.ClearLatest(offset)
}

// OffloadBefore offloads the log bytes of every sealed segment entirely below
// minOffset to the configured SegmentStore. See the interface doc.
func (l *commitLog) OffloadBefore(minOffset int64) (int, error) {
	if l.SegmentStore == nil || minOffset <= 0 {
		return 0, nil
	}
	if !l.tierWritable() {
		// Nothing offloaded, and not an error. A process that does not own the
		// tier is expected to call this on the same schedule as one that does
		// and simply do nothing; failing would make every caller special-case
		// its own role, and make a role change a source of spurious errors.
		return 0, nil
	}
	l.cleanMu.Lock()
	defer l.cleanMu.Unlock()
	// Snapshot the sealed segments (everything but the active one) under the
	// read lock; offloadTo mutates a segment internally but does not touch the
	// segment LIST, so it runs without the write lock and stays concurrent with
	// reads/appends on other segments.
	l.mu.RLock()
	var targets []*segment
	for i := 0; i < len(l.segments)-1; i++ {
		s := l.segments[i]
		if !s.isOffloaded() && s.LastOffset() >= 0 && s.LastOffset() < minOffset {
			targets = append(targets, s)
		}
	}
	l.mu.RUnlock()

	n := 0
	for _, s := range targets {
		// Gate on convergence: never offload a segment that still owes a block
		// consolidation rewrite. Offload is a pure byte copy, so a fragmented
		// segment offloaded early would freeze its bloated many-block layout (and
		// fat sparse index) into the store, where it can no longer be cheaply
		// re-consolidated and wastes cache budget forever. A later Clean converges
		// it and a subsequent OffloadBefore takes it then.
		if s.needsBlockConsolidation() {
			continue
		}
		logKey, idxKey := newStoreKeys(s.BaseOffset)
		if err := s.offloadTo(l.SegmentStore, logKey, idxKey,
			l.RemoteIndexCache); err != nil {
			return n, err
		}
		n++
	}
	if n > 0 {
		// After the objects, never before: the manifest is the tier's commit
		// point, so an object it does not name was never committed and is a
		// recognisable orphan rather than an ambiguity.
		if err := l.writeTierManifest(); err != nil {
			return n, err
		}
	}
	return n, nil
}

// TruncateBefore removes all messages from the log with offset < minOffset.
// Sealed segments entirely before minOffset are deleted. A boundary sealed
// segment (one that straddles minOffset) is rewritten keeping only records at
// or after minOffset. The active segment is never rewritten.
func (l *commitLog) TruncateBefore(minOffset int64) error {
	// Republish the tier after the segment set changes: dropping segments
	// can remove offloaded ones, and a manifest naming an object that is
	// gone sends a reader at something that will not open.
	defer func() { _ = l.writeTierManifest() }()
	l.cleanMu.Lock()
	defer l.cleanMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.segments) == 0 || minOffset <= 0 {
		return nil
	}

	// Find the first sealed segment whose last offset >= minOffset (the boundary).
	// All sealed segments before it are entirely obsolete.
	// If no sealed segment qualifies, boundaryIdx = len-1 (the active segment),
	// and we just delete all sealed segments.
	boundaryIdx := len(l.segments) - 1
	for i := 0; i < len(l.segments)-1; i++ {
		if l.segments[i].LastOffset() >= minOffset {
			boundaryIdx = i
			break
		}
	}

	// Delete all sealed segments before the boundary.
	for i := 0; i < boundaryIdx; i++ {
		if err := l.segments[i].Delete(); err != nil {
			return err
		}
	}

	// Rewrite the boundary segment if it's a sealed segment whose BaseOffset
	// falls before minOffset (meaning it straddles the cut point).
	if boundaryIdx < len(l.segments)-1 {
		boundary := l.segments[boundaryIdx]
		if boundary.BaseOffset < minOffset {
			ss := newSegmentScanner(boundary)
			defer ss.Close()
			var (
				newBaseOffset int64 = -1
				kept          []messageSet
			)
			for ms, _, err := ss.Scan(); err == nil; ms, _, err = ss.Scan() {
				if ms.Offset() >= minOffset {
					if newBaseOffset < 0 {
						newBaseOffset = ms.Offset()
					}
					kept = append(kept, ms)
				}
			}

			if newBaseOffset >= 0 {
				trimmed, err := boundary.Trimmed(newBaseOffset)
				if err != nil {
					return errors.Wrap(err, "create trimmed segment failed")
				}
				for _, ms := range kept {
					entries := entriesForMessageSet(trimmed.Position(), ms)
					if err := trimmed.WriteMessageSet(ms, entries); err != nil {
						trimmed.Delete()
						return errors.Wrap(err, "write trimmed segment failed")
					}
				}
				if err := trimmed.Finalize(); err != nil {
					trimmed.Delete()
					return errors.Wrap(err, "finalize trimmed segment failed")
				}
				// Seal so that uncommitted readers hitting EOF on this
				// segment immediately move to the next one instead of
				// waiting for more data that will never come.
				trimmed.Seal()
				if err := boundary.Delete(); err != nil {
					return err
				}
				l.segments[boundaryIdx] = trimmed
			} else {
				// Boundary segment had no records >= minOffset; delete it entirely.
				if err := boundary.Delete(); err != nil {
					return err
				}
				boundaryIdx++
			}
		}
	}

	l.segments = l.segments[boundaryIdx:]
	return l.leaderEpochCache.ClearEarliest(minOffset)
}

func (l *commitLog) Segments() []*segment {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments
}

// NotifyLEO registers and returns a channel which is closed when messages past
// the given log end offset are added to the log. If the given offset is no
// longer the log end offset, the channel is closed immediately. Waiter is an
// opaque value that uniquely identifies the entity waiting for data.
func (l *commitLog) NotifyLEO(waiter interface{}, expectedLEO int64) <-chan struct{} {
	return l.activeSegment().WaitForLEO(waiter, expectedLEO, l.NewestOffset())
}

// SetReadonly marks the log as readonly. When in readonly mode, new messages
// cannot be added to the log with Append and committed readers will read up to
// the log end offset (LEO), if the HW allows so, and then will receive an
// ErrCommitLogReadonly error. This will unblock committed readers waiting for
// data if they are at the LEO. Readers will continue to block if the HW is
// less than the LEO. This does not affect uncommitted readers. Messages can
// still be written to the log with AppendMessageSet for reconciliation
// purposes, e.g. when replicating from another log.
func (l *commitLog) SetReadonly(readonly bool) {
	value := int32(0)
	if readonly {
		value = 1
	}
	atomic.StoreInt32(&l.readonly, value)
	if readonly {
		l.mu.Lock()
		l.notifyReadonly()
		l.mu.Unlock()
	}
}

// IsReadonly indicates if the log is in readonly mode.
func (l *commitLog) IsReadonly() bool {
	return atomic.LoadInt32(&l.readonly) == 1
}

// IsConcurrencyControlEnabled indicates if the log should check for concurrency before appending messages
func (l *commitLog) IsConcurrencyControlEnabled() bool {
	return l.Options.ConcurrencyControl
}

// checkAndPerformSplit determines if a new log segment should be rolled out
// either because the active segment is full or MaxSegmentAge has passed since
// the first message was written to it. It then performs the split if eligible,
// returning any error resulting from the split. The returned bool indicates if
// a split was performed.
// checkAndPerformSplitLocked is checkAndPerformSplit for callers that do NOT
// already hold appendMu — currently the cleaner loop, which rolls on its own
// ticker with no append involved.
//
// A roll redefines where appends go, so it must not land between an append
// reading the tail and writing to it: split picks the new segment's base offset
// from the log's current end, which is exactly the offset the in-flight append
// is about to write at, so both segments end up claiming it.
func (l *commitLog) checkAndPerformSplitLocked() (bool, error) {
	l.appendMu.Lock()
	defer l.appendMu.Unlock()
	return l.checkAndPerformSplit()
}

// checkAndPerformSplit requires appendMu, since rolling a segment changes which
// segment the caller's append lands in.
func (l *commitLog) checkAndPerformSplit() (bool, error) {
	// Do this in a loop because segment splitting may fail due to a competing
	// thread performing the split at the same time. If this happens, we just
	// retry the check on the new active segment.
	for {
		activeSegment := l.activeSegment()
		if !activeSegment.CheckSplit(l.MaxSegmentAge) {
			return false, nil
		}
		if err := l.split(activeSegment); err != nil {
			// ErrSegmentExists indicates another thread has already performed
			// the segment split, so reload the new active segment and check
			// again.
			if err == ErrSegmentExists {
				continue
			}
			return false, err
		}
		activeSegment.Seal()
		return true, nil
	}
}

func (l *commitLog) split(oldActiveSegment *segment) error {
	offset := l.NewestOffset() + 1
	segment, err := newSegment(l.Path, offset, l.MaxSegmentBytes, true, "", l.Compression)
	if err != nil {
		return err
	}
	// Do a CAS on the active segment to ensure no other threads have replaced
	// it already. If this fails, it means another thread has already replaced
	// it, so delete the new segment and return ErrSegmentExists.
	if !atomic.CompareAndSwapPointer(
		(*unsafe.Pointer)(unsafe.Pointer(&l.vActiveSegment)),
		unsafe.Pointer(oldActiveSegment), unsafe.Pointer(segment)) {
		segment.Delete() // nolint: errcheck
		return ErrSegmentExists
	}
	l.mu.Lock()
	segments := append(l.segments, segment)
	l.segments = segments
	l.mu.Unlock()
	return nil
}

func (l *commitLog) cleanerLoop() {
	ticker := time.NewTicker(l.CleanerInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-l.closed:
			return
		}

		// Check to see if the active segment should be split.
		split, err := l.checkAndPerformSplitLocked()
		if err != nil {
			slog.Error(
				"Failed to split log",
				slog.String("path", l.Path),
				slog.String("error", err.Error()),
			)
			continue
		}

		// If we rolled a new segment, we don't need to run the cleaner since
		// it already ran.
		if split {
			continue
		}
		if l.DisableAutoClean {
			continue
		}

		if err := l.Clean(); err != nil {
			slog.Error(
				"Failed to clean log",
				slog.String("path", l.Path),
				slog.String("error", err.Error()),
			)
		}
	}
}

// CleanSpec parameterizes a transaction-aware clean. The commitlog provides
// the mechanism; a transactional layer (e.g. durable_streams) supplies the
// policy: which records' transactions aborted, where the decided prefix
// ends, and which per-message headers make a record transactional.
type CleanSpec struct {
	// Ceiling is the compaction bound: records at or above it are always
	// retained verbatim and never counted latest-per-key (they may be
	// undecided). <=0 falls back to the high watermark. Transactional
	// callers pass their LSO so open transactions can never shadow or be
	// compacted.
	Ceiling int64
	// StripBelow: records strictly below it are DECIDED, and nothing above
	// the log needs their per-record bookkeeping any more. Compaction removes
	// control records (AttrControl) below it, removes aborted data records,
	// and rewrites the survivors without StripHeaders. Offsets, timestamps,
	// leader epochs, keys, values and attribute bits survive the rewrite.
	StripBelow int64
	// StripHeaders are the per-message header keys removed below StripBelow.
	// Empty disables stripping, and with it marker removal — the two are only
	// safe together.
	//
	// The log attaches no meaning to these keys: they are whatever the caller
	// wrote and has since finished with, removed as bytes. A transactional
	// caller typically names the headers carrying its producer, epoch and
	// sequence, so that a decided record costs its readers nothing to skip —
	// but that is an example of the mechanism, not its definition, and a
	// caller with entirely different per-record bookkeeping uses it the same
	// way.
	StripHeaders []string
	// Aborted reports whether the data record at offset belongs to an
	// aborted transaction. Consulted only below Ceiling; must be safe for
	// concurrent use. Aborted records are removed and never counted
	// latest-per-key (an aborted record must not shadow a committed value).
	Aborted func(offset int64) bool
	// TombstoneGCBelow: a latest-per-key record carrying AttrTombstone at an
	// offset strictly below this, whose timestamp is older than
	// TombstoneRetention, is removed entirely — the key vanishes.
	TombstoneGCBelow int64
	// TombstoneRetention guards tombstone GC; zero disables it. Records with
	// timestamp 0 (pre-stamping logs) are never considered old enough.
	TombstoneRetention time.Duration
	// maxRewrites is an unexported deterministic rewrite cap for tests;
	// production callers bound passes by RewriteBudget.
	maxRewrites int
	// RewriteBudget bounds how long one pass may spend REWRITING segments
	// (digest skips stay free): once exceeded, remaining debt defers to the
	// next pass, so a pass always finishes inside a short-lived process's
	// kill window while reclamation scales to any inflow. The budget is
	// spent in drop-density order. 0 = unbounded. At least one rewrite
	// always proceeds.
	RewriteBudget time.Duration
	// TierRewriteBudget bounds, separately, how long one pass may spend
	// rewriting segments whose bytes live in a SegmentStore. Zero falls back to
	// RewriteBudget, so a caller that sets nothing sees no change.
	//
	// It is separate because the two rewrites cost wildly different things. A
	// local rewrite reads and writes local disk; a tiered one downloads the
	// object, rewrites, and uploads the result — orders of magnitude slower
	// against remote storage, and metered. Sharing one wall-clock budget lets a
	// single slow tiered rewrite consume the whole pass and starve local
	// compaction indefinitely, which is the case this exists to prevent: local
	// debt would grow while the pass spends its seconds on one remote object.
	//
	// Both budgets still guarantee at least one rewrite, so debt in either tier
	// always drains rather than deadlocking under a small budget.
	TierRewriteBudget time.Duration
	// skipTiered leaves segments whose bytes live in a SegmentStore entirely
	// alone: no rewrite, and no tier retention. Local segments compact and
	// retain as usual.
	//
	// Unexported deliberately. Whether this log may write to its store is a
	// property of the LOG, not of one pass — see Options.TierReadOnly and
	// SetTierReadOnly — and offering both invited a caller to set them to
	// disagree. CleanWithSpec derives this from the log's current mode.
	skipTiered bool
	// TierWriter is the identity stamped into any store objects this pass
	// writes. Set from the log's current value by CleanWithSpec; a caller
	// setting it directly overrides that for the pass.
	TierWriter string
}

// Clean applies retention and compaction rules against the log, if applicable.
func (l *commitLog) Clean() error {
	spec := CleanSpec{}
	if l.Options.CompactTombstoneRetention > 0 {
		// Spec-less tombstone GC for non-transactional compacted logs,
		// bounded like the rest of the spec-less compaction.
		spec.TombstoneGCBelow = l.HighWatermark()
		spec.TombstoneRetention = l.Options.CompactTombstoneRetention
	}
	_, err := l.CleanWithSpec(spec)
	return err
}

// CleanWithSpec applies retention and a transaction-aware compaction pass.
// See the interface doc for the returned verified floor.
func (l *commitLog) CleanWithSpec(spec CleanSpec) (int64, error) {
	l.cleanMu.Lock()
	defer l.cleanMu.Unlock()
	l.mu.RLock()
	oldSegments := l.segments
	l.mu.RUnlock()
	if !l.tierWritable() {
		// A read-only tier is left entirely alone, whatever the spec asked for:
		// a rewrite and a tier retention delete are both writes to a store this
		// log does not own. Local compaction proceeds exactly as usual.
		spec.skipTiered = true
	}
	// Reclaim what EARLIER passes superseded, before this pass adds to the queue.
	// Deliberately first: the objects queued below are the ones this pass is
	// still installing, and the manifest that stops naming them is not written
	// until the end of it.
	l.drainReclaim()

	// Collected per pass, under cleanMu, so the entries queued belong to this
	// call.
	l.compactCleaner.superseded = nil
	cleaned, epochCache, verified, cleanErr := l.clean(spec, oldSegments)
	superseded := l.compactCleaner.superseded
	l.compactCleaner.superseded = nil
	if cleaned == nil {
		// Nothing was installed, but a rewrite may still have landed new objects
		// before the pass failed, and the segments it swapped are on them. The
		// superseded ones are this log's garbage either way.
		l.queueReclaim(superseded)
		return -1, cleanErr
	}
	l.mu.Lock()
	newSegments := l.segments
	if len(newSegments) > len(oldSegments) {
		// New segments were added while cleaning. Rebase the new segments onto
		// the cleaned ones.
		rebase := newSegments[len(oldSegments):]
		cleaned = l.rebaseSegments(rebase, cleaned, epochCache)
	}
	l.segments = cleaned
	// Update the leader epoch offset cache to account for deleted segments. If
	// compaction ran, we need to regenerate the cache using the one returned
	// from compaction.
	var err error
	if epochCache != nil {
		err = l.leaderEpochCache.Replace(epochCache)
	} else {
		err = l.leaderEpochCache.ClearEarliest(l.segments[0].BaseOffset)
	}
	l.mu.Unlock()

	// Republish what the tier now holds. A pass can rewrite a segment onto new
	// objects and retention can drop others, so the manifest is stale the
	// moment either happens — and a stale manifest is worse than none, since it
	// names objects a reader would then fail to open.
	// Not when the pass skipped the tier: a read-only tier takes ZERO store
	// writes, and a manifest Put is a store write like any other. Nothing in
	// the tier changed, so the manifest is still accurate anyway.
	if !spec.skipTiered {
		manifestErr := l.writeTierManifest()
		// A manifest that did not land may still name what this pass superseded,
		// so reclamation holds off until one does. Recorded even when another
		// error is already being reported: whether the objects are safe to delete
		// does not depend on which error the caller sees.
		l.tierMu.Lock()
		l.tierManifestStale = manifestErr != nil
		l.tierMu.Unlock()
		if manifestErr != nil && err == nil && cleanErr == nil {
			err = manifestErr
		}
	}

	// Queued AFTER the manifest, so an entry is only ever considered for
	// deletion once a published manifest has stopped naming it.
	l.queueReclaim(superseded)

	// A partial retention failure (cleanErr) still swapped in the surviving
	// segments above; report it once the read path is consistent.
	if cleanErr != nil {
		return -1, cleanErr
	}
	return verified, err
}

// rebaseSegments adds the segments in from to the end of the slice of segments
// in to and adds any leader epoch offsets to the given leaderEpochCache.
func (l *commitLog) rebaseSegments(from, to []*segment, epochCache *leaderEpochCache) []*segment {
	to = append(to, from...)
	// Rebase any leader epoch offsets also. We don't check the error returned
	// here because Rebase can't return an error since epochCache is not
	// file-backed. The epoch cache is nil if compaction didn't run, in which
	// case skip this.
	if epochCache != nil {
		epochCache.Rebase(l.leaderEpochCache, from[0].BaseOffset) // nolint: errcheck
	}
	return to
}

// clean returns the cleaned segments, the pass's verified floor (see
// CleanWithSpec; -1 when compaction did not run) and, if compaction ran, a
// *leaderEpochCache maintaining the start offset for each new leader epoch.
func (l *commitLog) clean(spec CleanSpec, segments []*segment) ([]*segment, *leaderEpochCache, int64, error) {
	// Offloaded segments used to be held aside here as an immutable prefix,
	// because the rewriters build a local working segment and rewrite in place
	// and an offloaded segment has no local file to rewrite. That exclusion is
	// why whatever garbage a segment held when it offloaded was frozen there
	// permanently — a tombstone that offloaded before it could be collected
	// never took effect, and every value it shadowed was kept with it.
	//
	// They are no longer excluded. A rewrite of an offloaded segment becomes the
	// fresh store objects instead (see ReplaceOffloaded), and
	// retention is per tier, so their bytes count toward the tier's budget
	// rather than escaping every limit.
	cleaned, err := l.deleteCleaner.Clean(segments, spec.skipTiered)
	if err != nil {
		// A partial retention failure still hands back the surviving
		// segments; propagate them so the caller swaps them in — the deleted
		// prefix must leave the read path even when the clean errs.
		return cleaned, nil, -1, err
	}
	verified := int64(-1)
	var epochCache *leaderEpochCache
	if l.Compact {
		if spec.Ceiling <= 0 {
			spec.Ceiling = l.HighWatermark()
		}
		compacted, cache, v, err := l.compactCleaner.CompactSpec(spec, cleaned)
		if err != nil {
			// Keep the delete stage's result: its removals are already on
			// disk regardless of the compaction failure.
			return cleaned, nil, -1, err
		}
		cleaned, epochCache, verified = compacted, cache, v
	} else if consolidated, err := consolidateSegments(cleaned, spec.maxRewrites, spec.RewriteBudget); err != nil {
		// Non-compacted logs still owe block-layout maintenance: their
		// per-append tiny blocks otherwise accumulate blockRef memory and
		// open-time header walks forever (an uncompacted, append-heavy log
		// was observed gathering 16k-block segments across a long-running
		// soak). The consolidation-only pass rewrites records
		// VERBATIM — content, offsets and epochs untouched — into
		// cleanBlockTarget-sized blocks, budgeted like compaction rewrites.
		return cleaned, nil, -1, err
	} else {
		cleaned = consolidated
	}
	return cleaned, epochCache, verified, nil
}

func (l *commitLog) checkpointHWLoop() {
	ticker := time.NewTicker(l.HWCheckpointInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-l.closed:
			return
		}
		l.mu.RLock()
		if l.deleted {
			l.mu.RUnlock()
			return
		}
		if err := l.checkpointHW(); err != nil {
			// Transient on Windows: the atomic rename can hit a sharing
			// violation while a scanner holds the file. The checkpoint is
			// only an optimization (RecoverTail rides out staleness), so a
			// failed tick is retried on the next one — never fatal.
			slog.Warn("high-watermark checkpoint failed; retrying next tick",
				slog.String("path", l.Path), slog.String("err", err.Error()))
		}
		l.mu.RUnlock()
	}
}

// Sync makes the log durable through offset. See the interface doc for the
// contract; this is the group-commit barrier behind it.
//
// A caller already covered by a completed flush returns without an fsync. One
// whose offset a flush in flight will cover waits for it instead of issuing a
// second. Otherwise the caller leads: it snapshots the tail, flushes, and
// publishes what that flush covered — which is generally far more than its own
// offset, so the callers waiting behind it are covered too.
func (l *commitLog) Sync(offset int64) error {
	waited := false
	for {
		l.syncMu.Lock()
		if offset <= l.syncDurable {
			if waited {
				l.syncFollowers++
			}
			l.syncMu.Unlock()
			return nil
		}
		if l.syncFlushing {
			// Someone else is already flushing. Wait for it rather than queueing
			// a redundant fsync behind it, then re-check: their flush usually
			// covers this offset too, since they snapshot the tail AFTER this
			// append landed.
			wait := l.syncDone
			l.syncJoined++
			l.syncMu.Unlock()
			waited = true
			<-wait
			continue
		}
		l.syncLeaders++
		l.syncFlushing = true
		done := make(chan struct{})
		l.syncDone = done
		window := l.syncWindow
		l.syncJoined = 0
		l.syncMu.Unlock()

		// Hold the door open before flushing. Without this the barrier coalesces
		// only by accident — it flushes the instant it takes leadership, so a
		// caller that arrives a microsecond later is not covered and has to lead
		// a flush of its own. Measured that way, 98% of concurrent committers
		// ended up leading, which is no batching at all.
		//
		// The window is the PREVIOUS flush's duration, which self-tunes: on a
		// fast disk it is short, so the latency added is proportional to what an
		// fsync already costs; on a slow one it grows and the batches grow with
		// it, which is exactly where batching pays. Capped so a pathological
		// outlier cannot park later commits behind it.
		if window > 0 {
			timer := time.NewTimer(window)
			select {
			case <-timer.C:
			case <-l.closed:
			}
			timer.Stop()
		}

		// Snapshot the tail BEFORE flushing: every record up to here has already
		// been written to the OS (the append path advances the tail only after
		// its write returns), so the flush makes exactly this much durable.
		// Records appended during the flush are not claimed — they ride the next
		// one, which is the group-commit contract.
		target := l.NewestOffset()
		started := time.Now()
		err := l.syncSegmentData()
		elapsed := time.Since(started)

		l.syncMu.Lock()
		if err == nil && target > l.syncDurable {
			l.syncDurable = target
		}
		// The window tracks the last flush's duration unconditionally. Two
		// cleverer variants were measured and both lost:
		//
		//   - zeroing the window when nobody joined is self-reinforcing (with no
		//     window nobody can arrive in time to join, so it never re-arms):
		//     64 concurrent committers went from 0.019 fsyncs/commit to 0.42;
		//   - decaying it by half instead was unstable at high concurrency,
		//     landing at 0.167 — worse than at 16 writers.
		//
		// Tracking the flush duration is stable and self-tuning. The cost is that
		// a strictly serial committer waits a window it will never share, roughly
		// doubling its per-commit latency; concurrent committers get an order of
		// magnitude fewer fsyncs. That trade is documented on the interface so a
		// serial caller can choose SyncAll instead.
		if elapsed > maxSyncWindow {
			elapsed = maxSyncWindow
		}
		l.syncWindow = elapsed
		l.syncFlushing = false
		close(done)
		l.syncMu.Unlock()

		if err != nil {
			return err
		}
		if target < offset {
			// The flush succeeded and still did not reach this offset, which
			// means the log does not GO that far: retention moved the tail below
			// it after the caller appended there. Looping cannot fix that — the
			// tail only advances on appends this call does not make — so waiting
			// to be covered would spin fsyncs forever. The records are gone, so
			// there is nothing left to make durable and the request is satisfied
			// by what remains.
			return nil
		}
		// Otherwise round again rather than assuming success covered this
		// caller: a flush that started before this append landed can complete
		// without reaching its offset, and that caller must lead the next one.
	}
}

// SyncAll makes everything appended so far durable against power loss: it
// fsyncs EVERY segment's log and index written since its last sync (the
// periodic HW checkpoint only syncs the active segment, so sealed segments
// written since the last checkpoint may still be in OS buffers), then
// checkpoints the high watermark. After SyncAll returns, a reopened log
// recovers every record appended before the call. Used before
// externally-visible filesystem operations on the log's directory (e.g. an
// atomic stream promote via rename) whose observers must never see the log roll
// back past this point.
func (l *commitLog) SyncAll() error {
	if err := l.syncSegments(); err != nil {
		return err
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.checkpointHW()
}

// syncSegmentData fsyncs the LOG BYTES of every segment written since its last
// flush, leaving indexes alone — the durability hot path. An index behind its
// log is a state recovery already repairs, and seal flushes the index of any
// segment that rolls, so the unrepaired case never outlives the active segment.
func (l *commitLog) syncSegmentData() error {
	return l.forEachSegment((*segment).SyncData)
}

// syncSegments fsyncs every dirty segment, log and index both.
func (l *commitLog) syncSegments() error {
	return l.forEachSegment((*segment).Sync)
}

// forEachSegment fsyncs every segment with sync. It snapshots the segment slice
// rather than holding l.mu across the fsyncs: an append that rolls a new
// segment needs the write lock, so holding the read lock for the duration would
// stall the roll behind the very fsync a concurrent commit is waiting on. A
// segment appended after the snapshot is simply not covered by this call, which
// is the same boundary the per-segment sync already draws.
func (l *commitLog) forEachSegment(sync func(*segment) error) error {
	l.mu.RLock()
	segments := make([]*segment, len(l.segments))
	copy(segments, l.segments)
	l.mu.RUnlock()
	for _, seg := range segments {
		if err := sync(seg); err != nil {
			// A segment closed concurrently: Clean rewrites/closes segments
			// OUTSIDE l.mu (see the struct comment), so a sync racing a Clean
			// can grab a segment Clean just closed. Such a segment is already
			// durable (or being made durable by the rewrite that closed it), so
			// skip it and keep syncing the REST — crucially the active segment —
			// instead of aborting the whole sync. Aborting otherwise surfaced as a
			// spurious "sync ...: file already closed" under concurrent load (many
			// producers sharing one coordinator's txLog while maintenance runs).
			if errors.Is(err, os.ErrClosed) {
				continue
			}
			return errors.Wrap(err, "failed to sync segment")
		}
	}
	return nil
}

func (l *commitLog) checkpointHW() error {
	var (
		hw   = l.hw
		r    = strings.NewReader(strconv.FormatInt(hw, 10))
		file = filepath.Join(l.Path, hwFileName)
	)

	// fsync the log file before writing the HW to disk
	err := l.activeSegment().backing.Sync()
	if err != nil {
		return errors.Wrap(err, "failed to sync log file")
	}

	return atomicWriteWithRetry(file, r)
}

// Sidecars are small named metadata files owned by the log's CLIENT, stored
// in the log directory next to the segments (e.g. durable_streams' recovery
// floor checkpoint). Put writes atomically (temp + rename), so a crash never
// leaves a torn sidecar; Get returns os.ErrNotExist-satisfying errors when
// absent; Remove of an absent sidecar is a no-op. Names must not collide
// with the log's own files (segments, indexes, checkpoints).
func (l *commitLog) PutSidecar(name string, data []byte) error {
	return atomicWriteWithRetry(filepath.Join(l.Path, name), bytes.NewReader(data))
}

func (l *commitLog) GetSidecar(name string) ([]byte, error) {
	return os.ReadFile(filepath.Join(l.Path, name))
}

func (l *commitLog) RemoveSidecar(name string) error {
	err := os.Remove(filepath.Join(l.Path, name))
	if err != nil && os.IsNotExist(err) {
		return nil
	}
	return err
}

// SegmentBlockCounts reports each segment's in-memory block-index size
// (oldest first; raw segments report 0). Observability/test hook for the
// block-consolidation machinery.
func (l *commitLog) SegmentBlockCounts() []int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]int, 0, len(l.segments))
	for _, s := range l.segments {
		s.RLock()
		out = append(out, len(s.blocks))
		s.RUnlock()
	}
	return out
}
