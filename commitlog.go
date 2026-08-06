// Package commitlog provides an implementation for a file-backed write-ahead log.
package commitlog

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ligustah/commitlog/compress"
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

// ErrCorruptRecord reports a record whose stored CRC does not match its bytes.
// The record is NOT returned with it: a caller gets the error instead of the
// data, never both.
//
// This used to be a panic, on the reasoning that corruption on disk leaves "the
// server in an unrecoverable state". That was true of the server this package
// was extracted from and is wrong for a library embedded in someone else's
// process: one bad record took down a host that had a perfectly good answer
// available — skip the record, fail the read, resync the stream — and a read is
// exactly where a caller is positioned to choose between them.
//
// A sentinel rather than an opaque error because the choice depends on telling
// corruption apart from an ordinary read failure. The trade is real and worth
// stating: an error CAN be ignored where a panic cannot, so a caller that checks
// nothing now proceeds past a record it should not trust. Every caller of
// ReadMessage already handles an error return, and none can handle a panic.
var ErrCorruptRecord = errors.New("commitlog: record failed its CRC check")

// ErrSegmentUnreadable reports that a scan could not reach the end of a
// segment, so anything derived from that scan describes a prefix rather than the
// segment. Truncate, TruncateBefore and Clean wrap it, and leave the segment
// exactly as they found it when they return it.
//
// It is worth telling apart from any other failure of those calls: it says the
// bytes on this replica are damaged, which is a thing a caller with a peer to
// copy from can act on, and a thing retrying the same call cannot fix.
//
// The alternative, and what every one of these loops used to do, is to treat it
// as the end of the segment. That is how damage becomes LOSS rather than an
// error: each loop writes what it collected into a fresh segment and deletes the
// original, so whatever the scan could not reach is not in the copy and the
// source is gone a moment later, with the call reporting success.
//
// A compaction pass reports it rather than skipping the segment and carrying on.
// The pass runs unattended on a timer, so the argument for skipping is real —
// but it would take the disposition machinery a nil digest cannot go through,
// and a warning logged every tick forever is a quieter answer than this
// deserves. Retention is unaffected either way: the delete stage runs first and
// its removals are kept when compaction fails, so a damaged segment costs
// deferred compaction, not a filling disk.
var ErrSegmentUnreadable = errors.New("commitlog: segment could not be read to its end")

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
	MaxTierBytes    int64
	MaxTierMessages int64
	MaxTierAge      time.Duration
	Compact         bool // Run compaction on log clean
	// CompactMaxGoroutines bounds concurrent key-digest builds during a
	// compaction pass. Zero picks the default; negative is refused by New.
	//
	// It no longer means what its name suggests, and the gap is worth stating
	// rather than leaving for the next reader to discover in loadOrBuildDigests:
	// digest builds are capped at 2 regardless, because each holds a transient
	// per-segment key map and ten at once measured over 1GB on a soak. So the
	// only value of this field that changes anything is 1, which makes the
	// builds serial. Anything above 2 is the same as 2.
	CompactMaxGoroutines int
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
	// checking against the one the log already has. It means one thing — "I know
	// what this log is, record it" — and that one thing answers both cases New
	// otherwise refuses: retuning an existing log's compaction settings, and
	// opening a log that exists with no descriptor. Neither can be settled from
	// what is stored, which is why both need a human to say so.
	//
	// Requiring an explicit opt-in is the point: an accidentally empty config
	// must not be able to redefine what a log keeps. Ignored for a log being
	// created, which simply records what it was created with.
	//
	// It is NOT how a second process picks up a log someone else created. That
	// process passes the settings it believes the log has and is checked against
	// what the log says; AdoptOptions would skip exactly that check.
	AdoptOptions bool
	// DisableAutoClean stops the internal cleaner loop from running Clean.
	// Segment splitting (MaxSegmentAge rolls) keeps running. For logs whose
	// owner drives cleaning explicitly (CleanWithSpec) — an automatic clean
	// has no transaction awareness and must not race the owner's policy.
	DisableAutoClean bool
	// CleanRewriteBudget bounds how long ONE automatic pass may spend rewriting
	// segments. It is what the cleaner loop puts into CleanSpec.RewriteBudget,
	// which callers driving CleanWithSpec themselves have always been able to
	// set and the spec-less path could not reach at all.
	//
	// Defaults to CleanerInterval, which is the number that makes the loop
	// self-pacing rather than a guess: a pass that would spend longer than the
	// gap between ticks rewriting is a pass whose every tick arrives into the
	// one before it. durable_streams measured 6m42s per pass against a 5m
	// interval on a 4.5GB log.
	//
	// Stopping early is not lost work. Segments the budget did not reach are
	// carried through untouched and the pass installs what it did do, so the
	// log converges over several ticks instead of holding cleanMu for as long
	// as one pass happens to take. At least one rewrite always proceeds, so
	// even a pathologically small budget drains debt.
	//
	// Set it negative to mean "no budget", which is the behaviour every
	// spec-less pass had before this existed.
	CleanRewriteBudget   time.Duration
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
	// Refused HERE because every other place that meets an unknown codec meets it
	// too late. Compress has no error to return and falls through to storing the
	// bytes raw, so an unknown codec writes blocks headed with a byte no reader
	// accepts — parseBlockHeader calls Valid(), the only other call site, and that
	// runs on the way back. Worse, the descriptor records the codec by name, and
	// an unknown one renders as "codec(9)", which compress.Parse then refuses: the
	// log accepts the option, accepts the appends, and cannot open again.
	//
	// So the write path accepted exactly what the read path refuses, and said so
	// only after there were records to lose. A codec is a value the caller hands
	// in; checking it where it arrives is the whole fix.
	if !opts.Compression.Valid() {
		return nil, errors.Errorf("commitlog: unknown compression codec %d", byte(opts.Compression))
	}
	// Same reasoning, four fields along, and one defect rather than four.
	//
	// Each of these is defaulted by a test for ZERO, because zero is the unset
	// value. A test for zero reads as "the caller supplied a number" for every
	// value that is not exactly the zero value — so a negative passes the arm
	// that exists to catch a missing one and arrives somewhere that cannot cope:
	//
	//   CompactMaxGoroutines  make(chan struct{}, n)  panic: makechan: size out
	//                                                 of range
	//   HWCheckpointInterval  time.NewTicker(d)       panic: non-positive
	//   CleanerInterval       time.NewTicker(d)       interval for NewTicker
	//   MaxSegmentBytes       the split check         no panic — every append
	//                                                 rolls, forever. Measured:
	//                                                 the probe never returned.
	//
	// Not one of those failures happens at the call that set the option. Two are
	// panics on background tickers, with no caller left to hand an error to; the
	// third is a hang. Refused here, and refused rather than clamped, for the
	// reason the codec above is: clamping keeps the caller's mistake and hides
	// it, and a log built on a value nobody meant is not a log anyone can debug.
	for _, c := range []struct {
		name string
		bad  bool
		got  any
	}{
		{"CompactMaxGoroutines", opts.CompactMaxGoroutines < 0, opts.CompactMaxGoroutines},
		{"MaxSegmentBytes", opts.MaxSegmentBytes < 0, opts.MaxSegmentBytes},
		{"HWCheckpointInterval", opts.HWCheckpointInterval < 0, opts.HWCheckpointInterval},
		{"CleanerInterval", opts.CleanerInterval < 0, opts.CleanerInterval},
		// CleanRewriteBudget is NOT here, and the omission is deliberate: a
		// negative budget means "no budget at all", which is what every
		// spec-less pass had before one existed. It is the one field in this
		// group where a negative is a value the caller can mean, and
		// TestTheAutomaticCleanIsBounded asserts it survives the
		// zero-means-default rule. Adding it here turns that assertion red.
	} {
		if c.bad {
			return nil, errors.Errorf("commitlog: %s is %v; it must not be negative",
				c.name, c.got)
		}
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
	if opts.CleanRewriteBudget == 0 {
		opts.CleanRewriteBudget = opts.CleanerInterval
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
	// Set here rather than passed to newCompactCleaner because it closes over the
	// log the cleaner belongs to, which does not exist until this line.
	compactCleaner.commitTier = func(baseOffset int64, meta offloadMeta) error {
		return l.writeTierManifest(meta.tierObject(baseOffset))
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
	isNew, err := logIsNew(descOpts)
	if err != nil {
		return nil, err
	}
	if err := reconcileDescriptor(descOpts, isNew); err != nil {
		return nil, err
	}

	// Everything from here on can fail with segments already open, and the log
	// they belong to is about to be dropped on the floor — so it has to give
	// their handles and index mmaps back itself. open() appends segments one at
	// a time and returns on the first that will not open, so a directory whose
	// thirtieth segment is damaged has twenty-nine of them held; a caller that
	// retries (a supervisor, a broker reopening a partition, a test) holds that
	// many more each attempt and never gets them back short of exiting.
	//
	// It shows differently on the two platforms and neither is benign: on Windows
	// a mapped index makes the directory undeletable, and on Linux the leak is
	// silent until the process runs out of descriptors.
	if err := l.openOrRelease(); err != nil {
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

// openOrRelease opens the log and reconciles the leader epoch checkpoint
// against it, releasing every segment it managed to open if any of that fails.
//
// The release is best-effort and its errors are dropped: the caller is already
// failing, and the reason it is failing is more useful than a second error from
// closing a segment that was only opened to be thrown away. Errors here are also
// nearly always the same handle trouble the release exists to avoid.
func (l *commitLog) openOrRelease() (err error) {
	defer func() {
		if err == nil {
			return
		}
		for _, segment := range l.segments {
			_ = segment.Close()
		}
		l.segments = nil
		l.segmentsClosed = true
	}()
	if err := l.open(); err != nil {
		return err
	}
	// After an unclean shutdown, the leader epoch checkpoint file could be
	// ahead of the log (as the log is flushed asynchronously by default). To
	// account for this, remove all entries from the leader epoch checkpoint
	// file where the offset is greater than the log end offset.
	if err := l.leaderEpochCache.ClearLatest(l.activeSegment().NextOffset()); err != nil {
		return err
	}
	// The earliest leader epoch may not be flushed during a hard failure.
	// Recover it here.
	return l.leaderEpochCache.ClearEarliest(l.OldestOffset())
}

func (l *commitLog) open() error {
	// The tier is read BEFORE the directory, because it is what makes sense of
	// the directory. An offloaded segment leaves no local .log — and, under
	// option 1, still has a local .index — so an index with no log beside it is
	// either an offloaded segment's index or a genuine orphan, and only the
	// manifest can tell those apart.
	var tier []TierObject
	if l.SegmentStore != nil {
		var err error
		tier, err = readTierManifest(l.SegmentStore)
		if err != nil {
			return err
		}
	}
	offloaded := make(map[int64]bool, len(tier))
	for _, o := range tier {
		offloaded[o.BaseOffset] = true
	}

	files, err := os.ReadDir(l.Path)
	if err != nil {
		return errors.Wrap(err, "read dir failed")
	}
	// Which base offsets have local log bytes. The listing above already answers
	// that for every file in the directory, so the orphan check below reads it
	// instead of stat-ing the disk once per index file — the question is "is
	// there a .log for this stem", and the .log entries are right here. On the
	// 336-segment logs durable_streams reports, that was 336 syscalls asking
	// what one directory read had already returned.
	//
	// Same snapshot for both, which is also the more defensible answer: an index
	// and its log are judged against ONE view of the directory rather than
	// against a listing and a later stat that can disagree.
	hasLog := make(map[string]bool, len(files))
	for _, file := range files {
		if name := file.Name(); strings.HasSuffix(name, logFileSuffix) {
			hasLog[strings.TrimSuffix(name, logFileSuffix)] = true
		}
	}
	for _, file := range files {
		// If this file is an index file, make sure it has a corresponding .log
		// file OR a manifest entry. Only a truly orphaned index is removed.
		if strings.HasSuffix(file.Name(), indexFileSuffix) {
			stem := strings.TrimSuffix(file.Name(), indexFileSuffix)
			base, convErr := strconv.Atoi(stem)
			if !hasLog[stem] && (convErr != nil || !offloaded[int64(base)]) {
				if err := os.Remove(filepath.Join(l.Path, file.Name())); err != nil {
					return err
				}
			}
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
			// Recover high watermark. WithRetry because this runs immediately
			// after the previous process died, which on Windows is precisely when
			// its handle to this file may still be open; see ReadFileWithRetry.
			b, err := ReadFileWithRetry(filepath.Join(l.Path, file.Name()))
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
	// Every offloaded segment comes from here — the manifest read at the top is
	// the only record that a segment lives in the store. Local .log files were
	// read above and win where both describe one base offset, which is the state
	// a crash between the commit and the local delete leaves: the bytes are the
	// same either way, and the store object is collected on the next publish.
	//
	// It is what makes the tier self-describing rather than an appendage of
	// this directory: a process holding the store and an empty or partial log
	// directory opens the log and reaches the offloaded records, without being
	// handed bookkeeping by anyone.
	if _, err := l.adoptTierManifestLocked(tier); err != nil {
		return err
	}
	// After every source of segments has been read, and before anything reads
	// the list: two of them can describe the same records.
	if err := l.resolveSegmentOverlaps(); err != nil {
		return err
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
	// The HW checkpoint can claim MORE than the log can serve. It is fsynced on
	// its own schedule, and a crash can take back log bytes it had already
	// counted — a torn tail dropped at open is exactly that. Nothing downstream
	// treats "committed but absent" as a state to recover from: a reader
	// resolving the HW's segment finds none and the log answers
	// ErrSegmentNotFound for an offset far below it, so a stale checkpoint on the
	// last segment made the whole log unreadable.
	//
	// Clamping is the only honest answer — the records are not there, and a log
	// does not get to keep asserting they are. It also loses nothing a caller
	// could have used, because everything above the real tail is unreadable by
	// construction. The opposite skew (a checkpoint BELOW the tail) is the
	// ordinary one and stays for RecoverTail, which walks that suffix and
	// recovers it.
	if newest := activeSegment.NextOffset() - 1; l.hw > newest {
		slog.Warn("commitlog: high watermark checkpoint overshoots the log; clamping",
			slog.String("path", l.Path),
			slog.Int64("checkpoint", l.hw),
			slog.Int64("newest", newest),
		)
		l.hw = newest
	}
	return nil
}

// resolveSegmentOverlaps drops any segment whose records the segment before it
// already holds. Caller holds l.mu, or is open() before publication.
//
// It is the recovery for a crash in the middle of TruncateBefore. That call
// trims the boundary segment by writing the surviving records into a NEW file
// at a HIGHER base offset and then deleting the source, and those are two
// separate steps with a gap between them. Stopping in the gap — after Finalize
// has renamed the trim into place, before Delete removes the source — leaves
// two .log files whose ranges overlap: the source [B, L] and the trim [B+k, L].
// open() had no notion of overlap and took both, so a read walked the source to
// L and then began the trim again at B+k. Offsets came back TWICE, in order,
// with no error anywhere: confirmed as 0..7 then 6,7,8,9.
//
// The trim is a strict SUFFIX of the source, so the source on its own is a
// complete and correct log, and dropping the trim un-does an unfinished
// truncation the caller can simply run again. Dropping the source instead is
// wrong even though it is the newer file: the segments below the boundary are
// deleted one at a time BEFORE the trim is written, so a crash can leave some
// of them standing, and taking the source's low records away then opens a HOLE
// in the middle of the log. An un-done truncation is recoverable; a hole is not.
//
// An overlap that is not containment cannot be produced by any path in this
// package — every other rewrite renames over its source, keeping the base
// offset and so the name. It is reported rather than repaired, because serving
// an offset twice is the failure being fixed here and guessing at a partial
// overlap would only pick a different way to do it.
func (l *commitLog) resolveSegmentOverlaps() error {
	kept := make([]*segment, 0, len(l.segments))
	for _, s := range l.segments {
		prev := (*segment)(nil)
		if n := len(kept); n > 0 {
			prev = kept[n-1]
		}
		// NextOffset is the exclusive end, and an EMPTY segment reports its own
		// base for it — so an empty segment neither overlaps nor is overlapped,
		// which is what we want: it describes no records to duplicate.
		if prev == nil || s.BaseOffset >= prev.NextOffset() {
			kept = append(kept, s)
			continue
		}
		if s.NextOffset() > prev.NextOffset() {
			return errors.Errorf("commitlog: segments %d and %d overlap and "+
				"neither contains the other (%d..%d and %d..%d); refusing to "+
				"open a log that would serve an offset twice",
				prev.BaseOffset, s.BaseOffset, prev.BaseOffset,
				prev.NextOffset()-1, s.BaseOffset, s.NextOffset()-1)
		}
		slog.Warn("commitlog: dropping a segment whose records the previous one "+
			"already holds; a truncation was interrupted before it could remove "+
			"the segment it rewrote",
			slog.String("path", l.Path),
			slog.Int64("dropped_base_offset", s.BaseOffset),
			slog.Int64("covered_by_base_offset", prev.BaseOffset),
			slog.Int64("covered_through", prev.NextOffset()-1),
		)
		if s.isOffloaded() {
			// Closed, not Deleted: Delete would remove the tier OBJECT, and a
			// process that opens a log has not established that it owns the
			// tier. The duplicate is out of the segment list either way, and the
			// next publish by whoever does own it stops naming the object.
			if err := s.Close(); err != nil {
				return errors.Wrap(err, "close overlapping offloaded segment")
			}
			continue
		}
		if err := s.Delete(); err != nil {
			return errors.Wrap(err, "remove overlapping segment")
		}
	}
	l.segments = kept
	return nil
}

// Append writes the given batch of messages to the log and returns their
// corresponding offsets in the log. This will return ErrCommitLogReadonly if
// the log is in readonly mode.
func (l *commitLog) Append(msgs []*Message) ([]int64, error) {
	if l.IsReadonly() {
		return nil, ErrCommitLogReadonly
	}
	// Reading the tail and writing to it must be one step; see appendMu.
	l.appendMu.Lock()
	defer l.appendMu.Unlock()
	// Stamp append time on messages that carry no timestamp (Kafka's
	// LogAppendTime as the fallback for an unset CreateTime). Every
	// time-based feature — age retention, MaxSegmentAge rolling, the
	// CompactMinAge horizon, the timestamp-search APIs — reads segment
	// write times derived from these; producers that never stamp
	// timestamps would otherwise leave segments looking infinitely old
	// (age retention deletes everything, the compaction horizon protects
	// nothing). AppendMessageSet takes pre-encoded bytes and cannot be
	// stamped; replicating callers are expected to carry source timestamps.
	//
	// Read UNDER appendMu, which is what makes the stamp agree with the offset
	// it is stored at. Offsets are assigned under this lock; reading the clock
	// outside it let two appenders interleave as "A reads T2, B reads T1 and
	// wins the lock, A writes T2 after it" — so a later offset carried an
	// EARLIER timestamp. Every timestamp lookup binary-searches on the
	// assumption that offset order is timestamp order, and a search over
	// non-monotonic data does not fail, it answers. The window was as wide as
	// the scheduler cared to make it, and the records it corrupts are stamped
	// wrong on disk for good.
	now := timestamp()
	for _, m := range msgs {
		if m.Timestamp == 0 {
			m.Timestamp = now
		}
	}
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
	// The first SURVIVING segment, not the first entry. A retention pass deletes
	// as it walks and does not publish the survivors until it ends, so during
	// one the head of this list is a segment whose files are gone — and
	// answering with its base offset tells a caller the log starts somewhere a
	// read from that offset will not reach. A reader that trusted it saw records
	// "disappear" between the offset it was told and the first one it got back.
	for _, s := range l.segments {
		if seg, ok := s.current(); ok {
			return seg.FirstOffset()
		}
	}
	return l.segments[len(l.segments)-1].FirstOffset()
}

// LocalBytes reports how many bytes of log data this log occupies on LOCAL
// disk.
//
// It exists for the caller that has to decide what MOVING a log would cost, and
// its two exclusions follow from that question rather than from convenience:
//
//   - offloaded segments do not count. Their bytes are in a SegmentStore that
//     whoever takes the log over reads the same way this process does, so the
//     move does not copy them. A tiered log with a terabyte in object storage
//     and one live segment costs one live segment to move, and reporting the
//     terabyte would refuse every move of exactly the logs that are cheapest.
//   - indexes do not count. They are derived: a copy rebuilds its own, so
//     their bytes are never transferred.
//
// Computed from the positions the segments already hold, so this is arithmetic
// over a lock and not a walk of the filesystem — cheap enough to ask on a
// timer, which is the only way anything watching a whole broker can ask it.
//
// Segments mid-replacement are followed to their replacement and dropped if
// gone, the same rule OldestOffset uses: during a compaction or a retention
// pass the head of the list can be a segment whose files no longer exist, and
// counting it would report space that has already been given back.
func (l *commitLog) LocalBytes() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var n int64
	for _, s := range l.segments {
		seg, ok := s.current()
		if !ok || seg.isOffloaded() {
			continue
		}
		n += seg.Position()
	}
	return n
}

// EarliestOffsetAfterTimestamp returns the earliest offset whose timestamp is
// greater than or equal to the given timestamp.
func (l *commitLog) EarliestOffsetAfterTimestamp(timestamp int64) (int64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.earliestOffsetAfterTimestampLocked(timestamp)
}

// earliestOffsetAfterTimestampLocked is EarliestOffsetAfterTimestamp with the
// read lock already held, so LatestOffsetBeforeTimestamp can be defined in terms
// of it rather than carrying a second copy of the segment search — which is how
// the copy it did carry came to have a different set of bugs.
func (l *commitLog) earliestOffsetAfterTimestampLocked(timestamp int64) (int64, error) {
	// Find the first segment whose base timestamp is greater than the given
	// timestamp.
	// findSegmentIndexByTimestamp cannot fail: it reads each segment's own base
	// timestamp rather than its index, and handles the empty segment itself (see
	// the comment there). It used to return an error, and this had an io.EOF arm
	// besides — each removed when the thing that could produce it went, because
	// the only thing an unreachable arm can still do is convert a future failure
	// into a fabricated offset the caller would trust.
	idx := findSegmentIndexByTimestamp(l.segments, timestamp)
	// Search the previous segment for the first entry whose timestamp is
	// greater than or equal to the given timestamp. If this is the first
	// segment, just search it.
	at := 0
	if idx > 0 {
		at = idx - 1
	}
	// And keep walking back while the segment BEFORE this one still ends at or
	// after the target, because then it holds the earlier answer.
	//
	// The search above is by BASE timestamp and strictly greater, so a target
	// equal to some segment's base lands on that segment — and a timestamp does
	// not stop at a segment boundary. Records are stamped from a clock coarser
	// than the rate they are appended at, so a run of them shares an instant;
	// when a roll falls inside such a run, the new segment's base is exactly the
	// timestamp the previous segment's tail is still carrying. Asking for it
	// then answered with the first record of the LATER segment, and every record
	// of that instant in the earlier one — the ones the caller asked for first —
	// was skipped. A resume point is handed straight to a reader, so what that
	// costs is those records, silently.
	//
	// Bounded by a segment that ends before the target, which is where the
	// answer provably is not. In practice it steps back once: a tie has to span
	// the boundary to be here at all, and one clock tick does not usually span
	// whole segments.
	for at > 0 && l.segments[at-1].LastWriteTime() >= timestamp {
		at--
	}
	// Then forward from there, taking the first segment that holds an entry at
	// or after the target. A loop rather than "this segment, or else the next
	// one", which is what it used to be under idx < len(segments)-1, and which
	// was wrong three ways:
	//
	//   - The bound excluded the LAST segment, so a target landing in the gap
	//     between the previous segment's tail and the last segment's base — a
	//     roll that coincides with a pause, which is just a stream written to
	//     again after a quiet moment — was answered with the next assignable
	//     offset. Everything in that final segment was then in the caller's past
	//     by construction: it resumes from the end of the log and waits, having
	//     been told records sitting right there have already been handled.
	//   - "The next one" was segments[idx], which is not the segment after the
	//     one searched when idx is 0 — the state an empty log is in, since an
	//     empty segment sorts after every timestamp and the search lands on it
	//     rather than past it. That re-searched the segment that had just come up
	//     empty.
	//   - And one step was not always enough. The segment after the answer's may
	//     itself be empty — the active segment in the window just after a roll —
	//     and a single step landed on it and reported its not-found as a real
	//     error rather than reading past it.
	//
	// Bounded by the segment count and normally stopping at the first or second
	// iteration: everything skipped is a segment holding no record at or after
	// the target, which past the answer's segment means an empty one.
	for i := at; i < len(l.segments); i++ {
		entry, err := l.segments[i].findEntryByTimestamp(timestamp)
		if err == nil {
			return entry.Offset, nil
		}
		// ErrEntryNotFound only. io.EOF was accepted here too, which meant a
		// truncated index — position claiming more entries than are mapped — was
		// answered with an offset instead of an error. errors.Is, so a wrapped
		// not-found still reads as one.
		if !errors.Is(err, ErrEntryNotFound) {
			return 0, errors.Wrap(err, "failed to find log entry for timestamp")
		}
	}
	// Nothing in the log is at or after the target, so it is beyond the end:
	// answer with the next assignable offset.
	return l.segments[len(l.segments)-1].NextOffset(), nil
}

// LatestOffsetBeforeTimestamp returns the latest offset whose timestamp is less
// than or equal to the given timestamp.
func (l *commitLog) LatestOffsetBeforeTimestamp(timestamp int64) (int64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// Nothing in the log is at or before a time the whole log starts after.
	//
	// FirstWriteTime() of the oldest surviving segment, which is where the log
	// begins now rather than where it began: retention has taken everything
	// under it, and answering with a record that is gone would be worse than
	// refusing.
	if timestamp < l.segments[0].FirstWriteTime() {
		return 0, ErrTimestampBeforeLog
	}

	// The latest record at or before T is the one BELOW the earliest record
	// strictly after T, which is the other function's whole job. Defined that way
	// rather than searched for directly, because searching for it directly is
	// what this function used to do and it got the same class of thing wrong:
	// findEntryByTimestamp answers with the FIRST entry carrying a timestamp, so
	// an exact match returned the first record of a run sharing an instant where
	// the contract asks for the last — and the segment it searched was picked
	// without the walk-back, so a run spanning a boundary missed by however much
	// of it was on the other side. Neither showed in any test because every case
	// gave each record a timestamp of its own.
	//
	// timestamp+1 rather than timestamp: "after" here has to be strict, or the
	// run carrying T would be excluded along with it.
	if timestamp == math.MaxInt64 {
		// Saturated rather than wrapped. There is nothing after the largest
		// representable instant, so the answer is the newest record — and +1
		// would turn that into the one below the oldest.
		//
		// LastOffset(), not the bare field: this runs concurrently with appends,
		// which mutate lastOffset under the segment's write lock.
		return l.segments[len(l.segments)-1].LastOffset(), nil
	}
	after, err := l.earliestOffsetAfterTimestampLocked(timestamp + 1)
	if err != nil {
		return 0, err
	}
	return after - 1, nil
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
// value, even if the value is less than the current HW — the deliberate
// exception to SetHighWatermark's monotonicity. See the interface doc; it is
// not needed after Truncate, which lowers the watermark itself.
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
//
// l.mu is a different matter, and this holds it only to publish. It is the lock
// every reader takes through Segments(), so holding it across the scan, the
// rewrite and the unlinks stopped the whole log for the length of the call —
// the same convoy TruncateBefore had. Holding appendMu makes this simpler than
// the fix there: nothing can roll a segment underneath the call, so the list at
// publish time is the list that was snapshotted, and there is no rebase to do.
func (l *commitLog) Truncate(offset int64) error {
	// Republish the tier after the segment set changes: dropping segments
	// can remove offloaded ones, and a manifest naming an object that is
	// gone sends a reader at something that will not open.
	defer func() { _ = l.writeTierManifest() }()
	l.cleanMu.Lock()
	defer l.cleanMu.Unlock()
	l.appendMu.Lock()
	defer l.appendMu.Unlock()

	l.mu.RLock()
	closed := l.segmentsClosed
	snapshot := l.segments
	l.mu.RUnlock()
	// A truncation builds a replacement segment, so running it on a log whose
	// segments have already been closed would leave that replacement open with
	// nothing left to close it. Same invariant the roll path holds: never
	// install a segment into a set that has already been walked.
	if closed {
		return ErrCommitLogClosed
	}
	seg, idx := findSegment(snapshot, offset)
	if seg == nil {
		// Nothing to truncate.
		return nil
	}

	// The segment holding the offset is rewritten without its tail, unless the
	// offset is exactly its base — then there is nothing of it to keep and it
	// goes whole, provided it is not the only one left.
	replace := seg.BaseOffset != offset || idx == 0

	// Build the replacement BEFORE deleting anything. The scan can fail, and
	// until the first Delete every failure can be returned with the log exactly
	// as it was found. Deleting first meant a failure here left l.segments
	// naming files that were already gone and vActiveSegment pointing at a
	// segment that was already closed — the call returned an error and the next
	// append died on it, which is a worse outcome than the truncation not
	// happening.
	var newSegment *segment
	if replace {
		var err error
		if newSegment, err = seg.Truncated(); err != nil {
			return err
		}
		ss := newSegmentScanner(seg)
		defer ss.Close()
		for {
			ms, e, err := ss.Scan()
			if err != nil {
				// A scan ends for two very different reasons and this used to
				// treat them alike: it reached the end, or it hit something it
				// could not read. Both arrive as a non-nil error, and what
				// follows writes whatever was collected and DELETES the source —
				// so anything the scan could not reach was dropped, silently,
				// with the call reporting success. Rewriting a segment over an
				// unread suffix is the one thing that turns damage into loss.
				if !errors.Is(err, io.EOF) {
					_ = newSegment.Delete()
					return fmt.Errorf("%w: truncate of segment %d: %w",
						ErrSegmentUnreadable, seg.BaseOffset, err)
				}
				break
			}
			if ms.Offset() >= offset {
				break
			}
			if err := newSegment.WriteMessageSet(ms, []*entry{e}); err != nil {
				_ = newSegment.Delete()
				return err
			}
		}
	}

	// Delete all following segments, with l.mu released.
	//
	// Before the publish, and deliberately unlike TruncateBefore, which unlinks
	// after it. The records above the cut are the ones this call exists to make
	// unreachable — a follower reconciling after an unclean election is being
	// told they diverged — so the window where they can still be served has to
	// be the earliest one available, not the latest. Deleted-but-still-listed is
	// exactly the mid-pass state current() is written for: it answers ok=false
	// and findSegment moves on, which is what a reader does once a pass has
	// finished anyway.
	deleted := 0
	for i := idx + 1; i < len(snapshot); i++ {
		if err := snapshot[i].Delete(); err != nil {
			return err
		}
		deleted++
	}
	if !replace {
		if err := seg.Delete(); err != nil {
			return err
		}
		deleted++
	}

	// Renaming the rewrite over its source and linking the two, also outside the
	// lock. It has to happen before the publish either way: it is what makes a
	// reader holding the old snapshot resolve into the replacement rather than
	// into a segment that is simply gone.
	if replace {
		if err := newSegment.Replace(seg); err != nil {
			return err
		}
	}

	// Retain all preceding segments.
	segments := make([]*segment, len(snapshot)-deleted)
	for i := 0; i < idx; i++ {
		segments[i] = snapshot[i]
	}
	if replace {
		segments[idx] = newSegment
	}
	activeSegment := segments[len(segments)-1]

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.segmentsClosed {
		// Closed underneath us. closeSegments has already walked the set, so
		// publishing into it would leave the replacement open with nothing left
		// to close it — the same invariant checked on the way in, re-checked
		// because the lock was down in between.
		//
		// Close it rather than delete it. Replace has already renamed it over
		// its source, so what is on disk is the truncated log and a reopen finds
		// exactly that; only the handle would leak. Deleting would throw away
		// the work AND leave the source gone.
		if newSegment != nil {
			_ = newSegment.Close()
		}
		return ErrCommitLogClosed
	}
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&l.vActiveSegment)),
		unsafe.Pointer(activeSegment))
	l.segments = segments

	// A truncation can cut below the watermark, and then the watermark names
	// records that are gone. Clamp it, for the same reason and with the same
	// warning as a checkpoint that overshoots the log on reopen: the records are
	// not there, and a log does not get to keep asserting they are.
	//
	// Reachable in production rather than in theory. A follower reconciling
	// against a leader promoted from OUTSIDE the ISR — an unclean election — is
	// told to truncate below what it had locally committed, which is the whole
	// point of an unclean election and not something this call should refuse.
	//
	// Leaving it unclamped was worse than un-committing the records, because
	// SetHighWatermark is monotonic and cannot bring the watermark back down.
	// The log served NO committed reads at all until it was reopened — the
	// watermark resolved to no segment, so every committed reader failed to
	// build — and nothing said why at the point that caused it.
	if newest := activeSegment.NextOffset() - 1; l.hw > newest {
		slog.Warn("commitlog: truncation cut below the high watermark; clamping",
			slog.String("path", l.Path),
			slog.Int64("hw", l.hw),
			slog.Int64("newest", newest),
			slog.Int64("truncated_at", offset),
		)
		l.hw = newest
		l.notifyHWChange()
	}
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
		logKey, idxKey, blkKey := newStoreKeys(s.BaseOffset)
		meta, err := s.uploadTo(l.SegmentStore, logKey, idxKey, blkKey, l.RemoteIndexCache)
		if err != nil {
			return n, err
		}
		if meta.LogKey == "" {
			continue // already offloaded; nothing to commit
		}
		// The commit, one segment at a time. After the objects and BEFORE the
		// local bytes are dropped, which is the only ordering that is safe in
		// both directions: an object no manifest names was never committed and
		// is a recognisable orphan, and a local file is never removed against an
		// entry that is not yet published.
		//
		// Per segment rather than once per pass, because a batch would put every
		// segment in the pass on the wrong side of that second rule.
		if err := l.writeTierManifest(meta.tierObject(s.BaseOffset)); err != nil {
			return n, err
		}
		if err := s.attachOffloaded(l.SegmentStore, meta, l.RemoteIndexCache); err != nil {
			// An error there no longer means the segment stayed local: past the
			// point where the store backing is open the swap happens regardless,
			// and what is reported is a local cleanup that did not fully succeed.
			// The pass still stops, but the count has to agree with the manifest,
			// which already names this segment.
			if s.isOffloaded() {
				n++
			}
			return n, err
		}
		n++
	}
	return n, nil
}

// TruncateBefore removes all messages from the log with offset < minOffset.
// Sealed segments entirely before minOffset are deleted. A boundary sealed
// segment (one that straddles minOffset) is rewritten keeping only records at
// or after minOffset. The active segment is never rewritten.
// The lock discipline here is the same one CleanWithSpec uses, and for the same
// reason: decide under l.mu, do the FILE WORK with it released, then re-take it
// only to publish. Every reader and every appender takes l.mu — Segments()
// RLocks it, split() Locks it — so anything done while holding it is a hard stop
// for the whole log.
//
// This used to hold the write lock across all of it: N segment closes, N
// unlinks, and then a scan of the boundary segment end to end and a write of a
// whole new one. Reported downstream as a 10-minute test timeout whose stack was
// one truncator inside a Windows FlushFileBuffers with every reader and the
// writer queued on the mutex behind it. Not a deadlock — a convoy, and the log
// was unavailable for as long as the truncation took.
//
// Two things follow from letting go of the lock, and both have precedent above:
//
//   - An append can ROLL while it is down, so the surviving list cannot be
//     spliced from the snapshot alone; the segments split() appended have to be
//     carried over. Same rebase CleanWithSpec does.
//   - Close() does not take cleanMu, so the log can close underneath the
//     rewrite. Publishing into a set closeSegments has already walked would
//     leave the trim with nothing to close it, which is the leak split()'s CAS
//     comment describes. Hence the second segmentsClosed check.
//
// The deletes moving after publication also WIDEN the window in which the disk
// holds the trim and the source it was rewritten from at the same time. That is
// safe only because open() resolves overlapping segments; before it did, a crash
// in that window made a reopened log serve those records twice.
func (l *commitLog) TruncateBefore(minOffset int64) error {
	// Republish the tier after the segment set changes: dropping segments
	// can remove offloaded ones, and a manifest naming an object that is
	// gone sends a reader at something that will not open.
	defer func() { _ = l.writeTierManifest() }()
	l.cleanMu.Lock()
	defer l.cleanMu.Unlock()

	l.mu.RLock()
	// See Truncate: this one also builds a trimmed boundary segment.
	closed := l.segmentsClosed
	oldSegments := l.segments
	l.mu.RUnlock()
	if closed {
		return ErrCommitLogClosed
	}
	if len(oldSegments) == 0 || minOffset <= 0 {
		return nil
	}

	// Find the first sealed segment whose last offset >= minOffset (the boundary).
	// All sealed segments before it are entirely obsolete.
	// If no sealed segment qualifies, boundaryIdx = len-1 (the active segment),
	// and we just delete all sealed segments.
	boundaryIdx := len(oldSegments) - 1
	for i := 0; i < len(oldSegments)-1; i++ {
		if oldSegments[i].LastOffset() >= minOffset {
			boundaryIdx = i
			break
		}
	}

	// Rewrite the boundary segment if it's a sealed segment whose BaseOffset
	// falls before minOffset (meaning it straddles the cut point). This is the
	// expensive half — a whole segment read and a whole segment written — and it
	// runs with the lock down. It touches no shared state: the boundary is
	// sealed, so it is only read, and the trim is written under a .trimmed suffix
	// that open() does not pick up.
	var (
		boundary     *segment
		trimmed      *segment
		dropBoundary bool
	)
	if boundaryIdx < len(oldSegments)-1 && oldSegments[boundaryIdx].BaseOffset < minOffset {
		boundary = oldSegments[boundaryIdx]
		ss := newSegmentScanner(boundary)
		var (
			newBaseOffset int64 = -1
			kept          []messageSet
		)
		for {
			ms, _, err := ss.Scan()
			if err != nil {
				// See the same guard in Truncate. This one has the sharper
				// edge: if the FIRST scan fails, newBaseOffset stays -1, and
				// "no records at or above minOffset" is indistinguishable
				// from "could not read the first frame" — so the branch
				// below deleted the whole boundary segment, including every
				// record the caller asked to keep, and returned nil.
				if !errors.Is(err, io.EOF) {
					ss.Close()
					return fmt.Errorf("%w: truncate before, boundary "+
						"segment %d: %w", ErrSegmentUnreadable,
						boundary.BaseOffset, err)
				}
				break
			}
			if ms.Offset() >= minOffset {
				if newBaseOffset < 0 {
					newBaseOffset = ms.Offset()
				}
				kept = append(kept, ms)
			}
		}
		ss.Close()

		if newBaseOffset >= 0 {
			t, err := boundary.Trimmed(newBaseOffset)
			if err != nil {
				return errors.Wrap(err, "create trimmed segment failed")
			}
			for _, ms := range kept {
				entries := entriesForMessageSet(t.Position(), ms)
				if err := t.WriteMessageSet(ms, entries); err != nil {
					t.Delete()
					return errors.Wrap(err, "write trimmed segment failed")
				}
			}
			if err := t.Finalize(); err != nil {
				t.Delete()
				return errors.Wrap(err, "finalize trimmed segment failed")
			}
			// Seal so that uncommitted readers hitting EOF on this
			// segment immediately move to the next one instead of
			// waiting for more data that will never come.
			t.Seal()
			trimmed = t
		} else {
			// Boundary segment had no records >= minOffset; it goes entirely.
			dropBoundary = true
		}
	}

	// Publish. Everything above this point is undoable by returning an error;
	// nothing below it is, which is why the file removals come after.
	firstKept := boundaryIdx
	if dropBoundary {
		firstKept++
	}
	l.mu.Lock()
	if l.segmentsClosed {
		// The log closed while this was rewriting, which it does outside l.mu.
		// Same reasoning as CleanWithSpec: do not install into a set that
		// closeSegments has already walked. The trim is removed rather than
		// closed — it was never published, so nothing can be reading it, and
		// leaving the file would leave the directory holding an overlap for
		// open() to resolve on the next boot.
		l.mu.Unlock()
		if trimmed != nil {
			_ = trimmed.Delete()
		}
		return ErrCommitLogClosed
	}
	// Copy on write, for the same reason Truncate does. Segments() hands out the
	// slice HEADER, so every lock-free reader holding one indexes this backing
	// array — writing an element in place is a data race against all of them, and
	// the race detector calls it: reported downstream, red under -race in a
	// deletion and a truncate chaos test. Publishing a new array instead leaves
	// whatever a reader is already holding immutable.
	newSegments := l.segments
	survivors := make([]*segment, 0, len(newSegments)-firstKept)
	survivors = append(survivors, oldSegments[firstKept:]...)
	if trimmed != nil {
		// firstKept IS the boundary in this branch; the trim stands in for it.
		survivors[0] = trimmed
	}
	if len(newSegments) > len(oldSegments) {
		// An append rolled while the lock was down. split() only ever appends, so
		// the extra segments are at the tail and everything below them is
		// unchanged — carry them over rather than publishing a list that has
		// forgotten them.
		survivors = append(survivors, newSegments[len(oldSegments):]...)
	}
	l.segments = survivors
	err := l.leaderEpochCache.ClearEarliest(minOffset)
	l.mu.Unlock()
	if err != nil {
		return err
	}

	// Now the file work, with the log open for business again. A failure here
	// leaves files behind rather than corrupting anything: the surviving list is
	// already correct and already published, and what is left on disk is a
	// retention pass that did not finish, which the next one repeats.
	for i := 0; i < boundaryIdx; i++ {
		if err := oldSegments[i].Delete(); err != nil {
			return err
		}
	}
	if boundary != nil && (trimmed != nil || dropBoundary) {
		if trimmed != nil {
			// Before the delete, and this is load-bearing: a reader that
			// already resolved into the boundary is holding a segment about
			// to go. Without the link it reads one that is gone with no
			// replacement, which means "retention collected these" and sends
			// it to the NEXT segment — past the records this trim just
			// preserved. The caller asked to keep them and the log went on
			// reporting them present, so the read came back 1-3 records late
			// with no error anywhere.
			boundary.SupersededBy(trimmed)
		}
		if err := boundary.Delete(); err != nil {
			return err
		}
	}
	return nil
}

// Segments returns the log's segment slice. It returns the slice HEADER, not a
// copy — deliberately, because this is on the path of every read and copying
// here would allocate per call.
//
// That choice puts an obligation on the other side, and it is the one thing to
// know before changing the segment set: callers index the returned slice WITHOUT
// holding l.mu, so whoever changes the set publishes a NEW array rather than
// writing into the one readers are already indexing. Assigning l.segments is
// fine. Writing l.segments[i] = x is a data race against every reader holding a
// snapshot, whatever lock is held while doing it — TruncateBefore did exactly
// that and shipped it (fixed in v0.44.2).
//
// Appending is safe, and worth spelling out since it looks like a write into
// the shared array: append can only touch indices at or past len(l.segments),
// and a snapshot's length is fixed when it is taken, so no reader ever indexes
// there.
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
	//
	// UNREACHABLE as the code stands, and kept anyway. split has exactly one
	// caller — checkAndPerformSplit — and every path into it holds appendMu, so
	// two goroutines cannot both be here and the CAS cannot lose. It is the
	// residue of the era when a roll could also run on the cleaner's ticker
	// while an append was in flight, which is the bug appendMu now prevents
	// outright.
	//
	// Retained as a backstop, because it costs one atomic on a rare path and the
	// failure it catches (two rollers over the same files, the loser's cleanup
	// unlinking the winner's) was silent and expensive. But it has NO TEST and
	// cannot honestly have one: reaching it means calling split concurrently,
	// which means bypassing the lock the caller is required to hold, so a test
	// would be asserting against a caller that does not exist. Stated here
	// rather than left as an entry in hack/guardcheck.sh that would sit red
	// forever.
	//
	// If a second caller of split is ever added, this stops being vestigial and
	// the lock discipline above it needs re-deciding, not this line.
	//
	// Both steps of publishing the new segment — the CAS and the append to
	// l.segments — are held under l.mu, and that is what makes closing safe.
	// closeSegments walks l.segments and sets segmentsClosed under the same
	// lock, so a roll either finishes entirely before the walk and is closed by
	// it, or sees the flag below. Publishing outside the lock left a window in
	// between: an append still in flight when Close ran could roll a segment
	// AFTER the walk had passed it, so the log's own slice ended up naming a
	// segment nothing would ever close — a file handle and an index mmap held
	// until the process exited.
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.segmentsClosed {
		// The log closed underneath this append. Do not publish into a set that
		// has already been walked; refusing the append is the correct outcome
		// for a log that is closing, and it is the caller's own append that
		// fails rather than an unrelated one.
		segment.Delete() // nolint: errcheck
		return ErrCommitLogClosed
	}
	if !atomic.CompareAndSwapPointer(
		(*unsafe.Pointer)(unsafe.Pointer(&l.vActiveSegment)),
		unsafe.Pointer(oldActiveSegment), unsafe.Pointer(segment)) {
		segment.Delete() // nolint: errcheck
		return ErrSegmentExists
	}
	l.segments = append(l.segments, segment)
	return nil
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

	return AtomicWriteFileWithRetry(file, r)
}

// Sidecars are small named metadata files owned by the log's CLIENT, stored
// in the log directory next to the segments (e.g. durable_streams' recovery
// floor checkpoint). Put writes atomically (temp + rename), so a crash never
// leaves a torn sidecar; Get returns os.ErrNotExist-satisfying errors when
// absent; Remove of an absent sidecar is a no-op. Names must not collide
// with the log's own files (segments, indexes, checkpoints).
func (l *commitLog) PutSidecar(name string, data []byte) error {
	return AtomicWriteFileWithRetry(filepath.Join(l.Path, name), bytes.NewReader(data))
}

func (l *commitLog) GetSidecar(name string) ([]byte, error) {
	// Paired with PutSidecar's AtomicWriteFileWithRetry: a sidecar written with
	// a retry deserves to be read with one, and the documented contract that an
	// absent sidecar satisfies os.ErrNotExist is preserved — ReadFileWithRetry
	// returns that immediately rather than waiting the file out.
	return ReadFileWithRetry(filepath.Join(l.Path, name))
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
