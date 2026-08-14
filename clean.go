package commitlog

// Cleaning is the log's SUPERVISION of the cleaners, not a cleaner itself. The
// policies live in compact_cleaner.go and delete_cleaner.go; what is here is the
// part that only the log can do: hold the lock, decide which segments a pass may
// see, install the result, and republish the tier manifest afterwards.
//
// CleanSpec sits here for the same reason. It is the log's public contract for a
// pass, and — see its own doc — it is parameterized by things this layer cannot
// verify, which is a property of the boundary rather than of either cleaner.

import (
	"log/slog"
	"time"

	"github.com/pkg/errors"
)

func (l *commitLog) cleanerLoop() {
	l.cleanAtOpen()
	l.tickUntilClosed(l.CleanerInterval, l.cleanerTick)
}

// tickUntilClosed runs fn on every tick of interval, and returns when the log
// closes.
//
// One select, and the ORDER inside it is the whole reason this is shared: the
// closed arm returns before fn runs, so a log shutting down does not get one
// last pass out of a loop that has already been told to stop. Written out twice
// — here and in checkpointHWLoop — it was two chances to get that backwards,
// and this repo has already lost a day to a cleaner loop whose PASS was fine
// and whose loop was not.
//
// Go picks uniformly among ready cases, so a tick that arrives at the same
// moment as the close may still run fn once. That is a race with the close, not
// a use after it: everything fn touches is alive until Close returns, and Close
// waits for this loop.
func (l *commitLog) tickUntilClosed(interval time.Duration, fn func()) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-l.closed:
			return
		}
		fn()
	}
}

// cleanAtOpen runs one cleaner pass before the loop ever waits on its ticker.
//
// NewTicker does not fire until t+interval, and nothing on disk records when
// the last pass ran — so the loop above waited a whole CleanerInterval before
// its first pass, and the clock started over on every process start. A process
// that lives less than the interval therefore never cleaned at all. Not rarely,
// not late: never, for the life of the deployment, however much there was to
// reclaim. sqlcdc measured it — 149 restarts averaging 95.9s against a 5m
// interval, zero passes in four hours, and the single pass that did once fire
// reclaimed 69%.
//
// This is the same defect as the rolling tick below, one level out: there the
// pass worked and the loop skipped it, here the pass works and the loop never
// reaches it. Both hid because every compaction test called Clean() directly,
// which is the one path production does not take.
//
// Cleaning at open rather than persisting a last-clean timestamp, on purpose. A
// timestamp is a new durable file, a new parse, and a new way to be wrong about
// time — and the epoch checkpoint already shows what an unchecksummed sidecar
// costs. The price of the simpler answer is that a restart storm runs a pass per
// start rather than one per interval, and that is already bounded:
// CleanRewriteBudget exists precisely so a pass fits inside a short-lived
// process's kill window. The design anticipated short-lived processes; it just
// never started a pass in one.
//
// Nothing about startup latency changes. This runs on the background goroutine
// New has already returned from, so a caller is not made to wait for a
// compaction pass in order to open a log.
//
// The open pass reads a HIGH WATERMARK that open() has already restored (see the
// hwFileName branch there) — without that this would be a pass that cannot
// compact anything, and the fix would be decorative.
//
// It does narrow the gap between New returning and a caller calling
// SetTierReadOnly. That gap is not new and is already closed the right way: New
// publishes the log descriptor to the store on its own, so a process that does
// not own a tier has to say so through Tier.ReadOnly rather than
// afterwards. This widens an existing requirement, it does not add one.
// It runs the CLEAN only, and deliberately not a tick. cleanerTick rolls the
// active segment before it cleans, and it does that ahead of the
// DisableAutoClean check — so calling a whole tick here rolled a segment at
// open, on a schedule nobody asked for and even on logs that had switched the
// automatic cleaner off. TestCleaner read two segments where it had written one.
// Rolling is the periodic tick's business; the bug being fixed here is that the
// log never CLEANED, so that is all this does.
func (l *commitLog) cleanAtOpen() {
	// A log closed before this goroutine was scheduled has nothing to clean, and
	// Close is already waiting on bgWG for it.
	select {
	case <-l.closed:
		return
	default:
	}
	l.cleanOnce()
}

// cleanerTick is one pass of the cleaner loop: roll the active segment if it is
// due, then clean.
//
// Split out from the loop so it can be called ONCE, by a test, and observed.
// What broke here was not the pass but the loop that runs it, and every
// compaction test in this package called Clean() directly — the one path
// production never takes. A tick you can invoke is the difference between
// testing the cleaner and testing the cleaning.
func (l *commitLog) cleanerTick() {
	// Check to see if the active segment should be split. Whether one HAPPENED
	// is deliberately not consulted below — see there.
	if _, err := l.checkAndPerformSplitLocked(); err != nil {
		slog.Error(
			"Failed to split log",
			slog.String("path", l.Path),
			slog.String("error", err.Error()),
		)
		return
	}

	// A roll does NOT stand in for a clean, and the tick cleans whether or not
	// one happened. This used to return here on split, on the stated premise
	// that the cleaner "already ran" — it does not and never did:
	// checkAndPerformSplit rolls and seals, and Clean has exactly one caller,
	// which is the line below.
	//
	// So a rolling tick was a SKIPPED pass, not a redundant one, and which logs
	// that hurt depended entirely on load. A quiet log rarely has a segment
	// ready to roll and cleaned every tick; a log under continuous write always
	// does. Worse, the usual pairing makes it certain rather than likely:
	// CheckSplit is true once the active segment reaches MaxSegmentAge, so a log
	// with MaxSegmentAge at or below CleanerInterval has a roll pending at EVERY
	// tick and never cleaned at all. Reported by durable_streams from a 5.5h
	// soak — a 4.5GB compacted log, 336 segments, 239 live keys, zero rewrites,
	// ~66 consecutive ticks that each rolled and went home.
	l.cleanOnce()
}

// cleanOnce is the cleaning half of a tick, without the roll. Shared with
// cleanAtOpen so that the two cannot disagree about what DisableAutoClean
// suppresses.
func (l *commitLog) cleanOnce() {
	if l.DisableAutoClean {
		return
	}

	if err := l.Clean(); err != nil {
		slog.Error(
			"Failed to clean log",
			slog.String("path", l.Path),
			slog.String("error", err.Error()),
		)
	}
}

// Bound is an optional log offset. Its zero value is "no bound supplied", which
// is what a CleanSpec literal that omits the field gets, and At(0) is a bound AT
// offset zero — a distinction the fields using it cannot live without.
//
// It exists because those fields were an int64 whose zero value had to mean
// unset, and that was a live bug: 0 is the narrowest ceiling a caller can ask
// for and the strictest floor, so the one spec asking for maximum protection was
// the one that got none. A *int64 fixes that and brings its own problems — a nil
// to handle at every use, an address to take at every call site, and a pointer
// the caller can mutate after handing it over. A two-word value has none of
// those: it is comparable, it copies, and there is nothing to dereference.
type Bound struct {
	off int64
	set bool
}

// At returns a Bound at off. Any offset is valid, including 0 and including a
// negative one: HighWatermark() answers -1 for "nothing committed yet", callers
// pass that straight through, and there it correctly means "bound everything".
func At(off int64) Bound { return Bound{off: off, set: true} }

// Get returns the offset and whether one was supplied.
func (b Bound) Get() (int64, bool) { return b.off, b.set }

// Or returns the offset, or fallback when no bound was supplied.
func (b Bound) Or(fallback int64) int64 {
	if !b.set {
		return fallback
	}
	return b.off
}

// CleanSpec parameterizes a transaction-aware clean. The commitlog provides
// the mechanism; a transactional layer (e.g. durable_streams) supplies the
// policy: which records' transactions aborted, where the decided prefix
// ends, and which per-message headers make a record transactional.
type CleanSpec struct {
	// Ceiling is the compaction bound: records at or above it are always
	// retained verbatim and never counted latest-per-key (they may be
	// undecided). Transactional callers pass At(lso) so open transactions can
	// never shadow or be compacted. The zero Bound means no bound was supplied
	// and the pass uses the high watermark, which is what a non-transactional
	// caller wants: everything is decided.
	//
	// A Bound rather than an int64, for the reason Bound documents and which is
	// worth stating twice because the sentinel version of this field was a live
	// bug. At(0) is a REAL ceiling — "compact nothing" — and it is precisely what
	// a caller whose oldest open transaction begins at offset 0 must pass. When
	// the field was an int64 whose zero value meant unset, that caller silently
	// got the high watermark instead: the one spec that asked for maximum
	// protection was the one that compacted undecided records, and
	// TestCleanSpecCeilingAboveUndecidedLosesKey is what that costs.
	//
	// Supplying a Ceiling at all obliges the caller to set
	// Options.DisableAutoClean. The reason is in the sentence above: a ceiling is
	// worth supplying only when the high watermark is the WRONG bound, and the
	// high watermark is exactly the bound the automatic pass uses. Leave the
	// automatic pass on and it compacts, on its own timer and with no knowledge
	// of any transaction, the very records this ceiling was set to protect. The
	// spec would be honoured on every pass the caller drives and ignored on every
	// pass it does not.
	//
	// Ceiling bounds COMPACTION and says nothing about decidedness — that is
	// StripBelow's job. A transactional caller passes At(lso) because undecided
	// records must not be compacted, but holding the bound lower for an unrelated
	// reason is legitimate: durable_streams lowers it below its LSO to pin records
	// a lagging consumer group has not read. StripBelow may therefore exceed
	// Ceiling, and it costs nothing when it does — classify retains everything at
	// or above the ceiling before it considers stripping, so the ceiling wins. Do
	// not add a check refusing that pairing; v0.76.0 did and v0.77.0 reverted it.
	// See CleanWithSpec.
	Ceiling Bound
	// ceiling is Ceiling resolved against the log's high watermark. clean() sets
	// it and the compaction pass reads only it, so no code below this line has
	// to know the fallback or handle a nil.
	//
	// Derived rather than passed, exactly like skipTiers: the fallback is the
	// LOG's high watermark, and a caller must not be able to hand the pass a
	// resolved bound that disagrees with the one it asked for.
	ceiling int64
	// StripBelow: records strictly below it are DECIDED, and nothing above
	// the log needs their per-record bookkeeping any more. Compaction removes
	// control records (AttrControl) below it, removes aborted data records,
	// and rewrites the survivors without StripHeaders. Offsets, timestamps,
	// leader epochs, keys, values and attribute bits survive the rewrite.
	//
	// Independent of Ceiling, and legitimately above it. Where the two overlap
	// the ceiling wins: classify retains everything at or above spec.ceiling
	// before it considers stripping, so a record in [Ceiling, StripBelow) is kept
	// verbatim, headers and all. A StripBelow above the ceiling is therefore not a
	// contradiction to refuse but a bound that simply stops applying up there —
	// which is what a caller pinning records for a lagging consumer group ends up
	// writing. This field is the only one of the two that speaks about
	// decidedness.
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
	// RewriteBudget bounds how long one STAGE may spend REWRITING segments
	// (digest skips stay free): once exceeded, remaining debt defers to the
	// next pass, so reclamation scales to any inflow. The budget is spent in
	// drop-density order. 0 = unbounded. At least one rewrite always proceeds.
	//
	// A stage, not a pass, and the difference is a multiple rather than a
	// rounding error — size it accordingly. A pass runs the local rewrite stage,
	// one tiered rewrite stage PER TIER (see TierBudgets, which falls back to
	// this value for a tier it does not name), and the join stage, and each takes
	// a budget of its own. A compacted log with two tiers can therefore spend
	// 4×RewriteBudget before a pass returns.
	//
	// That is deliberate at every step and still worth stating plainly, because
	// the number a caller reaches for is a process kill window, and a kill window
	// is a property of the PASS. Sharing one counter would express each ordering
	// by starving whatever ran last: one slow remote rewrite would consume the
	// pass and local debt would grow untouched, and any log with compaction debt
	// would never join. Those are different rules than "the cheap tier is not
	// charged for the expensive one" and "bytes before file handles". The
	// worst case is bounded and knowable, so it is the caller's to divide.
	RewriteBudget time.Duration
	// TierBudgets bounds, per tier, how long one pass may spend rewriting
	// segments whose bytes live in that tier's store. A tier with no entry
	// falls back to RewriteBudget, so a caller that sets nothing sees no
	// change.
	//
	// An entry of 0 is REFUSED, rather than falling back or meaning unbounded.
	// It has two readings and this field can serve neither: unbounded is what 0
	// means on RewriteBudget one field up, and an unbounded tiered rewrite is
	// the pass-consuming case described below; unset is what absence from the
	// map already says. Say unbounded with a duration longer than a pass, and
	// unset by leaving the tier out.
	//
	// Separate from RewriteBudget because the two rewrites cost wildly
	// different things. A local rewrite reads and writes local disk; a tiered
	// one downloads the object, rewrites, and uploads the result — orders of
	// magnitude slower against remote storage, and metered. Sharing one
	// wall-clock budget lets a single slow tiered rewrite consume the whole
	// pass and starve local compaction indefinitely, which is the case this
	// exists to prevent: local debt would grow while the pass spends its
	// seconds on one remote object.
	//
	// Separate PER TIER for the same reason one step down: a rewrite in a fast
	// nearby store and one in a cold archive differ by as much as local and
	// remote do, and a caller that gives its archive a small budget must not
	// thereby shrink its hot tier's.
	//
	// Every budget still guarantees at least one rewrite, so debt anywhere
	// drains rather than deadlocking under a small budget.
	TierBudgets map[string]time.Duration
	// JoinBelow enables joining adjacent sealed LOCAL segments: a run of them is
	// replaced by one segment holding every record, while the result stays at or
	// below this many bytes. 0 disables it.
	//
	// Caller-driven, like everything else on this spec, because a join is worth
	// doing when load is low and commitlog cannot see load. It is its own stage
	// of the pass rather than part of compaction: compact() and
	// consolidateSegments run on opposite branches of `if l.Compact`, so a join
	// placed in either would only ever reach half the logs, and both accumulate
	// segments.
	//
	// The cap is about RETENTION GRANULARITY and the cost of a later rewrite, not
	// about rolling — a sealed segment is never appended to, and CheckSplit runs
	// on the active one. A joined segment is an ordinary segment: the same size
	// and age retention rules apply, with no join-specific horizon. The accepted
	// consequence is that a run mixing an older segment with a newer one frees
	// space only when the whole result ages out, which is what this cap bounds.
	JoinBelow int64
	// TierJoinBelow is JoinBelow per tier, for segments whose bytes live in a
	// store. A run never crosses a tier boundary — every input shares one — so
	// this is the cap for runs entirely inside the named tier.
	//
	// A tier ABSENT from this map is not joined. Note the contrast with
	// TierBudgets, which falls back to RewriteBudget: there is deliberately no
	// fallback here, because absence is how a READ-ONLY tier stays untouched
	// without having to be named. A fallback would join into a store this log may
	// not write to.
	TierJoinBelow map[string]int64
	// TierPlacement names, per segment base offset, the tier that segment
	// should live in after this pass. A segment absent from the map does not
	// move, and a segment already in the tier named does not move either.
	//
	// This is the second hop, and it is expressed rather than scheduled.
	// commitlog moves the bytes; the caller decides when, because descent
	// between stores is a policy question about cost and durability that only
	// the caller has the context for. The FIRST hop — local disk into the
	// nearest tier — is scheduled here, by LocalRetentionAge, because it is
	// about local disk pressure, which the log can see and the caller cannot.
	//
	// Keyed by base offset because that is how TierObject identifies a segment,
	// and by tier NAME rather than index because indices renumber when a caller
	// edits its chain and a renumber must not silently redirect bytes.
	//
	// A tier that is not in Options.Tiers is an error and nothing moves. A base
	// offset with no offloaded segment behind it is skipped — see movePlaced
	// for why those two differ.
	TierPlacement map[int64]string
	// skipTiers names the tiers this log may not write to. Segments in one are
	// left entirely alone: no rewrite, and no tier retention. Local segments,
	// and segments in the tiers not named, compact and retain as usual.
	//
	// Unexported deliberately. Whether this log may write to a store is a
	// property of the LOG, not of one pass — see Tier.ReadOnly and
	// SetTierReadOnly — and offering both invited a caller to set them to
	// disagree. CleanWithSpec derives this from the log's current modes.
	skipTiers map[string]bool
	// TierWriter is the identity stamped into any store objects this pass
	// writes. Set from the log's current value by CleanWithSpec; a caller
	// setting it directly overrides that for the pass.
	TierWriter string
	// RetentionFloor is the lowest offset RETENTION may not delete: a segment
	// is eligible for deletion only if every record in it lies strictly below
	// it. The zero Bound means no floor, which is what every caller had before
	// this existed.
	//
	// It bounds the DELETE cleaner only. Compaction is already bounded by
	// Ceiling, and a caller's floor is at or above its ceiling by construction:
	// the records this protects are precisely the ones not yet decided.
	//
	// The reason it exists is that retention and settlement answer different
	// questions. A transactional caller's staged records sit in the log for as
	// long as its transaction is open, and nothing about a segment's age, bytes
	// or message count knows that — so a long transaction over a small
	// retention limit had its own staged records collected out from under it,
	// and its commit then referred to offsets that no longer existed.
	//
	// A Bound rather than a sentinel, deliberately. Every obvious sentinel is a
	// real floor: 0 protects the whole log, which is exactly what a transaction
	// that began at offset 0 needs, and a caller writing `RetentionFloor: At(f)`
	// from a tracker that returns 0 for that case would silently get "no
	// protection" from an int64 field whose zero value had to mean unset.
	RetentionFloor Bound
}

// Clean applies retention and compaction rules against the log, if applicable.
func (l *commitLog) Clean() error {
	// The automatic pass is BOUNDED, and this is the only place that can bound
	// it: a caller driving CleanWithSpec sets its own budget, and the spec-less
	// path had no way to reach the field at all. See Options.CleanRewriteBudget
	// for why the default is CleanerInterval and why stopping early loses
	// nothing.
	spec := CleanSpec{}
	if l.Options.CleanRewriteBudget > 0 {
		spec.RewriteBudget = l.Options.CleanRewriteBudget
	}
	if l.Options.CompactTombstoneRetention > 0 {
		// Spec-less tombstone GC for non-transactional compacted logs,
		// bounded like the rest of the spec-less compaction.
		spec.TombstoneGCBelow = l.HighWatermark()
		spec.TombstoneRetention = l.Options.CompactTombstoneRetention
	}
	_, err := l.CleanWithSpec(spec)
	return err
}

// CleanWithSpec applies retention and a transaction-aware compaction pass, then
// offloads whatever LocalRetentionAge now puts past the local horizon. See the
// interface doc for the returned verified floor.
func (l *commitLog) CleanWithSpec(spec CleanSpec) (int64, error) {
	// A Ceiling on a log that still cleans automatically is refused rather than
	// documented, because the two settings do not merely disagree — the second
	// silently undoes the first. The pass this call drives protects everything at
	// or above the ceiling; the automatic pass has no spec, bounds itself at the
	// high watermark, and compacts exactly those records on its own timer. The
	// caller would see its ceiling honoured on every pass it drives and ignored
	// on every pass it does not, which is indistinguishable from working until an
	// undecided record goes missing.
	if _, ok := spec.Ceiling.Get(); ok && !l.DisableAutoClean {
		return 0, errors.New(
			"commitlog: CleanWithSpec was given a CleanSpec.Ceiling on a log whose " +
				"automatic cleaner is running, and the automatic pass would compact " +
				"the records the ceiling protects; set Options.DisableAutoClean")
	}
	// Refused for the same reason and by the same rule: a spec that cannot be
	// honoured fails loudly rather than being reinterpreted. Naming a tier and
	// giving it 0 has two readings and the field can serve neither. Read as
	// "unbounded", which is what 0 means on RewriteBudget one field up, it
	// removes the only bound on the remote rewrite that this field exists to
	// keep from consuming the pass. Read as "unset", it is a value the caller
	// wrote down and the pass ignores — and map ABSENCE already says unset, so
	// the caller who wanted that had a way to say it.
	//
	// Which is to say the entry is not bad input so much as input with no
	// meaning available, and the fix a caller needs differs by which they meant.
	// A duration longer than any pass is how you say unbounded here; deleting
	// the key is how you say unset.
	for tier, d := range spec.TierBudgets {
		if d == 0 {
			return 0, errors.Errorf(
				"commitlog: CleanSpec.TierBudgets names tier %q with a budget of 0, "+
					"which is neither unbounded (that is what 0 means on RewriteBudget, "+
					"and an unbounded tiered rewrite is what this field exists to "+
					"prevent) nor unset (absence from the map is); remove the entry, or "+
					"give it a duration longer than a pass", tier)
		}
	}
	// DO NOT add a check that StripBelow must not exceed Ceiling. v0.76.0 did,
	// and v0.77.0 took it back out after it broke durable_streams in production.
	//
	// The premise was that the two fields describe one boundary from opposite
	// sides — Ceiling saying "at or above me records may be UNDECIDED", StripBelow
	// saying "below me records ARE DECIDED" — so StripBelow above Ceiling had to
	// be a contradiction that the pass would resolve destructively. Both halves of
	// that were wrong.
	//
	// Ceiling is not a claim about decidedness. It bounds COMPACTION: a
	// transactional caller passes the LSO because undecided records must not be
	// compacted, but holding the bound lower for an unrelated reason is
	// legitimate, and durable_streams does exactly that — both fields built equal
	// at the LSO, then Ceiling lowered ALONE to pin records a lagging consumer
	// group has not read. Nothing about that pairing is inconsistent.
	//
	// Nor could the pass have done damage with it. classify returns dispRetain for
	// `offset >= spec.ceiling` BEFORE it looks at StripBelow at all, so the
	// ceiling already wins: a StripBelow reaching above it simply has no effect up
	// there. The thing the refusal was protecting against could not happen, and
	// the refusal itself rejected every pass on a stream that had a decided
	// transaction and a slow group at the same time.
	//
	// The lesson is the one docs/layering.md already carries and this failed to
	// respect: this package does not own these concepts. An invariant that reads
	// as arithmetic between two fields is still a claim about their MEANING, and
	// the meaning lives in the caller.
	verified, err := l.cleanPass(spec)
	// AFTER the pass, and outside it, for a reason that is not stylistic:
	// cleanPass holds cleanMu for its whole body and OffloadBefore takes cleanMu
	// itself, so calling this from inside would deadlock the log rather than
	// return anything. The offload also WANTS to be second — the pass is what
	// decides which segments still exist and which bytes they hold, and
	// offloading before it would copy records to the store that the pass was
	// about to drop.
	//
	// Its error does not mask the pass's. The pass already installed segments
	// and published a manifest by the time this runs; a failure to offload
	// leaves those bytes local, which is the state every log starts in and the
	// next pass retries from.
	if offErr := l.offloadLocalRetention(); offErr != nil && err == nil {
		err = offErr
	}
	return verified, err
}

// offloadLocalRetention offloads every sealed segment lying entirely before the
// local retention horizon. Zero LocalRetentionAge disables it.
//
// This is scheduling that used to sit outside the log, in durable_streams, and
// every input to it was already here: the horizon is this duration and a clock,
// the offset lookup is EarliestOffsetAfterTimestamp, and the "may this process
// write to the store" rule is tierWritable, which OffloadBefore consults for
// itself. A caller reproducing that rule keeps a second copy of it, and the
// copy that is not next to SetTierReadOnly is the one that drifts.
func (l *commitLog) offloadLocalRetention() error {
	if l.Options.LocalRetentionAge <= 0 || !l.hasTier() {
		return nil
	}
	// No tierWritable check here on purpose. OffloadBefore answers (0, nil)
	// for a log that does not own its tier, deliberately, so that every process
	// can run the same schedule and a role change is not a source of errors.
	// Repeating the check here would be the very duplication this exists to
	// remove.
	off, err := l.EarliestOffsetAfterTimestamp(timestamp() - int64(l.Options.LocalRetentionAge))
	if err != nil {
		return errors.Wrap(err, "local retention horizon")
	}
	if off <= 0 {
		// 0 means the oldest record in the log is already at or after the
		// horizon, so nothing is old enough. Not an error, and not a no-op worth
		// reporting.
		return nil
	}
	_, err = l.OffloadBefore(off)
	return err
}

func (l *commitLog) cleanPass(spec CleanSpec) (int64, error) {
	l.cleanMu.Lock()
	defer l.cleanMu.Unlock()
	l.mu.RLock()
	oldSegments := l.segments
	l.mu.RUnlock()
	// A read-only tier is left entirely alone, whatever the spec asked for: a
	// rewrite and a tier retention delete are both writes to a store this log
	// does not own. Per tier, because ownership is — a node can own its hot
	// tier and not the archive under it, and one flag for the chain would make
	// it give up compacting what it does own. Local compaction proceeds exactly
	// as usual either way.
	spec.skipTiers = l.readOnlyTierSet()
	// Reclaim what EARLIER passes superseded, before this pass adds to the queue.
	// Deliberately first: the objects queued below are the ones this pass is
	// still installing, and the manifest that stops naming them is not written
	// until the end of it.
	l.drainReclaim()

	// Collected per pass, under cleanMu, so the entries queued belong to this
	// call.
	l.compactCleaner.superseded = nil
	cleaned, verified, cleanErr := l.clean(spec, oldSegments)
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
	if l.segmentsClosed {
		// The log closed while this pass was rewriting, which it does outside
		// l.mu. Installing now would hand the log a set of freshly built
		// segments that closeSegments has already walked past, so nothing would
		// ever close them — the same leak a roll racing a close used to cause.
		// Close what this pass built instead; close() is idempotent, so the
		// segments carried over unchanged from the old set are unaffected.
		for _, segment := range cleaned {
			_ = segment.Close()
		}
		l.mu.Unlock()
		l.queueReclaim(superseded)
		return -1, ErrCommitLogClosed
	}
	newSegments := l.segments
	if len(newSegments) > len(oldSegments) {
		// New segments were added while cleaning. Rebase the new segments onto
		// the cleaned ones.
		rebase := newSegments[len(oldSegments):]
		cleaned = l.rebaseSegments(rebase, cleaned)
	}
	l.segments = cleaned
	// Move the epoch cache's floor up to what survived, and NOTHING else. A
	// clean removes records; it does not renumber them and it does not change
	// when a leadership began, so every entry the cache holds is still true.
	// The only entries that need touching are the ones anchored below the
	// surviving floor, and ClearEarliest re-anchors the newest of those at the
	// floor rather than dropping it.
	//
	// Compaction used to Replace() the whole cache with one the compactor
	// rebuilt from the per-record epoch stamps of the surviving records. That
	// cache could only ever be a SUBSET: on a leader nothing stamps a record at
	// all — the only writer that does is the follower path taking a leader's
	// framing verbatim, while Append writes 0 and NewLeaderEpoch writes to the
	// checkpoint and nowhere else. So one ordinary maintenance pass took the
	// log's epoch to 0, and downstream that epoch is the replication fence:
	// every follower of a compacted stream was refused, truncated, refused
	// again, and could not rejoin the in-sync set. Even where records DO carry
	// stamps the rebuild was the worse answer, since it anchors an epoch at the
	// first SURVIVING record carrying it rather than where leadership actually
	// started.
	err := l.leaderEpochCache.ClearEarliest(l.segments[0].BaseOffset)
	l.mu.Unlock()

	// Placement, once the pass's own segment list is installed: a move repoints
	// a segment the log is serving, so it has to be the segment the log will
	// still be serving afterwards, not one this pass replaced.
	//
	// Its error is held rather than returned, exactly like cleanErr below. A
	// move that stopped partway has already published a manifest or two, and
	// the republish underneath is what puts every tier back into agreement —
	// including clearing the MovedFrom marker of any move that did complete.
	placed, moveErr := l.movePlaced(spec.TierPlacement)
	superseded = append(superseded, placed...)

	// Republish what the tier now holds. A pass can rewrite a segment onto new
	// objects and retention can drop others, so the manifest is stale the
	// moment either happens — and a stale manifest is worse than none, since it
	// names objects a reader would then fail to open.
	// Unconditional now that manifests are per tier: writeTierManifest skips
	// the read-only ones itself, so a chain with one owned tier and one it does
	// not own republishes the half it may. A read-only tier takes ZERO store
	// writes, a manifest Put is a store write like any other, and nothing in it
	// changed, so its manifest is still accurate anyway.
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

	// Queued AFTER the manifest, so an entry is only ever considered for
	// deletion once a published manifest has stopped naming it.
	l.queueReclaim(superseded)

	// A partial retention failure (cleanErr) still swapped in the surviving
	// segments above; report it once the read path is consistent.
	if cleanErr != nil {
		return -1, cleanErr
	}
	// Reported after the republish for the same reason: a move that stopped
	// partway leaves the log serving whichever tier it got as far as, and the
	// caller sees the failure once every manifest agrees with that.
	if moveErr != nil {
		return -1, moveErr
	}
	return verified, err
}

// rebaseSegments adds the segments in from to the end of the slice of segments
// in to. It has no epoch work to do: the pass never rewrites the live epoch
// cache, so the entries covering these segments are still in it untouched.
func (l *commitLog) rebaseSegments(from, to []*segment) []*segment {
	return append(to, from...)
}

// clean returns the cleaned segments and the pass's verified floor (see
// CleanWithSpec; -1 when compaction did not run).
func (l *commitLog) clean(spec CleanSpec, segments []*segment) ([]*segment, int64, error) {
	// Offloaded segments used to be held aside here as an immutable prefix,
	// because the rewriters build a local working segment and rewrite in place
	// and an offloaded segment has no local file to rewrite. That exclusion is
	// why whatever garbage a segment held when it offloaded was frozen there
	// permanently — a tombstone that offloaded before it could be collected
	// never took effect, and every value it shadowed was kept with it.
	//
	// They are no longer excluded. A rewrite of an offloaded segment becomes the
	// fresh store objects instead (see uploadReplacement and swapReplacement),
	// and retention is per tier, so their bytes count toward the tier's budget
	// rather than escaping every limit.
	cleaned, err := l.deleteCleaner.Clean(segments, spec.skipTiers, spec.RetentionFloor)
	if err != nil {
		// A partial retention failure still hands back the surviving
		// segments; propagate them so the caller swaps them in — the deleted
		// prefix must leave the read path even when the clean errs.
		return cleaned, -1, err
	}
	verified := int64(-1)
	if l.Compact {
		// spec is a value, so this resolution is this pass's alone.
		spec.ceiling = spec.Ceiling.Or(l.HighWatermark())
		compacted, v, err := l.compactCleaner.CompactSpec(spec, cleaned)
		if err != nil {
			if compacted == nil {
				// The pass failed before it rewrote anything (loading the
				// digests, or the merge). Keep the delete stage's result: its
				// removals are already on disk regardless.
				return cleaned, -1, err
			}
			// It failed part-way, and what it hands back is the list it got as
			// far as: rewrites where they landed, sources everywhere else. That
			// is what the log is HOLDING — a rewrite has already renamed itself
			// over its source's files — so it is what the caller must swap in.
			// Returning the delete stage's list instead republished the closed
			// sources and left the replacements named by nothing, which is how
			// a Close that reported success left an index mapped.
			return compacted, -1, err
		}
		cleaned, verified = compacted, v
	} else if consolidated, err := consolidateSegments(cleaned, spec.maxRewrites, spec.RewriteBudget); err != nil {
		// Non-compacted logs still owe block-layout maintenance: their
		// per-append tiny blocks otherwise accumulate blockRef memory and
		// open-time header walks forever (an uncompacted, append-heavy log
		// was observed gathering 16k-block segments across a long-running
		// soak). The consolidation-only pass rewrites records
		// VERBATIM — content, offsets and epochs untouched — into
		// cleanBlockTarget-sized blocks, budgeted like compaction rewrites.
		//
		// The PARTIAL list, for the reason the compaction branch above gives:
		// consolidateSegments stops at the failure and hands back rewrites where
		// they landed and sources everywhere else, and a rewrite has already
		// renamed itself over its source's files. Returning the delete stage's
		// list instead republishes the closed sources and leaves the replacements
		// named by nothing — reads keep working through current()'s redirect, so
		// the only symptom is an index mapped until the process exits and a data
		// directory that will not remove.
		//
		// consolidateSegments grew its half of this fix when the compaction path
		// did; this call site kept discarding the result, so the half that
		// shipped was the half nothing could observe.
		return consolidated, -1, err
	} else {
		cleaned = consolidated
	}
	// Its own stage, after both branches, because it belongs to both: compaction
	// and consolidation are the two arms of `if l.Compact`, and a join placed in
	// either would only ever reach half the logs. Both accumulate segments —
	// compaction because a rewrite only ever shrinks one, consolidation because
	// it never moves a boundary at all.
	//
	// Budgeted AFTER their debt, from a budget of its own: the two arms above
	// have already spent theirs, and the ordering that matters is the one inside
	// this pass — a rewrite that reclaims bytes runs before a join that reclaims
	// file handles. A shared counter would express the same order by starving the
	// join whenever compaction had work, which is not the same thing.
	joined, joinSuperseded, joinErr := joinSegments(
		cleaned, spec, newRewriteBudget(spec.maxRewrites, spec.RewriteBudget),
		tierJoin{writable: l.tierWritable, commit: l.commitJoinedRun},
	)
	// Onto the cleaner's pile rather than returned separately: the pass drains
	// that one under cleanMu and queues it AFTER the manifest republish, which is
	// exactly the deferral a joined-away object needs. Appended even on the error
	// path below, because the stage collapses several runs and the ones that
	// COMMITTED before the failure have superseded objects that are this log's
	// garbage. The failing run itself contributes nothing — a run only hands back
	// reclaim entries once its swap has repointed the segment away from them.
	l.compactCleaner.superseded = append(l.compactCleaner.superseded, joinSuperseded...)
	if joinErr != nil {
		// The partial list, for the reason the two stages above give at length: a
		// committed join has already renamed itself over its first input.
		return joined, verified, joinErr
	}
	return joined, verified, nil
}
