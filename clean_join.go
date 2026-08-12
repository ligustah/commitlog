package commitlog

import (
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/pkg/errors"
)

// Segment join: replacing a run of adjacent sealed segments with one segment
// holding all their records, verbatim.
//
// Compaction only ever SHRINKS a segment — a rewrite keeps its predecessor's
// base offset and replaces it in place — so nothing ever merges two segments
// back into one and a long-lived log converges on many small ones. The bytes are
// fine; the count is the cost: a file set, an open handle, an index mapping and
// a slot in every full walk over l.segments, per segment, forever.
//
// Why this is not just another rewrite, and the whole reason it needs its own
// path: a segment is IDENTIFIED by its base offset — the file-name stem, the
// tier manifest key, the sidecar derivation, and the reader's "which segment
// holds offset N" search. A rewrite never disturbs that, which is exactly what
// lets Replace swap one under live readers. A join means one base offset ceases
// to exist, and every one of those has to agree about it at a single moment.
//
// See docs/segment-join.md for the hazards and the ordered sketch.

// joinRun is one group of ADJACENT sealed segments that will be replaced by a
// single segment carrying every record in them.
//
// Adjacency is not a convenience. The result spans the whole offset range from
// its first input's base to its last input's end, so a run that skipped over a
// segment in the middle would produce a segment whose range CONTAINS offsets it
// does not hold — and findSegment, which bounds only from above, would resolve
// those offsets to it and find nothing.
type joinRun struct {
	// first and last are indices into the pass's segment slice, inclusive.
	first, last int
	// tier is the store every input shares, and tiered says whether they are in
	// one at all. A run never crosses that boundary: a join is an optimisation,
	// and one that copies bytes between stores is not one.
	tier   string
	tiered bool
	// bytes is the result's logical size — the sum of the inputs'. Records are
	// copied verbatim, so nothing is dropped and the sum is exact.
	bytes int64
}

func (r joinRun) len() int { return r.last - r.first + 1 }

// joinTier reports the store a segment's bytes live in, and whether they live in
// one at all.
//
// Two returns rather than a name with "" meaning local. The empty string is a
// real map key, and a sentinel made of the invalid value is how this repo
// already shipped a bug: "" meant "all tiers" on one path and "nothing valid
// arrived" everywhere else. A caller that must not confuse "the local segments"
// with "the tier whose name I failed to read" gets a bool instead.
//
// Read bare, as compact_cleaner.go reads the same two fields: both are set when
// the segment attaches to a store and not touched again, and the pass holds no
// segment lock here.
func joinTier(s *segment) (string, bool) {
	if !s.isOffloaded() {
		return "", false
	}
	return s.tier, true
}

// joinCapFor is the largest a joined result may be for where these bytes live,
// or 0 for "do not join here".
//
// NOTE the difference from TierBudgets, which falls back to RewriteBudget for a
// tier with no entry. This does NOT fall back, and the difference is
// load-bearing: an unconfigured tier must be left alone, because that is how a
// READ-ONLY tier stays untouched without needing to be named separately. A
// fallback here would join into a store the log may not write to.
func (s CleanSpec) joinCapFor(tier string, tiered bool) int64 {
	if !tiered {
		return s.JoinBelow
	}
	return s.TierJoinBelow[tier]
}

// planJoins groups the pass's sealed segments into the runs a join would
// collapse. Greedy and left-to-right: a run grows while the next segment sits in
// the same store and the total stays within that store's cap.
//
// The ACTIVE segment is never included. It is still being appended to, so its
// extent is not settled, and joining it would move records out from under a
// writer holding its tail.
//
// A segment that cannot be joined at all — no configuration for its store, or
// already at the cap by itself — ENDS the run before it rather than being
// skipped over, because a run is adjacent by definition.
func planJoins(segments []*segment, spec CleanSpec) []joinRun {
	if len(segments) < 3 {
		// At most one sealed segment, so nothing to join it to.
		return nil
	}
	var (
		runs []joinRun
		cur  joinRun
		open bool
	)
	// A run of one is not a join. Dropping it here rather than at the call site
	// keeps "a returned run is work worth doing" true for every consumer.
	flush := func() {
		if open && cur.len() > 1 {
			runs = append(runs, cur)
		}
		open = false
	}
	for i, seg := range segments[:len(segments)-1] {
		var (
			tier, tiered = joinTier(seg)
			cap          = spec.joinCapFor(tier, tiered)
			size         = seg.Position()
		)
		if cap <= 0 || size >= cap {
			flush()
			continue
		}
		if open && cur.tiered == tiered && cur.tier == tier && cur.bytes+size <= cap {
			cur.last, cur.bytes = i, cur.bytes+size
			continue
		}
		flush()
		cur, open = joinRun{first: i, last: i, tier: tier, tiered: tiered, bytes: size}, true
	}
	flush()
	return runs
}

// joinOne builds the single segment that carries every record of one run and
// installs it, returning the result. inputs must be the run's segments in offset
// order, and there must be at least two of them.
//
// # The commit point, and why the local case needs no new mechanism
//
// The result is built at the run's LOWEST base offset, so installing it is
// Replace over the run's first segment — the same rename-over-the-source the
// rewrite path uses, and the same instant of commit. What is new is only that
// the result spans further than the segment it replaced, which makes every input
// ABOVE the first a strict subset of it.
//
// That is what makes a crash between the rename and the disposal below safe, and
// it is worth being precise about because the alternative — retiring the inputs
// first — is the one that loses records:
//
//   - Before the rename: the working copy is a suffixed file open() does not
//     look at, and every input is untouched. The join simply did not happen.
//   - After it: the joined bytes are under the first input's name, and inputs
//     2..n are segments whose offset range is entirely contained in it. That is
//     the state resolveSegmentOverlaps already exists to clear, and it clears it
//     the only way that is safe — keeping the superset and deleting the
//     contained duplicate. It was written for an interrupted truncation; a join
//     produces the identical shape, and deliberately so.
//
// A crash therefore resolves to the old set or the new single one, and never to
// an offset served by two segments — which is the property hazard 2 of
// docs/segment-join.md asks for, obtained here for free. The TIERED case has no
// equivalent, because a store has no rename; that is a separate commit point and
// a separate slice.
//
// bw and sc stay owned by the caller for the reason consolidateOne gives: one
// block writer and one decode-buffer pair serve the whole pass.
func joinOne(inputs []*segment, bw *blockWriter, sc *blockCache) (*segment, error) {
	first := inputs[0]
	// Refused rather than assumed, because the failure would be quiet and awful:
	// an offloaded segment has no local log to rename over, so Replace below
	// would install this copy under a name the segment does not read from and
	// retire the inputs anyway. joinSegments does not offer tiered runs — this is
	// here so that a later caller cannot wander in before the tiered commit point
	// exists.
	for _, in := range inputs {
		if in.isOffloaded() {
			return nil, errors.Errorf("commitlog: segment %d is offloaded; "+
				"a tiered join commits through the manifest, not a rename",
				in.BaseOffset)
		}
	}
	joined, err := first.Joined()
	if err != nil {
		return nil, err
	}
	// Disposed of on every way out until it is installed, and by the same
	// discriminator the two rewrite paths use: Replace clears the suffix at the
	// moment the copy stops being a copy and becomes the segment, so a suffix
	// still set means deleting it unlinks nothing anyone can reach. Left behind,
	// it holds a handle and an index mapping until the process exits.
	defer func() {
		joined.RLock()
		working := joined.suffix != ""
		joined.RUnlock()
		if working {
			joined.Delete() // nolint: errcheck — best effort on a failure path
		}
	}()
	bw.reset(joined)
	// One scanner per input, all closed AFTER the install. Closing each as its
	// scan ends would be tidier and is wrong: what Close releases is the tiered
	// backing's pin, and dropping it early lets drainReclaim judge a superseded
	// object unreferenced and delete it while this join is still reading the
	// segments beside it. The read STREAM is not the concern — Scan closes that
	// itself the moment it ends, which is what keeps an open read handle from
	// blocking the rename below on Windows.
	scanners := make([]*segmentScanner, 0, len(inputs))
	defer func() {
		for _, ss := range scanners {
			ss.Close() // nolint: errcheck — read-only
		}
	}()
	for _, in := range inputs {
		ss := newSegmentScannerCache(in, sc)
		scanners = append(scanners, ss)
		for {
			ms, _, err := ss.Scan()
			if err != nil {
				// A join reads every input to its end and then DELETES it, so a
				// walk that stopped early would write a prefix and unlink the file
				// holding the rest. Loudest of all the scan sites for that reason:
				// the rewrite paths at least leave the damaged bytes under the
				// source's name, and this one collects them.
				if !errors.Is(err, io.EOF) {
					return nil, fmt.Errorf("%w: join of segment %d: %w",
						ErrSegmentUnreadable, in.BaseOffset, err)
				}
				break
			}
			if err := bw.add(ms); err != nil {
				return nil, err
			}
		}
	}
	if err := bw.flush(); err != nil {
		return nil, err
	}
	if err := joined.Sync(); err != nil {
		return nil, err
	}
	// Whether this log keeps key digests at all, read while the path still names
	// the first input's. Rebuilding one costs a full scan of what was just
	// written, so it is done for a log that has them and not for one that does
	// not — a non-compacted log has never had a digest and has no use for one.
	wantDigest := exists(digestPath(first))
	if err := joined.Replace(first); err != nil {
		return nil, err
	}
	// ---- past the commit point ----
	//
	// Everything below is best-effort with a warning, and that is a considered
	// position rather than the usual one. The records are all in `joined` and it
	// is installed; the caller must publish it. Returning an error here would
	// abandon a pass that has already committed, which is the shape that leaves
	// rewrites named by nothing. Every failure below also self-corrects: what it
	// leaves behind is a contained duplicate or a digest that fails its own
	// staleness check, and both are cleaned up without anyone being told.
	retireJoinedInputs(inputs[1:], joined)
	if wantDigest {
		refreshJoinedDigest(joined, sc)
	}
	return joined, nil
}

// joinSegments is the join stage of a clean pass: it collapses every run worth
// collapsing and returns the segment list the caller must publish.
//
// Returning the list IS the splice, and that is the whole reason the stage has
// this shape rather than mutating l.segments as it goes. The caller swaps the
// returned slice in under the segment write lock, once, at the end of the pass —
// so a run's inputs all leave and its result arrives in the same instant. A
// window in which the list named both would be observable: the replacement link
// a join sets is MANY-to-one, and LocalBytes resolves every entry through it and
// SUMS Position(), so it would count the result once per input and report a log
// using more local bytes than it does.
//
// Budgeted as a rewrite, because that is what it costs — every byte of the run
// is read and written again. It runs after compaction's own debt for a reason
// worth stating: reclaiming bytes beats reclaiming file handles, so a pass with
// budget for one rewrite should spend it on the segment holding garbage rather
// than on the two that are merely small.
func joinSegments(segments []*segment, spec CleanSpec, budget *rewriteBudget) ([]*segment, error) {
	runs := planJoins(segments, spec)
	if len(runs) == 0 {
		return segments, nil
	}
	var (
		out  = make([]*segment, 0, len(segments))
		bw   = &blockWriter{}
		sc   = newBlockCache() // one decode-buffer pair for the whole stage
		next = 0               // the first segment not yet accounted for in out
	)
	for _, r := range runs {
		// A tiered run is planned but not executed yet: its commit point is a
		// manifest write that adds the result and removes every input together,
		// and until that exists a tiered run must be left exactly alone. Planned
		// anyway so the planner has one set of rules rather than a local set and
		// a tiered set that drift.
		if r.tiered || !budget.allow() {
			continue
		}
		out = append(out, segments[next:r.first]...)
		joined, err := joinOne(segments[r.first:r.last+1], bw, sc)
		if err != nil {
			// The PARTIAL list, for the reason both rewrite stages give: a join
			// that already committed has renamed itself over its first input and
			// retired the rest, so the list the log is HOLDING is the one with
			// this stage's results in it. Republishing the input list instead
			// would name closed segments and leave the results named by nothing.
			// Everything from this run on is untouched and carries over as it
			// stands, including the active segment.
			return append(out, segments[r.first:]...), err
		}
		out = append(out, joined)
		next = r.last + 1
		budget.note()
	}
	return append(out, segments[next:]...), nil
}

// retireJoinedInputs disposes of the inputs a join did NOT rename over.
//
// SupersededBy before Delete, and the order is the point: Delete closes the
// segment, and a reader that resolved into it between the close and the link
// would get the raw ErrSegmentClosed the link exists to turn into a redirect.
// The link may point at a segment with a DIFFERENT base offset — the first time
// anything does — which is safe for the narrow reason docs/segment-join.md
// records: every consumer of current() bounds the resolved segment only from
// above, and a join's result is a superset of its inputs.
func retireJoinedInputs(inputs []*segment, joined *segment) {
	for _, in := range inputs {
		in.SupersededBy(joined)
		if err := in.Delete(); err != nil {
			// The segment is out of the list and marked as left either way, so
			// nothing reads it. Its files are a duplicate contained in `joined`,
			// which resolveSegmentOverlaps deletes on the next open.
			slog.Warn("commitlog: a joined-away segment's files could not be "+
				"removed; the next open will drop them as a contained duplicate",
				slog.Int64("base_offset", in.BaseOffset),
				slog.String("err", err.Error()))
		}
	}
}

// refreshJoinedDigest replaces the key digest a join invalidated.
//
// The first input's digest is still sitting at the joined segment's path — the
// digest name carries no suffix, so the two segments share it — and it now
// describes a fraction of the records the segment holds. It cannot be believed
// and is not: loadKeyDigest rejects a digest whose logSize disagrees with the
// segment's position, and a join always grows the segment. So the stale file is
// inert rather than dangerous, and this is a cost optimisation — a rebuilt
// digest saves the next clean the scan it would otherwise make.
//
// Removed before the rebuild rather than counting on the overwrite, so that a
// rebuild which fails leaves no digest at all instead of one that will be read,
// rejected and rebuilt on every tick forever.
//
// The rebuilt digest carries NO strip stamp, which buildKeyDigest's -1 gives it
// for free and which is the only answer available: the inputs each carried their
// own stamp, verified under their own StripBelow, and a stamp is a claim about
// every record in the segment. Adopting either one would extend it over records
// it never covered, so the run's result is unverified until a clean verifies it.
// The cost is one scan on some later pass; the alternative is a stamp that lies.
func refreshJoinedDigest(joined *segment, sc *blockCache) {
	if err := os.Remove(digestPath(joined)); err != nil && !os.IsNotExist(err) {
		slog.Warn("commitlog: could not remove the stale key digest a join "+
			"invalidated; it will be rejected and rebuilt on every clean",
			slog.Int64("base_offset", joined.BaseOffset),
			slog.String("err", err.Error()))
		return
	}
	d, err := buildKeyDigest(joined, sc)
	if err == nil {
		err = writeKeyDigest(joined, d)
	}
	if err != nil {
		slog.Warn("commitlog: key digest rebuild after a join failed; the next "+
			"clean will rebuild it",
			slog.Int64("base_offset", joined.BaseOffset),
			slog.String("err", err.Error()))
	}
}
