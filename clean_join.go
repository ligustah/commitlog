package commitlog

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
