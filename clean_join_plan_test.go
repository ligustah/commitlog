package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// planJoins decides which segments get collapsed together, and every rule it
// encodes has a failure mode that is silent rather than loud — a run that
// crosses a tier copies bytes between stores, a run with a hole produces a
// segment whose offset range contains records it does not hold. So the planner
// is tested on its own, against hand-built segments, rather than only through a
// pass that would need a store to exercise the tiered cases at all.
//
// The segments here are literals: planJoins reads Position, isOffloaded and
// tier, and nothing else. Building real ones would need files, a store, and a
// full log for each case, and would test the fixture rather than the rule.
func planSeg(base, size int64, tier string) *segment {
	s := &segment{BaseOffset: base, position: size}
	if tier != "" {
		s.tier = tier
		s.store = stubJoinStore{}
	}
	return s
}

// stubJoinStore only has to make isOffloaded answer true; planJoins never calls
// through it.
type stubJoinStore struct{ SegmentStore }

func plannedRanges(runs []joinRun) [][2]int {
	out := make([][2]int, 0, len(runs))
	for _, r := range runs {
		out = append(out, [2]int{r.first, r.last})
	}
	return out
}

func TestPlanJoinsGroupsAdjacentSegmentsWithinTheCap(t *testing.T) {
	for _, tc := range []struct {
		name string
		segs []*segment
		spec CleanSpec
		want [][2]int
	}{
		{
			// The active (last) segment is never an input: it is still being
			// appended to, so its extent is not settled and joining it would move
			// records out from under a writer holding its tail.
			name: "the active segment is never joined",
			segs: []*segment{planSeg(0, 10, ""), planSeg(1, 10, ""), planSeg(2, 10, "")},
			spec: CleanSpec{JoinBelow: 100},
			want: [][2]int{{0, 1}},
		},
		{
			name: "nothing is joined without configuration",
			segs: []*segment{planSeg(0, 10, ""), planSeg(1, 10, ""), planSeg(2, 10, "")},
			spec: CleanSpec{},
			want: [][2]int{},
		},
		{
			// Greedy and left-to-right: the run grows while the total fits, then
			// the next segment starts a fresh one rather than being dropped.
			name: "a run stops at the cap and the next one starts",
			segs: []*segment{
				planSeg(0, 40, ""), planSeg(1, 40, ""), planSeg(2, 40, ""),
				planSeg(3, 40, ""), planSeg(4, 0, ""),
			},
			spec: CleanSpec{JoinBelow: 100},
			want: [][2]int{{0, 1}, {2, 3}},
		},
		{
			// A segment already at the cap cannot be in any run, and it ENDS the
			// run before it rather than being skipped: a run is adjacent by
			// definition, and jumping over it would leave the result claiming
			// offsets it does not hold.
			name: "an oversized segment breaks the run rather than being skipped",
			segs: []*segment{
				planSeg(0, 10, ""), planSeg(1, 10, ""), planSeg(2, 500, ""),
				planSeg(3, 10, ""), planSeg(4, 10, ""), planSeg(5, 0, ""),
			},
			spec: CleanSpec{JoinBelow: 100},
			want: [][2]int{{0, 1}, {3, 4}},
		},
		{
			// A join that crossed a tier boundary would copy bytes between
			// stores, which is not an optimisation. The boundary breaks the run
			// exactly as an oversized segment does.
			name: "a run never crosses a tier boundary",
			segs: []*segment{
				planSeg(0, 10, "hot"), planSeg(1, 10, "hot"),
				planSeg(2, 10, "cold"), planSeg(3, 10, "cold"),
				planSeg(4, 0, ""),
			},
			spec: CleanSpec{TierJoinBelow: map[string]int64{"hot": 100, "cold": 100}},
			want: [][2]int{{0, 1}, {2, 3}},
		},
		{
			name: "local and tiered segments never share a run",
			segs: []*segment{
				planSeg(0, 10, "hot"), planSeg(1, 10, "hot"),
				planSeg(2, 10, ""), planSeg(3, 10, ""),
				planSeg(4, 0, ""),
			},
			spec: CleanSpec{JoinBelow: 100, TierJoinBelow: map[string]int64{"hot": 100}},
			want: [][2]int{{0, 1}, {2, 3}},
		},
		{
			// Absence is how a read-only tier stays untouched, so it must not
			// inherit anything. This is the one place the join config differs
			// from TierBudgets, which DOES fall back.
			name: "an unconfigured tier is not joined and does not inherit the local cap",
			segs: []*segment{
				planSeg(0, 10, "archive"), planSeg(1, 10, "archive"),
				planSeg(2, 10, ""), planSeg(3, 10, ""),
				planSeg(4, 0, ""),
			},
			spec: CleanSpec{JoinBelow: 100},
			want: [][2]int{{2, 3}},
		},
		{
			name: "a lone joinable segment is not a run",
			segs: []*segment{
				planSeg(0, 500, ""), planSeg(1, 10, ""), planSeg(2, 500, ""),
				planSeg(3, 0, ""),
			},
			spec: CleanSpec{JoinBelow: 100},
			want: [][2]int{},
		},
		{
			name: "a log with nothing sealed enough to pair",
			segs: []*segment{planSeg(0, 10, ""), planSeg(1, 0, "")},
			spec: CleanSpec{JoinBelow: 100},
			want: [][2]int{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := plannedRanges(planJoins(tc.segs, tc.spec))
			require.Equal(t, tc.want, got)
		})
	}
}

// The result's size must be the sum of what goes into it: records are copied
// verbatim, so a run that reported less than it will produce would be a cap that
// does not bound anything.
func TestPlanJoinsReportsTheResultSize(t *testing.T) {
	segs := []*segment{
		planSeg(0, 30, ""), planSeg(1, 25, ""), planSeg(2, 20, ""), planSeg(3, 0, ""),
	}
	runs := planJoins(segs, CleanSpec{JoinBelow: 100})
	require.Len(t, runs, 1)
	require.Equal(t, int64(75), runs[0].bytes)
	require.Equal(t, 3, runs[0].len())
	require.False(t, runs[0].tiered)
}

// Every run a plan returns is work worth doing, so a consumer never has to
// re-check the things the planner already decided.
func TestPlanJoinsReturnsOnlyRunsWorthDoing(t *testing.T) {
	segs := make([]*segment, 0, 40)
	for i := range 39 {
		tier := ""
		if i%3 == 0 {
			tier = "hot"
		}
		segs = append(segs, planSeg(int64(i), int64(10+i%7), tier))
	}
	segs = append(segs, planSeg(39, 0, ""))

	runs := planJoins(segs, CleanSpec{
		JoinBelow:     40,
		TierJoinBelow: map[string]int64{"hot": 40},
	})
	require.NotEmpty(t, runs)
	for _, r := range runs {
		require.Greater(t, r.len(), 1, "a run of one is not a join")
		require.LessOrEqual(t, r.bytes, int64(40), "a run may not exceed its cap")
		require.Less(t, r.last, len(segs)-1, "the active segment must never be an input")
		var sum int64
		for i := r.first; i <= r.last; i++ {
			tier, tiered := joinTier(segs[i])
			require.Equal(t, r.tiered, tiered, "run %v mixes local and tiered", r)
			require.Equal(t, r.tier, tier, "run %v crosses a tier boundary", r)
			sum += segs[i].Position()
		}
		require.Equal(t, r.bytes, sum, "run %v misreports its result size", r)
	}
	// Runs are disjoint and ascending, or the splice would try to retire a
	// segment twice.
	for i := 1; i < len(runs); i++ {
		require.Greater(t, runs[i].first, runs[i-1].last,
			"runs %v and %v overlap", runs[i-1], runs[i])
	}
}
