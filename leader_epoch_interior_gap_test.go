package commitlog

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// The invariant every absent-epoch decision rests on: no mutator in this package
// can remove an epoch from the MIDDLE of the recorded set.
//
// It matters because a probe for an epoch the cache has no entry for is two
// completely different situations, and they want opposite answers. If the epoch
// is missing because retention ate it, the prober is merely stale and must keep
// its records. If it is missing because this node never held that tenure, the
// prober's records under it came from somewhere else and must go. The only thing
// that tells those apart is WHERE the missing epoch falls: retention works from
// the earliest end, so a hole strictly between two survivors cannot be its doing.
//
// That reasoning is only worth anything if the mutators actually behave that way,
// which is what this pins. Gaps in the epoch NUMBERS are ordinary and expected --
// a node records only the tenures it took part in, so 2,4,6 with nothing between
// is the normal shape. What must never happen is an epoch that WAS recorded
// disappearing from between two that still are.
//
// In-memory on purpose: flush() early-returns on an empty checkpointFile, so this
// sweeps the mutator logic without thousands of atomic file writes. The file path
// is covered by TestLeaderEpochCache.
func TestNoMutatorCanRemoveAnEpochFromTheMiddle(t *testing.T) {
	seed := []epochOffset{{2, 10}, {4, 20}, {6, 30}, {8, 40}, {10, 50}}

	build := func(t *testing.T) *leaderEpochCache {
		l := &leaderEpochCache{name: "gap"}
		for _, e := range seed {
			require.NoError(t, l.Assign(e.leaderEpoch, e.assignedAtOffset))
		}
		require.Len(t, l.epochOffsets, len(seed), "the fixture must record every epoch")
		return l
	}

	// Every boundary worth probing: one below, on, and one above each seeded
	// offset, plus the ends.
	offsets := []int64{-1, 0}
	for _, e := range seed {
		offsets = append(offsets, e.assignedAtOffset-1, e.assignedAtOffset, e.assignedAtOffset+1)
	}
	offsets = append(offsets, 60)

	check := func(t *testing.T, l *leaderEpochCache, what string) {
		t.Helper()
		live := l.epochOffsets
		if len(live) == 0 {
			return
		}

		// findEpoch is a sort.Search, so an out-of-order cache would not fail
		// loudly -- it would answer the wrong entry. Assert the ordering the
		// search assumes while we are here.
		for i := 1; i < len(live); i++ {
			require.Greater(t, live[i].leaderEpoch, live[i-1].leaderEpoch,
				"%s left the epochs out of order, which silently breaks findEpoch", what)
			require.GreaterOrEqual(t, live[i].assignedAtOffset, live[i-1].assignedAtOffset,
				"%s left the offsets out of order", what)
		}

		var (
			lo   = live[0].leaderEpoch
			hi   = live[len(live)-1].leaderEpoch
			have = make(map[uint64]bool, len(live))
		)
		for _, e := range live {
			have[e.leaderEpoch] = true
		}
		for _, e := range seed {
			if e.leaderEpoch > lo && e.leaderEpoch < hi && !have[e.leaderEpoch] {
				t.Fatalf("%s removed epoch %d from between surviving epochs %d and %d; "+
					"an interior gap now means two different things and the probe "+
					"cannot tell them apart", what, e.leaderEpoch, lo, hi)
			}
		}
	}

	for _, a := range offsets {
		for _, b := range offsets {
			t.Run(fmt.Sprintf("earliest_%d_then_latest_%d", a, b), func(t *testing.T) {
				l := build(t)
				require.NoError(t, l.ClearEarliest(a))
				check(t, l, fmt.Sprintf("ClearEarliest(%d)", a))
				require.NoError(t, l.ClearLatest(b))
				check(t, l, fmt.Sprintf("ClearEarliest(%d) then ClearLatest(%d)", a, b))
			})

			t.Run(fmt.Sprintf("latest_%d_then_earliest_%d", b, a), func(t *testing.T) {
				l := build(t)
				require.NoError(t, l.ClearLatest(b))
				check(t, l, fmt.Sprintf("ClearLatest(%d)", b))
				require.NoError(t, l.ClearEarliest(a))
				check(t, l, fmt.Sprintf("ClearLatest(%d) then ClearEarliest(%d)", b, a))
			})
		}
	}
}

// ClearEarliest re-anchors the HIGHEST epoch it removes, and that specific
// choice is what keeps retention from ever opening an interior gap.
//
// The removed set is a prefix in ascending epoch order, so re-adding
// earliest[len(earliest)-1] puts the top of that run back at the retention floor.
// Every epoch retention actually destroys is therefore strictly BELOW the
// earliest surviving entry's number -- there is no epoch left over that could sit
// above it and form a hole.
//
// Written against the mechanism rather than only the outcome, but not because
// the outcome test is blind to it -- I assumed it was and the falsification said
// otherwise. Re-anchoring earliest[0] instead reddens the sweep above in 200-odd
// subtests: putting the LOWEST removed epoch back at the floor strands the higher
// ones (4, 6, 8) as genuine interior holes between surviving 2 and 10, which is
// precisely the state the rule cannot survive. Both tests fail on that mutation.
//
// This one still earns its place by naming WHICH epoch must come back, so the
// next reader does not have to derive it from 200 failing offsets.
func TestClearEarliestKeepsTheHighestEpochItRemoves(t *testing.T) {
	l := &leaderEpochCache{name: "reanchor"}
	require.NoError(t, l.Assign(5, 10))
	require.NoError(t, l.Assign(6, 20))
	require.NoError(t, l.Assign(7, 30))
	require.NoError(t, l.Assign(13, 100))

	// A floor above 5, 6 and 7 but below 13: all three are removed and one comes
	// back at the floor.
	require.NoError(t, l.ClearEarliest(50))

	require.Len(t, l.epochOffsets, 2)
	require.Equal(t, uint64(7), l.epochOffsets[0].leaderEpoch,
		"the highest removed epoch is the one re-anchored")
	require.Equal(t, int64(50), l.epochOffsets[0].assignedAtOffset,
		"and it is re-anchored at the retention floor")
	require.Equal(t, uint64(13), l.epochOffsets[1].leaderEpoch)

	// 5 and 6 are gone, and both are below the earliest survivor. Neither can be
	// mistaken for a tenure this node never held, because the rule reads the
	// epoch NUMBER against epochOffsets[0] and both fall below it.
	require.Less(t, uint64(5), l.epochOffsets[0].leaderEpoch)
	require.Less(t, uint64(6), l.epochOffsets[0].leaderEpoch)

	// And nothing was destroyed between 7 and 13, which is the space a probe for
	// an unheld tenure would land in.
	require.Equal(t, 2, len(l.epochOffsets))
}
