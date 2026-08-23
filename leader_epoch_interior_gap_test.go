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

// All four things a probe can name, in one table, because the rule is a split
// and a per-case fixture would let one arm drift without the others noticing.
//
// The two unchanged rows are here deliberately: the change is narrow, and what
// makes it safe is precisely that below-earliest and above-latest still answer
// what they always did. A test that only covered the new arm could not tell a
// narrow fix from a wide one.
func TestWhereAnEpochIsMissingDecidesHowItIsAnswered(t *testing.T) {
	// Recorded: 2, 4, 6. Missing: everything else.
	build := func() *leaderEpochCache {
		l := &leaderEpochCache{name: "probe"}
		for _, e := range []epochOffset{{2, 10}, {4, 20}, {6, 30}} {
			if err := l.Assign(e.leaderEpoch, e.assignedAtOffset); err != nil {
				t.Fatal(err)
			}
		}
		return l
	}

	for _, tc := range []struct {
		name   string
		probe  uint64
		off    int64
		found  bool
		reason string
	}{
		{
			name: "recorded epoch answers from its successor", probe: 2,
			off: 20, found: true,
			reason: "epoch 2 ran until epoch 4 opened at 20",
		},
		{
			name: "recorded latest epoch has no successor", probe: 6,
			off: -1, found: false,
			reason: "nothing above 6 is recorded, so the caller substitutes the log end",
		},
		{
			name: "below the earliest recorded epoch answers from the earliest", probe: 1,
			off: 10, found: true,
			reason: "retention collapses entries under the floor; the prober is stale, not divergent",
		},
		{
			name: "interior gap answers from the epoch below", probe: 3,
			off: 10, found: true,
			reason: "this log spanned epoch 3 and never held it; ceiling would have said 20",
		},
		{
			name: "interior gap next to the latest still answers from below", probe: 5,
			off: 20, found: true,
			reason: "floor is 4 at 20; ceiling would have said 30",
		},
		{
			name: "above the latest recorded epoch is unanswerable", probe: 7,
			off: -1, found: false,
			reason: "the prober is ahead of this log and has nothing to discard",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			off, found := build().LastOffsetForLeaderEpoch(tc.probe)
			require.Equal(t, tc.found, found, tc.reason)
			require.Equal(t, tc.off, off, tc.reason)
		})
	}
}

// The regression the floor rule had to be narrowed around: an epoch retention
// destroyed must NOT be read as one this log never held.
//
// ClearEarliest collapses 5, 6 and 7 into a single entry for 7 at the floor, so
// 5 and 6 stop existing even though this log lived through them. Answering those
// from below — there is nothing below — would tell a merely-stale follower to
// discard its whole log. They fall under the earliest recorded epoch, so they
// keep the pre-existing answer instead.
func TestAnEpochRetentionDestroyedIsNotTreatedAsOneNeverHeld(t *testing.T) {
	l := &leaderEpochCache{name: "trimmed"}
	require.NoError(t, l.Assign(5, 10))
	require.NoError(t, l.Assign(6, 20))
	require.NoError(t, l.Assign(7, 30))
	require.NoError(t, l.Assign(13, 100))

	require.NoError(t, l.ClearEarliest(50))
	require.Equal(t, uint64(7), l.epochOffsets[0].leaderEpoch)

	for _, gone := range []uint64{5, 6} {
		off, found := l.LastOffsetForLeaderEpoch(gone)
		require.True(t, found, "epoch %d was destroyed by retention, not by divergence", gone)
		require.Equal(t, int64(50), off,
			"epoch %d must answer from the retention floor, not be treated as unheld", gone)
	}

	// And the epoch that genuinely was never held, in the space above the
	// earliest survivor, still gets the floor answer.
	off, found := l.LastOffsetForLeaderEpoch(9)
	require.True(t, found)
	require.Equal(t, int64(50), off, "floor for 9 is epoch 7, re-anchored at 50")
}

// The partition durable_streams lost, reduced to the cache that lost it.
//
// n3 held epoch 13, was deposed while n1 took 14, and was leader again at 15
// without ever recording 14. A probe for 14 against n3 used to be answered from
// epoch 15's anchor — which sat ABOVE the two divergent records — so the prober
// kept them and the fork became permanent.
func TestAProbeForATenureThisLogNeverHeldTruncatesToTheOneItDid(t *testing.T) {
	l := &leaderEpochCache{name: "n3"}
	require.NoError(t, l.Assign(13, 7000))
	require.NoError(t, l.Assign(15, 7439))

	off, found := l.LastOffsetForLeaderEpoch(14)
	require.True(t, found)
	require.Equal(t, int64(7000), off,
		"epoch 14 happened on another node; answering 7439 keeps the records at the fork")

	// The tenures this log DID hold are untouched by the new arm.
	off, found = l.LastOffsetForLeaderEpoch(13)
	require.True(t, found)
	require.Equal(t, int64(7439), off, "epoch 13 ran until 15 opened")

	off, found = l.LastOffsetForLeaderEpoch(15)
	require.False(t, found, "nothing above 15 is recorded")
	require.Equal(t, int64(-1), off)
}

// The floor answer is the floor epoch's ANCHOR, not the end of the floor epoch,
// so the floor epoch's own records are discarded too.
//
// This needs THREE recorded epochs to be visible at all, which is why it is a
// separate fixture. With only 13 and 15 recorded there is nothing below 13, so
// "13's anchor" and "the end of whatever preceded 13" are the same number and a
// test cannot tell which one the code returned. durable_streams read the
// changelog's "the last epoch actually held below it" as the END of 13 and asked
// whether that was intended; the phrase was wrong and this pins the answer the
// prose now describes.
//
// It has to be the anchor. The end of epoch 13 IS epoch 15's anchor -- the
// ceiling answer this release removed -- and that is exactly where a responder's
// own epoch-13 records collide with a prober's epoch-14 records at the same
// offsets. Stopping there keeps the fork.
func TestTheFloorAnswerIsTheAnchorNotTheEndOfTheFloorEpoch(t *testing.T) {
	l := &leaderEpochCache{name: "three"}
	require.NoError(t, l.Assign(11, 0))
	require.NoError(t, l.Assign(13, 1))
	require.NoError(t, l.Assign(15, 3))

	// Epoch 13's own records are offsets 2..3: from its anchor+1 up to and
	// including the anchor of the next recorded epoch.
	off, found := l.LastOffsetForLeaderEpoch(13)
	require.True(t, found)
	require.Equal(t, int64(3), off, "epoch 13 ran until 15 opened at 3")

	off, found = l.LastOffsetForLeaderEpoch(14)
	require.True(t, found)
	require.Equal(t, int64(1), off,
		"a probe for the unheld epoch 14 gets epoch 13's ANCHOR")
	require.NotEqual(t, int64(3), off,
		"3 is the END of epoch 13, which is epoch 15's anchor -- the ceiling "+
			"answer, and where the divergent records live")

	// The gap below the floor epoch behaves the same way: 12 is also unheld, and
	// also answers from 13's predecessor rather than from 13's end.
	off, found = l.LastOffsetForLeaderEpoch(12)
	require.True(t, found)
	require.Equal(t, int64(0), off, "epoch 12 is unheld; the floor below it is 11")
}

// A floor answer can never point below the oldest surviving record, which is
// what stops the wider truncation from becoming a whole-log wipe.
//
// The concern is specific to the floor rule: answering from BELOW deliberately
// truncates past the disputed range, and the caller obeys the offset it is given
// — Truncate(answer+1) on an offset under the log discards everything, because
// findSegment lands on the first segment and the rewrite keeps nothing. So the
// question the fix has to survive is whether an answer can sit under the
// retention floor.
//
// It cannot, and the reason is structural rather than arithmetic: every non-(-1)
// answer is some recorded entry's anchor, and retention re-anchors the earliest
// surviving entry AT the floor rather than leaving it below. So the smallest
// answer available is the floor itself. Pinned here because the property lives
// in two files -- the trim in this one, the truncation in commitlog.go -- and
// neither states it alone.
func TestNoAnswerPointsBelowTheOldestSurvivingRecord(t *testing.T) {
	l := &leaderEpochCache{name: "floorbound"}
	require.NoError(t, l.Assign(5, 10))
	require.NoError(t, l.Assign(6, 20))
	require.NoError(t, l.Assign(7, 30))
	require.NoError(t, l.Assign(13, 100))
	require.NoError(t, l.Assign(15, 140))

	const retentionFloor = 50
	require.NoError(t, l.ClearEarliest(retentionFloor))
	require.Equal(t, int64(retentionFloor), l.epochOffsets[0].assignedAtOffset,
		"retention re-anchors the earliest survivor AT the floor")

	// Every epoch anyone could name, well past both ends of what is recorded.
	for probe := uint64(0); probe <= 20; probe++ {
		off, found := l.LastOffsetForLeaderEpoch(probe)
		if !found {
			require.Equal(t, int64(-1), off,
				"probe %d: an unanswerable probe reports -1 and the caller substitutes the log end", probe)
			continue
		}
		require.GreaterOrEqual(t, off, int64(retentionFloor),
			"probe %d answered %d, below the oldest surviving record; the caller would "+
				"truncate to it and discard the whole log", probe, off)
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
