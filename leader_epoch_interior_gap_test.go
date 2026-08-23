package commitlog

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// A mutator contract worth pinning on its own: no mutator in this package can
// remove an epoch from the MIDDLE of the recorded set. ClearEarliest removes a
// prefix and re-adds the highest epoch it removed, ClearLatest removes a suffix,
// and assign only appends.
//
// Gaps in the epoch NUMBERS are ordinary and expected -- a node records only the
// tenures it LED, so 2,4,6 with nothing between is the normal shape. What must
// never happen is an epoch that WAS recorded disappearing from between two that
// still are.
//
// Read what this does and does not establish, because v0.101.0 got that wrong.
// It does establish that an interior gap was never recorded here. It does NOT
// establish that this node took no part in that tenure: a node that FOLLOWED
// holds every record of an epoch it never recorded, because only a leader
// assigns. v0.101.0 read "retention cannot have made this hole" as "this node
// was absent for it" and answered such probes from below, which told an ordinary
// follower to discard a log that had not diverged. See
// TestAFollowerDuringAnAbsentTenureIsNotToldToDiscardIt.
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
					"a trim must work from an end, and an entry that vanishes from "+
					"the middle silently rewrites what this log claims to have led",
					what, e.leaderEpoch, lo, hi)
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

// Everything a probe can name, in one table, so that a future attempt to make
// absent epochs answer differently by position has to change a visible row
// rather than a branch nobody enumerated.
//
// The rule is deliberately NOT a split any more: every epoch with no entry --
// below the earliest, in an interior gap, or above the latest -- is answered by
// the next recorded epoch's anchor, and above the latest there is none. v0.101.0
// made the interior case its own arm; the two rows carrying its old answers say
// what it returned.
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
			name: "interior gap answers from the successor, like any other gap", probe: 3,
			off: 20, found: true,
			reason: "this log never LED 3, which does not mean it was absent for it; v0.101.0 said 10",
		},
		{
			name: "interior gap next to the latest answers from the successor", probe: 5,
			off: 30, found: true,
			reason: "successor is 6 at 30; v0.101.0 said 20",
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

// Retention destroys epochs, and a stale prober asking about one must not be
// told to discard its log.
//
// ClearEarliest collapses 5, 6 and 7 into a single entry for 7 at the floor, so
// 5 and 6 stop existing even though this log lived through them. Answering from
// the successor puts the prober at the retention floor -- as far back as this log
// can serve it -- which is what a stale follower needs. There is nothing below to
// answer from, and a rule that reached for one would have said "discard
// everything".
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

	// And an epoch in the space above the earliest survivor is answered the same
	// way as any other absent epoch: from its successor, 13 at 100.
	off, found := l.LastOffsetForLeaderEpoch(9)
	require.True(t, found)
	require.Equal(t, int64(100), off, "the successor of 9 here is epoch 13, anchored at 100")
}

// The limitation this cache has and cannot close, pinned so nobody rediscovers
// it as a bug and ships v0.101.0 again.
//
// The partition durable_streams lost, reduced to the cache that lost it: n3 held
// epoch 13, was deposed while n1 took 14, kept appending two records under its
// stale tenure, and led again at 15 without ever recording 14. A probe for 14
// against n3 is answered from epoch 15's anchor, which sits ABOVE the divergent
// records, so the prober keeps them and the fork survives.
//
// That answer is WRONG here and this test asserts it anyway, because the cache
// cannot do better. Compare the fixture below with
// TestAFollowerDuringAnAbsentTenureIsNotToldToDiscardIt: both are two entries
// with a gap between them, and in that one the same answer is correct. Nothing
// in a sparse cache separates "I was deposed and kept writing" from "I followed
// and replicated" -- an entry means only "I LED this", so the tenure a node
// merely lived through leaves no trace either way. Kafka tells them apart with
// per-record epochs; this does not have them.
//
// The fix belongs upstream, where a deposed leader is stopped from appending.
// See CHANGELOG.md v0.95.8, and v0.103.0 for the release that tried to close it
// here and had to be reverted.
func TestAForkedTenureIsAnsweredAboveTheForkAndCannotBeDetectedHere(t *testing.T) {
	l := &leaderEpochCache{name: "n3"}
	require.NoError(t, l.Assign(13, 7000))
	require.NoError(t, l.Assign(15, 7439))

	off, found := l.LastOffsetForLeaderEpoch(14)
	require.True(t, found)
	require.Equal(t, int64(7439), off,
		"the known-unsound answer: 7439 is above the fork, so the divergent "+
			"records survive on both sides. Answering 7000 instead is what "+
			"v0.101.0 did, and it wiped ordinary followers")

	// The tenures this log DID record are unambiguous and unaffected.
	off, found = l.LastOffsetForLeaderEpoch(13)
	require.True(t, found)
	require.Equal(t, int64(7439), off, "epoch 13 ran until 15 opened")

	off, found = l.LastOffsetForLeaderEpoch(15)
	require.False(t, found, "nothing above 15 is recorded")
	require.Equal(t, int64(-1), off)
}

// An ordinary three-way handover: b1 leads, b2 leads, b1 leads again. b1 has no
// entry for b2's tenure and every record written during it.
//
// This is the case v0.101.0 got wrong and v0.103.0 restores. b1's cache holds
// epoch 1 anchored -1 and epoch 3 anchored 79; epoch 2 is absent because b1 only
// FOLLOWED then. But b1 replicated every record b2 wrote -- its own epoch-3
// anchor at 79 says so -- so a probe for epoch 2 must be answered 79, and
// nothing has diverged.
//
// Reported by durable_streams against a deterministic reproducer
// (broker/embed.TestRecordsSurviveRepeatedLeadershipChanges): 12 permanent
// replication halts, "would truncate to 0, at or below its own commit boundary
// of 79".
func TestAFollowerDuringAnAbsentTenureIsNotToldToDiscardIt(t *testing.T) {
	l := &leaderEpochCache{name: "b1"}
	require.NoError(t, l.Assign(1, -1))
	require.NoError(t, l.Assign(3, 79))

	off, found := l.LastOffsetForLeaderEpoch(2)
	require.True(t, found)
	require.Equal(t, int64(79), off,
		"b1 followed epoch 2 and holds all of its records; answering from below "+
			"tells the prober to discard a log that never diverged")

	// The sentinel is the loud half of it: the floor here is the log's first
	// epoch, anchored at -1, so the floor answer was "discard everything".
	require.NotEqual(t, int64(-1), off,
		"-1 means discard the whole log, from a node that agrees with the prober")
}

// An epoch that opened on an empty log anchors at -1, and a clean that trims
// nothing must leave it there.
//
// -1 is a sentinel meaning "nothing preceded this epoch", not an offset, but it
// compares as one: against a floor of 0 it looks sub-floor, and ClearEarliest
// re-anchored it to 0 -- asserting that exactly one record DID precede the
// epoch. The cost lands on the predecessor's probe, which is what this asserts.
//
// Found as a flake rather than a failure. TestTruncate writes epoch 1 on an
// empty log and then asserts a probe for epoch 0 answers -1; it went red only
// when a cleaner tick happened to land inside the test, which is why it failed
// on the first run in a process and passed on the next four. Present since at
// least v0.100.0, confirmed by running that release in a worktree.
func TestAnEpochOpenedOnAnEmptyLogSurvivesACleanThatTrimsNothing(t *testing.T) {
	l := &leaderEpochCache{name: "sentinel"}
	require.NoError(t, l.Assign(1, -1))
	require.NoError(t, l.Assign(2, 4))
	require.NoError(t, l.Assign(3, 9))

	off, found := l.LastOffsetForLeaderEpoch(0)
	require.True(t, found)
	require.Equal(t, int64(-1), off,
		"epoch 0 wrote nothing, so its probe means discard everything")

	// The floor a clean passes when the whole log survives.
	require.NoError(t, l.ClearEarliest(0))

	require.Len(t, l.epochOffsets, 3, "a floor of 0 trims no record, so it must trim no entry")
	require.Equal(t, int64(-1), l.epochOffsets[0].assignedAtOffset,
		"epoch 1 still opened on an empty log; re-anchoring to 0 invents a record before it")

	off, found = l.LastOffsetForLeaderEpoch(0)
	require.True(t, found)
	require.Equal(t, int64(-1), off,
		"answering 0 here tells a follower to KEEP offset 0, which belongs to epoch 1")

	// A real floor still trims, so the no-op is scoped to "nothing was removed"
	// rather than switching the pass off.
	require.NoError(t, l.ClearEarliest(5))
	require.Equal(t, int64(5), l.epochOffsets[0].assignedAtOffset,
		"a floor above an entry still re-anchors it")
}

// No answer can point below the oldest surviving record, which is what stops any
// truncation instruction from becoming a whole-log wipe.
//
// It matters because the caller obeys the offset it is given -- Truncate(answer+1)
// on an offset under the log discards everything, because findSegment lands on
// the first segment and the rewrite keeps nothing. So the question is whether an
// answer can ever sit under the retention floor.
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
