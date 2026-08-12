package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// twoTierSegs builds four one-record segments: the two oldest in "cold", the
// next in "hot", and a local active one. This is the shape a chain has —
// descent is by age, so the coldest store holds the oldest segments and the
// runs come out of the log in age order.
//
// Built as a fixture for the cleaner rather than through a log on purpose: the
// cleaner takes its tiers as data, so a run's retention can be driven directly
// at whatever tier shape a case needs, without an offload and a placement pass
// standing between the fixture and the behaviour under test.
func twoTierSegs(t *testing.T) []*segment {
	t.Helper()
	dir := tempDir(t)
	var segs []*segment
	for i := range 4 {
		s := createSegment(t, dir, int64(i*10), 4096)
		ms, entries, err := newMessageSetFromProto(int64(i*10), 0,
			[]*Message{{Value: []byte("0123456789")}})
		require.NoError(t, err)
		require.NoError(t, s.WriteMessageSet(ms, entries))
		segs = append(segs, s)
	}
	fakeOffloadedTo(segs[0], "cold")
	fakeOffloadedTo(segs[1], "cold")
	fakeOffloadedTo(segs[2], "hot")
	return segs
}

// twoTierCleaner cleans the fixture above. set adjusts the two tiers, which
// arrive as o.Tiers[0] ("cold") and o.Tiers[1] ("hot").
func twoTierCleaner(t *testing.T, set func(o *deleteCleanerOptions)) *deleteCleaner {
	t.Helper()
	opts := deleteCleanerOptions{
		Name: "chain",
		Tiers: []Tier{
			{Name: "cold", Store: &FileSegmentStore{}},
			{Name: "hot", Store: &FileSegmentStore{}},
		},
	}
	set(&opts)
	return newDeleteCleaner(opts)
}

// recordDeletes swaps the segment delete for a recorder, returning the map it
// fills. The cleaner tests never want the files actually gone; what is under
// test is which segments retention CHOSE.
func recordDeletes(t *testing.T) map[int64]bool {
	t.Helper()
	deleted := map[int64]bool{}
	restore := deleteSegment
	deleteSegment = func(s *segment) error { deleted[s.BaseOffset] = true; return nil }
	t.Cleanup(func() { deleteSegment = restore })
	return deleted
}

// Each tier spends its OWN budget. The oldest tier here admits nothing, so both
// of its objects go; the tier above it then gets its own limit applied rather
// than the one that emptied the tier below.
func TestEachTierSpendsItsOwnBudget(t *testing.T) {
	segs := twoTierSegs(t)
	deleted := recordDeletes(t)

	c := twoTierCleaner(t, func(o *deleteCleanerOptions) {
		o.Tiers[0].MaxBytes = 1                       // cold admits nothing
		o.Tiers[1].MaxBytes = segs[2].Position() * 10 // hot has ample room
	})
	out, err := c.Clean(segs, nil, Bound{})
	require.NoError(t, err)

	require.True(t, deleted[segs[0].BaseOffset], "cold is over its own budget")
	require.True(t, deleted[segs[1].BaseOffset])
	require.False(t, deleted[segs[2].BaseOffset],
		"hot's budget is hot's, not an inherited verdict from cold")
	require.Equal(t, []*segment{segs[2], segs[3]}, out)
}

// A newer tier may not delete while an older one still holds anything.
//
// Deleting segment 20 while segments 0 and 10 survive does not shorten the log,
// it punches a hole out of its middle: a reader walking forward would find
// records, then nothing, then records again. Each tier's budget is a statement
// about that tier's storage, and honouring one in isolation is how a hole gets
// made — so the oldest run drains first and the runs above it wait.
func TestANewerTierWaitsForTheOlderToDrain(t *testing.T) {
	segs := twoTierSegs(t)
	deleted := recordDeletes(t)

	c := twoTierCleaner(t, func(o *deleteCleanerOptions) {
		// Cold keeps one of its two; hot admits nothing at all.
		o.Tiers[0].MaxBytes = segs[1].Position()
		o.Tiers[1].MaxBytes = 1
	})
	out, err := c.Clean(segs, nil, Bound{})
	require.NoError(t, err)

	require.True(t, deleted[segs[0].BaseOffset], "cold's own oldest is over budget")
	require.False(t, deleted[segs[1].BaseOffset], "cold's budget holds this one")
	require.False(t, deleted[segs[2].BaseOffset],
		"hot is over budget, but deleting it around a surviving colder segment "+
			"would leave a hole in the middle of the log")
	require.Equal(t, []*segment{segs[1], segs[2], segs[3]}, out)
}

// A tier this log does not own blocks the tiers above it for the same reason a
// budget does: its segments are still there, so anything newer that went would
// leave a hole. Read-only is not "skip this tier and carry on", it is "this
// stretch of the log is not going anywhere".
func TestAReadOnlyTierHoldsTheTiersAboveIt(t *testing.T) {
	segs := twoTierSegs(t)
	deleted := recordDeletes(t)

	c := twoTierCleaner(t, func(o *deleteCleanerOptions) {
		o.Tiers[0].MaxBytes = 1 // cold would empty itself, if it were allowed to
		o.Tiers[1].MaxBytes = 1
	})
	out, err := c.Clean(segs, map[string]bool{"cold": true}, Bound{})
	require.NoError(t, err)

	require.False(t, deleted[segs[0].BaseOffset], "a tier this log does not own is not written to")
	require.False(t, deleted[segs[1].BaseOffset])
	require.False(t, deleted[segs[2].BaseOffset],
		"hot may not delete around segments cold is keeping")
	require.Equal(t, segs, out)
}

// The reverse: a read-only tier ABOVE an owned one costs the owned one nothing.
// Ownership is per tier because a node can own the store it writes and not the
// archive under it — and the whole point of that is that it keeps working on
// the half it does own.
func TestAReadOnlyTierAboveDoesNotHoldTheOneBelow(t *testing.T) {
	segs := twoTierSegs(t)
	deleted := recordDeletes(t)

	c := twoTierCleaner(t, func(o *deleteCleanerOptions) {
		o.Tiers[0].MaxBytes = 1
		o.Tiers[1].MaxBytes = 1
	})
	out, err := c.Clean(segs, map[string]bool{"hot": true}, Bound{})
	require.NoError(t, err)

	require.True(t, deleted[segs[0].BaseOffset], "cold is owned and over budget")
	require.True(t, deleted[segs[1].BaseOffset])
	require.False(t, deleted[segs[2].BaseOffset], "hot is not this log's to delete")
	require.Equal(t, []*segment{segs[2], segs[3]}, out)
}

// The retention floor is ONE allowance for the whole tiered half, spent across
// runs rather than renewed per run.
//
// Renewing it per run is the plausible mistake, and it is a data-loss one: each
// run would separately believe it may delete up to the floor, so a chain of two
// tiers would delete twice what a caller protected. Here the floor admits two
// segments, the oldest tier drains entirely on one of them, and the tier above
// must be left with the other one only.
func TestTheFloorIsSpentAcrossTiersNotPerTier(t *testing.T) {
	dir := tempDir(t)
	var segs []*segment
	for i := range 4 {
		s := createSegment(t, dir, int64(i*10), 4096)
		ms, entries, err := newMessageSetFromProto(int64(i*10), 0,
			[]*Message{{Value: []byte("0123456789")}})
		require.NoError(t, err)
		require.NoError(t, s.WriteMessageSet(ms, entries))
		segs = append(segs, s)
	}
	// One cold segment, then two hot ones: cold empties, so the runs above it
	// are not blocked and the floor is what is left holding them.
	fakeOffloadedTo(segs[0], "cold")
	fakeOffloadedTo(segs[1], "hot")
	fakeOffloadedTo(segs[2], "hot")
	deleted := recordDeletes(t)

	c := twoTierCleaner(t, func(o *deleteCleanerOptions) {
		o.Tiers[0].MaxBytes = 1 // both tiers admit nothing on their own
		o.Tiers[1].MaxBytes = 1
	})
	// The floor is at 25, inside segment 20 — so segments 0 and 10 lie entirely
	// below it and segment 20 does not. Two deletions are permitted in total.
	out, err := c.Clean(segs, nil, At(25))
	require.NoError(t, err)

	require.True(t, deleted[segs[0].BaseOffset], "cold's only segment is below the floor")
	require.True(t, deleted[segs[1].BaseOffset], "hot spends what cold left of the allowance")
	require.False(t, deleted[segs[2].BaseOffset],
		"the floor is one allowance for the tiered half, not one per tier")
	require.Equal(t, []*segment{segs[2], segs[3]}, out)
}

// A segment naming a tier that is not configured is refused, not cleaned with
// some default budget.
//
// The segment was opened from a manifest that named the tier, so a name with no
// Tier behind it means the log's own state disagrees with its configuration.
// The convenient answers are both destructive: an unlimited default keeps
// objects a caller believes it bounded, and a zero-limit default deletes a
// store's contents because the caller mistyped its name.
func TestASegmentNamingAnUnconfiguredTierIsRefused(t *testing.T) {
	segs := twoTierSegs(t)
	deleted := recordDeletes(t)

	c := twoTierCleaner(t, func(o *deleteCleanerOptions) {
		o.Tiers = o.Tiers[:1] // "hot" is gone; segs[2] still claims it
		o.Tiers[0].MaxBytes = 1
	})
	out, err := c.Clean(segs, nil, Bound{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "hot")

	// Cold ran before the unknown tier was reached, and its deletes stand —
	// they already happened. What must not happen is segs[2] being cleaned
	// under a budget nobody configured, and the surviving log must still be
	// contiguous so the caller can install it.
	require.False(t, deleted[segs[2].BaseOffset], "an unconfigured tier is not cleaned")
	require.Equal(t, []*segment{segs[2], segs[3]}, out)
}
