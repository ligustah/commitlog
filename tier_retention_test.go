package commitlog

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeOffloaded marks a segment as living in a store, without the machinery of
// a real offload. The delete cleaner only asks whether a segment is offloaded,
// so this is enough to drive the tier split.
func fakeOffloaded(s *segment) *segment {
	s.Lock()
	s.store = &FileSegmentStore{}
	s.Unlock()
	return s
}

func tierCleaner(t *testing.T, set func(o *deleteCleanerOptions)) *deleteCleaner {
	t.Helper()
	opts := deleteCleanerOptions{Name: "tier"}
	set(&opts)
	return newDeleteCleaner(opts)
}

// The local limits must stop counting offloaded segments. Their bytes are not
// on the disk those limits govern, so counting them deletes records to reclaim
// space that offloading already reclaimed.
func TestLocalRetentionIgnoresOffloadedSegments(t *testing.T) {
	dir := tempDir(t)
	var segs []*segment
	for i := 0; i < 4; i++ {
		s := createSegment(t, dir, int64(i*10), 4096)
		ms, entries, err := newMessageSetFromProto(int64(i*10), 0,
			[]*Message{{Value: []byte("0123456789")}}, false)
		require.NoError(t, err)
		require.NoError(t, s.WriteMessageSet(ms, entries))
		segs = append(segs, s)
	}
	// The two oldest live in a store.
	fakeOffloaded(segs[0])
	fakeOffloaded(segs[1])

	deleted := map[int64]bool{}
	restore := deleteSegment
	deleteSegment = func(s *segment) error { deleted[s.BaseOffset] = true; return nil }
	defer func() { deleteSegment = restore }()

	// A byte budget far below the total. With no tier limit set, the offloaded
	// prefix must survive it untouched.
	c := tierCleaner(t, func(o *deleteCleanerOptions) { o.Retention.Bytes = 1 })
	out, err := c.Clean(segs)
	require.NoError(t, err)

	require.False(t, deleted[segs[0].BaseOffset], "an offloaded segment is not on local disk")
	require.False(t, deleted[segs[1].BaseOffset], "an offloaded segment is not on local disk")
	require.Contains(t, out, segs[0])
	require.Contains(t, out, segs[1])
	require.Contains(t, out, segs[3], "the active segment always survives")
}

// The tier limit governs the offloaded prefix, and unlike the local pass it may
// reclaim every one of them — a tier has no active segment to protect.
func TestTierRetentionDropsOffloadedSegments(t *testing.T) {
	dir := tempDir(t)
	var segs []*segment
	for i := 0; i < 4; i++ {
		s := createSegment(t, dir, int64(i*10), 4096)
		ms, entries, err := newMessageSetFromProto(int64(i*10), 0,
			[]*Message{{Value: []byte("0123456789")}}, false)
		require.NoError(t, err)
		require.NoError(t, s.WriteMessageSet(ms, entries))
		segs = append(segs, s)
	}
	fakeOffloaded(segs[0])
	fakeOffloaded(segs[1])
	fakeOffloaded(segs[2])

	deleted := map[int64]bool{}
	restore := deleteSegment
	deleteSegment = func(s *segment) error { deleted[s.BaseOffset] = true; return nil }
	defer func() { deleteSegment = restore }()

	// Room for roughly one tiered segment's bytes.
	c := tierCleaner(t, func(o *deleteCleanerOptions) {
		o.Retention.TierBytes = segs[2].Position()
	})
	out, err := c.Clean(segs)
	require.NoError(t, err)

	require.True(t, deleted[segs[0].BaseOffset], "the oldest tiered segment must go first")
	require.True(t, deleted[segs[1].BaseOffset])
	require.False(t, deleted[segs[2].BaseOffset], "the newest tiered one fits the budget")
	require.False(t, deleted[segs[3].BaseOffset], "the local segment is not the tier's business")

	require.Equal(t, []*segment{segs[2], segs[3]}, out,
		"survivors must stay contiguous and in offset order")
}

// A tier age limit reclaims by write time, and may take the whole tier.
func TestTierRetentionByAgeCanEmptyTheTier(t *testing.T) {
	dir := tempDir(t)
	var segs []*segment
	for i := 0; i < 3; i++ {
		s := createSegment(t, dir, int64(i*10), 4096)
		ms, entries, err := newMessageSetFromProto(int64(i*10), 0,
			[]*Message{{Timestamp: 1, Value: []byte("v")}}, false)
		require.NoError(t, err)
		require.NoError(t, s.WriteMessageSet(ms, entries))
		segs = append(segs, s)
	}
	fakeOffloaded(segs[0])
	fakeOffloaded(segs[1])

	deleted := map[int64]bool{}
	restore := deleteSegment
	deleteSegment = func(s *segment) error { deleted[s.BaseOffset] = true; return nil }
	defer func() { deleteSegment = restore }()

	c := tierCleaner(t, func(o *deleteCleanerOptions) { o.Retention.TierAge = time.Nanosecond })
	out, err := c.Clean(segs)
	require.NoError(t, err)

	require.True(t, deleted[segs[0].BaseOffset])
	require.True(t, deleted[segs[1].BaseOffset],
		"a tier has no active segment, so the last tiered one is reclaimable too")
	require.Equal(t, []*segment{segs[2]}, out)
}

// With no store there are no offloaded segments, so the split is a no-op and
// retention behaves exactly as it did before it became per-tier.
func TestRetentionUnchangedWithoutATier(t *testing.T) {
	dir := tempDir(t)
	var segs []*segment
	for i := 0; i < 4; i++ {
		s := createSegment(t, dir, int64(i*10), 4096)
		ms, entries, err := newMessageSetFromProto(int64(i*10), 0,
			[]*Message{{Value: []byte("0123456789")}}, false)
		require.NoError(t, err)
		require.NoError(t, s.WriteMessageSet(ms, entries))
		segs = append(segs, s)
	}

	deleted := map[int64]bool{}
	restore := deleteSegment
	deleteSegment = func(s *segment) error { deleted[s.BaseOffset] = true; return nil }
	defer func() { deleteSegment = restore }()

	c := tierCleaner(t, func(o *deleteCleanerOptions) {
		o.Retention.Bytes = segs[3].Position()
	})
	out, err := c.Clean(segs)
	require.NoError(t, err)

	require.True(t, deleted[segs[0].BaseOffset])
	require.Contains(t, out, segs[3], "the active segment always survives")
	require.NotContains(t, out, segs[0])
}

// splitOffloadedPrefix cuts at the first local segment and keeps both halves in
// order, which is what lets the two budgets be applied independently.
func TestSplitOffloadedPrefix(t *testing.T) {
	dir := tempDir(t)
	var segs []*segment
	for i := 0; i < 4; i++ {
		segs = append(segs, createSegment(t, dir, int64(i*10), 1024))
	}
	fakeOffloaded(segs[0])
	fakeOffloaded(segs[1])

	tiered, local := splitOffloadedPrefix(segs)
	require.Equal(t, []*segment{segs[0], segs[1]}, tiered)
	require.Equal(t, []*segment{segs[2], segs[3]}, local)

	// All local, and all tiered, are both fine.
	allLocal, none := splitOffloadedPrefix(segs[2:])
	require.Empty(t, allLocal)
	require.Len(t, none, 2)

	allTiered, noLocal := splitOffloadedPrefix(segs[:2])
	require.Len(t, allTiered, 2)
	require.Empty(t, noLocal)
}
