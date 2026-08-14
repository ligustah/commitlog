package commitlog

import (
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ligustah/commitlog/compress"
)

// lyingBlockTableStore reports a size of its own choosing for the block table
// object, and the truth for everything else.
//
// The lie is armed AFTER the offload, so the upload writes a real table and the
// manifest describes a real segment. Only the read is being tested; a store that
// lied during the write would be testing the offload path instead.
type lyingBlockTableStore struct {
	*FileSegmentStore
	size atomic.Int64 // 0 disarmed
}

func (s *lyingBlockTableStore) Size(key string) (int64, error) {
	if n := s.size.Load(); n != 0 && strings.HasSuffix(key, blocksSuffix) {
		return n, nil
	}
	return s.FileSegmentStore.Size(key)
}

// offloadedBlockSegment builds a block-compressed log, offloads it into store,
// and REOPENS it, returning a segment whose table has not been fetched.
//
// The reopen is what produces that state and there is no shortcut to it. A
// segment that offloads inside this process keeps the table it already had
// (attachOffloadedLocked), and a reopen in option 1 — index kept local — fetches
// it during setupIndex. Only option 2, with a RemoteIndexCache, leaves
// blocksPending set for the first read to resolve, which is the one arrangement
// where a store's answer about the table's size reaches an allocation.
func offloadedBlockSegment(t *testing.T, store SegmentStore) *segment {
	t.Helper()
	dir := tempDir(t)
	cache, err := NewRemoteIndexCache(filepath.Join(dir, "idxcache"), 1<<20)
	require.NoError(t, err)
	t.Cleanup(func() { cache.Close() })

	opts := Options{
		Path:             filepath.Join(dir, "log"),
		MaxSegmentBytes:  1 << 14,
		Compression:      compress.Snappy,
		Tiers:            []Tier{{Name: defaultTierName, Store: store}},
		RemoteIndexCache: cache,
		DisableAutoClean: true,
	}

	l, err := New(opts)
	require.NoError(t, err)
	var last int64
	for i := 0; i < 400; i++ {
		offs, aerr := l.Append([]*Message{{Value: []byte("padding value for the block")}})
		require.NoError(t, aerr)
		last = offs[0]
	}
	l.SetHighWatermark(last)
	n, err := l.OffloadBefore(last)
	require.NoError(t, err)
	require.Positive(t, n, "nothing offloaded, so there is no store-side table to fetch")
	require.NoError(t, l.Close())

	againCL, err := New(opts)
	require.NoError(t, err)
	t.Cleanup(func() { againCL.Close() })
	again := againCL.(*commitLog)

	for _, seg := range again.segmentsSnapshot() {
		seg.RLock()
		off, pending := seg.isOffloaded(), seg.blocksPending
		seg.RUnlock()
		if off && pending {
			return seg
		}
	}
	t.Fatal("no offloaded segment with a table still to fetch")
	return nil
}

// THE SIZE THAT STEERS AN ALLOCATION IS THE STORE'S, AND NOTHING VERIFIED IT.
//
// fetchBlockTable read store.Size and allocated it, unchecked. Every length check
// in decodeBlockTable runs AFTER that allocation, so they could not protect it —
// which is the general shape: the checks that matter for an allocation are the
// ones that happen before it.
//
// Two ends, because they fail differently and only one of them looks like an
// error at all. A negative size is `makeslice: len out of range` — a panic, in
// the caller's process, out of a library. A huge one is simply taken: a remote
// store deciding how much of this process's memory it gets.
//
// This was the fourth reader of a caller-supplied store and the last one without
// the check. The other three (readStoreDescriptor, readTierManifest, the remote
// index cache's fetch) all refuse a bad size, and the first of them documents
// this exact hazard.
func TestABlockTableShorterThanATableIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name string
		size int64
	}{
		// Before the check this reached make([]byte, -1) and took the process
		// down. The test is that an error comes back at all.
		{"negative", -1},
		// A table with no blocks is the shortest object the format can produce;
		// anything under it cannot be one.
		{"shorter than an empty table", blockTableHeaderLen + 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs, err := NewFileSegmentStore(filepath.Join(tempDir(t), "store"))
			require.NoError(t, err)
			store := &lyingBlockTableStore{FileSegmentStore: fs}
			seg := offloadedBlockSegment(t, store)

			store.size.Store(tc.size)
			err = seg.ensureBlocksLoaded()
			require.Error(t, err)
			require.ErrorIs(t, err, ErrBlockTableFormat,
				"a store's impossible size must read as a malformed table, not as an outage")
		})
	}
}

// A SIZE PAST WHAT THE SEGMENT COULD NEED IS REFUSED WITHOUT BEING ALLOCATED.
//
// The upper bound, and the one where the error alone proves nothing: an
// oversized object is refused either way, because decodeBlockTable requires an
// exact length. What changes is WHEN — before the bytes are taken, or after. So
// the assertion is on allocation, which is the property the bound exists for.
//
// TotalAlloc rather than a live-heap reading: it only ever grows, so the delta
// records the allocation even though it was freed on the way out — which is
// exactly the case that would otherwise look identical to never allocating.
//
// The bound is derived, not picked. Every block occupies at least blockHeaderLen
// physical bytes, so a segment of physPosition bytes holds at most
// physPosition/blockHeaderLen blocks, and the table's layout is fixed-width. This
// segment is a few KB, so its ceiling is a few KB of table.
func TestABlockTablePastWhatTheSegmentCouldNeedIsNotAllocated(t *testing.T) {
	const claimed = 64 << 20

	fs, err := NewFileSegmentStore(filepath.Join(tempDir(t), "store"))
	require.NoError(t, err)
	store := &lyingBlockTableStore{FileSegmentStore: fs}
	seg := offloadedBlockSegment(t, store)

	seg.RLock()
	phys := seg.physPosition
	seg.RUnlock()
	require.Less(t, maxBlockTableBytes(phys), int64(claimed),
		"the fixture's own ceiling is above the claim, so nothing is being refused")

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	store.size.Store(claimed)
	err = seg.ensureBlocksLoaded()
	runtime.ReadMemStats(&after)

	require.ErrorIs(t, err, ErrBlockTableFormat)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(claimed/4),
		"the store's %d-byte claim was allocated before it was refused", claimed)
}

// AND AN HONEST SIZE STILL LOADS.
//
// The guard on the test above: a bound that refused everything would satisfy it
// completely, and would turn every tiered block segment into an unreadable one.
// Same fixture, lie disarmed.
func TestAnHonestlySizedBlockTableStillLoads(t *testing.T) {
	fs, err := NewFileSegmentStore(filepath.Join(tempDir(t), "store"))
	require.NoError(t, err)
	store := &lyingBlockTableStore{FileSegmentStore: fs}
	seg := offloadedBlockSegment(t, store)

	require.NoError(t, seg.ensureBlocksLoaded())
	seg.RLock()
	n := len(seg.blocks)
	seg.RUnlock()
	require.Positive(t, n, "the table loaded but described no blocks")
}
