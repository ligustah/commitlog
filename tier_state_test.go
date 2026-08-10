package commitlog

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// tieredLog builds a log with some sealed segments offloaded, and returns it
// with its store.
// isStoreMetaKey reports whether a store object DESCRIBES the log rather than
// holding it: the manifest says what the tier holds, the descriptor says what
// the log is. Neither is a segment, and every "objects in the store" count in
// these tests means segments.
//
// It lives here rather than being spelled out at each site because the set
// grows. Every place that had written `k != manifestKey` became quietly wrong
// the moment the descriptor was added, and each one failed separately with a
// number that looked like a real regression.
func isStoreMetaKey(k string) bool {
	return k == manifestKey || k == descriptorKey
}

// segmentObjectCount is that filter applied to a listing.
func segmentObjectCount(keys []string) int {
	n := 0
	for _, k := range keys {
		if !isStoreMetaKey(k) {
			n++
		}
	}
	return n
}

func tieredLog(t *testing.T) (*commitLog, *FileSegmentStore, int64) {
	t.Helper()
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	l, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  64,
		Tiers:            oneTier(store),
		DisableAutoClean: true,
	})
	t.Cleanup(cleanup)

	var last int64
	for i := 0; i < 24; i++ {
		offs, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("padding value")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)

	n, err := l.OffloadBefore(last)
	require.NoError(t, err)
	require.Positive(t, n, "the fixture needs offloaded segments")
	return l, store, last
}

// The tier state has to carry everything needed to place a segment without
// reading its object, since that is what the manifest publishes.
func TestTierStateDescribesEveryOffloadedSegment(t *testing.T) {
	l, _, _ := tieredLog(t)

	state, err := l.tierState()
	require.NoError(t, err)
	require.NotEmpty(t, state)

	for _, o := range state {
		require.NotEmpty(t, o.LogKey)
		require.Positive(t, o.PhysPosition)
		require.LessOrEqual(t, o.FirstOffset, o.LastOffset)
	}

	// Only offloaded segments appear: the active one is still being written.
	l.mu.RLock()
	active := l.segments[len(l.segments)-1].BaseOffset
	l.mu.RUnlock()
	for _, o := range state {
		require.NotEqual(t, active, o.BaseOffset)
	}
}

// A log with no store has no tier bookkeeping, and asking is not an error.
func TestTierStateEmptyWithoutAStore(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t)})
	defer cleanup()

	state, err := l.tierState()
	require.NoError(t, err)
	require.Empty(t, state)

	manifest, err := l.TierManifest()
	require.NoError(t, err)
	require.Empty(t, manifest)
}
