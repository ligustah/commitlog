package commitlog

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// tieredLog builds a log with some sealed segments offloaded, and returns it
// with its store.
func tieredLog(t *testing.T) (*commitLog, *FileSegmentStore, int64) {
	t.Helper()
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	l, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  64,
		SegmentStore:     store,
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
