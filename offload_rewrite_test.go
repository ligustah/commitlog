package commitlog

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// offloadedFixture builds a log with one offloaded sealed segment and returns
// the log, the store, and that segment.
func offloadedFixture(t *testing.T, cache *RemoteIndexCache) (*commitLog, *FileSegmentStore, *segment) {
	t.Helper()
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	l, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  64, // roll, so there is a sealed segment
		SegmentStore:     store,
		RemoteIndexCache: cache,
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
	require.Positive(t, n, "a sealed segment must have offloaded")

	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, s := range l.segments {
		s.RLock()
		off := s.store != nil
		s.RUnlock()
		if off {
			return l, store, s
		}
	}
	t.Fatal("no offloaded segment")
	return nil, nil, nil
}

// A rewrite of an offloaded segment must land on a NEW key and leave the old
// one intact for the caller to delete, because a reader that opened the segment
// beforehand holds a backing over the old key and is entitled to finish.
func TestReplaceOffloadedWritesNewGenerationAndKeepsTheOld(t *testing.T) {
	l, store, seg := offloadedFixture(t, nil)

	seg.RLock()
	oldKey, oldGen := seg.storeKey, seg.storeGen
	seg.RUnlock()
	require.Equal(t, 0, oldGen)

	// A reader holding the pre-rewrite backing.
	oldBacking := seg.backing

	fresh := freshLocalSegment(t, l, seg)
	superseded, err := seg.ReplaceOffloaded(fresh, "", nil)
	require.NoError(t, err)

	seg.RLock()
	newKey, newGen := seg.storeKey, seg.storeGen
	seg.RUnlock()

	require.Equal(t, 1, newGen, "the rewrite must allocate the next generation")
	require.NotEqual(t, oldKey, newKey, "it must not land on the key being read")
	require.Equal(t, segmentStoreKey(seg.BaseOffset, 1, ""), newKey)
	require.Equal(t, []string{oldKey}, superseded,
		"the old object must be reported, not silently dropped")

	// Both objects exist: the caller decides when the old one goes.
	keys, err := store.List()
	require.NoError(t, err)
	require.Contains(t, keys, oldKey, "the superseded object must survive the call")
	require.Contains(t, keys, newKey)

	// The pre-rewrite reader can still read, which is why deletion is deferred.
	buf := make([]byte, 4)
	_, err = oldBacking.ReadAt(buf, 0)
	require.NoError(t, err, "a reader holding the old generation must not be broken")
}

// The marker is the commit point: after a rewrite it names the new generation,
// so a reopen resolves the rewritten object rather than the superseded one.
func TestReplaceOffloadedMarkerNamesTheNewGeneration(t *testing.T) {
	l, _, seg := offloadedFixture(t, nil)

	fresh := freshLocalSegment(t, l, seg)
	_, err := seg.ReplaceOffloaded(fresh, "", nil)
	require.NoError(t, err)

	meta, err := readOffloadMarker(seg.offloadMarkerPath())
	require.NoError(t, err)
	require.Equal(t, 1, meta.Generation)
	require.Equal(t, segmentStoreKey(seg.BaseOffset, 1, ""), meta.LogKey,
		"the marker must resolve to the rewritten object")
}

// The read-ahead window must not survive the swap. Without the invalidation the
// rewrite would appear to succeed while reads kept being served from bytes
// cached before it.
func TestReplaceOffloadedClearsTheReadAheadWindow(t *testing.T) {
	l, _, seg := offloadedFixture(t, nil)

	// Warm the window so there is something stale to serve.
	buf := make([]byte, 4)
	_, err := seg.backing.ReadAt(buf, 0)
	require.NoError(t, err)
	sb, ok := seg.backing.(*storeBacking)
	require.True(t, ok)
	sb.mu.Lock()
	warmed := sb.bufOff >= 0
	sb.mu.Unlock()
	require.True(t, warmed, "the window should be populated at this point")

	fresh := freshLocalSegment(t, l, seg)
	_, err = seg.ReplaceOffloaded(fresh, "", nil)
	require.NoError(t, err)

	// The segment now reads through a different backing entirely, over the new
	// key, and the old one was cleared before being dropped.
	sb.mu.Lock()
	cleared := sb.bufOff < 0
	sb.mu.Unlock()
	require.True(t, cleared, "the superseded window must be invalidated")
	require.NotSame(t, sb, seg.backing, "the segment must read the new generation")
}

// Rewriting something that was never offloaded is a caller error, not a
// silently-wrong upload.
func TestReplaceOffloadedRefusesALocalSegment(t *testing.T) {
	dir := tempDir(t)
	seg := createSegment(t, dir, 0, 1024)
	_, err := seg.ReplaceOffloaded(seg, "", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not offloaded")
}

// freshLocalSegment builds a local segment holding the same records as src, as
// a rewriter would produce before installing it.
func freshLocalSegment(t *testing.T, l *commitLog, src *segment) *segment {
	t.Helper()
	fresh, err := src.Cleaned()
	require.NoError(t, err)
	t.Cleanup(func() { fresh.Delete() })

	bw := &blockWriter{}
	bw.reset(fresh)
	ss := newSegmentScannerCache(src, newBlockCache())
	for ms, _, err := ss.Scan(); err == nil; ms, _, err = ss.Scan() {
		require.NoError(t, bw.add(ms))
	}
	require.NoError(t, bw.flush())
	require.NoError(t, fresh.Sync())
	return fresh
}
