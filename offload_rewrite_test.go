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
	oldKey := seg.storeKey
	seg.RUnlock()

	// A reader holding the pre-rewrite backing.
	oldBacking := seg.backing

	fresh := freshLocalSegment(t, l, seg)
	superseded := replaceOffloaded(t, seg, fresh)

	seg.RLock()
	newKey := seg.storeKey
	seg.RUnlock()
	require.NotEqual(t, oldKey, newKey, "it must not land on the key being read")
	require.Len(t, superseded, 1, "the old object must be queued, not silently dropped")
	require.Equal(t, oldKey, superseded[0].key)
	require.Same(t, oldBacking, superseded[0].pin,
		"the entry must carry the backing readers hold, or nothing can tell when "+
			"the object has come free")

	// Both objects exist: the old one goes on a later pass, once unheld.
	keys, err := store.List()
	require.NoError(t, err)
	require.Contains(t, keys, oldKey, "the superseded object must survive the call")
	require.Contains(t, keys, newKey)

	// The pre-rewrite reader can still read, which is why deletion is deferred.
	buf := make([]byte, 4)
	_, err = oldBacking.ReadAt(buf, 0)
	require.NoError(t, err, "a reader holding the old generation must not be broken")
}

// The manifest is the commit point: after a rewrite it names the new
// generation, so a reopen resolves the rewritten object and not the superseded
// one. The publish happens BETWEEN the two halves, which is what the test walks
// through by hand.
func TestReplaceOffloadedManifestNamesTheNewGeneration(t *testing.T) {
	l, store, seg := offloadedFixture(t, nil)

	fresh := freshLocalSegment(t, l, seg)
	meta, _, err := seg.uploadReplacement(fresh)
	require.NoError(t, err)

	seg.RLock()
	stillOld := seg.storeKey
	seg.RUnlock()
	require.NotEqual(t, meta.LogKey, stillOld,
		"the segment must go on serving the old object until the commit")

	require.NoError(t, l.writeTierManifest(meta.tierObject(seg.BaseOffset, defaultTierName)))
	require.NoError(t, seg.swapReplacement(fresh, meta))

	objs, err := readTierManifest(store)
	require.NoError(t, err)
	var named string
	for _, o := range objs {
		if o.BaseOffset == seg.BaseOffset {
			named = o.LogKey
		}
	}
	seg.RLock()
	live := seg.storeKey
	seg.RUnlock()
	require.Equal(t, live, named,
		"the manifest must resolve to the rewritten object")
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
	replaceOffloaded(t, seg, fresh)

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
	_, _, err := seg.uploadReplacement(seg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not offloaded")
}

// replaceOffloaded runs both halves of a rewrite back to back, for the tests
// that are about what the SEGMENT does across the swap rather than about the
// manifest publish that separates them in the log.
func replaceOffloaded(t *testing.T, seg, fresh *segment) []pendingReclaim {
	t.Helper()
	meta, superseded, err := seg.uploadReplacement(fresh)
	require.NoError(t, err)
	require.NoError(t, seg.swapReplacement(fresh, meta))
	return superseded
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
