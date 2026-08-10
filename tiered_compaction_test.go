package commitlog

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// The whole point of the tiered-compaction work: a segment that has been
// offloaded is no longer exempt from compaction. Before this, whatever garbage
// a segment held when it offloaded was frozen there permanently — superseded
// values were kept forever because the rewriters were held away from the
// offloaded prefix.
//
// Here the same key is written repeatedly so the sealed segments are full of
// superseded copies, those segments are offloaded, and a compaction pass must
// then reach them: the store objects advance a generation and the superseded
// keys come back to the caller.
func TestCompactionRewritesOffloadedSegments(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	l, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  128,
		Compact:          true,
		Tiers:            oneTier(store),
		DisableAutoClean: true,
	})
	defer cleanup()

	// One key, many values: every copy but the last is superseded.
	var last int64
	for i := 0; i < 30; i++ {
		offs, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("value padding")}})
		require.NoError(t, err)
		last = offs[0]
	}
	// Pads so the real records are not stranded in the never-compacted active
	// segment.
	for _, k := range []string{"pad0", "pad1"} {
		offs, err := l.Append([]*Message{{Key: []byte(k), Value: []byte("p")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)

	n, err := l.OffloadBefore(last)
	require.NoError(t, err)
	require.Positive(t, n, "sealed segments must have offloaded")

	keysBefore := map[int64]string{}
	l.mu.RLock()
	for _, s := range l.segments {
		s.RLock()
		if s.store != nil {
			keysBefore[s.BaseOffset] = s.storeKey
		}
		s.RUnlock()
	}
	l.mu.RUnlock()
	require.NotEmpty(t, keysBefore, "the fixture needs offloaded segments")

	hw := l.HighWatermark()
	_, err = l.CleanWithSpec(CleanSpec{
		Ceiling:          At(hw + 1),
		TombstoneGCBelow: hw + 1,
	})
	require.NoError(t, err)

	// At least one offloaded segment must have been rewritten: a new generation,
	// and its previous objects queued for reclamation rather than deleted under
	// a live reader or leaked forever.
	advanced := 0
	l.mu.RLock()
	for _, s := range l.segments {
		s.RLock()
		if s.store != nil {
			if before, ok := keysBefore[s.BaseOffset]; ok && s.storeKey != before {
				advanced++
			}
		}
		s.RUnlock()
	}
	l.mu.RUnlock()

	require.Positive(t, advanced,
		"compaction must now reach offloaded segments — that exemption is what "+
			"froze garbage into the tier permanently")

	l.tierMu.Lock()
	queued := append([]pendingReclaim(nil), l.reclaim...)
	l.tierMu.Unlock()
	require.NotEmpty(t, queued, "a rewrite must queue the objects it superseded")

	// They survive the pass that superseded them. A reader that opened the
	// segment beforehand is still on the old object and is entitled to finish,
	// and this pass has no way to know it has gone.
	keys, err := store.List()
	require.NoError(t, err)
	for _, e := range queued {
		require.Contains(t, keys, e.key,
			"a superseded object must outlive the pass that superseded it")
	}

	// A later pass reclaims them. Nothing holds the backings by now, so this one
	// deletes rather than deferring again — the queue drains on its own instead
	// of growing until a caller thinks to empty it.
	_, err = l.CleanWithSpec(CleanSpec{Ceiling: At(hw + 1), TombstoneGCBelow: hw + 1})
	require.NoError(t, err)

	keys, err = store.List()
	require.NoError(t, err)
	for _, e := range queued {
		require.NotContains(t, keys, e.key,
			"a later pass must delete the superseded object: leaving it is the "+
				"leak that made this the caller's problem in the first place")
	}
	l.tierMu.Lock()
	require.Empty(t, l.reclaim, "a drained entry must leave the queue")
	l.tierMu.Unlock()

	// And the log still reads correctly across the rewritten tier.
	got := readFrom(t, l)
	require.NotEmpty(t, got)
}

// A log with no SegmentStore must take exactly the path it always did — nothing
// queued for reclamation, no behavioural change from the tiering machinery.
func TestCompactionWithoutAStoreQueuesNothingToReclaim(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:             tempDir(t),
		MaxSegmentBytes:  128,
		Compact:          true,
		DisableAutoClean: true,
	})
	defer cleanup()

	var last int64
	for i := 0; i < 20; i++ {
		offs, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("value padding")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)

	_, err := l.CleanWithSpec(CleanSpec{Ceiling: At(last + 1)})
	require.NoError(t, err)
	l.tierMu.Lock()
	require.Empty(t, l.reclaim, "no store means nothing can be superseded")
	l.tierMu.Unlock()
}
