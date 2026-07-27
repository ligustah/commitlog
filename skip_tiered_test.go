package commitlog

import (
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// writeCountingStore counts every mutating call, so a test can assert that a
// pass made NO tier writes rather than merely few.
type writeCountingStore struct {
	*FileSegmentStore
	puts    int
	deletes int
}

func (s *writeCountingStore) Put(key string, r io.Reader, size int64) error {
	s.puts++
	return s.FileSegmentStore.Put(key, r, size)
}

func (s *writeCountingStore) Delete(key string) error {
	s.deletes++
	return s.FileSegmentStore.Delete(key)
}

// SkipTiered must mean ZERO tier writes, not "fewer". No budget can express
// that — both rewrite budgets guarantee at least one rewrite so debt always
// drains — and for a replica that does not own tier writes, a single rewrite
// into shared storage is corruption rather than duplicated work.
func TestSkipTieredMakesNoTierWrites(t *testing.T) {
	dir := tempDir(t)
	fs, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)
	store := &writeCountingStore{FileSegmentStore: fs}

	l, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  128,
		Compact:          true,
		SegmentStore:     store,
		DisableAutoClean: true,
		// Tier retention that would otherwise reclaim aggressively: a delete is
		// a tier write too, so SkipTiered has to suppress it as well.
		MaxTierBytes: 1,
	})
	defer cleanup()

	var last int64
	for i := 0; i < 16; i++ {
		offs, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("value padding")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)
	n, err := l.OffloadBefore(last)
	require.NoError(t, err)
	require.Positive(t, n)

	// More local records, so the pass has genuine local work to do.
	for i := 0; i < 16; i++ {
		offs, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("value padding")}})
		require.NoError(t, err)
		last = offs[0]
	}
	for _, k := range []string{"pad0", "pad1"} {
		offs, err := l.Append([]*Message{{Key: []byte(k), Value: []byte("p")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)

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

	store.puts, store.deletes = 0, 0

	hw := l.HighWatermark()
	_, superseded, err := l.CleanWithSpec(CleanSpec{
		Ceiling:          hw + 1,
		TombstoneGCBelow: hw + 1,
		RewriteBudget:    time.Hour, // generous: only SkipTiered may hold it back
		SkipTiered:       true,
	})
	require.NoError(t, err)

	require.Zero(t, store.puts, "a skipped tier must take no uploads at all")
	require.Zero(t, store.deletes,
		"a retention delete is a tier write too, so it must be suppressed as well")
	require.Empty(t, superseded, "nothing was superseded, so nothing to hand back")

	// No generation moved.
	l.mu.RLock()
	for _, s := range l.segments {
		s.RLock()
		if s.store != nil {
			if before, ok := keysBefore[s.BaseOffset]; ok {
				require.Equal(t, before, s.storeKey,
					"segment %d was rewritten despite SkipTiered", s.BaseOffset)
			}
		}
		s.RUnlock()
	}
	l.mu.RUnlock()

	require.NotEmpty(t, readFrom(t, l), "the log must still read")
}

// The point of skipping the tier is that LOCAL compaction still happens — a
// non-owner wants its own disk reclaimed, it just must not touch shared
// storage.
func TestSkipTieredStillCompactsLocalSegments(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	l, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  128,
		Compact:          true,
		SegmentStore:     store,
		DisableAutoClean: true,
	})
	defer cleanup()

	var last int64
	for i := 0; i < 12; i++ {
		offs, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("value padding")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)
	_, err = l.OffloadBefore(last)
	require.NoError(t, err)

	// Local segments full of superseded copies of one key.
	for i := 0; i < 20; i++ {
		offs, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("value padding")}})
		require.NoError(t, err)
		last = offs[0]
	}
	for _, k := range []string{"pad0", "pad1"} {
		offs, err := l.Append([]*Message{{Key: []byte(k), Value: []byte("p")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)

	before := 0
	l.mu.RLock()
	for _, s := range l.segments {
		s.RLock()
		if s.store == nil {
			before += int(s.MessageCount())
		}
		s.RUnlock()
	}
	l.mu.RUnlock()

	hw := l.HighWatermark()
	_, _, err = l.CleanWithSpec(CleanSpec{
		Ceiling:          hw + 1,
		TombstoneGCBelow: hw + 1,
		RewriteBudget:    time.Hour,
		SkipTiered:       true,
	})
	require.NoError(t, err)

	after := 0
	l.mu.RLock()
	for _, s := range l.segments {
		s.RLock()
		if s.store == nil {
			after += int(s.MessageCount())
		}
		s.RUnlock()
	}
	l.mu.RUnlock()

	require.Less(t, after, before,
		"local segments must still compact — skipping the tier is not skipping the pass")
}
