package commitlog

import (
	"bytes"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// countingStore wraps a FileSegmentStore and counts Size calls, one per cache
// download, so tests can prove hits don't re-fetch and evicted entries re-fetch.
type countingStore struct {
	*FileSegmentStore
	sizes atomic.Int64
}

func (c *countingStore) Size(key string) (int64, error) {
	c.sizes.Add(1)
	return c.FileSegmentStore.Size(key)
}

// writeIndexObject builds a real (sealed, shrunk) index and stores its bytes
// under key, so the cache downloads and opens a genuine index.
func writeIndexObject(t *testing.T, store SegmentStore, key string, baseOffset int64, entries []*entry) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "src.index")
	idx, err := newIndex(options{path: p, baseOffset: baseOffset})
	require.NoError(t, err)
	// newIndex pre-allocates and leaves position at the file size; the real
	// setup path resets it via InitializePosition before writing. Mirror that,
	// or entries land at the end of the pre-allocated file.
	_, err = idx.InitializePosition()
	require.NoError(t, err)
	require.NoError(t, idx.writeEntries(entries))
	require.NoError(t, idx.Shrink())
	require.NoError(t, idx.Close())
	b, err := os.ReadFile(p)
	require.NoError(t, err)
	require.NoError(t, store.Put(key, bytes.NewReader(b), int64(len(b))))
}

func newCountingStore(t *testing.T) *countingStore {
	fs, err := NewFileSegmentStore(t.TempDir())
	require.NoError(t, err)
	return &countingStore{FileSegmentStore: fs}
}

func TestRemoteIndexCache_AcquireReadsEntries(t *testing.T) {
	store := newCountingStore(t)
	ents := []*entry{
		{Offset: 100, Timestamp: 11, Position: 0, Size: 10},
		{Offset: 101, Timestamp: 22, Position: 10, Size: 12},
	}
	writeIndexObject(t, store, "k.index", 100, ents)

	c, err := NewRemoteIndexCache(filepath.Join(t.TempDir(), "cache"), 1<<20)
	require.NoError(t, err)
	defer c.Close()

	idx, release, err := c.acquire(store, "k.index", 100)
	require.NoError(t, err)
	defer release()

	var e entry
	require.NoError(t, idx.ReadEntryAtFileOffset(&e, 0))
	require.Equal(t, int64(100), e.Offset)
	require.Equal(t, int64(11), e.Timestamp)
	require.NoError(t, idx.ReadEntryAtFileOffset(&e, entryWidth))
	require.Equal(t, int64(101), e.Offset)
	require.Equal(t, int64(22), e.Timestamp)
}

func TestRemoteIndexCache_HitDoesNotRefetch(t *testing.T) {
	store := newCountingStore(t)
	writeIndexObject(t, store, "k.index", 0, []*entry{{Offset: 0, Timestamp: 1, Position: 0, Size: 8}})

	c, err := NewRemoteIndexCache(filepath.Join(t.TempDir(), "cache"), 1<<20)
	require.NoError(t, err)
	defer c.Close()

	_, rel1, err := c.acquire(store, "k.index", 0)
	require.NoError(t, err)
	rel1()
	_, rel2, err := c.acquire(store, "k.index", 0)
	require.NoError(t, err)
	rel2()

	require.Equal(t, int64(1), store.sizes.Load(), "a cache hit must not re-download")
}

func TestRemoteIndexCache_EvictsLRUOverBudget(t *testing.T) {
	store := newCountingStore(t)
	// Each object is one entry = entryWidth (20) bytes.
	writeIndexObject(t, store, "a.index", 0, []*entry{{Offset: 0, Timestamp: 1, Position: 0, Size: 8}})
	writeIndexObject(t, store, "b.index", 0, []*entry{{Offset: 0, Timestamp: 1, Position: 0, Size: 8}})

	// Budget holds one entry (entryWidth) but not two.
	c, err := NewRemoteIndexCache(filepath.Join(t.TempDir(), "cache"), entryWidth+4)
	require.NoError(t, err)
	defer c.Close()

	_, relA, err := c.acquire(store, "a.index", 0)
	require.NoError(t, err)
	relA()
	_, relB, err := c.acquire(store, "b.index", 0)
	require.NoError(t, err)
	relB()

	// a was the LRU and unpinned, so acquiring b evicted it; re-acquiring a
	// re-downloads.
	require.Equal(t, int64(2), store.sizes.Load())
	_, relA2, err := c.acquire(store, "a.index", 0)
	require.NoError(t, err)
	relA2()
	require.Equal(t, int64(3), store.sizes.Load(), "evicted entry must re-download")
}

func TestRemoteIndexCache_PinnedNotEvicted(t *testing.T) {
	store := newCountingStore(t)
	writeIndexObject(t, store, "a.index", 0, []*entry{{Offset: 0, Timestamp: 1, Position: 0, Size: 8}})
	writeIndexObject(t, store, "b.index", 0, []*entry{{Offset: 0, Timestamp: 1, Position: 0, Size: 8}})

	c, err := NewRemoteIndexCache(filepath.Join(t.TempDir(), "cache"), entryWidth+4)
	require.NoError(t, err)
	defer c.Close()

	// Hold a pinned (do not release).
	idxA, relA, err := c.acquire(store, "a.index", 0)
	require.NoError(t, err)

	// Acquiring b puts the cache over budget, but a is pinned and must survive.
	_, relB, err := c.acquire(store, "b.index", 0)
	require.NoError(t, err)
	relB()

	// a is still valid and served from cache (no re-download) — it was skipped by
	// eviction because it was pinned.
	var e entry
	require.NoError(t, idxA.ReadEntryAtFileOffset(&e, 0))
	require.Equal(t, int64(2), store.sizes.Load(), "pinned entry must not have been evicted/refetched")

	// Once released, a is evictable. A fresh miss (c) runs eviction with a
	// unpinned, reclaiming it; re-acquiring a then re-downloads.
	relA()
	writeIndexObject(t, store, "c.index", 0, []*entry{{Offset: 0, Timestamp: 1, Position: 0, Size: 8}})
	_, relC, err := c.acquire(store, "c.index", 0)
	require.NoError(t, err)
	relC()

	_, relA2, err := c.acquire(store, "a.index", 0)
	require.NoError(t, err)
	relA2()
	require.Greater(t, store.sizes.Load(), int64(3), "unpinned entry becomes evictable after release")
}
