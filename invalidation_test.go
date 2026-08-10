package commitlog

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// readCountingStore counts ReadAt calls per key, so a test can tell a
// served-from-the-window read from one that went back to the store.
type readCountingStore struct {
	SegmentStore
	reads map[string]int
}

func (s *readCountingStore) ReadAt(key string, p []byte, off int64) (int, error) {
	s.reads[key]++
	return s.SegmentStore.ReadAt(key, p, off)
}

// The read-ahead window must be droppable. Without invalidation it is held for
// the backing's lifetime, so an object that changes under a live key keeps
// being served from bytes cached before the change — indefinitely, since
// nothing else clears it.
func TestStoreBackingInvalidateForcesRefetch(t *testing.T) {
	fs, err := NewFileSegmentStore(filepath.Join(tempDir(t), "store"))
	require.NoError(t, err)
	store := &readCountingStore{SegmentStore: fs, reads: map[string]int{}}

	const key = "obj.log"
	payload := []byte("the quick brown fox jumps over the lazy dog")
	require.NoError(t, store.Put(key, bytes.NewReader(payload), int64(len(payload))))

	b := newStoreBackingSize(store, key, int64(len(payload)))

	buf := make([]byte, 3)
	_, err = b.ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, "the", string(buf))
	afterFirst := store.reads[key]
	require.Positive(t, afterFirst)

	// A second read inside the window must not touch the store.
	_, err = b.ReadAt(buf, 4)
	require.NoError(t, err)
	require.Equal(t, "qui", string(buf))
	require.Equal(t, afterFirst, store.reads[key], "the read-ahead must serve this")

	// After invalidating, the same read goes back to the store.
	b.Invalidate()
	_, err = b.ReadAt(buf, 4)
	require.NoError(t, err)
	require.Equal(t, "qui", string(buf))
	require.Greater(t, store.reads[key], afterFirst,
		"an invalidated window must refetch instead of serving cached bytes")
}

func invalidationIndexEntries() []*entry {
	return []*entry{
		{Offset: 0, Timestamp: 11, Position: 0, Size: 10},
		{Offset: 1, Timestamp: 22, Position: 10, Size: 12},
	}
}

// Invalidating an entry nothing holds removes it outright, so the next acquire
// refetches rather than reading an index describing an object that may no
// longer exist.
func TestRemoteIndexCacheInvalidateDropsUnpinnedEntry(t *testing.T) {
	store := newCountingStore(t)
	writeIndexObject(t, store, "k.index", 0, invalidationIndexEntries())

	c, err := NewRemoteIndexCache(filepath.Join(tempDir(t), "cache"), 1<<20)
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })

	_, release, err := c.acquire(store, "k.index", 0)
	require.NoError(t, err)
	release()

	c.mu.Lock()
	_, present := c.entries["k.index"]
	c.mu.Unlock()
	require.True(t, present, "the entry should be cached at this point")

	c.Invalidate("k.index")

	c.mu.Lock()
	_, present = c.entries["k.index"]
	total := c.total
	c.mu.Unlock()
	require.False(t, present, "an invalidated entry must not be findable")
	require.Zero(t, total, "its bytes must leave the budget")

	// The next acquire refetches, which is the whole point.
	before := store.sizes.Load()
	_, release2, err := c.acquire(store, "k.index", 0)
	require.NoError(t, err)
	release2()
	require.Greater(t, store.sizes.Load(), before,
		"an invalidated key must be refetched from the store")
}

// Invalidating an entry a live seek is holding must not close it underneath
// that seek. It is detached immediately — so nothing new finds it — and the
// last release closes it.
func TestRemoteIndexCacheInvalidateDefersWhilePinned(t *testing.T) {
	store := newCountingStore(t)
	writeIndexObject(t, store, "k.index", 0, invalidationIndexEntries())

	c, err := NewRemoteIndexCache(filepath.Join(tempDir(t), "cache"), 1<<20)
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })

	idx, release, err := c.acquire(store, "k.index", 0)
	require.NoError(t, err)

	c.Invalidate("k.index") // while still pinned

	c.mu.Lock()
	_, present := c.entries["k.index"]
	c.mu.Unlock()
	require.False(t, present, "it must stop being findable at once")

	// The held index is still usable — that is what the pin guarantees.
	require.NotNil(t, idx)
	require.EqualValues(t, 2, idx.CountEntries(),
		"an invalidated entry must not be closed under a live seek")

	release() // last pin: now it may close

	// A fresh acquire refetches rather than reviving the detached entry.
	_, release2, err := c.acquire(store, "k.index", 0)
	require.NoError(t, err)
	release2()

	c.mu.Lock()
	_, present = c.entries["k.index"]
	c.mu.Unlock()
	require.True(t, present, "the refetched entry should be cached again")
}

// Invalidating a key the cache never held is a no-op rather than an error, so a
// rewrite need not know whether an index was ever fetched.
func TestRemoteIndexCacheInvalidateUnknownKeyIsNoOp(t *testing.T) {
	c, err := NewRemoteIndexCache(filepath.Join(tempDir(t), "cache"), 1<<20)
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })
	require.NotPanics(t, func() { c.Invalidate("never-cached") })
}
