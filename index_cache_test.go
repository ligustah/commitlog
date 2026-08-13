package commitlog

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// gatedStore blocks inside the download so a second acquire of the same key is
// guaranteed to overlap the first, and records every download that STARTED.
type gatedStore struct {
	*FileSegmentStore
	entered chan struct{} // one send per download that reached the store
	release chan struct{} // closed to let them finish
	starts  atomic.Int64
}

func (g *gatedStore) Size(key string) (int64, error) {
	g.starts.Add(1)
	g.entered <- struct{}{}
	<-g.release
	return g.FileSegmentStore.Size(key)
}

// Two concurrent acquires of one key must produce ONE download.
//
// Not a saving — a correctness requirement. The cache file is named from the
// object key alone, so a second download is a second os.Create of the SAME
// path, truncating a file the first acquire has already mmapped. That is a
// SIGBUS in the first reader's next seek on unix, and on Windows the loser's
// discard deletes the winner's file. Two readers seeking into one cold segment
// is ordinary: withIndex runs under the segment read lock.
//
// The gate is what makes this deterministic rather than a race the suite might
// miss. The first acquire is parked inside the store, so the second cannot
// possibly find a cached entry — it either waits for the first, or starts its
// own download, and there is no third outcome to be lucky about.
func TestRemoteIndexCache_ConcurrentAcquiresDownloadOnce(t *testing.T) {
	fs, err := NewFileSegmentStore(t.TempDir())
	require.NoError(t, err)
	store := &gatedStore{
		FileSegmentStore: fs,
		entered:          make(chan struct{}, 4), // buffered: an extra start must not deadlock
		release:          make(chan struct{}),
	}
	writeIndexObject(t, store, "k.index", 0, []*entry{{Offset: 0, Timestamp: 1, Position: 0, Size: 8}})
	store.starts.Store(0) // writeIndexObject's own Put is not a download

	c, err := NewRemoteIndexCache(filepath.Join(t.TempDir(), "cache"), 1<<20)
	require.NoError(t, err)
	defer c.Close()

	var (
		wg   sync.WaitGroup
		idxs [2]*index
		errs [2]error
	)
	acquire := func(i int) {
		defer wg.Done()
		idx, release, err := c.acquire(store, "k.index", 0)
		idxs[i], errs[i] = idx, err
		if err == nil {
			t.Cleanup(release)
		}
	}
	wg.Add(1)
	go acquire(0)
	<-store.entered // the first is now parked inside the download

	wg.Add(1)
	go acquire(1)

	// The second either waits for the first (correct) or reaches the store on its
	// own (the defect). Give it long enough that reaching the store is the only
	// explanation for a second start, then let both finish either way — failing
	// here would leak two goroutines blocked on release.
	secondDownload := false
	select {
	case <-store.entered:
		secondDownload = true
	case <-time.After(time.Second):
	}
	close(store.release)
	wg.Wait()

	require.False(t, secondDownload,
		"a second acquire started its own download into the same cache file")
	require.Equal(t, int64(1), store.starts.Load(), "one key, one download")
	for i := range idxs {
		require.NoError(t, errs[i])
		var e entry
		require.NoError(t, idxs[i].ReadEntryAtFileOffset(&e, 0),
			"acquire %d got an index it cannot read", i)
		require.Equal(t, int64(0), e.Offset)
	}
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
