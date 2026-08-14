package commitlog

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A store's io.EOF may be WRAPPED, and the cache download is a third place that
// reads a caller's store.
//
// The ReadAt contract on SegmentStore already says this: its sentinels may be
// wrapped and commitlog compares with errors.Is. That sentence was written when
// the two sites in storeBacking were fixed, and this loop was not one of them —
// it broke on `rerr == io.EOF` and treated everything else as fatal. So a store
// that wraps its errors, which is what Go code does, turned the ordinary end of
// an object into "read remote index": every cold seek into an offloaded segment
// failed, after writing the whole index to disk and then deleting it.
//
// The fixture is a store whose Size is honest and whose ReadAt wraps. The very
// first read reaches the end of a small index object, so this is not a rare
// arm — it is every download.
func TestACachedIndexFetchAcceptsAWrappedEndOfObject(t *testing.T) {
	fs, err := NewFileSegmentStore(t.TempDir())
	require.NoError(t, err)
	store := wrappedErrStore{fs}

	entries := []*entry{
		{Offset: 100, Timestamp: 11, Position: 0, Size: 8},
		{Offset: 101, Timestamp: 12, Position: 8, Size: 8},
	}
	writeIndexObject(t, fs, "k.index", 100, entries)

	c, err := NewRemoteIndexCache(filepath.Join(t.TempDir(), "cache"), 1<<20)
	require.NoError(t, err)
	defer c.Close()

	idx, release, err := c.acquire(store, "k.index", 100)
	require.NoError(t, err,
		"a store that wraps its io.EOF could not have its index downloaded at all")
	defer release()

	var e entry
	require.NoError(t, idx.ReadEntryAtFileOffset(&e, 0))
	require.Equal(t, int64(100), e.Offset, "the cached index is not the one stored")
}

// shortObjectStore reports an object as longer than it is — the store
// disagreeing with itself between Size and ReadAt.
type shortObjectStore struct {
	*FileSegmentStore
	extra int64
}

func (s shortObjectStore) Size(key string) (int64, error) {
	n, err := s.FileSegmentStore.Size(key)
	if err != nil {
		return n, err
	}
	return n + s.extra, nil
}

// An object that ends before the size the store itself just reported is refused,
// not cached.
//
// This is NOT the stale recorded size storeBacking tolerates. That size comes
// out of a manifest written long before the read; this one was asked of the
// store one call earlier, so a short read means the object is not the object
// Size described.
//
// What accepting it produced was worse than a failure: newIndex maps whatever
// landed and reads it as a complete index, so every seek resolves against a
// truncated table. Missing entries come back as "not found" for offsets the
// segment holds — a wrong answer, arriving silently, from a cache that reported
// success.
func TestACachedIndexShorterThanItsReportedSizeIsRefused(t *testing.T) {
	fs, err := NewFileSegmentStore(t.TempDir())
	require.NoError(t, err)

	writeIndexObject(t, fs, "k.index", 100, []*entry{
		{Offset: 100, Timestamp: 11, Position: 0, Size: 8},
	})

	c, err := NewRemoteIndexCache(filepath.Join(t.TempDir(), "cache"), 1<<20)
	require.NoError(t, err)
	defer c.Close()

	_, _, err = c.acquire(shortObjectStore{FileSegmentStore: fs, extra: entryWidth}, "k.index", 100)
	require.Error(t, err, "a short object was cached as if it were the whole index")
	require.Contains(t, err.Error(), "the store reported",
		"the refusal must name the disagreement it found")
}

// zeroReadStore answers reads with (0, nil), which io.ReaderAt forbids. It gives
// up after enough calls to prove the loop kept asking, so a regression fails
// rather than hanging the suite.
type zeroReadStore struct {
	*FileSegmentStore
	calls atomic.Int64
}

var errZeroReadGaveUp = errors.New("test store: asked too many times")

func (s *zeroReadStore) ReadAt(key string, p []byte, off int64) (int, error) {
	if s.calls.Add(1) > 64 {
		return 0, fmt.Errorf("after %d calls: %w", s.calls.Load(), errZeroReadGaveUp)
	}
	return 0, nil
}

// A store that returns no bytes and no error is named as the contract breach it
// is, rather than asked again forever.
//
// The loop's only other response to (0, nil) is another ReadAt at the same
// offset, which is a seek that never returns and never says why — the hardest
// possible failure to diagnose from the outside, and one this package can end in
// a line.
func TestAStoreReturningNothingIsRefusedRatherThanRetriedForever(t *testing.T) {
	fs, err := NewFileSegmentStore(t.TempDir())
	require.NoError(t, err)

	writeIndexObject(t, fs, "k.index", 100, []*entry{
		{Offset: 100, Timestamp: 11, Position: 0, Size: 8},
	})

	c, err := NewRemoteIndexCache(filepath.Join(t.TempDir(), "cache"), 1<<20)
	require.NoError(t, err)
	defer c.Close()

	store := &zeroReadStore{FileSegmentStore: fs}
	type result struct{ err error }
	done := make(chan result, 1)
	go func() {
		_, _, aerr := c.acquire(store, "k.index", 100)
		done <- result{aerr}
	}()

	select {
	case r := <-done:
		require.Error(t, r.err)
		require.False(t, errors.Is(r.err, errZeroReadGaveUp),
			"the loop kept asking until the store gave up, rather than refusing the first (0, nil)")
		require.Contains(t, r.err.Error(), "0 bytes and no error",
			"the refusal must name what the store did")
	case <-time.After(30 * time.Second):
		t.Fatal("the fetch never returned")
	}
	require.Equal(t, int64(1), store.calls.Load(),
		"one (0, nil) is enough to know; asking again cannot change it")
}
