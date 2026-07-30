package commitlog

import (
	"context"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ligustah/commitlog/compress"
)

// indexKeyCountingStore counts ReadAt calls against .index objects, so a test can
// prove boot does not fetch any remote index until the first seek.
type indexKeyCountingStore struct {
	*FileSegmentStore
	indexReads atomic.Int64
}

func (s *indexKeyCountingStore) ReadAt(key string, p []byte, off int64) (int, error) {
	if strings.HasSuffix(key, indexSuffix) {
		s.indexReads.Add(1)
	}
	return s.FileSegmentStore.ReadAt(key, p, off)
}

// offloadIndexEnv sets up a small-segment log with a store and a remote index
// cache, plus append/read helpers, so a few appends roll several sealed segments
// that can be offloaded index-and-all.
type offloadIndexEnv struct {
	t     *testing.T
	dir   string
	store *indexKeyCountingStore
	cache *RemoteIndexCache
	codec compress.Codec
}

func (e *offloadIndexEnv) open() CommitLog {
	l, err := New(Options{
		Name:             "offload",
		Path:             filepath.Join(e.dir, "log"),
		MaxSegmentBytes:  512,
		Compression:      e.codec,
		SegmentStore:     e.store,
		RemoteIndexCache: e.cache,
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return l
}

func newOffloadIndexEnv(t *testing.T, codec compress.Codec) *offloadIndexEnv {
	dir := t.TempDir()
	fs, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	store := &indexKeyCountingStore{FileSegmentStore: fs}
	cache, err := NewRemoteIndexCache(filepath.Join(dir, "idxcache"), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cache.Close() })
	return &offloadIndexEnv{t: t, dir: dir, store: store, cache: cache, codec: codec}
}

func (e *offloadIndexEnv) appendMsg(l CommitLog, v string) int64 {
	offs, err := l.Append([]*Message{{Value: []byte(v)}})
	if err != nil {
		e.t.Fatal(err)
	}
	l.SetHighWatermark(offs[0])
	return offs[0]
}

func (e *offloadIndexEnv) readVal(l CommitLog, off int64) string {
	r, err := l.NewReader(From(off), Follow())
	if err != nil {
		e.t.Fatal(err)
	}
	msg, _, _, _, err := r.ReadMessage(context.Background(), make([]byte, HeaderBufferLen))
	if err != nil {
		e.t.Fatalf("read offset %d: %v", off, err)
	}
	return string(msg.Value())
}

func testOffloadIndexLifecycle(t *testing.T, codec compress.Codec) {
	e := newOffloadIndexEnv(t, codec)
	l := e.open()

	const n = 40
	vals := make([]string, n)
	offs := make([]int64, n)
	for i := 0; i < n; i++ {
		vals[i] = "record-" + string(rune('A'+i%26)) + "-padded-so-many-segments-roll-here"
		offs[i] = e.appendMsg(l, vals[i])
	}

	count, err := l.OffloadBefore(offs[n-3]) // keep the active tail local
	if err != nil {
		t.Fatal(err)
	}
	if count == 0 {
		t.Fatal("expected segments offloaded")
	}

	// Both the log and index objects are in the store; the offloaded segments'
	// local .log and .index files are gone.
	keys, err := e.store.List()
	if err != nil {
		t.Fatal(err)
	}
	var logObjs, idxObjs int
	for _, k := range keys {
		if strings.HasSuffix(k, logSuffix) {
			logObjs++
		} else if strings.HasSuffix(k, indexSuffix) {
			idxObjs++
		}
	}
	if logObjs != count || idxObjs != count {
		t.Fatalf("store has %d log + %d index objects; want %d each", logObjs, idxObjs, count)
	}
	localIdx, _ := filepath.Glob(filepath.Join(e.dir, "log", "*"+indexSuffix))
	if len(localIdx) > n { // only the still-local tail segments keep an index
		t.Fatalf("unexpectedly many local index files: %d", len(localIdx))
	}

	// Every record reads correctly: the offloaded prefix reads through the store
	// with its index fetched into the cache, the tail is still local.
	for i := 0; i < n; i++ {
		if got := e.readVal(l, offs[i]); got != vals[i] {
			t.Fatalf("read offset %d = %q; want %q", offs[i], got, vals[i])
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a process restart: close the old cache (releasing its mmaps, as
	// process exit would) and open a fresh one over the same dir, which clears it.
	// A post-restart seek then genuinely re-fetches rather than hitting warm
	// entries.
	if err := e.cache.Close(); err != nil {
		t.Fatal(err)
	}
	freshCache, err := NewRemoteIndexCache(filepath.Join(e.dir, "idxcache"), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { freshCache.Close() })
	e.cache = freshCache

	// Reopen from the v2 markers. Boot must not fetch any remote index.
	e.store.indexReads.Store(0)
	l2 := e.open()
	defer l2.Close()
	if got := e.store.indexReads.Load(); got != 0 {
		t.Fatalf("boot fetched %d remote index reads; want 0 (lazy open)", got)
	}

	// A seek into an offloaded segment fetches its index and reads correctly.
	if got := e.readVal(l2, offs[0]); got != vals[0] {
		t.Fatalf("after reopen, read offset %d = %q; want %q", offs[0], got, vals[0])
	}
	if e.store.indexReads.Load() == 0 {
		t.Fatal("a seek into an offloaded segment should have fetched its index")
	}
	for i := 0; i < n; i++ {
		if got := e.readVal(l2, offs[i]); got != vals[i] {
			t.Fatalf("after reopen, read offset %d = %q; want %q", offs[i], got, vals[i])
		}
	}
}

// The full option-2 lifecycle for raw segments: offload index-and-all, read
// through, reopen lazily, and read again.
func TestOffloadIndex_Raw(t *testing.T) { testOffloadIndexLifecycle(t, compress.None) }

// Same lifecycle for block-compressed segments (sparse index).
func TestOffloadIndex_Block(t *testing.T) { testOffloadIndexLifecycle(t, compress.Snappy) }
