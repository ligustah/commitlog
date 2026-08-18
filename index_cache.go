package commitlog

import (
	"container/list"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
)

// defaultRemoteIndexCacheBytes is the RemoteIndexCache's default on-disk budget,
// matching Kafka's RemoteIndexCache default (1 GiB). It is a ceiling, so the
// local footprint only grows with the actual cold-read working set.
const defaultRemoteIndexCacheBytes = 1 << 30

// RemoteIndexCache is a process-wide, byte-budgeted LRU cache of segment indexes
// that have been offloaded to a SegmentStore alongside their log bytes (tiered
// storage, option 2). A sealed index is fixed-size, so a cached entry is the
// index object downloaded to a local file and opened exactly like a local index
// — offset/timestamp seeks work unchanged. One cache is shared across every log
// in a process (a single budget, Kafka's RemoteIndexCache model); evicting an
// entry closes its index and deletes the cached file. Fetches are lazy: an
// offloaded segment downloads its index only on the first seek into it, so boot
// never pays for cold segments' indexes.
type RemoteIndexCache struct {
	dir      string
	maxBytes int64

	mu      sync.Mutex
	entries map[string]*list.Element // index object key -> *cachedIndex element
	lru     *list.List               // most-recently-used at the front
	total   int64                    // sum of cached entries' on-disk bytes
	// inflight names the object keys a download is currently running for, so a
	// second acquire of the same key waits instead of starting its own. See
	// acquire: the download deliberately runs outside mu, and the cache file it
	// writes is named from the key alone, so "run both and discard one" is not
	// available here — the two would be writing the same file.
	inflight map[string]chan struct{}
}

// cachedIndex is one entry: the downloaded index file, opened, plus a pin count
// so an index in use by a live seek is never evicted out from under it.
type cachedIndex struct {
	objectKey string
	idx       *index
	path      string
	bytes     int64
	refs      int
	// stale marks an entry Invalidate has detached from the cache while a seek
	// still held it. It is no longer findable or evictable; the last release
	// closes it. Without this an invalidated-but-pinned entry would either be
	// closed under a live reader or leak its file forever.
	stale bool
}

func (ci *cachedIndex) close() {
	if ci.idx != nil {
		// Discarding: the file is removed on the next line, and a cached index is
		// a read-only download of an object the store still holds — there is
		// nothing dirty to flush even in principle.
		_ = ci.idx.CloseDiscarding()
	}
	_ = os.Remove(ci.path)
}

// NewRemoteIndexCache creates a cache rooted at dir with a total on-disk byte
// budget (<= 0 uses the 1 GiB default). It clears any files left in dir by a
// previous run: cached indexes are pure derived data, always re-fetchable from
// the store, so a stale file must never be trusted across a restart.
func NewRemoteIndexCache(dir string, maxBytes int64) (*RemoteIndexCache, error) {
	if maxBytes <= 0 {
		maxBytes = defaultRemoteIndexCacheBytes
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, errors.Wrap(err, "create remote index cache dir")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, errors.Wrap(err, "read remote index cache dir")
	}
	for _, e := range entries {
		_ = os.Remove(filepath.Join(dir, e.Name()))
	}
	return &RemoteIndexCache{
		dir:      dir,
		maxBytes: maxBytes,
		entries:  make(map[string]*list.Element),
		inflight: make(map[string]chan struct{}),
		lru:      list.New(),
	}, nil
}

// cacheFileName maps an index object key to a collision-free filename in the
// cache dir. The object key carries a random per-upload id, so two streams'
// like-named index objects (both "…000.index") never share a file.
func (c *RemoteIndexCache) cacheFileName(objectKey string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(objectKey))
	return filepath.Join(c.dir, fmt.Sprintf("%016x.index", h.Sum64()))
}

// acquire returns the index for an offloaded segment, downloading its index
// object (objectKey) from store into the cache on a miss, and a release func the
// caller MUST call when done seeking. baseOffset opens the index. The entry is
// pinned between acquire and release, so it is never evicted while a seek holds
// it.
//
// The cache is keyed by the OBJECT KEY, and that choice is load-bearing. It used
// to be keyed by the segment's log path and base offset, which is unique across
// every log in a process but NOT across time: delete a log's directory, create a
// new log at the same path, and its segments restart at base offset 0 and
// produce byte-identical keys. A hit then returned the dead log's index without
// consulting the store at all — and an index applied to a different log's bytes
// is not a stale answer, it is a wrong one. Reported downstream: a seek for
// offset 5 began at 7, in order, with no error.
//
// An object key cannot do that. newStoreKeys mints a fresh 128-bit id for every
// upload attempt, and openOffloadedSegment takes the key from the manifest
// verbatim, so it is unique across logs and across incarnations by construction —
// with no nonce to persist and no invalidation for a caller to remember.
// One download runs per key at a time, and that is a correctness requirement
// rather than a saving. The download runs outside c.mu, because it is I/O, and
// it writes to cacheFileName(objectKey) — a path derived from the key and
// nothing else. So two acquires of one key are two writers of ONE FILE: the
// second os.Create TRUNCATES a file the first has already opened and MMAPPED,
// which is a SIGBUS in the first one's next seek on unix, and on Windows a
// discard of the loser that deletes the winner's file out from under it.
//
// It was reachable by ordinary use, not by a pathological caller: withIndex
// runs under the segment READ lock, so two readers seeking into the same cold
// segment race here by design. The arrangement it replaces — fetch twice and
// discard the loser — is the right shape only when the two attempts are
// independent, and a shared destination is exactly what makes them not.
func (c *RemoteIndexCache) acquire(store SegmentStore, objectKey string, baseOffset int64) (*index, func(), error) {
	for {
		c.mu.Lock()
		if el, ok := c.entries[objectKey]; ok {
			ci := el.Value.(*cachedIndex)
			ci.refs++
			c.lru.MoveToFront(el)
			c.mu.Unlock()
			return ci.idx, func() { c.release(ci) }, nil
		}
		if wait, ok := c.inflight[objectKey]; ok {
			c.mu.Unlock()
			<-wait
			// Round again rather than taking the leader's result: by now the
			// entry is normally in the map, but it may have been evicted, or the
			// leader's fetch may have failed. Both are ordinary states this loop
			// already handles, and re-reading the map is what makes the pin the
			// waiter takes belong to the entry that actually survived.
			continue
		}
		done := make(chan struct{})
		c.inflight[objectKey] = done
		c.mu.Unlock()

		ci, err := c.fetch(store, objectKey, baseOffset)

		c.mu.Lock()
		delete(c.inflight, objectKey)
		if err != nil {
			c.mu.Unlock()
			close(done)
			// The waiters retry, each in turn, exactly as they would have if this
			// caller had never arrived. A failed fetch is not a verdict to share:
			// the store may have been briefly unreachable, and broadcasting one
			// caller's error would fail every other caller on evidence that is
			// already stale.
			return nil, nil, err
		}
		ci.refs = 1
		el := c.lru.PushFront(ci)
		c.entries[objectKey] = el
		c.total += ci.bytes
		// Safe against the entry just inserted: refs is 1, and eviction skips
		// anything pinned.
		c.evictLocked()
		c.mu.Unlock()
		// After the insert, so a waiter that wakes finds the entry rather than
		// racing back around to fetch it again.
		close(done)
		return ci.idx, func() { c.release(ci) }, nil
	}
}

// fetch downloads the index object to the cache dir and opens it.
func (c *RemoteIndexCache) fetch(store SegmentStore, objectKey string, baseOffset int64) (*cachedIndex, error) {
	size, err := store.Size(objectKey)
	if err != nil {
		return nil, errors.Wrap(err, "remote index size")
	}
	// Refused here rather than left to the short-read check below, which cannot
	// see it: that check asks whether the download reached the size the store
	// reported, and for a size of zero it reached it exactly. Zero is the one
	// length between the two checks that already stand here — the (0, nil)
	// contract breach inside the loop, and the ended-early check after it.
	//
	// What got through was worse than a failed seek. newIndex pre-allocates when
	// it finds an EMPTY file, which is the arm a genuinely fresh index takes, so
	// an empty download is indistinguishable from a new index and gets the same
	// treatment: a 10MB zero-filled table, mapped and read as this segment's. Every
	// seek then resolves against it and answers rather than failing. And the entry
	// is recorded as `bytes: size` — zero — so it never counts toward the cache's
	// total, can never be evicted for size, and the budget reads as empty while the
	// disk fills.
	//
	// readStoreDescriptor already refuses `size <= 0` for the same reason, one
	// reader over. Negative is folded in: the loop cannot run for it either.
	if size <= 0 {
		return nil, errors.Errorf(
			"commitlog: store reports remote index %s as %d bytes; an index object "+
				"holding no entries cannot be the index of an offloaded segment",
			objectKey, size)
	}
	path := c.cacheFileName(objectKey)
	f, err := os.Create(path)
	if err != nil {
		return nil, errors.Wrap(err, "create cached index file")
	}
	// Every exit below leaves nothing behind: a partial download is not a smaller
	// index, it is bytes newIndex would map and read as a whole one.
	fail := func(err error, msg string) (*cachedIndex, error) {
		f.Close()       // nolint: errcheck — the file is removed next
		os.Remove(path) // nolint: errcheck — best effort on a failure path
		return nil, errors.Wrap(err, msg)
	}
	buf := make([]byte, 64<<10)
	off := int64(0)
	for off < size {
		n, rerr := store.ReadAt(objectKey, buf, off)
		if n > 0 {
			if _, werr := f.Write(buf[:n]); werr != nil {
				return fail(werr, "write cached index")
			}
			off += int64(n)
		}
		if rerr != nil {
			// errors.Is, not ==: store is the CALLER's implementation, and the
			// ReadAt contract on SegmentStore says its sentinels may be wrapped.
			// That contract was written when the two sites in storeBacking were
			// fixed; this is a third site that reads a caller's store and it kept
			// the `==`. Wrapped, a store's ordinary end-of-object arrived here as
			// an outage — so every cold seek into an offloaded segment failed with
			// "read remote index", having already written and then deleted a cache
			// file holding the whole index.
			if errors.Is(rerr, io.EOF) {
				break
			}
			return fail(rerr, "read remote index")
		}
		if n == 0 {
			// (0, nil) violates io.ReaderAt, and the loop's only other response to
			// it is to ask again forever — a seek that never returns and never
			// says why. Named as the contract breach it is.
			return fail(
				errors.Errorf("store returned 0 bytes and no error at offset %d of %d",
					off, size),
				"read remote index")
		}
	}
	// Short of what the store ITSELF reported one call earlier. Not the stale
	// recorded size storeBacking tolerates — that size comes from a manifest
	// written long before, while this one was just asked of the store — so the
	// object is not the object Size described. Refused, because the alternative
	// is a cache file shorter than the index it claims to be: newIndex maps
	// whatever landed, every seek resolves against a truncated table, and the
	// answers are wrong rather than missing.
	if off != size {
		return fail(
			errors.Errorf("object ended after %d of the %d bytes the store reported",
				off, size),
			"read remote index")
	}
	if err := f.Close(); err != nil {
		os.Remove(path) // nolint: errcheck — best effort on a failure path
		return nil, errors.Wrap(err, "close cached index")
	}
	idx, err := newIndex(options{path: path, baseOffset: baseOffset})
	if err != nil {
		os.Remove(path)
		return nil, errors.Wrap(err, "open cached index")
	}
	if _, err := idx.InitializePosition(); err != nil {
		// Discarding: same as above, and this download never became usable.
		idx.CloseDiscarding()
		os.Remove(path)
		return nil, errors.Wrap(err, "initialize cached index")
	}
	return &cachedIndex{objectKey: objectKey, idx: idx, path: path, bytes: size}, nil
}

// release drops one pin on a cached entry, which stays cached (evictable once
// unpinned) until LRU eviction reclaims it.
//
// It takes the entry rather than its key because an invalidated entry is no
// longer in the map, and the pin still has to be dropped — and the last one is
// what closes it.
func (c *RemoteIndexCache) release(ci *cachedIndex) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ci.refs > 0 {
		ci.refs--
	}
	if ci.refs == 0 && ci.stale {
		ci.close()
	}
}

// Invalidate drops the cached index for an index OBJECT KEY, so the next seek
// refetches it from the store rather than reading an index that describes an
// object which no longer exists.
//
// Needed because eviction is LRU-only: without this a cached index outlives the
// object it describes, and there is no size pressure that would reliably remove
// it. A rewrite that replaces a segment's index object must call this, or seeks
// keep resolving against the pre-rewrite layout.
//
// An entry a live seek is holding is detached rather than closed — it stops
// being findable immediately, and the last release closes it — so this never
// pulls an index out from under a reader.
func (c *RemoteIndexCache) Invalidate(objectKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.entries[objectKey]
	if !ok {
		return
	}
	ci := el.Value.(*cachedIndex)
	c.lru.Remove(el)
	delete(c.entries, objectKey)
	c.total -= ci.bytes
	ci.stale = true
	if ci.refs == 0 {
		ci.close()
	}
}

// evictLocked reclaims least-recently-used, unpinned entries until the total is
// back within budget (or nothing more can be evicted). Caller holds c.mu.
func (c *RemoteIndexCache) evictLocked() {
	for c.total > c.maxBytes {
		victim := c.lru.Back()
		for victim != nil && victim.Value.(*cachedIndex).refs > 0 {
			victim = victim.Prev() // skip entries a live seek is holding
		}
		if victim == nil {
			return // everything over budget is currently pinned
		}
		ci := victim.Value.(*cachedIndex)
		c.lru.Remove(victim)
		delete(c.entries, ci.objectKey)
		c.total -= ci.bytes
		ci.close()
	}
}

// Close evicts every entry (closing indexes and deleting cached files). Safe to
// call once at process shutdown.
func (c *RemoteIndexCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, el := range c.entries {
		el.Value.(*cachedIndex).close()
	}
	c.entries = make(map[string]*list.Element)
	c.lru.Init()
	c.total = 0
	return nil
}
