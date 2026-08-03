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
		_ = ci.idx.Close()
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
// upload attempt, and openOffloadedSegment takes the key from the offload marker
// verbatim, so it is unique across logs and across incarnations by construction —
// with no nonce to persist and no invalidation for a caller to remember.
func (c *RemoteIndexCache) acquire(store SegmentStore, objectKey string, baseOffset int64) (*index, func(), error) {
	c.mu.Lock()
	if el, ok := c.entries[objectKey]; ok {
		ci := el.Value.(*cachedIndex)
		ci.refs++
		c.lru.MoveToFront(el)
		c.mu.Unlock()
		return ci.idx, func() { c.release(ci) }, nil
	}
	c.mu.Unlock()

	// Miss: download and open outside the lock (it is I/O). A concurrent acquire
	// of the same key may also fetch; the insert below dedups, discarding the
	// loser's copy.
	ci, err := c.fetch(store, objectKey, baseOffset)
	if err != nil {
		return nil, nil, err
	}

	c.mu.Lock()
	if el, ok := c.entries[objectKey]; ok {
		existing := el.Value.(*cachedIndex)
		existing.refs++
		c.lru.MoveToFront(el)
		c.mu.Unlock()
		ci.close() // discard the duplicate we raced to fetch
		return existing.idx, func() { c.release(existing) }, nil
	}
	ci.refs = 1
	el := c.lru.PushFront(ci)
	c.entries[objectKey] = el
	c.total += ci.bytes
	c.evictLocked()
	c.mu.Unlock()
	return ci.idx, func() { c.release(ci) }, nil
}

// fetch downloads the index object to the cache dir and opens it.
func (c *RemoteIndexCache) fetch(store SegmentStore, objectKey string, baseOffset int64) (*cachedIndex, error) {
	size, err := store.Size(objectKey)
	if err != nil {
		return nil, errors.Wrap(err, "remote index size")
	}
	path := c.cacheFileName(objectKey)
	f, err := os.Create(path)
	if err != nil {
		return nil, errors.Wrap(err, "create cached index file")
	}
	buf := make([]byte, 64<<10)
	for off := int64(0); off < size; {
		n, rerr := store.ReadAt(objectKey, buf, off)
		if n > 0 {
			if _, werr := f.Write(buf[:n]); werr != nil {
				f.Close()
				os.Remove(path)
				return nil, errors.Wrap(werr, "write cached index")
			}
			off += int64(n)
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			f.Close()
			os.Remove(path)
			return nil, errors.Wrap(rerr, "read remote index")
		}
	}
	if err := f.Close(); err != nil {
		os.Remove(path)
		return nil, errors.Wrap(err, "close cached index")
	}
	idx, err := newIndex(options{path: path, baseOffset: baseOffset})
	if err != nil {
		os.Remove(path)
		return nil, errors.Wrap(err, "open cached index")
	}
	if _, err := idx.InitializePosition(); err != nil {
		idx.Close()
		os.Remove(path)
		return nil, errors.Wrap(err, "initialize cached index")
	}
	return &cachedIndex{objectKey: objectKey, idx: idx, path: path, bytes: size}, nil
}

// release drops one pin on a cached entry. The entry stays cached (evictable
// once unpinned) until LRU eviction reclaims it.
// release drops one pin. It takes the entry rather than its key because an
// invalidated entry is no longer in the map, and the pin still has to be
// dropped — and the last one is what closes it.
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
