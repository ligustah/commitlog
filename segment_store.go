package commitlog

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
)

// segmentBacking is the byte store behind a segment's log file. The default is a
// local file (localBacking); a tiered/remote store implements the same
// interface so an offloaded, sealed segment reads through transparently. Only
// the local, active segment uses Write — remote backings are read-only, since a
// segment is offloaded only after it is sealed.
//
// This is the seam tiered storage builds on: it decouples "where a segment's
// bytes live" from the segment's block/index/scan logic above it, which reads
// exclusively through ReadAt and appends exclusively through Write.
type segmentBacking interface {
	// ReadAt reads len(p) bytes at off, with io.ReaderAt semantics.
	ReadAt(p []byte, off int64) (int, error)
	// Stream returns a reader over the backing's bytes from off to the end,
	// for a caller that knows it is going to read them all. See
	// SegmentStore.Stream for why that distinction is worth expressing.
	//
	// The caller must Close it. Consult StreamPays first — a backing may
	// implement this and still be better read through ReadAt.
	Stream(off int64) (io.ReadCloser, error)
	// StreamPays reports whether a sequential sweep is better served by Stream
	// than by repeated ReadAt.
	//
	// It is false for a local file and true for a store, because the two are
	// paying for different things. A store charges per REQUEST, so collapsing a
	// sweep into one is a large, structural win. A local read costs a syscall
	// against an OS that is already doing readahead for us, and opening a
	// second handle to stream from costs a syscall of its own — measurably
	// worse, not better: routing local scans through a stream made this repo's
	// test suite take five times as long, all of it in file opens.
	StreamPays() bool
	// Write appends p to the backing (active, local segments only).
	Write(p []byte) (int, error)
	// Size returns the current byte length of the backing.
	Size() (int64, error)
	// Sync flushes buffered writes to durable storage (no-op for read-only
	// backings).
	Sync() error
	// Close releases the backing's handle.
	Close() error
	// Name returns a human-readable identifier (the file path for a local
	// backing), used in diagnostics and delete.
	Name() string
}

// localBacking is a segmentBacking over a local *os.File — the default and, for
// the active segment, the only writable backing.
type localBacking struct {
	f *os.File
}

// openLocalBacking opens (creating if needed) an append-mode local file backing.
func openLocalBacking(path string) (*localBacking, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &localBacking{f: f}, nil
}

func (l *localBacking) ReadAt(p []byte, off int64) (int, error) { return l.f.ReadAt(p, off) }

// StreamPays is false: the OS page cache already reads ahead for a sequential
// local read, and opening a handle to stream from costs more than it saves.
func (l *localBacking) StreamPays() bool { return false }

// Stream opens a SECOND handle rather than seeking the backing's own. The
// active segment's handle is in append mode and shared with the writer, so
// seeking it would move the file position out from under an append; and a
// reader that owns its handle can be closed independently of the segment.
//
// Provided for completeness and for a caller that explicitly wants it; scans do
// not use it, because StreamPays is false.
func (l *localBacking) Stream(off int64) (io.ReadCloser, error) {
	f, err := os.Open(l.f.Name())
	if err != nil {
		return nil, err
	}
	if off > 0 {
		if _, err := f.Seek(off, io.SeekStart); err != nil {
			f.Close()
			return nil, err
		}
	}
	return f, nil
}
func (l *localBacking) Write(p []byte) (int, error) { return l.f.Write(p) }
func (l *localBacking) Sync() error                 { return l.f.Sync() }
func (l *localBacking) Close() error                { return l.f.Close() }
func (l *localBacking) Name() string                { return l.f.Name() }

func (l *localBacking) Size() (int64, error) {
	fi, err := l.f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// ErrRestoreRequired is returned by a restore-required tier (e.g. an archival
// store like Glacier) when an offloaded object is read before it has been
// restored to a live tier. Callers promote the segment with a restore step and
// retry.
var ErrRestoreRequired = errors.New("commitlog: segment offloaded to a restore-required tier; restore before reading")

// SegmentStore is a backing store for offloaded (sealed, read-only) segment log
// objects — a tier below local disk. A sealed segment's log bytes are put under
// a key; reads fetch byte ranges from that key. The filesystem implementation
// (FileSegmentStore) ships with commitlog and has no external dependencies;
// cloud/blob stores (S3/GCS/…) implement the same interface from outside
// commitlog so the cloud dependency never enters it.
//
// A store declares its read capability via LiveRead: a live-read store serves
// transparent read-through, while a restore-required store returns
// ErrRestoreRequired until the object is restored.
type SegmentStore interface {
	// Put stores size bytes read from r under key, overwriting any existing
	// object (idempotent re-offload).
	Put(key string, r io.Reader, size int64) error
	// ReadAt reads len(p) bytes at off from the object under key, with
	// io.ReaderAt semantics (a short read returns io.EOF). A restore-required
	// store returns ErrRestoreRequired.
	ReadAt(key string, p []byte, off int64) (int, error)
	// Stream returns a reader over the object under key from off to its end,
	// for a caller that knows it is going to read all of it. The caller must
	// Close it. A restore-required store returns ErrRestoreRequired.
	//
	// This exists because ReadAt alone cannot express "I want the whole
	// object", and against an object store that distinction is the bill: cost
	// is per REQUEST, not per byte. Reading a 1 GiB object through a windowed
	// ReadAt is a thousand GETs for bytes that one GET would have delivered.
	// The scans that dominate — compaction, recovery, digest building, replay
	// — all sweep a segment front to back, so this is the primary read mode
	// rather than an optimisation for an unusual case.
	//
	// It is pull-shaped (returning a reader) rather than push-shaped (a
	// WriteTo) so a caller can stop early without contortions, and because
	// every backend already has one to hand back — gocloud's NewRangeReader,
	// os.Open — which keeps implementations free of adapter code. Put already
	// takes an io.Reader, so the two directions stay symmetric.
	Stream(key string, off int64) (io.ReadCloser, error)
	// Size returns the byte length of the object under key.
	Size(key string) (int64, error)
	// List returns the keys present in the store.
	List() ([]string, error)
	// Delete removes the object under key; deleting an absent key is a no-op.
	Delete(key string) error
	// LiveRead reports whether the store serves transparent read-through
	// (true) or requires an explicit restore before reads succeed (false).
	LiveRead() bool
}

// FileSegmentStore is a filesystem SegmentStore: each offloaded segment's log
// bytes are a file under Dir. It covers the fast-disk → slow-disk tiering case
// with zero external dependencies. It is a live-read store.
type FileSegmentStore struct {
	dir string
}

// NewFileSegmentStore returns a filesystem store rooted at dir, creating it if
// needed.
func NewFileSegmentStore(dir string) (*FileSegmentStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, errors.Wrap(err, "create segment store dir")
	}
	return &FileSegmentStore{dir: dir}, nil
}

// objectPath maps a key to a file path. Keys are log-relative segment
// identifiers (no separators), so the join stays within dir.
func (s *FileSegmentStore) objectPath(key string) string {
	return filepath.Join(s.dir, key)
}

func (s *FileSegmentStore) Put(key string, r io.Reader, size int64) error {
	tmp := s.objectPath(key) + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return errors.Wrap(err, "create store object")
	}
	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		os.Remove(tmp)
		return errors.Wrap(err, "write store object")
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return errors.Wrap(err, "sync store object")
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	// Rename is the commit point: an offloaded object is either fully present or
	// absent, never half-written.
	return os.Rename(tmp, s.objectPath(key))
}

func (s *FileSegmentStore) ReadAt(key string, p []byte, off int64) (int, error) {
	f, err := os.Open(s.objectPath(key))
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return f.ReadAt(p, off)
}

func (s *FileSegmentStore) Stream(key string, off int64) (io.ReadCloser, error) {
	f, err := os.Open(s.objectPath(key))
	if err != nil {
		return nil, err
	}
	if off > 0 {
		if _, err := f.Seek(off, io.SeekStart); err != nil {
			f.Close()
			return nil, err
		}
	}
	return f, nil
}

func (s *FileSegmentStore) Size(key string) (int64, error) {
	fi, err := os.Stat(s.objectPath(key))
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func (s *FileSegmentStore) List() ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	var keys []string
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) == ".tmp" {
			continue
		}
		keys = append(keys, e.Name())
	}
	return keys, nil
}

func (s *FileSegmentStore) Delete(key string) error {
	err := os.Remove(s.objectPath(key))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (s *FileSegmentStore) LiveRead() bool { return true }

// prefetchSize is how far ahead storeBacking reads on a cache miss. Sequential
// scans (recovery, compaction, replay) then serve most reads from the buffer
// instead of a per-frame round trip to the store — the read-ahead the tiered
// design calls for. Sized to comfortably cover a message-set frame plus slack.
const prefetchSize = 1 << 20 // 1 MiB

// storeBacking is a read-only segmentBacking over an offloaded object in a
// SegmentStore. It reads through transparently — a ReadAt fetches from the
// store — with a single read-ahead buffer so a sequential scan doesn't pay a
// store round trip per frame. Writes are rejected: a segment is offloaded only
// after it is sealed.
type storeBacking struct {
	store SegmentStore
	key   string
	size  int64

	// refs counts the readers currently holding this backing — scans that took
	// it and have not closed. A rewrite swaps the segment onto a new object and
	// queues this one for reclamation; the object may only be deleted once this
	// reaches zero, because a reader that took the backing before the swap is
	// still reading the old object and is entitled to finish.
	//
	// Atomic rather than under mu: mu is held across store I/O in refill, and a
	// reader acquiring a backing has no business waiting behind someone else's
	// fetch. The ordering that matters is not with reads anyway — it is that the
	// acquire happens under the SEGMENT lock, so it cannot slip past the swap
	// that decided this backing was finished with. See segmentScanner.
	refs atomic.Int64

	mu     sync.Mutex
	buf    []byte // read-ahead buffer contents
	bufOff int64  // logical offset of buf[0]; -1 when empty
}

// acquire registers a reader against this backing. Callers MUST pair it with
// release, and MUST call it while holding the read lock of the segment they
// took the backing from — see the field comment on refs.
func (b *storeBacking) acquire() { b.refs.Add(1) }

// release drops a reader's claim.
func (b *storeBacking) release() { b.refs.Add(-1) }

// referenced reports whether any reader still holds this backing. Only
// meaningful for a backing a segment has already swapped away from: while a
// segment still serves reads from it, a zero here is a lull, not a conclusion.
func (b *storeBacking) referenced() bool { return b.refs.Load() > 0 }

// acquireBacking registers a reader against b when b is one that can be
// reclaimed, and returns what to release. A local file backing is not
// reference-counted: nothing supersedes it out from under a reader, since a
// local rewrite renames over the source and the reader's file handle stays
// valid until it closes.
func acquireBacking(b segmentBacking) *storeBacking {
	sb, ok := b.(*storeBacking)
	if !ok {
		return nil
	}
	sb.acquire()
	return sb
}

// newStoreBacking opens a read-only backing over key in store. It fetches the
// object size once; a restore-required store surfaces ErrRestoreRequired here.
func newStoreBacking(store SegmentStore, key string) (*storeBacking, error) {
	size, err := store.Size(key)
	if err != nil {
		return nil, err
	}
	return &storeBacking{store: store, key: key, size: size, bufOff: -1}, nil
}

// newStoreBackingSize opens a read-only backing over key with an already-known
// object size, skipping the store round-trip newStoreBacking makes. Used on boot
// from a v2 offload marker (which records the log object's size) so placing a
// cold segment touches the store zero times.
func newStoreBackingSize(store SegmentStore, key string, size int64) (*storeBacking, error) {
	return &storeBacking{store: store, key: key, size: size, bufOff: -1}, nil
}

func (b *storeBacking) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("commitlog: negative read offset %d", off)
	}
	if off >= b.size {
		return 0, io.EOF
	}
	n := 0
	for n < len(p) {
		cur := off + int64(n)
		if cur >= b.size {
			return n, io.EOF
		}
		m, err := b.readOne(p[n:], cur)
		n += m
		if err != nil {
			if err == io.EOF && n == len(p) {
				return n, nil
			}
			return n, err
		}
	}
	return n, nil
}

// readOne serves as much of the request as the read-ahead buffer covers,
// refilling it from the store (a prefetch window) on a miss.
func (b *storeBacking) readOne(p []byte, off int64) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.bufOff < 0 || off < b.bufOff || off >= b.bufOff+int64(len(b.buf)) {
		if err := b.refill(off); err != nil {
			return 0, err
		}
	}
	start := off - b.bufOff
	return copy(p, b.buf[start:]), nil
}

// refill fetches a prefetch window starting at off (clamped to the object) into
// the buffer. A short store read at the tail is expected and fine.
func (b *storeBacking) refill(off int64) error {
	want := int64(prefetchSize)
	if off+want > b.size {
		want = b.size - off
	}
	if cap(b.buf) < int(want) {
		b.buf = make([]byte, want)
	}
	b.buf = b.buf[:want]
	nread, err := b.store.ReadAt(b.key, b.buf, off)
	if err != nil && !(err == io.EOF && nread > 0) {
		b.bufOff = -1
		b.buf = b.buf[:0]
		return err
	}
	b.buf = b.buf[:nread]
	b.bufOff = off
	if nread == 0 {
		return io.EOF
	}
	return nil
}

// Invalidate drops the read-ahead window, so the next read refetches from the
// store instead of serving bytes cached before now.
//
// Unique keys per upload mean a rewrite always writes a NEW object and this
// backing keeps reading the one it opened — which is the point, and why no
// invalidation is needed on that path. This exists for the cases where an
// object CAN change under a live key: any store whose contents are managed
// from outside
// commitlog. Without it the window is held for the backing's lifetime with no
// way to drop it, so a stale extent stays servable indefinitely.
//
// Safe to call concurrently with reads: a read in flight has already copied out
// of the buffer under the same mutex.
// Stream hands back the store's own reader over the object, bypassing the
// read-ahead window entirely. That is the point: the window exists to amortise
// requests for a caller reading in pieces, and a caller that says it wants the
// rest of the object does not need amortising — it needs one request.
// StreamPays is true: a store charges per request, and a sweep served by
// ranged reads pays one per window for bytes a single request would deliver.
func (b *storeBacking) StreamPays() bool { return true }

func (b *storeBacking) Stream(off int64) (io.ReadCloser, error) {
	return b.store.Stream(b.key, off)
}

func (b *storeBacking) Invalidate() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bufOff = -1
	b.buf = b.buf[:0]
}

func (b *storeBacking) Write(p []byte) (int, error) {
	return 0, errors.New("commitlog: offloaded segment is read-only")
}
func (b *storeBacking) Size() (int64, error) { return b.size, nil }
func (b *storeBacking) Sync() error          { return nil }
func (b *storeBacking) Close() error         { return nil }
func (b *storeBacking) Name() string         { return "store:" + b.key }
