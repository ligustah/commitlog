package commitlog

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/pkg/errors"
	"github.com/tysonmote/gommap"
)

var errIndexCorrupt = errors.New("corrupt index file")

// gommapMu serializes every gommap.Map / UnsafeUnmap call in this package.
// gommap keeps a package-level handle registry keyed by mapping address with
// no internal locking, so concurrent map/unmap calls from different goroutines
// — an append rolling a segment while the cleaner mmaps a rewrite target —
// race on that registry. Mapping operations are rare (segment create, seal,
// expand, close), so one mutex costs nothing.
var gommapMu sync.Mutex

func mmapFile(f *os.File) (gommap.MMap, error) {
	gommapMu.Lock()
	defer gommapMu.Unlock()
	return gommap.Map(f.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
}

func unmapFile(m gommap.MMap) error {
	gommapMu.Lock()
	defer gommapMu.Unlock()
	return m.UnsafeUnmap()
}

const (
	offsetWidth    = 4
	timestampWidth = 8
	positionWidth  = 4
	sizeWidth      = 4
	entryWidth     = offsetWidth + timestampWidth + positionWidth + sizeWidth
)

type index struct {
	options
	mmap gommap.MMap
	file *os.File
	size int64
	mu   sync.RWMutex
	// mapMu guards the LIFETIME of mmap, so a flush can run without mu and
	// still be sure the mapping it is flushing is not being torn down. Held
	// shared by the flush, exclusively by the unmap/remap paths (expand, shrink,
	// close). Every teardown path also holds mu exclusively, so code that
	// touches the mapping under mu does not need it.
	//
	// Lock order is mu then mapMu, never the reverse.
	mapMu    sync.RWMutex
	position int64
	closed   bool
}

type entry struct {
	Offset      int64
	Timestamp   int64
	LeaderEpoch uint64
	Position    int64
	Size        int32
}

// relEntry is an Entry relative to the base fileOffset
type relEntry struct {
	Offset    int32
	Timestamp int64
	Position  int32
	Size      int32
}

func newRelEntry(e *entry, baseOffset int64) relEntry {
	return relEntry{
		Offset:    int32(e.Offset - baseOffset),
		Timestamp: e.Timestamp,
		Position:  int32(e.Position),
		Size:      e.Size,
	}
}

func (rel relEntry) fill(e *entry, baseOffset int64) {
	e.Offset = baseOffset + int64(rel.Offset)
	e.Timestamp = rel.Timestamp
	e.Position = int64(rel.Position)
	e.Size = rel.Size
}

type options struct {
	path       string
	bytes      int64
	baseOffset int64
}

func newIndex(opts options) (idx *index, err error) {
	if opts.bytes == 0 {
		opts.bytes = 10 * 1024 * 1024
	}
	if opts.path == "" {
		return nil, errors.New("path is empty")
	}
	idx = &index{
		options: opts,
	}
	idx.file, err = os.OpenFile(opts.path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, errors.Wrap(err, "open file failed")
	}
	fi, err := idx.file.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat file failed")
	}
	// Pre-allocate the index if we just created it.
	if fi.Size() == 0 {
		if err := idx.file.Truncate(roundDown(opts.bytes, entryWidth)); err != nil {
			return nil, err
		}
	}
	// Get updated stats after resize.
	fi, err = idx.file.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat file failed")
	}
	idx.position = fi.Size()
	idx.size = fi.Size()

	idx.mmap, err = mmapFile(idx.file)
	if err != nil {
		return nil, errors.Wrap(err, "mmap file failed")
	}
	return idx, nil
}

// Position returns the current position in the index to write to next. This
// value also represents the total length of the index.
func (idx *index) Position() int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.position
}

func (idx *index) CountEntries() int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.position / entryWidth
}

func (idx *index) writeEntries(entries []*entry) (err error) {
	b := new(bytes.Buffer)
	for _, entry := range entries {
		relEntry := newRelEntry(entry, idx.baseOffset)
		if err = binary.Write(b, encoding, relEntry); err != nil {
			return errors.Wrap(err, "binary write failed")
		}
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.closed {
		return ErrSegmentClosed
	}
	if err := idx.writeAt(b.Bytes(), idx.position); err != nil {
		return errors.Wrap(err, "index write failed")
	}
	idx.position += entryWidth * int64(len(entries))
	return nil
}

// ReadEntryAtFileOffset is used to read an index entry at the given
// byte offset of the index file. ReadEntryAtLogOffset is generally
// more useful for higher level use.
func (idx *index) ReadEntryAtFileOffset(e *entry, fileOffset int64) (err error) {
	p := make([]byte, entryWidth)
	if _, err = idx.ReadAt(p, fileOffset); err != nil {
		return err
	}
	b := bytes.NewReader(p)
	rel := &relEntry{}
	err = binary.Read(b, encoding, rel)
	if err != nil {
		return errors.Wrap(err, "binary read failed")
	}
	idx.mu.RLock()
	rel.fill(e, idx.baseOffset)
	idx.mu.RUnlock()
	return nil
}

// ReadEntryAtLogOffset is used to read an index entry at the given
// log offset of the index file.
func (idx *index) ReadEntryAtLogOffset(e *entry, logOffset int64) error {
	return idx.ReadEntryAtFileOffset(e, logOffset*entryWidth)
}

func (idx *index) ReadAt(p []byte, offset int64) (n int, err error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.closed {
		return 0, ErrSegmentClosed
	}
	if idx.position < offset+entryWidth {
		return 0, io.EOF
	}
	// position says an entry is there; the mapping says otherwise. Report it
	// rather than index past the mapping.
	//
	// This should be unreachable — it exists because when it WAS reachable, the
	// symptom was a panic inside a library, which takes the caller's process
	// down with it. An error the caller can handle is strictly better than
	// that, whatever the cause, and a corrupt index is not something to paper
	// over with io.EOF: that would read as "no more entries" and silently
	// truncate whatever was scanning.
	if int64(len(idx.mmap)) < offset+entryWidth {
		return 0, errors.Wrapf(errIndexCorrupt,
			"index %s claims %d bytes but only %d are mapped (reading at %d)",
			idx.path, idx.position, len(idx.mmap), offset)
	}
	n = copy(p, idx.mmap[offset:offset+entryWidth])
	return n, nil
}

func (idx *index) writeAt(p []byte, offset int64) error {
	// Check if we need to expand the index file.
	if pSize := int64(len(p)); offset+pSize >= idx.size {
		// Expand the index file.
		newSize := roundDown(idx.size+idx.bytes, entryWidth)
		if newSize < offset+pSize {
			newSize = idx.size + pSize
		}
		err := idx.file.Truncate(newSize)
		if err != nil {
			panic(errors.Wrap(err, "failed to expand index file"))
		}
		idx.size = newSize

		// Unmap the old index BEFORE creating the new mapping. On Windows,
		// gommap stores mmap handles keyed by virtual address in a package-level
		// map. If MapViewOfFile returns the same address as the old mapping,
		// the new handle overwrites the old one; the subsequent UnsafeUnmap of
		// the old slice then closes the new handle, leaving idx.mmap with an
		// invalid entry and causing ERROR_INVALID_HANDLE on the next Sync.
		//
		// Exclude a concurrent flush for the swap: it holds the mapping shared
		// without mu, so this is the one teardown mu alone does not cover.
		idx.mapMu.Lock()
		// An index shrunk while empty has no mapping to release — a zero-length
		// file cannot be mapped. Unmapping nothing is not an error to report,
		// it is a step that does not apply.
		if oldMmap := idx.mmap; oldMmap != nil {
			if err := unmapFile(oldMmap); err != nil {
				idx.mapMu.Unlock()
				return errors.Wrap(err, "failed to unmap memory mapped index file")
			}
		}
		idx.mmap, err = mmapFile(idx.file)
		idx.mapMu.Unlock()
		if err != nil {
			panic(errors.Wrap(err, "failed to mmap expanded index file"))
		}
	}

	copy(idx.mmap[offset:], p)
	return nil
}

// Sync flushes the index to stable storage WITHOUT holding mu across the
// flush. mu also guards entry writes, so holding it here would block every
// append to this index for the flush's whole duration — and an append blocked
// behind a flush is an append that cannot join a caller's next commit batch,
// which is how group commit gets defeated a layer down. The mapping is pinned
// with mapMu instead, which the remap-on-expand path takes exclusively, so the
// flush can never touch a mapping being torn down.
//
// An entry written during the flush may or may not be covered by it, exactly as
// at the segment level; the caller's next sync covers it.
func (idx *index) Sync() error {
	idx.mu.RLock()
	if idx.closed {
		idx.mu.RUnlock()
		return ErrSegmentClosed
	}
	// Pin the mapping BEFORE releasing mu — in the same order the remap path
	// takes them — so it cannot be unmapped in the gap between the two.
	idx.mapMu.RLock()
	defer idx.mapMu.RUnlock()
	mmap, file := idx.mmap, idx.file
	idx.mu.RUnlock()
	return syncMmap(mmap, file)
}

// sync flushes the index with mu already held exclusively, for the close path
// where the whole teardown is serialized anyway.
func (idx *index) sync() error {
	if idx.closed {
		return ErrSegmentClosed
	}
	return syncMmap(idx.mmap, idx.file)
}

func (idx *index) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.closed {
		return nil
	}
	// Report a failed flush, but NEVER before releasing the mapping and the
	// handle: an index left mapped cannot have its file unlinked on Windows, so
	// bailing out here turns a flush failure into a segment that can never be
	// deleted and a maintenance pass that fails identically forever. Losing the
	// unflushed tail is recoverable; leaking the mapping is not.
	syncErr := idx.sync()
	// Unmap before shrinking: on Windows, SetEndOfFile fails with
	// ERROR_USER_MAPPED_FILE if any view of the file mapping is still open.
	// idx.mmap may already be nil if Shrink() was called on an empty index.
	if idx.mmap != nil {
		idx.mapMu.Lock()
		err := unmapFile(idx.mmap)
		idx.mmap = nil
		idx.mapMu.Unlock()
		if err != nil {
			return err
		}
	}
	if syncErr != nil {
		// The mapping is gone; drop the handle too so the file is removable,
		// then report why the flush failed.
		idx.file.Close() // nolint: errcheck
		idx.closed = true
		return syncErr
	}
	if err := idx.shrink(); err != nil {
		return err
	}
	if err := idx.file.Close(); err != nil {
		return err
	}
	idx.closed = true
	return nil
}

// Shrink truncates the memory-mapped index file to the size of its contents.
// Uses a write lock because the Windows implementation remaps idx.mmap.
func (idx *index) Shrink() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.shrink()
}

func (idx *index) Name() string {
	return idx.file.Name()
}

// offloadReader returns a reader over the index's content bytes (up to position)
// and their length, for uploading the index to a SegmentStore. A sealed index is
// shrunk to its content, so this is the whole meaningful file.
func (idx *index) offloadReader() (io.Reader, int64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.closed {
		return nil, 0, ErrSegmentClosed
	}
	return io.NewSectionReader(idx.file, 0, idx.position), idx.position, nil
}

func (idx *index) InitializePosition() (*entry, error) {
	// Find the first empty entry.
	n := int(idx.size / entryWidth)
	entry := new(entry)
	i := sort.Search(n, func(i int) bool {
		if err := idx.ReadEntryAtFileOffset(entry, int64(i*entryWidth)); err != nil {
			panic(err)
		}
		return entry.Position == 0 && entry.Timestamp == 0 && entry.Size == 0
	})
	// Initialize the position.
	idx.mu.Lock()
	idx.position = int64(i * entryWidth)
	idx.mu.Unlock()

	if i == 0 {
		// Index is empty.
		return nil, nil
	}

	// Return the last entry in the index.
	i--
	if err := idx.ReadEntryAtFileOffset(entry, int64(i*entryWidth)); err != nil {
		return nil, err
	}
	// Do some sanity checks.
	if entry.Offset < idx.baseOffset {
		return nil, errIndexCorrupt
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.position%entryWidth != 0 {
		return nil, errIndexCorrupt
	}
	return entry, nil
}

// numEntries returns how many entries the index currently holds. Valid after
// InitializePosition.
func (idx *index) numEntries() int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.position / entryWidth
}
