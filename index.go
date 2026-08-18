package commitlog

import (
	"bytes"
	"encoding/binary"
	stderrors "errors"
	"io"
	"os"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/tysonmote/gommap"
)

var errIndexCorrupt = errors.New("corrupt index file")

// gommapMu serializes every gommap.Map / UnsafeUnmap call in this package.
//
// The registry it protects is on the MAP side, and only on Windows:
// gommap_windows.go's MapRegion writes the package-level mmapAttrs map under no
// lock at all, so two concurrent gommap.Map calls — an append rolling a segment
// while the cleaner maps a rewrite target — are a concurrent map write, which
// the runtime turns into a process-wide throw. The two registries UnsafeUnmap
// mutates (handleMap, fileHandleMap) do have gommap's own handleLock around
// them, and gommap.Map off Windows touches no package state whatsoever. An
// earlier version of this comment said the registry had "no internal locking"
// flatly, which is true of exactly one of the three maps and made the unmap
// side look like it was resting on this mutex. It is not.
//
// Holding this mutex across UnsafeUnmap is therefore redundant, and it is kept
// anyway because dropping it buys almost nothing. gommap's unmap calls flush()
// first, unconditionally, and flush holds handleLock across
// syscall.FlushFileBuffers — so every unmap in the process is already
// serialized on an fsync one level down, whatever this mutex does. Measured on
// 64 dirty 64KB mappings: 3.8ms per unmap with 64-way concurrency and this
// mutex removed entirely, against 28µs for the same teardown issued through
// MapViewOfFile / UnmapViewOfFile directly with no flush.
//
// That 136x is why Close, CloseFlushed and CloseDiscarding are
// indistinguishable on Windows: the fsync they choose between is not the one
// that costs, and the one that costs is not reachable from this side of the
// dependency. A consumer tearing down hundreds of small indexes sees them
// strictly in series. Fixing it means owning the mapping here rather than
// narrowing anything in this file.
//
// Mapping operations are rare (segment create, seal, expand, close), so the
// mutex itself costs nothing beyond what gommap already costs.
var gommapMu sync.Mutex

// mmapFile is a var so a test can make the mapping FAIL. What it guards
// against — an index left claiming more than its mapping covers — is reachable
// only when the OS refuses to map, and unlike the Windows truncate refusal
// there is no way to provoke that for real without a mapping large enough to
// be a hazard of its own. A test that cannot reach the failure path is a test
// that proves the recovery works by never running it.
var mmapFile = func(f *os.File) (gommap.MMap, error) {
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
	// flushes counts calls that actually reached syncMmap. It exists so "this
	// close did NOT fsync" is a claim a test can falsify — without it, a flush
	// skipped and a flush performed leave the same file on disk, and the guard
	// on closeSegment's dirty-mark branch would have nothing to go red against.
	//
	// Atomic because Sync() flushes under mu held SHARED, so two readers can
	// reach it at once. Counted rather than a bool: the interesting failures are
	// "once per segment, forever" rather than "at all".
	flushes atomic.Int64
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
		return nil, errors.New("index path is empty")
	}
	idx = &index{
		options: opts,
	}
	idx.file, err = os.OpenFile(opts.path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, errors.Wrap(err, "open index file")
	}
	fi, err := idx.file.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat index file")
	}
	// Pre-allocate the index if we just created it.
	if fi.Size() == 0 {
		if err := idx.file.Truncate(roundDown(opts.bytes, entryWidth)); err != nil {
			return nil, errors.Wrap(err, "pre-allocate index file")
		}
	}
	// Get updated stats after resize.
	fi, err = idx.file.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat index file after pre-allocation")
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

// searchEntries binary-searches the index for the first entry pred accepts and
// returns its ordinal together with the number of entries — so ordinal == count
// means no entry satisfied pred. It reads into e, which pred is handed on each
// step and which holds the entry found on return.
//
// A read failure inside the search reports the candidate as satisfying pred.
// That is not a way of ignoring it — the error comes back — but it decides
// WHERE a failed search lands, and the direction is the point: reporting true
// sends sort.Search left, so the ordinal it settles on is no higher than the
// true one. Every caller here either treats the error as fatal or uses the
// ordinal as a starting point to scan forward from, and a starting point that
// is too low costs a longer scan while one that is too high steps over the
// record entirely.
//
// This was written out four times: once per lookup path (offset and timestamp)
// and once per index layout (dense entries, and the sparse anchors of a
// block-compressed segment). The two halves of each pair differed only in the
// field compared, and the two pairs had drifted apart in spelling on top of
// that — one addressing entries by file offset and its twin by log offset,
// which ReadEntryAtLogOffset shows to be the same call. Four look-alike loops
// where the real differences are a comparison and an error policy is a place
// where a reader cannot tell which differences are deliberate.
//
// Callers hold whatever lock the index requires; this adds none.
func (idx *index) searchEntries(e *entry, pred func(*entry) bool) (int, int, error) {
	n := int(idx.CountEntries())
	var serr error
	i := sort.Search(n, func(i int) bool {
		if err := idx.ReadEntryAtLogOffset(e, int64(i)); err != nil {
			serr = err
			return true
		}
		return pred(e)
	})
	if serr != nil {
		return 0, n, serr
	}
	if i == n {
		return n, n, nil
	}
	// Re-read: sort.Search's last probe is not necessarily at i, so e holds
	// whichever entry it happened to look at last.
	if err := idx.ReadEntryAtLogOffset(e, int64(i)); err != nil {
		return 0, n, err
	}
	return i, n, nil
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
	// A write needs room in TWO things, and they can run out separately.
	//
	// This used to ask `offset+pSize >= idx.size` alone, with the recorded size
	// standing in for how much room there is. It stops being true when an
	// expansion fails partway: the file grows, then the old mapping is torn down
	// and a new one built, and both of those steps can fail. size was recorded at
	// the truncate, so a failure after it left size describing a file while the
	// write copies into a mapping — a shorter one if the unmap failed, none at
	// all if the remap did. The next write read size, concluded the room was
	// already there, skipped the expansion, and sliced past the end of the
	// mapping: a panic raised inside a library, in the caller's goroutine.
	// (Slicing a nil mapping at [0:] is legal instead, so an index shrunk while
	// empty went the other way and silently wrote nothing; see Shrink.)
	//
	// Asking the mapping alone is wrong in the opposite direction, and worse. The
	// unix shrink truncates WITHOUT unmapping, so there the mapping outlives the
	// file it describes and stays longer than it: a write inside such a mapping
	// but past the end of the file is a SIGBUS, which is not a Go panic and
	// cannot be recovered. Windows hides that — its shrink must unmap to
	// truncate at all, so the two agree there and only there.
	//
	// So require both. Neither is a proxy for the other, and the write has to
	// land in bytes that are mapped AND backed by the file.
	if pSize := int64(len(p)); offset+pSize >= idx.size || offset+pSize > int64(len(idx.mmap)) {
		// Expand the index file.
		newSize := roundDown(idx.size+idx.bytes, entryWidth)
		if newSize < offset+pSize {
			newSize = idx.size + pSize
		}
		err := idx.file.Truncate(newSize)
		if err != nil {
			return errors.Wrap(err, "failed to expand index file")
		}

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
			idx.mmap = nil
		}
		idx.mmap, err = mmapFile(idx.file)
		idx.mapMu.Unlock()
		if err != nil {
			return errors.Wrap(err, "failed to mmap expanded index file")
		}
		// Recorded only once the mapping it describes exists. size no longer
		// decides whether there is room — the mapping does — but it still sizes
		// the NEXT expansion and reports the entry count, and a value carried
		// over from an expansion that never finished would misstate both.
		idx.size = newSize
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
	idx.flushes.Add(1)
	return syncMmap(mmap, file)
}

// sync flushes the index with mu already held exclusively, for the close path
// where the whole teardown is serialized anyway.
func (idx *index) sync() error {
	if idx.closed {
		return ErrSegmentClosed
	}
	idx.flushes.Add(1)
	return syncMmap(idx.mmap, idx.file)
}

func (idx *index) Close() error {
	return idx.closeIndex(true, true)
}

// CloseFlushed closes an index whose bytes have ALREADY reached stable storage,
// skipping the fsync and doing everything else Close does.
//
// The caller is asserting durability, not guessing at it. segment.dirtyIndex is
// the only thing entitled to make that assertion: it is set by every write and
// cleared only by a flush that RETURNED NIL, so "not dirty" means some earlier
// sync on this index succeeded and nothing has touched it since. An fsync then
// has nothing left to push, and on Windows FlushFileBuffers is not free when it
// has nothing to do — it flushes the DEVICE cache, so its cost tracks whatever
// else on the disk is dirty rather than this file. Closing a log with N sealed
// segments paid that N times in a row for no change to what is on disk.
//
// It still shrinks, because that is unrelated to durability: a pre-allocated
// index is trimmed to its content on the way out whether or not it was flushed.
func (idx *index) CloseFlushed() error {
	return idx.closeIndex(false, true)
}

// CloseDiscarding closes the index for a caller that is about to UNLINK the
// file, so it neither flushes it nor shrinks it.
//
// Both of those are durability work, and durability is meaningless for bytes
// that are about to stop existing: the flush pushes an index to stable storage
// microseconds before the file is removed, and the shrink resizes a file that
// will not outlive the call. On Windows each is a blocking syscall
// (FlushFileBuffers, SetEndOfFile), and a retention pass dropping N segments
// paid both N times — inside the log's write lock, with every reader and
// appender queued behind it. Reported downstream as a 10-minute test timeout
// whose stack was one truncator in FlushFileBuffers and everyone else on the
// mutex.
//
// What it still does is release the mapping and the handle, which is not
// optional: a mapped index cannot be unlinked on Windows at all.
//
// Which caps what this can save on Windows, and the cap is worth stating
// because the paragraph above reads like the fsync goes away entirely. It does
// not: gommap's unmap calls FlushFileBuffers itself, unconditionally, before it
// releases anything (see gommapMu). So this drops ONE of the two fsyncs a
// durable Close pays, not both. Measured by BenchmarkIndexTeardownParts on the
// real close path: 4.55ms durable against 2.52ms here, a 1.81x. The remaining
// 2.52ms is the dependency's and is not reachable from this file.
func (idx *index) CloseDiscarding() error {
	return idx.closeIndex(false, false)
}

// closeIndex releases the mapping and the handle, optionally flushing first and
// trimming after.
//
// flush and trim are SEPARATE because they answer different questions and the
// three callers want three different pairs. They were one `durable` flag while
// only Close and CloseDiscarding existed, which read as one concept but was two
// agreeing by coincidence; CloseFlushed is the case that pulls them apart, and
// folding it back into one flag would silently stop trimming an index it only
// meant to stop flushing.
func (idx *index) closeIndex(flush, trim bool) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.closed {
		return nil
	}
	// Report what failed, but NEVER before releasing the mapping and the handle:
	// an index left mapped cannot have its file unlinked on Windows, so bailing
	// out turns a failure here into a segment that can never be deleted and a
	// maintenance pass that fails identically forever. Losing the unflushed tail
	// is recoverable; leaking the mapping is not.
	//
	// That rule was written for the flush and applied only to the flush, while
	// the two steps after it still returned early — so a refused SetEndOfFile
	// (the shrink) left the index unmapped, the handle open and the index marked
	// OPEN, which is the wedge the flush path exists to avoid, reached one step
	// later. Every failure now runs the teardown to the end and reports at the
	// bottom; each step's error is collected rather than returned.
	var errs []error
	if flush {
		errs = append(errs, idx.sync())
	}
	// Unmap before shrinking: on Windows, SetEndOfFile fails with
	// ERROR_USER_MAPPED_FILE if any view of the file mapping is still open.
	// idx.mmap may already be nil if Shrink() was called on an empty index.
	if idx.mmap != nil {
		idx.mapMu.Lock()
		err := unmapFile(idx.mmap)
		idx.mmap = nil
		idx.mapMu.Unlock()
		errs = append(errs, err)
	}
	// The shrink is the one step worth SKIPPING after an earlier failure. It
	// trims a file that is being closed, so it is an optimization either way —
	// and if the unmap is what failed, the view it could not release is exactly
	// what makes SetEndOfFile refuse, so attempting it would only add a second
	// error describing the first.
	//
	// position == size is the second reason to skip it, and it is the common one:
	// a sealed index on disk was already trimmed, so a reopened segment's file is
	// exactly its content and SetEndOfFile is asked to change nothing. Deciding
	// that from idx.size rather than by stat-ing the file is safe HERE, unlike on
	// the write path where the mapping and the file run out separately — the
	// shrink is an optimization in both directions, so a stale-high size costs a
	// pointless truncate and a stale-low one costs an untrimmed file. Neither is
	// a wrong answer to anything.
	if trim && idx.position < idx.size && stderrors.Join(errs...) == nil {
		errs = append(errs, idx.shrink())
	}
	errs = append(errs, idx.file.Close())
	// Marked closed even when a step failed. The handle is gone either way, so
	// there is nothing a second attempt could release — and leaving it open
	// invites a caller to retry forever against a file it no longer holds.
	idx.closed = true
	return stderrors.Join(errs...)
}

// reset discards every entry, leaving an empty index over the same file. The
// caller is expected to rebuild it from the log immediately; an index nobody
// refills answers every lookup with not-found.
//
// Zeroing rather than truncating, because an empty entry IS how the end of the
// index is found — InitializePosition binary-searches for the first all-zero
// one, so leaving stale bytes past the new position would resurrect them.
func (idx *index) reset() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.closed {
		return ErrSegmentClosed
	}
	n := idx.position
	if mapped := int64(len(idx.mmap)); n > mapped {
		n = mapped
	}
	clear(idx.mmap[:n])
	idx.position = 0
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
	// sort.Search's predicate cannot fail, so the read error is carried out and
	// checked below — the same shape findSegmentIndexByTimestamp uses. Sorting a
	// failed read to the right ends the search promptly; the value of i is then
	// meaningless, which is why nothing uses it before the error is checked.
	var readErr error
	i := sort.Search(n, func(i int) bool {
		if err := idx.ReadEntryAtFileOffset(entry, int64(i*entryWidth)); err != nil {
			readErr = err
			return true
		}
		return entry.Position == 0 && entry.Timestamp == 0 && entry.Size == 0
	})
	if readErr != nil {
		return nil, errors.Wrap(readErr, "failed to read index entry while initializing position")
	}
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
