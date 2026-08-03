package commitlog

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ligustah/commitlog/compress"
	"github.com/pkg/errors"
)

const (
	fileFormat      = "%020d%s"
	logSuffix       = ".log"
	cleanedSuffix   = ".cleaned"
	truncatedSuffix = ".truncated"
	trimmedSuffix   = ".trimmed"
	indexSuffix     = ".index"
	// offloadedSuffix marks a sealed segment whose log bytes live in a
	// SegmentStore rather than a local .log file. The marker file's content is
	// the store key. Its presence (with the local .log absent) tells open() to
	// reopen the segment through the store, keeping the local index.
	offloadedSuffix = ".offloaded"
)

var (
	// ErrEntryNotFound is returned when a segment search cannot find a
	// specific entry.
	ErrEntryNotFound = errors.New("entry not found")

	// ErrSegmentClosed is returned on reads/writes to a closed segment.
	ErrSegmentClosed = errors.New("segment has been closed")

	// ErrSegmentExists is returned when attempting to create a segment that
	// already exists.
	ErrSegmentExists = errors.New("segment already exists")

	// ErrSegmentReplaced is returned when attempting to read from a segment
	// that has been replaced due to log compaction. When this error is
	// encountered, operations should be retried in order to run against the
	// new segment.
	ErrSegmentReplaced = errors.New("segment was replaced")

	// ErrCommitLogDeleted is returned when attempting to read from a commit
	// log that has been deleted.
	ErrCommitLogDeleted = errors.New("commit log was deleted")

	// ErrCommitLogClosed is returned when attempting to read from a commit
	// log that has been closed.
	ErrCommitLogClosed = errors.New("commit log was closed")

	// timestamp returns the current time in Unix nanoseconds. This function
	// exists for mocking purposes.
	timestamp = func() int64 { return time.Now().UnixNano() }
)

type segment struct {
	backing        segmentBacking
	Index          *index
	BaseOffset     int64
	firstOffset    int64
	lastOffset     int64
	firstWriteTime int64
	lastWriteTime  int64
	position       int64
	maxBytes       int64
	path           string
	suffix         string
	waiters        map[interface{}]chan struct{}
	sealed         bool
	closed         bool
	replaced       bool
	// gone marks a segment whose files have been removed (Delete). Distinct from
	// closed, which a segment can also be while its files are intact. See
	// current(). Guarded by the segment lock.
	gone bool
	// replacement is the segment that superseded this one, set by Replace. See
	// current(): it exists because a compaction pass closes each source segment
	// as it rewrites it but does not publish the rewritten list until the whole
	// pass ends, so for that whole window the log's segment list hands out
	// segments that are closed. Guarded by the segment lock.
	replacement *segment
	// dirtyData and dirtyIndex report whether the log file and the index have
	// been written since each was last fsynced, so a durability pass can skip
	// what is already on stable storage instead of paying an fsync per segment
	// per call. They are SEPARATE because the durability hot path flushes log
	// bytes only: a shared mark would be cleared by a data-only sync and the
	// index would then be silently skipped by the next full one.
	//
	// Both start true: a segment opened from disk was written by a process whose
	// flush state we cannot know. Guarded by the segment lock.
	dirtyData  bool
	dirtyIndex bool

	// Block compression. When blockMode is set, each WriteMessageSet is stored
	// as a compressed block and the log's logical byte space (position, index
	// positions, message framing) is decoupled from the physical file layout.
	// position stays logical; physPosition tracks the actual file size. blocks
	// maps logical ranges to physical block locations for reads. codec is the
	// configured codec for new blocks (per-block codec is recorded in headers).
	codec        compress.Codec
	blockMode    bool
	blocks       []blockRef
	physPosition int64
	cache        *blockCache
	// Set when opening this segment dropped an unresolvable tail from the log
	// (a crash mid-append). The index still anchors into the bytes that went,
	// so it has to be rederived rather than trusted.
	droppedTornTail bool

	// Set when the segment's log bytes have been offloaded to a SegmentStore
	// (backing is a *storeBacking). storeKey is the object key; store is the
	// tier it lives in — retained so Delete can remove the object.
	store    SegmentStore
	storeKey string

	// Set additionally when the segment's INDEX has also been offloaded (tiered
	// storage, option 2): the index object lives under indexKey and is fetched on
	// demand into indexCache; Index is nil. For an option-1 offloaded segment
	// (log offloaded, index kept local) and for a local segment these are unset
	// and Index is the resident local index.
	indexKey   string
	indexCache *RemoteIndexCache

	sync.RWMutex
}

// isOffloaded reports whether the segment's log bytes live in a SegmentStore
// rather than a local file. Caller holds at least the read lock.
func (s *segment) isOffloaded() bool { return s.store != nil }

// withIndex runs fn against the segment's index: the resident local index for a
// normal or option-1 offloaded segment, or — for an option-2 offloaded segment
// whose index lives in the store — the index fetched into the shared cache on
// this seek and released after. Callers hold the segment read lock.
func (s *segment) withIndex(fn func(idx *index) error) error {
	if s.Index != nil {
		return fn(s.Index)
	}
	// indexKey is what the cache is keyed by, so an empty one is not a cache
	// miss to be papered over -- it would collide with every other segment in
	// the same state. A segment reaching here without one is an option-1
	// offloaded segment whose LOCAL index failed to open, which is corruption.
	if s.indexCache == nil || s.store == nil || s.indexKey == "" {
		return errIndexCorrupt
	}
	idx, release, err := s.indexCache.acquire(s.store, s.indexKey, s.BaseOffset)
	if err != nil {
		return err
	}
	defer release()
	return fn(idx)
}

func (s *segment) offloadMarkerPath() string {
	return filepath.Join(s.path, fmt.Sprintf(fileFormat, s.BaseOffset, offloadedSuffix))
}

// newStoreKeys returns the object keys for one UPLOAD of the segment at
// baseOffset: the zero-padded base offset, so keys sort and group the way the
// local log filenames do, followed by a value unique to this attempt.
//
// Every upload gets a fresh key, and that is the whole design. Two properties
// come out of it, and neither is available from a deterministic key:
//
//   - A rewrite cannot disturb a reader. SegmentStore.Put overwrites
//     unconditionally and has no compare-and-swap form, so rewriting in place
//     would change an object out from under whoever is reading it. Writing
//     somewhere new instead leaves that reader on a key that cannot change.
//   - A RETRY cannot destroy anything. An upload that failed ambiguously — a
//     timeout, a dropped connection — may still be in flight; retrying to the
//     same key races the original, and a deterministic key makes that race
//     invisible. A fresh key turns it into a spare object instead.
//
// It also means two processes writing one store cannot collide, though
// commitlog does not rely on that: it assumes it is the only writer (see the
// CommitLog interface for that contract). This is the same reasoning Kafka's
// tiered storage uses in requiring a unique id per copy attempt "even when it
// retries ... for the same log segment data" — uniqueness is cheaper than
// coordination, and unlike coordination it cannot be got subtly wrong.
//
// The cost is that a key cannot be recomputed, only remembered. That is already
// true: the offload marker records keys VERBATIM and is the only thing that
// resolves them, so nothing anywhere derives a key for an existing object.
func newStoreKeys(baseOffset int64) (logKey, indexKey string) {
	u := newUploadID()
	return fmt.Sprintf("%020d.%s%s", baseOffset, u, logSuffix),
		fmt.Sprintf("%020d.%s%s", baseOffset, u, indexSuffix)
}

// newUploadID returns a value no other upload will use. It is random rather
// than a counter because a counter has to be read from somewhere, and every
// place it could be read from is state that a crash, a restart or a second
// process can leave stale — which is exactly how a "unique" key stops being
// unique.
func newUploadID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// UNREACHABLE, and left as a panic on purpose — the one place in this
		// package where that is still the right answer.
		//
		// crypto/rand.Read "never returns an error, and always fills b
		// entirely"; it crashes the program itself if the OS source fails. So
		// this branch cannot be taken, and threading an error out of it would
		// mean changing newUploadID, newStoreKeys and ten call sites to handle a
		// condition that cannot arise — while the crash it is meant to prevent
		// would already have happened inside rand.Read.
		panic("commitlog: cannot generate an upload id: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// offloadMeta is the JSON content of a v2 .offloaded marker. It carries enough to
// place the segment at boot without reading its (now remote) index: boundaries
// for offset/time routing, and the log object size so the store backing opens
// without a size round-trip. A v1 marker (option 1, index kept local) is instead
// the raw log key bytes; readOffloadMarker tells them apart by the leading '{'.
type offloadMeta struct {
	LogKey         string `json:"log_key"`
	IndexKey       string `json:"index_key,omitempty"` // empty => index kept local (option 1)
	FirstOffset    int64  `json:"first_offset"`
	LastOffset     int64  `json:"last_offset"`
	FirstWriteTime int64  `json:"first_write_time"`
	LastWriteTime  int64  `json:"last_write_time"`
	Position       int64  `json:"position"`
	PhysPosition   int64  `json:"phys_position"`
	BlockMode      bool   `json:"block_mode"`
}

// readOffloadMarker reads a .offloaded marker. A v2 marker is JSON; a v1 marker
// (option 1) is the raw log key, returned as offloadMeta{LogKey} with IndexKey
// empty so the caller keeps using the local index.
func readOffloadMarker(path string) (offloadMeta, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return offloadMeta{}, err
	}
	if len(b) > 0 && b[0] == '{' {
		var m offloadMeta
		if err := json.Unmarshal(b, &m); err != nil {
			return offloadMeta{}, errors.Wrap(err, "parse offload marker")
		}
		return m, nil
	}
	return offloadMeta{LogKey: string(b)}, nil
}

// offloadTo uploads the segment's local log bytes to store under key, swaps the
// backing to a read-only storeBacking, writes the .offloaded marker, and
// deletes the local .log file. The index stays local. The segment must be
// sealed and not already offloaded; the marker is written before the local log
// is removed so a crash mid-offload leaves a recoverable state (marker present
// + object uploaded, local log may or may not be gone — open() prefers the
// store when the marker exists).
// offloadTo uploads the segment's log bytes to store under key and swaps the
// backing to a read-only storeBacking. When cache is non-nil (tiered storage,
// option 2) it also uploads the index object and drops the local index, so no
// per-segment index file remains on local disk; reads then fetch the index into
// the shared cache on demand. A v2 .offloaded marker records the segment's
// boundaries and log size so a restart places it without reading the remote
// index. The marker is the commit point: it is written after both objects are
// uploaded and before the local files are removed, so a crash mid-offload leaves
// a recoverable state (objects present + marker => open through the store).
// key and idxKey are the object keys the caller allocated for this upload (see
// newStoreKeys). They are passed rather than derived here because they cannot be
// derived: every upload gets its own, so only the caller that allocated them
// knows which objects are being written.
func (s *segment) offloadTo(store SegmentStore, key, idxKey string, cache *RemoteIndexCache) error {
	s.Lock()
	defer s.Unlock()
	if s.closed {
		return ErrSegmentClosed
	}
	if s.store != nil {
		return nil // already offloaded
	}
	if !s.sealed {
		return errors.New("commitlog: cannot offload an unsealed segment")
	}
	size, err := s.backing.Size()
	if err != nil {
		return err
	}
	if err := store.Put(key, io.NewSectionReader(s.backing, 0, size), size); err != nil {
		return errors.Wrap(err, "offload put")
	}

	// Option 2: upload the index object too (before the marker), so the marker's
	// presence implies both objects exist.
	var indexKey string
	if cache != nil {
		indexKey = idxKey
		r, isize, err := s.Index.offloadReader()
		if err != nil {
			return errors.Wrap(err, "read index for offload")
		}
		if err := store.Put(indexKey, r, isize); err != nil {
			return errors.Wrap(err, "offload index put")
		}
	}

	meta := offloadMeta{
		LogKey:         key,
		IndexKey:       indexKey,
		FirstOffset:    s.firstOffset,
		LastOffset:     s.lastOffset,
		FirstWriteTime: s.firstWriteTime,
		LastWriteTime:  s.lastWriteTime,
		Position:       s.position,
		PhysPosition:   size,
		BlockMode:      s.blockMode,
	}
	return s.attachOffloadedLocked(store, meta, cache)
}

// attachOffloadedLocked turns a LOCAL segment into an offloaded one pointing at
// objects that already exist in the store: it writes the marker (the commit
// point), drops the local files, and swaps in a backing over the object.
//
// It is shared by the two ways a segment can come to be offloaded — uploading
// its own bytes, and adopting objects another process uploaded — so the two
// cannot drift into disagreeing about what an offloaded segment looks like. The
// caller has already established that the objects exist; this does not upload.
//
// Caller holds the segment lock.
func (s *segment) attachOffloadedLocked(store SegmentStore, meta offloadMeta,
	cache *RemoteIndexCache) error {

	sb, err := newStoreBackingSize(store, meta.LogKey, meta.PhysPosition)
	if err != nil {
		return err
	}
	localLog := s.backing.Name()
	if err := s.backing.Close(); err != nil {
		return errors.Wrap(err, "close local backing")
	}
	var localIndex string
	if meta.IndexKey != "" && cache != nil {
		localIndex = s.Index.Name()
		if err := s.Index.Close(); err != nil {
			return errors.Wrap(err, "close local index")
		}
	}

	markerBytes, err := json.Marshal(meta)
	if err != nil {
		return errors.Wrap(err, "encode offload marker")
	}
	// Marker (commit point), then drop the local files.
	if err := os.WriteFile(s.offloadMarkerPath(), markerBytes, 0o644); err != nil {
		return errors.Wrap(err, "write offload marker")
	}
	if err := os.Remove(localLog); err != nil && !os.IsNotExist(err) {
		return errors.Wrap(err, "remove local log")
	}
	if localIndex != "" {
		if err := os.Remove(localIndex); err != nil && !os.IsNotExist(err) {
			return errors.Wrap(err, "remove local index")
		}
	}

	s.backing = sb
	s.store = store
	s.storeKey = meta.LogKey
	// Set explicitly rather than left to the zero value. A first offload is
	s.firstOffset = meta.FirstOffset
	s.lastOffset = meta.LastOffset
	s.firstWriteTime = meta.FirstWriteTime
	s.lastWriteTime = meta.LastWriteTime
	s.position = meta.Position
	s.physPosition = meta.PhysPosition
	s.blockMode = meta.BlockMode
	if meta.IndexKey != "" && cache != nil {
		s.Index = nil
		s.indexKey = meta.IndexKey
		s.indexCache = cache
	}
	return nil
}

func newSegment(path string, baseOffset, maxBytes int64, isNew bool, suffix string, codec compress.Codec) (*segment, error) {
	s := &segment{
		maxBytes:    maxBytes,
		BaseOffset:  baseOffset,
		firstOffset: -1,
		lastOffset:  -1,
		path:        path,
		suffix:      suffix,
		codec:       codec,
		waiters:     make(map[interface{}]chan struct{}),
		dirtyData:   true,
		dirtyIndex:  true,
	}
	// If this is a new segment, ensure the file doesn't already exist.
	if isNew && exists(s.logPath()) {
		return nil, ErrSegmentExists
	}
	backing, err := openLocalBacking(s.logPath())
	if err != nil {
		return nil, errors.Wrap(err, "open file failed")
	}
	s.backing = backing
	if err := s.initPositions(); err != nil {
		return nil, err
	}
	err = s.setupIndex()
	return s, err
}

// openOffloadedSegment opens a sealed segment whose log bytes live in store
// under key (the local .log is gone; the index is still local). It reads block
// metadata and positions through the store backing and loads the local index,
// so it behaves like any other sealed segment for reads.
// openOffloadedSegment opens a sealed segment whose log bytes live in store. meta
// comes from the .offloaded marker. For a v1 marker (option 1, meta.IndexKey
// empty) the local index is still present and is loaded normally. For a v2 marker
// (option 2) the index has been offloaded too: boundaries come from the marker,
// the index stays remote (fetched into cache on first seek), and Index is nil.
func openOffloadedSegment(path string, baseOffset, maxBytes int64, codec compress.Codec, store SegmentStore, meta offloadMeta, cache *RemoteIndexCache) (*segment, error) {
	s := &segment{
		maxBytes:    maxBytes,
		BaseOffset:  baseOffset,
		firstOffset: -1,
		lastOffset:  -1,
		path:        path,
		codec:       codec,
		waiters:     make(map[interface{}]chan struct{}),
		sealed:      true,
		store:       store,
		// Taken from the marker VERBATIM. Nothing recomputes a key from the
		// base offset — it could not, since each upload allocated its own — so
		// objects written by any earlier version stay resolvable.
		storeKey: meta.LogKey,
	}

	if meta.IndexKey == "" {
		// Option 1: index kept local. Size unknown from the marker, so the
		// backing fetches it; then load the local index as usual.
		sb, err := newStoreBacking(store, meta.LogKey)
		if err != nil {
			return nil, errors.Wrap(err, "open store backing")
		}
		s.backing = sb
		if err := s.initPositions(); err != nil {
			return nil, err
		}
		return s, s.setupIndex()
	}

	// Option 2: index offloaded. The marker records the log object size, so the
	// backing opens without a size round-trip. initPositions still reconstructs
	// the block layout from the log (as option 1 does); the boundaries the index
	// would supply come from the marker instead, and no index is read on open.
	sb, err := newStoreBackingSize(store, meta.LogKey, meta.PhysPosition)
	if err != nil {
		return nil, errors.Wrap(err, "open store backing")
	}
	s.backing = sb
	if err := s.initPositions(); err != nil {
		return nil, err
	}
	s.firstOffset = meta.FirstOffset
	s.lastOffset = meta.LastOffset
	s.firstWriteTime = meta.FirstWriteTime
	s.lastWriteTime = meta.LastWriteTime
	s.Index = nil
	s.indexKey = meta.IndexKey
	s.indexCache = cache
	return s, nil
}

// initPositions inspects the (already-open) log file, detects whether it uses
// block compression, and initializes position/physPosition/blocks. A fresh
// (empty) segment uses the block format only when a codec is configured, so a
// None codec is byte-for-byte compatible with pre-compression logs. An existing
// segment is classified by its first byte: blockMagic means a compressed
// segment (scan its block headers), anything else is a legacy raw segment, which
// stays raw even if a codec is now configured so formats never mix in one file.
func (s *segment) initPositions() error {
	size, err := s.backing.Size()
	if err != nil {
		return errors.Wrap(err, "stat file failed")
	}
	s.physPosition = size
	s.cache = newBlockCache()
	s.blocks = s.blocks[:0]
	if size == 0 {
		s.blockMode = s.codec != compress.None
		s.position = 0
		return nil
	}
	var magic [1]byte
	if _, err := s.backing.ReadAt(magic[:], 0); err != nil {
		return errors.Wrap(err, "read format magic failed")
	}
	if magic[0] == blockMagic {
		s.blockMode = true
		return s.scanBlocks(size)
	}
	s.blockMode = false
	s.position = size
	return nil
}

// scanBlocks reconstructs the in-memory block index by walking the block headers
// in the file, and sets position (logical total) and physPosition.
//
// The walk is a chain: each block's header gives the length that locates the
// next one. A crash mid-append leaves the last link half-written, and there is
// no way to resume past it — but everything BEFORE it is intact, and a segment
// that refuses to open takes the whole log with it, every sealed segment
// included. So a tail that does not resolve ends the walk instead of failing
// it, which is what a raw segment already does: it accepts the bytes and lets
// the frame checksum reject the torn frame at read time.
//
// A version mismatch is the exception, and stays an error. The magic byte
// matched, so those bytes are a real header written by a build whose layout
// this one does not know — reading past it would be guessing, and dropping it
// would delete data another build can read.
func (s *segment) scanBlocks(size int64) error {
	var (
		phys    int64
		logical int64
		hdr     [blockHeaderLen]byte
	)
	for phys < size {
		if size-phys < int64(blockHeaderLen) {
			break // not enough left for a header: the write was cut inside one
		}
		if _, err := s.backing.ReadAt(hdr[:], phys); err != nil {
			return errors.Wrap(err, "read block header failed")
		}
		codec, uLen, cLen, err := parseBlockHeader(hdr[:])
		if err != nil {
			if errors.Is(err, ErrBlockFormat) {
				return err
			}
			break // garbage where a header should be: a partial flush
		}
		physLen := int64(blockHeaderLen) + int64(cLen)
		if phys+physLen > size {
			break // header whole, the payload it promises is not
		}
		s.blocks = append(s.blocks, blockRef{
			logicalStart: logical,
			logicalLen:   int64(uLen),
			physStart:    phys,
			physLen:      physLen,
			codec:        codec,
		})
		phys += physLen
		logical += int64(uLen)
	}
	if phys != size {
		// The torn bytes have to GO, not merely be ignored. The file is opened
		// O_APPEND, so the next block would be written after them and the walk
		// that reads this segment back would stop at the same place — the
		// segment would accept records it could never serve.
		if err := s.discardTornTail(phys, size); err != nil {
			return err
		}
		s.droppedTornTail = true
	}
	s.position = logical
	s.physPosition = phys
	return nil
}

// discardTornTail removes the unresolvable bytes at the end of a segment — a
// half-written block, or a half-written frame in a raw one.
//
// Only a local file can be cut, and only a local file needs it: a store object
// is written in one shot, so a torn one is not a crash artefact but damage, and
// rewriting the object to hide it would be the wrong answer. There, the scanned
// prefix is served and the tail is left alone.
func (s *segment) discardTornTail(keep, size int64) error {
	lb, ok := s.backing.(*localBacking)
	if !ok {
		return nil
	}
	// Through the path, not the open handle. The segment's handle is O_APPEND,
	// which on Windows is opened for FILE_APPEND_DATA alone and cannot resize
	// the file — Truncate on it fails with "Access is denied", and a recovery
	// that fails is the unopenable log all over again.
	if err := os.Truncate(lb.f.Name(), keep); err != nil {
		return errors.Wrapf(err, "truncate torn tail (%d of %d bytes)",
			size-keep, size)
	}
	return lb.f.Sync()
}

// setupIndex creates and initializes an index.
// Initialization is:
// - Initialize index position
// - Initialize firstOffset/lastOffset
// - Initialize firstWriteTime/lastWriteTime
func (s *segment) setupIndex() (err error) {
	s.Index, err = newIndex(options{
		path:       s.indexPath(),
		baseOffset: s.BaseOffset,
	})
	if err != nil {
		return err
	}
	lastEntry, err := s.Index.InitializePosition()
	if err != nil {
		return err
	}
	if lastEntry != nil && s.indexOvershootsLog(lastEntry) && s.rebuildOverIndex() {
		// The rebuild sets firstOffset/lastOffset and their timestamps as it
		// walks, so there is nothing left for the code below to read back out.
		// Re-running InitializePosition here would not work anyway: it reads
		// through ReadAt, which is gated on the index's position, and newIndex
		// seeds that to the whole file where a rebuilt index holds only what the
		// log actually contains.
		return errors.Wrap(s.rebuildIndexFromLog(),
			"rebuild index over a log it does not describe failed")
	}
	// If lastEntry is nil, the index is empty.
	if lastEntry == nil {
		return nil
	}
	if s.blockMode {
		// The sparse index anchors each block at its first message, so the
		// index's first/last entries are block anchors, not the segment's
		// first/last messages. firstOffset/firstWriteTime come from the first
		// block's anchor directly, but lastOffset/lastWriteTime require
		// scanning the final block's frames to its last message. The log
		// (blocks, rebuilt by scanBlocks) is the source of truth for the
		// physical extent; the last anchor's position marks the final block's
		// logical start.
		var firstEntry entry
		if err := s.Index.ReadEntryAtFileOffset(&firstEntry, 0); err != nil {
			return err
		}
		s.firstOffset = firstEntry.Offset
		s.firstWriteTime = firstEntry.Timestamp
		last, err := s.lastFrameInBlock(lastEntry.Position)
		if err != nil {
			return errors.Wrap(err, "recover last offset failed")
		}
		s.lastOffset = last.Offset
		s.lastWriteTime = last.Timestamp
		return nil
	}
	s.lastOffset = lastEntry.Offset
	s.lastWriteTime = lastEntry.Timestamp
	// Read the first entry to get firstOffset and firstWriteTime.
	var firstEntry entry
	if err := s.Index.ReadEntryAtFileOffset(&firstEntry, 0); err != nil {
		return err
	}
	s.firstOffset = firstEntry.Offset
	s.firstWriteTime = firstEntry.Timestamp
	return nil
}

// indexOvershootsLog reports an index that cannot describe the log beside it,
// because its last entry ends past where the log ends.
//
// This is the signature a crash mid-install leaves. Replacing a segment with its
// rewrite is TWO renames — the log file, then the index file — and stopping
// between them pairs the compacted log with the SOURCE's index, every position
// in which was computed against a strictly larger file (a rewrite only ever
// drops records). Nothing else on disk marks it: both files are individually
// well formed, and only their relationship is wrong.
//
// The direction is what makes this safe to act on. An index BEHIND its log is
// ordinary — the append path writes the frame before the entry, so a crash
// there leaves a short index, and reconcileIndexTail fills it in. An index AHEAD
// of its log describes a file that no longer exists.
func (s *segment) indexOvershootsLog(last *entry) bool {
	if s.blockMode {
		// Entries are block anchors, whose Size spans a frame inside the block
		// rather than the block itself; only the start position is comparable.
		return last.Position >= s.position
	}
	return last.Position+int64(last.Size) > s.position
}

// rebuildOverIndex decides what to do about an index that reaches past its log:
// derive a new one from the log that is actually there, or keep the one on disk.
//
// Three accidents produce the overshoot, and only one of them can be answered by
// keeping the index.
//
//   - A TORN WRITE cut the log's tail. Whether the entries above the cut can be
//     kept turns on whether the BYTES they describe can be: a raw segment's torn
//     frame is dropped from the file by reconcileIndexTailRaw, so the entries for
//     it must go too, and the rebuild does both in one walk. A block segment's
//     tail was already dropped by scanBlocks, which is what droppedTornTail
//     records. Either way there is a shortened log to derive from, and deriving
//     is the whole answer.
//   - A CRASH BETWEEN Replace's TWO RENAMES left the source's index over the
//     rewrite. Every entry is wrong, including the ones that happen to fit,
//     because the rewrite dropped records and shifted everything after them.
//   - Nothing at all, on a block segment: the sparse anchors are logical
//     positions and the last one legitimately sits at the final block's start.
//     indexDescribesLog is what recognises that and leaves it alone — the one
//     case where the index on disk is the truth and a rebuild would be work
//     spent to arrive back where it started.
//
// This USED to keep the index for any torn write, on the reasoning that later
// recovery would trim the log's tail and leave the surviving entries exactly
// right. Nothing trimmed it. RecoverTail returns early whenever the checkpoint
// sits at or above the recovered tail — which a torn last record is precisely
// the case for — and it truncates by OFFSET, which a half-written frame does not
// have. So the entries above the cut survived, the log grew past them on the
// next append, and readers walked into the remnant.
func (s *segment) rebuildOverIndex() bool {
	if s.droppedTornTail {
		// The bytes those entries anchor into are already gone. It is not worth
		// asking whether the index describes the log: it described the log as it
		// was a moment ago, and the part that no longer fits is exactly the part
		// that must go.
		return true
	}
	if !s.blockMode {
		return true
	}
	return !s.indexDescribesLog()
}

// indexDescribesLog asks whether the index belongs to the log beside it, by
// checking the deepest entry that still fits inside the log against the frame
// that entry points at.
//
// One frame read tells a stale index apart from an index that is merely sparse.
// The deepest fitting entry is used rather than the first, because drift
// accumulates: an early entry can still land on the right frame by coincidence
// when the first dropped record is further in.
func (s *segment) indexDescribesLog() bool {
	for i := s.Index.numEntries() - 1; i >= 0; i-- {
		var e entry
		if err := s.Index.ReadEntryAtFileOffset(&e, i*entryWidth); err != nil {
			return false
		}
		if e.Position+int64(e.Size) > s.position {
			continue // past the log's end: says nothing about which log this is
		}
		got, ok := s.frameOffsetAt(e.Position)
		return ok && got == e.Offset
	}
	// Not one entry fits inside the log. Whatever this index describes, it is
	// not this.
	return false
}

// frameOffsetAt reads the offset out of the message-set frame at a logical
// position, without going through the read path's locking or caches — this runs
// during open, before the segment is reachable.
func (s *segment) frameOffsetAt(pos int64) (int64, bool) {
	hdr := make([]byte, msgSetHeaderLen)
	if s.blockMode {
		blk := s.findBlock(pos)
		if blk == nil {
			return 0, false
		}
		_, data, err := s.decodeBlock(nil, *blk, nil, nil)
		if err != nil {
			return 0, false
		}
		at := pos - blk.logicalStart
		if at < 0 || at+msgSetHeaderLen > int64(len(data)) {
			return 0, false
		}
		copy(hdr, data[at:])
	} else if _, err := s.backing.ReadAt(hdr, pos); err != nil {
		return 0, false
	}
	return messageSet(hdr).Offset(), true
}

// rebuildIndexFromLog throws the index away and reconstructs it by walking the
// log. Sound because the log is the record and the index is only a lookup table
// over it: everything an entry holds is recoverable from the frame it points at.
func (s *segment) rebuildIndexFromLog() error {
	if err := s.Index.reset(); err != nil {
		return err
	}
	s.firstOffset, s.lastOffset = -1, -1
	s.firstWriteTime, s.lastWriteTime = 0, 0
	// reconcileIndexTail resumes after the last indexed entry; over an emptied
	// index that means walking the whole log from the start.
	return s.reconcileIndexTail()
}

// lastFrameInBlock scans message frames starting at logical position start
// (a block's logical start) and returns the entry for the final frame within
// that block. Used during recovery to find a block-compressed segment's true
// last offset, since the sparse index only records each block's first message.
func (s *segment) lastFrameInBlock(start int64) (*entry, error) {
	blk := s.findBlock(start)
	if blk == nil {
		return nil, errIndexCorrupt
	}
	// One-shot transient decode, deliberately NOT via the segment's cache:
	// this runs once per segment open/install, and cache-routed reads left
	// every freshly installed rewrite retaining a decode buffer pair it
	// might never serve a read from (part of run 32's per-segment heap
	// ratchet).
	_, data, err := s.decodeBlock(nil, *blk, nil, nil)
	if err != nil {
		return nil, err
	}
	pos := start - blk.logicalStart
	var last *entry
	for pos+msgSetHeaderLen <= int64(len(data)) {
		m := messageSet(data[pos:])
		size := m.Size()
		last = &entry{
			Offset:      m.Offset(),
			Timestamp:   m.Timestamp(),
			LeaderEpoch: m.LeaderEpoch(),
			Position:    blk.logicalStart + pos,
			Size:        size + msgSetHeaderLen,
		}
		pos += int64(msgSetHeaderLen) + int64(size)
	}
	if last == nil {
		return nil, errIndexCorrupt
	}
	return last, nil
}

// reconcileIndexTail rebuilds index entries for any log records the index does
// not yet cover. The append path writes a log frame BEFORE its index entry
// (WriteMessageSet), and checkpointHW fsyncs only the log backing (not the
// index), so a crash can leave the log physically AHEAD of the index. Without
// this, the segment would take lastOffset/NextOffset from the stale index and
// under-report its tail: a seek (index) and a sequential scan (physical log)
// would disagree on which record an offset names, and the next append could
// land on an existing un-indexed record. Run on open for the active segment;
// a no-op when the index already covers the log.
func (s *segment) reconcileIndexTail() error {
	if s.Index == nil {
		return nil // offloaded: index is remote and the segment is immutable
	}
	if s.blockMode {
		return s.reconcileIndexTailBlocks()
	}
	return s.reconcileIndexTailRaw()
}

// reconcileIndexTailRaw scans raw message-set frames past the last indexed one
// and appends their index entries, advancing lastOffset.
//
// A partial (torn) tail frame is DROPPED from the file, for the same reason
// scanBlocks drops a torn block: the handle is O_APPEND, so the next record
// would be written after the remnant rather than over it, and the reader —
// resuming at the recovered position — would walk straight into it and fail its
// checksum. That does not cost the torn record alone. It costs the whole
// segment from the recovered position on, because a reader cannot get past the
// bad frame to reach anything written after it, so a log that read back cleanly
// before the append reads back as nothing at all afterwards.
//
// Which makes leaving it to RecoverTail (the old plan) not merely late but
// unreachable: it returns early whenever the checkpoint is at or above the
// recovered tail, which a torn last record is exactly the case for, and its
// Truncate takes an OFFSET — and a half-written frame is not a record with one.
func (s *segment) reconcileIndexTailRaw() error {
	var startPos int64
	if n := s.Index.numEntries(); n > 0 {
		var last entry
		if err := s.Index.ReadEntryAtFileOffset(&last, (n-1)*entryWidth); err != nil {
			return err
		}
		startPos = last.Position + int64(last.Size)
	}
	var (
		hdr  = make([]byte, msgSetHeaderLen)
		torn bool
	)
	for startPos < s.position {
		if startPos+int64(msgSetHeaderLen) > s.position {
			torn = true // fewer bytes left than a header: cut inside one
			break
		}
		if _, err := s.backing.ReadAt(hdr, startPos); err != nil {
			// A read that FAILED says nothing about what is on disk, so the tail
			// stays: dropping bytes on a transient IO error would turn a
			// retryable open into permanent data loss.
			break
		}
		m := messageSet(hdr)
		frameLen := int64(msgSetHeaderLen) + int64(m.Size())
		if startPos+frameLen > s.position {
			torn = true // header whole, the payload it promises is not
			break
		}
		e := &entry{
			Offset:      m.Offset(),
			Timestamp:   m.Timestamp(),
			LeaderEpoch: m.LeaderEpoch(),
			Position:    startPos,
			Size:        int32(frameLen),
		}
		if err := s.Index.writeEntries([]*entry{e}); err != nil {
			return err
		}
		if s.firstOffset == -1 {
			s.firstOffset, s.firstWriteTime = e.Offset, e.Timestamp
		}
		s.lastOffset, s.lastWriteTime = e.Offset, e.Timestamp
		startPos += frameLen
	}
	if !torn {
		return nil
	}
	if err := s.discardTornTail(startPos, s.position); err != nil {
		return err
	}
	s.position = startPos
	s.physPosition = startPos
	return nil
}

// reconcileIndexTailBlocks adds a sparse-index anchor for any block scanBlocks
// reconstructed past the last anchored one (a crash between the block write and
// its anchor write), then recomputes lastOffset from the true last block.
func (s *segment) reconcileIndexTailBlocks() error {
	anchored := int(s.Index.numEntries()) // one anchor per block
	if anchored >= len(s.blocks) {
		return nil
	}
	for i := anchored; i < len(s.blocks); i++ {
		blk := s.blocks[i]
		_, data, err := s.decodeBlock(nil, blk, nil, nil)
		if err != nil {
			return err
		}
		if len(data) < msgSetHeaderLen {
			break
		}
		m := messageSet(data)
		anchor := &entry{
			Offset:      m.Offset(),
			Timestamp:   m.Timestamp(),
			LeaderEpoch: m.LeaderEpoch(),
			Position:    blk.logicalStart,
			Size:        int32(msgSetHeaderLen) + m.Size(),
		}
		if err := s.Index.writeEntries([]*entry{anchor}); err != nil {
			return err
		}
		if s.firstOffset == -1 {
			s.firstOffset, s.firstWriteTime = anchor.Offset, anchor.Timestamp
		}
	}
	last, err := s.lastFrameInBlock(s.blocks[len(s.blocks)-1].logicalStart)
	if err != nil {
		return err
	}
	s.lastOffset, s.lastWriteTime = last.Offset, last.Timestamp
	return nil
}

// CheckSplit determines if a new log segment should be rolled out either
// because this segment is full or LogRollTime has passed since the first
// message was written to the segment.
// maxSegmentBlocks bounds a block-mode segment's block COUNT before it rolls,
// independent of its byte size. Each live block costs a blockRef (~40B) and a
// sparse-index anchor for as long as the segment exists, so a small-append
// workload filling a byte-sized segment (default 1GB) accumulates millions of
// tiny blocks — run 25 measured ~316MB of blockRefs across the ACTIVE
// segments alone. Rolling at 16k blocks caps that at ~650KB per active
// segment and hands the tiny blocks to the next clean's consolidation
// rewrite within a tick. Large-batch workloads never get near it (16k blocks
// of 256KB is 4GB — the byte cap rolls first).
const maxSegmentBlocks = 16 << 10

func (s *segment) CheckSplit(logRollTime time.Duration) bool {
	s.RLock()
	defer s.RUnlock()
	if s.position >= s.maxBytes {
		return true
	}
	if s.blockMode && len(s.blocks) >= maxSegmentBlocks {
		return true
	}
	if logRollTime == 0 || s.firstWriteTime == 0 {
		// Don't roll a new segment if there have been no writes to the segment
		// or LogRollTime is disabled.
		return false
	}
	// Check if LogRollTime has passed since first write.
	return timestamp()-s.firstWriteTime >= int64(logRollTime)
}

// Seal a segment from being written to. This is called on the former active
// segment after a new segment is rolled or when the segment is closed. This is
// a no-op if the segment is already sealed.
func (s *segment) Seal() {
	s.Lock()
	defer s.Unlock()
	s.seal()
}

func (s *segment) seal() {
	if s.sealed {
		return
	}
	s.sealed = true
	// Notify any readers waiting for data.
	s.notifyWaiters()
	if s.Index != nil {
		s.Index.Shrink() // nolint: errcheck
		// Sealing is the index's flush point, because it is the moment after
		// which nothing else will repair it. The durability hot path flushes log
		// bytes only, relying on open() rebuilding a short index tail — but that
		// rebuild runs on the ACTIVE segment alone, so a segment that rolls
		// between syncs would otherwise keep a permanently short index. One extra
		// fsync per roll, off the hot path, confines the unflushed index to the
		// active segment that open already fixes, and makes an offset in a sealed
		// segment durable by construction.
		//
		// Best-effort, like the shrink above: a failure here costs a rebuilt
		// index tail, not data, and seal runs on paths that cannot return one.
		s.Index.Sync() // nolint: errcheck
		s.dirtyIndex = false
	}
}

func (s *segment) NextOffset() int64 {
	s.RLock()
	defer s.RUnlock()
	// If the segment hasn't been written to, the next offset should be the
	// base offset.
	if s.lastOffset == -1 {
		return s.BaseOffset
	}
	return s.lastOffset + 1
}

func (s *segment) FirstOffset() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.firstOffset
}

func (s *segment) FirstWriteTime() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.firstWriteTime
}

func (s *segment) LastWriteTime() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.lastWriteTime
}

func (s *segment) LastOffset() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.lastOffset
}

func (s *segment) Position() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.position
}

// tiered reports whether this segment's bytes live in a SegmentStore rather
// than a local file. The two are worth telling apart wherever the SHAPE of a
// read matters rather than its result: a store charges per request and is
// served best by many in flight, a local file is a syscall against a page cache
// that is already reading ahead.
func (s *segment) tiered() bool {
	s.RLock()
	defer s.RUnlock()
	_, ok := s.backing.(*storeBacking)
	return ok
}

func (s *segment) IsEmpty() bool {
	s.RLock()
	defer s.RUnlock()
	return s.firstOffset == -1
}

func (s *segment) MessageCount() int64 {
	s.RLock()
	defer s.RUnlock()
	// For a raw segment the dense index has one entry per message, so the
	// entry count is the message count. For a block-compressed segment the
	// index is sparse (one entry per block), so derive the message count from
	// the offset span instead. Offsets are contiguous within every block (a
	// message set is assigned baseOffset+i), and appended segments are
	// contiguous across blocks, so this is exact for normally-written logs.
	// After compaction a compressed segment stores one message per block and
	// may have offset gaps, in which case this is an upper bound — acceptable
	// for the retention heuristic that consumes it.
	// A raw segment with a resident index counts entries directly. A raw segment
	// whose index is offloaded (Index nil) derives the count from its offset span
	// — exact for a raw segment (one contiguous message per offset) and avoiding a
	// store fetch for a mere retention heuristic.
	if !s.blockMode && s.Index != nil {
		return s.Index.CountEntries()
	}
	if s.lastOffset < 0 {
		return 0
	}
	return s.lastOffset - s.firstOffset + 1
}

func (s *segment) WriteMessageSet(ms []byte, entries []*entry) error {
	s.Lock()
	defer s.Unlock()
	if _, err := s.write(ms, entries); err != nil {
		return err
	}
	// A block-compressed segment uses a sparse index: one entry per block,
	// anchored at the block's first message (its base offset, logical start
	// position, and first timestamp). Seeks binary-search these anchors and
	// then scan forward within the block to the exact target offset. Raw
	// (legacy) segments keep the dense one-entry-per-message index.
	if s.blockMode {
		return s.Index.writeEntries(entries[:1])
	}
	return s.Index.writeEntries(entries)
}

// write a byte slice to the log at the current position. This increments the
// offset as well as sets the position to the new tail.
func (s *segment) write(p []byte, entries []*entry) (n int, err error) {
	if s.closed {
		return 0, ErrSegmentClosed
	}
	s.dirtyData = true
	s.dirtyIndex = true
	if s.blockMode {
		if err = s.appendBlock(p); err != nil {
			return 0, err
		}
		n = len(p)
	} else {
		n, err = s.backing.Write(p)
		if err != nil {
			return n, errors.Wrap(err, "log write failed")
		}
		s.position += int64(n)
	}
	// Guard on firstOffset, not firstWriteTime: messages appended without
	// timestamps leave firstWriteTime 0 forever, so every batch would
	// overwrite firstOffset with its own first offset — a live handle then
	// reports the LAST batch's offset as the segment's first (correct again
	// only after a reopen recovers it from the index).
	if s.firstOffset == -1 {
		first := entries[0]
		s.firstOffset = first.Offset
		s.firstWriteTime = first.Timestamp
	}
	last := entries[len(entries)-1]
	s.lastOffset = last.Offset
	s.lastWriteTime = last.Timestamp
	s.notifyWaiters()
	return n, nil
}

// compressMinBlock is the smallest payload worth running the codec on.
// Sub-4KB blocks compress marginally at best (run 22's per-append blocks
// averaged 140 bytes), and the block count — not the byte count — is what
// costs memory and boot time; storing small appends raw skips the encoder
// on the hot commit path, and the clean's ~256KB consolidation rewrite
// (cleanBlockTarget) compresses the same bytes properly later.
const compressMinBlock = 4 << 10

// appendBlock compresses p into a self-describing block and appends it.
// Small payloads (below compressMinBlock) and payloads the codec cannot
// shrink are stored raw (codec None) so we never inflate incompressible
// data nor burn encoder CPU where the ratio cannot matter. position
// advances by the logical (uncompressed) length; physPosition by the
// physical length.
func (s *segment) appendBlock(p []byte) error {
	codec := s.codec
	var payload []byte
	if len(p) < compressMinBlock {
		codec = compress.None
		payload = p
	} else if payload = codec.Compress(p); len(payload) >= len(p) {
		codec = compress.None
		payload = p
	}
	hdr := encodeBlockHeader(codec, uint32(len(p)), uint32(len(payload)))
	buf := make([]byte, 0, len(hdr)+len(payload))
	buf = append(buf, hdr...)
	buf = append(buf, payload...)
	n, err := s.backing.Write(buf)
	if err != nil {
		return errors.Wrap(err, "block write failed")
	}
	s.blocks = append(s.blocks, blockRef{
		logicalStart: s.position,
		logicalLen:   int64(len(p)),
		physStart:    s.physPosition,
		physLen:      int64(n),
		codec:        codec,
	})
	s.position += int64(len(p))
	s.physPosition += int64(n)
	return nil
}

// needsBlockConsolidation reports whether the segment's block index is
// pathologically fine-grained: enough blocks to matter and MANY more of
// them than the rewrite target layout would produce. The append path writes
// one block per message set, so per-commit appends make one block each —
// blockRef memory, the sparse index, the open-time header walk and zstd's
// ratio all scale with block COUNT. Cleans force one consolidation rewrite
// on such segments (see cleanBlockTarget).
//
// The comparison is against the TARGET layout (position/cleanBlockTarget
// blocks), not an average-block-size floor: view-output streams write
// multi-KB logical batches whose per-block average cleared a size floor
// while segments still carried 16k blocks each (run 26 measured 7.6M live
// blockRefs ≈ 365MB across such segments — a size-floor veto never fired).
func (s *segment) needsBlockConsolidation() bool {
	s.RLock()
	defer s.RUnlock()
	if !s.blockMode || len(s.blocks) < 1024 {
		return false
	}
	targetBlocks := s.position/cleanBlockTarget + 1
	return int64(len(s.blocks)) > 4*targetBlocks
}

// ReadAt reads len(p) bytes from the segment's logical byte space starting at
// off. For a raw segment this is a direct file read; for a block-compressed
// segment it maps the logical range onto the decompressed block(s).
func (s *segment) ReadAt(p []byte, off int64) (n int, err error) {
	s.RLock()
	defer s.RUnlock()
	return s.readAtLocked(p, off)
}

// readAtLocked is the body of ReadAt without acquiring the segment lock. It is
// used both by ReadAt and by index seeks (findEntry/findEntryByTimestamp) which
// already hold the read lock and must not re-acquire it (sync.RWMutex read locks
// are not reentrant in the presence of a waiting writer).
func (s *segment) readAtLocked(p []byte, off int64) (n int, err error) {
	if s.closed {
		// gone counts as replaced: both mean this segment has left the log and
		// the caller should re-resolve against the current one, which is what
		// the reader does with ErrSegmentReplaced. Only ErrSegmentClosed says
		// "this handle is shut" — a claim about the segment, not about the log —
		// and a reader has nowhere to go with it.
		if s.replaced || s.gone {
			return 0, ErrSegmentReplaced
		}
		return 0, ErrSegmentClosed
	}
	if !s.blockMode {
		return s.backing.ReadAt(p, off)
	}
	return s.readBlocks(p, off)
}

// readBlocks serves a read from the logical byte space of a block-compressed
// segment, decompressing and copying across as many blocks as the request
// spans. It mirrors os.File.ReadAt semantics: a short read returns io.EOF.
func (s *segment) readBlocks(p []byte, off int64) (int, error) {
	return s.readBlocksCache(s.cache, nil, p, off)
}

func (s *segment) readBlocksCache(c *blockCache, st *scanStream, p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("commitlog: negative read offset %d", off)
	}
	if off >= s.position {
		return 0, io.EOF
	}
	n := 0
	for n < len(p) {
		cur := off + int64(n)
		if cur >= s.position {
			return n, io.EOF
		}
		blk := s.findBlock(cur)
		if blk == nil {
			return n, io.EOF
		}
		m, err := s.blockCopyIntoCache(c, st, p[n:], *blk, cur-blk.logicalStart)
		if err != nil {
			return n, err
		}
		n += m
	}
	return n, nil
}

// scanStream serves a sweep's reads from ONE open stream over the segment's
// bytes, instead of a ranged read per call.
//
// It is shaped as a ReadAt so it drops into the existing read paths unchanged,
// but it is only a stream underneath: a read that starts exactly where the last
// one ended is served by reading forward, and anything else falls back to a
// ranged read on the backing.
//
// Falling back rather than re-opening is deliberate. A jump is either a one-off
// (in which case a fresh stream would cost a request and be abandoned) or the
// caller is not really sweeping (in which case ranged reads are the right
// shape anyway). The stream keeps its position across a fallback, so a sweep
// that steps aside once carries on streaming afterwards.
//
// A nil *scanStream is valid and reads through to the backing, so callers that
// are not sweeping need no special case.
type scanStream struct {
	backing segmentBacking
	rc      io.ReadCloser
	pos     int64
	// broken records that the stream failed and must not be retried: a store
	// that cannot serve one should not be asked once per record.
	broken bool
}

func newScanStream(backing segmentBacking) *scanStream {
	return &scanStream{backing: backing}
}

func (ss *scanStream) ReadAt(p []byte, off int64) (int, error) {
	if ss == nil || ss.broken || len(p) == 0 {
		return ss.readRanged(p, off)
	}
	if ss.rc == nil {
		rc, err := ss.backing.Stream(off)
		if err != nil {
			// Not fatal: the bytes are still reachable the ranged way, and a
			// sweep that works slowly beats one that fails.
			ss.broken = true
			return ss.readRanged(p, off)
		}
		ss.rc, ss.pos = rc, off
	}
	if off != ss.pos {
		return ss.readRanged(p, off)
	}
	n, err := io.ReadFull(ss.rc, p)
	ss.pos += int64(n)
	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}
	return n, err
}

func (ss *scanStream) readRanged(p []byte, off int64) (int, error) {
	return ss.backing.ReadAt(p, off)
}

// Close releases the stream. Safe on a nil scanStream and on one that never
// opened, so a sweep can defer it unconditionally.
func (ss *scanStream) Close() error {
	if ss == nil || ss.rc == nil {
		return nil
	}
	err := ss.rc.Close()
	ss.rc, ss.broken = nil, true
	return err
}

// scanReadAt is ReadAt for one-shot sequential scans: block decodes go
// through the caller's cache instead of the segment's, so a pass over N
// segments retains one decode buffer pair, not N (see blockCopyIntoCache).
//
// st carries the sweep's open stream, or is nil for a caller that is not
// sweeping.
func (s *segment) scanReadAt(c *blockCache, st *scanStream, p []byte, off int64) (n int, err error) {
	s.RLock()
	defer s.RUnlock()
	if s.closed {
		// gone counts as replaced: both mean this segment has left the log and
		// the caller should re-resolve against the current one, which is what
		// the reader does with ErrSegmentReplaced. Only ErrSegmentClosed says
		// "this handle is shut" — a claim about the segment, not about the log —
		// and a reader has nowhere to go with it.
		if s.replaced || s.gone {
			return 0, ErrSegmentReplaced
		}
		return 0, ErrSegmentClosed
	}
	if !s.blockMode {
		if st != nil {
			return st.ReadAt(p, off)
		}
		return s.backing.ReadAt(p, off)
	}
	return s.readBlocksCache(c, st, p, off)
}

// findBlock returns the block whose logical range contains the given logical
// offset, or nil if none does.
func (s *segment) findBlock(logical int64) *blockRef {
	i := sort.Search(len(s.blocks), func(i int) bool {
		return s.blocks[i].logicalStart+s.blocks[i].logicalLen > logical
	})
	if i >= len(s.blocks) || logical < s.blocks[i].logicalStart {
		return nil
	}
	return &s.blocks[i]
}

// blockCopyIntoCache copies the block's decompressed bytes from srcOff into
// dst, decoding at most once per block visit via a single-entry cache. The
// cache OWNS its buffers and recycles them on displacement — callers only ever
// receive copies, made under the cache lock, so a displaced buffer is never
// observed mid-overwrite. Before recycling, every displacement abandoned a
// fresh decode buffer to the GC; run 31's anomaly heap capture showed ~276MB of
// those pending collection during one clean pass over a ~1200-segment stream.
//
// The cache is the CALLER's choice: readers pass the segment's own (repeated
// reads of a hot segment), one-shot scans pass a per-PASS cache shared across
// every segment the scan visits — routing scans through the segment cache left
// every scanned segment retaining a decode buffer pair for its lifetime,
// O(segments) heap per clean pass (run 32's ~500MB-1GB transients and creeping
// baseline).
func (s *segment) blockCopyIntoCache(c *blockCache, st *scanStream, dst []byte, b blockRef, srcOff int64) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.seg != s || c.start != b.physStart {
		raw, data, err := s.decodeBlock(st, b, c.raw, c.data)
		c.raw = raw
		if err != nil {
			// The decode may have scribbled over c.data; don't serve it.
			c.seg, c.start = nil, -1
			return 0, err
		}
		c.data = data
		c.seg, c.start = s, b.physStart
	}
	if srcOff >= int64(len(c.data)) {
		return 0, io.EOF
	}
	return copy(dst, c.data[srcOff:]), nil
}

// decodeBlock reads the block's payload into rawBuf and decompresses it into
// dataBuf, growing either as needed, and returns the (possibly regrown)
// buffers. data holds exactly the block's logical bytes and never aliases
// raw — raw (codec None) payloads are copied — so callers may recycle the two
// buffers independently.
func (s *segment) decodeBlock(st *scanStream, b blockRef, rawBuf, dataBuf []byte) (raw, data []byte, err error) {
	need := int(b.payloadLen())
	if cap(rawBuf) < need {
		rawBuf = make([]byte, need)
	}
	raw = rawBuf[:need]
	// Through the sweep's stream when there is one: a scan visits blocks in
	// ascending physical order, so these are exactly the reads that stream.
	readAt := s.backing.ReadAt
	if st != nil {
		readAt = st.ReadAt
	}
	if _, err := readAt(raw, b.payloadStart()); err != nil {
		return raw, nil, errors.Wrap(err, "read block payload failed")
	}
	data, err = b.codec.DecompressInto(dataBuf, raw)
	if err != nil {
		return raw, nil, errors.Wrap(err, "decompress block failed")
	}
	if int64(len(data)) != b.logicalLen {
		return raw, nil, fmt.Errorf("commitlog: block decompressed to %d bytes, want %d", len(data), b.logicalLen)
	}
	if len(data) > 0 && &data[0] == &raw[0] {
		if cap(dataBuf) < len(data) {
			dataBuf = make([]byte, len(data))
		}
		dataBuf = dataBuf[:len(data)]
		copy(dataBuf, data)
		data = dataBuf
	}
	return raw, data, nil
}

func (s *segment) notifyWaiters() {
	for r, ch := range s.waiters {
		close(ch)
		delete(s.waiters, r)
	}
}

func (s *segment) WaitForLEO(waiter interface{}, expectedLEO, actualLEO int64) <-chan struct{} {
	s.Lock()
	defer s.Unlock()
	// Check expected LEO against last known LEO and against the current
	// (active) segment's last offset in case the LEO changed since we last
	// checked it. If the current segment's last offset is -1, this means the
	// segment is empty and we should wait for data.
	if expectedLEO != actualLEO || (expectedLEO != s.lastOffset && s.lastOffset != -1) {
		// LEO has since changed so close channel immediately.
		ch := make(chan struct{})
		close(ch)
		return ch
	}
	return s.waitForData(waiter, s.position)
}
func (s *segment) WaitForData(waiter interface{}, pos int64) <-chan struct{} {
	s.Lock()
	ch := s.waitForData(waiter, pos)
	s.Unlock()
	return ch
}

func (s *segment) waitForData(waiter interface{}, pos int64) <-chan struct{} {
	// Check if we're already registered.
	wait, ok := s.waiters[waiter]
	if ok {
		return wait
	}
	wait = make(chan struct{})
	// Unblock immediately if the segment is sealed (no more data will be
	// written), if new data has been written past our position, or if the
	// segment has reached its maximum capacity.
	if s.sealed || s.position > pos || s.position >= s.maxBytes {
		close(wait)
	} else {
		s.waiters[waiter] = wait
	}
	return wait
}

func (s *segment) removeWaiter(waiter interface{}) {
	s.Lock()
	delete(s.waiters, waiter)
	s.Unlock()
}

// Sync flushes the segment's log file and index to stable storage. A no-op on
// a closed segment, and on one with nothing written since its last sync.
//
// The fsync deliberately runs OUTSIDE the segment lock. The append path needs
// that same lock, so holding it across an fsync stalls every append to this
// segment for the fsync's whole duration — which defeats a caller's group
// commit, since the appends that would form its next batch are exactly the ones
// landing while a sync is in flight. An append that lands mid-fsync is simply
// not covered by that fsync and waits for the next one, which is already the
// group-commit contract, so nothing is weakened: the segment stays marked dirty
// and the following sync flushes it.
func (s *segment) Sync() error {
	return s.sync(true)
}

// SyncData flushes ONLY the segment's log bytes, leaving the index alone. This
// is the durability hot path: an index behind its log is a state recovery
// already repairs, since the append path writes the log frame before the index
// entry and open rebuilds the missing tail. Skipping it halves the fsyncs a
// per-commit caller pays. The index is flushed when the segment seals, which is
// what keeps that repair confined to the active segment.
func (s *segment) SyncData() error {
	return s.sync(false)
}

// sync flushes the log bytes, and the index too when withIndex is set.
//
// Each half's dirty mark is taken BEFORE its fsync and restored if that fsync
// fails. Clearing first is what makes an append landing mid-flush safe — it
// re-marks the segment and rides the next sync rather than being erased by this
// one — and restoring on failure is what stops a reported error from leaving a
// segment that looks durable while its bytes are still in OS buffers.
func (s *segment) sync(withIndex bool) error {
	s.Lock()
	if s.closed {
		s.Unlock()
		return nil
	}
	var (
		backing = s.backing
		idx     *index // non-nil only if the index needs flushing
		data    = s.dirtyData
	)
	if withIndex && s.dirtyIndex {
		idx = s.Index // nil for an offloaded index: nothing local to flush
		s.dirtyIndex = false
	}
	s.dirtyData = false
	s.Unlock()
	if !data && idx == nil {
		return nil
	}

	// With the lock released a concurrent Clean can close the segment under us
	// (rewrites run outside the log mutex). Such a segment is already durable,
	// or is being made durable by the rewrite that closed it, so treat a closed
	// half as success — the same tolerance the whole-log sync path applies.
	var err error
	if data {
		if err = backing.Sync(); errors.Is(err, os.ErrClosed) {
			err = nil
		}
	}
	// The index carries its own mutex and takes it for both writes and flushes,
	// so flushing without the segment lock cannot race the remap-on-expand that
	// would otherwise flush a mapping being torn down.
	if err == nil && idx != nil {
		if ierr := idx.Sync(); ierr != nil &&
			!errors.Is(ierr, os.ErrClosed) && !errors.Is(ierr, ErrSegmentClosed) {
			err = ierr
		}
	}
	if err != nil {
		s.Lock()
		if data {
			s.dirtyData = true
		}
		if idx != nil {
			s.dirtyIndex = true
		}
		s.Unlock()
		return err
	}
	return nil
}

// Close a segment such that it can no longer be read from or written to. This
// operation is idempotent.
func (s *segment) Close() error {
	s.Lock()
	defer s.Unlock()
	return s.close()
}

func (s *segment) close() error {
	return s.closeSegment(true)
}

// closeDiscarding closes for a caller about to remove the segment's files, so
// the index is neither flushed nor shrunk on the way out. See
// index.CloseDiscarding. Callers hold the segment lock, as with close.
func (s *segment) closeDiscarding() error {
	return s.closeSegment(false)
}

func (s *segment) closeSegment(durable bool) error {
	if s.closed {
		return nil
	}
	// Close BOTH halves before reporting either failure. Bailing out after a
	// failed backing close skipped the index close, leaving the index MAPPED and
	// the segment still marked open — and a mapped file cannot be unlinked on
	// Windows, so every later attempt to remove that segment failed with a
	// sharing violation and the log grew without bound.
	//
	// An already-closed half is the state close() wants, not a failure: like the
	// sync path, a maintenance pass can reach a segment another pass just closed
	// (rewrites run outside the log mutex). Treat os.ErrClosed as success so the
	// segment still reaches the closed state instead of getting stuck open.
	berr := s.backing.Close()
	if errors.Is(berr, os.ErrClosed) {
		berr = nil
	}
	var ierr error
	if s.Index != nil {
		if durable {
			ierr = s.Index.Close()
		} else {
			ierr = s.Index.CloseDiscarding()
		}
		if errors.Is(ierr, os.ErrClosed) {
			ierr = nil
		}
	}
	if berr != nil {
		return berr
	}
	if ierr != nil {
		return ierr
	}
	s.closed = true
	s.seal()
	return nil
}

// newWorkingSegment opens the suffixed working copy a rewrite builds before
// renaming it over its source (Cleaned/Truncated/Trimmed).
//
// The working copy MUST start empty. A process killed mid-rewrite leaves its
// half-written copy on disk — reopening skips it (open() matches only ".log"),
// so it survives to the next maintenance pass, and the backing opens
// O_CREATE|O_APPEND. Without this the next pass would append its rewrite AFTER
// the dead pass's bytes and then rename that frankenstein over live data: the
// digest rebuild panics on the malformed leading frame, and the panic unwinds
// leaving the source segment's index still mapped, so every later attempt to
// remove it fails on Windows with a sharing violation. Discarding the leftover
// is always safe — a working copy holds no committed data until its rename.
func newWorkingSegment(path string, baseOffset, maxBytes int64, suffix string, codec compress.Codec) (*segment, error) {
	for _, stem := range []string{logSuffix, indexSuffix} {
		p := filepath.Join(path, fmt.Sprintf(fileFormat, baseOffset, stem+suffix))
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return nil, errors.Wrap(err, "remove stale rewrite working copy")
		}
	}
	return newSegment(path, baseOffset, maxBytes, false, suffix, codec)
}

// ReplaceOffloaded installs fresh — a fully-written LOCAL working segment — as
// the current objects of this offloaded segment, and returns the object keys the
// rewrite superseded.
//
// This is what lets a tiered segment be compacted at all. A local rewrite gets
// its atomicity from Replace's rename over the same path; a store has no
// equivalent, because Put overwrites unconditionally and cannot be made
// conditional. A fresh key is the substitute: the new bytes go to a key
// nothing is reading, and the MARKER is the commit point that decides which
// object the segment reads, exactly as it does for a first offload.
//
// Ordering, and what a crash at each step leaves:
//
//  1. upload the new log object, and the index object for an offloaded index.
//     A crash here leaves objects nothing points at — orphans, reclaimable by
//     comparing the store's keys against the markers.
//  2. rewrite the marker. THIS IS THE COMMIT POINT. Before it the segment is
//     the old object; after it, the new one.
//  3. invalidate the caches that would otherwise keep serving the old bytes:
//     the backing's read-ahead window and, for an offloaded index, its cache
//     entry. Skipping this is how a rewrite would appear to succeed and still
//     serve pre-rewrite reads.
//  4. swap in a backing over the new key.
//
// The superseded objects are QUEUED rather than deleted here. A reader that
// opened this segment before the swap holds a backing over the old key and is
// entitled to finish; deleting underneath it would turn a rewrite into a read
// error. Each returned entry carries the backing that was serving the object,
// so the log can tell when the last such reader is gone — see pendingReclaim.
// That is also why deletion must be explicit rather than implied by the
// overwrite it replaces: a rewrite that empties a segment leaves the old
// objects behind with nothing to overwrite them.
func (s *segment) ReplaceOffloaded(fresh *segment, cache *RemoteIndexCache) ([]pendingReclaim, error) {
	s.Lock()
	defer s.Unlock()
	if s.store == nil {
		return nil, errors.New("commitlog: segment is not offloaded")
	}

	// The backing this segment is serving reads from right now. It becomes the
	// superseded one below, and whoever holds it is why its object cannot be
	// deleted yet. Read here, under the same lock the swap happens under.
	oldBacking, _ := s.backing.(*storeBacking)

	var (
		newKey, freshIndexKey = newStoreKeys(s.BaseOffset)
		newIndexKey           string
		superseded            = []pendingReclaim{{key: s.storeKey, pin: oldBacking}}
	)

	size, err := fresh.backing.Size()
	if err != nil {
		return nil, err
	}
	if err := s.store.Put(newKey, io.NewSectionReader(fresh.backing, 0, size), size); err != nil {
		return nil, errors.Wrap(err, "put rewritten segment")
	}
	if s.indexKey != "" {
		newIndexKey = freshIndexKey
		r, isize, err := fresh.Index.offloadReader()
		if err != nil {
			return nil, errors.Wrap(err, "read rewritten index")
		}
		if err := s.store.Put(newIndexKey, r, isize); err != nil {
			return nil, errors.Wrap(err, "put rewritten index")
		}
		// No pin: an index object has no long-lived holder to count. It is
		// fetched whole into indexCache, which is invalidated below, so a reader
		// either already has its bytes or will fetch the new key. Only a fetch
		// already in flight can still be reading this object, and what covers
		// that is the deferral — the entry is not considered for deletion until
		// a later pass, by which time a single in-flight request is long done.
		superseded = append(superseded, pendingReclaim{key: s.indexKey})
	}

	meta := offloadMeta{
		LogKey:         newKey,
		IndexKey:       newIndexKey,
		FirstOffset:    fresh.firstOffset,
		LastOffset:     fresh.lastOffset,
		FirstWriteTime: fresh.firstWriteTime,
		LastWriteTime:  fresh.lastWriteTime,
		Position:       fresh.position,
		PhysPosition:   size,
		BlockMode:      fresh.blockMode,
	}
	markerBytes, err := json.Marshal(meta)
	if err != nil {
		return nil, errors.Wrap(err, "encode offload marker")
	}
	if err := os.WriteFile(s.offloadMarkerPath(), markerBytes, 0o644); err != nil {
		return nil, errors.Wrap(err, "write offload marker")
	}

	// Committed. From here the segment IS the new object, so anything still
	// able to serve the old one has to be cleared before the swap.
	if sb, ok := s.backing.(*storeBacking); ok {
		sb.Invalidate()
	}
	if s.indexKey != "" && s.indexCache != nil {
		// s.indexKey is still the OLD object's key here -- the swap to
		// newIndexKey happens below, after the marker is committed -- so this
		// names the entry that is about to describe an object nobody should
		// read again.
		s.indexCache.Invalidate(s.indexKey)
	}

	// An option-1 offloaded segment keeps its index on LOCAL disk, and that
	// index describes the bytes it was built from. Leaving it in place after a
	// rewrite points every seek at positions in the superseded object — the
	// records are still in the store, and the log reads back short. Install the
	// rewrite's index over it, the same way a local Replace does.
	localIndex := s.indexKey == "" && s.Index != nil && fresh.Index != nil

	sb, err := newStoreBackingSize(s.store, newKey, size)
	if err != nil {
		return nil, err
	}
	s.backing = sb
	s.storeKey = newKey
	s.indexKey = newIndexKey
	s.firstOffset = fresh.firstOffset
	s.lastOffset = fresh.lastOffset
	s.firstWriteTime = fresh.firstWriteTime
	s.lastWriteTime = fresh.lastWriteTime
	s.position = fresh.position
	s.physPosition = size
	s.blocks = fresh.blocks
	s.blockMode = fresh.blockMode

	// Installed LAST, because setupIndex re-derives the segment's boundaries
	// from the index and needs the new blockMode, blocks and position to read it
	// the way the rewrite wrote it.
	if localIndex {
		if err := s.Index.Close(); err != nil {
			return nil, errors.Wrap(err, "close stale local index")
		}
		if err := fresh.Index.Close(); err != nil {
			return nil, errors.Wrap(err, "close rewritten index")
		}
		if err := os.Rename(fresh.indexPath(), s.indexPath()); err != nil {
			return nil, errors.Wrap(err, "install rewritten index")
		}
		if err := s.setupIndex(); err != nil {
			return nil, errors.Wrap(err, "reopen rewritten index")
		}
	}
	return superseded, nil
}

// Cleaned creates a cleaned segment for this segment.
func (s *segment) Cleaned() (*segment, error) {
	return newWorkingSegment(s.path, s.BaseOffset, s.maxBytes, cleanedSuffix, s.codec)
}

// Truncated creates a truncated segment for this segment.
func (s *segment) Truncated() (*segment, error) {
	return newWorkingSegment(s.path, s.BaseOffset, s.maxBytes, truncatedSuffix, s.codec)
}

// Trimmed creates a new segment at baseOffset with trimmedSuffix, used when
// rewriting a segment to drop records before a given offset during TruncateBefore.
// The new segment has a different BaseOffset than the receiver.
func (s *segment) Trimmed(baseOffset int64) (*segment, error) {
	return newWorkingSegment(s.path, baseOffset, s.maxBytes, trimmedSuffix, s.codec)
}

// Finalize promotes a trimmed segment (one with trimmedSuffix) to its final
// name by renaming the backing files to remove the suffix, then reopens it.
// Called after writing kept records into a Trimmed segment.
func (s *segment) Finalize() error {
	s.Lock()
	defer s.Unlock()
	if err := s.close(); err != nil {
		return err
	}
	finalLog := filepath.Join(s.path, fmt.Sprintf(fileFormat, s.BaseOffset, logSuffix))
	finalIdx := filepath.Join(s.path, fmt.Sprintf(fileFormat, s.BaseOffset, indexSuffix))
	if err := os.Rename(s.logPath(), finalLog); err != nil {
		return errors.Wrap(err, "rename trimmed log failed")
	}
	if err := os.Rename(s.indexPath(), finalIdx); err != nil {
		return errors.Wrap(err, "rename trimmed index failed")
	}
	s.suffix = ""
	backing, err := openLocalBacking(s.logPath())
	if err != nil {
		return errors.Wrap(err, "reopen trimmed segment failed")
	}
	s.backing = backing
	s.closed = false
	if err := s.initPositions(); err != nil {
		return err
	}
	return s.setupIndex()
}

// Replace replaces the given segment with the callee.
func (s *segment) Replace(old *segment) error {
	s.Lock()
	defer s.Unlock()
	old.Lock()
	defer old.Unlock()
	if err := old.close(); err != nil {
		return err
	}
	if err := s.close(); err != nil {
		return err
	}
	if err := os.Rename(s.logPath(), old.logPath()); err != nil {
		return err
	}
	if err := os.Rename(s.indexPath(), old.indexPath()); err != nil {
		return err
	}
	s.suffix = ""
	backing, err := openLocalBacking(s.logPath())
	if err != nil {
		return errors.Wrap(err, "open file failed")
	}
	s.backing = backing
	s.closed = false
	old.replaced = true
	old.replacement = s
	if err := s.initPositions(); err != nil {
		return err
	}
	return s.setupIndex()
}

// SupersededBy records that next carries the records this segment still owes a
// reader, so current() redirects to it instead of reporting the segment gone.
//
// Replace does this as part of renaming a rewrite over its source. A boundary
// segment trimmed at a NEW base offset cannot take that path — its replacement
// is a differently NAMED file, so there is nothing to rename over — and
// TruncateBefore, which is the only caller in that position, deleted the source
// without ever recording the link. A reader already resolved into it then read
// a gone segment with no replacement, which is the retention case: skip to the
// NEXT segment. So it skipped the surviving records sitting in the trim, which
// were exactly the ones the caller had asked to keep.
//
// The distinction current() draws is by the LINK, not the flags. Anything that
// deletes a segment whose records did not all go with it owes this call.
func (s *segment) SupersededBy(next *segment) {
	s.Lock()
	defer s.Unlock()
	s.replaced = true
	s.replacement = next
}

// replacementDepth bounds how far current() follows the chain. A stale pointer
// gains one link per compaction pass that touches it, so any real chain is one
// or two long; the bound only stops a corrupted cycle from hanging a reader.
const replacementDepth = 64

// current resolves s to the segment a reader should actually use: s itself, the
// segment that superseded it, or nothing at all when compaction removed it.
//
// It exists because a pass mutates segments long before the log publishes the
// result. Installing a rewrite renames the new files over the source's and
// CLOSES the source (Replace); a segment whose every record was superseded is
// deleted outright (cleanupEmptySegment); and retention deletes the segments
// over its limit as it walks them. None of that leaves l.segments — that list
// is swapped once, at the very end of the pass — so for the whole of it the log
// hands out segments that are closed or gone, and resolving an offset through
// one fails with ErrSegmentClosed for a record that is either sitting in the
// replacement or legitimately no longer anywhere. The symptom was an ordinary
// Read against a maintaining log failing at random with "segment has been
// closed".
//
// The cases are told apart by the link, not by the flags: a replacement is a
// redirect, and anything else that is gone — deleted by retention, or rewritten
// to nothing — reports ok=false so findSegment moves on to the next segment,
// which is what a reader already does after retention has run to completion.
func (s *segment) current() (*segment, bool) {
	for range replacementDepth {
		s.RLock()
		next, gone := s.replacement, s.replaced || s.gone
		s.RUnlock()
		if next == nil {
			return s, !gone
		}
		s = next
	}
	return s, true
}

// findEntry returns the first entry whose offset is greater than or equal to
// the given offset. For a raw segment this binary-searches the dense index and
// returns the exact per-message entry. For a block-compressed segment it
// binary-searches the sparse (per-block) index for the block that may contain
// the offset, then scans that block's frames forward to the first message with
// offset >= the target, yielding an exact per-message entry (position, size,
// timestamp) just as the dense path does.
func (s *segment) findEntry(offset int64) (*entry, error) {
	s.RLock()
	defer s.RUnlock()
	// A segment that has left the log answers "re-resolve", not "closed". The
	// index only knows it is shut, so without this a lookup on a segment a pass
	// had already swapped or deleted came back as ErrSegmentClosed — which a
	// reader can do nothing with, where ErrSegmentReplaced sends it to the
	// segment list for the live one.
	if s.replaced || s.gone {
		return nil, ErrSegmentReplaced
	}
	if s.blockMode {
		anchor, err := s.anchorPositionForOffset(offset)
		if err != nil {
			return nil, err
		}
		return s.scanForward(anchor, func(m messageSet) bool {
			return m.Offset() >= offset
		})
	}
	var result *entry
	err := s.withIndex(func(index *index) error {
		entry := &entry{}
		n := int(index.Position() / entryWidth)
		var serr error
		i := sort.Search(n, func(i int) bool {
			if e := index.ReadEntryAtFileOffset(entry, int64(i*entryWidth)); e != nil {
				serr = e
				return true
			}
			return entry.Offset >= offset
		})
		if serr != nil {
			return serr
		}
		if i == n {
			return ErrEntryNotFound
		}
		if e := index.ReadEntryAtFileOffset(entry, int64(i*entryWidth)); e != nil {
			return e
		}
		result = entry
		return nil
	})
	return result, err
}

// findEntryByTimestamp returns the first entry whose timestamp is greater than
// or equal to the given timestamp. For a block-compressed segment the sparse
// index gives per-block granularity; findEntryByTimestamp locates the block
// that may contain the first qualifying message and scans forward to it,
// returning an exact per-message entry.
func (s *segment) findEntryByTimestamp(timestamp int64) (*entry, error) {
	s.RLock()
	defer s.RUnlock()
	// A segment that has left the log answers "re-resolve", not "closed". The
	// index only knows it is shut, so without this a lookup on a segment a pass
	// had already swapped or deleted came back as ErrSegmentClosed — which a
	// reader can do nothing with, where ErrSegmentReplaced sends it to the
	// segment list for the live one.
	if s.replaced || s.gone {
		return nil, ErrSegmentReplaced
	}
	if s.blockMode {
		anchor, err := s.anchorPositionForTimestamp(timestamp)
		if err != nil {
			return nil, err
		}
		return s.scanForward(anchor, func(m messageSet) bool {
			return m.Timestamp() >= timestamp
		})
	}
	var result *entry
	err := s.withIndex(func(index *index) error {
		entry := &entry{}
		n := int(index.CountEntries())
		var serr error
		i := sort.Search(n, func(i int) bool {
			if e := index.ReadEntryAtLogOffset(entry, int64(i)); e != nil {
				serr = e
				return true
			}
			return entry.Timestamp >= timestamp
		})
		if serr != nil {
			return serr
		}
		if i == n {
			return ErrEntryNotFound
		}
		if e := index.ReadEntryAtLogOffset(entry, int64(i)); e != nil {
			return e
		}
		result = entry
		return nil
	})
	return result, err
}

// anchorPositionForOffset binary-searches the sparse index for the block that
// may contain the given offset (the greatest block base offset <= offset) and
// returns that block's logical start position, the point from which to scan
// frames forward. Callers hold the segment read lock.
func (s *segment) anchorPositionForOffset(offset int64) (int64, error) {
	var pos int64
	err := s.withIndex(func(index *index) error {
		n := int(index.Position() / entryWidth)
		if n == 0 {
			return nil
		}
		e := &entry{}
		// First anchor whose base offset is strictly greater than the target; the
		// containing block is the one before it.
		idx := sort.Search(n, func(i int) bool {
			if err := index.ReadEntryAtFileOffset(e, int64(i*entryWidth)); err != nil {
				return true
			}
			return e.Offset > offset
		})
		if idx > 0 {
			idx--
		}
		if err := index.ReadEntryAtFileOffset(e, int64(idx*entryWidth)); err != nil {
			return nil
		}
		pos = e.Position
		return nil
	})
	return pos, err
}

// anchorPositionForTimestamp binary-searches the sparse index for the block
// from which to begin scanning for the first message with timestamp >= the
// target. It starts one block before the first anchor whose timestamp is >=
// the target, since the qualifying message may be an interior message of the
// preceding block. Callers hold the segment read lock.
func (s *segment) anchorPositionForTimestamp(timestamp int64) (int64, error) {
	var pos int64
	err := s.withIndex(func(index *index) error {
		n := int(index.Position() / entryWidth)
		if n == 0 {
			return nil
		}
		e := &entry{}
		idx := sort.Search(n, func(i int) bool {
			if err := index.ReadEntryAtFileOffset(e, int64(i*entryWidth)); err != nil {
				return true
			}
			return e.Timestamp >= timestamp
		})
		if idx > 0 {
			idx--
		}
		if err := index.ReadEntryAtFileOffset(e, int64(idx*entryWidth)); err != nil {
			return nil
		}
		pos = e.Position
		return nil
	})
	return pos, err
}

// scanForward walks message frames from the given logical start position and
// returns the entry for the first frame satisfying match, or ErrEntryNotFound
// at end of segment. It reads only frame headers via the segment's logical byte
// space (transparently spanning blocks), so it naturally continues past a block
// boundary when the target lies in a later block. Callers hold the read lock.
func (s *segment) scanForward(start int64, match func(m messageSet) bool) (*entry, error) {
	hdr := make([]byte, msgSetHeaderLen)
	pos := start
	for {
		if _, err := s.readAtLocked(hdr, pos); err != nil {
			// ONLY the end of the segment means "nothing here matched". This scan
			// has no end bound — it relies on the read failing to terminate — so
			// io.EOF is its stop condition and ErrEntryNotFound is the right answer.
			//
			// Every OTHER error used to arrive here as ErrEntryNotFound too, and
			// that was a silent wrong answer rather than a lost one.
			// ErrSegmentClosed, ErrSegmentReplaced (which compaction produces
			// routinely, and which Reader.ReadMessage RETRIES rather than accepts),
			// a corrupt index, a failed store fetch for a tiered segment: each
			// became "no entry matches your timestamp", and both
			// LatestOffsetBeforeTimestamp and EarliestOffsetAfterTimestamp turn
			// that into a plausible offset with a NIL error — the newest offset in
			// the segment, or one past the end of the log. A consumer resuming
			// as-of a timestamp would be told it was already at the end and skip
			// every record it had not read.
			if errors.Is(err, io.EOF) {
				return nil, ErrEntryNotFound
			}
			return nil, errors.Wrapf(err, "scan for entry at position %d", pos)
		}
		m := messageSet(hdr)
		size := m.Size()
		if match(m) {
			return &entry{
				Offset:      m.Offset(),
				Timestamp:   m.Timestamp(),
				LeaderEpoch: m.LeaderEpoch(),
				Position:    pos,
				Size:        size + msgSetHeaderLen,
			}, nil
		}
		pos += int64(msgSetHeaderLen) + int64(size)
	}
}

// Delete closes the segment and then deletes its log and index files. For an
// offloaded segment it removes the store object and the .offloaded marker
// instead of a local .log (the log file is already gone); the local index is
// still removed.
//
// The store object is NOT fenced against the log's current writer identity, and
// that is deliberate. The writer fence exists for keys a caller learned from a
// store LISTING, where nothing establishes that the object is theirs (see
// CommitLog.DeleteStoreObjects). A segment the log HOLDS is a different claim
// entirely: its own offload marker names the object, which is stronger evidence
// of ownership than the stamp and is what the log has always acted on.
//
// Fencing here instead breaks retention outright. After ownership moves, every
// segment already in the tier carries the PREVIOUS identity's stamp, so a fenced
// retention pass could never drop its oldest segment again and the tier would
// grow without bound — strictly worse than the orphaned object the fence was
// protecting against.
//
// The rule this rests on — that a log owns what its own markers name — is the
// same one that lets a caller reclaim superseded keys, and it costs nothing
// even where several processes share a store: if two of them held markers
// naming the SAME object, neither could safely delete it whatever stamp it
// carried, so the fence would not have saved that topology either.
func (s *segment) Delete() error {
	s.Lock()
	defer s.Unlock()
	// closeDiscarding, not close: everything this segment owns is unlinked
	// below, so flushing and shrinking its index first is work whose result
	// nothing can ever read.
	if err := s.closeDiscarding(); err != nil {
		return err
	}
	// Nothing may resolve an offset through this segment again. It matters
	// because a retention pass, exactly like a compaction one, deletes as it goes
	// and does not publish the surviving list until the pass ends — so a deleted
	// segment stays in l.segments, and findSegment would hand it to a reader that
	// then failed for offsets retention had lawfully collected. Marked here
	// rather than at the cleaner because every path that deletes a segment owes
	// this, including the ones that do it outside a pass.
	//
	// Closing and marking gone are ONE step, under one hold of the lock. Closing
	// first and marking at the end left a window where the segment was closed but
	// not yet gone, and a reader that resolved into it there got the raw
	// ErrSegmentClosed the flag exists to turn into a redirect. Marking before the
	// removal below can only mean a segment whose files survive a failed delete is
	// skipped rather than errored on — and it is closed either way, so skipping is
	// the better of the two answers.
	s.gone = true
	if s.isOffloaded() {
		if err := s.store.Delete(s.storeKey); err != nil {
			return errors.Wrap(err, "delete offloaded object")
		}
		// Option 2: the index is an object too — remove it (retention reclaims the
		// store).
		if s.indexKey != "" {
			if err := s.store.Delete(s.indexKey); err != nil {
				return errors.Wrap(err, "delete offloaded index object")
			}
		}
		if err := os.Remove(s.offloadMarkerPath()); err != nil && !os.IsNotExist(err) {
			return errors.Wrap(err, "remove offload marker")
		}
	} else if exists(s.backing.Name()) {
		if err := os.Remove(s.backing.Name()); err != nil {
			return err
		}
	}
	// A local index file exists unless the index was offloaded (Index nil).
	if s.Index != nil && exists(s.Index.Name()) {
		if err := os.Remove(s.Index.Name()); err != nil {
			return err
		}
	}
	// A final segment (no working suffix) also owns a key-digest sidecar;
	// suffixed working copies (.cleaned/.truncated/.trimmed) share the base
	// offset with the real segment and must not remove its digest.
	if s.suffix == "" {
		removeKeyDigest(s)
	}
	return nil
}

type segmentScanner struct {
	s   *segment
	pos int64
	// stream is the sweep's single open read over the segment. A scan walks
	// every frame front to back, so the reads it makes are precisely the ones
	// worth serving from one stream rather than one ranged request each —
	// against an object store that is the difference between a GET per window
	// and a GET per segment, and cost there is per request, not per byte.
	//
	// Opened lazily on the first read and closed by Close.
	stream *scanStream
	// cache holds the scan's block-decode buffers. Passing one cache to
	// every scanner of a multi-segment pass (clean, digest build,
	// consolidation) keeps the whole pass at one retained buffer pair;
	// letting scans hit the segments' own caches instead left each scanned
	// segment holding one for its lifetime (run 32: ~500MB-1GB transients).
	cache *blockCache
	// pin is the claim this scan holds on a tiered backing, released on Close.
	// Nil for a local segment, which has nothing to reclaim. It is what keeps a
	// superseded object alive while this scan is still reading it.
	pin *storeBacking
}

// segmentScans counts scanner constructions; tests assert on it to prove a
// converged clean touches no sealed segment's records.
var segmentScans atomic.Int64

func newSegmentScanner(segment *segment) *segmentScanner {
	return newSegmentScannerCache(segment, newBlockCache())
}

// newSegmentScannerCache scans with a caller-owned (typically pass-shared)
// decode cache.
func newSegmentScannerCache(segment *segment, c *blockCache) *segmentScanner {
	segmentScans.Add(1)
	// Under the lock: a rewrite swaps the backing while holding the write lock,
	// so reading the field bare races it. The scan then keeps the backing it
	// started on, which is the same guarantee a rewrite already gives a reader —
	// the object a reader opened is never the one a rewrite replaces.
	//
	// The claim is registered under that SAME lock, not after it. A rewrite
	// decides an object is unreferenced while holding the write lock; acquiring
	// outside it lets a scanner take a backing the rewrite has already judged
	// finished with, and the object is then deleted under a live reader.
	segment.RLock()
	backing := segment.backing
	pin := acquireBacking(backing)
	segment.RUnlock()
	// Only where it pays. A nil stream reads straight through to the backing,
	// so a local scan behaves exactly as it did before streaming existed.
	var stream *scanStream
	if backing.StreamPays() {
		stream = newScanStream(backing)
	}
	return &segmentScanner{
		s:      segment,
		cache:  c,
		stream: stream,
		pin:    pin,
	}
}

// Close releases the scan's stream. A scanner that is dropped without it leaks
// whatever the backing handed out — a file descriptor locally, an open HTTP
// response against an object store.
func (s *segmentScanner) Close() error {
	// The pin outlives the stream deliberately. Scan releases the stream as soon
	// as it hits the end (see there), but the scanner may still read through the
	// backing afterwards, and the object has to stay until the scan is done with
	// it — which is here. Cleared so a second Close does not double-release.
	if s.pin != nil {
		s.pin.release()
		s.pin = nil
	}
	return s.stream.Close()
}

// Scan should be called repeatedly to iterate over the messages in the
// segment, it will return io.EOF when there are no more messages.
//
// The scanner walks the segment's logical byte space directly (one message
// frame at a time) rather than the index, so it iterates every message
// regardless of index density. This is required for block-compressed segments,
// whose sparse index has only one entry per block, and is equivalent to the
// old index-driven scan for raw segments (positions and sizes match).
func (s *segmentScanner) Scan() (messageSet, *entry, error) {
	header := make(messageSet, msgSetHeaderLen)
	if _, err := s.s.scanReadAt(s.cache, s.stream, header, s.pos); err != nil {
		// The scan is over, so release the stream HERE rather than leaving it to
		// the caller's defer. A caller typically rewrites the segment straight
		// after draining it, and the defer would not have run yet: on Windows an
		// open read handle blocks the rename that installs the rewrite, turning
		// a leaked handle into a hard failure rather than a slow leak.
		s.stream.Close()
		return nil, nil, err
	}
	// Nothing below may trust the header until the header says it can be
	// trusted. Its CRC covers the size field, and reading a length out of bytes
	// that failed it was how damage in ONE segment killed the whole process: the
	// size went straight into make(), and a size out of range is not an error
	// there, it is a panic. Unrecoverable in the caller, fatal to every unrelated
	// log in the same binary, and raised during routine maintenance rather than
	// at a moment anyone is watching.
	//
	// The read path has checked this since the frame header CRC existed. The scan
	// never did, which is why the paths that walk a segment to REWRITE it —
	// compaction, Truncate, TruncateBefore — were the ones that could be made to
	// crash by a caller's damaged data.
	if want, got := storedHeaderCrc(header), headerCrc(header); want != got {
		s.stream.Close()
		return nil, nil, errors.Wrapf(ErrCorruptRecord,
			"frame header at %d failed CRC: expected 0x%08x, got 0x%08x",
			s.pos, want, got)
	}
	size := header.Size()
	// A frame cannot be longer than what follows its own header. A size that
	// says otherwise passed its CRC, so the damage is elsewhere — in the length
	// itself, or in the segment's extent — and either way there is nothing here
	// to read.
	if remaining := s.s.Position() - (s.pos + msgSetHeaderLen); int64(size) > remaining {
		s.stream.Close()
		return nil, nil, errors.Wrapf(ErrCorruptRecord,
			"frame at %d declares %d payload bytes with %d left in the segment",
			s.pos, size, remaining)
	}
	payload := make([]byte, size)
	if _, err := s.s.scanReadAt(s.cache, s.stream, payload, s.pos+msgSetHeaderLen); err != nil {
		return nil, nil, err
	}
	msgSet := append(header, payload...)
	e := &entry{
		Offset:      header.Offset(),
		Timestamp:   header.Timestamp(),
		LeaderEpoch: header.LeaderEpoch(),
		Position:    s.pos,
		Size:        size + msgSetHeaderLen,
	}
	s.pos += int64(msgSetHeaderLen) + int64(size)
	return msgSet, e, nil
}

func (s *segment) logPath() string {
	return filepath.Join(s.path, fmt.Sprintf(fileFormat, s.BaseOffset, logSuffix+s.suffix))
}

func (s *segment) indexPath() string {
	return filepath.Join(s.path, fmt.Sprintf(fileFormat, s.BaseOffset, indexSuffix+s.suffix))
}
