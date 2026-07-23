package commitlog

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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

// indexOffloaded reports whether the segment's index (not just its log) lives in
// the store (tiered storage, option 2), so seeks fetch it via indexCache.
func (s *segment) indexOffloaded() bool { return s.Index == nil && s.indexCache != nil }

// indexCacheKey uniquely identifies this segment's index across every log in the
// process (the shared cache is keyed by it), so two logs' like-named index
// objects never collide.
func (s *segment) indexCacheKey() string {
	return s.path + "|" + strconv.FormatInt(s.BaseOffset, 10)
}

// withIndex runs fn against the segment's index: the resident local index for a
// normal or option-1 offloaded segment, or — for an option-2 offloaded segment
// whose index lives in the store — the index fetched into the shared cache on
// this seek and released after. Callers hold the segment read lock.
func (s *segment) withIndex(fn func(idx *index) error) error {
	if s.Index != nil {
		return fn(s.Index)
	}
	if s.indexCache == nil || s.store == nil {
		return errIndexCorrupt
	}
	idx, release, err := s.indexCache.acquire(s.store, s.indexKey, s.indexCacheKey(), s.BaseOffset)
	if err != nil {
		return err
	}
	defer release()
	return fn(idx)
}

func (s *segment) offloadMarkerPath() string {
	return filepath.Join(s.path, fmt.Sprintf(fileFormat, s.BaseOffset, offloadedSuffix))
}

// segmentStoreKey is the store object key for a segment at baseOffset: the
// zero-padded base offset, matching the local log filename stem so keys are
// unique within a per-log store and stable across restarts.
func segmentStoreKey(baseOffset int64) string {
	return fmt.Sprintf("%020d%s", baseOffset, logSuffix)
}

// segmentIndexStoreKey is the store object key for an offloaded segment's index
// (option 2), mirroring the log key with the index suffix.
func segmentIndexStoreKey(baseOffset int64) string {
	return fmt.Sprintf("%020d%s", baseOffset, indexSuffix)
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
func (s *segment) offloadTo(store SegmentStore, key string, cache *RemoteIndexCache) error {
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
		indexKey = segmentIndexStoreKey(s.BaseOffset)
		r, isize, err := s.Index.offloadReader()
		if err != nil {
			return errors.Wrap(err, "read index for offload")
		}
		if err := store.Put(indexKey, r, isize); err != nil {
			return errors.Wrap(err, "offload index put")
		}
	}

	sb, err := newStoreBackingSize(store, key, size)
	if err != nil {
		return err
	}
	localLog := s.backing.Name()
	if err := s.backing.Close(); err != nil {
		return errors.Wrap(err, "close local backing")
	}
	var localIndex string
	if cache != nil {
		localIndex = s.Index.Name()
		if err := s.Index.Close(); err != nil {
			return errors.Wrap(err, "close local index")
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
	s.storeKey = key
	if cache != nil {
		s.Index = nil
		s.indexKey = indexKey
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
		storeKey:    meta.LogKey,
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
// in the file, and sets position (logical total) and physPosition (file size).
func (s *segment) scanBlocks(size int64) error {
	var (
		phys    int64
		logical int64
		hdr     [blockHeaderLen]byte
	)
	for phys < size {
		if _, err := s.backing.ReadAt(hdr[:], phys); err != nil {
			return errors.Wrap(err, "read block header failed")
		}
		codec, uLen, cLen, err := parseBlockHeader(hdr[:])
		if err != nil {
			return err
		}
		physLen := int64(blockHeaderLen) + int64(cLen)
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
		return fmt.Errorf("commitlog: block scan overran segment (%d != %d)", phys, size)
	}
	s.position = logical
	s.physPosition = size
	return nil
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
	_, data, err := s.decodeBlock(*blk, nil, nil)
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
		if s.replaced {
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
	return s.readBlocksCache(s.cache, p, off)
}

func (s *segment) readBlocksCache(c *blockCache, p []byte, off int64) (int, error) {
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
		m, err := s.blockCopyIntoCache(c, p[n:], *blk, cur-blk.logicalStart)
		if err != nil {
			return n, err
		}
		n += m
	}
	return n, nil
}

// scanReadAt is ReadAt for one-shot sequential scans: block decodes go
// through the caller's cache instead of the segment's, so a pass over N
// segments retains one decode buffer pair, not N (see blockCopyIntoCache).
func (s *segment) scanReadAt(c *blockCache, p []byte, off int64) (n int, err error) {
	s.RLock()
	defer s.RUnlock()
	if s.closed {
		if s.replaced {
			return 0, ErrSegmentReplaced
		}
		return 0, ErrSegmentClosed
	}
	if !s.blockMode {
		return s.backing.ReadAt(p, off)
	}
	return s.readBlocksCache(c, p, off)
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

// blockCopyInto copies the block's decompressed bytes from srcOff into dst,
// decoding at most once per block visit via the segment's single-entry
// cache. The cache OWNS its buffers and recycles them on displacement —
// callers only ever receive copies, made under the cache lock, so a
// displaced buffer is never observed mid-overwrite. Before recycling, every
// displacement abandoned a fresh decode buffer to the GC; run 31's anomaly
// heap capture showed ~276MB of those pending collection during one clean
// pass over a ~1200-segment stream.
func (s *segment) blockCopyInto(dst []byte, b blockRef, srcOff int64) (int, error) {
	return s.blockCopyIntoCache(s.cache, dst, b, srcOff)
}

// blockCopyIntoCache is blockCopyInto against a caller-chosen cache: readers
// use the segment's own (repeated reads of a hot segment), one-shot scans use
// a per-PASS cache shared across every segment the scan visits — routing
// scans through the segment cache left every scanned segment retaining a
// decode buffer pair for its lifetime, O(segments) heap per clean pass
// (run 32's ~500MB-1GB transients and creeping baseline).
func (s *segment) blockCopyIntoCache(c *blockCache, dst []byte, b blockRef, srcOff int64) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.seg != s || c.start != b.physStart {
		raw, data, err := s.decodeBlock(b, c.raw, c.data)
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
func (s *segment) decodeBlock(b blockRef, rawBuf, dataBuf []byte) (raw, data []byte, err error) {
	need := int(b.payloadLen())
	if cap(rawBuf) < need {
		rawBuf = make([]byte, need)
	}
	raw = rawBuf[:need]
	if _, err := s.backing.ReadAt(raw, b.payloadStart()); err != nil {
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

// blockData returns a freshly allocated copy of the block's decompressed
// bytes, bypassing the cache. For rare non-hot paths (open-time last-frame
// recovery) where holding a slice across other reads is convenient.
func (s *segment) blockData(b blockRef) ([]byte, error) {
	_, data, err := s.decodeBlock(b, nil, nil)
	return data, err
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
// a closed segment.
func (s *segment) Sync() error {
	s.Lock()
	defer s.Unlock()
	if s.closed {
		return nil
	}
	if err := s.backing.Sync(); err != nil {
		return err
	}
	if s.Index == nil {
		return nil // offloaded index: nothing local to sync
	}
	return s.Index.Sync()
}

// Close a segment such that it can no longer be read from or written to. This
// operation is idempotent.
func (s *segment) Close() error {
	s.Lock()
	defer s.Unlock()
	return s.close()
}

func (s *segment) close() error {
	if s.closed {
		return nil
	}
	if err := s.backing.Close(); err != nil {
		return err
	}
	if s.Index != nil {
		if err := s.Index.Close(); err != nil {
			return err
		}
	}
	s.closed = true
	s.seal()
	return nil
}

// Cleaned creates a cleaned segment for this segment.
func (s *segment) Cleaned() (*segment, error) {
	return newSegment(s.path, s.BaseOffset, s.maxBytes, false, cleanedSuffix, s.codec)
}

// Truncated creates a truncated segment for this segment.
func (s *segment) Truncated() (*segment, error) {
	return newSegment(s.path, s.BaseOffset, s.maxBytes, false, truncatedSuffix, s.codec)
}

// Trimmed creates a new segment at baseOffset with trimmedSuffix, used when
// rewriting a segment to drop records before a given offset during TruncateBefore.
// The new segment has a different BaseOffset than the receiver.
func (s *segment) Trimmed(baseOffset int64) (*segment, error) {
	return newSegment(s.path, baseOffset, s.maxBytes, false, trimmedSuffix, s.codec)
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
	if err := s.initPositions(); err != nil {
		return err
	}
	return s.setupIndex()
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
			return nil, ErrEntryNotFound
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
func (s *segment) Delete() error {
	if err := s.Close(); err != nil {
		return err
	}
	s.Lock()
	defer s.Unlock()
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
	// cache holds the scan's block-decode buffers. Passing one cache to
	// every scanner of a multi-segment pass (clean, digest build,
	// consolidation) keeps the whole pass at one retained buffer pair;
	// letting scans hit the segments' own caches instead left each scanned
	// segment holding one for its lifetime (run 32: ~500MB-1GB transients).
	cache *blockCache
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
	return &segmentScanner{s: segment, cache: c}
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
	if _, err := s.s.scanReadAt(s.cache, header, s.pos); err != nil {
		return nil, nil, err
	}
	size := header.Size()
	payload := make([]byte, size)
	if _, err := s.s.scanReadAt(s.cache, payload, s.pos+msgSetHeaderLen); err != nil {
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
