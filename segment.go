package commitlog

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
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
	writer         io.Writer
	reader         io.Reader
	log            *os.File
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

	sync.RWMutex
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
	log, err := os.OpenFile(s.logPath(), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, errors.Wrap(err, "open file failed")
	}
	s.log = log
	if err := s.initPositions(); err != nil {
		return nil, err
	}
	s.writer = log
	s.reader = log
	err = s.setupIndex()
	return s, err
}

// initPositions inspects the (already-open) log file, detects whether it uses
// block compression, and initializes position/physPosition/blocks. A fresh
// (empty) segment uses the block format only when a codec is configured, so a
// None codec is byte-for-byte compatible with pre-compression logs. An existing
// segment is classified by its first byte: blockMagic means a compressed
// segment (scan its block headers), anything else is a legacy raw segment, which
// stays raw even if a codec is now configured so formats never mix in one file.
func (s *segment) initPositions() error {
	info, err := s.log.Stat()
	if err != nil {
		return errors.Wrap(err, "stat file failed")
	}
	size := info.Size()
	s.physPosition = size
	s.cache = newBlockCache()
	s.blocks = s.blocks[:0]
	if size == 0 {
		s.blockMode = s.codec != compress.None
		s.position = 0
		return nil
	}
	var magic [1]byte
	if _, err := s.log.ReadAt(magic[:], 0); err != nil {
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
		if _, err := s.log.ReadAt(hdr[:], phys); err != nil {
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
	if lastEntry != nil {
		s.lastOffset = lastEntry.Offset
		s.lastWriteTime = lastEntry.Timestamp
		// Read the first entry to get firstOffset and firstWriteTime.
		var firstEntry entry
		if err := s.Index.ReadEntryAtFileOffset(&firstEntry, 0); err != nil {
			return err
		}
		s.firstOffset = firstEntry.Offset
		s.firstWriteTime = firstEntry.Timestamp
	}
	return nil
}

// CheckSplit determines if a new log segment should be rolled out either
// because this segment is full or LogRollTime has passed since the first
// message was written to the segment.
func (s *segment) CheckSplit(logRollTime time.Duration) bool {
	s.RLock()
	defer s.RUnlock()
	if s.position >= s.maxBytes {
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
	s.Index.Shrink() // nolint: errcheck
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
	return s.Index.CountEntries()
}

func (s *segment) WriteMessageSet(ms []byte, entries []*entry) error {
	s.Lock()
	defer s.Unlock()
	if _, err := s.write(ms, entries); err != nil {
		return err
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
		n, err = s.writer.Write(p)
		if err != nil {
			return n, errors.Wrap(err, "log write failed")
		}
		s.position += int64(n)
	}
	if s.firstWriteTime == 0 {
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

// appendBlock compresses p into a self-describing block and appends it. If the
// compressed form isn't smaller than the input the block is stored raw (codec
// None) so we never inflate incompressible data. position advances by the
// logical (uncompressed) length; physPosition advances by the physical length.
func (s *segment) appendBlock(p []byte) error {
	codec := s.codec
	payload := codec.Compress(p)
	if len(payload) >= len(p) {
		codec = compress.None
		payload = p
	}
	hdr := encodeBlockHeader(codec, uint32(len(p)), uint32(len(payload)))
	buf := make([]byte, 0, len(hdr)+len(payload))
	buf = append(buf, hdr...)
	buf = append(buf, payload...)
	n, err := s.writer.Write(buf)
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

// ReadAt reads len(p) bytes from the segment's logical byte space starting at
// off. For a raw segment this is a direct file read; for a block-compressed
// segment it maps the logical range onto the decompressed block(s).
func (s *segment) ReadAt(p []byte, off int64) (n int, err error) {
	s.RLock()
	defer s.RUnlock()
	if s.closed {
		if s.replaced {
			return 0, ErrSegmentReplaced
		}
		return 0, ErrSegmentClosed
	}
	if !s.blockMode {
		return s.log.ReadAt(p, off)
	}
	return s.readBlocks(p, off)
}

// readBlocks serves a read from the logical byte space of a block-compressed
// segment, decompressing and copying across as many blocks as the request
// spans. It mirrors os.File.ReadAt semantics: a short read returns io.EOF.
func (s *segment) readBlocks(p []byte, off int64) (int, error) {
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
		data, err := s.blockData(*blk)
		if err != nil {
			return n, err
		}
		n += copy(p[n:], data[cur-blk.logicalStart:])
	}
	return n, nil
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

// blockData returns the decompressed bytes of a block, serving the most-recently
// decompressed block from the cache to avoid re-inflating on sequential reads.
func (s *segment) blockData(b blockRef) ([]byte, error) {
	if cached := s.cache.get(b.physStart); cached != nil {
		return cached, nil
	}
	raw := make([]byte, b.payloadLen())
	if _, err := s.log.ReadAt(raw, b.payloadStart()); err != nil {
		return nil, errors.Wrap(err, "read block payload failed")
	}
	data, err := b.codec.Decompress(raw)
	if err != nil {
		return nil, errors.Wrap(err, "decompress block failed")
	}
	if int64(len(data)) != b.logicalLen {
		return nil, fmt.Errorf("commitlog: block decompressed to %d bytes, want %d", len(data), b.logicalLen)
	}
	s.cache.put(b.physStart, data)
	return data, nil
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
	if err := s.log.Close(); err != nil {
		return err
	}
	if err := s.Index.Close(); err != nil {
		return err
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
	log, err := os.OpenFile(s.logPath(), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return errors.Wrap(err, "reopen trimmed segment failed")
	}
	s.log = log
	s.writer = log
	s.reader = log
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
	log, err := os.OpenFile(s.logPath(), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return errors.Wrap(err, "open file failed")
	}
	s.log = log
	s.writer = log
	s.reader = log
	s.closed = false
	old.replaced = true
	if err := s.initPositions(); err != nil {
		return err
	}
	return s.setupIndex()
}

// findEntry returns the first entry whose offset is greater than or equal to
// the given offset.
func (s *segment) findEntry(offset int64) (*entry, error) {
	s.RLock()
	defer s.RUnlock()
	var (
		entry = &entry{}
		n     = int(s.Index.Position() / entryWidth)
		err   error
	)
	idx := sort.Search(n, func(i int) bool {
		if e := s.Index.ReadEntryAtFileOffset(entry, int64(i*entryWidth)); e != nil {
			err = e
			return true
		}
		return entry.Offset >= offset
	})
	if err != nil {
		return nil, err
	}
	if idx == n {
		return nil, ErrEntryNotFound
	}
	err = s.Index.ReadEntryAtFileOffset(entry, int64(idx*entryWidth))
	return entry, err
}

// findEntryByTimestamp returns the first entry whose timestamp is greater than
// or equal to the given timestamp.
func (s *segment) findEntryByTimestamp(timestamp int64) (*entry, error) {
	s.RLock()
	defer s.RUnlock()
	var (
		entry = &entry{}
		n     = int(s.Index.CountEntries())
		err   error
	)
	idx := sort.Search(n, func(i int) bool {
		if e := s.Index.ReadEntryAtLogOffset(entry, int64(i)); e != nil {
			err = e
			return true
		}
		return entry.Timestamp >= timestamp
	})
	if err != nil {
		return nil, err
	}
	if idx == n {
		return nil, ErrEntryNotFound
	}
	err = s.Index.ReadEntryAtLogOffset(entry, int64(idx))
	return entry, err
}

// Delete closes the segment and then deletes its log and index files.
func (s *segment) Delete() error {
	if err := s.Close(); err != nil {
		return err
	}
	s.Lock()
	defer s.Unlock()
	if exists(s.log.Name()) {
		if err := os.Remove(s.log.Name()); err != nil {
			return err
		}
	}
	if exists(s.Index.Name()) {
		if err := os.Remove(s.Index.Name()); err != nil {
			return err
		}
	}
	return nil
}

type segmentScanner struct {
	s  *segment
	is *indexScanner
}

func newSegmentScanner(segment *segment) *segmentScanner {
	return &segmentScanner{s: segment, is: newIndexScanner(segment.Index)}
}

// Scan should be called repeatedly to iterate over the messages in the
// segment, it will return io.EOF when there are no more messages.
func (s *segmentScanner) Scan() (messageSet, *entry, error) {
	entry, err := s.is.Scan()
	if err != nil {
		return nil, nil, err
	}
	header := make(messageSet, msgSetHeaderLen)
	_, err = s.s.ReadAt(header, entry.Position)
	if err != nil {
		return nil, nil, err
	}
	payload := make([]byte, header.Size())
	_, err = s.s.ReadAt(payload, entry.Position+msgSetHeaderLen)
	if err != nil {
		return nil, nil, err
	}
	msgSet := append(header, payload...)
	return msgSet, entry, nil
}

func (s *segment) logPath() string {
	return filepath.Join(s.path, fmt.Sprintf(fileFormat, s.BaseOffset, logSuffix+s.suffix))
}

func (s *segment) indexPath() string {
	return filepath.Join(s.path, fmt.Sprintf(fileFormat, s.BaseOffset, indexSuffix+s.suffix))
}
