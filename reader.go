package commitlog

import (
	"context"
	"errors"
	"hash/crc32"
	"io"
	"sync"

	pkgErrors "github.com/pkg/errors"
)

// ErrCommitLogReadonly is returned when the end of a readonly CommitLog has
// been reached.
var ErrCommitLogReadonly = errors.New("end of readonly log")

type contextReader interface {
	Read(context.Context, []byte) (int, error)
	// segmentBounds reports the offset range [base, next) of the segment this
	// reader is currently positioned in, and whether it has one yet.
	//
	// It is read AFTER a record, never before. A frame never straddles a
	// segment boundary, so once a record has been read the reader is positioned
	// in the segment that record came from — and `next` reflects any append that
	// landed meanwhile. Capturing before the read gets both of those wrong: it
	// would attribute a record to the previous segment when the read crossed a
	// boundary, and it would reject a legitimately-new record at the live tail
	// whose offset had not been assigned when the bounds were taken.
	segmentBounds() (base, next int64, ok bool)
}

// Reader reads messages atomically from a CommitLog. Readers should not be
// used concurrently.
type Reader struct {
	ctxReader   contextReader
	offset      int64
	log         *commitLog
	uncommitted bool
	noWait      bool // never block for future appends
	spec        readSpec
	// prefix serves a KeyPrefix read, skipping past non-matching records over
	// sealed segments instead of reading them. Nil for an unfiltered read, and
	// exhausted (then nil) once the read reaches the live tail.
	prefix *prefixSource
}

// NewReader opens a Reader over the log. With no options it reads every
// committed record from the oldest surviving one and returns io.EOF at the end
// of the data; see From, Until, Follow, Uncommitted, KeyPrefix, SkipSuperseded
// and IncludeControl.
//
// Two defaults are the opposite of the constructors this replaces, on purpose.
// It TERMINATES rather than follows, because a reader that unexpectedly ends is
// noticed by its caller while one that unexpectedly follows blocks forever. And
// it reads COMMITTED data only, which was previously an unlabelled bool at the
// call site.
//
// One combination is refused: KeyPrefix with Uncommitted, and neither Until nor
// IncludeControl. Reading past the commit boundary yields records whose
// transactions are undecided, and the markers that say which committed are
// keyless — the filter drops them. The caller would hold records it cannot
// classify, silently. Bound the read at your commit boundary with Until, or take
// the markers with IncludeControl.
func (l *commitLog) NewReader(opts ...ReadOption) (*Reader, error) {
	spec, err := l.resolve(opts)
	if err != nil {
		return nil, err
	}
	r := &Reader{
		offset:      spec.offset,
		log:         l,
		uncommitted: spec.uncommitted,
		noWait:      !spec.follow,
		spec:        spec,
	}
	if spec.prefixSet {
		r.prefix = newPrefixSource(l, spec)
	}
	r.ctxReader, err = l.newSourceReader(spec)
	return r, err
}

// newSourceReader builds the underlying sequential reader for a spec.
func (l *commitLog) newSourceReader(spec readSpec) (contextReader, error) {
	if spec.uncommitted {
		return l.newReaderUncommitted(spec.offset, !spec.follow)
	}
	return l.newReaderCommitted(spec.offset, !spec.follow)
}

// newRecoveryReader returns an uncommitted reader that does NOT block waiting
// for future appends: it returns io.EOF as soon as it drains the readable
// bytes. RecoverTail scans a static tail (not a live writer), so blocking for
// appends that will never come is never correct there — this guarantees the
// scan always terminates even if the reconstructed LEO overshoots the log on
// disk.
func (l *commitLog) newRecoveryReader(offset int64) (*Reader, error) {
	return l.NewReader(From(offset), Uncommitted())
}

// ReadMessage reads a single message from the underlying CommitLog or blocks
// until one is available. It returns the SerializedMessage in addition to its
// offset, timestamp, and leader epoch. This may return uncommitted messages if
// the reader was created with the uncommitted flag set to true.
//
// ReadMessage should not be called concurrently, and the headersBuf slice
// must have a capacity of at least HeaderBufferLen.
//
// TODO: Should this just return a MessageSet directly instead of a Message and
// the MessageSet header values?
func (r *Reader) ReadMessage(ctx context.Context, headersBuf []byte) (SerializedMessage, int64, int64, uint64, error) {
	for {
		msg, offset, timestamp, leaderEpoch, err := r.readOne(ctx, headersBuf)
		if err != nil {
			return nil, 0, 0, 0, err
		}
		if r.spec.untilSet && offset > r.spec.until {
			// Past the caller's bound: end as if the data had run out, so a
			// bounded read terminates identically whether the bound or the log
			// end came first.
			r.offset = offset
			return nil, 0, 0, 0, io.EOF
		}
		// The sealed portion is already filtered by the digest; this catches
		// the tail, which has no digest to filter with.
		if !r.spec.matchesPrefix(msg) {
			continue
		}
		return msg, offset, timestamp, leaderEpoch, nil
	}
}

// readOne returns the next record from whichever source is live: the planned,
// digest-driven one while sealed segments remain, then the sequential reader.
func (r *Reader) readOne(
	ctx context.Context, headersBuf []byte) (SerializedMessage, int64, int64, uint64, error) {

	if r.prefix != nil {
		rec, ok, err := r.prefix.pop()
		if err != nil {
			return nil, 0, 0, 0, err
		}
		if ok {
			r.offset = rec.offset + 1
			return rec.msg, rec.offset, rec.ts, rec.epoch, nil
		}
		// Sealed segments exhausted: hand over to the sequential reader,
		// positioned where planning stopped. From here the read is at the live
		// tail, where records arrive one at a time and there is nothing to
		// plan from anyway.
		if r.prefix.next > r.offset {
			r.offset = r.prefix.next
		}
		r.prefix = nil
		cr, rerr := r.log.newSourceReader(r.specAt(r.offset))
		if rerr != nil {
			return nil, 0, 0, 0, pkgErrors.Wrap(rerr, "failed to reposition reader at the tail")
		}
		r.ctxReader = cr
	}
RETRY:
	msg, offset, timestamp, leaderEpoch, err := readMessage(ctx, r.ctxReader, headersBuf)
	if err != nil {
		if r.log.IsDeleted() {
			// The log was deleted while we were trying to read.
			return nil, 0, 0, 0, ErrCommitLogDeleted
		} else if r.log.IsClosed() {
			// The log was closed while we were trying to read.
			return nil, 0, 0, 0, ErrCommitLogClosed
		} else if pkgErrors.Cause(err) == ErrCommitLogReadonly && r.log.IsReadonly() {
			// The log was set to readonly while we were trying to read.
			return nil, 0, 0, 0, ErrCommitLogReadonly
		} else if pkgErrors.Cause(err) == ErrSegmentReplaced {
			// ErrSegmentReplaced indicates we attempted to read from a log
			// segment that was replaced due to compaction, so reinitialize the
			// contextReader and try again to read from the new segment.
			if r.ctxReader, err = r.log.newSourceReader(r.specAt(r.offset)); err != nil {
				return nil, 0, 0, 0, pkgErrors.Wrap(err, "failed to reinitialize reader")
			}
			goto RETRY
		} else {
			return nil, 0, 0, 0, err
		}
	}
	// The frame header is not covered by any checksum — the CRC spans the
	// message payload only — so a damaged offset field is served as truth unless
	// something cross-checks it. A record must carry an offset belonging to the
	// segment it was found in; anything else is a fabricated identity, and a
	// caller that resumes from a reported offset resumes from nowhere.
	//
	// Bounds are taken AFTER the read, deliberately: see segmentBounds.
	//
	// This cannot replace the CRC and does not try to. It catches an offset
	// outside the segment's range, not one swapped with another record inside it
	// — the header has no checksum to make that detectable, and adding one would
	// change the format.
	if base, next, ok := r.ctxReader.segmentBounds(); ok && (offset < base || offset >= next) {
		return nil, 0, 0, 0, pkgErrors.Wrapf(ErrCorruptRecord,
			"record claims offset %d, outside its segment's range [%d, %d)",
			offset, base, next)
	}
	r.offset = offset + 1
	return msg, offset, timestamp, leaderEpoch, err
}

// specAt is this reader's spec repositioned to offset, for rebuilding the
// underlying reader without losing follow/committed semantics.
func (r *Reader) specAt(offset int64) readSpec {
	s := r.spec
	s.offset = offset
	return s
}

// ReadMessageMetadata reads a single message and returns its metadata — offset,
// attributes, and headers. headersBuf must have a capacity of at least
// HeaderBufferLen. The payloadBuf slice is reused across calls; callers should
// pass the returned slice back on the next call to avoid per-message
// allocations.
//
// This is intended for metadata-only scans such as LSO rebuild where only the
// Attributes byte and message headers (producer ID, epoch, sequence) are needed.
//
// TWO THINGS IT DOES NOT DO, both of which ReadMessage does:
//
//   - It does NOT CRC-validate the payload. A record corrupted on disk is
//     returned here as data, where ReadMessage refuses it. Reading a value
//     through this path means reading it unverified.
//   - It does NOT give you memory you own. Raw — and Key(), Value() and the
//     Headers values taken from it — point INTO payloadBuf, so the next call
//     overwrites them in place. Copy anything you keep past that call.
//
// The second one is quiet when it bites: a shorter following record overwrites
// only the HEAD of a retained value and leaves its tail alone, so what you hold
// is still the right length and still parses, with another record's bytes at the
// front. Nothing errors. Decode as you go, or copy.
func (r *Reader) ReadMessageMetadata(ctx context.Context, headersBuf []byte, payloadBuf []byte) (MessageMetadata, []byte, error) {
RETRY:
	meta, newBuf, err := readMessageMetadata(ctx, r.ctxReader, headersBuf, payloadBuf)
	if err != nil {
		if r.log.IsDeleted() {
			return MessageMetadata{}, newBuf, ErrCommitLogDeleted
		} else if r.log.IsClosed() {
			return MessageMetadata{}, newBuf, ErrCommitLogClosed
		} else if pkgErrors.Cause(err) == ErrCommitLogReadonly && r.log.IsReadonly() {
			return MessageMetadata{}, newBuf, ErrCommitLogReadonly
		} else if pkgErrors.Cause(err) == ErrSegmentReplaced {
			if r.ctxReader, err = r.log.newSourceReader(r.specAt(r.offset)); err != nil {
				return MessageMetadata{}, newBuf, pkgErrors.Wrap(err, "failed to reinitialize reader")
			}
			payloadBuf = newBuf
			goto RETRY
		} else {
			return MessageMetadata{}, newBuf, err
		}
	}
	r.offset = meta.Offset + 1
	return meta, newBuf, nil
}

type uncommittedReader struct {
	cl  *commitLog
	seg *segment
	mu  sync.Mutex
	pos int64
	br  bufReader
	// noWait makes the reader return io.EOF the moment it drains the readable
	// bytes instead of parking for future appends. RecoverTail uses it to scan
	// a static tail so recovery can never hang if the reconstructed LEO
	// overshoots the log actually on disk (an index-ahead-of-log inconsistency).
	noWait bool
}

func (r *uncommittedReader) Read(ctx context.Context, p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var (
		segments = r.cl.Segments()
		readSize int
		waiting  bool
	)

	// Initialise buffered reader on first use.
	if r.br.seg == nil {
		r.br.reset(r.seg, r.pos)
	}

LOOP:
	for {
		readSize, err = r.br.Read(p[n:])
		n += readSize
		r.pos = r.br.pos
		if err != nil && err != io.EOF {
			break
		}
		if n == len(p) {
			break
		}
		if readSize != 0 && err == nil {
			waiting = false
			continue
		}

		// We hit the end of the segment.
		if err == io.EOF && !waiting {
			// Check if there are more segments.
			nextSeg := findSegmentByBaseOffset(segments, r.seg.BaseOffset+1)
			if nextSeg != nil {
				r.seg = nextSeg
				r.br.reset(nextSeg, 0)
				continue
			}
			// Otherwise, wait for segment to be written to (or split).
			waiting = true
			if werr := r.waitForData(ctx, r.seg); werr != nil {
				err = werr
				break
			}
			// The segment may have more data now. Refill buffer.
			r.br.reset(r.seg, r.pos)
			continue
		}

		// We hit an EOF after waiting for data which means a new segment was
		// rolled, so move to the next segment.
		segments = r.cl.Segments()
		nextSeg := findSegmentByBaseOffset(segments, r.seg.BaseOffset+1)

		// If there are not enough segments to read, wait for new segment to be
		// appended or the context to be canceled.
		for nextSeg == nil {
			if werr := r.waitForData(ctx, r.seg); werr != nil {
				err = werr
				break LOOP
			}
			segments = r.cl.Segments()
			nextSeg = findSegmentByBaseOffset(segments, r.seg.BaseOffset+1)
		}
		r.seg = nextSeg
		r.br.reset(nextSeg, 0)
		r.pos = 0
		waiting = false
	}

	return n, err
}

// waitForData parks until the segment has more bytes. It returns nil when there
// may be more to read, and otherwise the reason there is not — io.EOF for a
// genuine end, or the context's error when the CALLER gave up.
//
// Those two were one answer (a bool) and the caller turned false into io.EOF, so
// a cancellation arrived at the caller as end-of-data. See
// committedReader.waitForHW for why that is the wrong thing to tell someone
// tailing a log.
func (r *uncommittedReader) waitForData(ctx context.Context, seg *segment) error {
	if r.noWait {
		// Recovery scan: the readable bytes are drained and no more are coming.
		// Return end-of-data instead of parking for appends that never arrive.
		return io.EOF
	}
	wait := seg.WaitForData(r, r.pos)
	select {
	case <-r.cl.closed:
		seg.removeWaiter(r)
		// io.EOF here is NOT end-of-data — records past r.pos may well exist. It
		// survives only because Reader.ReadMessage turns any error into
		// ErrCommitLogClosed when IsClosed(), and IsClosed() reads this same
		// channel, so the conversion cannot lose the race. A second consumer of
		// contextReader that skipped that conversion would inherit a reader that
		// reports a closed log as a fully drained one.
		return io.EOF
	case <-ctx.Done():
		seg.removeWaiter(r)
		return ctx.Err()
	case <-wait:
		return nil
	}
}

// newReaderUncommitted returns a contextReader which reads data from the log
// starting at the given offset.
func (l *commitLog) newReaderUncommitted(offset int64, noWait bool) (contextReader, error) {
	seg, contains := findSegmentContains(l.Segments(), offset)
	if seg == nil {
		return nil, ErrSegmentNotFound
	}
	position := int64(0)
	if contains {
		e, err := seg.findEntry(offset)
		if err != nil {
			return nil, err
		}
		position = e.Position
	}
	return &uncommittedReader{
		cl:     l,
		seg:    seg,
		pos:    position,
		br:     bufReader{seg: seg, pos: position, bufStart: position},
		noWait: noWait,
	}, nil
}

type committedReader struct {
	cl    *commitLog
	seg   *segment
	hwSeg *segment
	mu    sync.Mutex
	pos   int64
	hwPos int64
	hw    int64
	br    bufReader
	// noWait ends the read at the high watermark instead of parking for it to
	// advance. This is what a non-Follow committed reader needs: "committed
	// data ran out" is an end condition for a bounded pass, not something to
	// wait through.
	noWait bool
}

func (r *committedReader) Read(ctx context.Context, p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	segments := r.cl.Segments()

	if r.seg == nil {
		offset := r.hw + 1
		hw := r.cl.HighWatermark()
		for hw == r.hw {
			if r.noWait {
				return 0, io.EOF
			}
			err = r.waitForHW(ctx, hw)
			if err != nil {
				return
			}
			hw = r.cl.HighWatermark()
		}
		r.hw = hw
		segments = r.cl.Segments()
		hwIdx, hwPos, err := getHWPos(segments, r.hw)
		if err != nil {
			return 0, err
		}
		r.hwSeg = segments[hwIdx]
		r.hwPos = hwPos
		r.seg, _ = findSegment(segments, offset)
		if r.seg == nil {
			return 0, ErrSegmentNotFound
		}
		entry, err := r.seg.findEntry(offset)
		if err != nil {
			return 0, err
		}
		r.pos = entry.Position
		r.br.reset(r.seg, r.pos)
	}

	return r.readLoop(ctx, p, segments)
}

func (r *committedReader) readLoop(
	ctx context.Context, p []byte, segments []*segment) (n int, err error) {

	var readSize int
LOOP:
	for {
		lim := int64(len(p[n:]))
		if r.seg == r.hwSeg {
			lim = min(lim, r.hwPos-r.pos)
		}
		if lim <= 0 {
			// HW boundary reached — sync.
			hw := r.cl.HighWatermark()
			for hw == r.hw {
				if r.noWait {
					err = io.EOF
					break LOOP
				}
				err = r.waitForHW(ctx, hw)
				if err != nil {
					break LOOP
				}
				hw = r.cl.HighWatermark()
			}
			r.hw = hw
			segments = r.cl.Segments()
			hwIdx, hwPos, err := getHWPos(segments, r.hw)
			if err != nil {
				break
			}
			r.hwPos = hwPos
			r.hwSeg = segments[hwIdx]
			continue
		}

		// For small reads within our buffered window, use br. For reads
		// that cross the HW boundary or are larger than the buffer, fall
		// through to direct ReadAt.
		if r.br.seg != nil && lim <= int64(bufReadSize) {
			buf := p[n:int(int64(n)+lim)]
			readSize, err = r.br.Read(buf)
			n += readSize
			r.pos = r.br.pos
		} else {
			readSize, err = r.seg.ReadAt(p[n:int(int64(n)+lim)], r.pos)
			n += readSize
			r.pos += int64(readSize)
			// Keep the buffered reader in sync: the next small read must
			// continue after the bytes consumed here, not from the buffer's
			// stale pre-ReadAt position.
			r.br.pos = r.pos
		}
		if err != nil && err != io.EOF {
			break
		}
		if n == len(p) {
			break
		}
		if readSize != 0 && err == nil {
			continue
		}

		// We hit the end of the segment, so jump to the next one.
		if err == io.EOF {
			nextSeg := findSegmentByBaseOffset(segments, r.seg.BaseOffset+1)
			if nextSeg == nil {
				err = errors.New("no segment to consume")
				break
			}
			r.seg = nextSeg
			r.pos = 0
			r.br.reset(nextSeg, 0)
			continue
		}

		// We hit the HW, so sync the latest.
		hw := r.cl.HighWatermark()
		for hw == r.hw {
			err = r.waitForHW(ctx, hw)
			if err != nil {
				break LOOP
			}
			hw = r.cl.HighWatermark()
		}
		r.hw = hw
		segments = r.cl.Segments()
		hwIdx, hwPos, err := getHWPos(segments, r.hw)
		if err != nil {
			break
		}
		r.hwPos = hwPos
		r.hwSeg = segments[hwIdx]
	}

	return n, err
}

func (r *committedReader) waitForHW(ctx context.Context, hw int64) error {
	wait := r.cl.waitForHW(r, hw)
	select {
	case <-r.cl.closed:
		r.cl.removeHWWaiter(r)
		// See uncommittedReader.waitForData: end-of-data is the wrong statement
		// here too, and the same upstream conversion is what makes it safe.
		return io.EOF
	case <-ctx.Done():
		r.cl.removeHWWaiter(r)
		// The CALLER's context, not the log's state. Returning io.EOF here made a
		// cancellation indistinguishable from "the data ran out" — and io.EOF is
		// this package's documented end-of-read signal, so a caller reading with a
		// per-read deadline would treat a timeout as end-of-stream and stop
		// consuming, silently, at the tail. Whether more data is coming is not
		// something a cancelled context says anything about.
		return ctx.Err()
	case readonly := <-wait:
		if readonly {
			return ErrCommitLogReadonly
		}
		return nil
	}
}

// newReaderCommitted returns a contextReader which reads only committed data
// from the log starting at the given offset. With noWait it ends at the high
// watermark instead of parking for it to advance.
func (l *commitLog) newReaderCommitted(offset int64, noWait bool) (contextReader, error) {
	var (
		hw       = l.HighWatermark()
		hwPos    = int64(-1)
		segments = l.Segments()
		hwSeg    *segment
	)

	// If offset exceeds HW, wait for the next message. This also covers the
	// case when the log is empty.
	if offset > hw || l.OldestOffset() == -1 {
		return &committedReader{
			cl:     l,
			seg:    nil,
			pos:    -1,
			hwSeg:  hwSeg,
			hwPos:  hwPos,
			hw:     hw,
			noWait: noWait,
		}, nil
	}

	if hw != -1 {
		hwIdx, hwPosition, err := getHWPos(segments, hw)
		if err != nil {
			return nil, err
		}
		hwPos = hwPosition
		hwSeg = segments[hwIdx]
	}

	position := int64(0)
	seg, contains := findSegmentContains(segments, offset)
	if contains {
		entry, err := seg.findEntry(offset)
		if err != nil {
			return nil, err
		}
		position = entry.Position
	}
	return &committedReader{
		cl:     l,
		seg:    seg,
		pos:    position,
		hwSeg:  hwSeg,
		hwPos:  hwPos,
		hw:     hw,
		br:     bufReader{seg: seg, pos: position, bufStart: position},
		noWait: noWait,
	}, nil
}

func getHWPos(segments []*segment, hw int64) (int, int64, error) {
	hwSeg, hwIdx := findSegment(segments, hw)
	if hwSeg == nil {
		return 0, 0, ErrSegmentNotFound
	}
	hwEntry, err := hwSeg.findEntry(hw)
	if err != nil {
		return 0, 0, err
	}
	return hwIdx, hwEntry.Position + int64(hwEntry.Size), nil
}

// maxPayloadChunk bounds how far a frame's declared size is TRUSTED before any
// of it has been read.
//
// The size field is not covered by any checksum — the CRC lives inside the
// payload it describes — so a torn or damaged frame can declare any length up to
// 4GiB, and allocating that up front hands an out-of-memory kill to whoever
// embedded this log. Found by FuzzTornLogServesOnlyAPrefix, which truncates a
// segment and leaves the reader parsing a size out of whatever bytes follow: the
// fuzzing worker died with "terminated unexpectedly" rather than any assertion.
//
// So the payload is read in chunks and the buffer grows only as bytes actually
// ARRIVE. A frame claiming 4GiB of a file that holds a hundred bytes now costs
// one chunk and an error, instead of the process.
const maxPayloadChunk = 1 << 20

// readPayload fills size bytes from reader, growing into reuse when it can. It
// never allocates more than one chunk beyond what has already been read, so a
// bogus size costs a chunk rather than the address space.
func readPayload(ctx context.Context, reader contextReader, size int, reuse []byte) ([]byte, error) {
	buf := reuse[:0]
	for len(buf) < size {
		want := size - len(buf)
		if want > maxPayloadChunk {
			want = maxPayloadChunk
		}
		have := len(buf)
		if cap(buf) < have+want {
			grown := make([]byte, have, have+want)
			copy(grown, buf)
			buf = grown
		}
		buf = buf[:have+want]
		if _, err := reader.Read(ctx, buf[have:]); err != nil {
			return reuse[:0], err
		}
	}
	return buf, nil
}

// readMessage reads a single message from the reader or blocks until one is
// available. It returns the Message in addition to its offset, timestamp, and
// leader epoch. This may return uncommitted messages if the reader was created
// with the uncommitted flag set to true.
func readMessage(ctx context.Context, reader contextReader, headersBuf []byte) (SerializedMessage, int64, int64, uint64, error) {
	// A buffer too small to hold a frame header cannot be read into, and must not
	// be indexed past. Reported by durable_streams: 24 of their call sites still
	// allocated 28 bytes, and on v0.41.0 every one PANICKED inside
	// storedHeaderCrc — an index past the end of the caller's slice.
	//
	// That is a caller mistake and still not something to crash their process
	// over, which is the whole point of ErrCorruptRecord replacing a panic
	// earlier. It also cannot be left to the Read below: Read fills whatever it
	// is given, so a short buffer silently consumes a partial header and
	// desynchronises the stream, and the failure then surfaces somewhere else
	// entirely.
	//
	// The error names HeaderBufferLen rather than a number, so the fix is
	// copy-pasteable and stays correct if the header changes again.
	if len(headersBuf) < msgSetHeaderLen {
		return nil, 0, 0, 0, pkgErrors.Errorf(
			"commitlog: headersBuf is %d bytes, need at least HeaderBufferLen (%d)",
			len(headersBuf), msgSetHeaderLen)
	}
	if _, err := reader.Read(ctx, headersBuf); err != nil {
		return nil, 0, 0, 0, pkgErrors.Wrap(err, "failed to read message headers")
	}
	// Verify the header before trusting anything in it. offset, timestamp, leader
	// epoch and size are a record's IDENTITY, and until the header carried its own
	// checksum a damaged one was reported as fact — see headerCrcPos. The payload
	// CRC below cannot help: it covers the value, and a swapped offset leaves the
	// value perfectly intact.
	//
	// Checked before `size` is used, because size is one of the fields being
	// verified: trusting it first is how a corrupt length becomes a bad
	// allocation.
	if want, got := storedHeaderCrc(headersBuf), headerCrc(headersBuf); want != got {
		return nil, 0, 0, 0, pkgErrors.Wrapf(ErrCorruptRecord,
			"frame header failed CRC: expected 0x%08x, got 0x%08x", want, got)
	}
	var (
		offset      = int64(encoding.Uint64(headersBuf[offsetPos:]))
		timestamp   = int64(encoding.Uint64(headersBuf[timestampPos:]))
		leaderEpoch = encoding.Uint64(headersBuf[leaderEpochPos:])
		size        = encoding.Uint32(headersBuf[sizePos:])
	)
	buf, err := readPayload(ctx, reader, int(size), nil)
	if err != nil {
		return nil, 0, 0, 0, pkgErrors.Wrap(err, "failed to ready message payload")
	}
	m := SerializedMessage(buf)
	// The frame's size field is not covered by any checksum — the CRC lives
	// INSIDE the payload it describes — so a torn or damaged frame can claim a
	// length too short to hold that checksum. Reading it would index past the
	// end: Crc() takes m[0:4], and on a size of 0 that panicked out of the
	// caller's process, which is exactly what a log embedded in someone else's
	// binary must not do.
	//
	// Anything this short cannot be a record. encode never emits fewer than a
	// checksum, a magic byte, an attribute byte and two length prefixes, so
	// refusing here rejects only frames that were already impossible, and every
	// longer malformation is left for the CRC below to catch.
	//
	// NOW SHADOWED, and kept as defence in depth. Since the frame header carries
	// its own checksum, a damaged `size` fails that check before this line is
	// reached, so the only way here is a header that verifies while declaring a
	// size encode cannot produce — which nothing writes. guardcheck used to cover
	// this via the torn-log target and reported it uncovered the moment the header
	// CRC landed; it is not untested by oversight, it is unreachable. See the roll
	// CAS in split() for the same situation and the same reasoning.
	if len(m) < 4 {
		return nil, 0, 0, 0, pkgErrors.Wrapf(ErrCorruptRecord,
			"record at offset %d: frame claims %d bytes, too short to hold a checksum", offset, len(m))
	}
	// Check the CRC on the message. Returned, not panicked: see ErrCorruptRecord
	// for why a library embedded in someone else's process must not take it down
	// over a record the caller could have skipped.
	crc := m.Crc()
	if c := crc32.Checksum(m[4:], crc32cTable); crc != c {
		return nil, 0, 0, 0, pkgErrors.Wrapf(ErrCorruptRecord,
			"record at offset %d: expected CRC 0x%08x, got 0x%08x", offset, crc, c)
	}
	return m, offset, timestamp, leaderEpoch, nil
}

// readMessageMetadata reads a message from the log, parses headers and
// attributes, and returns them without CRC-validating the payload. The
// payloadBuf slice is reused across calls to avoid per-message allocations.
// Callers should pass the returned slice back on the next call.
//
// This is intended for metadata-only scans (LSO rebuild, offset tracking)
// where the value bytes are not needed and full deserialization is wasteful.
func readMessageMetadata(ctx context.Context, reader contextReader, hdrBuf []byte, payloadBuf []byte) (MessageMetadata, []byte, error) {
	// See readMessage: a short header buffer is a caller mistake, not a panic.
	if len(hdrBuf) < msgSetHeaderLen {
		return MessageMetadata{}, payloadBuf, pkgErrors.Errorf(
			"commitlog: headersBuf is %d bytes, need at least HeaderBufferLen (%d)",
			len(hdrBuf), msgSetHeaderLen)
	}
	if _, err := reader.Read(ctx, hdrBuf); err != nil {
		return MessageMetadata{}, payloadBuf, pkgErrors.Wrap(err, "failed to read message headers")
	}
	// Verify the header before trusting anything in it. offset, timestamp, leader
	// epoch and size are a record's IDENTITY, and until the header carried its own
	// checksum a damaged one was reported as fact — see headerCrcPos. The payload
	// CRC below cannot help: it covers the value, and a swapped offset leaves the
	// value perfectly intact.
	//
	// Checked before `size` is used, because size is one of the fields being
	// verified: trusting it first is how a corrupt length becomes a bad
	// allocation.
	if want, got := storedHeaderCrc(hdrBuf), headerCrc(hdrBuf); want != got {
		return MessageMetadata{}, payloadBuf, pkgErrors.Wrapf(ErrCorruptRecord,
			"frame header failed CRC: expected 0x%08x, got 0x%08x", want, got)
	}
	var (
		offset      = int64(encoding.Uint64(hdrBuf[offsetPos:]))
		timestamp   = int64(encoding.Uint64(hdrBuf[timestampPos:]))
		leaderEpoch = encoding.Uint64(hdrBuf[leaderEpochPos:])
		size        = encoding.Uint32(hdrBuf[sizePos:])
	)
	// Chunked for the same reason as readMessage: an unchecksummed size field
	// must not turn a torn frame into a 4GiB allocation. See maxPayloadChunk.
	buf, err := readPayload(ctx, reader, int(size), payloadBuf)
	if err != nil {
		return MessageMetadata{}, payloadBuf, pkgErrors.Wrap(err, "failed to read message payload")
	}
	payloadBuf = buf
	return MessageMetadata{
		Offset:      offset,
		Timestamp:   timestamp,
		LeaderEpoch: leaderEpoch,
		Attributes:  int8(buf[5]),
		Headers:     parseHeadersAfterValue(buf),
		Raw:         SerializedMessage(buf),
	}, payloadBuf, nil
}

// segmentBounds implements contextReader.
func (r *uncommittedReader) segmentBounds() (int64, int64, bool) {
	r.mu.Lock()
	seg := r.seg
	r.mu.Unlock()
	if seg == nil {
		return 0, 0, false
	}
	return seg.BaseOffset, seg.NextOffset(), true
}

// segmentBounds implements contextReader.
func (r *committedReader) segmentBounds() (int64, int64, bool) {
	r.mu.Lock()
	seg := r.seg
	r.mu.Unlock()
	if seg == nil {
		return 0, 0, false
	}
	return seg.BaseOffset, seg.NextOffset(), true
}
