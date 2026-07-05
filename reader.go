package commitlog

import (
	"context"
	"errors"
	"io"
	"sync"

	pkgErrors "github.com/pkg/errors"
)

// ErrCommitLogReadonly is returned when the end of a readonly CommitLog has
// been reached.
var ErrCommitLogReadonly = errors.New("end of readonly log")

type contextReader interface {
	Read(context.Context, []byte) (int, error)
}

// Reader reads messages atomically from a CommitLog. Readers should not be
// used concurrently.
type Reader struct {
	ctxReader   contextReader
	offset      int64
	log         *commitLog
	uncommitted bool
}

// NewReader creates a new Reader starting at the given offset. If uncommitted
// is true, the Reader will read uncommitted messages from the log. Otherwise,
// it will only return committed messages.
func (l *commitLog) NewReader(offset int64, uncommitted bool) (*Reader, error) {
	var (
		ctxReader contextReader
		err       error
	)
	if uncommitted {
		ctxReader, err = l.newReaderUncommitted(offset)
	} else {
		ctxReader, err = l.newReaderCommitted(offset)
	}
	return &Reader{
		ctxReader:   ctxReader,
		offset:      offset,
		log:         l,
		uncommitted: uncommitted,
	}, err
}

// ReadMessage reads a single message from the underlying CommitLog or blocks
// until one is available. It returns the SerializedMessage in addition to its
// offset, timestamp, and leader epoch. This may return uncommitted messages if
// the reader was created with the uncommitted flag set to true.
//
// ReadMessage should not be called concurrently, and the headersBuf slice
// should have a capacity of at least 28.
//
// TODO: Should this just return a MessageSet directly instead of a Message and
// the MessageSet header values?
func (r *Reader) ReadMessage(ctx context.Context, headersBuf []byte) (SerializedMessage, int64, int64, uint64, error) {
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
			if r.uncommitted {
				r.ctxReader, err = r.log.newReaderUncommitted(r.offset)
			} else {
				r.ctxReader, err = r.log.newReaderCommitted(r.offset)
			}
			if err != nil {
				return nil, 0, 0, 0, pkgErrors.Wrap(err, "failed to reinitialize reader")
			}
			goto RETRY
		} else {
			return nil, 0, 0, 0, err
		}
	}
	r.offset = offset + 1
	return msg, offset, timestamp, leaderEpoch, err
}

type uncommittedReader struct {
	cl  *commitLog
	seg *segment
	mu  sync.Mutex
	pos int64
	br  bufReader
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
			if !r.waitForData(ctx, r.seg) {
				err = io.EOF
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
			if !r.waitForData(ctx, r.seg) {
				err = io.EOF
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

func (r *uncommittedReader) waitForData(ctx context.Context, seg *segment) bool {
	wait := seg.WaitForData(r, r.pos)
	select {
	case <-r.cl.closed:
		seg.removeWaiter(r)
		return false
	case <-ctx.Done():
		seg.removeWaiter(r)
		return false
	case <-wait:
		return true
	}
}

// newReaderUncommitted returns a contextReader which reads data from the log
// starting at the given offset.
func (l *commitLog) newReaderUncommitted(offset int64) (contextReader, error) {
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
		cl:  l,
		seg: seg,
		pos: position,
		br:  bufReader{seg: seg, pos: position, bufStart: position},
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
}

func (r *committedReader) Read(ctx context.Context, p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	segments := r.cl.Segments()

	if r.seg == nil {
		offset := r.hw + 1
		hw := r.cl.HighWatermark()
		for hw == r.hw {
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
		return io.EOF
	case <-ctx.Done():
		r.cl.removeHWWaiter(r)
		return io.EOF
	case readonly := <-wait:
		if readonly {
			return ErrCommitLogReadonly
		}
		return nil
	}
}

// newReaderCommitted returns a contextReader which reads only committed data
// from the log starting at the given offset.
func (l *commitLog) newReaderCommitted(offset int64) (contextReader, error) {
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
			cl:    l,
			seg:   nil,
			pos:   -1,
			hwSeg: hwSeg,
			hwPos: hwPos,
			hw:    hw,
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
		cl:    l,
		seg:   seg,
		pos:   position,
		hwSeg: hwSeg,
		hwPos: hwPos,
		hw:    hw,
		br:    bufReader{seg: seg, pos: position, bufStart: position},
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

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}
