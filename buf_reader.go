package commitlog

import (
	"io"
)

const bufReadSize = 64 * 1024

// bufReader wraps a segment and provides buffered sequential reads.
// Instead of calling seg.ReadAt for every small read (28 bytes + payload),
// it fills a 64 KB buffer with one ReadAt call and serves subsequent reads
// from memory. This reduces syscalls from 2 per message to ~1 per 500 messages
// (for 100-byte records).
type bufReader struct {
	seg      *segment
	data     []byte // buffered data
	pos      int64  // current byte offset in the segment
	bufStart int64  // byte offset in segment where data[0] maps to
}

func (b *bufReader) Read(p []byte) (int, error) {
	if b.seg == nil {
		return 0, io.EOF
	}
	// If the requested data is entirely within our buffer, serve from memory.
	if b.pos >= b.bufStart && b.pos+int64(len(p)) <= b.bufStart+int64(len(b.data)) {
		offset := int(b.pos - b.bufStart)
		n := copy(p, b.data[offset:])
		b.pos += int64(n)
		if len(p) > n {
			return n, io.EOF
		}
		return n, nil
	}

	// Buffer miss — refill.
	n, err := b.fill()
	if err != nil && n == 0 {
		return 0, err
	}
	// Now serve from the fresh buffer.
	offset := int(b.pos - b.bufStart)
	if offset < 0 || offset >= len(b.data) {
		return 0, io.EOF
	}
	read := copy(p, b.data[offset:])
	b.pos += int64(read)
	if read < len(p) {
		// We hit end of buffer and there's no more data (err from fill).
		return read, err
	}
	return read, nil
}

func (b *bufReader) fill() (int, error) {
	buf := b.data
	if cap(buf) < bufReadSize {
		buf = make([]byte, bufReadSize)
	}
	buf = buf[:bufReadSize]
	n, err := b.seg.ReadAt(buf, b.pos)
	b.data = buf[:n]
	b.bufStart = b.pos
	return n, err
}

func (b *bufReader) reset(seg *segment, pos int64) {
	b.seg = seg
	b.pos = pos
	b.bufStart = pos
	b.data = b.data[:0] // force refill on next Read
}
