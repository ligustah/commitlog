package commitlog

import (
	"github.com/pkg/errors"
)

const bufReadSize = 64 * 1024

// errBufReaderUnpositioned is returned when a bufReader is read before it has
// been given a segment. It is a programming error in this package, not a
// condition a caller can cause.
//
// It exists because the alternative — io.EOF — is this package's END OF DATA
// signal, and answering "you have read everything" to "I was never positioned"
// makes an unpositioned reader indistinguishable from a fully drained one. A
// scan would stop at zero records and report success. See index.ReadAt for the
// same choice on the mmap path, and the v0.42.0 entry for what this costs a
// consumer when it happens for real.
var errBufReaderUnpositioned = errors.New("bufReader read before being positioned on a segment")

// bufReader wraps a segment and provides buffered sequential reads.
// Instead of calling seg.ReadAt for every small read (28 bytes + payload),
// it fills a 64 KB buffer with one ReadAt call and serves subsequent reads
// from memory. This reduces syscalls from 2 per message to ~1 per 500 messages
// (for 100-byte records).
//
// It never ORIGINATES io.EOF — it only forwards the backing's. Whether the log
// ends is the segment's answer, and this type has no way to know it: an empty
// buffer means the last fill came up empty, which is a statement about the
// buffer. Three branches here used to return io.EOF on their own account, all
// for conditions that were not the end of anything.
type bufReader struct {
	seg      *segment
	data     []byte // buffered data
	pos      int64  // current byte offset in the segment
	bufStart int64  // byte offset in segment where data[0] maps to
}

func (b *bufReader) Read(p []byte) (int, error) {
	if b.seg == nil {
		return 0, errBufReaderUnpositioned
	}
	// If the requested data is entirely within our buffer, serve from memory.
	if b.pos >= b.bufStart && b.pos+int64(len(p)) <= b.bufStart+int64(len(b.data)) {
		offset := int(b.pos - b.bufStart)
		n := copy(p, b.data[offset:])
		b.pos += int64(n)
		// The guard above already established len(b.data)-offset >= len(p), so
		// copy filled p entirely and a short read here is impossible. Kept as an
		// assertion rather than dropped: if that guard is ever loosened, a
		// half-full BUFFER must not be reported as the end of the LOG, and a
		// silent short read would be taken by Read's callers as "more may follow"
		// and spun on. Either way the answer is an error, not io.EOF.
		if len(p) > n {
			return n, errors.Errorf(
				"bufReader served %d of %d bytes from a buffer the guard said held them", n, len(p))
		}
		return n, nil
	}

	// Buffer miss — refill.
	n, err := b.fill()
	if err != nil && n == 0 {
		return 0, err
	}
	// Now serve from the fresh buffer. fill sets bufStart to pos, so offset is 0
	// and this can only be the empty-buffer case: fill returned no bytes AND no
	// error. Every backing here documents os.File.ReadAt semantics, where a read
	// that reaches the end returns io.EOF — which the n == 0 check above has
	// already returned. So no bytes and no reason is a backing breaking its
	// contract, and it is not the same statement as "the log ends here".
	offset := int(b.pos - b.bufStart)
	if offset < 0 || offset >= len(b.data) {
		return 0, errors.Errorf(
			"segment read at %d returned no bytes and no error", b.pos)
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
