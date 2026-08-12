package commitlog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// A header buffer LARGER than the header is read as a header, not as a header
// plus the front of the payload.
//
// ReadMessage documents headersBuf as needing "a capacity of at least
// HeaderBufferLen", and every consumer reads that as permission to pass a bigger
// one — a reused scratch buffer, a rounded-up size. But the read filled the
// whole slice: the underlying readers loop until they have len(p) bytes, so a
// 64-byte buffer consumed 32 bytes of header and 32 bytes of the record's
// payload, and the next frame began mid-payload.
//
// What that produces is not an obvious failure. The stream is off by however
// many bytes the buffer was oversized, so the next header fails its CRC and the
// read reports ErrCorruptRecord — a healthy log, called corrupt, by a caller who
// did exactly what the doc said.
//
// The mirror of this was already reported from the field: durable_streams had 24
// sites passing 28 bytes, and every one panicked. That direction was loud and got
// a named error. This one is silent, and the doc invites it.
func TestAnOversizedHeaderBufferReadsOnlyTheHeader(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{Name: "hdrbuf", Path: dir, MaxSegmentBytes: 64 << 20})
	require.NoError(t, err)
	cl := l.(*commitLog)
	defer cl.Close() // nolint: errcheck

	const records = 4
	for range records {
		off, err := cl.Append([]*Message{{
			Key:   []byte("k"),
			Value: []byte("a value long enough to be worth overrunning into"),
		}})
		require.NoError(t, err)
		cl.SetHighWatermark(off[0])
	}

	ctx := context.Background()

	// "At least HeaderBufferLen", taken at its word.
	big := make([]byte, HeaderBufferLen*2)
	r, err := cl.NewReader(From(0))
	require.NoError(t, err)
	for i := range records {
		_, off, _, _, err := r.ReadMessage(ctx, big)
		require.NoErrorf(t, err, "record %d with an oversized header buffer", i)
		require.Equal(t, int64(i), off)
	}

	// The metadata path takes the same buffer and must agree.
	bigMeta := make([]byte, HeaderBufferLen*2)
	rm, err := cl.NewReader(From(0))
	require.NoError(t, err)
	var payload []byte
	for i := range records {
		meta, buf, err := rm.ReadMessageMetadata(ctx, bigMeta, payload)
		require.NoErrorf(t, err, "metadata record %d with an oversized header buffer", i)
		require.Equal(t, int64(i), meta.Offset)
		payload = buf
	}
}
