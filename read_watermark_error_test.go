package commitlog

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// A committed read that cannot locate the high watermark says so.
//
// readLoop re-locates the watermark whenever it reaches the one it holds, and it
// wrote that lookup as `hwSeg, hwPos, err := getHWPos(...)`. All three names are
// new in that scope, so the `err` it tests is NOT the named return — the `break`
// below it left the loop with the outer err still nil, and Read returned
// (n, nil). Two of the three copies of this block did it.
//
// A caller cannot see the difference, because n is not what it reads: readMessage
// ignores the byte count and parses whatever is in headersBuf. The caller passes
// the SAME buffer every call, so a zero-byte read leaves the PREVIOUS record's
// header sitting there — a valid header, with a valid CRC, describing a payload
// that has already been served. readMessage duly asks for that payload, comes
// back to a watermark it now agrees with, and parks. So the symptom of a failed
// lookup was a follower hanging forever on a healthy log, with the reason (the
// segment holding the watermark was gone, or had just been replaced by
// compaction) discarded one frame earlier.
//
// The bounded context is what keeps this test honest about that: unfixed it can
// only end by deadline, and a deadline is precisely what a reader must not turn
// an error into.
//
// The watermark is pushed past the log here because that is the deterministic
// way to make getHWPos fail. The route that matters in production is the same
// lookup racing a compaction swap: findSegment resolves, Replace runs, findEntry
// answers ErrSegmentReplaced — the one error the reader knows how to retry, and
// the one this swallowed.
func TestACommittedReadReportsALostWatermarkRatherThanCorruption(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{Name: "hwerr", Path: dir, MaxSegmentBytes: 64 << 20})
	require.NoError(t, err)
	cl := l.(*commitLog)
	defer cl.Close() // nolint: errcheck

	for i := range 3 {
		_, err := cl.Append([]*Message{{Key: []byte("k"), Value: []byte{byte(i)}}})
		require.NoError(t, err)
	}
	// Committed as far as the first record, so the reader below is built with a
	// watermark it can locate and reaches the sync path only on its second read.
	cl.SetHighWatermark(0)

	r, err := cl.NewReader(From(0), Follow())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	buf := make([]byte, HeaderBufferLen)
	_, off, _, _, err := r.ReadMessage(ctx, buf)
	require.NoError(t, err)
	require.Equal(t, int64(0), off)

	// Past everything the log holds: getHWPos can no longer find a segment for
	// it. The reader is entitled to fail — it is not entitled to invent a record
	// or to wait.
	cl.SetHighWatermark(500)

	_, _, _, _, err = r.ReadMessage(ctx, buf)
	require.Error(t, err)
	require.NotErrorIs(t, err, context.DeadlineExceeded,
		"the reader parked instead of reporting the lookup that failed")
	require.ErrorIs(t, errors.Cause(err), ErrSegmentNotFound)
}
