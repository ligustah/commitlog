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
// The fixture is the compaction swap itself, driven rather than raced. Every
// segment is marked as left behind by a pass in flight, which is the state
// current() reports with ok=false and findSegment skips — so the lookup finds no
// segment at all and getHWPos fails, deterministically and by the route that
// matters in production.
//
// It used to push the watermark PAST the log instead, which was the easier way
// to make findSegment come back empty. That stopped being a failure: a watermark
// above the tail is a state a correct follower reaches, told what the leader
// committed before the records arrive, and the reader now clamps to what it
// holds rather than failing every read on the log. See clampHW. The fixture went
// with the behaviour it depended on; the contract this test is actually about
// — a failed lookup must be REPORTED, not swallowed into (n, nil) — did not
// change, so it is asserted here on the harder fixture.
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

	// Every segment goes out from under the lookup, exactly as a compaction pass
	// leaves them until it publishes. current() answers ok=false for each, so
	// findSegment skips them all and hands back nothing.
	for _, seg := range cl.segmentsSnapshot() {
		seg.Lock()
		seg.left = true
		seg.Unlock()
	}
	// And the watermark advances, so the reader re-locates rather than parking on
	// the one it already holds. It is entitled to fail here — it is not entitled
	// to invent a record or to wait.
	cl.SetHighWatermark(2)

	_, _, _, _, err = r.ReadMessage(ctx, buf)
	require.Error(t, err)
	require.NotErrorIs(t, err, context.DeadlineExceeded,
		"the reader parked instead of reporting the lookup that failed")
	require.ErrorIs(t, errors.Cause(err), ErrSegmentNotFound)
}
