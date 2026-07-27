package commitlog

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// The contract that matters: what ReadMessageSet returns is exactly what
// AppendMessageSet accepts. A follower replicates bytes rather than
// reconstructing the framing, which is the whole reason for this to exist —
// hand-rolled framing breaks silently the moment the format moves.
func TestReadMessageSetRoundTripsIntoAnotherLog(t *testing.T) {
	src, cleanupSrc := setupWithOptions(t, Options{
		Path: tempDir(t), MaxSegmentBytes: 1 << 20,
	})
	defer cleanupSrc()

	want := map[int64]string{}
	for i := 0; i < 12; i++ {
		v := fmt.Sprintf("v%d", i)
		offs, err := src.Append([]*Message{{Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte(v)}})
		require.NoError(t, err)
		want[offs[0]] = v
	}
	src.SetHighWatermark(src.NewestOffset())

	dst, cleanupDst := setupWithOptions(t, Options{
		Path: tempDir(t), MaxSegmentBytes: 1 << 20,
	})
	defer cleanupDst()

	// Replicate in bounded chunks, as a follower would.
	for next := int64(0); next <= src.NewestOffset(); {
		ms, err := src.ReadMessageSet(next, 64)
		require.NoError(t, err)
		require.NotEmpty(t, ms, "a follower must always make progress")

		offs, err := dst.AppendMessageSet(ms)
		require.NoError(t, err, "the source's framing must be appendable verbatim")
		require.NotEmpty(t, offs)
		next = offs[len(offs)-1] + 1
	}

	dst.SetHighWatermark(dst.NewestOffset())
	require.Equal(t, src.NewestOffset(), dst.NewestOffset(),
		"the replica must reach the same tail")

	got := readFrom(t, dst)
	for off, v := range want {
		require.Equal(t, v, got[off], "record at %d must survive replication", off)
	}
}

// Records above the high watermark must be included. Replication is how the
// watermark advances, so withholding them would deadlock it.
func TestReadMessageSetIncludesRecordsAboveTheHighWatermark(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path: tempDir(t), MaxSegmentBytes: 1 << 20,
	})
	defer cleanup()

	for i := 0; i < 6; i++ {
		_, err := l.Append([]*Message{{Value: []byte(fmt.Sprintf("v%d", i))}})
		require.NoError(t, err)
	}
	// Deliberately leave the watermark behind the tail.
	l.SetHighWatermark(1)

	ms, err := l.ReadMessageSet(0, 1<<20)
	require.NoError(t, err)

	count := 0
	for off := 0; off < len(ms); {
		size := int(messageSet(ms[off:]).Size())
		off += msgSetHeaderLen + size
		count++
	}
	require.Equal(t, 6, count,
		"all six records must be returned, not just those at or below the watermark")
}

// A maxBytes smaller than the first frame still yields that frame. Returning a
// truncated message set would give a follower bytes it cannot append, and
// returning nothing would stall it forever.
func TestReadMessageSetNeverReturnsAPartialFrame(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path: tempDir(t), MaxSegmentBytes: 1 << 20,
	})
	defer cleanup()

	_, err := l.Append([]*Message{{Value: []byte("a value long enough to exceed the budget")}})
	require.NoError(t, err)
	l.SetHighWatermark(l.NewestOffset())

	ms, err := l.ReadMessageSet(0, 1)
	require.NoError(t, err)
	require.NotEmpty(t, ms, "a follower must make progress even on a tiny budget")

	size := int(messageSet(ms).Size())
	require.Equal(t, msgSetHeaderLen+size, len(ms),
		"exactly one whole frame, not a truncation")

	// And it is appendable, which is the point.
	dst, cleanupDst := setupWithOptions(t, Options{
		Path: tempDir(t), MaxSegmentBytes: 1 << 20,
	})
	defer cleanupDst()
	_, err = dst.AppendMessageSet(ms)
	require.NoError(t, err)
}

// An offset below the oldest surviving record clamps up to it, so a follower
// resuming from a position retention has passed carries on rather than failing.
func TestReadMessageSetClampsBelowOldest(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path: tempDir(t), MaxSegmentBytes: 64,
	})
	defer cleanup()

	for i := 0; i < 20; i++ {
		_, err := l.Append([]*Message{{Value: []byte("padding value")}})
		require.NoError(t, err)
	}
	l.SetHighWatermark(l.NewestOffset())
	require.NoError(t, l.TruncateBefore(10))
	require.Greater(t, l.OldestOffset(), int64(0), "retention must have moved the floor")

	ms, err := l.ReadMessageSet(0, 1<<20)
	require.NoError(t, err, "resuming below the floor must not fail")
	require.NotEmpty(t, ms)
}

// An offset past the end has no segment to read from, which is a distinct
// answer from "this range held nothing".
func TestReadMessageSetPastTheEnd(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path: tempDir(t), MaxSegmentBytes: 1 << 20,
	})
	defer cleanup()

	_, err := l.Append([]*Message{{Value: []byte("v")}})
	require.NoError(t, err)
	l.SetHighWatermark(l.NewestOffset())

	_, err = l.ReadMessageSet(l.NewestOffset()+100, 1<<20)
	require.ErrorIs(t, err, ErrSegmentNotFound)

	_, err = l.ReadMessageSet(0, 0)
	require.Error(t, err, "a non-positive budget is a caller error")
}
