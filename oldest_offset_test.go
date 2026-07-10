package commitlog

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// OldestOffset must be correct on a live, appending handle — not only after a
// fresh open. Exercises the lifecycles that rebuild or replace segment 0.
func TestOldestOffsetLiveHandle(t *testing.T) {
	opts := Options{Path: tempDir(t), MaxSegmentBytes: 256}
	l, cleanup := setupWithOptions(t, opts)
	defer l.Close()
	defer cleanup()

	// Empty log.
	require.Equal(t, int64(-1), l.OldestOffset(), "empty log")

	// Fresh appends on the same handle.
	for i := 0; i < 20; i++ {
		_, err := l.Append([]*Message{{Value: []byte("v" + strconv.Itoa(i))}})
		require.NoError(t, err)
	}
	l.SetHighWatermark(19)
	require.Equal(t, int64(0), l.OldestOffset(), "after appends")
	require.Equal(t, int64(19), l.NewestOffset())

	// Tail truncation (the durable_streams open pattern: Truncate(HW+1)).
	require.NoError(t, l.Truncate(15))
	require.Equal(t, int64(0), l.OldestOffset(), "after tail truncate")

	// Keep appending on the same handle after the truncate.
	for i := 15; i < 30; i++ {
		_, err := l.Append([]*Message{{Value: []byte("v" + strconv.Itoa(i))}})
		require.NoError(t, err)
	}
	require.Equal(t, int64(0), l.OldestOffset(), "after post-truncate appends")

	// Head truncation (retention floor): oldest must advance.
	require.NoError(t, l.TruncateBefore(10))
	oldLive := l.OldestOffset()
	require.NoError(t, l.Close())

	// A fresh open is the ground truth; the live handle must have agreed.
	l2, err := New(opts)
	require.NoError(t, err)
	defer l2.Close()
	require.Equal(t, l2.OldestOffset(), oldLive,
		"live handle after TruncateBefore disagrees with fresh open")
	require.Equal(t, int64(10), l2.OldestOffset(), "fresh open after TruncateBefore")
}

// Same comparison for a truncate-at-open sequence on an empty-then-written log
// (exactly what durable_streams does on every OpenStream).
func TestOldestOffsetTruncateOnOpen(t *testing.T) {
	opts := Options{Path: tempDir(t)}
	l, cleanup := setupWithOptions(t, opts)
	defer l.Close()
	defer cleanup()

	require.NoError(t, l.Truncate(l.HighWatermark()+1)) // Truncate(0) on empty
	_, err := l.Append([]*Message{{Value: []byte("a")}, {Value: []byte("b")}})
	require.NoError(t, err)
	l.SetHighWatermark(1)
	require.Equal(t, int64(0), l.OldestOffset(), "live handle after truncate-at-open")

	require.NoError(t, l.Close())
	l2, err := New(opts)
	require.NoError(t, err)
	defer l2.Close()
	require.Equal(t, int64(0), l2.OldestOffset(), "fresh open")
}
