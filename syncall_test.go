package commitlog

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestSyncAll writes across several segments (the periodic HW checkpoint only
// syncs the active one), calls SyncAll, and verifies (a) it succeeds with
// sealed segments present, (b) the high watermark checkpoint file reflects the
// tip, and (c) a reopened log recovers every record — the property an atomic
// stream promote relies on before renaming the log's directory.
func TestSyncAll(t *testing.T) {
	dir := t.TempDir()
	l, err := New(Options{
		Name:            "syncall",
		Path:            dir,
		MaxSegmentBytes: 256, // force several sealed segments
	})
	require.NoError(t, err)

	const n = 50
	var last int64
	for i := 0; i < n; i++ {
		offsets, err := l.Append([]*Message{{Value: []byte("payload-payload-payload")}})
		require.NoError(t, err)
		last = offsets[len(offsets)-1]
	}
	l.SetHighWatermark(last)

	cl := l.(*commitLog)
	cl.mu.RLock()
	segs := len(cl.segments)
	cl.mu.RUnlock()
	require.Greater(t, segs, 1, "test must span multiple segments")

	require.NoError(t, l.SyncAll())

	// The HW checkpoint file is durable and current.
	hwBytes, err := os.ReadFile(dir + "/" + hwFileName)
	require.NoError(t, err)
	require.NotEmpty(t, hwBytes)
	require.NoError(t, l.Close())

	// A reopened log recovers everything up to the checkpointed HW.
	l2, err := New(Options{Name: "syncall", Path: dir, MaxSegmentBytes: 256})
	require.NoError(t, err)
	defer l2.Close()
	require.EqualValues(t, last, l2.HighWatermark())
	require.EqualValues(t, last, l2.NewestOffset())
	r, err := l2.NewReader(0, false)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	headers := make([]byte, msgSetHeaderLen)
	count := 0
	for int64(count) <= last { // offsets are consecutive: last == n-1
		_, _, _, _, err := r.ReadMessage(ctx, headers)
		require.NoError(t, err, "at message %d of %d", count, n)
		count++
	}
	require.Equal(t, n, count)
}
