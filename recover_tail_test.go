package commitlog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// A reopened log must recover the committed records above the (≤5s stale) HW
// checkpoint instead of amputating them: served records were being
// retroactively unwritten after kill -9 (found by the sqlcdc soak's Follow
// mirror). A genuinely torn suffix is still truncated.
func TestRecoverTailExtendsPastStaleCheckpoint(t *testing.T) {
	dir := t.TempDir()
	l, err := New(Options{Path: dir, MaxSegmentBytes: 256})
	require.NoError(t, err)
	var last int64
	for i := 0; i < 10; i++ {
		offs, err := l.Append([]*Message{{Key: []byte{byte(i)}, Value: []byte("v")}})
		require.NoError(t, err)
		last = offs[0]
	}
	// Simulate the stale checkpoint: a graceful Close persists the real HW,
	// so rewrite the checkpoint file to 4 afterwards — the on-disk state a
	// kill -9 leaves when the last checkpoint tick was ≤5s stale.
	l.SetHighWatermark(last)
	require.NoError(t, l.Close())
	require.NoError(t, os.WriteFile(filepath.Join(dir, "replication-offset-checkpoint"), []byte("4"), 0666))

	l2, err := New(Options{Path: dir, MaxSegmentBytes: 256})
	require.NoError(t, err)
	defer l2.Close()
	require.EqualValues(t, 4, l2.HighWatermark(), "reopen sees the stale checkpoint")
	require.NoError(t, l2.(*commitLog).RecoverTail())
	require.EqualValues(t, last, l2.HighWatermark(), "RecoverTail must extend to the real tail")
	require.EqualValues(t, last, l2.NewestOffset())
}

// A torn suffix (power loss mid-write) is dropped; everything before it is
// recovered.
func TestRecoverTailTruncatesTornSuffix(t *testing.T) {
	dir := t.TempDir()
	l, err := New(Options{Path: dir}) // single segment
	require.NoError(t, err)
	for i := 0; i < 6; i++ {
		_, err := l.Append([]*Message{{Key: []byte{byte(i)}, Value: []byte("v")}})
		require.NoError(t, err)
	}
	l.SetHighWatermark(2)
	require.NoError(t, l.(*commitLog).checkpointHW())
	require.NoError(t, l.Close())

	// Tear the tail: append garbage bytes to the segment file.
	logs, err := filepath.Glob(filepath.Join(dir, "*.log"))
	require.NoError(t, err)
	f, err := os.OpenFile(logs[len(logs)-1], os.O_APPEND|os.O_WRONLY, 0666)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xDE, 0xAD, 0xBE})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	l2, err := New(Options{Path: dir})
	require.NoError(t, err)
	defer l2.Close()
	require.NoError(t, l2.(*commitLog).RecoverTail())
	// All six real records recovered; the garbage did not extend the tail.
	require.EqualValues(t, 5, l2.HighWatermark())
	r, err := l2.NewReader(0, true)
	require.NoError(t, err)
	headers := make([]byte, 28)
	for i := 0; i <= 5; i++ {
		_, off, _, _, err := r.ReadMessage(t.Context(), headers)
		require.NoError(t, err)
		require.EqualValues(t, i, off)
	}
}
