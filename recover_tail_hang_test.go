package commitlog

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The recovery reader must return io.EOF the moment it drains the readable
// bytes rather than parking for future appends. RecoverTail relies on this so
// its forward scan can never hang: a normal uncommitted reader would block here
// forever (it tails a live writer), which is exactly how RecoverTail used to
// spin when the reconstructed LEO overshot the log actually on disk.
func TestRecoveryReaderReturnsEOFInsteadOfBlocking(t *testing.T) {
	dir := t.TempDir()
	l, err := New(Options{Path: dir, MaxSegmentBytes: 256})
	require.NoError(t, err)
	cl := l.(*commitLog)
	defer cl.Close()

	for i := 0; i < 5; i++ {
		_, err := cl.Append([]*Message{{Value: []byte("v")}})
		require.NoError(t, err)
	}

	r, err := cl.newRecoveryReader(0)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		headers := make([]byte, msgSetHeaderLen)
		var last error
		for {
			_, _, _, _, e := r.ReadMessage(context.Background(), headers)
			if e != nil {
				last = e
				break
			}
		}
		done <- last
	}()

	select {
	case e := <-done:
		require.ErrorIs(t, e, io.EOF, "recovery reader should drain to io.EOF")
	case <-time.After(10 * time.Second):
		t.Fatal("recovery reader blocked at end of data instead of returning io.EOF")
	}
}

// RecoverTail must terminate even when the reconstructed LEO (NewestOffset,
// derived from the index) overshoots the log actually on disk — an
// index-ahead-of-log inconsistency. It truncate-heals the un-backed phantom
// suffix rather than parking forever on the uncommitted reader.
func TestRecoverTailTerminatesOnIndexAheadOfLog(t *testing.T) {
	dir := t.TempDir()
	l, err := New(Options{
		Path:                 dir,
		MaxSegmentBytes:      1 << 20,
		HWCheckpointInterval: time.Hour,
		CleanerInterval:      time.Hour,
	})
	require.NoError(t, err)
	cl := l.(*commitLog)

	app := func(v string) int64 {
		o, err := cl.Append([]*Message{{Key: []byte("k"), Value: []byte(v)}})
		require.NoError(t, err)
		cl.SetHighWatermark(o[0])
		return o[0]
	}

	// A synced prefix: fsync fence at a block boundary.
	for i := 0; i < 6; i++ {
		app(fmt.Sprintf("v%d", i))
	}
	require.NoError(t, cl.SyncAll())
	fenceNewest := cl.NewestOffset()

	logs, err := filepath.Glob(filepath.Join(dir, "*"+logFileSuffix))
	require.NoError(t, err)
	sort.Strings(logs)
	active := logs[len(logs)-1]
	fi, err := os.Stat(active)
	require.NoError(t, err)
	fenceSize := fi.Size()

	// Extra records past the fence, then a clean Close persists the index + log
	// (and the checkpoint) up to the extras.
	for i := 0; i < 4; i++ {
		app(fmt.Sprintf("x%d", i))
	}
	require.NoError(t, cl.Close())

	// Roll the checkpoint back to the fence and shrink the log to the fence
	// block boundary — the index still references the extras, so the
	// reconstructed LEO now overshoots the bytes on disk.
	require.NoError(t, os.WriteFile(filepath.Join(dir, hwFileName),
		[]byte(strconv.FormatInt(fenceNewest, 10)), 0666))
	require.NoError(t, os.Truncate(active, fenceSize))

	l2, err := New(Options{
		Path:                 dir,
		MaxSegmentBytes:      1 << 20,
		HWCheckpointInterval: time.Hour,
		CleanerInterval:      time.Hour,
	})
	require.NoError(t, err)
	defer l2.Close()
	cl2 := l2.(*commitLog)

	// RecoverTail must return promptly, not hang.
	done := make(chan error, 1)
	go func() { done <- cl2.RecoverTail() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("RecoverTail hung on an index-ahead-of-log tail")
	}

	// The phantom suffix was dropped: the tail sits at the fence and the log is
	// readable end to end.
	require.LessOrEqual(t, cl2.NewestOffset(), fenceNewest,
		"phantom suffix above the readable log was not truncated")
	require.EqualValues(t, cl2.NewestOffset(), cl2.HighWatermark(),
		"HW should equal the recovered tail")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := cl2.newRecoveryReader(cl2.OldestOffset())
	require.NoError(t, err)
	headers := make([]byte, msgSetHeaderLen)
	for {
		_, _, _, _, e := r.ReadMessage(ctx, headers)
		if e != nil {
			require.ErrorIs(t, e, io.EOF, "recovered log must read cleanly to EOF")
			break
		}
	}
}
