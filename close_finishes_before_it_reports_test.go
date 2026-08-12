package commitlog

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A Close whose high-watermark checkpoint fails must still close the segments
// and still give the directory back.
//
// The checkpoint used to be able to abort the rest of Close by returning early,
// which is wrong in two directions at once. The checkpoint is an optimization —
// checkpointHWLoop logs a failed tick and carries on, because RecoverTail rides
// out a stale one — while closing the segments is the part nothing else will do.
// And Close releases the directory lock unconditionally, on the documented
// grounds that the claim is given back only after the segments are shut. An
// early return made that false: the lock went back while every segment was
// still open with its index mapped, which is the two-writer state the lock
// exists to prevent.
//
// durable_streams closes and reopens one directory synchronously (a
// provisionally-opened stream reopened with real config, and a promote), so the
// release half is load-bearing for a real caller: a Close that kept the lock on
// a transient checkpoint failure would brick that path until the process
// restarted.
//
// The checkpoint is broken by breaking the real write rather than substituting
// for it — the checkpoint's destination is made a directory, so the atomic
// rename onto it cannot succeed on any platform.
func TestAFailedCheckpointStillClosesTheLogAndReleasesTheDirectory(t *testing.T) {
	// The Close path waits out its full retry budget before giving up, and the
	// condition sabotaged here never clears. Shorten it so the test does not
	// spend five seconds proving a rename fails.
	restore := waitedOnRetryBudget
	waitedOnRetryBudget = 100 * time.Millisecond
	t.Cleanup(func() { waitedOnRetryBudget = restore })

	dir := tempDir(t)
	l, err := New(Options{Path: dir, MaxSegmentBytes: 256})
	require.NoError(t, err)

	for i := 0; i < 20; i++ {
		offs, err := l.Append([]*Message{{Key: []byte{byte(i)}, Value: []byte("v")}})
		require.NoError(t, err)
		l.SetHighWatermark(offs[0])
	}

	cl := l.(*commitLog)
	require.Greater(t, len(cl.segments), 1, "the fixture needs more than one segment, "+
		"so a close that stops at the first failure is distinguishable from one that walks the set")

	// Break the checkpoint: a rename onto an existing directory fails.
	hw := filepath.Join(dir, hwFileName)
	require.NoError(t, os.RemoveAll(hw))
	require.NoError(t, os.Mkdir(hw, 0755))

	require.Error(t, l.Close(), "a Close that could not checkpoint must say so")

	// It reported the failure AND finished the work.
	for i, s := range cl.segments {
		s.RLock()
		closed := s.closed
		s.RUnlock()
		require.True(t, closed, "segment %d must be closed even though the checkpoint failed", i)
	}

	// Clear the sabotage — it is the checkpoint that was under test, not the
	// reopen — and check the directory was handed back.
	require.NoError(t, os.Remove(hw))

	reopened, err := New(Options{Path: dir, MaxSegmentBytes: 256})
	require.NoError(t, err, "a failed Close must still release the directory, or a "+
		"close-then-reopen caller is stuck until the process exits")
	defer reopened.Close()
}
