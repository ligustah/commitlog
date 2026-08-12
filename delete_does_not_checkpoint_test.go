package commitlog

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Delete must not write a high-watermark checkpoint, and a broken checkpoint
// destination must not be able to stop it.
//
// A checkpoint records where to resume a log that still exists. Delete is
// removing the log, so the write is wasted at best. Delete already said as much
// about the background loop -- it sets l.deleted before signalling close
// specifically so the checkpoint loop skips a directory about to be removed --
// and then called closeSegments, which wrote that same checkpoint synchronously
// on the way out.
//
// The cost of that only appeared once the checkpoint stopped aborting the close
// (see TestAFailedCheckpointStillClosesTheLogAndReleasesTheDirectory): the
// error then reached Delete, whose early return skips BOTH the lock release and
// the removal. The result was the worst of every branch -- segments closed, the
// directory locked for the life of the process, and every file still on disk --
// from a best-effort write nobody wanted. sqlcdc reported it against real
// failed deletes.
//
// The checkpoint is broken the same way as in the Close test, by breaking the
// real write instead of substituting for it: its destination is made a
// directory, so the atomic rename onto it cannot succeed on any platform.
func TestDeleteDoesNotCheckpointAndCannotBeStoppedByOne(t *testing.T) {
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

	hw := filepath.Join(dir, hwFileName)
	require.NoError(t, os.RemoveAll(hw))
	require.NoError(t, os.Mkdir(hw, 0755))

	require.NoError(t, l.Delete(),
		"a checkpoint Delete has no reason to write must not be able to fail it")

	_, err = os.Stat(dir)
	require.True(t, os.IsNotExist(err),
		"Delete must remove the directory; an early return on the checkpoint error "+
			"would have left it locked, undeleted, and unopenable for the process lifetime")
}
