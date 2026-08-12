package commitlog

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A Delete that fails must still give the directory back.
//
// The tempting rule is the opposite one, and it is what this code did first:
// hold the claim when the delete fails, on the grounds that the files are still
// open and nobody else should walk into a half-deleted directory. That argument
// assumes a retry, and the retry assumes the caller still has the log.
//
// durable_streams drops it — a failed delete forgets the log so the name stays
// openable — which is where the argument collapses. Once the last reference is
// gone nothing can call Delete again, so the lock is held until the process
// exits: the directory can no longer be deleted (no handle to delete it with)
// and no longer be opened (a fresh New is refused with ErrLogLocked). One
// transient sharing violation on one segment would brick the name for the life
// of the process. They spotted this against the released design, before it was.
//
// Nothing is given up by releasing. The lock exists to keep a SECOND WRITER
// out, and after Delete this log is not one: l.deleted is set, the background
// loops are joined, appends are refused. Whatever handles leaked from the
// failed close have no writer behind them.
//
// The refusal is built with refusingBacking, the same fixture
// TestCloseWalksEverySegmentEvenWhenOneRefuses uses: it replaces one segment's
// backing and nothing else, so the code under test — Delete's ordering — runs
// for real.
func TestAFailedDeleteStillReleasesTheDirectory(t *testing.T) {
	dir := tempDir(t)
	l, cleanup := setupWithOptions(t, Options{
		Path:                 dir,
		MaxSegmentBytes:      64,
		DisableAutoClean:     true,
		HWCheckpointInterval: time.Hour,
		CleanerInterval:      time.Hour,
	})
	defer cleanup()

	for i := 0; i < 24; i++ {
		_, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("padding value")}})
		require.NoError(t, err)
	}
	require.Greater(t, len(l.segments), 1, "the fixture needs a segment that closes normally too")

	boom := errors.New("this segment will not close")
	victim := l.segments[0]
	victim.Lock()
	real := victim.backing
	victim.backing = refusingBacking{err: boom}
	victim.Unlock()
	// The refusal leaks the victim's own handle by construction. Release it so
	// the reopen below is measuring the directory claim and not this deliberate
	// leak — on Windows a live handle is its own reason for an open to fail,
	// and the two must not be confusable.
	defer real.Close() // nolint: errcheck

	require.ErrorIs(t, l.Delete(), boom, "the failure must still be reported")

	// The claim is what this test is about: the log the caller just threw away
	// must not have taken the directory's name with it.
	reopened, err := New(Options{Path: dir, MaxSegmentBytes: 64})
	require.NotErrorIs(t, err, ErrLogLocked,
		"a failed Delete kept the directory lock; the caller has already dropped the "+
			"log, so nothing can ever release it or retry the delete")
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
}
