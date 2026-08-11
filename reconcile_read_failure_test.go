package commitlog

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A reconcile that cannot READ the tail it is reconciling must fail the open
// AND leave the tail on disk. Both halves, because they are separate decisions
// and only one of them was wrong.
//
// Keeping the bytes was always right: a read that failed says nothing about
// what is on disk, so discarding the tail on a transient IO error would turn a
// retryable open into permanent data loss. What was wrong is that it also
// returned nil — the error path broke out of the loop with torn false, so the
// function fell through to `return nil` and the segment opened having
// reconciled NOTHING. lastOffset stayed at the stale index tail while position
// was the file size, so the next append took its offset from NextOffset and
// wrote a record at an offset the file already held. A replica reported holding
// exactly one record twice, and the open that produced it had said it was fine.
//
// Asserting only the error would accept an implementation that returned the
// error and discarded the tail — which trades a duplicate record for a lost
// one. durable_streams caught that gap in the first version of this test, and
// then caught it a second time in the version written to close it.
//
// Both misses came from HOW the read failure was injected. Wrapping the backing
// in a failing type defeats discardTornTail, which type-asserts
// s.backing.(*localBacking) and returns nil for anything else — so the discard
// under test could not run, and a patch that called it changed nothing the
// fixture could see. The failure is therefore injected by closing the segment's
// own file handle: reads fail for real, the backing stays a *localBacking, and
// os.Truncate works through the PATH, so the discard path stays live and a
// wrong implementation really does destroy the bytes.
func TestAReconcileThatCannotReadTheTailFails(t *testing.T) {
	opts := Options{
		Path:                 tempDir(t),
		MaxSegmentBytes:      1 << 20, // one segment
		HWCheckpointInterval: time.Hour,
		CleanerInterval:      time.Hour,
	}
	l, err := New(opts)
	require.NoError(t, err)
	cl := l.(*commitLog)
	for i := 0; i < 7; i++ {
		offs, aerr := cl.Append([]*Message{{Key: []byte("k"), Value: []byte(fmt.Sprintf("value-%d", i))}})
		require.NoError(t, aerr)
		cl.SetHighWatermark(offs[0])
	}
	require.NoError(t, cl.SyncAll())
	defer cl.Close() // nolint: errcheck

	seg := cl.activeSegment()
	logPath := filepath.Join(opts.Path, fmt.Sprintf(fileFormat, seg.BaseOffset, logFileSuffix))
	fi, err := os.Stat(logPath)
	require.NoError(t, err)
	sizeBefore := fi.Size()
	require.Positive(t, sizeBefore, "the fixture wrote no log bytes to preserve")

	// Empty the index so the walk starts at 0 with the whole log ahead of it —
	// the state rebuildIndexFromLog produces, and the one that gives the failing
	// read real bytes beyond it to either keep or discard.
	//
	// Not an inflated position: with position faked past the true end, startPos
	// already equals the file size, so a torn-tail discard truncates to where
	// the file already ends and changes nothing. The fixture could not tell the
	// two wrong implementations apart.
	seg.Lock()
	require.NoError(t, seg.Index.reset())
	seg.firstOffset, seg.lastOffset = -1, -1
	lb, ok := seg.backing.(*localBacking)
	require.True(t, ok, "the fixture needs the real local backing: closing its "+
		"handle is what makes the read fail while leaving the discard path live")
	require.NoError(t, lb.f.Close())
	seg.Unlock()

	// No floor: this test is about the read failing, not about what a torn tail
	// may drop, and -1 leaves the discard path exactly as it was.
	rerr := seg.reconcileIndexTailRaw(-1)

	// A working handle again, so the assertions below can read the log. The old
	// one is closed for good; reopening the path is what the retry this error is
	// supposed to provoke would do anyway.
	seg.Lock()
	fresh, oerr := openBackingWithRetry(logPath)
	require.NoError(t, oerr)
	seg.backing = fresh
	seg.Unlock()

	require.ErrorIs(t, rerr, os.ErrClosed,
		"a reconcile whose read of the tail failed reported success, leaving "+
			"lastOffset at the stale index tail for the next append to collide with")

	// The other half, and the one two earlier versions of this test could not
	// see: the bytes are still on disk. discardTornTail truncates through
	// os.Truncate(lb.f.Name(), keep), which does not care that the handle is
	// closed — so an implementation that discards the tail here really does
	// shorten the file, and this assertion really does fail.
	fi, err = os.Stat(logPath)
	require.NoError(t, err)
	require.Equal(t, sizeBefore, fi.Size(),
		"the failed reconcile truncated the log — a read that FAILED says "+
			"nothing about what is on disk, so discarding the tail turns a "+
			"transient IO error into permanent data loss")

	// And that the records are reachable, not merely present: a discard that
	// moved position without truncating leaves the file its original length.
	// A failed open has to be RETRYABLE, not just non-destructive.
	require.NoError(t, seg.rebuildIndexFromLog())
	require.EqualValues(t, 6, seg.NextOffset()-1,
		"the records did not survive the failed reconcile")
}
