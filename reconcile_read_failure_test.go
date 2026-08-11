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
// one. durable_streams caught that gap in the first version of this test.
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
	realBacking := seg.backing
	// Embedded, so everything except the data read still works: the index lives
	// in its own file, and Close still reaches the real backing underneath.
	seg.backing = &readFailingBacking{realBacking}
	seg.Unlock()

	rerr := seg.reconcileIndexTailRaw()

	seg.Lock()
	seg.backing = realBacking
	seg.Unlock()

	require.ErrorIs(t, rerr, errInjectedBackingRead,
		"a reconcile whose read of the tail failed reported success, leaving "+
			"lastOffset at the stale index tail for the next append to collide with")

	// The other half: the tail is still there to retry over. Asserted by
	// REBUILDING from the log rather than by comparing the file size, because a
	// byte count cannot fail here — discardTornTail type-asserts *localBacking,
	// and behind the injected wrapper it returns without truncating anything.
	// An implementation that discarded the tail would still leave the file its
	// original length, and a size assertion would sail through while the
	// records were unreachable.
	//
	// Rebuilding catches both the file truncation and the bookkeeping one, and
	// it is the property that actually matters: a failed open has to be
	// RETRYABLE, not merely non-destructive.
	require.NoError(t, seg.rebuildIndexFromLog())
	require.EqualValues(t, 6, seg.NextOffset()-1,
		"the records did not survive the failed reconcile — a read that FAILED "+
			"says nothing about what is on disk, so discarding the tail turns a "+
			"transient IO error into permanent data loss")

	fi, err = os.Stat(logPath)
	require.NoError(t, err)
	require.Equal(t, sizeBefore, fi.Size(), "the log file changed length")
}
