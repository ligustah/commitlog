package commitlog

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A reconcile that cannot READ the tail it is reconciling must fail the open.
//
// The tail is deliberately kept on disk when the read fails — dropping bytes on
// a transient IO error would turn a retryable open into permanent data loss, and
// that half was always right. What was wrong is that it also returned nil: the
// error path broke out of the loop with torn false, so the function fell through
// to `return nil` and the segment opened having reconciled NOTHING. lastOffset
// stayed at the stale index tail while position was the file size, so the next
// append took its offset from NextOffset and wrote a record at an offset the
// file already held. A replica reported holding exactly one record twice, and
// the open that produced it had said it was fine.
//
// The distinction the test has to hold is "kept the bytes" vs "claimed success".
// Only the second is the defect, and only an error tells them apart.
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

	// The state a crash leaves: the log physically ahead of what the index
	// covers, so reconcile has a tail to walk. Inflating position rather than
	// truncating the index file is deliberate — the index is mmapped, so a
	// truncation underneath it would not change what numEntries reports, and
	// the test would walk nothing and pass on an empty loop.
	seg.Lock()
	realPosition, realBacking := seg.position, seg.backing
	seg.position = realPosition + int64(msgSetHeaderLen) + 8
	// Embedded, so everything except the data read still works: the index lives
	// in its own file, and Close still reaches the real backing underneath.
	seg.backing = &readFailingBacking{realBacking}
	seg.Unlock()

	rerr := seg.reconcileIndexTailRaw()

	seg.Lock()
	seg.position, seg.backing = realPosition, realBacking
	seg.Unlock()

	require.ErrorIs(t, rerr, errInjectedBackingRead,
		"a reconcile whose read of the tail failed reported success, leaving "+
			"lastOffset at the stale index tail for the next append to collide with")
}
