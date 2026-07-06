package commitlog

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestClose_JoinsBackgroundLoopsBeforeReopen is a regression test for the
// background-goroutine timing issue: Close signaled the checkpoint/cleaner loops
// to stop but did not wait for them to exit before closing segments, so a loop
// already past its select kept operating on segment files after Close returned.
// On Windows that held file handles/mmaps and made reopening the same path fail;
// under -race it surfaced as a data race on segment state.
//
// The loop below closes and immediately reopens the same path with aggressive
// cleaner/checkpoint intervals. It must succeed every iteration.
func TestClose_JoinsBackgroundLoopsBeforeReopen(t *testing.T) {
	path := tempDir(t)

	for i := 0; i < 40; i++ {
		l, err := New(Options{
			Path:                 path,
			MaxSegmentBytes:      64,
			MaxLogBytes:          256,
			CleanerInterval:      time.Millisecond,
			HWCheckpointInterval: time.Millisecond,
		})
		require.NoError(t, err, "reopen iteration %d must not fail (lingering goroutine held the path?)", i)

		batch := make([]*Message, 20)
		for j := range batch {
			batch[j] = &Message{Value: []byte(strconv.Itoa(i*100 + j))}
		}
		offsets, err := l.Append(batch)
		require.NoError(t, err)
		l.SetHighWatermark(offsets[len(offsets)-1])

		// Give the cleaner/checkpoint loops a chance to be mid-iteration.
		time.Sleep(2 * time.Millisecond)

		require.NoError(t, l.Close())
	}
}

// TestClose_Idempotent verifies Close can be called repeatedly (and concurrently
// via the sync.Once guard) without panicking on a double channel close.
func TestClose_Idempotent(t *testing.T) {
	l, cleanup := setup(t)
	defer cleanup()

	require.NoError(t, l.Close())
	require.NoError(t, l.Close())
	require.NoError(t, l.Close())
	require.True(t, l.IsClosed())
}
