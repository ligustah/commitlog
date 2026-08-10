package commitlog

import (
	"errors"
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

// refusingBacking fails its Close and nothing else. The embedded interface is
// nil on purpose: closeSegments must touch no other method, and a panic here
// would say it does.
type refusingBacking struct {
	segmentBacking
	err error
}

func (r refusingBacking) Close() error { return r.err }

// A segment that refuses to close must not take the rest of the set with it.
//
// This is the last walk of l.segments — segmentsClosed is set right after, and
// no caller retries a Close it was already told failed — so returning at the
// first error left every LATER segment holding its file handle and its index
// mmap for the life of the process. On Windows a mapped index cannot be
// unlinked, so the log's own directory then could not be removed either, which
// is how it surfaces: a TempDir cleanup failing on an .index long after every
// Close returned.
func TestCloseWalksEverySegmentEvenWhenOneRefuses(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:                 tempDir(t),
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
	require.Greater(t, len(l.segments), 2, "the fixture needs segments AFTER the refusing one")

	boom := errors.New("this segment will not close")
	victim := l.segments[0]
	victim.Lock()
	real := victim.backing
	victim.backing = refusingBacking{err: boom}
	victim.Unlock()
	// The refusal leaks the victim's own handle by construction, which is the
	// bug being reproduced; release it here so the fixture's teardown is
	// measuring the OTHER segments and not this deliberate one. That teardown
	// failing with "cannot access the file because it is being used by another
	// process" is precisely the symptom, so it must have exactly one cause.
	defer real.Close() // nolint: errcheck

	err := l.Close()
	require.ErrorIs(t, err, boom, "the failure must still be reported, not swallowed")

	for i, s := range l.segments[1:] {
		s.RLock()
		closed := s.closed
		s.RUnlock()
		require.True(t, closed,
			"segment %d was left open behind the one that refused", i+1)
	}
}
