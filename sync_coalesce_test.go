package commitlog

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// countingLog builds a log whose every segment backing counts its fsyncs, and
// returns the log plus a reader for the running total.
func countingLog(t testing.TB, opts Options) (*commitLog, func() int64) {
	l, cleanup := setupWithOptions(t, opts)
	t.Cleanup(cleanup)
	var total int64
	l.mu.RLock()
	for _, seg := range l.segments {
		seg.Lock()
		seg.backing = &atomicCountingBacking{segmentBacking: seg.backing, n: &total}
		seg.Unlock()
	}
	l.mu.RUnlock()
	return l, func() int64 { return atomic.LoadInt64(&total) }
}

type atomicCountingBacking struct {
	segmentBacking
	n *int64
}

func (b *atomicCountingBacking) Sync() error {
	atomic.AddInt64(b.n, 1)
	return b.segmentBacking.Sync()
}

// An offset already covered by a completed flush costs nothing. This is what
// makes it safe for a caller to ask for durability per commit rather than
// tracking coverage itself.
func TestSyncCoveredOffsetIssuesNoFsync(t *testing.T) {
	l, fsyncs := countingLog(t, Options{Path: tempDir(t), MaxSegmentBytes: 1 << 20})

	offs, err := l.Append([]*Message{{Value: []byte("v")}})
	require.NoError(t, err)
	require.NoError(t, l.Sync(offs[0]))
	after := fsyncs()
	require.Positive(t, after, "the first sync must actually flush")

	// Same offset, and an older one: both already durable.
	require.NoError(t, l.Sync(offs[0]))
	require.NoError(t, l.Sync(offs[0]-1))
	require.Equal(t, after, fsyncs(), "an already-durable offset must not fsync")
}

// The point of the barrier: commits landing together share a flush. Without
// coalescing this costs one fsync per caller; with it, a caller whose offset
// the in-flight flush will cover waits for it instead of queueing another.
func TestConcurrentSyncsShareOneFsync(t *testing.T) {
	const writers = 32

	l, fsyncs := countingLog(t, Options{Path: tempDir(t), MaxSegmentBytes: 1 << 20})

	// Everyone appends first, so every offset is covered by any flush that
	// starts after this point — the shape of a batch of commits arriving at once.
	offs := make([]int64, writers)
	for i := range offs {
		got, err := l.Append([]*Message{{Value: []byte("v")}})
		require.NoError(t, err)
		offs[i] = got[0]
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(off int64) {
			defer wg.Done()
			<-start
			if err := l.Sync(off); err != nil {
				t.Error(err)
			}
		}(offs[i])
	}
	close(start)
	wg.Wait()

	require.Less(t, fsyncs(), int64(writers),
		"%d concurrent syncs cost %d fsyncs — they are not coalescing",
		writers, fsyncs())
	require.Positive(t, fsyncs())
}

// A failed flush must not publish coverage. Otherwise every later caller below
// that offset returns success against data that never reached the disk — a
// durability primitive reporting an error and then silently pretending.
func TestFailedSyncDoesNotAdvanceCoverage(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t), MaxSegmentBytes: 1 << 20})
	t.Cleanup(cleanup)

	offs, err := l.Append([]*Message{{Value: []byte("v")}})
	require.NoError(t, err)

	l.mu.RLock()
	seg := l.segments[0]
	l.mu.RUnlock()
	failing := &failingBacking{segmentBacking: seg.backing, failures: 1}
	seg.Lock()
	seg.backing = failing
	seg.Unlock()

	require.Error(t, l.Sync(offs[0]), "the failing fsync must be reported")

	// The retry must genuinely flush rather than find itself already covered.
	require.NoError(t, l.Sync(offs[0]))
	require.Equal(t, 2, failing.syncs,
		"a failed flush must not leave the offset marked durable")
}

// The hot path flushes log bytes only. The index is left to seal and to
// recovery, so a durability sync must not pay for it.
func TestSyncDoesNotFlushTheIndex(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t), MaxSegmentBytes: 1 << 20})
	t.Cleanup(cleanup)

	offs, err := l.Append([]*Message{{Value: []byte("v")}})
	require.NoError(t, err)
	require.NoError(t, l.Sync(offs[0]))

	l.mu.RLock()
	seg := l.segments[0]
	l.mu.RUnlock()
	seg.RLock()
	dirtyIndex, dirtyData := seg.dirtyIndex, seg.dirtyData
	seg.RUnlock()

	require.False(t, dirtyData, "the log bytes must have been flushed")
	require.True(t, dirtyIndex,
		"the index must be left for seal and recovery, not flushed on the hot path")

	// SyncAll still covers it.
	require.NoError(t, l.SyncAll())
	seg.RLock()
	dirtyIndex = seg.dirtyIndex
	seg.RUnlock()
	require.False(t, dirtyIndex, "SyncAll must still flush the index")
}

// Sealing is the index's flush point, because open() rebuilds a short index
// tail for the ACTIVE segment only — so a segment that rolls between syncs
// would otherwise keep a permanently short index that nothing repairs.
func TestSealFlushesTheIndex(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 128, // roll quickly
	})
	t.Cleanup(cleanup)

	for i := 0; i < 40; i++ {
		_, err := l.Append([]*Message{{Value: []byte("some value padding")}})
		require.NoError(t, err)
	}

	l.mu.RLock()
	defer l.mu.RUnlock()
	require.Greater(t, len(l.segments), 1, "the log must have rolled")
	for i, seg := range l.segments[:len(l.segments)-1] {
		seg.RLock()
		sealed, dirtyIndex := seg.sealed, seg.dirtyIndex
		seg.RUnlock()
		if !sealed {
			continue
		}
		require.False(t, dirtyIndex,
			"sealed segment %d still has an unflushed index; nothing will repair it", i)
	}
}
