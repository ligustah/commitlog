package commitlog

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// syncLog builds a log with an append helper, for the durability-cost tests.
func syncLog(t testing.TB) (*commitLog, func(i int)) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 1024 * 1024,
	})
	t.Cleanup(cleanup)
	app := func(i int) {
		offs, err := l.Append([]*Message{{Value: []byte(fmt.Sprintf("v%d", i))}})
		require.NoError(t, err)
		l.SetHighWatermark(offs[0])
	}
	return l, app
}

// Sync is the durability primitive and must not pay for the high-watermark
// checkpoint: the checkpoint costs a SECOND fsync of the active segment plus an
// atomic rewrite of the checkpoint file, and it is only an optimization —
// recovery rides out a stale checkpoint. SyncAll keeps it, for the promote path
// whose observers must not see the log roll back.
func TestSyncSkipsHighWatermarkCheckpoint(t *testing.T) {
	l, app := syncLog(t)
	hwPath := filepath.Join(l.Path, hwFileName)

	app(0)
	require.NoError(t, l.SyncAll())
	before, err := os.ReadFile(hwPath)
	require.NoError(t, err)

	// More records, then a durability sync: the checkpoint must be untouched.
	for i := 1; i < 10; i++ {
		app(i)
	}
	require.NoError(t, l.Sync(l.NewestOffset()))
	after, err := os.ReadFile(hwPath)
	require.NoError(t, err)
	require.Equal(t, before, after, "Sync must not rewrite the high-watermark checkpoint")

	// SyncAll still advances it.
	require.NoError(t, l.SyncAll())
	final, err := os.ReadFile(hwPath)
	require.NoError(t, err)
	require.NotEqual(t, before, final, "SyncAll must still checkpoint the high watermark")
}

// A flush must fsync only the segments written since their last one — the
// SEALED ones behind the tail are already durable and must not be paid for
// again on every commit.
//
// The log is rolled deliberately so there are several segments. An earlier
// version of this test used a single-segment log, which made "only the dirty
// segment" vacuous: with one segment, flushing all of them and flushing only
// the dirty one are the same thing, and the test passed with dirty tracking
// disabled entirely.
func TestSyncFsyncsDirtySegmentsOnly(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 64, // roll constantly
	})
	t.Cleanup(cleanup)

	var last int64
	for i := 0; i < 20; i++ {
		offs, err := l.Append([]*Message{{Value: []byte("some padding value")}})
		require.NoError(t, err)
		last = offs[0]
	}
	require.NoError(t, l.Sync(last))

	// Append once more FIRST, then wrap: the append may roll a fresh segment,
	// and that new one is precisely the segment expected to be flushed.
	offs, err := l.Append([]*Message{{Value: []byte("some padding value")}})
	require.NoError(t, err)

	l.mu.RLock()
	segs := append([]*segment(nil), l.segments...)
	l.mu.RUnlock()
	require.Greater(t, len(segs), 2, "the log must have rolled for this to mean anything")

	counters := make([]*countingBacking, len(segs))
	for i, seg := range segs {
		c := &countingBacking{segmentBacking: seg.backing}
		counters[i] = c
		seg.Lock()
		seg.backing = c
		seg.Unlock()
	}

	// Only the ACTIVE segment took that write, so only it may be fsynced — the
	// sealed ones behind it were made durable by the first flush.
	require.NoError(t, l.Sync(offs[0]))

	for i, c := range counters[:len(counters)-1] {
		require.Zero(t, c.syncs,
			"sealed segment %d was already durable and must not be fsynced again", i)
	}
	require.Equal(t, 1, counters[len(counters)-1].syncs,
		"the appended-to segment must be fsynced exactly once")
}

// BenchmarkSyncCost contrasts the durability primitive with the promote-path
// barrier on one appended record. The gap is the high-watermark checkpoint: a
// second fsync of the active segment plus the atomic checkpoint rewrite.
func BenchmarkSyncCost(b *testing.B) {
	for _, bc := range []struct {
		name string
		fn   func(l *commitLog) error
	}{
		{"Sync", func(l *commitLog) error { return l.Sync(l.NewestOffset()) }},
		{"SyncAll", (*commitLog).SyncAll},
	} {
		b.Run(bc.name, func(b *testing.B) {
			l, app := syncLog(b)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				app(i)
				if err := bc.fn(l); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSyncConcurrent measures many writers committing to one log at once —
// the group-commit shape — and reports **fsyncs/op** alongside the time.
//
// The fsync count is the number that demonstrates coalescing; wall-clock here
// is dominated by whichever caller happens to be leading a flush and does not
// cleanly attribute. Each caller syncs the offset IT appended, which is what
// lets the barrier cover it off someone else's flush.
func BenchmarkSyncConcurrent(b *testing.B) {
	l, fsyncs := benchCountingLog(b)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			offs, err := l.Append([]*Message{{Value: []byte("v")}})
			if err != nil {
				b.Fatal(err)
			}
			if err := l.Sync(offs[0]); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(fsyncs())/float64(b.N), "fsyncs/op")
}

// BenchmarkSyncTailConcurrent asks for the log's CURRENT tail rather than the
// caller's own offset. The tail advances with every append, so it is never
// covered by a flush already in flight and every caller leads one — the same
// barrier, defeated by what the caller asks for. The contrast in fsyncs/op is
// the argument for syncing the offset you were given.
func BenchmarkSyncTailConcurrent(b *testing.B) {
	l, fsyncs := benchCountingLog(b)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := l.Append([]*Message{{Value: []byte("v")}}); err != nil {
				b.Fatal(err)
			}
			if err := l.Sync(l.NewestOffset()); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(fsyncs())/float64(b.N), "fsyncs/op")
}

// benchCountingLog is a single-segment log whose backing counts fsyncs.
func benchCountingLog(b *testing.B) (*commitLog, func() int64) {
	return countingLog(b, Options{Path: tempDir(b), MaxSegmentBytes: 1 << 30})
}
