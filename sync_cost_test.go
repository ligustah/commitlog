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

// Sync fsyncs every segment written since its last sync, and nothing else — a
// second Sync with no appends in between must not touch the disk at all.
func TestSyncFsyncsDirtySegmentsOnly(t *testing.T) {
	l, app := syncLog(t)
	app(0)
	require.NoError(t, l.Sync(l.NewestOffset()))

	l.mu.RLock()
	segs := append([]*segment(nil), l.segments...)
	l.mu.RUnlock()
	counters := make([]*countingBacking, len(segs))
	for i, seg := range segs {
		c := &countingBacking{segmentBacking: seg.backing}
		counters[i] = c
		seg.Lock()
		seg.backing = c
		seg.Unlock()
	}

	require.NoError(t, l.Sync(l.NewestOffset()))
	for i, c := range counters {
		require.Zero(t, c.syncs, "segment %d had nothing new to flush", i)
	}

	app(1)
	require.NoError(t, l.Sync(l.NewestOffset()))
	total := 0
	for _, c := range counters {
		total += c.syncs
	}
	require.Equal(t, 1, total, "exactly the appended-to segment must be fsynced")
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
