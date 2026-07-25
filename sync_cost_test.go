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
	require.NoError(t, l.Sync())
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
	require.NoError(t, l.Sync())

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

	require.NoError(t, l.Sync())
	for i, c := range counters {
		require.Zero(t, c.syncs, "segment %d had nothing new to flush", i)
	}

	app(1)
	require.NoError(t, l.Sync())
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
		{"Sync", (*commitLog).Sync},
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

// BenchmarkSyncConcurrent measures per-commit cost when many writers sync the
// same log at once — the shape a group-commit batcher sits on. With the fsync
// held outside the segment lock, appends keep landing during a sync instead of
// serializing behind it.
func BenchmarkSyncConcurrent(b *testing.B) {
	l, _ := syncLog(b)
	var i int
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i++
			if _, err := l.Append([]*Message{{Value: []byte("v")}}); err != nil {
				b.Fatal(err)
			}
			if err := l.Sync(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
