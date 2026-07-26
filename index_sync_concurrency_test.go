package commitlog

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testIndex(t testing.TB, bytes int64) *index {
	idx, err := newIndex(options{
		path:       filepath.Join(tempDir(t), "0.index"),
		bytes:      bytes,
		baseOffset: 0,
	})
	require.NoError(t, err)
	t.Cleanup(func() { idx.Close() })
	idx.position = 0
	return idx
}

func indexEntries(n, from int) []*entry {
	out := make([]*entry, n)
	for i := range out {
		out[i] = &entry{
			Offset:    int64(from + i),
			Timestamp: int64(from+i) * 10,
			Position:  int64(from+i) * 100,
			Size:      100,
		}
	}
	return out
}

// The index flush must not hold the mutex that entry writes need. An append
// blocked behind a flush cannot join a caller's next commit batch, which is how
// group commit gets defeated a layer below the segment.
//
// The flush is made slow by having a large mapping to walk, and the assertion
// is a RATIO rather than an absolute count, so it survives a slow or loaded
// machine (both terms scale together) while still separating the two regimes by
// a wide margin. Writes per flush measured here: ~477 with the flush outside
// the mutex, ~8 with it inside — a plain "more writes than flushes" bar passes
// in both and catches nothing.
func TestIndexWriteDuringSyncDoesNotSerialize(t *testing.T) {
	idx := testIndex(t, 64*1024*1024)
	require.NoError(t, idx.writeEntries(indexEntries(1024, 0)))

	var (
		wg       sync.WaitGroup
		stop     = make(chan struct{})
		writes   int64
		writesMu sync.Mutex
	)
	// Hammer entry writes for the duration.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := idx.writeEntries(indexEntries(1, 2048+i)); err != nil {
				return
			}
			writesMu.Lock()
			writes++
			writesMu.Unlock()
		}
	}()

	// Flush repeatedly while the writer runs. With the flush under the write
	// mutex, the writer makes almost no progress.
	deadline := time.Now().Add(500 * time.Millisecond)
	syncs := 0
	for time.Now().Before(deadline) {
		require.NoError(t, idx.Sync())
		syncs++
	}
	close(stop)
	wg.Wait()

	writesMu.Lock()
	got := writes
	writesMu.Unlock()
	t.Logf("%d entry writes landed across %d flushes (%.1f per flush)",
		got, syncs, float64(got)/float64(syncs))
	require.Greater(t, got, int64(syncs)*25,
		"entry writes are serializing behind index flushes: at most a couple of "+
			"dozen land per flush when the flush holds the write mutex")
}

// The flush pins the mapping without holding mu, so the remap-on-expand must
// exclude it explicitly — otherwise a flush can walk a mapping being torn down.
// Drive expansion (which remaps) against continuous flushing; under -race this
// is the check that the two are properly ordered.
func TestIndexSyncRacesRemapOnExpand(t *testing.T) {
	// A small initial mapping so writes force repeated expansion+remap.
	idx := testIndex(t, 4*entryWidth)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := idx.Sync(); err != nil {
				return
			}
		}
	}()

	for i := 0; i < 4000; i++ {
		require.NoError(t, idx.writeEntries(indexEntries(1, i)))
	}
	close(stop)
	wg.Wait()

	require.NoError(t, idx.Sync())
	require.Equal(t, int64(4000), idx.CountEntries())
}

// BenchmarkIndexSyncWithWriter measures entry-write throughput while a flusher
// runs against the same index — the shape a group-commit batcher creates.
func BenchmarkIndexSyncWithWriter(b *testing.B) {
	idx := testIndex(b, 64*1024*1024)
	require.NoError(b, idx.writeEntries(indexEntries(1024, 0)))

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := idx.Sync(); err != nil {
				return
			}
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := idx.writeEntries(indexEntries(1, 4096+i)); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	close(stop)
	wg.Wait()
}
