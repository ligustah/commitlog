package commitlog

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Concurrent readers over offloaded segments (tiered storage option 2) hammer
// the process-wide RemoteIndexCache: many small indexes, a tiny budget, so
// acquire/fetch/evict/release race continuously. Pin safety must hold — an
// index a live seek is holding is never evicted out from under it — and every
// read returns the exact committed bytes. Run under `go test -race` for the
// full effect; the assertions are schedule-independent so it is deterministic
// either way, kept out of the seeded fuzz corpus so a failing seed stays
// replayable.
func TestOffloadIndexCacheConcurrentReaders(t *testing.T) {
	base := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(base, "store"))
	require.NoError(t, err)
	cache, err := NewRemoteIndexCache(filepath.Join(base, "idxcache"), 2<<10) // tiny → constant eviction
	require.NoError(t, err)

	opts := Options{
		Path:                 filepath.Join(base, "log"),
		MaxSegmentBytes:      128, // many sealed segments => many offloaded indexes
		SegmentStore:         store,
		RemoteIndexCache:     cache,
		DisableAutoClean:     true,
		HWCheckpointInterval: time.Hour,
		CleanerInterval:      time.Hour,
	}
	l, err := New(opts)
	require.NoError(t, err)
	cl := l.(*commitLog)
	defer cl.Close()

	const n = 240
	for i := 0; i < n; i++ {
		o, err := cl.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("k%d", i%8)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}})
		require.NoError(t, err)
		cl.SetHighWatermark(o[0])
	}
	require.NoError(t, cl.SyncAll())

	// Offload every sealed segment (index offloaded too, fetched to the cache on
	// read). The active segment stays local.
	_, err = cl.OffloadBefore(cl.NewestOffset())
	require.NoError(t, err)

	pre := fzReadAll(t, cl) // ground-truth committed bytes per offset

	var wg sync.WaitGroup
	errCh := make(chan error, 128)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for iter := 0; iter < 25; iter++ {
				r, err := cl.NewReader(cl.OldestOffset(), true)
				if err != nil {
					errCh <- err
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				headers := make([]byte, msgSetHeaderLen)
				newest := cl.NewestOffset()
				for {
					msg, off, _, _, e := r.ReadMessage(ctx, headers)
					if e != nil {
						errCh <- fmt.Errorf("read at offset %d: %w", off, e)
						break
					}
					if string(msg) != string(pre[off]) {
						errCh <- fmt.Errorf("offset %d: got %q want %q", off, msg, pre[off])
						break
					}
					if off >= newest {
						break
					}
				}
				cancel()
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		t.Fatal(e)
	}
}
