package commitlog

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Clean rewrites segments outside the log mutex so reads and appends stay
// concurrent. That is only safe if maintenance operations are mutually
// exclusive: a concurrent TruncateBefore (external retention, as used by bus
// retention loops) or a second Clean (explicit maintenance racing the internal
// cleaner loop) landing mid-rewrite deletes segment files the clean is
// scanning, and the clean's final swap resurrects the deleted segments in the
// segment list. This hammers appends, Clean, and TruncateBefore concurrently
// and then verifies the log is still fully readable from OldestOffset to
// NewestOffset.
func TestCleanConcurrentWithTruncateBefore(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 256, // roll constantly so maintenance has segments to chew
		Compact:         true,
	})
	defer cleanup()

	const (
		appends = 2000
		keys    = 50
	)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Maintenance hammer #1: explicit Clean (compaction), like an application
	// maintenance ticker racing the internal cleaner loop.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := l.Clean(); err != nil {
				t.Error(err)
				return
			}
		}
	}()

	// Maintenance hammer #2: external retention truncating the old prefix.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			newest := l.NewestOffset()
			if newest > 200 {
				if err := l.TruncateBefore(newest - 200); err != nil {
					t.Error(err)
					return
				}
			}
		}
	}()

	// Writer: keyed messages so compaction has superseded versions to drop.
	for i := 0; i < appends; i++ {
		key := []byte(fmt.Sprintf("key-%d", i%keys))
		_, err := l.Append([]*Message{{Key: key, Value: []byte(fmt.Sprintf("value-%d", i))}})
		require.NoError(t, err)
	}
	close(stop)
	wg.Wait()

	// One final maintenance pass, then the whole surviving log must be
	// readable end to end: no segment in the list may point at a deleted file
	// and no rewrite may have corrupted records.
	require.NoError(t, l.Clean())
	oldest, newest := l.OldestOffset(), l.NewestOffset()
	require.Greater(t, newest, oldest)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r, err := l.NewReader(oldest, true)
	require.NoError(t, err)
	headers := make([]byte, 28)
	read := 0
	for {
		_, offset, _, _, err := r.ReadMessage(ctx, headers)
		require.NoError(t, err, "log must remain readable after concurrent maintenance (read %d messages)", read)
		read++
		if offset >= newest {
			break
		}
	}
	require.GreaterOrEqual(t, read, 1)
}
