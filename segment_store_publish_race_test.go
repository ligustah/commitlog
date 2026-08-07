package commitlog

import (
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// A read of a store object must survive that object being republished.
//
// FileSegmentStore.Put commits by renaming a temp file over the object path.
// The read side opened that path with a bare os.Open, which on Windows fails
// with ERROR_SHARING_VIOLATION — "The process cannot access the file because it
// is being used by another process" — rather than succeeding or reporting the
// file missing. Not a crash-recovery window and not a corrupted store: an
// ordinary publish, on a healthy machine.
//
// The object here is the tier manifest, because that is the one this cost.
// readTierManifest sizes the manifest and then reads it, both against a path a
// publish renames over, and it runs inside open() — so losing the race did not
// degrade a read, it failed the whole log open. Reported by CI on v0.61.0's
// race (windows) job, from a test that merely opened an offloaded log.
//
// Both syscalls are covered deliberately. Retrying only the read would leave
// Size as the call that fails, which is the same bug one line earlier.
//
// On unix rename is atomic and a reader never observes the window, so this
// passes there without exercising anything. That is worth keeping rather than
// guarding out: it is the same code on both platforms, and a unix-only
// regression in the retry loop would still be caught.
func TestAStoreReadSurvivesAConcurrentPublish(t *testing.T) {
	store, err := NewFileSegmentStore(tempDir(t))
	require.NoError(t, err)

	const key = "manifest"
	// Small on purpose. What decides whether a reader ever observes the window is
	// the share of the publisher's cycle spent in the rename rather than in
	// create/write/sync, so a smaller payload raises the rename RATE and with it
	// the odds a single run reproduces. A real tier manifest is small too.
	payload := bytes.Repeat([]byte("m"), 64)
	put := func() error {
		return store.Put(key, bytes.NewReader(payload), int64(len(payload)))
	}
	require.NoError(t, put())

	// Republish continuously for the duration of the reads, from exactly ONE
	// goroutine. A second publisher would widen the window faster than more read
	// iterations do, and it is the wrong instrument: Put names its temp file
	// path+".tmp" per key, so two concurrent publishes of the same key truncate
	// each other's in-flight write. That is a different defect, and a test that
	// tripped over it would be red for a reason it does not name.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var putErr error
	var mu sync.Mutex
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := put(); err != nil {
				mu.Lock()
				putErr = err
				mu.Unlock()
				return
			}
		}
	}()

	// Enough iterations that a single run is reliable rather than lucky. With the
	// read-side retry removed, the first loss came at iteration 291 of 300 — a
	// count that passes far more often than it should, which would make this a
	// test that reports coverage it does not have.
	buf := make([]byte, len(payload))
	for i := 0; i < 3000; i++ {
		size, serr := store.Size(key)
		require.NoErrorf(t, serr, "Size lost the race to a publish on iteration %d", i)
		require.EqualValues(t, len(payload), size)

		_, rerr := store.ReadAt(key, buf, 0)
		require.NoErrorf(t, rerr, "ReadAt lost the race to a publish on iteration %d", i)
		require.Equal(t, payload, buf, "a read that won the race still returned the wrong bytes")
	}

	close(stop)
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	require.NoError(t, putErr, "the publisher itself failed, so the reads above proved nothing")
}
