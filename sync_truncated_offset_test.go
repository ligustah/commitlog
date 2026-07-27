package commitlog

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Sync waits until the durable watermark reaches the requested offset, and that
// watermark is taken from the log's tail. Retention can move the tail BELOW an
// offset a caller still holds — it appended at 100, retention dropped
// everything past 50, and it then asks for 100 to be made durable. The
// condition can never be met, so a Sync that only loops would spin forever
// issuing fsyncs against a log that will never reach it.
//
// The offset is gone, which means there is nothing left to make durable, so the
// call must return rather than hang. Found by breaking the watermark during an
// assertion-strength audit: the barrier's tests did not fail, they timed out.
func TestSyncReturnsForAnOffsetTheLogNoLongerReaches(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 1 << 20,
	})
	defer cleanup()

	var last int64
	for i := 0; i < 20; i++ {
		offs, err := l.Append([]*Message{{Value: []byte("v")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)

	// Drop the tail below the offset the caller is still holding.
	require.NoError(t, l.Truncate(10))
	require.Less(t, l.NewestOffset(), last, "the tail must now be below it")

	done := make(chan error, 1)
	go func() { done <- l.Sync(last) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Sync hung on an offset the log no longer reaches; it can never " +
			"be covered, so looping until it is never terminates")
	}
}
