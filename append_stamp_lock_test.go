package commitlog

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// An append must read the clock while it holds appendMu.
//
// The property being protected is that offset order is timestamp order, which
// every timestamp lookup binary-searches on and none of them can check. Stating
// it as "the stamps come out monotonic" would need two appenders racing, and a
// race that has to be caught by timing is a test that goes quiet on a loaded
// machine — the failure mode this package has already been bitten by. So it is
// stated as the discipline that produces it instead: offsets are assigned under
// appendMu, so the clock read that stamps them has to happen under appendMu too.
//
// Asked of the append itself, from inside the clock. Go's mutexes are not
// reentrant, so TryLock fails on the appending goroutine exactly when the lock
// is already held — and succeeds, loudly, when the read has drifted back outside
// the critical section.
func TestAnAppendStampsItsTimeUnderTheAppendLock(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t)})
	defer cleanup()

	var (
		mu       sync.Mutex
		probes   int
		unlocked int
	)
	// Installed around the Append only. Segment creation during New() reads the
	// clock too, and it has no business holding this log's append lock.
	before := timestamp
	timestamp = func() int64 {
		mu.Lock()
		probes++
		if l.appendMu.TryLock() {
			// Nothing held it, so offsets are being handed out somewhere this
			// read cannot see.
			unlocked++
			l.appendMu.Unlock()
		}
		mu.Unlock()
		return before()
	}
	_, err := l.Append([]*Message{{Value: []byte("stamped by the log")}})
	timestamp = before
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.Positive(t, probes, "the append never read the clock, so this proves nothing")
	require.Zero(t, unlocked,
		"%d of %d clock reads during an append ran with appendMu free; a "+
			"concurrent appender can take the lock in that window and store a "+
			"LATER offset with an EARLIER timestamp", unlocked, probes)
}
