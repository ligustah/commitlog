package commitlog

import (
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// Both tests here replace a guard whose only coverage was a concurrency test.
//
// Those tests assert a RATE — reads served while a truncation runs — so they
// prove the property by timing, and timing is not an assertion. On a loaded CI
// runner the neutralised code still passed them, and guardcheck reported the
// guards as uncovered while nothing was actually wrong with the code. A guard
// that only bites on a fast machine goes quiet exactly when nobody is watching.
//
// The fix is to make the truncation act at a point the test CONTROLS rather than
// at a moment it hopes to catch. A SegmentStore is the seam: an offloaded
// segment reads and deletes through it, so a wrapping store is called
// synchronously from inside the truncation, on its goroutine, at a known step.
// Nothing here sleeps, races or measures a rate.

// storeHook wraps a store and runs during a specific operation on it.
type storeHook struct {
	SegmentStore
	mu       sync.Mutex
	onDelete func()
	onRead   func()
	deletes  int
	reads    int
}

func (s *storeHook) Delete(key string) error {
	s.mu.Lock()
	s.deletes++
	fn := s.onDelete
	s.mu.Unlock()
	if fn != nil {
		fn()
	}
	return s.SegmentStore.Delete(key)
}

func (s *storeHook) ReadAt(key string, p []byte, off int64) (int, error) {
	s.mu.Lock()
	s.reads++
	fn := s.onRead
	s.mu.Unlock()
	if fn != nil {
		fn()
	}
	return s.SegmentStore.ReadAt(key, p, off)
}

// The boundary scan streams rather than reading at an offset — storeBacking
// prefetches — so the read hook has to cover both to be sure of firing.
func (s *storeHook) Stream(key string, off int64) (io.ReadCloser, error) {
	s.mu.Lock()
	s.reads++
	fn := s.onRead
	s.mu.Unlock()
	if fn != nil {
		fn()
	}
	return s.SegmentStore.Stream(key, off)
}

func (s *storeHook) counts() (deletes, reads int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deletes, s.reads
}

// hookedTieredLog builds a log whose sealed segments have all been offloaded, so every
// delete and every boundary read during a truncation goes through the hook.
func hookedTieredLog(t *testing.T, hook *storeHook) (*commitLog, int64) {
	t.Helper()
	root := tempDir(t)
	fs, err := NewFileSegmentStore(filepath.Join(root, "store"))
	require.NoError(t, err)
	hook.SegmentStore = fs

	l, err := New(Options{
		Path:             filepath.Join(root, "log"),
		MaxSegmentBytes:  128,
		DisableAutoClean: true,
		Tiers:            oneTier(hook),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })

	cl := l.(*commitLog)
	var last int64
	for n := int64(0); n < 400; n++ {
		offs, aerr := cl.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("k:%d", n%16)),
			Value: []byte(strconv.FormatInt(n, 10) + ":padding to force segment rolls"),
		}})
		require.NoError(t, aerr)
		last = offs[len(offs)-1]
		cl.SetHighWatermark(last)
	}

	cl.mu.RLock()
	segs := len(cl.segments)
	cl.mu.RUnlock()
	require.Greater(t, segs, 20, "need enough segments for a truncation to have work to do")

	// Offload every sealed segment, so the ones a truncation deletes — and the
	// one it rewrites — are backed by the hook.
	n, err := cl.OffloadBefore(last)
	require.NoError(t, err)
	require.Greater(t, n, 0, "nothing was offloaded; the hook would never be called")
	return cl, last
}

// Truncate must unlink with l.mu RELEASED.
//
// Stated without timing: the unlink itself asks whether the lock is free. If
// Truncate holds l.mu across its deletes, TryRLock fails — on Truncate's own
// goroutine, since Go's mutexes are not reentrant — and this fails with no
// dependence on how fast anything runs. Neutralizing the guard takes l.mu around
// the delete loop, which is exactly what this observes.
func TestATruncateUnlinksWithTheSegmentLockAvailable(t *testing.T) {
	hook := &storeHook{}
	l, last := hookedTieredLog(t, hook)

	var (
		mu      sync.Mutex
		blocked int
	)
	hook.onDelete = func() {
		// The lock this probes is the one every reader and appender needs. Held
		// here means the log is a hard stop for the duration of the unlinks.
		if l.mu.TryRLock() {
			l.mu.RUnlock()
			return
		}
		mu.Lock()
		blocked++
		mu.Unlock()
	}

	require.NoError(t, l.Truncate(last/2))

	deletes, _ := hook.counts()
	require.Greater(t, deletes, 0,
		"no segment was unlinked, so this proves nothing about when they are")

	mu.Lock()
	defer mu.Unlock()
	require.Zero(t, blocked,
		"%d of %d unlinks ran with l.mu held; every reader and appender was "+
			"stopped for the length of the delete loop", blocked, deletes)
}

// TruncateBefore must unlink with l.mu RELEASED, for the same reason and by the
// same observation as the Truncate case above. Its unlinks run AFTER the publish
// step, which is why the neutralization can hold the lock to the end of the call
// without deadlocking on it.
func TestATruncationUnlinksWithTheSegmentLockAvailable(t *testing.T) {
	hook := &storeHook{}
	l, last := hookedTieredLog(t, hook)

	var (
		mu      sync.Mutex
		blocked int
	)
	hook.onDelete = func() {
		if l.mu.TryRLock() {
			l.mu.RUnlock()
			return
		}
		mu.Lock()
		blocked++
		mu.Unlock()
	}

	require.NoError(t, l.TruncateBefore(last/2))

	deletes, _ := hook.counts()
	require.Greater(t, deletes, 0,
		"no segment was unlinked, so this proves nothing about when they are")

	mu.Lock()
	defer mu.Unlock()
	require.Zero(t, blocked,
		"%d of %d unlinks ran with l.mu held; a retention pass is mostly unlinks, "+
			"so this stops every reader and appender for most of its length",
		blocked, deletes)
}

// TruncateBefore must carry segments an append rolled while its lock was down.
//
// The window is real but hard to hit on purpose: TruncateBefore snapshots the
// segment list, releases l.mu to rewrite the boundary, and re-takes it to
// publish. An append during the rewrite calls split(), which appends to
// l.segments — so publishing the spliced snapshot alone would forget it, losing
// records that were already acknowledged.
//
// Here the append happens FROM INSIDE the rewrite. The boundary is offloaded, so
// scanning it reads through the hook, and the hook appends enough to roll. That
// is the window, entered deterministically rather than raced for.
func TestATruncationCarriesASegmentRolledDuringItsRewrite(t *testing.T) {
	hook := &storeHook{}
	l, last := hookedTieredLog(t, hook)

	var (
		once     sync.Once
		rolled   int64
		rollErr  error
		rollDone bool
	)
	hook.onRead = func() {
		// Once only: this runs inside the boundary scan, which reads repeatedly.
		once.Do(func() {
			rollDone = true
			for n := 0; n < 60; n++ {
				offs, err := l.Append([]*Message{{
					Key:   []byte("late"),
					Value: []byte("appended while the truncation had the lock down"),
				}})
				if err != nil {
					rollErr = err
					return
				}
				rolled = offs[len(offs)-1]
				l.SetHighWatermark(rolled)
			}
		})
	}

	// A cut INSIDE a segment, so the boundary rewrite runs and the hook fires.
	require.NoError(t, l.TruncateBefore(last/2))
	require.NoError(t, rollErr)
	require.True(t, rollDone,
		"the boundary was never read, so nothing rolled under the truncation")
	require.Greater(t, rolled, last, "the appends did not advance the log")

	// The records written during the truncation are still there. This is the
	// whole claim: without the carry, the published list forgets the segments
	// split() appended, and these offsets become unreachable.
	require.Equal(t, rolled, l.NewestOffset(),
		"the newest offset went backwards; a rolled segment was dropped at publish")

	l.mu.RLock()
	defer l.mu.RUnlock()
	require.NotEmpty(t, l.segments)
	require.Equal(t, rolled, l.segments[len(l.segments)-1].NextOffset()-1,
		"the last published segment does not hold the last acknowledged record")
}
