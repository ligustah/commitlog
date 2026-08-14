//go:build windows

package commitlog

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// openDenyAll opens path with a share mode of 0, so every other opener — and
// ReplaceFile — is refused until the handle closes. That is what a scanner or a
// not-yet-reaped process looks like to the checkpoint write.
func openDenyAll(t *testing.T, path string) syscall.Handle {
	t.Helper()
	p, err := syscall.UTF16PtrFromString(path)
	require.NoError(t, err)
	h, err := syscall.CreateFile(p, syscall.GENERIC_READ, 0, nil,
		syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	require.NoError(t, err, "could not take an exclusive handle on %s", path)
	return h
}

// A transient exclusive handle on the destination must not fail the write: the
// checkpoint path retries briefly, and the handle clears in milliseconds. This
// is the failure a consumer hit under a kill/restart soak — "cannot replace
// ...replication-offset-checkpoint...: Access is denied" — which killed the
// process rather than being ridden out.
func TestAtomicWriteRetriesThroughTransientHandle(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "checkpoint")

	require.NoError(t, AtomicWriteFileWithRetry(path, bytes.NewReader([]byte("first"))))

	h := openDenyAll(t, path)
	released := make(chan struct{})
	go func() {
		time.Sleep(120 * time.Millisecond) // well inside the retry bound
		syscall.CloseHandle(h)             // nolint: errcheck
		close(released)
	}()

	// Must succeed despite the destination being locked when it starts.
	require.NoError(t, AtomicWriteFileWithRetry(path, bytes.NewReader([]byte("second"))),
		"a transient exclusive handle must be retried through, not returned as an error")
	<-released

	// And the retry must write the SAME payload — atomic_file.WriteFile consumes
	// the reader, so an unbuffered retry would truncate this to nothing.
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "second", string(got), "retry wrote the wrong bytes")
}

// A handle that never clears must still fail, and within the bound rather than
// hanging: two live writers are a real conflict, not a transient one.
func TestAtomicWritePermanentHandleStillFails(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "checkpoint")
	require.NoError(t, AtomicWriteFileWithRetry(path, bytes.NewReader([]byte("first"))))

	h := openDenyAll(t, path)

	start := time.Now()
	err := AtomicWriteFileWithRetry(path, bytes.NewReader([]byte("second")))
	elapsed := time.Since(start)

	// Release before reading: the deny-all handle blocks this test too.
	require.NoError(t, syscall.CloseHandle(h))

	require.Error(t, err, "a permanently held destination must fail, not be hidden")
	require.Less(t, elapsed, 10*time.Second, "retry bound must terminate promptly")

	got, rerr := os.ReadFile(path)
	require.NoError(t, rerr)
	require.Equal(t, "first", string(got), "a failed write must leave the old contents")
}

// Reopening a log rides out a transient handle on its DESCRIPTOR.
//
// This is the same window as the checkpoint above, on the file that can least
// afford it. The descriptor is what says the log exists and what it is, so a
// sharing violation on it fails the whole of New() — not a degraded open, no
// log at all — and the window is exactly a restart after a hard kill, which is
// when a supervisor restarts a process in the first place.
//
// Both halves are exercised at once and neither is redundant: reconcileDescriptor
// READS the descriptor and then WRITES it back to keep its fields current, so
// the open needs openWithRetry on the read and AtomicWriteFileWithRetry on the
// write. Both used to be bare — os.Open and the atomic-file library — and a
// deny-all handle on this file failed the open with "The process cannot access
// the file because it is being used by another process".
//
// The fixture CHECKS ITSELF, and that is the difference between this test and
// the three above it. Those hold a file across a call that reaches it
// immediately, so any hold at all covers the attempt. New() reaches the
// descriptor only after claiming the directory, building the epoch cache and
// running init(), so the hold has to outlast all of that — and on the CI
// Windows runner a fixed 120ms did not. Nothing went red there. The read simply
// landed after the handle had cleared, the retry was never exercised, and the
// only thing that noticed was guardcheck, which reported the guard uncovered
// while the test itself stayed green.
//
// So the hold is priced from an unheld reopen on the machine actually running
// the test, and then the test asserts the two things that make it mean
// anything: that the handle really does deny access here, and that the open
// really did WAIT. Without the second, a window that closes early is again a
// silent pass — a test that proves nothing looks exactly like a test that
// passes.
func TestReopeningALogRidesOutAHeldDescriptor(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{Path: dir, MaxSegmentBytes: 1 << 20, CleanerInterval: time.Hour})
	require.NoError(t, err)
	_, err = l.Append([]*Message{{Key: []byte("k"), Value: []byte("v")}})
	require.NoError(t, err)
	require.NoError(t, l.Close())

	priced := time.Now()
	warm, err := New(Options{Path: dir, MaxSegmentBytes: 1 << 20, CleanerInterval: time.Hour})
	require.NoError(t, err)
	require.NoError(t, warm.Close())
	// Strictly longer than a whole unheld reopen, so the descriptor read is
	// inside the window by construction rather than by luck.
	hold := 2*time.Since(priced) + 500*time.Millisecond
	require.Less(t, hold, waitedOnRetryBudget,
		"the hold must sit inside the read's retry budget, or a correct retry gives up first")

	descriptor := filepath.Join(dir, descriptorFileName)
	h := openDenyAll(t, descriptor)
	// The fixture's own precondition. A share mode of 0 is what makes every
	// later open fail, and a filesystem that ignored it would leave this test
	// asserting that an unobstructed open succeeds.
	if f, derr := os.Open(descriptor); derr == nil {
		f.Close() // nolint: errcheck
		syscall.CloseHandle(h)
		t.Fatal("the exclusive handle did not deny access, so nothing below is a test")
	}

	released := make(chan struct{})
	go func() {
		time.Sleep(hold)
		syscall.CloseHandle(h) // nolint: errcheck
		close(released)
	}()

	start := time.Now()
	l2, err := New(Options{Path: dir, MaxSegmentBytes: 1 << 20, CleanerInterval: time.Hour})
	elapsed := time.Since(start)
	require.NoError(t, err,
		"a transient handle on the descriptor must be waited out, not fail the open")
	// An open that returned before the handle cleared never met it: the read
	// happened after the window closed, and the retry this test is named for
	// was not exercised. Reported as a failure rather than a pass, because the
	// pass is indistinguishable from the real thing.
	require.GreaterOrEqual(t, elapsed, hold-50*time.Millisecond,
		"New() returned in %s while the descriptor was held for %s — it reached the "+
			"read after the window closed, so the retry was never exercised", elapsed, hold)
	<-released
	require.NoError(t, l2.Close())
	remove(t, dir)
}

// Publishing a key digest rides out a transient handle on the DESTINATION.
//
// This is the publisher's end of the window openWithRetry covers from the
// reader's end, and the digest is the one publish on that path with a budget of
// its own: a lost digest is free, because every caller rebuilds it from the
// segment when it is absent, so it takes the tick's 500ms rather than the five
// seconds a waiting caller gets. The hold therefore has to fit inside 500ms and
// still outlast the encode-and-write that precedes the rename, which is why it
// is measured from an unheld publish rather than picked.
//
// Before this the rename was bare. A scanner holding the previous digest open —
// or a reader of it that has not been reaped — failed the publish outright, and
// the pass then rebuilt the digest by walking the whole segment on its next
// tick, having just walked it to produce the one it could not install.
func TestPublishingADigestRidesOutAHeldDestination(t *testing.T) {
	l, app := specLog(t)
	for i := 0; i < 6; i++ {
		app(&Message{Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("v")})
	}
	l.mu.RLock()
	seg := l.segments[0]
	l.mu.RUnlock()

	d, err := buildKeyDigest(seg, newBlockCache())
	require.NoError(t, err)

	priced := time.Now()
	require.NoError(t, writeKeyDigest(seg, d), "the digest must exist before it can be held")
	hold := 2*time.Since(priced) + 100*time.Millisecond
	require.Less(t, hold, tickWriteRetryBudget,
		"the hold must sit inside the digest's own budget, or a correct retry gives up first")

	h := openDenyAll(t, digestPath(seg))
	released := make(chan struct{})
	go func() {
		time.Sleep(hold)
		syscall.CloseHandle(h) // nolint: errcheck
		close(released)
	}()

	start := time.Now()
	err = writeKeyDigest(seg, d)
	elapsed := time.Since(start)
	require.NoError(t, err,
		"a transient handle on the digest must be retried through, not fail the publish")
	// See TestReopeningALogRidesOutAHeldDescriptor: a publish that returned
	// before the handle cleared never met it, and a fixture that missed its own
	// window passes exactly like a working retry.
	require.GreaterOrEqual(t, elapsed, hold-50*time.Millisecond,
		"writeKeyDigest returned in %s while the destination was held for %s — it "+
			"reached the rename after the window closed", elapsed, hold)
	<-released
}

// SyncAll rides out a handle that outlives the TICK's budget.
//
// This is the case a single budget could not express. The checkpoint write is
// shared between the HW checkpoint tick and SyncAll, and only one of them has
// anything behind it: a lost tick is retried by the next tick, while SyncAll is
// a durability barrier a caller invoked and is waiting on. Bounding both at the
// tick's 500ms turned a transient Windows handle — exactly what the retry
// exists to ride out — into a failed stream creation in durable_streams,
// surfacing as "cannot replace ...replication-offset-checkpoint...: Access is
// denied" out of a user-facing operation.
//
// The hold is deliberately longer than tickWriteRetryBudget and well inside
// waitedOnRetryBudget: a suite whose only handle clears in 120ms cannot see a
// budget that is too short for the caller holding it.
//
// It sits MIDWAY between the two rather than just past the lower one, and the
// margin is what this test is actually made of. The sleep below starts when the
// goroutine is scheduled; the retry's deadline starts when checkpointHW is
// reached, and SyncAll fsyncs every segment first. Everything in between comes
// out of the margin. At tickWriteRetryBudget+250ms the Windows runner spent all
// 250ms on those fsyncs, so under the guard's mutation — SyncAll given the
// tick's budget — the handle was already gone by the shortened deadline, the
// call succeeded, and guardcheck reported the guard as NO COVERAGE. Green on
// every other runner, on a test that had not changed.
//
// Deriving it from both budgets rather than picking a number also means it
// tracks if either one moves, and there is no interval left to pick badly.
func TestSyncAllRidesOutAHandleTheTickWouldGiveUpOn(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:                 tempDir(t),
		MaxSegmentBytes:      1 << 20,
		HWCheckpointInterval: time.Hour, // no background tick racing this
		CleanerInterval:      time.Hour,
	})
	defer cleanup()

	_, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("v")}})
	require.NoError(t, err)
	l.SetHighWatermark(0)
	require.NoError(t, l.SyncAll(), "the checkpoint file must exist before it can be held")

	hold := (tickWriteRetryBudget + waitedOnRetryBudget) / 2
	require.Greater(t, hold, tickWriteRetryBudget, "the fixture must sit BETWEEN the two budgets")
	require.Less(t, hold, waitedOnRetryBudget, "the fixture must sit BETWEEN the two budgets")

	h := openDenyAll(t, filepath.Join(l.Path, hwFileName))
	released := make(chan struct{})
	var closedAt time.Time
	go func() {
		time.Sleep(hold)
		closedAt = time.Now()
		syscall.CloseHandle(h) // nolint: errcheck
		close(released)
	}()

	require.NoError(t, l.SyncAll(),
		"a barrier with nothing behind it must wait the handle out, not fail the caller")
	returned := time.Now()

	// The margin's own assertion, and the reason it is here rather than left
	// implicit: "SyncAll returned nil" is also what a SyncAll that never met the
	// held handle returns. Only the ordering separates waiting it out from
	// missing it. Reading closedAt after the receive is what orders the write.
	<-released
	require.True(t, returned.After(closedAt),
		"SyncAll returned %s before the handle was released, so it never waited on it — "+
			"the fixture is racing the fsyncs, not the budget", closedAt.Sub(returned))
}
