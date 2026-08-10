//go:build windows

package commitlog

import (
	"bytes"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The deterministic half of TestAStoreReadSurvivesAConcurrentPublish.
//
// That test races a reader against a real publisher, which is the honest shape
// of the bug but reproduces only probabilistically — with the retry removed it
// failed 7 runs in 8, and a guard that reports itself uncovered one run in eight
// is not a guard. This one takes the exclusive handle by hand, so the window is
// open for as long as the test says and removing either retry fails it every
// time.
//
// Size is asserted here but is NOT retried, and that asymmetry is the finding
// rather than an oversight. readTierManifest sizes the manifest and then reads
// it, so symmetry argued for retrying both — but os.Stat goes through
// GetFileAttributesEx, which does not open a handle and is not refused by one.
// Neither this deny-all handle nor the racing test could make Size fail, and
// guardcheck duly reported the statWithRetry it had as uncovered. It was
// removed. The assertion stays as the record of what does not need it.
func TestAStoreReadRetriesThroughAHeldObject(t *testing.T) {
	store, err := NewFileSegmentStore(tempDir(t))
	require.NoError(t, err)

	const key = "manifest"
	payload := []byte("tier-manifest-bytes")
	require.NoError(t, store.Put(key, bytes.NewReader(payload), int64(len(payload))))

	path, err := store.objectPath(key)
	require.NoError(t, err)

	h := openDenyAll(t, path)

	// Prove the handle denies on THIS machine before anything depends on it. A
	// runner where it does not would pass every assertion below for the wrong
	// reason, and pass them just as happily with the retry removed.
	if f, oerr := os.Open(path); oerr == nil {
		f.Close()              // nolint: errcheck
		syscall.CloseHandle(h) // nolint: errcheck
		t.Fatal("the exclusive handle did not deny an open; this test proves nothing")
	}

	// Asserted while the handle is held, and deliberately not retried.
	size, err := store.Size(key)
	require.NoError(t, err, "Size was refused by a handle that does not refuse it")
	require.EqualValues(t, len(payload), size)

	// The release is timed from the moment the READ starts, not from the moment
	// the handle was taken. Timed from the handle, everything between the two —
	// the open probe, Size, two testify assertions — comes out of the window,
	// and on a slow runner it can consume all of it: the read then begins after
	// the handle is already gone and succeeds without retrying. That is exactly
	// how this guard reported NO COVERAGE on CI while passing locally.
	buf := make([]byte, len(payload))
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		close(started)
		_, rerr := store.ReadAt(key, buf, 0)
		result <- rerr
	}()

	<-started
	time.Sleep(300 * time.Millisecond) // well inside readRetryBudget
	require.NoError(t, syscall.CloseHandle(h))

	require.NoError(t, <-result, "ReadAt gave up on a handle that cleared 300ms in")
	require.Equal(t, payload, buf)
}

// The publish side of the same window. A reader holding the DESTINATION open
// makes the rename that commits an object fail outright, so retrying only the
// readers moves the error to the publisher instead of removing it.
func TestAStorePublishRetriesThroughAHeldDestination(t *testing.T) {
	store, err := NewFileSegmentStore(tempDir(t))
	require.NoError(t, err)

	const key = "manifest"
	first := []byte("first")
	require.NoError(t, store.Put(key, bytes.NewReader(first), int64(len(first))))

	path, err := store.objectPath(key)
	require.NoError(t, err)

	h := openDenyAll(t, path)
	if f, oerr := os.Open(path); oerr == nil {
		f.Close()              // nolint: errcheck
		syscall.CloseHandle(h) // nolint: errcheck
		t.Fatal("the exclusive handle did not deny an open; this test proves nothing")
	}

	// Timed from the start of the publish, for the reason given on the read
	// test, and shorter than the read's hold because the write side's budget is
	// atomicWriteRetryBudget rather than readRetryBudget.
	second := []byte("second")
	result := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		result <- store.Put(key, bytes.NewReader(second), int64(len(second)))
	}()

	<-started
	time.Sleep(150 * time.Millisecond) // well inside atomicWriteRetryBudget
	require.NoError(t, syscall.CloseHandle(h))

	require.NoError(t, <-result, "the publish gave up on a handle that cleared 150ms in")

	// The retry must have COMMITTED, not merely returned nil.
	size, err := store.Size(key)
	require.NoError(t, err)
	require.EqualValues(t, len(second), size, "the publish reported success without replacing the object")
}

// A handle that never clears must still fail, and within the bound: an object
// this store genuinely cannot read is not something to hide behind an unbounded
// wait. The twin of TestARecoveryReadOfAPermanentlyHeldFileStillFails.
func TestAStoreReadOfAPermanentlyHeldObjectStillFails(t *testing.T) {
	store, err := NewFileSegmentStore(tempDir(t))
	require.NoError(t, err)

	const key = "manifest"
	payload := []byte("held")
	require.NoError(t, store.Put(key, bytes.NewReader(payload), int64(len(payload))))

	path, err := store.objectPath(key)
	require.NoError(t, err)

	h := openDenyAll(t, path)

	start := time.Now()
	_, rerr := store.ReadAt(key, make([]byte, len(payload)), 0)
	elapsed := time.Since(start)

	// Release before asserting: the deny-all handle outlives a failed require.
	require.NoError(t, syscall.CloseHandle(h))

	require.Error(t, rerr, "a permanently held object must fail, not be hidden")
	require.Less(t, elapsed, 2*readRetryBudget,
		"the retry must terminate on its budget (%s), took %s", readRetryBudget, elapsed)
}
