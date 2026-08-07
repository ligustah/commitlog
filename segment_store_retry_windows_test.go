//go:build windows

package commitlog

import (
	"bytes"
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
	released := make(chan struct{})
	go func() {
		time.Sleep(300 * time.Millisecond)
		syscall.CloseHandle(h) // nolint: errcheck
		close(released)
	}()

	size, err := store.Size(key)
	require.NoError(t, err, "Size gave up on a handle that clears in 300ms")
	require.EqualValues(t, len(payload), size)

	buf := make([]byte, len(payload))
	_, err = store.ReadAt(key, buf, 0)
	require.NoError(t, err, "ReadAt gave up on a handle that clears in 300ms")
	require.Equal(t, payload, buf)

	<-released
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
	released := make(chan struct{})
	go func() {
		time.Sleep(300 * time.Millisecond)
		syscall.CloseHandle(h) // nolint: errcheck
		close(released)
	}()

	second := []byte("second")
	require.NoError(t, store.Put(key, bytes.NewReader(second), int64(len(second))),
		"the publish gave up on a handle that clears in 300ms")
	<-released

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
