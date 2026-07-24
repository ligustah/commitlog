//go:build windows

package commitlog

import (
	"bytes"
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

	require.NoError(t, atomicWriteWithRetry(path, bytes.NewReader([]byte("first"))))

	h := openDenyAll(t, path)
	released := make(chan struct{})
	go func() {
		time.Sleep(120 * time.Millisecond) // well inside the retry bound
		syscall.CloseHandle(h)             // nolint: errcheck
		close(released)
	}()

	// Must succeed despite the destination being locked when it starts.
	require.NoError(t, atomicWriteWithRetry(path, bytes.NewReader([]byte("second"))),
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
	require.NoError(t, atomicWriteWithRetry(path, bytes.NewReader([]byte("first"))))

	h := openDenyAll(t, path)

	start := time.Now()
	err := atomicWriteWithRetry(path, bytes.NewReader([]byte("second")))
	elapsed := time.Since(start)

	// Release before reading: the deny-all handle blocks this test too.
	require.NoError(t, syscall.CloseHandle(h))

	require.Error(t, err, "a permanently held destination must fail, not be hidden")
	require.Less(t, elapsed, 10*time.Second, "retry bound must terminate promptly")

	got, rerr := os.ReadFile(path)
	require.NoError(t, rerr)
	require.Equal(t, "first", string(got), "a failed write must leave the old contents")
}
