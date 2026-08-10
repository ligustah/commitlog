//go:build windows

package commitlog

import (
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The read-side twin of TestAtomicWriteRetriesThroughTransientHandle.
//
// A log recovers immediately after the previous process died, and on Windows
// that process's handles are not closed when TerminateProcess returns — the OS
// reclaims them asynchronously. An open in that window fails with
// ERROR_SHARING_VIOLATION, and open()'s high-watermark read had no retry, so the
// whole log failed to open. Reported from sqlcdc as one daemon restart in ~30
// that did not come up at all: "read high watermark file failed: ... The process
// cannot access the file because it is being used by another process."
func TestRecoveryReadsRetryThroughTransientHandle(t *testing.T) {
	dir := tempDir(t)

	// Deliberately NOT setupWithOptions' cleanup: that removes the directory,
	// and this test needs the closed log's files to still be on disk.
	l, err := New(Options{Path: dir, DisableAutoClean: true})
	require.NoError(t, err)
	var last int64
	for n := 0; n < 8; n++ {
		offs, aerr := l.Append([]*Message{{Key: []byte("k"), Value: []byte("v")}})
		require.NoError(t, aerr)
		last = offs[len(offs)-1]
	}
	l.SetHighWatermark(last)
	require.NoError(t, l.Close())

	hwPath := filepath.Join(dir, hwFileName)
	// Prove the checkpoint is actually there to be contended for; otherwise this
	// test could pass by reading nothing.
	raw, err := ReadFileWithRetry(hwPath)
	require.NoError(t, err, "the closed log left no %s to contend for", hwFileName)
	persisted, err := strconv.ParseInt(string(raw), 10, 64)
	require.NoError(t, err)

	h := openDenyAll(t, hwPath)
	released := make(chan struct{})
	go func() {
		// Deliberately longer than the 500ms the bound used to be, so this test
		// exercises the budget it now has rather than the one it happened to
		// fit inside. sqlcdc's soak lost restarts in exactly this gap.
		time.Sleep(1200 * time.Millisecond)
		syscall.CloseHandle(h) // nolint: errcheck
		close(released)
	}()

	reopened, reopenCleanup := setupWithOptions(t, Options{Path: dir, DisableAutoClean: true})
	defer reopenCleanup()
	<-released

	require.Equal(t, persisted, reopened.HighWatermark(),
		"the log opened, but did not recover the watermark it was holding")
}

// A handle that never clears must still fail, and within the bound. A file this
// log genuinely cannot read is not something to hide behind an unbounded wait.
func TestARecoveryReadOfAPermanentlyHeldFileStillFails(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "held")
	require.NoError(t, AtomicWriteFileWithRetry(path, strings.NewReader("held")))

	h := openDenyAll(t, path)

	start := time.Now()
	_, err := ReadFileWithRetry(path)
	elapsed := time.Since(start)

	// Release before asserting: the deny-all handle outlives a failed require.
	require.NoError(t, syscall.CloseHandle(h))

	require.Error(t, err, "a permanently held file must fail, not be hidden")
	require.Less(t, elapsed, 2*waitedOnRetryBudget,
		"the retry must terminate on its budget (%s), took %s", waitedOnRetryBudget, elapsed)
}
