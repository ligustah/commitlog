package commitlog

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Absent must stay instantly distinguishable from locked.
//
// This is the half of the retry that is easy to lose: a read that waits out its
// whole bound before reporting a missing file turns every legitimate absence
// into a stall. A log with no checkpoint yet, an unwritten sidecar and a first
// open are all normal states, not races, and the caller learns nothing by
// waiting. The retry exists for a handle that will clear, and a file that is not
// there has no handle to clear.
func TestAReadOfAMissingFileDoesNotWaitOutTheRetryBound(t *testing.T) {
	dir := tempDir(t)
	defer remove(t, dir)

	start := time.Now()
	_, err := ReadFileWithRetry(filepath.Join(dir, "no-such-file"))
	elapsed := time.Since(start)

	require.Error(t, err)
	require.True(t, os.IsNotExist(err),
		"a missing file must still report as missing, got %v", err)

	// Anything near the full bound means the absence was retried rather than
	// returned.
	require.Less(t, elapsed, waitedOnRetryBudget/10,
		"a missing file was retried for %s; the retry bound is %s", elapsed, waitedOnRetryBudget)
}

// The ordinary path still works: a readable file comes back on the first try,
// with its bytes intact.
func TestAReadableFileIsReturnedUnchanged(t *testing.T) {
	dir := tempDir(t)
	defer remove(t, dir)

	path := filepath.Join(dir, "checkpoint")
	want := []byte("1234567890")
	require.NoError(t, os.WriteFile(path, want, 0o644))

	got, err := ReadFileWithRetry(path)
	require.NoError(t, err)
	require.Equal(t, want, got)
}
