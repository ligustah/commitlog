package commitlog

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The retry bound is an amount of TIME, not a number of attempts.
//
// It used to be `i >= atomicWriteRetries`, 25 attempts 20ms apart. That is a
// bound on how many times you ask, and what the retry waits for — Windows
// reclaiming a dead process's handles after TerminateProcess returns — takes an
// amount of time that depends on what the machine is doing, not on how often it
// is asked. So the real bound was 500ms, a number that appeared nowhere and
// changed meaning silently with the delay constant. sqlcdc measured it losing 2
// of 86 daemon restarts in a 3h50m kill -9 soak on a loaded box.
//
// A directory stands in for the permanently unreadable file: it fails on every
// platform with something that is not os.ErrNotExist, which is exactly the class
// the retry keeps trying. So the read waits out the whole budget, and the elapsed
// time is the bound, observable without a platform-specific handle.
func TestTheReadRetryBoundIsATimeBudgetNotAnAttemptCount(t *testing.T) {
	restore := waitedOnRetryBudget
	waitedOnRetryBudget = 1500 * time.Millisecond
	t.Cleanup(func() { waitedOnRetryBudget = restore })

	dir := tempDir(t)
	defer remove(t, dir)

	start := time.Now()
	_, err := ReadFileWithRetry(dir)
	elapsed := time.Since(start)

	require.Error(t, err, "reading a directory must fail")
	require.False(t, os.IsNotExist(err),
		"the stand-in must be a retried error, not an absence, got %v", err)

	// An attempt count of 25 at 20ms would have given up after ~500ms, whatever
	// the budget said.
	require.GreaterOrEqual(t, elapsed, waitedOnRetryBudget-100*time.Millisecond,
		"the read gave up after %s; the budget is %s, so the bound is still "+
			"counting attempts rather than spending time", elapsed, waitedOnRetryBudget)
	require.Less(t, elapsed, 2*waitedOnRetryBudget,
		"the read overran its budget by more than double (%s)", elapsed)
}
