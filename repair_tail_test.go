package commitlog

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// RepairTail truncates the torn suffix and publishes NOTHING.
//
// This is the half a replica needs. Its log legitimately holds records above
// the commit boundary -- a follower fetches before they commit, a leader writes
// before they reach the in-sync set -- so RecoverTail's watermark raise turns an
// ordinary restart into a promise about records no other node holds. The node
// then refuses a shorter leader's truncation, correctly, and the partition
// wedges.
//
// The fixture is deliberately the SAME shape as
// TestRecoverTailTruncatesTornSuffix, so the two differ in exactly one
// observable: where the watermark ends up.
func TestRepairTailTruncatesTheTornSuffixWithoutPublishingIt(t *testing.T) {
	dir := t.TempDir()
	l, err := New(Options{Path: dir}) // single segment
	require.NoError(t, err)
	for i := 0; i < 6; i++ {
		_, err := l.Append([]*Message{{Key: []byte{byte(i)}, Value: []byte("v")}})
		require.NoError(t, err)
	}
	l.SetHighWatermark(2)
	require.NoError(t, l.(*commitLog).checkpointHW(waitedOnRetryBudget))
	require.NoError(t, l.Close())

	// Tear the tail: append garbage bytes to the segment file.
	logs, err := filepath.Glob(filepath.Join(dir, "*.log"))
	require.NoError(t, err)
	f, err := os.OpenFile(logs[len(logs)-1], os.O_APPEND|os.O_WRONLY, 0666)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xDE, 0xAD, 0xBE})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	l2, err := New(Options{Path: dir})
	require.NoError(t, err)
	defer l2.Close()
	require.EqualValues(t, 2, l2.HighWatermark(), "fixture: reopen sees the stale checkpoint")

	tail, err := l2.(*commitLog).RepairTail()
	require.NoError(t, err)

	// The structural half happened: all six real records survive and the
	// garbage did not extend the tail.
	require.EqualValues(t, 5, tail, "RepairTail reports the tail it left behind")
	require.EqualValues(t, 5, l2.NewestOffset())

	// The policy half did NOT. This is the whole point of the method.
	require.EqualValues(t, 2, l2.HighWatermark(),
		"RepairTail must not publish records the caller has not decided are committed")

	// The records are readable as UNCOMMITTED, which is what a replica needs:
	// present in the log, not yet declared committed.
	r, err := l2.NewReader(From(0), Uncommitted(), Follow())
	require.NoError(t, err)
	headers := make([]byte, HeaderBufferLen)
	for i := 0; i <= 5; i++ {
		_, off, _, _, err := r.ReadMessage(context.Background(), headers)
		require.NoError(t, err)
		require.EqualValues(t, i, off)
	}

	// And the caller can still publish on its own terms, to its own bound --
	// the replication barrier, not the tail.
	l2.SetHighWatermark(4)
	require.EqualValues(t, 4, l2.HighWatermark())
}

// RecoverTail still publishes to the scanned tail. Pinned alongside the above
// because the split must not quietly change the single-node contract: a log
// that is its own authority still gets both halves from one call.
func TestRecoverTailStillPublishesToTheScannedTail(t *testing.T) {
	dir := t.TempDir()
	l, err := New(Options{Path: dir})
	require.NoError(t, err)
	for i := 0; i < 6; i++ {
		_, err := l.Append([]*Message{{Key: []byte{byte(i)}, Value: []byte("v")}})
		require.NoError(t, err)
	}
	l.SetHighWatermark(2)
	require.NoError(t, l.(*commitLog).checkpointHW(waitedOnRetryBudget))
	require.NoError(t, l.Close())

	l2, err := New(Options{Path: dir})
	require.NoError(t, err)
	defer l2.Close()
	require.NoError(t, l2.(*commitLog).RecoverTail())
	require.EqualValues(t, 5, l2.HighWatermark(),
		"the single-node contract is unchanged: RecoverTail extends to the real tail")
}
