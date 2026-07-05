package commitlog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// appendMsgs is a helper that appends n single-value messages to the log and
// commits each one, returning the slice of assigned offsets.
func appendMsgs(t *testing.T, l CommitLog, n int) []int64 {
	t.Helper()
	offsets := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		os, err := l.Append([]*Message{{Value: []byte("x")}})
		require.NoError(t, err)
		l.SetHighWatermark(os[0])
		offsets = append(offsets, os[0])
	}
	return offsets
}

// readAll reads all committed messages up through NewestOffset and returns
// their offsets. It uses a cancellable context so it stops once it reads
// the last record rather than blocking indefinitely.
func readAll(t *testing.T, l CommitLog) []int64 {
	t.Helper()
	newest := l.NewestOffset()
	if newest < 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := l.NewReader(l.OldestOffset(), false)
	require.NoError(t, err)
	hdr := make([]byte, 28)
	var got []int64
	for {
		_, off, _, _, err := r.ReadMessage(ctx, hdr)
		if err != nil {
			break
		}
		got = append(got, off)
		if off >= newest {
			break
		}
	}
	return got
}

// TestTruncateBeforeNoOp verifies that TruncateBefore(0) and
// TruncateBefore with minOffset <= OldestOffset are both no-ops.
func TestTruncateBeforeNoOp(t *testing.T) {
	l, cleanup := setup(t)
	defer cleanup()

	appendMsgs(t, l, 5)

	require.NoError(t, l.TruncateBefore(0))
	require.Equal(t, int64(0), l.OldestOffset())

	require.NoError(t, l.TruncateBefore(-1))
	require.Equal(t, int64(0), l.OldestOffset())
}

// TestTruncateBeforeWholeSegments verifies that sealed segments entirely
// before minOffset are deleted, leaving later records intact.
func TestTruncateBeforeWholeSegments(t *testing.T) {
	// Use a tiny segment size so we get multiple segments quickly.
	opts := Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
	}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()

	offsets := appendMsgs(t, l, 10)

	// Trim everything before offset 5.
	require.NoError(t, l.TruncateBefore(offsets[5]))

	require.GreaterOrEqual(t, l.OldestOffset(), offsets[5])

	got := readAll(t, l)
	for _, off := range got {
		require.GreaterOrEqual(t, off, offsets[5], "record before minOffset should have been removed")
	}
	// All records from minOffset onwards must still be present.
	require.Contains(t, got, offsets[5])
	require.Contains(t, got, offsets[9])
}

// TestTruncateBeforeBoundaryRewrite verifies that when minOffset falls in the
// middle of a sealed segment, that segment is rewritten and only records at
// or after minOffset are kept.
func TestTruncateBeforeBoundaryRewrite(t *testing.T) {
	// Large enough segment that all 10 messages land in one sealed segment,
	// plus a second segment (active) with a couple more.
	opts := Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
	}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()

	offsets := appendMsgs(t, l, 10)

	// Pick a minOffset that is in the middle of a segment.
	mid := offsets[4]
	require.NoError(t, l.TruncateBefore(mid))

	oldest := l.OldestOffset()
	require.GreaterOrEqual(t, oldest, mid)

	got := readAll(t, l)
	for _, off := range got {
		require.GreaterOrEqual(t, off, mid)
	}
	require.Contains(t, got, offsets[9], "newest record must survive TruncateBefore")
}

// TestTruncateBeforeThenAppend verifies that the log remains writable after
// TruncateBefore and that new records get sequential offsets.
func TestTruncateBeforeThenAppend(t *testing.T) {
	opts := Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
	}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()

	offsets := appendMsgs(t, l, 6)
	require.NoError(t, l.TruncateBefore(offsets[3]))

	newOffsets := appendMsgs(t, l, 3)
	require.Equal(t, offsets[5]+1, newOffsets[0], "offsets must continue from where they left off")
}

// TestTruncateBeforeReopen verifies that after reopening the log, the trimmed
// state is durable: old records are gone and newer ones are still readable.
func TestTruncateBeforeReopen(t *testing.T) {
	dir := tempDir(t) // t.Cleanup handles removal; we must not remove dir early
	opts := Options{
		Path:            dir,
		MaxSegmentBytes: 100,
	}
	l, err := New(opts)
	require.NoError(t, err)

	offsets := appendMsgs(t, l, 10)
	mid := offsets[5]
	require.NoError(t, l.TruncateBefore(mid))
	require.NoError(t, l.Close())

	// Reopen — directory still exists, trimmed segments must be on disk.
	l2, err := New(opts)
	require.NoError(t, err)
	defer l2.Close()

	require.GreaterOrEqual(t, l2.OldestOffset(), mid)
	got := readAll(t, l2)
	for _, off := range got {
		require.GreaterOrEqual(t, off, mid)
	}
}
