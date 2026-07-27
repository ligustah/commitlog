package commitlog

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// readFrom returns every record from oldest to newest, keyed by offset.
func readFrom(t *testing.T, l CommitLog) map[int64]string {
	t.Helper()
	out := map[int64]string{}
	oldest := l.OldestOffset()
	if oldest < 0 {
		return out
	}
	r, err := l.NewReader(oldest, true)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	headers := make([]byte, 28)
	newest := l.NewestOffset()
	for {
		msg, off, _, _, err := r.ReadMessage(ctx, headers)
		require.NoError(t, err)
		out[off] = string(SerializedMessage(msg).Value())
		if off >= newest {
			return out
		}
	}
}

// WHAT THIS DOES NOT PROVE, stated first because the obvious reading is wrong:
// it does not verify that Sync makes anything durable. A reopen inside the same
// OS reads through the page cache, so it sees writes that never reached the
// disk Ã¢â‚¬â€ this test passes with Sync replaced by `return nil`, which was checked
// rather than assumed. Durability against power loss cannot be observed from
// inside the process at all; the closest available evidence is that the right
// backings get fsynced, which TestSyncFsyncsDirtySegmentsOnly asserts by
// counting.
//
// What it DOES pin down is still worth having: that a log which has been synced
// and then reopened WITHOUT a clean Close reads back complete and in order
// through the public API Ã¢â‚¬â€ no record dropped, no offset renumbered, no torn
// frame Ã¢â‚¬â€ across a segment roll. That is recovery behaviour, not durability,
// and the distinction matters enough to keep it in the name and the comment.
//
// The log is deliberately not closed before reopening, since Close flushes and
// would hide exactly the case of interest.
//
// The cost of that stand-in is that neither log can then shut down cleanly on
// Windows Ã¢â‚¬â€ closing an index truncates it, which fails while any other mapping
// of the file is open Ã¢â‚¬â€ so teardown here ignores close errors. That is an
// artifact of two logs sharing one directory, which real callers never do; it
// says nothing about the durability being asserted.
func TestSyncedLogReopensCompleteWithoutClose(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{Path: dir, MaxSegmentBytes: 256})
	require.NoError(t, err)

	want := map[int64]string{}
	for i := 0; i < 40; i++ {
		v := fmt.Sprintf("v%d", i)
		offs, err := l.Append([]*Message{{Key: []byte{byte(i)}, Value: []byte(v)}})
		require.NoError(t, err)
		want[offs[0]] = v
		l.SetHighWatermark(offs[0])
	}

	require.NoError(t, l.Sync(l.NewestOffset()), "the durability barrier")

	// Reopen from the bytes on disk, without closing the original.
	l2, err := New(Options{Path: dir, MaxSegmentBytes: 256})
	require.NoError(t, err)
	t.Cleanup(func() { l2.Close(); l.Close() }) // nolint: errcheck Ã¢â‚¬â€ see above
	require.NoError(t, l2.(*commitLog).RecoverTail())

	got := readFrom(t, l2)
	for off, v := range want {
		require.Equal(t, v, got[off],
			"record at offset %d was appended before Sync and must survive", off)
	}
}

// Records appended AFTER an earlier Sync must still be there on reopen. Dirty
// tracking is what could plausibly break this Ã¢â‚¬â€ a segment marked clean by the
// first sync and then skipped by the second while holding newer records.
//
// Same caveat as above: the page cache means this cannot detect a missing
// fsync, only a record that went missing outright. The fsync-level assertion
// for this case is the syncs-per-segment count in TestSyncSkipsCleanSegment.
func TestRecordsAppendedAfterASyncSurviveReopen(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{Path: dir, MaxSegmentBytes: 1 << 20}) // one segment
	require.NoError(t, err)

	first, err := l.Append([]*Message{{Key: []byte("a"), Value: []byte("first")}})
	require.NoError(t, err)
	l.SetHighWatermark(first[0])
	require.NoError(t, l.Sync(l.NewestOffset()))

	// Appended after the segment was marked clean by the sync above.
	second, err := l.Append([]*Message{{Key: []byte("b"), Value: []byte("second")}})
	require.NoError(t, err)
	l.SetHighWatermark(second[0])
	require.NoError(t, l.Sync(l.NewestOffset()))

	l2, err := New(Options{Path: dir, MaxSegmentBytes: 1 << 20})
	require.NoError(t, err)
	t.Cleanup(func() { l2.Close(); l.Close() }) // nolint: errcheck Ã¢â‚¬â€ see above
	require.NoError(t, l2.(*commitLog).RecoverTail())

	got := readFrom(t, l2)
	require.Equal(t, "first", got[first[0]])
	require.Equal(t, "second", got[second[0]],
		"a record appended after an earlier Sync must still be flushed by the next one")
}
