package commitlog

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Truncating a fully committed log all the way to EMPTY, and using it after.
//
// TestTruncatingBelowTheWatermarkClampsIt covers a cut that leaves records
// behind: the watermark lands on a real offset, in a segment that still exists,
// and everything resolves. This is the same path taken to its limit, where the
// clamp has no offset to land on and the watermark becomes -1.
//
// Not a hypothetical. sqlcdc's #465 trace is exactly this: replication called
// Truncate(0) on a __transaction_state log holding 97 committed records, and
// commitlog clamped `hw=97 newest=-1 truncated_at=0`. Truncate is deliberately
// not allowed to refuse that — an unclean election means discarding locally
// committed records, and refusing would break the case the call exists for — so
// the caller's decision to cut to zero is the caller's to make. What is this
// package's business is that the log it hands back still works.
//
// A watermark of -1 is the state a log has before anything is committed, and it
// has bitten here before: a committed reader built against a log with no high
// watermark reported "no segment to consume" rather than an empty read.
// Reaching it by TRUNCATION rather than by starting fresh is what is new, and
// the difference that could matter is that the segment files, the epoch cache
// and the checkpoint have all been through a life before arriving there.
func TestTruncatingToEmptyLeavesTheLogUsable(t *testing.T) {
	dir := tempDir(t)
	l, cleanup := setupWithOptions(t, Options{
		Path:            dir,
		MaxSegmentBytes: 256, // several segments, so the cut spans more than one
	})
	defer cleanup()

	const records = 97 // the count from the trace
	for i := range records {
		offs, err := l.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("k:%d", i)),
			Value: []byte(strings.Repeat("x", 48)),
		}})
		require.NoError(t, err)
		l.SetHighWatermark(offs[0])
	}
	require.EqualValues(t, records-1, l.NewestOffset())
	require.EqualValues(t, l.NewestOffset(), l.HighWatermark(),
		"the fixture needs a fully committed log, which is what makes the cut lossy")
	require.Greater(t, len(l.segmentsSnapshot()), 1, "the cut should span more than one segment")

	// The call from the trace, verbatim in shape: cut to 0 on a log where every
	// record is committed.
	require.NoError(t, l.Truncate(0))

	require.EqualValues(t, -1, l.NewestOffset(), "the log is not empty after a cut to 0")
	require.EqualValues(t, -1, l.HighWatermark(),
		"the watermark still names records the truncation removed; it cannot be "+
			"lowered by a caller, so this is where the log would be stuck")

	// The log has to keep taking writes, because that is what happens next in
	// production: the node re-replicates from the new leader.
	//
	// Offset 0, not 97: the records are gone and the offsets go with them. That
	// is what an unclean election costs, and asserting it here is what keeps a
	// "fix" that silently preserved the offsets from looking correct.
	offs, err := l.Append([]*Message{{Key: []byte("k:after"), Value: []byte("v:after")}})
	require.NoError(t, err, "the emptied log would not accept an append")
	require.EqualValues(t, 0, offs[0], "the emptied log did not restart its offsets")

	// And the watermark must be free to move again. SetHighWatermark is
	// monotonic, so had the clamp left it at 97 this is where the log would have
	// stopped: every subsequent commit silently ignored as going backwards.
	l.SetHighWatermark(offs[0])
	require.EqualValues(t, 0, l.HighWatermark(),
		"the watermark would not advance after being clamped to -1")

	// A committed reader must build and serve the re-replicated record. This is
	// the symptom the v0.44.1 fix was about, reached from the truncation side.
	r, err := l.NewReader(From(l.OldestOffset()))
	require.NoError(t, err, "no committed reader could be built after the log was emptied")

	// Bounded: the record is committed, so this must not block. A follow reader
	// that parks here is the failure, and an unbounded read would hang the suite
	// rather than report it.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, offset, _, _, err := r.ReadMessage(ctx, make([]byte, HeaderBufferLen))
	require.NoError(t, err, "the re-filled log served no committed records")
	require.EqualValues(t, 0, offset)
}
