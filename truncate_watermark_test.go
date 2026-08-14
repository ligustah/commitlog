package commitlog

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A truncation must not leave the watermark naming records it removed.
//
// Truncate is allowed to cut below the high watermark — a follower reconciling
// against a leader promoted from outside the ISR is told to discard records it
// had locally committed, which is what an unclean election means. What it may
// not do is finish with the watermark still pointing above the log.
//
// That state is not merely untidy. SetHighWatermark is monotonic, so a caller
// cannot bring the watermark back down; the watermark resolves through
// findSegment, which returns nil past the last segment, so every committed
// reader fails to build; and the log stays that way until it is reopened, where
// a checkpoint above the log is clamped. So the log recovered from this on
// restart and never in flight, and nothing was raised at the call that caused it.
func TestTruncateBelowTheWatermarkClampsIt(t *testing.T) {
	dir := tempDir(t)
	l, cleanup := setupWithOptions(t, Options{
		Path:            dir,
		MaxSegmentBytes: 256,
	})
	defer cleanup()

	for i := range 100 {
		offs, err := l.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("k:%d", i)),
			Value: []byte(strings.Repeat("x", 48)),
		}})
		require.NoError(t, err)
		l.SetHighWatermark(offs[0])
	}
	require.EqualValues(t, l.NewestOffset(), l.HighWatermark(),
		"test needs a fully committed log to cut into")

	cut := l.NewestOffset() - 40
	require.NoError(t, l.Truncate(cut))

	require.EqualValues(t, cut-1, l.NewestOffset(), "truncation did not remove the tail")
	require.EqualValues(t, l.NewestOffset(), l.HighWatermark(),
		"the watermark still names records the truncation removed")

	// The point of clamping, rather than a tidier number: the log keeps working.
	// Unclamped, this is where it stopped — no committed reader could be built
	// at all, because the watermark resolved to no segment.
	r, err := l.NewReader(From(l.OldestOffset()))
	require.NoError(t, err, "no committed reader could be built after the truncation")
	_, offset, _, _, err := r.ReadMessage(context.Background(), make([]byte, HeaderBufferLen))
	require.NoError(t, err, "the log served no committed records after the truncation")
	require.EqualValues(t, l.OldestOffset(), offset)

	// And it is still writable, with the watermark free to advance again.
	offs, err := l.Append([]*Message{{Key: []byte("k:after"), Value: []byte("v:after")}})
	require.NoError(t, err)
	l.SetHighWatermark(offs[0])
	require.EqualValues(t, offs[0], l.HighWatermark(),
		"the watermark would not advance after being clamped")
}

// The clamp must be exactly that, and not a rewrite of the watermark on every
// truncation. Cutting above the watermark removes only uncommitted records —
// the ordinary case, and the one durable_streams' open path uses as
// Truncate(HW+1) — so the watermark is still true and must be left alone.
func TestTruncateAboveTheWatermarkLeavesItAlone(t *testing.T) {
	dir := tempDir(t)
	l, cleanup := setupWithOptions(t, Options{
		Path:            dir,
		MaxSegmentBytes: 256,
	})
	defer cleanup()

	for i := range 100 {
		_, err := l.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("k:%d", i)),
			Value: []byte(strings.Repeat("x", 48)),
		}})
		require.NoError(t, err)
	}

	// Committed well short of the tail, so there is an uncommitted suffix to cut.
	committed := l.NewestOffset() - 30
	l.SetHighWatermark(committed)
	require.EqualValues(t, committed, l.HighWatermark())

	require.NoError(t, l.Truncate(committed+10))

	require.EqualValues(t, committed, l.HighWatermark(),
		"a truncation above the watermark moved it")
	require.EqualValues(t, committed+9, l.NewestOffset())
}
