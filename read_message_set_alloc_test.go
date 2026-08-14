package commitlog

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// THE BUFFER IS SIZED BY WHAT IS IN THE SEGMENT, NOT BY WHAT THE CALLER ASKED FOR.
//
// maxBytes is a ceiling on what the caller is willing to receive. It says nothing
// about how much there is to send, and on the path this function exists for the
// two are nowhere near each other: a follower caught up to the head asks for its
// whole fetch size every time and gets back the one frame that just landed.
// Pre-allocating maxBytes allocated the entire fetch size on every such call.
//
// The assertion is on cap() rather than on a MemStats delta because the slice IS
// the allocation — ReadMessageSet returns `out` itself, so its capacity is
// exactly the number under test, with no sampling and nothing to make it flaky.
func TestReadMessageSetSizesItsBufferToTheSegmentNotTheBudget(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path: tempDir(t), MaxSegmentBytes: 1 << 20,
	})
	defer cleanup()

	for i := 0; i < 8; i++ {
		_, err := l.Append([]*Message{{Value: []byte(fmt.Sprintf("v%d", i))}})
		require.NoError(t, err)
	}
	l.SetHighWatermark(l.NewestOffset())

	extent := l.activeSegment().Position()

	// A follower's fetch size against a log holding a few hundred bytes. The
	// premise of the test, asserted rather than assumed: the budget really is far
	// above the extent, so the two measures cannot both be right.
	const budget = 32 << 20
	require.Less(t, extent, int64(budget/16),
		"the segment is not small next to the budget, so nothing distinguishes the two")

	got, err := l.ReadMessageSet(0, budget)
	require.NoError(t, err)
	require.NotEmpty(t, got)

	require.LessOrEqual(t, int64(cap(got)), extent,
		"the caller's %d-byte budget was allocated for a %d-byte segment", budget, extent)
}

// AND THE OTHER SIDE OF THE SAME min(): A BUDGET BELOW THE SEGMENT STILL BINDS.
//
// The fix is a minimum of two bounds and each side of it can be dropped on its
// own, so each side needs a fixture that can see it. This is the inverse case —
// a large segment and a small fetch — where sizing by the extent alone is the
// worse of the two mistakes: a follower asking for 64 bytes of a gigabyte
// segment would allocate the gigabyte.
//
// The cap() assertion is what makes that visible. Asserting only on len() would
// prove nothing here, because the truncation is done by the loop's own maxBytes
// break and not by the buffer's size — a fix that dropped this clamp entirely
// would still return the right BYTES, just after taking the whole segment's
// worth of memory to do it.
func TestReadMessageSetStillHonoursABudgetBelowTheSegment(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path: tempDir(t), MaxSegmentBytes: 1 << 24,
	})
	defer cleanup()

	for i := 0; i < 500; i++ {
		_, err := l.Append([]*Message{{
			Value: []byte(fmt.Sprintf("value-%03d-%s", i, "padding to make the segment worth measuring")),
		}})
		require.NoError(t, err)
	}
	l.SetHighWatermark(l.NewestOffset())

	extent := l.activeSegment().Position()
	budget := int(extent / 8)
	require.Positive(t, budget)

	got, err := l.ReadMessageSet(0, budget)
	require.NoError(t, err)
	require.NotEmpty(t, got, "a follower must always make progress")
	require.LessOrEqual(t, len(got), budget,
		"the budget is the smaller of the two bounds and must be the one that binds")
	require.LessOrEqual(t, int64(cap(got)), int64(budget),
		"the whole %d-byte segment was allocated to answer a %d-byte fetch", extent, budget)

	// And the whole segment when the budget is the larger bound: the extent must
	// size the buffer without also truncating the answer.
	all, err := l.ReadMessageSet(0, int(extent)*4)
	require.NoError(t, err)
	require.EqualValues(t, extent, len(all),
		"every framed byte in the segment must come back")
}
