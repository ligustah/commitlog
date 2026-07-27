package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Reclamation is segment-granular: whole sealed segments below the floor are
// dropped and a boundary SEALED segment is rewritten, but the active segment
// never is. So a log whose records all still live in one active segment frees
// nothing and its oldest offset does not move — which is success, not failure.
//
// This pins the honest contract, because the interface used to promise
// "OldestOffset() >= minOffset after this call" while stating in the next
// breath that the active segment is never rewritten. A consumer gating
// retention on the oldest offset reaching its floor would wait forever.
func TestTruncateBeforeDoesNotMoveOldestWithinTheActiveSegment(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 1 << 20, // everything stays in one active segment
	})
	defer cleanup()

	var last int64
	for i := 0; i < 20; i++ {
		offs, err := l.Append([]*Message{{Value: []byte("v")}})
		require.NoError(t, err)
		last = offs[0]
	}
	require.EqualValues(t, 0, l.OldestOffset())

	floor := last - 5
	require.NoError(t, l.TruncateBefore(floor))

	require.Less(t, l.OldestOffset(), floor,
		"a single active segment cannot be reclaimed, so the oldest offset "+
			"stays put — callers must not gate on it reaching the floor")
	require.EqualValues(t, last, l.NewestOffset(), "nothing may be lost")
}

// Where there ARE sealed segments below the floor, they are reclaimed and the
// oldest offset does move — the guarantee holds in the direction that matters:
// nothing at or above the floor is ever discarded.
func TestTruncateBeforeReclaimsSealedSegments(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 64, // roll constantly, so there are sealed segments
	})
	defer cleanup()

	var last int64
	for i := 0; i < 40; i++ {
		offs, err := l.Append([]*Message{{Value: []byte("padding value")}})
		require.NoError(t, err)
		last = offs[0]
	}

	floor := last - 5
	require.NoError(t, l.TruncateBefore(floor))
	require.Greater(t, l.OldestOffset(), int64(0), "sealed segments must be reclaimed")
	require.LessOrEqual(t, l.OldestOffset(), floor,
		"nothing at or above the floor may be discarded")
	require.EqualValues(t, last, l.NewestOffset())
}
