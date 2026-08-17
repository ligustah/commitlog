package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A log trimmed to a non-zero base and then emptied disagrees with itself about
// whether it is empty: OldestOffset answers -1, NewestOffset answers base-1.
// NewestOffset is derived from the active segment, and an unwritten segment's
// next offset is its base, so there is no -1 left to report.
//
// This is documented on CommitLog.NewestOffset as a warning against using it as
// an emptiness test. The warning names a specific value, so it is pinned here —
// otherwise "returns -1 if empty" could be restored as an obvious cleanup and
// the callers that were told to ask OldestOffset would look paranoid.
func TestNewestOffsetOnEmptiedTrimmedLog(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:             tempDir(t),
		MaxSegmentBytes:  20,
		DisableAutoClean: true,
	})
	defer cleanup()
	defer l.Close()

	require.Equal(t, int64(-1), l.NewestOffset(), "a fresh log is the case the doc's -1 covers")

	appendMsgs(t, l, 5)
	require.NoError(t, l.TruncateBefore(3))

	base := l.OldestOffset()
	require.Greater(t, base, int64(0), "the trim must leave a non-zero base or this proves nothing")

	// Empty it from the other end. The log now holds no records at all.
	require.NoError(t, l.Truncate(base))

	require.Equal(t, int64(-1), l.OldestOffset(),
		"OldestOffset is the emptiness test the doc sends callers to")
	require.Equal(t, base-1, l.NewestOffset(),
		"NewestOffset reports base-1, an offset naming a record that is gone")
	require.GreaterOrEqual(t, l.NewestOffset(), int64(0),
		"and it is non-negative, so `NewestOffset() < 0` concludes the log has records")

	// The arithmetic the doc says stays sound: +1 is where the next append lands.
	next := l.NewestOffset() + 1
	offs, err := l.Append([]*Message{{Value: []byte("x")}})
	require.NoError(t, err)
	require.Equal(t, next, offs[0], "NewestOffset()+1 is the next append offset in this state too")
}
