package commitlog

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// A retention deletion that fails partway must leave a consistent log: the
// segments deleted so far are a pure OLDEST prefix (no holes), the surviving
// segments — failed one included — stay in the read path, and a later Clean
// finishes the job.
func TestDeleteCleanerPartialFailure(t *testing.T) {
	opts := Options{Path: tempDir(t), MaxSegmentBytes: 256}
	l, cleanup := setupWithOptions(t, opts)
	defer l.Close()
	defer cleanup()

	for i := 0; i < 30; i++ {
		_, err := l.Append([]*Message{{Value: []byte("value-" + strconv.Itoa(i))}})
		require.NoError(t, err)
	}
	l.SetHighWatermark(29)
	segs := l.Segments()
	require.GreaterOrEqual(t, len(segs), 4, "need several segments to drop")

	// Retention that keeps roughly the last two segments.
	cl := l
	cl.MaxLogBytes = segs[len(segs)-1].Position() + segs[len(segs)-2].Position()
	cl.deleteCleaner.Retention.Bytes = cl.MaxLogBytes

	// Fail the SECOND deletion.
	boom := errors.New("injected delete failure")
	calls := 0
	// Restore the REAL function, captured here — not a hand-written stand-in
	// that happens to look like it. A replacement calling s.Delete() directly
	// skips the writer fence, so every test running afterwards would exercise
	// an unfenced delete path and a fence regression would go unnoticed.
	restore := deleteSegment
	deleteSegment = func(s *segment, w string) error {
		calls++
		if calls == 2 {
			return boom
		}
		return restore(s, w)
	}
	defer func() { deleteSegment = restore }()

	err := l.Clean()
	require.ErrorIs(t, err, boom)

	// The surviving segments must be contiguous, start at the FAILED segment
	// (the first deletion succeeded), and agree with what a reader can reach.
	after := cl.Segments()
	require.Equal(t, segs[1].BaseOffset, after[0].BaseOffset,
		"survivors must start at the failed segment")
	for i := 1; i < len(after); i++ {
		require.Equal(t, after[i-1].NextOffset(), after[i].BaseOffset,
			"survivors must be contiguous")
	}
	require.Equal(t, after[0].FirstOffset(), l.OldestOffset())

	// Reading from the new oldest offset must work (no deleted file in path).
	r, err := l.NewReader(l.OldestOffset(), false)
	require.NoError(t, err)
	headers := make([]byte, 28)
	_, off, _, _, err := r.ReadMessage(context.Background(), headers)
	require.NoError(t, err)
	require.Equal(t, l.OldestOffset(), off)

	// With the fault cleared, the next Clean completes the retention.
	deleteSegment = restore
	require.NoError(t, l.Clean())
	final := cl.Segments()
	require.Len(t, final, 2, "retention should keep the last two segments")
}
