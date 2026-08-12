package commitlog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Append fills a zero Timestamp in on the CALLER'S Message, not on a copy of it.
//
// TestAppendStampsMissingTimestamps already covers what lands on DISK. This
// covers the other half — that the caller can read the stamp back off the struct
// it passed in — which nothing asserted, and which a refactor that stamped a
// copy would break silently while leaving every on-disk assertion green.
//
// It is worth a test because mutating a caller-owned struct is the surprising
// direction. The v0.70.0 sweep found the field block next to this one
// documented backwards — Offset and LeaderEpoch described as "filled in by the
// log on the way out" when nothing ever wrote either — so the one field that
// really is written back should have something holding it there.
func TestAppendStampsTheCallersMessage(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t), MaxSegmentBytes: 1 << 20})
	defer l.Close()
	defer cleanup()

	unstamped := &Message{Value: []byte("no timestamp of its own")}
	explicit := &Message{Value: []byte("brings its own"), Timestamp: 12345}

	offsets, err := l.Append([]*Message{unstamped, explicit})
	require.NoError(t, err)
	require.Len(t, offsets, 2)

	require.NotZero(t, unstamped.Timestamp,
		"Append must stamp the caller's own Message, not a copy of it")
	require.Equal(t, int64(12345), explicit.Timestamp,
		"a timestamp the caller supplied must survive untouched")

	// And what it wrote back is what it wrote down: a stamp the caller cannot
	// trust against the record is not worth writing back.
	l.SetHighWatermark(offsets[1])
	r, err := l.NewReader(From(offsets[0]), Uncommitted())
	require.NoError(t, err)
	headers := make([]byte, HeaderBufferLen)

	_, _, ts, _, err := r.ReadMessage(context.Background(), headers)
	require.NoError(t, err)
	require.Equal(t, unstamped.Timestamp, ts,
		"the stamp handed back to the caller must be the one on disk")

	_, _, ts, _, err = r.ReadMessage(context.Background(), headers)
	require.NoError(t, err)
	require.Equal(t, int64(12345), ts)
}
