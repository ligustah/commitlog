package commitlog

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The exported scan reader must terminate at the end of the data. A tailing
// reader (NewReader) parks there waiting for an append or a watermark that may
// never come, which is how a bounded sweep turns into a hang — the shape that
// stalled RecoverTail before v0.18.0 and that a consumer hit independently in
// its own abort scan.
func TestNewScanReaderStopsAtEndOfData(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t), MaxSegmentBytes: 4096})
	t.Cleanup(cleanup)

	for i := 0; i < 5; i++ {
		offs, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("v")}})
		require.NoError(t, err)
		// Deliberately leave the high watermark BELOW the tail: a committed
		// reader would refuse to go past it, and the scan reader must not care.
		if i == 0 {
			l.SetHighWatermark(offs[0])
		}
	}

	r, err := l.NewScanReader(0)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	hdr := make([]byte, 28)
	read := 0
	for {
		_, _, _, _, err := r.ReadMessage(ctx, hdr)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err, "scan reader failed before draining")
		read++
		require.LessOrEqual(t, read, 5, "scan reader returned more records than exist")
	}

	// All five are visible even though only the first is committed: the scan
	// reader is uncommitted by construction, and it ended rather than blocking.
	require.Equal(t, 5, read, "scan reader must see uncommitted records and stop after the last")

	// Reaching EOF must be terminal, not a pause that resumes on the next call.
	_, _, _, _, err = r.ReadMessage(ctx, hdr)
	require.True(t, errors.Is(err, io.EOF), "EOF must be sticky for a static scan, got %v", err)
}

// An offset with no segment behind it is refused at construction rather than
// handed back as a reader that immediately ends. The distinction is the point:
// "this range was dropped by retention" and "this range held no records" would
// otherwise be the same answer, and a rebuild that scanned nothing at all would
// look exactly like one that found nothing.
func TestNewScanReaderUnbackedOffsetIsRefused(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t), MaxSegmentBytes: 4096})
	t.Cleanup(cleanup)

	_, err := l.NewScanReader(0)
	require.ErrorIs(t, err, ErrSegmentNotFound, "an empty log must refuse the scan, not fake an empty one")

	// With data present the same call succeeds, so the refusal is about the
	// range and not about scan readers in general.
	offs, aerr := l.Append([]*Message{{Key: []byte("k"), Value: []byte("v")}})
	require.NoError(t, aerr)
	l.SetHighWatermark(offs[0])

	r, err := l.NewScanReader(0)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, off, _, _, rerr := r.ReadMessage(ctx, make([]byte, 28))
	require.NoError(t, rerr)
	require.Equal(t, int64(0), off)
}
