package commitlog

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A Follow reader parked at the true end of the log must be woken by a record
// appended AFTERWARDS.
//
// Prompted by durable_streams tracing a live-tail stall and observing that every
// test in their repo — and, checking, very nearly every test in this one —
// appends first and then consumes. TestReaderFollow already parks a reader and
// wakes it by advancing the high watermark, but all ten records existed on disk
// before the reader started: only the watermark moved. A consumer that stops
// delivering the moment it genuinely catches up would pass that test.
//
// This is the other case. Drain to the real end, park, then append. It is the
// shape every tailing consumer depends on and the one nothing here covered.
//
// Deliberately bounded by a timeout on the read: a stall must FAIL rather than
// hang the suite, because the failure being hunted is "never delivered", not
// "delivered wrongly".
func TestFollowReaderWokenByAnAppendAfterDraining(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t)})
	defer l.Close() // nolint: errcheck
	defer cleanup()

	offs, err := l.Append([]*Message{{Key: []byte("k0"), Value: []byte("first")}})
	require.NoError(t, err)
	l.SetHighWatermark(offs[0])

	r, err := l.NewReader(From(0), Follow())
	require.NoError(t, err)
	hdr := make([]byte, HeaderBufferLen)

	// Drain to the true end: after this the reader has everything the log holds.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	msg, off, _, _, err := r.ReadMessage(ctx, hdr)
	require.NoError(t, err)
	require.Equal(t, int64(0), off)
	require.Equal(t, "first", string(msg.Value()))

	// Now append, with the reader already parked at the end rather than merely
	// behind a watermark.
	go func() {
		time.Sleep(50 * time.Millisecond)
		offs, aerr := l.Append([]*Message{{Key: []byte("k1"), Value: []byte("second")}})
		if aerr == nil {
			l.SetHighWatermark(offs[0])
		}
	}()

	tailCtx, tailCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer tailCancel()
	msg, off, _, _, err = r.ReadMessage(tailCtx, hdr)
	require.NoError(t, err, "a record appended after the reader drained was never delivered")
	require.Equal(t, int64(1), off)
	require.Equal(t, "second", string(msg.Value()))
}

// The same, repeated: a tailing reader must keep waking, not just once.
//
// One wake-up can be produced by a single latch releasing. A consumer that
// tails is asking for the mechanism to work every time, so this drains and
// appends several times in sequence.
func TestFollowReaderKeepsWakingAcrossManyAppends(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t)})
	defer l.Close() // nolint: errcheck
	defer cleanup()

	offs, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("v0")}})
	require.NoError(t, err)
	l.SetHighWatermark(offs[0])

	r, err := l.NewReader(From(0), Follow())
	require.NoError(t, err)
	hdr := make([]byte, HeaderBufferLen)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, off, _, _, err := r.ReadMessage(ctx, hdr)
	require.NoError(t, err)
	require.Equal(t, int64(0), off)

	for i := 1; i <= 5; i++ {
		want := []byte(string(rune('a'+i)) + "-value")
		go func() {
			time.Sleep(20 * time.Millisecond)
			o, aerr := l.Append([]*Message{{Key: []byte("k"), Value: want}})
			if aerr == nil {
				l.SetHighWatermark(o[0])
			}
		}()

		readCtx, readCancel := context.WithTimeout(context.Background(), 10*time.Second)
		msg, off, _, _, rerr := r.ReadMessage(readCtx, hdr)
		readCancel()
		require.NoError(t, rerr, "append %d after draining was never delivered", i)
		require.Equal(t, int64(i), off)
		require.Equal(t, string(want), string(msg.Value()))
	}
}

// A filtered reader must not be satisfied by a tail record it filters out.
//
// The analogue of the bug durable_streams spent hours on: their wait was
// expressed against an offset watermark, but commit markers occupy offsets and
// reads skip them — so the condition became satisfiable when no READABLE record
// existed, and the loop woke to find nothing.
//
// The same state is reachable here through a KeyPrefix read, which is the only
// read that filters: the watermark advances for a record the filter rejects. The
// reader must stay parked, not return early and not spin.
//
// TestReaderFollowSeesLaterAppends is close but appends the non-matching and
// matching records together, so it never observes the log in the state where
// only an unreadable record has arrived. This isolates it.
func TestFilteredFollowReaderIgnoresANonMatchingTailRecord(t *testing.T) {
	l, app := specLog(t)
	app(&Message{Key: []byte("k:1"), Value: []byte("first")})
	app(&Message{Key: []byte("pad"), Value: []byte("padpadpad")})

	r, err := l.NewReader(KeyPrefix([]byte("k:")), Follow())
	require.NoError(t, err)
	hdr := make([]byte, HeaderBufferLen)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	msg, _, _, _, err := r.ReadMessage(ctx, hdr)
	require.NoError(t, err)
	require.Equal(t, "first", string(msg.Value()))

	// Advance the log with a record the filter REJECTS, and nothing else.
	app(&Message{Key: []byte("other"), Value: []byte("skipped")})

	// The reader must still be waiting: a short deadline expiring is the
	// assertion. Returning a record here would mean serving something the filter
	// excludes; returning nil early would mean ending a Follow read at a
	// watermark rather than at data.
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	_, _, _, _, err = r.ReadMessage(shortCtx, hdr)
	shortCancel()
	// DeadlineExceeded specifically, not merely "an error": a bare require.Error
	// would also accept a genuine fault and report it as correct waiting.
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"a non-matching tail record must not satisfy a filtered read — and the read must be WAITING, not failing")

	// And a matching record still gets through afterwards, so the reader parked
	// rather than breaking.
	app(&Message{Key: []byte("k:2"), Value: []byte("second")})
	msg, _, _, _, err = r.ReadMessage(ctx, hdr)
	require.NoError(t, err, "the reader stopped delivering after skipping a tail record")
	require.Equal(t, "second", string(msg.Value()))
}
