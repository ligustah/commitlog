package commitlog

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A watermark set ABOVE the log's tail bounds the reader instead of failing it.
//
// A follower learns what the leader committed before the records themselves
// arrive, so SetHighWatermark is legitimately handed an offset this log does
// not hold yet. That used to send getHWPos looking for a segment containing it,
// and every committed reader on the log failed with ErrSegmentNotFound -- an
// error naming a missing segment for what was really a watermark the log could
// not honour yet. It was also unrecoverable from the caller's side, because
// SetHighWatermark is monotonic and refuses to walk back down.
func TestAWatermarkAboveTheTailBoundsTheReaderRatherThanFailingIt(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t), MaxSegmentBytes: 1024})
	defer cleanup()

	appendToLog(t, l, []keyValue{
		{[]byte("a"), []byte("1")},
		{[]byte("b"), []byte("2")},
	}, true)
	require.EqualValues(t, 1, l.NewestOffset(), "premise: the log holds 0 and 1")

	l.SetHighWatermark(999)
	require.EqualValues(t, 999, l.HighWatermark(),
		"the caller's claim is kept, not clamped away -- the bound rises on its "+
			"own as records arrive, and lowering it here would need Override to undo")

	r, err := l.NewReader(From(0))
	require.NoError(t, err, "a watermark above the tail must not fail reader construction")

	headers := make([]byte, HeaderBufferLen)
	for want := int64(0); want <= 1; want++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, off, _, _, err := r.ReadMessage(ctx, headers)
		cancel()
		require.NoError(t, err, "record %d is present and committed", want)
		require.EqualValues(t, want, off)
	}

	// And it stops at the tail rather than running past it into records that do
	// not exist. A reader is non-following by default, so "committed data ran
	// out" ends the pass instead of parking.
	_, _, _, _, err = r.ReadMessage(context.Background(), headers)
	require.ErrorIs(t, err, io.EOF,
		"the reader must not serve past the tail just because the watermark is above it")
}

// The wait still fires. This is the half a clamp gets wrong: bound the reader on
// a value that syncHW also waits on, and waitForHW sees a watermark that has
// already moved, returns instantly, and the loop spins instead of parking.
func TestACommittedReaderStillParksAndWakesWhenTheWatermarkAdvances(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t), MaxSegmentBytes: 1024})
	defer cleanup()

	appendToLog(t, l, []keyValue{{[]byte("a"), []byte("1")}}, true)
	l.SetHighWatermark(0)

	r, err := l.NewReader(From(0), Follow())
	require.NoError(t, err)
	headers := make([]byte, HeaderBufferLen)

	_, _, _, _, err = r.ReadMessage(context.Background(), headers)
	require.NoError(t, err, "the one committed record")

	// Nothing more is committed, so this must BLOCK rather than return.
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _, _, _, err := r.ReadMessage(ctx, headers)
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("the reader returned instead of parking at the watermark: %v", err)
	case <-time.After(250 * time.Millisecond):
	}

	// Now give it something and advance the watermark: it must wake.
	appendToLog(t, l, []keyValue{{[]byte("b"), []byte("2")}}, true)
	l.SetHighWatermark(1)

	select {
	case err := <-done:
		require.NoError(t, err, "the parked reader must wake when the watermark advances")
	case <-time.After(10 * time.Second):
		t.Fatal("the parked reader was never woken by the watermark advancing")
	}
}

// The over-set case parks and wakes too, and what wakes it is the watermark
// moving -- not the records arriving.
//
// Appends do not signal hw waiters; only the three watermark writers do. So a
// follower sitting above its own tail stays parked until the next watermark
// change, even as records land. That is the honest bound on the fix: it turns a
// hard ErrSegmentNotFound into a reader that waits, not into one that tracks the
// tail on its own.
func TestAnOverSetWatermarkParksAtTheTailAndWakesOnTheNextAdvance(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t), MaxSegmentBytes: 1024})
	defer cleanup()

	appendToLog(t, l, []keyValue{{[]byte("a"), []byte("1")}}, true)
	l.SetHighWatermark(999)

	r, err := l.NewReader(From(0), Follow())
	require.NoError(t, err)
	headers := make([]byte, HeaderBufferLen)

	_, off, _, _, err := r.ReadMessage(context.Background(), headers)
	require.NoError(t, err)
	require.EqualValues(t, 0, off, "everything the log actually holds is served")

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _, _, _, err := r.ReadMessage(ctx, headers)
		done <- err
	}()

	// Let it actually reach the park first. Without this the append below can
	// land before the reader ever blocks, and it simply reads through -- which
	// is a test that raced, not a reader that woke.
	select {
	case err := <-done:
		t.Fatalf("the reader returned instead of parking at the tail: %v", err)
	case <-time.After(250 * time.Millisecond):
	}

	// The record arrives, but the watermark does not move: still parked. The
	// SetHighWatermark inside appendToLog is a no-op here, since 1 is below the
	// 999 already stored and the setter is monotonic.
	appendToLog(t, l, []keyValue{{[]byte("b"), []byte("2")}}, true)
	select {
	case err := <-done:
		t.Fatalf("an append alone woke the reader; only a watermark change should: %v", err)
	case <-time.After(250 * time.Millisecond):
	}

	// Move the watermark and it wakes.
	l.SetHighWatermark(1000)
	select {
	case err := <-done:
		require.NoError(t, err, "the parked reader must wake when the watermark advances")
	case <-time.After(10 * time.Second):
		t.Fatal("the parked reader was never woken")
	}
}
