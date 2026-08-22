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
	// DisableAutoClean throughout this file. Every assertion here is about where
	// the TAIL is relative to the watermark, and a background pass that rolls or
	// drops a segment moves the tail underneath them. A sibling test in this
	// package was written without it, went green locally and on one CI run, and
	// failed on the next when a pass happened to land mid-test.
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t), MaxSegmentBytes: 1024, DisableAutoClean: true})
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
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t), MaxSegmentBytes: 1024, DisableAutoClean: true})
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

// The over-set case parks at the tail and is woken by the records ARRIVING.
//
// This inverts what v0.96.0 shipped, deliberately. There, appends did not signal
// watermark waiters, so a follower sitting above its own tail stayed parked as
// records landed -- and those records are committed by the caller's own claim
// the moment they arrive, since the watermark is already above them. Nothing
// else was ever going to say so: SetHighWatermark notifies only when the value
// INCREASES, so a leader restating the same watermark is silent, and the reader
// waited for a change that had already happened in every way except the one it
// watched. Present, committed, unreadable, no error -- the failure has no caller
// to fail.
func TestAnOverSetWatermarkParksAtTheTailAndWakesWhenRecordsArrive(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t), MaxSegmentBytes: 1024, DisableAutoClean: true})
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

	// The record arrives and the watermark does NOT move -- the SetHighWatermark
	// inside appendToLog is a no-op here, since 1 is below the 999 already stored
	// and the setter is monotonic. The append itself must wake the reader, since
	// nothing else is coming.
	appendToLog(t, l, []keyValue{{[]byte("b"), []byte("2")}}, true)
	select {
	case err := <-done:
		require.NoError(t, err,
			"a record arriving below an already-set watermark is committed and present, so the reader must be woken by it")
	case <-time.After(10 * time.Second):
		t.Fatal("the reader was never woken by a committed record arriving; only the caller's deadline would end this read")
	}
}

// An append does NOT wake a committed reader when the watermark is at or below
// the tail, which is the ordinary case. The bound matters: waking on every
// append would make a plain tailing read spin against a watermark that has not
// moved, and the wake above is earned only because the watermark already
// covered the arriving record.
func TestAnAppendDoesNotWakeACommittedReaderWhenTheWatermarkIsAtTheTail(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t), MaxSegmentBytes: 1024, DisableAutoClean: true})
	defer cleanup()

	appendToLog(t, l, []keyValue{{[]byte("a"), []byte("1")}}, true)
	l.SetHighWatermark(0)

	r, err := l.NewReader(From(0), Follow())
	require.NoError(t, err)
	headers := make([]byte, HeaderBufferLen)
	_, off, _, _, err := r.ReadMessage(context.Background(), headers)
	require.NoError(t, err)
	require.EqualValues(t, 0, off)

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _, _, _, err := r.ReadMessage(ctx, headers)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("the reader returned instead of parking: %v", err)
	case <-time.After(250 * time.Millisecond):
	}

	// Append WITHOUT moving the watermark past the old tail: offset 1 is above
	// the watermark of 0, so it is not committed and the reader must stay put.
	_, err = l.Append([]*Message{{Key: []byte("b"), Value: []byte("2")}})
	require.NoError(t, err)

	select {
	case err := <-done:
		t.Fatalf("an uncommitted record woke a committed reader: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
}

// A READONLY log ends a reader whose watermark sits above the tail, rather than
// parking it forever.
//
// This is the one wait that can never end on its own. The test above pins the
// parking as correct on a live log -- the records may still arrive, and the
// watermark moving is what wakes the reader. A readonly log has neither: Append
// is refused, so nothing will move the tail or the watermark, and nothing else
// wakes an HW waiter. The reader sat there until its own caller gave up, on a
// log that had already declared it was finished.
//
// It became reachable in v0.96.0. Before that an over-set watermark failed
// reader CONSTRUCTION, so no reader ever existed in this state to park -- which
// is why waitForHW's readonly arm tested the watermark for EQUALITY with the
// tail and nothing noticed.
func TestAReadonlyLogEndsAReaderWhoseWatermarkSitsAboveTheTail(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path: tempDir(t), MaxSegmentBytes: 1 << 20,
	})
	defer cleanup()

	_, err := l.Append([]*Message{{Value: []byte("a")}, {Value: []byte("b")}})
	require.NoError(t, err)

	// The caller's claim runs ahead of the records, and the log keeps it: the
	// setter is monotonic and nothing lowers it.
	l.SetHighWatermark(l.NewestOffset() + 5)
	l.SetReadonly(true)
	require.Greater(t, l.HighWatermark(), l.NewestOffset(),
		"fixture: the watermark must sit ABOVE the tail, or this asserts the equality case")

	r, err := l.NewReader(Follow())
	require.NoError(t, err, "an over-set watermark must not fail construction")

	hdr := make([]byte, HeaderBufferLen)
	for i := 0; i < 2; i++ {
		_, _, _, _, err := r.ReadMessage(context.Background(), hdr)
		require.NoError(t, err, "the reader must serve what the log HOLDS, record %d", i)
	}

	// A generous bound: the point is that it ends by itself, not how fast. If
	// the readonly arm is missed this deadline is the only thing that returns,
	// and the error is the context's rather than the log's.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _, _, _, err = r.ReadMessage(ctx, hdr)
	require.ErrorIs(t, err, ErrCommitLogReadonly,
		"a readonly log must END a reader that has been served everything it holds, not park it until the caller gives up")
	require.NotErrorIs(t, err, context.DeadlineExceeded,
		"the reader parked: only the caller's deadline ended it")
}
