package commitlog

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// segmentWaiters reports how many readers are parked on a segment waiting for
// bytes. Used to establish that a reader is ACTUALLY parked before the fixture
// rolls underneath it — without it, "park then roll" and "roll then read" are
// the same test run twice, and only one of them exercises the arm below.
func segmentWaiters(s *segment) int {
	s.Lock()
	defer s.Unlock()
	return len(s.waiters)
}

// waitForParkedReader blocks until exactly one reader is parked on seg, and
// fails the test if that never happens.
func waitForParkedReader(t *testing.T, seg *segment) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if segmentWaiters(seg) == 1 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("no reader parked on the segment; the roll below would be observed " +
		"by a reader that never waited, which is a different code path")
}

// An uncommitted reader parked at the tail must be carried across the roll that
// seals the segment under it, and go on to deliver a record written to the NEW
// segment.
//
// This is uncommittedReader.Read's second segment-advance arm — the one reached
// only after waitForData has returned and the read STILL hits io.EOF, which
// means the bytes it was woken for went somewhere else. Nothing covered it.
// Every other tailing test in this suite either appends into the segment the
// reader is already in (so the first arm never fires) or opens the reader after
// the roll has happened (so the reader finds the next segment without ever
// waiting). Both leave this arm to be reasoned about rather than run.
//
// The arm matters because it is where the reader's position is reset by hand —
// r.pos = 0 for the new segment — while the sibling arm relies on the next
// iteration to resync it. Two spellings of the same advance, and this is the
// one with no test under it.
//
// The roll is AGE-driven and driven by a single cleanerTick, so the fixture
// appends nothing to produce it: an append notifies the segment's waiters
// itself, and a reader woken by the append it is about to read would prove
// nothing about being woken by the SEAL.
func TestAnUncommittedReaderParkedAtTheTailIsCarriedAcrossARoll(t *testing.T) {
	l, err := New(Options{
		Name: "reader-roll", Path: tempDir(t),
		// Far from full, so the roll is unambiguously the age one. A segment at
		// its byte limit hands waitForData an already-closed channel, which would
		// wake the reader for a reason this test is not about.
		MaxSegmentBytes:      64 << 20,
		MaxSegmentAge:        time.Millisecond,
		CleanerInterval:      time.Hour, // the loop must not race the tick below
		HWCheckpointInterval: time.Hour,
	})
	require.NoError(t, err)
	defer l.Close() // nolint: errcheck
	cl := l.(*commitLog)

	_, err = l.Append([]*Message{{Key: []byte("k0"), Value: []byte("before")}})
	require.NoError(t, err)

	// Uncommitted, so the read is bounded by bytes on disk rather than by a
	// watermark: this test is about the segment boundary, and a committed reader
	// would park on the high watermark instead of on the segment.
	r, err := l.NewReader(From(0), Uncommitted(), Follow())
	require.NoError(t, err)
	hdr := make([]byte, HeaderBufferLen)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	msg, off, _, _, err := r.ReadMessage(ctx, hdr)
	require.NoError(t, err)
	require.Equal(t, int64(0), off)
	require.Equal(t, "before", string(msg.Value()))

	type result struct {
		value string
		off   int64
		err   error
	}
	done := make(chan result, 1)
	go func() {
		readCtx, readCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer readCancel()
		m, o, _, _, rerr := r.ReadMessage(readCtx, make([]byte, HeaderBufferLen))
		res := result{off: o, err: rerr}
		if rerr == nil {
			res.value = string(m.Value())
		}
		done <- res
	}()

	sealed := cl.activeSegment()
	waitForParkedReader(t, sealed)

	// Past MaxSegmentAge, so the tick is certain to roll rather than merely
	// likely to.
	time.Sleep(5 * time.Millisecond)
	require.True(t, sealed.CheckSplit(cl.MaxSegmentAge),
		"the fixture must present the tick with a segment that is due to roll")
	cl.cleanerTick()
	require.NotSame(t, sealed, cl.activeSegment(), "the tick did not roll")

	// The record the parked reader is owed, written to the segment the roll
	// created. Reaching it means the reader followed the boundary; a reader that
	// stayed with the sealed segment waits here until its context expires.
	_, err = l.Append([]*Message{{Key: []byte("k1"), Value: []byte("after")}})
	require.NoError(t, err)

	select {
	case res := <-done:
		require.NoError(t, res.err,
			"a reader parked when its segment was sealed never reached the segment the roll created")
		require.Equal(t, int64(1), res.off)
		require.Equal(t, "after", res.value)
	case <-time.After(20 * time.Second):
		t.Fatal("the parked reader neither delivered nor failed")
	}
}

// The same boundary, crossed WITHOUT ever parking: the roll and the record that
// follows it both land before the reader asks for more.
//
// This is uncommittedReader.Read's first advance arm, and it exists here as the
// deliberate pair to the test above. The two arms do the same thing by
// different means — one resets the position explicitly, the other leaves it to
// the next iteration — and which of them a run takes is decided by a race
// between the reader and the roll. A single test covers whichever one it
// happened to hit and reports the other as covered too.
func TestAnUncommittedReaderCrossesARollItNeverParkedFor(t *testing.T) {
	l, err := New(Options{
		Name: "reader-roll-nopark", Path: tempDir(t),
		MaxSegmentBytes:      64 << 20,
		MaxSegmentAge:        time.Millisecond,
		CleanerInterval:      time.Hour,
		HWCheckpointInterval: time.Hour,
	})
	require.NoError(t, err)
	defer l.Close() // nolint: errcheck
	cl := l.(*commitLog)

	_, err = l.Append([]*Message{{Key: []byte("k0"), Value: []byte("before")}})
	require.NoError(t, err)

	r, err := l.NewReader(From(0), Uncommitted(), Follow())
	require.NoError(t, err)
	hdr := make([]byte, HeaderBufferLen)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	msg, off, _, _, err := r.ReadMessage(ctx, hdr)
	require.NoError(t, err)
	require.Equal(t, int64(0), off)
	require.Equal(t, "before", string(msg.Value()))

	// Roll and append with the reader idle between calls, so the next read finds
	// the next segment already there and never waits for data.
	sealed := cl.activeSegment()
	require.Zero(t, segmentWaiters(sealed),
		"the reader must NOT be parked here, or this is the other test")
	time.Sleep(5 * time.Millisecond)
	require.True(t, sealed.CheckSplit(cl.MaxSegmentAge))
	cl.cleanerTick()
	require.NotSame(t, sealed, cl.activeSegment(), "the tick did not roll")

	_, err = l.Append([]*Message{{Key: []byte("k1"), Value: []byte("after")}})
	require.NoError(t, err)

	msg, off, _, _, err = r.ReadMessage(ctx, hdr)
	require.NoError(t, err, "the reader did not follow a roll that had already happened")
	require.Equal(t, int64(1), off)
	require.Equal(t, "after", string(msg.Value()))
}
