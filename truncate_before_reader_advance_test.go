package commitlog

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// One read must never return the same offset twice, or go backwards, however a
// truncation moves the segments under it.
//
// The reader advanced by asking for the next segment whose base offset is
// above its own — and then reset to that segment's position zero. That query is
// wrong whenever a segment can be replaced by one
// with a HIGHER base offset covering a SUFFIX of the same range, which is
// exactly what TruncateBefore's boundary trim is: source 0..5 becomes 4..5. A
// reader that finished the source is handed the trim and replays it from its
// base, so it serves 5 and then 4.
//
// Reported from durable_streams against v0.50.0 as a single ReadWithOffsets
// batch that was not monotonic — "4 after 5", "242 after 242". The records
// served were all genuine and self-consistent, which is the useful half of that
// report: never a bad index or a stale seek, just the reader being sent back
// into a range it had already walked.
//
// The v0.50.0 lock rework is what made this reachable rather than what broke it.
// Publishing the new segment list before unlinking the source leaves a window
// where the source is still live and readable AND its trim is already published,
// so an ordinary scan can walk from one into the other. Under the old
// all-under-l.mu ordering the source was always closed before the trim became
// reachable, and the reader took the ErrSegmentReplaced path instead, which
// re-resolves by OFFSET and lands correctly.
func TestAReadAcrossATruncationIsMonotonic(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:             tempDir(t),
		MaxSegmentBytes:  160, // a handful of records per segment; roll constantly
		DisableAutoClean: true,
	})
	defer cleanup()

	const records = 400
	var last int64
	for n := 0; n < records; n++ {
		offs, err := l.Append([]*Message{{
			Key:   []byte("k" + strconv.Itoa(n)),
			Value: []byte("v" + strconv.Itoa(n)),
		}})
		require.NoError(t, err)
		last = offs[len(offs)-1]
	}
	l.SetHighWatermark(last)

	var (
		stop    = make(chan struct{})
		wg      sync.WaitGroup
		bad     atomic.Value // string
		batches atomic.Int64
	)

	// Truncator: walks the floor up, one record at a time, so the cut keeps
	// landing INSIDE a segment and the boundary is rewritten every time.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for cut := int64(1); cut < last-16; cut++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := l.TruncateBefore(cut); err != nil {
				return
			}
		}
	}()

	// Reader: the shape the report used — open at the published floor, read a
	// batch, and require it to be monotonic within that one scan.
	wg.Add(1)
	go func() {
		defer wg.Done()
		hdr := make([]byte, HeaderBufferLen)
		for {
			select {
			case <-stop:
				return
			default:
			}
			floor := l.OldestOffset()
			if floor < 0 {
				continue
			}
			r, err := l.NewReader(From(floor), Uncommitted())
			if err != nil {
				continue // the floor moved out from under it; not what we measure
			}
			prev := int64(-1)
			for i := 0; i < 16; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_, off, _, _, err := r.ReadMessage(ctx, hdr)
				cancel()
				if err != nil {
					break
				}
				if prev >= 0 && off <= prev {
					bad.Store("a read returned offsets out of order: " +
						strconv.FormatInt(off, 10) + " after " + strconv.FormatInt(prev, 10))
					return
				}
				prev = off
			}
			batches.Add(1)
		}
	}()

	deadline := time.After(20 * time.Second)
	for bad.Load() == nil {
		select {
		case <-deadline:
			goto done
		default:
		}
		time.Sleep(10 * time.Millisecond)
		if l.OldestOffset() >= last-16 {
			break
		}
	}
done:
	close(stop)
	wg.Wait()

	t.Logf("%d batches read", batches.Load())
	require.Greater(t, batches.Load(), int64(10), "too few batches to prove anything")
	if v := bad.Load(); v != nil {
		t.Fatal(v.(string))
	}
}

// The same invariant, stated directly and without timing: a reader that has
// consumed a segment advances PAST it, not into the trim that replaced it.
//
// This builds the exact state a truncation publishes — the trim in the segment
// list, the source still alive because its unlink has not happened yet — and
// asks findSegmentAfter what comes after the source. Nothing here is racing, so
// this is the guard's test; the chaos test above is the reproduction.
func TestASegmentAdvanceSkipsTheTrimOfTheSegmentJustRead(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:             tempDir(t),
		MaxSegmentBytes:  160,
		DisableAutoClean: true,
	})
	defer cleanup()

	var last int64
	for n := 0; n < 60; n++ {
		offs, err := l.Append([]*Message{{
			Key:   []byte("k" + strconv.Itoa(n)),
			Value: []byte("v" + strconv.Itoa(n)),
		}})
		require.NoError(t, err)
		last = offs[len(offs)-1]
	}
	l.SetHighWatermark(last)

	segs := l.Segments()
	require.GreaterOrEqual(t, len(segs), 3)
	source := segs[0]
	require.Greater(t, source.LastOffset(), source.BaseOffset,
		"the source must hold at least two records so a trim is a proper suffix")

	// Build the trim exactly as TruncateBefore does: everything at or above the
	// cut, into a new segment based at the cut.
	cut := source.BaseOffset + 1
	trimmed, err := source.Trimmed(cut)
	require.NoError(t, err)
	ss := newSegmentScanner(source)
	for {
		ms, _, err := ss.Scan()
		if err != nil {
			break
		}
		if ms.Offset() >= cut {
			require.NoError(t, trimmed.WriteMessageSet(ms, entriesForMessageSet(trimmed.Position(), ms)))
		}
	}
	ss.Close()
	require.NoError(t, trimmed.Finalize())
	trimmed.Seal()
	defer trimmed.Delete()

	require.Equal(t, cut, trimmed.BaseOffset)
	require.Equal(t, source.NextOffset(), trimmed.NextOffset(),
		"a trim ends where its source did; that is what makes it a suffix")

	// What TruncateBefore publishes: the trim in place of the source. The source
	// itself is NOT in this list, but is still open and readable — its unlink
	// comes after the publish, and that gap is the whole window.
	published := append([]*segment{trimmed}, segs[1:]...)

	next := findSegmentAfter(published, source)
	require.NotNil(t, next, "a reader that finished the source has nowhere to go")
	require.NotSame(t, trimmed, next,
		"advancing off the source landed in its own trim, which would replay "+
			"offsets %d..%d that the reader has already served",
		trimmed.BaseOffset, trimmed.LastOffset())
	require.Equal(t, segs[1].BaseOffset, next.BaseOffset,
		"advancing off the source must land on the segment after it")
}
