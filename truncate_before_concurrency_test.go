package commitlog

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Retention is background work, and a log that stops serving for the duration of
// it is not doing background work. TruncateBefore held l.mu across every segment
// close, every unlink and the whole boundary rewrite, and every reader takes that
// same lock through Segments() — so one truncation was a hard stop for the whole
// log. Reported downstream as a 10-minute test timeout whose stack was one
// truncator inside a Windows FlushFileBuffers with everyone else queued behind
// it.
//
// The assertion is a COUNT, not a duration: with the lock held end to end, a read
// that starts during the truncation cannot finish until it ends, so essentially
// none complete. It does not matter how long the truncation takes, which is what
// keeps this from being a timing test.
func TestReadsAreServedWhileATruncationRuns(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:             tempDir(t),
		MaxSegmentBytes:  128, // many small segments, so there is real work to do
		DisableAutoClean: true,
	})
	defer cleanup()

	const records = 1000
	for n := int64(0); n < records; n++ {
		offs, err := l.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("k:%d", n%16)),
			Value: []byte(strconv.FormatInt(n, 10) + ":padding to force segment rolls"),
		}})
		require.NoError(t, err)
		l.SetHighWatermark(offs[len(offs)-1])
	}
	l.mu.RLock()
	segs := len(l.segments)
	l.mu.RUnlock()
	require.Greater(t, segs, 100, "need enough segments for the truncation to take real time")

	// The cut lands INSIDE a segment, so the boundary rewrite runs too — that is
	// the expensive half, a whole segment read and a whole segment written.
	cut := l.NewestOffset() / 2
	require.Greater(t, cut, int64(0))

	var (
		stop     = make(chan struct{})
		total    atomic.Int64
		during   atomic.Int64
		readErr  atomic.Value // error
		wg       sync.WaitGroup
		truncing atomic.Bool
	)
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
			// A read that actually resolves a segment: NewReader goes through
			// Segments(), which is the RLock the truncation was starving.
			r, err := l.NewReader(From(l.OldestOffset()), Uncommitted())
			if err != nil {
				continue // the floor moved out from under it; not what we measure
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, _, _, _, err = r.ReadMessage(ctx, hdr)
			cancel()
			if err != nil {
				continue
			}
			total.Add(1)
			// Counted only if the truncation was in flight for the WHOLE read:
			// sampled after, so a read that began before it started cannot count.
			if truncing.Load() {
				during.Add(1)
			}
		}
	}()

	// Establish what this machine serves when nothing is truncating, so the
	// assertion below can be a RATIO. An absolute floor would only catch the
	// lock being held across the whole call; a ratio also catches it being held
	// across just the deletes, or just the rewrite, which is the shape a partial
	// regression here would take.
	const warmup = 250 * time.Millisecond
	time.Sleep(warmup)
	baseline := float64(total.Load()) / warmup.Seconds()

	truncing.Store(true)
	start := time.Now()
	require.NoError(t, l.TruncateBefore(cut))
	elapsed := time.Since(start)
	truncing.Store(false)

	close(stop)
	wg.Wait()
	if err := readErr.Load(); err != nil {
		t.Fatalf("read failed: %v", err)
	}

	// A quarter of the undisturbed rate. Deliberately generous: a truncation
	// does compete for the disk and for each segment's own lock, so reads are
	// expected to be somewhat slower while one runs — just not STOPPED.
	want := int64(0.25 * baseline * elapsed.Seconds())
	t.Logf("truncation of %d segments took %s; %d reads completed inside it "+
		"(baseline %.0f/s, floor %d)", segs, elapsed, during.Load(), baseline, want)
	// The meaningfulness check is on `want`, not on the clock. An earlier version
	// of this asserted elapsed > 20ms and failed on Linux CI at 19.69ms — where
	// the fix was in fact working, 381 reads against a floor of 99. Unlinking 500
	// files is an order of magnitude cheaper there than on Windows, so a wall
	// clock floor measures the filesystem rather than the lock. `want` already
	// says what actually has to be true: the window was long enough that an
	// unblocked log would have served at least 200 reads inside it.
	require.Greater(t, want, int64(50), "the window was too small to assert on")
	require.Greater(t, during.Load(), want,
		"reads were starved while the truncation ran (%s)", elapsed)

	// And it still did the job.
	require.Equal(t, cut, l.OldestOffset())
}

// An append that ROLLS while a truncation has the lock down adds a segment to
// the live list, and the truncation must publish a list that still has it. It
// used to be impossible to lose — the whole call held the write lock — and is
// the first thing releasing the lock puts at risk.
func TestATruncationDoesNotLoseASegmentRolledUnderIt(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:             tempDir(t),
		MaxSegmentBytes:  128,
		DisableAutoClean: true,
	})
	defer cleanup()

	appended := int64(0)
	for n := int64(0); n < 1000; n++ {
		offs, err := l.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("k:%d", n%16)),
			Value: []byte(strconv.FormatInt(n, 10) + ":padding to force segment rolls"),
		}})
		require.NoError(t, err)
		l.SetHighWatermark(offs[len(offs)-1])
		appended = offs[len(offs)-1]
	}
	cut := l.NewestOffset() / 2

	var (
		stop  = make(chan struct{})
		wg    sync.WaitGroup
		wrErr atomic.Value // error
		last  atomic.Int64
	)
	last.Store(appended)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := int64(0); ; n++ {
			select {
			case <-stop:
				return
			default:
			}
			offs, err := l.Append([]*Message{{
				Key:   []byte(fmt.Sprintf("late:%d", n%16)),
				Value: []byte("written while the truncation was running, forcing rolls"),
			}})
			if err != nil {
				wrErr.CompareAndSwap(nil, err)
				return
			}
			l.SetHighWatermark(offs[len(offs)-1])
			last.Store(offs[len(offs)-1])
		}
	}()

	require.NoError(t, l.TruncateBefore(cut))
	close(stop)
	wg.Wait()
	if err := wrErr.Load(); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	// Everything written during the truncation is still readable, which is only
	// true if the segments those appends rolled survived publication.
	require.Greater(t, last.Load(), appended, "the writer never rolled anything")
	require.Equal(t, last.Load(), l.NewestOffset())
	r, err := l.NewReader(From(last.Load()), Uncommitted())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, off, _, _, err := r.ReadMessage(ctx, make([]byte, HeaderBufferLen))
	require.NoError(t, err, "the last record written under the truncation is unreachable")
	require.Equal(t, last.Load(), off)

	// The segment list is still contiguous end to end.
	l.mu.RLock()
	defer l.mu.RUnlock()
	for i := 1; i < len(l.segments); i++ {
		require.Equal(t, l.segments[i-1].NextOffset(), l.segments[i].BaseOffset,
			"segment %d does not start where %d ends", i, i-1)
	}
}

// A boundary segment trimmed at a new base offset must point readers at the
// trim, and this asserts it against a SNAPSHOT rather than by timing.
//
// findSegment resolves through the slice a reader is holding, not through
// l.segments, and Segments() hands out the header — so a reader that took its
// snapshot before a truncation published still reaches the boundary after the
// truncation has unlinked it. Without the link that segment is gone with no
// replacement, which reads as "retention collected these", and findSegment
// skips to the NEXT segment: past the very records the trim preserved.
//
// TestChaosAReadFromThePublishedFloorStartsAtIt used to cover this and no
// longer does. That is not the hazard going away, it is the window narrowing:
// truncation now publishes the new list BEFORE it unlinks, so a reader that
// re-resolves finds the trim in the published list and never consults the
// boundary at all. Only a snapshot older than the publish still gets there, and
// the chaos test cannot reliably manufacture one. This can.
func TestAStaleSegmentSnapshotFollowsATrimmedBoundary(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:             tempDir(t),
		MaxSegmentBytes:  256,
		DisableAutoClean: true,
	})
	defer cleanup()

	var last int64
	for n := 0; n < 200; n++ {
		offs, err := l.Append([]*Message{{
			Key:   []byte("k" + strconv.Itoa(n)),
			Value: []byte("v" + strconv.Itoa(n)),
		}})
		require.NoError(t, err)
		last = offs[len(offs)-1]
	}
	l.SetHighWatermark(last)

	segs := l.Segments()
	require.GreaterOrEqual(t, len(segs), 4, "need segments below and above the boundary")
	boundary := segs[1]
	require.Greater(t, boundary.LastOffset(), boundary.BaseOffset,
		"the boundary must hold at least two records, so the cut straddles it")

	// Inside the boundary, so the trim keeps records rather than dropping it.
	cut := boundary.BaseOffset + 1

	// The snapshot, taken before the truncation. This is the reader.
	stale := l.Segments()

	require.NoError(t, l.TruncateBefore(cut))

	seg, _ := findSegment(stale, cut)
	require.NotNil(t, seg, "a snapshot taken before the truncation lost the offset entirely")
	require.Equal(t, cut, seg.BaseOffset,
		"a snapshot resolved to the segment at %d instead of the trim at %d — "+
			"the boundary went without recording what took over its records",
		seg.BaseOffset, cut)
}
