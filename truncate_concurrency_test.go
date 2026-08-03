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

// Truncate swaps the active segment and the segment list, so like a segment
// roll it redefines where an append lands — and it does so without the lock
// appends hold. It can therefore land between an append reading the active
// segment and that append writing to it.
//
// It is nevertheless safe, and the argument is on Truncate itself: it mutates
// the very segment the appender is holding, and both take that segment's write
// lock, so the two are ordered. Either the append lands and is then
// legitimately truncated, or the segment is closed first and the append fails
// honestly.
//
// This test guards that conclusion rather than re-deriving it. The invariant:
// an append that returns SUCCESS must be readable afterwards unless truncation
// legitimately removed it, and the log must stay structurally intact — readable
// end to end, with no offset appearing twice.
//
// Note the workload reuses offsets: truncating the tail back and appending
// again hands the same offset to a different record, which is why the check is
// framed around the surviving range rather than around every offset ever
// acknowledged.
func TestTruncateRacingAppendsLosesNoAcknowledgedRecord(t *testing.T) {
	const (
		writers = 8
		each    = 60
	)

	l, cleanup := setupWithOptions(t, Options{
		Path:             tempDir(t),
		MaxSegmentBytes:  256, // roll often, so truncation has segments to cut
		DisableAutoClean: true,
	})
	defer cleanup()

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		acked    = map[int64]bool{}
		failures int
	)

	// Truncate the tail back repeatedly while appends are in flight.
	stop := make(chan struct{})
	var truncator sync.WaitGroup
	truncator.Add(1)
	go func() {
		defer truncator.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			newest := l.NewestOffset()
			if newest > 20 {
				if err := l.Truncate(newest - 10); err != nil {
					t.Error(err)
					return
				}
			}
		}
	}()

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < each; j++ {
				offs, err := l.Append([]*Message{{Value: []byte("v")}})
				mu.Lock()
				if err != nil {
					failures++
				} else {
					for _, o := range offs {
						acked[o] = true
					}
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(stop)
	truncator.Wait()

	t.Logf("acknowledged=%d failed=%d newest=%d oldest=%d",
		len(acked), failures, l.NewestOffset(), l.OldestOffset())

	// Everything still present must be readable end to end, and every
	// acknowledged offset at or below the surviving tail must be there.
	newest := l.NewestOffset()
	l.SetHighWatermark(newest)
	got := readFrom(t, l)
	oldest := l.OldestOffset()

	for off := range acked {
		if off < oldest || off > newest {
			continue // legitimately truncated away
		}
		require.Contains(t, got, off,
			"offset %d was acknowledged to a caller and sits within the "+
				"surviving range [%d,%d], but is not readable", off, oldest, newest)
	}

	// Structural integrity: the surviving range must be contiguous, with every
	// offset present exactly once. A gap or a duplicate means a truncation and
	// an append disagreed about where the log ends.
	if oldest >= 0 {
		for off := oldest; off <= newest; off++ {
			require.Contains(t, got, off,
				"offset %d is missing from the surviving range [%d,%d]",
				off, oldest, newest)
		}
		require.Len(t, got, int(newest-oldest+1),
			"the readable log has records outside [%d,%d]", oldest, newest)
	}

	// An append may legitimately fail here — Truncate closing the segment under
	// it is the honest outcome — but it must not fail often enough to suggest
	// the two are fighting rather than taking turns.
	require.Less(t, failures, writers*each/2,
		"%d of %d appends failed; truncation should occasionally lose a race "+
			"with an append, not routinely", failures, writers*each)
}

// The same convoy TestReadsAreServedWhileATruncationRuns covers for
// TruncateBefore, on the other truncation.
//
// Truncate held l.mu — the lock every reader takes through Segments() — across
// the scan of the boundary segment, the write of its replacement, and the
// unlink of every segment above the cut. A follower reconciling after an
// unclean election can be told to truncate a long way back, so that is not a
// small amount of work, and for all of it the log served nothing.
//
// Unlike TruncateBefore, appendMu is held throughout and that is NOT being
// changed: the boundary scan runs outside the segment's own lock, so an append
// extending the segment mid-scan tears the copy — which is exactly what
// TestTruncateRacingAppendsLosesNoAcknowledgedRecord above exists for. Appends
// are meant to wait here. Reads are not.
//
// The assertion is a COUNT against a measured baseline, not a duration, for the
// same reason as the TruncateBefore test: with the lock held end to end a read
// that starts during the truncation cannot finish until it ends, so essentially
// none complete, however long or short the call happens to be.
func TestReadsAreServedWhileATruncateRuns(t *testing.T) {
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
	require.Greater(t, segs, 100, "need enough segments for the truncate to take real time")

	// Half the log goes, and the cut lands INSIDE a segment so the boundary
	// rewrite runs too — a whole segment read and a whole segment written, which
	// is the expensive half of the call.
	cut := l.NewestOffset() / 2
	require.Greater(t, cut, int64(0))

	var (
		stop     = make(chan struct{})
		total    atomic.Int64
		during   atomic.Int64
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
			// From the FLOOR, which a Truncate never moves — so unlike the
			// TruncateBefore test there is nothing here that is expected to fail
			// benignly as the log changes under the reader.
			r, err := l.NewReader(From(l.OldestOffset()), Uncommitted())
			if err != nil {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, _, _, _, err = r.ReadMessage(ctx, hdr)
			cancel()
			if err != nil {
				continue
			}
			total.Add(1)
			// Counted only if the truncate was in flight for the WHOLE read:
			// sampled after, so a read that began before it started cannot count.
			if truncing.Load() {
				during.Add(1)
			}
		}
	}()

	// What this machine serves when nothing is truncating, so the assertion can
	// be a RATIO. An absolute floor would only catch the lock being held across
	// the whole call; a ratio also catches it being held across just the deletes
	// or just the rewrite, which is the shape a partial regression would take.
	const warmup = 250 * time.Millisecond
	time.Sleep(warmup)
	baseline := float64(total.Load()) / warmup.Seconds()

	truncing.Store(true)
	start := time.Now()
	require.NoError(t, l.Truncate(cut))
	elapsed := time.Since(start)
	truncing.Store(false)

	close(stop)
	wg.Wait()

	// A quarter of the undisturbed rate. Deliberately generous: a truncate does
	// compete for the disk and for each segment's own lock, so reads are
	// expected to be somewhat slower while one runs — just not STOPPED.
	want := int64(0.25 * baseline * elapsed.Seconds())
	t.Logf("truncate of %d segments took %s; %d reads completed inside it "+
		"(baseline %.0f/s, floor %d)", segs, elapsed, during.Load(), baseline, want)
	require.Greater(t, elapsed, 20*time.Millisecond,
		"the truncate was too fast to prove anything; raise the segment count")
	require.Greater(t, want, int64(20), "the baseline was too low to assert on")
	require.Greater(t, during.Load(), want,
		"reads were starved while the truncate ran (%s)", elapsed)

	// And it still did the job.
	require.Equal(t, cut-1, l.NewestOffset(), "the truncate did not cut where it was asked to")
	require.Equal(t, int64(0), l.OldestOffset(), "the floor moved, and it should not have")

	// End to end, so the boundary rewrite is not merely present but readable.
	r, err := l.NewReader(From(0), Uncommitted())
	require.NoError(t, err)
	hdr := make([]byte, HeaderBufferLen)
	for at := int64(0); at < cut; at++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, off, _, _, err := r.ReadMessage(ctx, hdr)
		cancel()
		require.NoError(t, err, "reading back offset %d after the truncate", at)
		require.Equal(t, at, off, "the log skipped a record the truncate kept")
	}
}
