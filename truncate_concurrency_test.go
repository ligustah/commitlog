package commitlog

import (
	"sync"
	"testing"

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
