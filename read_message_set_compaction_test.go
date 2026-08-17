package commitlog

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ReadMessageSet must survive a compaction pass running underneath it.
//
// This is the replication path: a follower calls it in a loop to fetch the
// leader's own framing. It is also the one caller that cannot re-resolve
// anything itself — l.segments is not exported, so "the segment you resolved
// against was replaced" is a fact only this package can act on.
//
// It leaked that fact in two ways. This test falsifies the FIRST — restore the
// old resolve-once body and it fails in under a second, naming the sentinel. It
// cannot falsify the second, and the arm for that one says so in its own
// comment rather than borrowing this test's credit:
//
//   - findEntry on a segment a pass had already swapped returned
//     ErrSegmentReplaced verbatim. The interface doc then classified a commitlog
//     sentinel as PERMANENT with two named exceptions, and this was not one of
//     them — so a follower applying the documented rule stops replicating a log
//     that is merely compacting. The doc has since been corrected to state a
//     remedy per sentinel, with re-resolve named for this pair; the resolve loop
//     below is what makes the correction true for this caller.
//   - a swap between the resolve and the scan surfaced as ErrSegmentClosed from
//     Scan(), which the arm there filed under ErrSegmentUnreadable. That is the
//     sentinel meaning "the bytes on this replica are damaged, restore from a
//     peer" — reported for a routine pass, about bytes sitting intact in the
//     replacement. A wrong answer that costs a full restore is worse than one
//     that costs a stall.
//
// The assertion is simply that no error appears. With retention off there is no
// window in which a record leaves the log entirely, the log is never closed or
// deleted during the run, and offset 0 always resolves — either into a segment
// that contains it or, once the oldest is dropped, to the clamp. Every error
// this prober can see is the window under test.
//
// The pass has to be DRIVEN: the background cleaner's interval is minutes, so a
// test that merely sets Compact and waits never replaces a segment. Same reason
// TestTimestampLookupsWhileCompactionReplacesSegments runs Clean in a loop with
// DisableAutoClean, and the departure gate below is that test's, for its reason
// — a write count is a throughput proxy that lets machine speed decide the
// verdict, where segmentDepartures counts the event the window is made of.
func TestReadMessageSetWhileCompactionReplacesSegments(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:             tempDir(t),
		MaxSegmentBytes:  256,  // roll constantly, so a pass always has work
		Compact:          true, // the mutator under test
		DisableAutoClean: true, // driven below, not left to the interval
	})
	defer cleanup()

	var (
		wg      sync.WaitGroup
		stop    = make(chan struct{})
		bad     atomic.Value // string
		writes  atomic.Int64
		cleans  atomic.Int64
		fetches atomic.Int64
		// Closed once a record exists. An EMPTY log answers ErrSegmentNotFound
		// for offset 0 and is right to — every segment's NextOffset() is 0, so
		// there is genuinely nothing at or after the request. Without this gate
		// the fetchers beat the first append and the run died in 0.03s on the
		// one error here that is not the window under test.
		filled = make(chan struct{})
	)
	fail := func(format string, args ...any) {
		bad.CompareAndSwap(nil, fmt.Sprintf(format, args...))
	}
	stopped := func() bool {
		select {
		case <-stop:
			return true
		default:
			return false
		}
	}

	// A small key space: compaction only replaces a segment when there is
	// something superseded in it to drop.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; !stopped(); i++ {
			offs, err := l.Append([]*Message{{
				Key:   []byte(fmt.Sprintf("k:%d", i%8)),
				Value: []byte("value padding to force segment rolls"),
			}})
			if err != nil {
				fail("append: %v", err)
				return
			}
			l.SetHighWatermark(offs[0])
			if writes.Add(1) == 1 {
				close(filled)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stopped() {
			if err := l.Clean(); err != nil {
				fail("clean: %v", err)
				return
			}
			cleans.Add(1)
		}
	}()

	// From offset 0, because that is where the pass works. A follower resuming
	// near the head races nothing: the active segment is the one segment a
	// compaction pass never rewrites.
	for f := range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-filled:
			case <-stop:
				return
			}
			for !stopped() {
				if _, err := l.ReadMessageSet(0, 4096); err != nil {
					fail("fetcher %d: ReadMessageSet(0, 4096): %v", f, err)
					return
				}
				fetches.Add(1)
			}
		}()
	}

	const wantDepartures = 300
	departedAtStart := segmentDepartures.Load()
	departed := func() int64 { return segmentDepartures.Load() - departedAtStart }
	// A liveness backstop for a log where maintenance has stopped moving
	// segments at all, not a performance assertion — see the timestamp test.
	deadline := time.After(5 * time.Minute)
	for bad.Load() == nil && (departed() < wantDepartures || fetches.Load() < 3000) {
		select {
		case <-deadline:
			close(stop)
			wg.Wait()
			t.Fatalf("too few maintenance windows opened: departures=%d writes=%d fetches=%d",
				departed(), writes.Load(), fetches.Load())
		default:
		}
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()

	if v := bad.Load(); v != nil {
		t.Fatal(v.(string))
	}
	t.Logf("writes=%d cleans=%d fetches=%d departures=%d",
		writes.Load(), cleans.Load(), fetches.Load(), departed())
	require.Positive(t, cleans.Load(), "no compaction pass ran")
	require.Greater(t, fetches.Load(), int64(1000), "not enough fetches to race anything")
	require.GreaterOrEqual(t, departed(), int64(wantDepartures),
		"no segment left the log, so the window under test never opened")
}
