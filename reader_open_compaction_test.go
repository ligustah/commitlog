package commitlog

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Opening a reader while compaction is replacing segments.
//
// Building a reader takes two steps against the segment list — find the segment
// holding the offset, then look the offset up in its index — and Replace closes
// the old segment between them. The lookup then fails with ErrSegmentClosed for
// an offset that is valid and whose record is sitting in the replacement.
//
// TestConcurrentReadersAndProbesOnLiveLog looks like it should have covered
// this: it opens readers in a loop with Compact set. It does not, because
// nothing there ever RUNS a pass — compaction is left to the background cleaner,
// whose interval is minutes, so in a five-second test no segment is ever
// replaced. Compaction has to be driven for the window to exist at all, which is
// the whole difference between the two tests.
func TestOpeningAReaderWhileCompactionReplacesSegments(t *testing.T) {
	openWhileMaintaining(t, Options{
		Path:             tempDir(t),
		MaxSegmentBytes:  256, // roll constantly, so a pass always has work
		Compact:          true,
		DisableAutoClean: true, // driven below, not left to the interval
	})
}

// The same race with RETENTION as the mutator. A delete pass removes segments
// as it walks them and publishes the survivors only at the end, exactly like a
// compaction pass — so a reader resolving an offset mid-pass lands on a segment
// whose files are already gone. Separate from the compaction case because the
// mechanism that closes the segment is different (Delete, not Replace) and
// nothing links it to a successor.
func TestOpeningAReaderWhileRetentionDeletesSegments(t *testing.T) {
	openWhileMaintaining(t, Options{
		Path:             tempDir(t),
		MaxSegmentBytes:  256,
		MaxLogMessages:   40, // tight: every pass wants to delete
		DisableAutoClean: true,
	})
}

func openWhileMaintaining(t *testing.T, opts Options) {
	t.Helper()
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()

	var (
		wg     sync.WaitGroup
		stop   = make(chan struct{})
		bad    atomic.Value // string
		writes atomic.Int64
		cleans atomic.Int64
		opens  atomic.Int64
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
			writes.Add(1)
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

	// Openers. The assertion is only about CONSTRUCTION: a reader for a live
	// offset must be obtainable while the log compacts under it.
	for r := range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stopped() {
				oldest := l.OldestOffset()
				if oldest < 0 {
					continue
				}
				rd, err := l.NewReader(From(oldest), Uncommitted())
				if err != nil {
					// The offset can genuinely leave the log between the two
					// calls; that is retention, not a swap, and the caller is
					// expected to re-resolve.
					if errors.Is(err, ErrSegmentNotFound) {
						continue
					}
					fail("opener %d: NewReader(From(%d)): %v", r, oldest, err)
					return
				}
				_ = rd
				opens.Add(1)
			}
		}()
	}

	// Gate on DEPARTURES, not on the writes. Opening a reader is far cheaper than
	// appending, so a run bounded by opens alone finishes while the log is still
	// one segment long — nothing has left it, no pass has moved anything, and the
	// window this test exists to enter never opens. The first draft did exactly
	// that: three thousand opens against thirteen records.
	//
	// A write count is a THROUGHPUT PROXY for that condition, and it puts machine
	// speed in the verdict: the identical gate in
	// TestTimestampLookupsWhileCompactionReplacesSegments failed CI on a runner
	// roughly 2.2x slower than usual, reaching 799 of its 1000 writes inside the
	// 60s deadline while its probes ran to 898x their own threshold. This helper
	// had the same gate and simply had not drawn the short straw yet.
	//
	// DEPARTURES and not supersessions, because this helper serves BOTH mutators.
	// Retention deletes a segment and links it to nothing — its own doc above says
	// so — so a counter of replacements sits at zero for the whole retention run
	// and the gate can only ever time out. What both cases have in common is a
	// segment leaving the published list, which is what segmentDepartures counts
	// and what a reader mid-pass can trip over.
	//
	// Three hundred is calibrated, not picked: it puts these runs at 3-8s with
	// ~950-2000 appends, so the hammering the old 60-second gate bought is kept
	// while what ENDS the run is progress rather than the clock. See the note in
	// the timestamp test for why the figure is this large — deletions are far more
	// frequent than replacements, and a budget of 10 finished in under a second.
	const wantDepartures = 300
	departedAtStart := segmentDepartures.Load()
	departed := func() int64 { return segmentDepartures.Load() - departedAtStart }
	// A LIVENESS backstop for a log where maintenance has stopped moving segments
	// altogether, not a performance assertion — conflating the two is what the old
	// gate did. It costs nothing in the passing case, so it sits far enough out
	// that no merely slow machine can reach it.
	deadline := time.After(5 * time.Minute)
	for bad.Load() == nil && (departed() < wantDepartures || opens.Load() < 3000) {
		select {
		case <-deadline:
			close(stop)
			wg.Wait()
			t.Fatalf("too few maintenance windows opened: departures=%d writes=%d opens=%d",
				departed(), writes.Load(), opens.Load())
		default:
		}
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()

	if v := bad.Load(); v != nil {
		t.Fatal(v.(string))
	}
	t.Logf("writes=%d cleans=%d opens=%d departures=%d",
		writes.Load(), cleans.Load(), opens.Load(), departed())
	require.Positive(t, cleans.Load(), "no compaction pass ran")
	require.Greater(t, opens.Load(), int64(1000), "not enough readers were opened to race anything")
	// Was `writes > 500`, justified as "too little was written for a pass to have
	// superseded anything" — a guess at the number, made when nothing counted the
	// thing it was guessing at. Asserting the count itself drops both the slack
	// and the machine-speed dependency.
	require.GreaterOrEqual(t, departed(), int64(wantDepartures),
		"no segment left the log, so the window under test never opened")
}
