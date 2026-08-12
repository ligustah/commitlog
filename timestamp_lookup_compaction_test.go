package commitlog

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A timestamp lookup must survive a compaction pass running underneath it.
//
// A pass mutates segments long before the log publishes the result: Replace
// renames the rewrite over its source and marks the source `replaced`,
// cleanupEmptySegment deletes a segment whose every record was superseded, and
// l.segments is swapped once, at the very END of the pass. So for the whole of
// it the log hands out segments that are replaced or gone.
//
// That is precisely what current() exists to resolve, and the OFFSET path goes
// through it — findSegment resolves every candidate and skips the ones the pass
// removed. The TIMESTAMP path never learned. earliestOffsetAfterTimestampLocked
// walked l.segments[i] directly, so findEntryByTimestamp was called on a segment
// the pass had already replaced and answered ErrSegmentReplaced, which the loop
// correctly refuses to read as "not in this segment" and returns as an error.
//
// Both public lookups fail that way, because LatestOffsetBeforeTimestamp is
// defined in terms of the same loop. The result is a resume-by-timestamp that
// fails at random on a healthy compacting log, for records that are sitting in
// the replacement.
//
// It surfaced as a flake in TestReopenAfterSealingAnEmptyIndexIsUsable — which
// sets Compact and appends 200 records under ONE key, so a pass collapses whole
// segments — failing once in the full suite and never in 150 isolated runs. That
// ratio is the tell that this is a window, not a wrong answer.
//
// The pass has to be DRIVEN for the window to exist at all: the background
// cleaner's interval is minutes, so a test that merely sets Compact and waits
// never replaces a segment. Same reason TestOpeningAReaderWhileCompactionReplacesSegments
// runs Clean in a loop with DisableAutoClean.
func TestTimestampLookupsWhileCompactionReplacesSegments(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:             tempDir(t),
		MaxSegmentBytes:  256,  // roll constantly, so a pass always has work
		Compact:          true, // the mutator under test
		DisableAutoClean: true, // driven below, not left to the interval
	})
	defer cleanup()

	var (
		wg     sync.WaitGroup
		stop   = make(chan struct{})
		bad    atomic.Value // string
		writes atomic.Int64
		cleans atomic.Int64
		probes atomic.Int64
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

	// Both lookups, because they fail together: LatestOffsetBeforeTimestamp is
	// earliestOffsetAfterTimestampLocked with the target shifted by one.
	//
	// The targets are chosen to walk the WHOLE segment list rather than stop at
	// the first candidate — 0 is before every record, so the search starts at the
	// oldest segment, the one a pass is most likely to have just rewritten; an
	// hour ahead is after every record, so every segment reports not-found and
	// the loop visits all of them. Retention is off, so neither can legitimately
	// fail: there is no window in which a record leaves the log entirely, and
	// ErrTimestampBeforeLog is unreachable with a target in the future.
	for p := range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stopped() {
				if _, err := l.EarliestOffsetAfterTimestamp(0); err != nil {
					fail("prober %d: EarliestOffsetAfterTimestamp(0): %v", p, err)
					return
				}
				if _, err := l.LatestOffsetBeforeTimestamp(timestamp() + 3600_000_000_000); err != nil {
					fail("prober %d: LatestOffsetBeforeTimestamp(now+1h): %v", p, err)
					return
				}
				probes.Add(2)
			}
		}()
	}

	// Gate on DEPARTURES, not on a write count. A run bounded by probes alone
	// finishes while the log is still one segment long — nothing superseded, no
	// pass replacing anything, and the window never opens — so something has to
	// hold the test open until it does. A write count is a THROUGHPUT PROXY for
	// that, and it made machine speed decide the verdict: CI on a runner ~2.2x
	// slower than usual (1058s for this package against a 450-500s band) reached
	// 799 of 1000 writes inside a 60s deadline and failed, while probes ran to
	// 2,693,442 — 898x their own threshold. Nothing about that verdict concerned
	// the property under test.
	//
	// segmentDepartures counts the event the window is made of, so this waits
	// for the window to have opened rather than for the machine to be fast. On a
	// slower machine the same 300 windows simply take longer to accumulate: the
	// exercise stays constant and only the wall clock moves.
	//
	// Three hundred is calibrated against two measurements, and both mattered.
	//
	// REPLACEMENTS alone are front-loaded and then asymptotic — 3 within the first
	// half-second, but only 19-26 after 140 seconds, as each Clean() pass slows
	// over a longer segment list. A budget of 100 counting only those was simply
	// unreachable and timed out. So a departure counter that ignored deletions
	// could not be given a large budget at all.
	//
	// Counting DELETIONS too (which is what a mid-pass reader trips over just the
	// same) changes the supply completely: departures then arrive fast enough that
	// 10 finished in under a second — far less hammering than the 60-second gate
	// this replaces, and a weaker test for it. 300 puts the run at 4-8s with ~1300
	// appends, keeping the exercise while letting PROGRESS end the run.
	const wantDepartures = 300
	departedAtStart := segmentDepartures.Load()
	departed := func() int64 { return segmentDepartures.Load() - departedAtStart }
	// Five minutes for a run that takes seconds. The deadline is a LIVENESS
	// backstop for a log where maintenance has stopped moving segments at all,
	// not a performance assertion — that confusion is what the old gate was.
	// Costing nothing in the passing case, it should sit far enough out that no
	// merely slow machine can reach it.
	deadline := time.After(5 * time.Minute)
	for bad.Load() == nil && (departed() < wantDepartures || probes.Load() < 3000) {
		select {
		case <-deadline:
			close(stop)
			wg.Wait()
			t.Fatalf("too few maintenance windows opened: departures=%d writes=%d probes=%d",
				departed(), writes.Load(), probes.Load())
		default:
		}
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()

	if v := bad.Load(); v != nil {
		t.Fatal(v.(string))
	}
	t.Logf("writes=%d cleans=%d probes=%d departures=%d",
		writes.Load(), cleans.Load(), probes.Load(), departed())
	require.Positive(t, cleans.Load(), "no compaction pass ran")
	require.Greater(t, probes.Load(), int64(1000), "not enough lookups to race anything")
	// Was `writes > 500`, justified as "too little was written for a pass to have
	// superseded anything" — a guess at this number, made when nothing counted
	// it. The guess is measurably wrong in the generous direction: 64 writes
	// produced 3 departures here. Asserting the count itself removes both the
	// slack and the machine-speed dependency the proxy carried.
	require.GreaterOrEqual(t, departed(), int64(wantDepartures),
		"no segment left the log, so the window under test never opened")
}
