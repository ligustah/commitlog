package commitlog

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TruncateBefore(f) keeps f. So a floor that has been published and not raised
// since must stay readable: a reader opened at f has to come back with f.
//
// It did not. It came back 1, 2 or 3 records late while OldestOffset() still
// answered f — the log asserting a record is there while a read from it starts
// past it, with no error anywhere. Reported from durable_streams against
// v0.47.0, where it was the consumer-facing shape of retention: a consumer
// resuming at its own published floor silently skipped the first records after
// it. It had been carried as an unreproducible CI sighting for weeks.
//
// The race needs a reader that resolved into the boundary segment just before
// the trim replaced it, so the test runs the truncator with NO sleep. That is
// deliberate and not a stress artefact: TruncateBefore is idempotent by
// contract, so hammering it has to be safe.
//
// Retires on DANGER rather than on a write count — a run that never truncated,
// never moved the floor or never took a read against a still floor proves
// nothing, and would pass just as well against the bug.
//
// Two of those dangers used to be assumed rather than counted. "A truncation
// ran" is not "a truncation REWROTE the boundary segment": a cut landing on a
// segment's own base drops whole segments and never builds the replacement a
// reader can be holding the original of. And "a read was taken" is not "a read
// overlapped a trim in flight". Both are counted and floored now, so a change
// that stops the truncator racing the readers fails here instead of passing on
// twelve thousand reads of a quiet log.
func TestChaosAReadFromThePublishedFloorStartsAtIt(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:             tempDir(t),
		MaxSegmentBytes:  256, // roll constantly; the cut lands INSIDE a segment
		DisableAutoClean: true,
	})
	defer cleanup()

	// The floor trails the tail, so there is always something below it to
	// collect and the boundary segment is always straddled.
	const (
		lag         = 120
		readers     = 12
		minAdvances = 40
		minChecked  = 12000
		// Measured over six runs: rewrites 19-32, duringTrim 11706-11789 of
		// ~12030 checked. The rewrite spread tracks how loaded the box is --
		// the low end came from a run sharing the machine. Both floors sit well
		// under the low end, so a loaded runner does not fail on its own
		// precondition — but both are far enough above
		// zero that the danger cannot quietly evaporate. duringTrim runs at 98%
		// of checked because the truncator hammers without a sleep; the floor is
		// there to notice if it ever stops doing so.
		minRewrites   = 10
		minDuringTrim = 2000
	)

	var (
		stop      = make(chan struct{})
		wg        sync.WaitGroup
		violation atomic.Value // string
		floor     atomic.Int64
		writes    atomic.Int64
		truncs    atomic.Int64
		checked   atomic.Int64
		advances  atomic.Int64
		// The two dangers this test was built around, counted rather than
		// assumed. `truncs` and `checked` say a truncation ran and a read was
		// taken; neither says the truncation REWROTE the boundary segment (a
		// cut on a segment's own base only drops whole ones) or that any read
		// overlapped one in flight. Without both, a change that stopped the
		// truncator racing the readers leaves this passing on twelve thousand
		// reads of a quiet log.
		rewrites   atomic.Int64
		duringTrim atomic.Int64
		// trimSeq increments once per TruncateBefore call and trimming is true
		// for its duration. A reader that sees trimming both before it opens
		// and after it has read, with the SAME seq at both ends, ran entirely
		// inside one trim — which a pair of booleans alone cannot establish,
		// since the truncator hammers and two adjacent trims read the same.
		trimSeq  atomic.Int64
		trimming atomic.Bool
	)
	floor.Store(-1)
	fail := func(format string, args ...any) {
		violation.CompareAndSwap(nil, fmt.Sprintf(format, args...))
	}
	stopped := func() bool {
		select {
		case <-stop:
			return true
		default:
		}
		return false
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := int64(0); !stopped(); n++ {
			offs, err := l.Append([]*Message{{
				Key:   []byte(fmt.Sprintf("k:%d", n%16)),
				Value: []byte(strconv.FormatInt(n, 10) + ":padding to force segment rolls"),
			}})
			if err != nil {
				fail("append %d: %v", n, err)
				return
			}
			l.SetHighWatermark(offs[len(offs)-1])
			writes.Add(1)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stopped() {
			if want := l.NewestOffset() - lag; want > floor.Load() {
				floor.Store(want)
				advances.Add(1)
			}
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stopped() {
			f := floor.Load()
			if f <= 0 {
				time.Sleep(time.Millisecond)
				continue
			}
			// The segment holding f before the call. If f is that segment's own
			// base there is nothing to rewrite, and the iteration simply does
			// not count towards the rewrite floor.
			var boundary *segment
			for _, s := range l.segmentsSnapshot() {
				if s.BaseOffset < f && f <= s.LastOffset() {
					boundary = s
					break
				}
			}
			trimSeq.Add(1)
			trimming.Store(true)
			err := l.TruncateBefore(f)
			trimming.Store(false)
			if err != nil {
				fail("TruncateBefore(%d): %v", f, err)
				return
			}
			truncs.Add(1)
			// Counted on identity, not on the resulting base offset: an
			// untouched segment already starting at f satisfies the offset
			// alone, so that check would score a whole-segment delete as a
			// rewrite.
			if after := l.segmentsSnapshot(); boundary != nil && len(after) > 0 &&
				after[0].BaseOffset == f && after[0] != boundary {
				rewrites.Add(1)
			}
		}
	}()

	ctx := context.Background()
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			headers := make([]byte, HeaderBufferLen)
			for !stopped() {
				f := floor.Load()
				if f <= 0 {
					time.Sleep(time.Millisecond)
					continue
				}
				seq0, inTrim := trimSeq.Load(), trimming.Load()
				r, err := l.NewReader(From(f))
				if err != nil {
					if floor.Load() != f {
						continue // the floor moved; f became collectible
					}
					fail("opening a reader at the published floor %d failed: %v", f, err)
					return
				}
				_, off, _, _, err := r.ReadMessage(ctx, headers)
				after := floor.Load()
				if err != nil {
					if after != f {
						continue
					}
					fail("reading from the published floor %d failed: %v", f, err)
					return
				}
				// Nothing is owed once the floor has moved: f became
				// collectible the instant it did.
				if after != f {
					continue
				}
				checked.Add(1)
				if inTrim && trimming.Load() && trimSeq.Load() == seq0 {
					duringTrim.Add(1)
				}
				if off != f {
					fail("a read from the published floor %d came back starting at %d "+
						"(oldest=%d newest=%d)", f, off, l.OldestOffset(), l.NewestOffset())
					return
				}
			}
		}()
	}

	unmet := func() string {
		switch {
		case truncs.Load() == 0:
			return "the truncator never ran"
		case l.OldestOffset() <= 0:
			return "nothing was ever deleted"
		case advances.Load() <= minAdvances:
			return fmt.Sprintf("the floor advanced %d times, below the floor of %d",
				advances.Load(), minAdvances)
		case checked.Load() <= minChecked:
			return fmt.Sprintf("only %d reads were taken against a still floor, "+
				"below the floor of %d", checked.Load(), minChecked)
		case rewrites.Load() <= minRewrites:
			return fmt.Sprintf("only %d truncations rewrote the boundary segment, "+
				"below the floor of %d — whole-segment deletes cannot reproduce "+
				"this bug", rewrites.Load(), minRewrites)
		case duringTrim.Load() <= minDuringTrim:
			return fmt.Sprintf("only %d reads ran inside a truncation, below the "+
				"floor of %d — reads that never overlap a trim are not racing it",
				duringTrim.Load(), minDuringTrim)
		}
		return ""
	}

	var reason string
	deadline := time.After(120 * time.Second)
	for violation.Load() == nil {
		reason = unmet()
		if reason == "" {
			break
		}
		select {
		case <-deadline:
			close(stop)
			wg.Wait()
			t.Fatalf("the run never became dangerous enough to assert on: %s", reason)
		default:
			time.Sleep(time.Millisecond)
		}
	}
	close(stop)
	wg.Wait()

	t.Logf("writes=%d truncations=%d advances=%d checked=%d rewrites=%d duringTrim=%d oldest=%d newest=%d",
		writes.Load(), truncs.Load(), advances.Load(), checked.Load(),
		rewrites.Load(), duringTrim.Load(), l.OldestOffset(), l.NewestOffset())
	if v := violation.Load(); v != nil {
		t.Fatal(v.(string))
	}
}
