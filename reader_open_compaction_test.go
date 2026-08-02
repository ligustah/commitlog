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

	// Gate on the WRITES as well as the opens. Opening a reader is far cheaper
	// than appending, so a run bounded by opens alone finishes while the log is
	// still one segment long — nothing has been superseded, no pass replaces
	// anything, and the window this test exists to enter never opens. The first
	// draft did exactly that: three thousand opens against thirteen records.
	deadline := time.After(60 * time.Second)
	for bad.Load() == nil && (writes.Load() < 1000 || opens.Load() < 3000) {
		select {
		case <-deadline:
			close(stop)
			wg.Wait()
			t.Fatalf("too slow: writes=%d opens=%d", writes.Load(), opens.Load())
		default:
		}
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()

	if v := bad.Load(); v != nil {
		t.Fatal(v.(string))
	}
	t.Logf("writes=%d cleans=%d opens=%d", writes.Load(), cleans.Load(), opens.Load())
	require.Positive(t, cleans.Load(), "no compaction pass ran")
	require.Greater(t, opens.Load(), int64(1000), "not enough readers were opened to race anything")
	require.Greater(t, writes.Load(), int64(500),
		"too little was written for a pass to have superseded anything")
}
