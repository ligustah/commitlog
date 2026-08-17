package commitlog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Concurrent readers on a LIVE log, alongside timestamp and offset probing.
//
// This exists because the question was asked directly and deserved evidence
// rather than an assertion: multiple readers on a log that is being written are
// supported, and probing it with EarliestOffsetAfterTimestamp /
// LatestOffsetBeforeTimestamp while segments roll underneath is supported too.
//
// (A single *Reader* is still not safe for concurrent use by several
// goroutines — that is a different claim, and it is documented on the type.)
//
// The shape mirrors the reported failure: small segments so rolls are constant,
// a second reader plus a probe loop hitting the ACTIVE segment, whose index is
// legitimately empty in the window just after a roll.
func TestConcurrentReadersAndProbesOnLiveLog(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 256, // roll constantly: maximise the empty-index window
		Compact:         true,
	})
	defer cleanup()

	var (
		wg       sync.WaitGroup
		stop     = make(chan struct{})
		failures atomic.Int64
		writes   atomic.Int64
		probes   atomic.Int64
		reads    atomic.Int64
	)
	fail := func(format string, args ...any) {
		failures.Add(1)
		t.Errorf(format, args...)
	}

	// Writer: continuous appends, rolling segments the whole time.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			offs, err := l.Append([]*Message{{
				Key:   []byte(fmt.Sprintf("k:%d", i%16)),
				Value: []byte("value padding to force rolls"),
			}})
			if err != nil {
				fail("append: %v", err)
				return
			}
			l.SetHighWatermark(offs[0])
			writes.Add(1)
		}
	}()

	// Probers: the reported path. EarliestOffsetAfterTimestamp lands on the
	// active segment, whose index is empty in the window right after a roll.
	for p := 0; p < 4; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				now := timestamp()
				for _, ts := range []int64{
					0,                      // epoch: before everything
					now,                    // right now
					now + int64(time.Hour), // future: past the end
				} {
					if _, err := l.EarliestOffsetAfterTimestamp(ts); err != nil &&
						!errors.Is(err, ErrEntryNotFound) && !errors.Is(err, ErrSegmentNotFound) {
						fail("EarliestOffsetAfterTimestamp(%d): %v", ts, err)
						return
					}
					if _, err := l.LatestOffsetBeforeTimestamp(ts); err != nil &&
						!errors.Is(err, ErrEntryNotFound) && !errors.Is(err, ErrSegmentNotFound) &&
						!errors.Is(err, ErrTimestampBeforeLog) {
						fail("LatestOffsetBeforeTimestamp(%d): %v", ts, err)
						return
					}
					probes.Add(1)
				}
				// Probe HIGH offsets too, including past the end — the other
				// half of the report.
				newest := l.NewestOffset()
				for _, off := range []int64{newest, newest + 1, newest + 100} {
					if off < 0 {
						continue
					}
					for _, seg := range l.segmentsSnapshot() {
						_, _ = seg.findEntry(off) // an error is fine; a panic is not
					}
				}
			}
		}(p)
	}

	// Readers: several at once over the same live log.
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				oldest := l.OldestOffset()
				if oldest < 0 {
					continue
				}
				rd, err := l.NewReader(From(oldest))
				if err != nil {
					if errors.Is(err, ErrSegmentNotFound) {
						continue
					}
					fail("reader %d: NewReader: %v", r, err)
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				headers := make([]byte, HeaderBufferLen)
				for i := 0; i < 64; i++ {
					if _, _, _, _, err := rd.ReadMessage(ctx, headers); err != nil {
						if errors.Is(err, io.EOF) || errors.Is(err, ErrSegmentNotFound) ||
							errors.Is(err, ErrSegmentReplaced) {
							break
						}
						cancel()
						fail("reader %d: ReadMessage: %v", r, err)
						return
					}
					reads.Add(1)
				}
				cancel()
			}
		}(r)
	}

	// Runs until the floors below are MET, not for a fixed five seconds — the same
	// change TestRetentionNeverWritesIntoASliceAReaderIsHolding needed, made here
	// before it costs a release rather than after. A count paired with a fixed
	// window measures the machine: whether 100 writes land in five seconds is a
	// fact about the runner, and that test's floor of 10 came back 2 on a loaded
	// windows box. Five seconds stays as a MINIMUM because the overlap is the
	// point; what changes is the ceiling.
	//
	// The margins here are far wider than that test's — on a quiet box this makes
	// writes 3701, probes 29928 and reads 636144 against floors of 100, so 37x at
	// the narrowest against its 6.4x, because these count cheap operations rather
	// than contended boundary rewrites. Wide enough that it was not failing, not
	// wide enough to argue with: the runner that produced 2 of 10 there would
	// bring the narrowest of these to roughly the floor.
	const (
		minChurn    = 5 * time.Second
		churnBudget = 60 * time.Second
	)
	churnStart := time.Now()
	for {
		enough := writes.Load() > 100 && probes.Load() > 100 && reads.Load() > 100
		if enough && time.Since(churnStart) >= minChurn {
			break
		}
		if time.Since(churnStart) >= churnBudget {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	require.Zero(t, failures.Load(), "concurrent readers and probes must not error")
	require.Greater(t, writes.Load(), int64(100),
		"writer did not produce enough churn in %s", churnBudget)
	require.Greater(t, probes.Load(), int64(100),
		"probers did not run enough in %s", churnBudget)
	require.Greater(t, reads.Load(), int64(100),
		"readers did not read enough in %s", churnBudget)
	t.Logf("writes=%d probes=%d reads=%d", writes.Load(), probes.Load(), reads.Load())
}
