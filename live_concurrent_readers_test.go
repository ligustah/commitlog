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

	time.Sleep(5 * time.Second)
	close(stop)
	wg.Wait()

	require.Zero(t, failures.Load(), "concurrent readers and probes must not error")
	require.Greater(t, writes.Load(), int64(100), "writer did not produce enough churn")
	require.Greater(t, probes.Load(), int64(100), "probers did not run enough")
	require.Greater(t, reads.Load(), int64(100), "readers did not read enough")
	t.Logf("writes=%d probes=%d reads=%d", writes.Load(), probes.Load(), reads.Load())
}
