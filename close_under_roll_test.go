package commitlog

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A segment rolled while the log is closing must still end up closed.
//
// Closing walks l.segments under l.mu and marks the log closed. Rolling
// published the new segment in two steps, and only the second one took that
// lock — so an append still in flight when Close ran could install a segment
// AFTER the walk had gone past it. The log's own slice then named a segment
// nothing would ever close: a file handle and an index mmap held until the
// process exited, and on Windows a directory that cannot be removed.
//
// Shutdown is exactly when this happens. A process takes a signal and closes
// its log with requests still in flight; the appends that lose that race are
// the ones that were mid-roll.
func TestASegmentRolledWhileTheLogClosesIsStillClosed(t *testing.T) {
	// Repeated because it is a race, not a sequence. Once is a coin toss.
	for attempt := range 12 {
		t.Run(fmt.Sprintf("attempt-%d", attempt), func(t *testing.T) {
			dir := tempDir(t)
			l, err := New(Options{
				Path: dir,
				// Small enough that the writers below roll constantly, so the
				// close lands in the middle of a roll rather than between two.
				MaxSegmentBytes: 256,
			})
			require.NoError(t, err)

			var (
				wg    sync.WaitGroup
				start = make(chan struct{})
				stop  = make(chan struct{})
				value = []byte(strings.Repeat("x", 64))
			)
			for range 4 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					for range 4000 {
						// stop, and not only the error, is what bounds this.
						// A log that leaks a rolled segment goes on ACCEPTING
						// writes into it — the segment is open, so the append
						// succeeds — and the writers would run to completion
						// leaking one more segment per roll.
						select {
						case <-stop:
							return
						default:
						}
						// A refusal is the right answer once the log is
						// closing, and is not what this test is about.
						if _, err := l.Append([]*Message{{Value: value}}); err != nil {
							return
						}
					}
				}()
			}
			close(start)
			time.Sleep(2 * time.Millisecond)

			// Close under the load, then let the writers finish discovering
			// that the log is gone before looking at anything.
			require.NoError(t, l.(*commitLog).Close())
			close(stop)
			wg.Wait()

			cl := l.(*commitLog)
			cl.mu.RLock()
			for _, seg := range cl.segments {
				seg.RLock()
				closed := seg.closed
				seg.RUnlock()
				require.True(t, closed,
					"segment %d is in the log's own segment list and was never closed",
					seg.BaseOffset)
			}
			cl.mu.RUnlock()

			// And the proof that costs nothing to believe: on Windows an open
			// handle or a mapped index refuses the removal outright.
			require.NoError(t, os.RemoveAll(dir),
				"the log directory could not be removed after Close")
		})
	}
}

// The same invariant from the other side: the calls that BUILD a segment must
// refuse a log whose segments have already been closed.
//
// A roll is not the only way a new segment reaches l.segments. Compaction
// rewrites outside the log mutex and installs at the end; both truncations
// build a replacement for the segment they cut. Each of those installs into a
// set that closeSegments may already have walked, and a segment installed after
// the walk is one nothing will ever close.
//
// Stated directly rather than raced for, because the window for the maintenance
// calls is not the same as the roll's — they hold l.mu across their whole body,
// so what has to be ruled out is running them at all on a closed log, not
// interleaving with the close.
func TestMaintenanceOnAClosedLogBuildsNothing(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(l *commitLog) error
	}{
		{"Truncate", func(l *commitLog) error { return l.Truncate(50) }},
		{"TruncateBefore", func(l *commitLog) error { return l.TruncateBefore(50) }},
		{"Clean", func(l *commitLog) error { return l.Clean() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := tempDir(t)
			l, err := New(Options{
				Path:            dir,
				MaxSegmentBytes: 256,
				Compact:         true,
			})
			require.NoError(t, err)
			cl := l.(*commitLog)
			for i := range 200 {
				offs, err := cl.Append([]*Message{{
					Key:   []byte(fmt.Sprintf("k:%d", i%8)),
					Value: []byte(strings.Repeat("x", 48)),
				}})
				require.NoError(t, err)
				cl.SetHighWatermark(offs[0])
			}
			require.NoError(t, cl.Close())

			// Failing is the right answer. Quietly succeeding is what leaves an
			// open segment behind.
			require.ErrorIs(t, tc.run(cl), ErrCommitLogClosed,
				"%s ran on a closed log", tc.name)

			cl.mu.RLock()
			for _, seg := range cl.segments {
				seg.RLock()
				closed := seg.closed
				seg.RUnlock()
				require.True(t, closed,
					"%s left segment %d open", tc.name, seg.BaseOffset)
			}
			cl.mu.RUnlock()
			require.NoError(t, os.RemoveAll(dir),
				"the log directory could not be removed after %s", tc.name)
		})
	}
}
