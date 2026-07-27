package commitlog

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// Concurrent appends must each get their own offset. Append reads the active
// segment's NextOffset and Position to build the message set, then writes —
// and if those two steps are not atomic with respect to each other, two
// appends racing on one log read the same tail and are both stamped with it.
// The log then holds two records claiming the same offset, written over
// overlapping byte ranges: silent corruption of the offset sequence, with no
// error to either caller.
func TestConcurrentAppendsGetDistinctOffsets(t *testing.T) {
	const writers = 32

	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 1 << 20, // one segment: no rolls to muddy the result
	})
	defer cleanup()

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		offs []int64
	)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got, err := l.Append([]*Message{{
				Key:   []byte(fmt.Sprintf("k%d", i)),
				Value: []byte(fmt.Sprintf("v%d", i)),
			}})
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			offs = append(offs, got...)
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	require.Len(t, offs, writers)
	seen := map[int64]int{}
	for _, o := range offs {
		seen[o]++
	}
	for off, n := range seen {
		require.Equal(t, 1, n,
			"offset %d was handed to %d concurrent appends — the log's offset "+
				"sequence is corrupt and the records overlap on disk", off, n)
	}
	require.EqualValues(t, writers-1, l.NewestOffset(),
		"every append must advance the tail exactly once")
}

// The same race seen from the log's own bookkeeping: the tail must account for
// every record written, so a reader walking from oldest to newest sees exactly
// as many records as there were appends.
func TestConcurrentAppendsLeaveAReadableLog(t *testing.T) {
	const writers = 32

	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 1 << 20,
	})
	defer cleanup()

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := l.Append([]*Message{{
				Key:   []byte(fmt.Sprintf("k%d", i)),
				Value: []byte(fmt.Sprintf("v%d", i)),
			}}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	l.SetHighWatermark(l.NewestOffset())

	got := readFrom(t, l)
	require.Len(t, got, writers,
		"a record per append must be readable; a short count means appends "+
			"overwrote each other")
}

// A segment roll must not run concurrently with an append. The cleaner loop
// rolls on its own ticker, independently of any append, and split builds the
// new segment before swapping it in — but "refuse if the file already exists"
// and "create the file" are two steps, so two rollers can both end up holding a
// segment over the SAME files. The one that loses the compare-and-swap is then
// discarded with a best-effort Delete, which closes and unlinks files the
// winner is actively using.
//
// On Windows that unlink fails, the error is swallowed, and the log is left
// with a handle and mapping nothing will ever close — which is what this test
// detects, by requiring the log to shut down and its directory to be removable.
// On POSIX it is worse and quieter: unlinking an open file succeeds, so the
// live active segment's files are removed out from under it with no error
// anywhere.
//
// This drives the same call the cleaner loop makes rather than waiting on its
// ticker, so the interleaving is exercised deterministically.
func TestSegmentRollDoesNotRaceInFlightAppends(t *testing.T) {
	const (
		writers = 16
		each    = 20
	)

	dir := tempDir(t)
	l, err := New(Options{
		Path:             dir,
		MaxSegmentBytes:  128, // roll constantly
		DisableAutoClean: true,
	})
	require.NoError(t, err)
	cl := l.(*commitLog)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// The cleaner loop's split check, driven concurrently. Tracked on its OWN
	// WaitGroup: it only stops when signalled, so waiting for it alongside the
	// appenders would deadlock. It yields between iterations so a tight loop on
	// appendMu cannot starve them.
	var splitter sync.WaitGroup
	splitter.Add(1)
	go func() {
		defer splitter.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := cl.checkAndPerformSplitLocked(); err != nil {
				t.Error(err)
				return
			}
			runtime.Gosched()
		}
	}()

	var (
		mu   sync.Mutex
		offs []int64
	)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < each; j++ {
				got, err := l.Append([]*Message{{
					Key:   []byte(fmt.Sprintf("k%d-%d", i, j)),
					Value: []byte(fmt.Sprintf("v%d-%d", i, j)),
				}})
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				offs = append(offs, got...)
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	close(stop)
	splitter.Wait()

	seen := map[int64]int{}
	for _, o := range offs {
		seen[o]++
	}
	for off, n := range seen {
		require.Equal(t, 1, n,
			"offset %d was handed out %d times — a segment roll landed inside an "+
				"append, so two segments claim it", off, n)
	}
	require.Len(t, seen, writers*each)

	// Every record must still be readable. This is the assertion that catches a
	// roll landing inside an append: the append writes into the segment it read,
	// which by then is no longer the active one, so its record either collides
	// with one the new segment hands out at the same offset or is stranded
	// behind it.
	l.SetHighWatermark(l.NewestOffset())
	require.Len(t, readFrom(t, l), writers*each,
		"a record per append must be readable after concurrent rolls")

	// A strictly increasing offset partition, with no segment starting where
	// another has already written.
	func() {
		cl.mu.RLock()
		defer cl.mu.RUnlock()
		for i := 1; i < len(cl.segments); i++ {
			prev, cur := cl.segments[i-1], cl.segments[i]
			require.Greater(t, cur.BaseOffset, prev.BaseOffset,
				"segments %d and %d start at the same offset", i-1, i)
			require.GreaterOrEqual(t, cur.BaseOffset, prev.NextOffset(),
				"segment %d starts at %d, inside segment %d which already reaches %d",
				i, cur.BaseOffset, i-1, prev.NextOffset())
		}
	}()

	// The assertion that actually catches the discarded roll: every file the log
	// ever opened must be closed, so the log shuts down and its directory can be
	// removed. A segment built by a roll that lost the compare-and-swap is
	// dropped without ever being closed, and its handle and mapping keep the
	// directory alive.
	require.NoError(t, l.Close())
	require.NoError(t, os.RemoveAll(dir),
		"a leaked segment handle from a discarded roll keeps the directory open")
}
