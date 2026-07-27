package commitlog

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// syncsPerCommit runs `writers` concurrent committers, each appending and then
// syncing the offset it was given, and returns the fsyncs paid per commit.
func syncsPerCommit(t *testing.T, writers, perWriter int) float64 {
	t.Helper()

	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 1 << 30, // one segment, so a flush is one fsync
	})
	t.Cleanup(cleanup)

	var fsyncs int64
	l.mu.RLock()
	for _, seg := range l.segments {
		seg.Lock()
		seg.backing = &atomicCountingBacking{segmentBacking: seg.backing, n: &fsyncs}
		seg.Unlock()
	}
	l.mu.RUnlock()

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				offs, err := l.Append([]*Message{{Value: []byte("v")}})
				if err != nil {
					t.Error(err)
					return
				}
				if err := l.Sync(offs[0]); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()

	per := float64(atomic.LoadInt64(&fsyncs)) / float64(writers*perWriter)
	l.syncMu.Lock()
	leaders, followers := l.syncLeaders, l.syncFollowers
	l.syncMu.Unlock()
	t.Logf("writers=%-3d commits=%-5d fsyncs=%-5d  fsyncs/commit=%.3f  "+
		"led=%d rode=%d",
		writers, writers*perWriter, fsyncs, per, leaders, followers)
	return per
}

// Batching must get BETTER as committers are added — that is the entire claim
// Sync makes, and the property that broke: a barrier which flushed the moment
// it took leadership left 98% of concurrent committers leading their own flush,
// so the cost per commit stayed flat from 4 writers up and a consumer measured
// it slower than batching above the log.
//
// The bar is deliberately about the SHAPE (does it improve with concurrency?)
// rather than an absolute number, which would encode this machine's disk.
func TestSyncBatchingImprovesWithConcurrency(t *testing.T) {
	const perWriter = 50

	few := syncsPerCommit(t, 4, perWriter)
	many := syncsPerCommit(t, 64, perWriter)

	require.Less(t, many, few/4,
		"batching must improve materially with concurrency: 64 writers paid "+
			"%.3f fsyncs per commit against %.3f at 4 — flat cost means the "+
			"barrier is not accumulating and callers are better off batching "+
			"above the log", many, few)
	require.Less(t, many, 0.25,
		"64 concurrent committers should share flushes heavily, not pay "+
			"%.3f fsyncs each", many)
}

// Letting callers queue behind the flush in flight is NOT enough on its own —
// the obvious design, and the one shipped in v0.22.0. It captures only callers
// that arrive DURING an fsync, and on a fast disk that is a sliver of the
// cycle: everyone else arrives in the gap after a flush ends, finds no flush in
// flight, and immediately leads one of their own.
//
// Measured at 64 concurrent committers, with and without the leader's window:
//
//	no window: 2323 flushes led, 1011 rides  (0.73 fsyncs/commit)
//	window:      51 flushes led, 3149 rides  (0.016 fsyncs/commit)
//
// This test pins the consequence — most committers must RIDE a flush rather
// than lead one — so a future change that drops the window fails here with the
// reason attached rather than as an unexplained slowdown downstream.
func TestMostCommittersRideAFlushRatherThanLeadOne(t *testing.T) {
	const (
		writers   = 64
		perWriter = 50
	)

	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 1 << 30,
	})
	t.Cleanup(cleanup)

	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				offs, err := l.Append([]*Message{{Value: []byte("v")}})
				if err != nil {
					t.Error(err)
					return
				}
				if err := l.Sync(offs[0]); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()

	l.syncMu.Lock()
	leaders, followers := l.syncLeaders, l.syncFollowers
	l.syncMu.Unlock()
	t.Logf("led=%d rode=%d", leaders, followers)

	require.Greater(t, followers, leaders*5,
		"only %d of %d committers rode someone else's flush; the leader is not "+
			"holding the door open long enough for a batch to form",
		followers, leaders+followers)
}
