package commitlog

import (
	"sync"
	"sync/atomic"
	"testing"
)

// TestProbeSyncBatching is an instrument, not an assertion: it reports how the
// barrier actually behaves under concurrent committers so the batching can be
// reasoned about from data rather than from the shape of the code.
func TestProbeSyncBatching(t *testing.T) {
	for _, writers := range []int{1, 4, 16, 64} {
		writers := writers
		t.Run("", func(t *testing.T) {
			const perWriter = 50

			l, cleanup := setupWithOptions(t, Options{
				Path:            tempDir(t),
				MaxSegmentBytes: 1 << 30,
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

			var (
				wg      sync.WaitGroup
				covered int64 // returned without leading a flush
				waited  int64 // parked on someone else's flush at least once
			)
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
						before := atomic.LoadInt64(&fsyncs)
						if err := l.Sync(offs[0]); err != nil {
							t.Error(err)
							return
						}
						if atomic.LoadInt64(&fsyncs) == before {
							atomic.AddInt64(&covered, 1)
						}
						_ = waited
					}
				}()
			}
			wg.Wait()

			total := int64(writers * perWriter)
			t.Logf("writers=%d commits=%d fsyncs=%d  fsyncs/commit=%.3f  "+
				"commits covered without any flush=%d (%.0f%%)",
				writers, total, fsyncs, float64(fsyncs)/float64(total),
				covered, 100*float64(covered)/float64(total))
		})
	}
}
