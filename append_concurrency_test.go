package commitlog

import (
	"fmt"
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
