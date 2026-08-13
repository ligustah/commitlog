package commitlog

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A segment slice handed to a reader must never be written to again.
//
// segmentsSnapshot() returns the slice HEADER, not a copy — deliberately, because it is
// called on the path of every read and copying there would allocate per call.
// The cost of that choice is an obligation on the other side: whoever changes
// the segment set publishes a NEW array rather than writing into the one
// readers are already indexing. Truncate has always done that. TruncateBefore
// did not, and replaced its boundary segment in place while lock-free readers
// were indexing the same backing array.
//
// Reported downstream, red under -race there. This is the commitlog-level
// version: readers taking snapshots and indexing them while retention rewrites
// the boundary underneath. It asserts nothing by itself — the race detector is
// the assertion, so this test is only meaningful under -race.
func TestRetentionNeverWritesIntoASliceAReaderIsHolding(t *testing.T) {
	dir := tempDir(t)
	l, cleanup := setupWithOptions(t, Options{
		Path:            dir,
		MaxSegmentBytes: 512,
	})
	defer cleanup()

	var (
		wg   sync.WaitGroup
		stop = make(chan struct{})
	)

	// Writers, to keep segments rolling so retention always has something to
	// cut and a boundary segment to rewrite.
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
				Value: []byte(strings.Repeat("x", 64)),
			}})
			if err != nil {
				return
			}
			l.SetHighWatermark(offs[0])
		}
	}()

	// Readers, doing exactly what the race needs: hold a snapshot and index it
	// without the log mutex, which is what every lock-free lookup does.
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				snapshot := l.segmentsSnapshot()
				for _, seg := range snapshot {
					_ = seg.BaseOffset
					_ = seg.LastOffset()
				}
				if len(snapshot) > 0 {
					_ = findSegmentAfter(snapshot, snapshot[0])
				}
			}
		}()
	}

	// And a full read through the public surface, so the ordinary path is
	// exercised too rather than only the direct slice walk.
	wg.Add(1)
	go func() {
		defer wg.Done()
		hdr := make([]byte, HeaderBufferLen)
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
			r, err := l.NewReader(From(oldest))
			if err != nil {
				continue
			}
			for range 32 {
				if _, _, _, _, err := r.ReadMessage(context.Background(), hdr); err != nil {
					break
				}
			}
		}
	}()

	// Retention, rewriting the boundary segment over and over.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if newest := l.NewestOffset(); newest > 40 {
				// A cut that lands INSIDE a segment, so the boundary is
				// rewritten rather than only whole segments dropped. Rewriting
				// is the operation that used to write in place.
				_ = l.TruncateBefore(newest - 20)
			}
			time.Sleep(time.Millisecond)
		}
	}()

	time.Sleep(3 * time.Second)
	close(stop)
	wg.Wait()

	// Nothing to assert: under -race the detector fails the test, and without
	// it this is a liveness check that the log survives the churn.
	require.GreaterOrEqual(t, l.NewestOffset(), int64(0), "the log wrote nothing")
}
