package commitlog

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
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
// the boundary underneath. It asserts nothing about VALUES — the race detector
// is the assertion, so this test is only meaningful under -race.
//
// Which is exactly why the counters below are not decoration. A detector that
// finds nothing is indistinguishable from a test that never performed the
// operation under suspicion, and this one has two ways to perform none: the
// truncation's error used to be discarded outright, and the branch that
// rewrites a boundary only runs when the cut STRADDLES a sealed segment. Cut on
// a segment base instead and TruncateBefore unlinks whole segments and returns
// nil, having never touched a shared array — a clean run, and a vacuous one.
//
// So the straddle is a construction rather than a hope: the cut is one past a
// sealed segment's base, which the segment therefore spans by definition. A
// concurrent roll cannot spoil it — rolls only append, so the chosen segment
// stays sealed and stays the first one reaching the cut. The floors then say
// the rewrite ran, and that readers were in fact indexing snapshots while it did.
func TestRetentionNeverWritesIntoASliceAReaderIsHolding(t *testing.T) {
	dir := tempDir(t)
	l, cleanup := setupWithOptions(t, Options{
		Path:            dir,
		MaxSegmentBytes: 512,
	})
	defer cleanup()

	var (
		wg       sync.WaitGroup
		stop     = make(chan struct{})
		rewrites atomic.Int64
		walks    atomic.Int64
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
					// Counted only for a snapshot with something in it: an
					// empty one is a walk that indexed no shared array and so
					// could not have raced the rewrite either way.
					walks.Add(1)
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
			// A cut that lands INSIDE a segment, so the boundary is rewritten
			// rather than only whole segments dropped. Rewriting is the
			// operation that used to write in place.
			//
			// Taken from the segment set instead of from NewestOffset minus a
			// constant, because a constant cannot promise to land mid-segment:
			// one past a sealed segment's base is inside that segment as long
			// as it holds a second record, which is a thing this loop can check.
			snap := l.segmentsSnapshot()
			if len(snap) < 3 {
				time.Sleep(time.Millisecond)
				continue
			}
			s := snap[len(snap)-2] // sealed: the active one is never rewritten
			if s.LastOffset() <= s.BaseOffset {
				time.Sleep(time.Millisecond)
				continue
			}
			cut := s.BaseOffset + 1
			if err := l.TruncateBefore(cut); err == nil {
				// Counted on the OUTCOME rather than on a nil return, so the
				// count cannot quietly come to mean something else if the
				// condition selecting a boundary is ever rephrased. The log now
				// starting at cut is only half of it: the surviving segment must
				// also be a DIFFERENT object from the one that straddled the
				// cut, which is precisely what "rewritten" means and what a
				// whole-segment delete never produces. Checking only the offset
				// was tried and is wrong — move the cut onto s.BaseOffset and it
				// keeps counting, because an untouched s already starts there.
				if after := l.segmentsSnapshot(); len(after) > 0 &&
					after[0].BaseOffset == cut && after[0] != s {
					rewrites.Add(1)
				}
			}
			time.Sleep(time.Millisecond)
		}
	}()

	time.Sleep(3 * time.Second)
	close(stop)
	wg.Wait()

	// Nothing to assert about VALUES: under -race the detector fails the test,
	// and without it this is a liveness check that the log survives the churn.
	// What is asserted is that the churn HAPPENED — see the doc. Both floors are
	// far below what three seconds produces here (hundreds of rewrites, millions
	// of walks) and are sized to catch zero, not to measure the machine.
	require.GreaterOrEqual(t, l.NewestOffset(), int64(0), "the log wrote nothing")
	require.Greater(t, rewrites.Load(), int64(10),
		"retention never rewrote a boundary segment, so nothing here could have raced "+
			"a reader's snapshot and a clean run proves nothing")
	require.Greater(t, walks.Load(), int64(100),
		"no reader ever indexed a non-empty snapshot, so there was nothing to race")
	t.Logf("boundary rewrites=%d snapshot walks=%d", rewrites.Load(), walks.Load())
}
