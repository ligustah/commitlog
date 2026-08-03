package commitlog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// A follower held open across compaction and retention passes, for as long as
// the log lives.
//
// Every other test that reads a maintaining log opens a reader, reads, and
// closes it. That covers reader CONSTRUCTION against a log being mutated —
// which is where two defects were found — but never the scan's own segment
// jump. A reader that runs off the end of one segment asks the log for the next
// one and switches to it in place, and that lookup is the only one on the read
// path that does not resolve a departed segment to its successor. Nothing
// reached it before, because a short read finishes inside the segment it
// started in.
//
// The invariant needs no oracle. One writer appends an increasing sequence and
// stores each number as the record's value, and both cleaners only ever REMOVE
// records — compaction drops superseded ones, retention drops a prefix. So
// whatever a forward reader gets back is a subsequence of what was written, and
// the numbers it sees must strictly increase. A jump that lands on the wrong
// segment shows up as a number that does not, whether the segment is stale
// (numbers repeat) or the wrong one entirely (numbers jump backwards).
//
// A follower can also lawfully lose its place: it stalls, retention collects
// past it, and it resumes at the oldest surviving record instead. That skips
// records, which is the point of a retention limit, and the number sequence has
// to survive it — a resume must never make a number repeat. The run asserts it
// happened (`overtaken`), because a follower that always keeps up never resumes
// anywhere but where it left off.
//
// It also handles being told to re-resolve mid-scan, which in practice does not
// happen: the counter stays at zero on every run. Worth keeping as a fact about
// the log rather than deleting as dead code — it says the read path answers a
// follower without ever surfacing a maintenance error to it.
//
// Run over every storage format. A block-compressed segment is a different
// storage path end to end — records live inside compressed blocks, the index
// holds sparse anchors into the decompressed stream rather than file positions,
// and a read decodes a whole block to reach one record. Maintenance rewrites
// those blocks. None of it had ever run while something was reading.
func TestChaosAFollowerNeverSeesTheSequenceGoBackwards(t *testing.T) {
	for _, codec := range []compress.Codec{compress.None, compress.Snappy, compress.Zstd} {
		t.Run(fmt.Sprintf("codec=%d", codec), func(t *testing.T) {
			followerNeverSeesTheSequenceGoBackwards(t, codec)
		})
	}
}

func followerNeverSeesTheSequenceGoBackwards(t *testing.T, codec compress.Codec) {
	l, cleanup := setupWithOptions(t, Options{
		Path: tempDir(t),
		// Small enough to roll every few records, so a follower crosses segments
		// constantly and the jump is on the hot path rather than an event.
		MaxSegmentBytes: 256,
		// Both cleaners armed, retention tight enough that a follower which
		// stalls for a moment is overtaken by it. A follower that always keeps up
		// never has to resume anywhere but where it left off, and the resume path
		// is half of what a real consumer does.
		Compact:        true,
		MaxLogMessages: 50,
		Compression:    codec,
	})
	defer cleanup()

	const (
		hotKeys  = 12   // rewritten constantly, so compaction has work
		enough   = 3000 // records the writer must land before the run retires
		maxSteps = 64   // messages per reader lifetime before it is rebuilt
	)

	var (
		stop      = make(chan struct{})
		wg        sync.WaitGroup
		violation atomic.Value // string
		writes    atomic.Int64
		cleans    atomic.Int64
		got       atomic.Int64 // messages the follower returned
		rebuilds  atomic.Int64 // times the log told it to re-resolve
		overtaken atomic.Int64 // resumes where retention had passed the follower
		crossed   atomic.Int64 // reads that spanned a segment boundary
	)
	fail := func(format string, args ...any) {
		violation.CompareAndSwap(nil, fmt.Sprintf(format, args...))
	}
	stopped := func() bool {
		select {
		case <-stop:
			return true
		default:
		}
		return false
	}

	// The writer. One goroutine, so the sequence it produces is totally ordered
	// and the reader's check needs nothing shared.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := int64(1); !stopped(); n++ {
			offs, err := l.Append([]*Message{{
				Key:   []byte(fmt.Sprintf("k:%d", n%hotKeys)),
				Value: []byte(strconv.FormatInt(n, 10) + ":padding to force segment rolls"),
			}})
			if err != nil {
				fail("append %d: %v", n, err)
				return
			}
			l.SetHighWatermark(offs[0])
			writes.Add(1)
		}
	}()

	// Maintenance: compaction and retention, one pass per call, as fast as it
	// will go.
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

	// The follower. Reads forward forever, rebuilding when the log tells it to,
	// and asserting only that the numbers never go backwards.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			<-stop
			cancel()
		}()

		headers := make([]byte, HeaderBufferLen)
		last := int64(0)
		lastOffset := int64(-1)
		for life := 0; !stopped(); life++ {
			// Stall now and then, long enough that retention runs past where the
			// follower stopped. A consumer that pauses — for a slow handler, a
			// rebalance, a GC — and comes back to find its position collected is
			// the ordinary case, not an exotic one.
			// Long enough that the writer lands more records than retention
			// keeps, so the follower's position is certainly collected rather
			// than nearly collected. At 250ms it came back ten offsets short of
			// being overtaken, run after run — close enough to look covered and
			// never actually be.
			if life%6 == 5 {
				select {
				case <-stop:
					return
				case <-time.After(800 * time.Millisecond):
				}
			}
			// Resume where we got to, or at the oldest surviving record if
			// retention has already passed it. Both are ordinary consumer
			// behaviour; neither may make the sequence repeat.
			from := l.OldestOffset()
			if lastOffset >= 0 && lastOffset+1 > from {
				from = lastOffset + 1
			} else if lastOffset >= 0 {
				overtaken.Add(1)
			}
			rd, err := l.NewReader(From(from), Follow())
			if err != nil {
				if segmentSwapped(err) || errors.Is(err, ErrSegmentNotFound) {
					rebuilds.Add(1)
					continue
				}
				fail("NewReader(from=%d): %v", from, err)
				return
			}

			startSeg := int64(-1)
			for step := 0; step < maxSteps && !stopped(); step++ {
				// Stall WITHIN a scan, not only between them. A follower that
				// only ever pauses between readers is never holding a segment
				// when retention comes for it, so the mid-scan departure — the
				// case the reader answers with "re-resolve" — never happens. The
				// period is coprime to the lifetime so the stalls do not land at
				// the same point in every reader.
				if step%17 == 16 {
					select {
					case <-stop:
						return
					case <-time.After(100 * time.Millisecond):
					}
				}
				msg, offset, _, _, err := rd.ReadMessage(ctx, headers)
				if err != nil {
					if stopped() || errors.Is(err, context.Canceled) {
						return
					}
					if segmentSwapped(err) || errors.Is(err, ErrSegmentNotFound) ||
						errors.Is(err, io.EOF) {
						rebuilds.Add(1)
						break
					}
					fail("ReadMessage after %d (offset last %d): %v", last, offset, err)
					return
				}
				n, err := strconv.ParseInt(
					string(msg.Value()[:indexOfColon(msg.Value())]), 10, 64)
				if err != nil {
					fail("record at offset %d has an unparseable value %q: %v",
						offset, msg.Value(), err)
					return
				}
				if n <= last {
					fail("the sequence went backwards: read %d at offset %d "+
						"after %d, resuming from %d",
						n, offset, last, from)
					return
				}
				last = n
				lastOffset = offset
				got.Add(1)

				// Which segment served this record. A change means the scan
				// crossed a boundary in place, which is the path under test.
				if seg := l.segmentBaseFor(offset); seg != startSeg {
					if startSeg >= 0 {
						crossed.Add(1)
					}
					startSeg = seg
				}
			}
			rd = nil
		}
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for writes.Load() < enough && violation.Load() == nil {
			time.Sleep(time.Millisecond)
		}
	}()
	select {
	case <-done:
	case <-time.After(120 * time.Second):
		close(stop)
		wg.Wait()
		t.Fatal("the writer never landed enough records")
	}
	close(stop)
	wg.Wait()

	if v := violation.Load(); v != nil {
		t.Fatal(v.(string))
	}

	t.Logf("writes=%d cleans=%d read=%d rebuilds=%d overtaken=%d crossed=%d "+
		"oldest=%d newest=%d",
		writes.Load(), cleans.Load(), got.Load(), rebuilds.Load(),
		overtaken.Load(), crossed.Load(), l.OldestOffset(), l.NewestOffset())

	// The run has to have been dangerous. Without these it passes on a log that
	// never rolled, never collected, and never made the follower cross anything.
	require.Positive(t, cleans.Load(), "no maintenance pass ran")
	require.Positive(t, l.OldestOffset(), "retention never collected anything")
	require.Greater(t, got.Load(), int64(500),
		"the follower barely read, so it can hardly have caught anything")
	require.Positive(t, crossed.Load(),
		"the follower never crossed a segment boundary mid-scan, which is the "+
			"path this test exists for")
	require.Positive(t, overtaken.Load(),
		"retention never got past the follower, so it never had to resume "+
			"anywhere but where it left off")
}

// indexOfColon finds the separator between the sequence number and the padding.
func indexOfColon(b []byte) int {
	for i, c := range b {
		if c == ':' {
			return i
		}
	}
	return len(b)
}

// segmentBaseFor reports the base offset of the segment currently holding an
// offset, or -1. Test-only: the follower uses it to notice that a read crossed a
// segment boundary without having to reach into the reader.
func (l *commitLog) segmentBaseFor(offset int64) int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	seg, _ := findSegment(l.segments, offset)
	if seg == nil {
		return -1
	}
	return seg.BaseOffset
}
