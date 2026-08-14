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
// happen: `rebuilds` stays at zero on every run. That zero was worth nothing
// until 2026-08-14, because the stall that was supposed to provoke a mid-scan
// departure was a fixed 100ms and nothing established it had ever provoked one —
// "the log never surfaces a maintenance error to a scanning follower" and "the
// stall was too short" are the same zero. The stall now waits for the departure
// instead of sleeping through it: it returns only once the record the follower
// last read has been collected, while it still holds the reader that read it, so
// the next read is made from a position that no longer exists. Every 17th step of
// every reader lifetime, on every codec.
//
// It is still zero. THAT is the fact about the log — the read path resolves a
// departed position to its successor internally and a follower scanning across
// retention never sees an error at all.
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
		hotKeys  = 12  // rewritten constantly, so compaction has work
		enough   = 500 // records the writer must land, which minReads already implies
		maxSteps = 64  // messages per reader lifetime before it is rebuilt
		minReads = 500 // records the follower must get through, or it caught nothing
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
			// Stall now and then, until retention has run past where the follower
			// stopped. A consumer that pauses — for a slow handler, a rebalance,
			// a GC — and comes back to find its position collected is the
			// ordinary case, not an exotic one.
			//
			// Waiting for the CONDITION, not for a stretch of wall clock. This
			// was 800ms, chosen because 250ms came back ten offsets short of
			// being overtaken run after run — close enough to look covered and
			// never actually be. But an interval picked that way describes one
			// machine: the writer it has to outpace slows down under -race and
			// under load exactly as everything else does, so on a busy box 800ms
			// stopped buying enough records and `overtaken` stayed at zero. That
			// is the LAST condition in `unmet`, so the run burned its whole
			// 120-second deadline with every other one already met and failed on
			// its own precondition — a red suite saying nothing about the log.
			// The same shape as the 3000-write count described below, left
			// behind by the same rewrite that removed it.
			//
			// `l.OldestOffset() > lastOffset` is not an approximation of being
			// overtaken; it is the exact test the resume below makes. Waiting on
			// it is bounded by the writer landing about MaxLogMessages more
			// records whatever the machine's speed, and on an idle box it
			// returns well inside the 800ms it replaces.
			if life%6 == 1 {
				// life 1 rather than life 5: the first five lifetimes bought
				// nothing the assertions need, and 1-in-6 stalls thereafter is
				// unchanged — most lifetimes must still resume where they left
				// off, which is the other half of what this covers.
				for l.OldestOffset() <= lastOffset {
					select {
					case <-stop:
						return
					case <-time.After(time.Millisecond):
					}
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
				//
				// The SAME condition as the stall between lifetimes, and for the
				// same reason: this was a fixed 100ms, which had to outlast
				// retention reaching a position an open reader is sitting on, and
				// a constant cannot win a race against another goroutine on an
				// arbitrary machine. Worse here than there, because the failure
				// was silent — `rebuilds` was zero on every run and got read as a
				// fact about the log, when "the log never surfaces a maintenance
				// error mid-scan" and "the stall never provoked one" produce the
				// same zero and nothing here told them apart.
				//
				// Waiting on the condition tells them apart. When this returns,
				// the record this follower last read has been collected while it
				// still holds the reader that read it, so the next ReadMessage is
				// made from a position that no longer exists. It is still zero.
				// That is now a fact about the log rather than an assumption
				// about the sleep: see the count's own note above.
				//
				// It is also most of the run's wall clock. The fixed sleep was
				// paid on every 17th step whether or not anything was collected
				// during it; under -race the three codecs took 92.21s together
				// and now take 7.20s, with overtaken, crossed and read the same
				// or better.
				if step%17 == 16 {
					for l.OldestOffset() <= lastOffset {
						select {
						case <-stop:
							return
						case <-time.After(time.Millisecond):
						}
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

	// Retire the run when it has become DANGEROUS, not when the writer has
	// landed a fixed count. The writer is the cheap participant: it appends,
	// while the follower decodes a block, checks a CRC and re-resolves its
	// position. Waiting on the writer therefore retires the run at a moment that
	// depends on the ratio between the two, and that ratio is not a constant —
	// under -race the follower slows by much more than the writer does, so the
	// run ended with the follower having read 225 records against a floor of 500
	// and the suite failed on its own precondition rather than on the invariant.
	// Red on CI's race job through several releases, green on every laptop.
	//
	// Waiting for the conditions the assertions below need makes the run
	// self-pacing on any machine at any speed, and turns the deadline into the
	// only thing that can be wrong — with a message naming what never happened.
	//
	// Which only holds while nothing INSIDE the run is paced by a constant. One
	// was missed the first time — the follower's stall was a fixed 800ms — and
	// it produced this failure again, one condition further along: a red run
	// naming `overtaken` with every other condition met. The message did its
	// job; see the stall for what it pointed at.
	//
	// Which is why the write count below must NOT be one of those conditions. It
	// was 3000 and it was the last one met on every run, so it — not danger —
	// was what retired the run, and the rewrite above had changed nothing but
	// the error message. Dropping it to a count minReads already implies takes
	// the run from 16-20s to under 5s here, because everything the assertions
	// need is in place by roughly 700 writes: the follower is past its floor,
	// retention has collected, it has crossed boundaries mid-scan and been
	// overtaken at least once. The remaining 2300 records bought nothing but
	// wall clock, and on the Windows runner — some 7x slower than this machine —
	// they bought a red suite at 2611 of 3000 with every real condition already
	// met. It stays in `unmet` for its message alone: a writer that dies gets
	// named as the writer instead of surfacing as a follower that read nothing.
	unmet := func() string {
		switch {
		case writes.Load() < enough:
			return fmt.Sprintf("the writer landed %d of %d records", writes.Load(), enough)
		case cleans.Load() == 0:
			return "no maintenance pass ran"
		case l.OldestOffset() <= 0:
			return "retention never collected anything"
		case got.Load() <= minReads:
			return fmt.Sprintf("the follower read %d records, below the floor of %d",
				got.Load(), minReads)
		case crossed.Load() == 0:
			return "the follower never crossed a segment boundary mid-scan"
		case overtaken.Load() == 0:
			return "retention never got past the follower"
		}
		return ""
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for unmet() != "" && violation.Load() == nil {
			time.Sleep(time.Millisecond)
		}
	}()
	select {
	case <-done:
	case <-time.After(120 * time.Second):
		reason := unmet()
		close(stop)
		wg.Wait()
		t.Fatalf("the run never became dangerous enough to assert on: %s", reason)
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
	// These now hold by construction — the run does not retire until they do —
	// so they are here to fail loudly if that wait is ever weakened, rather than
	// to be the first place the shortfall is noticed.
	require.Positive(t, cleans.Load(), "no maintenance pass ran")
	require.Positive(t, l.OldestOffset(), "retention never collected anything")
	require.Greater(t, got.Load(), int64(minReads),
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
