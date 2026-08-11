package commitlog

import (
	"hash/crc32"
	"sync"

	"github.com/pkg/errors"
)

// Locating a filtered read's records is the digest's job (see prefix_source.go).
// Getting them off the device is this file's, and it is a purely economic
// problem: the wanted records are scattered, and there are two ways to collect
// them.
//
//   - Read a contiguous span and discard the frames between the ones wanted.
//   - Address each one, paying a request per record.
//
// Which wins is not a property of the storage device in the abstract — it is
// what that tier charges for. So both the coalescing budget and the fan-out are
// configured per tier and chosen per SEGMENT, since a log mid-offload holds
// both kinds at once.

// How wide a gap between wanted records is read THROUGH rather than split into a
// second request. Per tier, because that is where the setting can be attached —
// NOT because either tier is one kind of device.
//
// The LOCAL default is deliberately CONSERVATIVE rather than descriptive. It
// suits a device where seeking is expensive relative to reading — a spinning
// disk, where one seek costs milliseconds and reading megabytes to avoid it is a
// bargain. On an NVMe it is far too large: random access there is nearly free,
// so a much smaller budget (with a correspondingly higher concurrency) is the
// better shape. That is a property of the hardware, not of being local, and it
// is why the value is configurable rather than inferred.
//
// The TIER default is MEASURED (TestPrefixReadCostProfile) and equals the
// breakeven gap at commonly quoted egress pricing, ~4.4KB. It was 64KB, chosen
// by argument; the measurement showed 64KB behaving identically to 1MB on every
// shape tested — coalescing everything — so a default justified on price was
// sitting an order of magnitude above the price breakeven. A deployment reading
// from inside the same region, where bytes really are free, should raise it.
const (
	defaultPrefixReadCoalesceBytes     = 4 << 20
	defaultPrefixReadTierCoalesceBytes = 4 << 10
)

// Fan-out is bounded PER TIER, and how wide it should be is a property of the
// DEVICE, not of the tier.
//
// A store answers many requests at once, so keeping many in flight is how its
// round trips turn into throughput — hence a high tier default.
//
// Local is where "it depends" bites hardest, and the default assumes the
// unfavourable case. On a spinning disk concurrent random reads mostly defeat
// each other: the queue serializes on one head, and parallelism buys seeks
// rather than bandwidth. On an NVMe the opposite holds — random access is
// nearly free and a DEEP queue is precisely how the device is saturated, so 8 is
// far too low and there is no reason it should not match or exceed the tier
// value.
//
// Neither of them bounds compaction, which is bounded by TIME rather than by a
// worker count: a rewrite is CPU- and write-bound, not a scattered read that
// spends nearly all its time waiting, so what limits it is a deadline
// (CleanRewriteBudget, TierBudgets) and not a number of goroutines.
const (
	defaultPrefixReadConcurrency     = 8
	defaultPrefixReadTierConcurrency = 64
)

// coalesceBudget resolves a configured gap budget. Zero takes the default, as
// everywhere else in Options — so a NEGATIVE value is what expresses "never
// coalesce": every gap splits, giving one request per isolated record. That is
// the maximum-concurrency and maximum-request-count setting, and it has to be
// sayable without colliding with "unset".
func coalesceBudget(v, def int64) int64 {
	switch {
	case v == 0:
		return def
	case v < 0:
		return 0
	}
	return v
}

func concurrencyBudget(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

// prefixRun is a span of wanted records close enough together to read in one
// contiguous pass. Runs are the unit of BOTH decisions this makes: the coalesce
// budget decides where one run ends and the next begins, and each run is then
// fetched CONCURRENTLY with every other run in the segment.
//
// Parallelising per segment instead (the obvious shape, since each segment is
// its own file or object) caps the fan-out at the number of segments holding
// hits. A prefix whose keys are concentrated would then barely fan out at all,
// however many records it wanted — the wrong ceiling when the point is
// throughput.
type prefixRun struct {
	segIdx int
	start  int64 // byte position to begin reading at
	offs   []int64
}

// planRuns groups one segment's wanted offsets (ascending) into runs, splitting
// wherever the byte gap to the next record exceeds coalesce.
func planRuns(seg *segment, segIdx int, offs []int64, coalesce int64) ([]prefixRun, error) {
	var (
		runs   []prefixRun
		cur    prefixRun
		cursor int64 = -1
	)
	for _, off := range offs {
		e, err := seg.findEntry(off)
		if err != nil {
			return nil, errors.Wrapf(err, "locate prefix-read record at offset %d", off)
		}
		if cursor < 0 || e.Position-cursor > coalesce {
			if len(cur.offs) > 0 {
				runs = append(runs, cur)
			}
			cur = prefixRun{segIdx: segIdx, start: e.Position}
		}
		cur.offs = append(cur.offs, off)
		cursor = e.Position + int64(e.Size)
	}
	if len(cur.offs) > 0 {
		runs = append(runs, cur)
	}
	return runs, nil
}

// fetchRuns reads every run of one segment concurrently, returning the records
// keyed by offset.
func fetchRuns(seg *segment, runs []prefixRun, conc int) (map[int64]prefixQueued, error) {
	if len(runs) == 0 {
		return nil, nil
	}
	if conc > len(runs) {
		conc = len(runs)
	}
	var (
		mu   sync.Mutex
		out  = make(map[int64]prefixQueued)
		errs = make([]error, len(runs))
		wg   sync.WaitGroup
		sem  = make(chan struct{}, conc)
	)
	for n, run := range runs {
		wg.Add(1)
		go func(n int, run prefixRun) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			// A cache per goroutine, not one shared: blockCache holds the
			// decode buffers a scan reuses, so sharing one across concurrent
			// scans would have them overwrite each other's blocks.
			got, err := collectRun(seg, run, newBlockCache())
			if err != nil {
				errs[n] = err
				return
			}
			mu.Lock()
			for off, rec := range got {
				out[off] = rec
			}
			mu.Unlock()
		}(n, run)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// collectRun reads one run's records in a SINGLE forward pass from run.start.
//
// There is no gap decision left here — planRuns already made it, by ending a run
// wherever reading on stopped being worth it. So this reads straight through,
// keeping whatever records it was asked for and discarding the frames between
// them, which is exactly the trade the run boundaries encode.
func collectRun(seg *segment, run prefixRun, sc *blockCache) (map[int64]prefixQueued, error) {
	if len(run.offs) == 0 {
		return nil, nil
	}
	// Constructed through newSegmentScannerCache, NOT as a literal: it takes
	// the backing and registers the reader's claim on it under one lock. A
	// scanner assembled by hand holds no claim, and a tiered object it is
	// reading can be reclaimed underneath it.
	ss := newSegmentScannerCache(seg, sc)
	defer ss.Close() // nolint: errcheck — read-only
	ss.pos = run.start

	out := make(map[int64]prefixQueued, len(run.offs))
	for i := 0; i < len(run.offs); {
		ms, _, err := ss.Scan()
		if err != nil {
			return nil, errors.Wrapf(err, "read prefix-read record at offset %d", run.offs[i])
		}
		switch off := ms.Offset(); {
		case off < run.offs[i]:
			// A frame we are not collecting — the waste this run trades for not
			// paying a second request.
		case off == run.offs[i]:
			msg := ms.Message()
			cp := make(SerializedMessage, len(msg))
			copy(cp, msg)
			// The CRC, on the way past. Every other route that hands a record to
			// a caller checks it — readMessage treats a mismatch as unrecoverable
			// — but this one used to return the bytes unexamined, so a corrupt
			// record read by KEY PREFIX was served silently while the same record
			// read sequentially was refused. Serving it is the worst of the three
			// available answers.
			//
			// It is not a real cost here: the copy above already touches every
			// byte, so this adds a second pass over bytes that are in cache, not
			// a request or a decode. The digest is what this path optimises away,
			// and the digest cannot speak for a record's contents.
			//
			// An error rather than readMessage's panic, deliberately: this runs
			// in collectRun's worker goroutines, and a panic there cannot be
			// recovered by the caller — it takes the process with it. Refusing
			// the read reaches the same place without that.
			if want, got := cp.Crc(), crc32.Checksum(cp[4:], crc32cTable); want != got {
				// The same sentinel the sequential path returns: which route
				// found the record is this package's business, not the caller's,
				// and a caller matching on corruption should not have to know
				// whether a digest happened to plan the read.
				return nil, errors.Wrapf(ErrCorruptRecord,
					"record at offset %d: expected CRC 0x%08x, got 0x%08x", off, want, got)
			}
			out[off] = prefixQueued{
				msg:    cp,
				offset: off,
				ts:     ms.Timestamp(),
				epoch:  ms.LeaderEpoch(),
			}
			i++
		default:
			// Ran past a wanted offset: the digest named a record the segment
			// does not hold there. Fail rather than return a short answer that
			// looks like the key was never written.
			return nil, errors.Errorf(
				"commitlog: prefix read overshot offset %d in segment %d (next record is %d)",
				run.offs[i], seg.BaseOffset, off)
		}
	}
	return out, nil
}

// prefixUpperBound returns the first key that sorts after every key beginning
// with prefix, or nil when there is none (an empty prefix, or one that is all
// 0xFF — in both cases the range runs to the end).
func prefixUpperBound(prefix []byte) []byte {
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] == 0xFF {
			continue
		}
		end := make([]byte, i+1)
		copy(end, prefix[:i+1])
		end[i]++
		return end
	}
	return nil
}
