package commitlog

import (
	"bytes"
	"sort"
	"sync"

	"github.com/pkg/errors"
)

// A key-prefix read answers "the latest surviving record for every key under
// this prefix, as of some offset" without replaying the log. It exists for
// state transfer: moving a keyed working set between processes by shipping the
// compacted state rather than the history that produced it.
//
// It leans on the key digests (see keydigest.go), which already hold every
// keyed record's offset and tombstone flag SORTED BY KEY. A prefix is therefore
// a contiguous range in that order, and the read merges digest entries and
// fetches only the records that survive — instead of reading every record in
// every segment.
//
// Two properties are worth stating plainly, because both are easy to assume
// wrongly:
//
// The digest STREAMS; it is not addressable. There is no offset table over the
// keyed section, so sorted order lets the merge STOP EARLY once it leaves the
// prefix range but never START LATE. Keys below the prefix are still decoded on
// the way in. The saving is in records not read, which is the dominant cost —
// not in digest entries not decoded.
//
// The digest is an OPTIMISATION, never the definition. A missing, corrupt or
// stale sidecar is rebuilt by scanning the segment, and the answer is identical
// either way. This is not incidental: the read's output becomes a destination's
// state, so a sidecar problem that silently changed the result would ship
// incomplete state. A dropped tombstone in particular resurrects a deleted key
// at the destination with nothing to report it.

// PrefixRecord is one key's surviving record in a prefix read.
//
// Message is the record VERBATIM, so a tombstone arrives as a tombstone —
// carrying AttrTombstone, with whatever payload it was written with. Tombstones
// are RETURNED, NOT OMITTED: the destination of a state transfer has to delete
// the key, and it cannot do that from a record that was filtered out.
type PrefixRecord struct {
	Offset  int64
	Message SerializedMessage
}

// ReadKeyPrefix returns the latest surviving record for every key beginning
// with prefix, in key order, along with the offset the answer is COMPLETE
// THROUGH. An empty prefix matches every key.
//
// The read covers SEALED SEGMENTS ONLY and never the active one. That is what
// makes completeThrough meaningful: it is a boundary the caller tails from, not
// a moving target. The active segment is deliberately excluded rather than
// merged in — it holds no digest (see loadOrBuildDigests: only sealed segments
// get one), so including it would force a full scan of the tail on every call,
// which is the expensive case this read exists to avoid.
//
// upTo bounds the read from above: records above it are ignored, exactly as a
// reader stopping there would see them. A caller with a commit boundary (an LSO,
// a high watermark) passes it here so the transferred state matches what its
// consumers can see. A negative upTo means "everything sealed". The returned
// completeThrough is the lower of upTo and the sealed boundary, and it is
// returned even when no records match — an empty prefix range is still complete
// through somewhere.
//
// The result is latest-per-key: superseded copies are not returned, and neither
// is a key whose every copy sits above the bound. Tombstones ARE returned (see
// PrefixRecord).
//
// This does NOT persist any digest it has to build. A read that rewrote
// sidecars would turn a concurrent caller's read into a write against segments
// a clean may be rewriting; the cost of rebuilding is the caller's to pay, and
// a clean will persist it in due course anyway.
func (l *commitLog) ReadKeyPrefix(prefix []byte, upTo int64) ([]PrefixRecord, int64, error) {
	l.mu.RLock()
	segments := make([]*segment, len(l.segments))
	copy(segments, l.segments)
	l.mu.RUnlock()

	if len(segments) == 0 {
		return nil, -1, nil
	}
	var (
		sealed = segments[:len(segments)-1]
		// Sealed segments cover exactly what lies below the active segment's
		// base, so that boundary IS the snapshot the caller tails from.
		completeThrough = segments[len(segments)-1].BaseOffset - 1
	)
	if upTo >= 0 && upTo < completeThrough {
		completeThrough = upTo
	}
	if len(sealed) == 0 || completeThrough < 0 {
		return nil, completeThrough, nil
	}

	sc := newBlockCache()
	digests := make([]*keyDigest, len(sealed))
	for i, seg := range sealed {
		if d := loadKeyDigest(seg); d != nil {
			digests[i] = d
			continue
		}
		// The fallback that keeps the digest an optimisation rather than the
		// definition: no usable sidecar, so derive the same facts by scanning.
		d, err := buildKeyDigest(seg, sc)
		if err != nil {
			return nil, -1, errors.Wrap(err, "build key digest for prefix read")
		}
		digests[i] = d
	}

	hits, err := mergePrefix(digests, prefix, completeThrough)
	if err != nil {
		return nil, -1, err
	}

	coalesce := perTierBytes{
		local: coalesceBudget(l.Options.PrefixReadCoalesceBytes, defaultPrefixReadCoalesceBytes),
		tier:  coalesceBudget(l.Options.PrefixReadTierCoalesceBytes, defaultPrefixReadTierCoalesceBytes),
	}
	conc := perTierCount{
		local: concurrencyBudget(l.Options.PrefixReadConcurrency, defaultPrefixReadConcurrency),
		tier:  concurrencyBudget(l.Options.PrefixReadTierConcurrency, defaultPrefixReadTierConcurrency),
	}
	msgs, err := fetchHits(sealed, hits, conc, coalesce)
	if err != nil {
		return nil, -1, err
	}

	out := make([]PrefixRecord, 0, len(hits))
	for _, h := range hits {
		m, ok := msgs[h.offset]
		if !ok {
			return nil, -1, errors.Errorf("commitlog: prefix read lost the record at offset %d", h.offset)
		}
		out = append(out, PrefixRecord{Offset: h.offset, Message: m})
	}
	return out, completeThrough, nil
}

// prefixHit is one key's winning record, located but not yet read.
type prefixHit struct {
	segIdx int
	offset int64
}

// mergePrefix walks the digests in key order and returns, for every key under
// prefix, the highest-offset copy at or below bound — in key order.
func mergePrefix(digests []*keyDigest, prefix []byte, bound int64) ([]prefixHit, error) {
	var (
		its = make([]*digestIter, len(digests))
		all = make([]*digestIter, len(digests))
	)
	defer func() {
		// Release sidecar handles promptly: a clean renames over .keys files
		// and Windows refuses that while one is open.
		for _, it := range all {
			if it != nil {
				it.close()
			}
		}
	}()
	for i, d := range digests {
		it, err := newDigestIter(d)
		if err != nil {
			return nil, err
		}
		all[i], its[i] = it, it
		if !it.next() {
			if err := it.err(); err != nil {
				return nil, err
			}
			its[i] = nil // empty keyed section
		}
	}

	var (
		end    = prefixUpperBound(prefix)
		hits   []prefixHit
		minKey []byte
	)
	for {
		minIdx := -1
		for i, it := range its {
			if it == nil {
				continue
			}
			if minIdx == -1 || bytes.Compare(it.key, its[minIdx].key) < 0 {
				minIdx = i
			}
		}
		if minIdx == -1 {
			break
		}
		// Stop early: keys are sorted, so once the smallest key across every
		// segment has left the prefix range, nothing later can re-enter it.
		if end != nil && bytes.Compare(its[minIdx].key, end) >= 0 {
			break
		}
		// Copy — an iterator's key is only valid until its next(), and the
		// gather below advances the very iterator minKey came from.
		minKey = append(minKey[:0], its[minIdx].key...)
		// Keys below the prefix are still walked (the section streams; the
		// merge cannot start late), but contribute nothing.
		inRange := bytes.HasPrefix(minKey, prefix)

		var (
			latestOff int64 = -1
			latestIdx int
		)
		for i, it := range its {
			if it == nil || !bytes.Equal(it.key, minKey) {
				continue
			}
			if inRange {
				for _, r := range it.recs {
					if r.offset > bound {
						continue
					}
					if r.offset > latestOff {
						latestOff, latestIdx = r.offset, i
					}
				}
			}
			if !it.next() {
				if err := it.err(); err != nil {
					return nil, err
				}
				its[i] = nil
			}
		}
		if inRange && latestOff >= 0 {
			hits = append(hits, prefixHit{segIdx: latestIdx, offset: latestOff})
		}
	}
	return hits, nil
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

// defaultPrefixReadCoalesceBytes is the gap the fetch will read THROUGH rather
// than pay for a second request (see Options.PrefixReadCoalesceBytes for the
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
// The TIER default is small because a store charges per request and answers many
// at once, so splitting is what gives the fan-out something to parallelize. A
// negative setting gives one request per isolated record: the FASTEST and most
// EXPENSIVE shape. Where bytes are actually priced the breakeven is computable —
// gap = 1e9 * C_req / C_GB — and that is the number to use instead of this one.
//
// Both are argued, NOT measured.
const (
	defaultPrefixReadCoalesceBytes     = 4 << 20
	defaultPrefixReadTierCoalesceBytes = 64 << 10
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
// Both are argued rather than measured, and neither is CompactMaxGoroutines
// (10) — that bounds segment REWRITES, which are CPU- and write-bound, not
// scattered reads that spend nearly all their time waiting.
const (
	defaultPrefixReadConcurrency     = 8
	defaultPrefixReadTierConcurrency = 64
)

// prefixRun is a span of wanted records close enough together to read in one
// contiguous pass. Runs are the unit of BOTH decisions this fetch makes: the
// coalesce threshold decides where one run ends and the next begins, and each
// run is then fetched CONCURRENTLY with every other run, across all segments.
//
// Parallelising per segment instead (the obvious shape, since each segment is
// its own file or object) caps the fan-out at the number of segments holding
// hits. A prefix whose keys are concentrated in a few segments would then barely
// fan out at all, however many records it wanted — which is the wrong ceiling
// when the whole point is throughput.
type prefixRun struct {
	segIdx int
	start  int64 // byte position to begin reading at
	offs   []int64
}

// planRuns groups one segment's wanted offsets into runs, splitting wherever the
// byte gap to the next record exceeds coalesce.
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

// perTierBytes and perTierCount carry a setting that differs between segments
// held locally and segments offloaded to a SegmentStore. Both tiers can be live
// in one log — an offload in progress is exactly that — so the choice is made
// per SEGMENT, not once per read.
type perTierBytes struct{ local, tier int64 }
type perTierCount struct{ local, tier int }

// fetchHits reads every hit's record, returning them keyed by offset.
//
// The two tiers' fan-outs are enforced INDEPENDENTLY: a run against a tiered
// segment never consumes local capacity or vice versa, so a log holding both
// does not have its store reads throttled behind its disk reads.
func fetchHits(sealed []*segment, hits []prefixHit, conc perTierCount, coalesce perTierBytes) (map[int64]SerializedMessage, error) {
	bySeg := make(map[int][]int64, len(sealed))
	for _, h := range hits {
		bySeg[h.segIdx] = append(bySeg[h.segIdx], h.offset)
	}
	if len(bySeg) == 0 {
		return nil, nil
	}
	// Planning reads the index only, so it is cheap and stays sequential; the
	// requests it plans are what get fanned out.
	var runs []prefixRun
	segIdxs := make([]int, 0, len(bySeg))
	for segIdx := range bySeg {
		segIdxs = append(segIdxs, segIdx)
	}
	sort.Ints(segIdxs)
	for _, segIdx := range segIdxs {
		offs := bySeg[segIdx]
		sort.Slice(offs, func(i, j int) bool { return offs[i] < offs[j] })
		// Per segment, not per read: a log mid-offload holds both kinds at
		// once, and each segment should be read the way ITS bytes are stored.
		budget := coalesce.local
		if sealed[segIdx].tiered() {
			budget = coalesce.tier
		}
		r, err := planRuns(sealed[segIdx], segIdx, offs, budget)
		if err != nil {
			return nil, err
		}
		runs = append(runs, r...)
	}

	var (
		mu      sync.Mutex
		out     = make(map[int64]SerializedMessage, len(hits))
		errs    = make([]error, len(runs))
		wg      sync.WaitGroup
		localCh = make(chan struct{}, conc.local)
		tierCh  = make(chan struct{}, conc.tier)
	)
	for n, run := range runs {
		wg.Add(1)
		go func(n int, run prefixRun) {
			defer wg.Done()
			sem := localCh
			if sealed[run.segIdx].tiered() {
				sem = tierCh
			}
			sem <- struct{}{}
			defer func() { <-sem }()
			// A cache per goroutine, not one shared: blockCache holds the
			// decode buffers a scan reuses, so sharing one across concurrent
			// scans would have them overwrite each other's blocks.
			got, err := collectRun(sealed[run.segIdx], run, newBlockCache())
			if err != nil {
				errs[n] = err
				return
			}
			mu.Lock()
			for off, m := range got {
				out[off] = m
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
func collectRun(seg *segment, run prefixRun, sc *blockCache) (map[int64]SerializedMessage, error) {
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

	out := make(map[int64]SerializedMessage, len(run.offs))
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
			out[off] = cp
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
