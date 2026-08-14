package commitlog

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"sort"

	"github.com/pkg/errors"
)

// A KeyPrefix read is served two different ways, and the seam between them is
// where the digests stop existing.
//
// Over a SEALED segment that HAS a digest, prefixSource plans from it: it learns
// which offsets carry matching keys without reading a record, then reads only
// those. That is the acceleration — cost tracks the records returned rather than
// the records the log holds.
//
// Everywhere else there is nothing to plan from, and the answer is to read
// records and test them. That covers two cases, not one:
//
//   - The ACTIVE segment, which never has a digest. Once the Reader reaches it
//     it stays there: it has caught up with the live tail, where records arrive
//     one at a time and per-record filtering is the only thing available anyway.
//   - A SEALED segment with no usable digest — absent, corrupt, or stale. This
//     is the permanent state of every sealed segment on a log with Compact
//     disabled, because the compact cleaner is the only thing that ever writes a
//     sidecar, so it is a steady state and not a transient one.
//
// The second case used to BUILD a digest and throw it away. That was strictly
// more work than the scan it was avoiding: buildKeyDigest reads every record in
// the segment AND holds a map over every distinct key in it, and the offsets it
// produced were then read a second time. It is the same allocation the compact
// cleaner caps at two concurrent builds, having measured ten of them over
// ~40MB segments at >1GB — and on this path nothing capped it at all, since the
// number in flight is however many readers happen to be doing prefix reads.
// scanSegmentFiltered replaces it with one pass that keeps only what matches.
//
// Planning is LAZY, one segment at a time, so a filtered read starts returning
// records immediately and holds at most one segment's matching records in
// memory rather than the whole result.

// prefixQueued is one fetched record waiting to be served.
type prefixQueued struct {
	msg    SerializedMessage
	offset int64
	ts     int64
	epoch  uint64
}

// prefixSource serves the sealed portion of a filtered read.
type prefixSource struct {
	log  *commitLog
	spec readSpec

	queue []prefixQueued
	qi    int

	// next is the offset planning resumes from.
	next int64
	// servedThrough is NextOffset of the last segment whose records were
	// queued: the point the SEARCH has reached, as distinct from next, which
	// is the point a resumed READ must continue from.
	//
	// They differ because pop walks next back to the last record it served, so
	// a segment whose records have all been handed out still looks unfinished
	// to the loop below — its NextOffset is above next. Without this the loop
	// visits every segment a second time to discover it has nothing left. With
	// a digest that second visit is nearly free, which is why it survived; with
	// no digest it is an entire extra pass over the segment, and it doubled the
	// cost of exactly the reads scanSegmentFiltered exists to make cheaper.
	servedThrough int64
	// done is set once there are no sealed segments left to plan: the caller
	// switches to reading the tail sequentially.
	done bool
}

func newPrefixSource(l *commitLog, spec readSpec) *prefixSource {
	return &prefixSource{log: l, spec: spec, next: spec.offset}
}

// bound returns the highest offset this read may return, or -1 for none.
func (p *prefixSource) bound() int64 {
	b := p.spec.until
	if !p.spec.uncommitted {
		if hw := p.log.HighWatermark(); b < 0 || hw < b {
			b = hw
		}
	}
	return b
}

// pop returns the next record from the sealed portion. ok is false once the
// sealed portion is exhausted, after which the caller reads sequentially from
// p.next.
func (p *prefixSource) pop() (prefixQueued, bool, error) {
	for {
		if p.qi < len(p.queue) {
			rec := p.queue[p.qi]
			p.qi++
			p.next = rec.offset + 1
			return rec, true, nil
		}
		if p.done {
			return prefixQueued{}, false, nil
		}
		if err := p.fillFromNextSegment(); err != nil {
			return prefixQueued{}, false, err
		}
	}
}

// fillFromNextSegment plans and fetches the next sealed segment that holds
// matching records, leaving them in the queue.
func (p *prefixSource) fillFromNextSegment() error {
	p.queue, p.qi = p.queue[:0], 0
	// Resume the search past the segment that was just drained. See
	// servedThrough: pop left next pointing at the last record it served.
	if p.servedThrough > p.next {
		p.next = p.servedThrough
	}

	segments := p.log.segmentsSnapshot()
	if len(segments) == 0 {
		p.done = true
		return nil
	}
	sealed := segments[:len(segments)-1]
	bound := p.bound()

	for _, seg := range sealed {
		if seg.NextOffset() <= p.next {
			continue // entirely behind us
		}
		if bound >= 0 && seg.BaseOffset > bound {
			p.done = true
			return nil
		}
		recs, err := p.serveSegment(seg, bound)
		if err != nil {
			return err
		}
		p.next = seg.NextOffset()
		if len(recs) == 0 {
			// Nothing here. With a digest this cost no reads at all — skipping a
			// whole segment unread is the thing a scan cannot do and the digest
			// can. Without one it cost a scan, which is the price of not having
			// the digest rather than a price this branch adds.
			continue
		}
		p.servedThrough = seg.NextOffset()
		p.queue = recs
		return nil
	}
	p.done = true
	return nil
}

// serveSegment returns the matching records of one sealed segment, in offset
// order, by whichever of the two routes the segment's digest allows.
//
// The digest stays an OPTIMISATION rather than the definition: both routes
// return the same records, and only the cost differs. That is what
// TestReaderKeyPrefixMatchesScan pins by running its whole comparison three
// times — freshly built digests, persisted sidecars, and none at all.
func (p *prefixSource) serveSegment(seg *segment, bound int64) ([]prefixQueued, error) {
	d := loadKeyDigest(seg)
	if d == nil {
		return p.scanSegmentFiltered(seg, bound)
	}
	hits, err := digestHits(d, p.spec, p.next, bound)
	if err != nil {
		return nil, err
	}
	if len(hits) == 0 {
		return nil, nil
	}
	return p.fetch(seg, hits)
}

// scanSegmentFiltered reads one segment start to finish and keeps the records
// the read wants, for a segment whose digest is missing, corrupt or stale.
//
// It is the same filtering the Reader applies to the active segment, and it
// answers in ONE pass: the alternative it replaced built a whole key digest
// just to learn the offsets, then read those offsets again. The map here is
// keyed by MATCHING keys only, where a digest build holds every distinct key in
// the segment — so the memory the compact cleaner bounds so carefully is not
// merely bounded on this path, it is not allocated.
//
// io.EOF is how this stops, the ordinary meaning: unlike collectRun, which is
// hunting specific offsets a digest named and so treats reaching the end as
// damage, this one has no promise to fall short of. It reads what is there.
func (p *prefixSource) scanSegmentFiltered(seg *segment, bound int64) ([]prefixQueued, error) {
	// newSegmentScannerCache, not a literal: it registers the reader's claim on
	// the backing under one lock, and a tiered object read without that claim can
	// be reclaimed underneath the scan. collectRun says the same thing.
	ss := newSegmentScannerCache(seg, newBlockCache())
	defer ss.Close() // nolint: errcheck — read-only

	var (
		out  []prefixQueued
		dead []bool
		// skipSuperseded: index into out of the newest copy of each key seen so
		// far. Decided WITHIN this segment, exactly as digestHits decides it —
		// which is what makes the two routes agree.
		latest map[string]int
	)
	if p.spec.skipSuperseded {
		latest = make(map[string]int)
	}
	for {
		ms, _, err := ss.Scan()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("%w: prefix scan of segment %d: %w",
				ErrSegmentUnreadable, seg.BaseOffset, err)
		}
		off := ms.Offset()
		if off < p.next || (bound >= 0 && off > bound) {
			continue
		}
		var (
			msg     = ms.Message()
			attrs   = msg.Attributes()
			control = attrs&AttrControl != 0
			key     = msg.Key()
		)
		switch {
		case control:
			// No key at all, so the prefix cannot speak to it; IncludeControl is
			// the only thing that admits one. Checked BEFORE the key test below,
			// which would otherwise drop it as unkeyed.
			if !p.spec.includeControl {
				continue
			}
		case key == nil || !bytes.HasPrefix(key, p.spec.keyPrefix):
			// Unkeyed records cannot match a prefix, and non-matching ones are
			// the whole point of the filter.
			continue
		}
		cp := make(SerializedMessage, len(msg))
		copy(cp, msg)
		// The CRC, for the reason collectRun gives at length: every route that
		// hands a record to a caller checks it, and a corrupt record served
		// because a digest happened not to exist would be the one route that
		// did not. The copy above already touched every byte.
		if want, got := cp.Crc(), crc32.Checksum(cp[4:], crc32cTable); want != got {
			return nil, errors.Wrapf(ErrCorruptRecord,
				"record at offset %d: expected CRC 0x%08x, got 0x%08x", off, want, got)
		}
		rec := prefixQueued{msg: cp, offset: off, ts: ms.Timestamp(), epoch: ms.LeaderEpoch()}
		if latest != nil {
			if !control {
				if i, ok := latest[string(key)]; ok {
					// Retire the earlier copy in place rather than overwriting it
					// with this one: out is built in offset order, and reusing the
					// old slot would put this record back at the old one's position.
					dead[i] = true
				}
				latest[string(key)] = len(out)
			}
			// Only tracked when there is supersession to resolve. Without
			// SkipSuperseded nothing ever reads this, and every record here is
			// one the caller asked for.
			dead = append(dead, false)
		}
		out = append(out, rec)
	}
	if latest == nil {
		return out, nil
	}
	kept := out[:0]
	for i, rec := range out {
		if !dead[i] {
			kept = append(kept, rec)
		}
	}
	return kept, nil
}

// digestHits walks a digest's keyed section and returns the offsets whose keys
// match the prefix and fall within [from, bound], ascending.
func digestHits(d *keyDigest, spec readSpec, from, bound int64) ([]int64, error) {
	it, err := newDigestIter(d)
	if err != nil {
		return nil, err
	}
	defer it.close()

	var (
		end  = prefixUpperBound(spec.keyPrefix)
		hits []int64
	)
	// The read's window, written once. It was spelled out at all three sites
	// below — twice over keyed records and once over control offsets, where the
	// third copy says the same thing about a differently named variable and
	// nothing compares it to the other two. A bound of -1 means unbounded.
	inWindow := func(off int64) bool {
		return off >= from && (bound < 0 || off <= bound)
	}
	for it.next() {
		// Sorted by key, so once past the prefix range nothing later re-enters
		// it. The section streams, so keys BELOW the prefix are still decoded
		// on the way in — it can stop early, never start late.
		if end != nil && bytes.Compare(it.key, end) >= 0 {
			break
		}
		if !bytes.HasPrefix(it.key, spec.keyPrefix) {
			continue
		}
		if spec.skipSuperseded {
			// The last copy of this key WITHIN this segment, among those the
			// read can still return. Decided from the digest alone — no
			// lookahead, which is why this streams and can follow.
			best := int64(-1)
			for _, r := range it.recs {
				if inWindow(r.offset) && r.offset > best {
					best = r.offset
				}
			}
			if best >= 0 {
				hits = append(hits, best)
			}
			continue
		}
		for _, r := range it.recs {
			if inWindow(r.offset) {
				hits = append(hits, r.offset)
			}
		}
	}
	if err := it.err(); err != nil {
		return nil, err
	}
	if spec.includeControl {
		for _, off := range d.control {
			if inWindow(off) {
				hits = append(hits, off)
			}
		}
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i] < hits[j] })
	return hits, nil
}

// fetch reads the planned offsets out of one segment, in offset order.
func (p *prefixSource) fetch(seg *segment, hits []int64) ([]prefixQueued, error) {
	coalesce := coalesceBudget(
		p.log.Options.PrefixReadCoalesceBytes, defaultPrefixReadCoalesceBytes)
	conc := concurrencyBudget(
		p.log.Options.PrefixReadConcurrency, defaultPrefixReadConcurrency)
	if seg.tiered() {
		coalesce = coalesceBudget(
			p.log.Options.PrefixReadTierCoalesceBytes, defaultPrefixReadTierCoalesceBytes)
		conc = concurrencyBudget(
			p.log.Options.PrefixReadTierConcurrency, defaultPrefixReadTierConcurrency)
	}

	runs, err := planRuns(seg, hits, coalesce)
	if err != nil {
		return nil, err
	}
	byOffset, err := fetchRuns(seg, runs, conc)
	if err != nil {
		return nil, err
	}
	out := make([]prefixQueued, 0, len(hits))
	for _, off := range hits {
		rec, ok := byOffset[off]
		if !ok {
			return nil, errors.Errorf("commitlog: prefix read lost the record at offset %d", off)
		}
		out = append(out, rec)
	}
	return out, nil
}
