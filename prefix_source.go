package commitlog

import (
	"bytes"
	"sort"

	"github.com/pkg/errors"
)

// A KeyPrefix read is served two different ways, and the seam between them is
// where the digests stop existing.
//
// Over SEALED segments, prefixSource plans from the key digest: it learns which
// offsets in a segment carry matching keys without reading a record, then reads
// only those. That is the acceleration — cost tracks the records returned
// rather than the records the log holds.
//
// Over the ACTIVE segment there is no digest, so there is nothing to plan from
// and the Reader falls back to reading records and testing them. Once it does,
// it stays there: it has caught up with the live tail, where records arrive one
// at a time and per-record filtering is the only thing available anyway.
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
		hits, err := p.planSegment(seg, bound)
		if err != nil {
			return err
		}
		if len(hits) == 0 {
			// Nothing here — skip the whole segment without reading a record.
			// This is the case a scan cannot do and the digest can.
			p.next = seg.NextOffset()
			continue
		}
		recs, err := p.fetch(seg, hits)
		if err != nil {
			return err
		}
		p.queue = recs
		p.next = seg.NextOffset()
		return nil
	}
	p.done = true
	return nil
}

// planSegment returns the offsets in seg to read, ascending.
func (p *prefixSource) planSegment(seg *segment, bound int64) ([]int64, error) {
	d := loadKeyDigest(seg)
	if d == nil {
		// No usable sidecar. Rebuilding by scanning keeps the digest an
		// OPTIMISATION rather than the definition: the records returned are
		// the same either way, and only the cost differs.
		var err error
		if d, err = buildKeyDigest(seg, newBlockCache()); err != nil {
			return nil, errors.Wrap(err, "build key digest for prefix read")
		}
	}
	return digestHits(d, p.spec, p.next, bound)
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
				if r.offset < from || (bound >= 0 && r.offset > bound) {
					continue
				}
				if r.offset > best {
					best = r.offset
				}
			}
			if best >= 0 {
				hits = append(hits, best)
			}
			continue
		}
		for _, r := range it.recs {
			if r.offset < from || (bound >= 0 && r.offset > bound) {
				continue
			}
			hits = append(hits, r.offset)
		}
	}
	if err := it.err(); err != nil {
		return nil, err
	}
	if spec.includeControl {
		for _, off := range d.control {
			if off < from || (bound >= 0 && off > bound) {
				continue
			}
			hits = append(hits, off)
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
