package commitlog

import (
	"bytes"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/pkg/errors"
)

const defaultCompactMaxGoroutines = 10

// compactCleanerOptions contains configuration settings for the
// compactCleaner.
type compactCleanerOptions struct {
	Name          string
	MaxGoroutines int
	// MinAge protects a compaction horizon: a segment whose most recent write is
	// newer than MinAge is kept intact rather than compacted. Zero disables it.
	MinAge time.Duration
	// TombstoneRetention is the spec-less tombstone GC guard
	// (Options.CompactTombstoneRetention); CleanSpec overrides it.
	TombstoneRetention time.Duration
}

// compactCleaner implements the compaction policy which replaces segments with
// compacted ones, i.e. retaining only the last message for a given key.
type compactCleaner struct {
	compactCleanerOptions
}

// NewCompactCleaner returns a new cleaner which performs log compaction by
// rewriting segments such that they contain only the last message for a given
// key.
func newCompactCleaner(opts compactCleanerOptions) *compactCleaner {
	if opts.MaxGoroutines == 0 {
		opts.MaxGoroutines = defaultCompactMaxGoroutines
	}
	return &compactCleaner{opts}
}

// Compact performs spec-less compaction bounded at hw: latest-per-key with
// no transaction awareness, plus tombstone GC when the cleaner is configured
// with a TombstoneRetention. Compat wrapper over CompactSpec.
func (c *compactCleaner) Compact(hw int64, segments []*segment) ([]*segment,
	*leaderEpochCache, error) {

	spec := CleanSpec{Ceiling: hw}
	if c.TombstoneRetention > 0 {
		spec.TombstoneGCBelow = hw
		spec.TombstoneRetention = c.TombstoneRetention
	}
	compacted, cache, _, err := c.CompactSpec(spec, segments)
	return compacted, cache, err
}

// CompactSpec performs log compaction under a CleanSpec: segments up to but
// excluding the active (last) segment are rewritten so that, below the spec's
// Ceiling, only the latest message per key survives. With a transactional
// spec it additionally removes aborted records, garbage-collects expired
// tombstones, drops control markers below StripBelow, and strips the
// transactional headers off surviving decided records. Returns the compacted
// segments and a leaderEpochCache containing the earliest offsets for each
// leader epoch, or nil if nothing was compacted.
func (c *compactCleaner) CompactSpec(spec CleanSpec, segments []*segment) ([]*segment,
	*leaderEpochCache, int64, error) {

	if len(segments) <= 1 {
		return segments, nil, -1, nil
	}

	slog.Debug("Compacting log", slog.String("name", c.Name))
	before := time.Now()
	compacted, epochCache, removed, verified, err := c.compact(spec, segments)
	if err == nil {
		slog.Debug("Finished compacting log %s",
			slog.String("name", c.Name),
			slog.Int("removed", removed),
			slog.Int("before", len(segments)),
			slog.Int("after", len(compacted)),
			slog.Duration("duration", time.Since(before)),
		)

	}

	return compacted, epochCache, verified, errors.Wrap(err, "failed to compact log")

}

// dropSet is a per-segment bitmap of record offsets the merge decided to
// remove (superseded copies and expired tombstones). One bit per offset in
// [base, next) — the whole log's drop state costs bits-per-record instead of
// the map-of-every-key the pre-digest cleaner materialized (which transiently
// allocated >1GB on large logs).
type dropSet struct {
	base  int64
	bits  []uint64
	count int
}

func newDropSet(base, next int64) *dropSet {
	n := next - base
	if n < 0 {
		n = 0
	}
	return &dropSet{base: base, bits: make([]uint64, (n+63)/64)}
}

func (d *dropSet) set(off int64) {
	i := off - d.base
	if i < 0 || i >= int64(len(d.bits))*64 {
		return
	}
	w, b := i/64, uint(i%64)
	if d.bits[w]&(1<<b) == 0 {
		d.bits[w] |= 1 << b
		d.count++
	}
}

func (d *dropSet) get(off int64) bool {
	if d == nil {
		return false
	}
	i := off - d.base
	if i < 0 || i >= int64(len(d.bits))*64 {
		return false
	}
	return d.bits[i/64]&(1<<uint(i%64)) != 0
}

func (c *compactCleaner) compact(spec CleanSpec, segments []*segment) ([]*segment,
	*leaderEpochCache, int, int64, error) {

	// Latest-per-key is computed by a streaming merge over per-segment sorted
	// key digests (persistent sidecars for sealed segments, in-memory for the
	// active one) into per-segment drop bitsets. Segments whose digest proves
	// the pass would change nothing are kept without reading a single record.
	digests, err := c.loadOrBuildDigests(segments)
	if err != nil {
		return nil, nil, 0, -1, err
	}
	merged, err := c.mergeDigests(spec, segments, digests)
	if err != nil {
		return nil, nil, 0, -1, err
	}

	var (
		compacted  = make([]*segment, 0, len(segments))
		epochCache = newLeaderEpochCacheNoFile(c.Name)
		removed    = 0
		// The verified floor: highest offset of the CONSECUTIVE oldest run
		// of sealed segments this pass rewrote or digest-proved converged.
		// An age-protected segment (kept unread, headers and abort markers
		// intact) breaks the chain — everything above it stays unverified
		// regardless of its own disposition, because a floor must cover a
		// gap-free prefix.
		verified      = int64(-1)
		verifiedChain = true
	)

	// A protected compaction horizon: a segment whose newest write is within
	// MinAge is kept intact. The merge still records the latest offset per key
	// across all segments (including protected ones), so compacting an older
	// segment correctly drops a key whose latest copy lives in a protected recent
	// segment. Segments are ordered oldest→newest, so protection naturally covers
	// the recent tail.
	var horizon int64
	if c.MinAge > 0 {
		horizon = timestamp() - int64(c.MinAge)
	}

	feedEpochs := func(d *keyDigest) error {
		for _, e := range d.epochs {
			if e.epoch > epochCache.LastLeaderEpoch() {
				if err := epochCache.Assign(e.epoch, e.firstOffset); err != nil {
					return err
				}
			}
		}
		return nil
	}

	// Write new segments. Skip the last segment since we will not compact it.
	// TODO: Join segments that are below the bytes limit.
	for i, seg := range segments[:len(segments)-1] {
		segEnd := seg.NextOffset() - 1
		if horizon > 0 && seg.LastWriteTime() > horizon {
			// Within the protected horizon — keep whole.
			verifiedChain = false
			compacted = append(compacted, seg)
			continue
		}
		if c.canSkip(spec, digests[i], merged, i) && !seg.needsBlockConsolidation() {
			// Digest proves a rewrite would keep every record byte-for-byte:
			// converged without reading the segment. A pathologically
			// fine-grained block index still forces one consolidation
			// rewrite — identical records, ~1000x fewer blocks.
			compacted = append(compacted, seg)
			if err := feedEpochs(digests[i]); err != nil {
				return nil, nil, 0, -1, err
			}
			if verifiedChain {
				verified = segEnd
			}
			continue
		}
		cleaned, msgsRemoved, err := c.cleanSegment(spec, seg, merged.drops[i], epochCache)
		if err != nil {
			return nil, nil, 0, -1, err
		}
		if cleaned != nil {
			compacted = append(compacted, cleaned)
		}
		removed += msgsRemoved
		if verifiedChain {
			verified = segEnd
		}
	}

	// Add the last segment back in to the compacted list and feed its epoch
	// assignments (its digest covers every record).
	last := segments[len(segments)-1]
	compacted = append(compacted, last)
	if err := feedEpochs(digests[len(segments)-1]); err != nil {
		return nil, nil, 0, -1, err
	}

	// Stripping applies to offsets strictly below StripBelow, so the record
	// AT StripBelow keeps its headers; a spec without strip semantics
	// verifies nothing.
	if verified > spec.StripBelow-1 {
		verified = spec.StripBelow - 1
	}
	return compacted, epochCache, removed, verified, nil
}

// mergeResult carries the merge's per-segment decisions.
type mergeResult struct {
	// drops[i]: offsets in segment i removed as superseded or as expired
	// tombstones. nil when nothing in the segment is dropped.
	drops []*dropSet
	// abortedKeyed[i]: segment i holds a keyed data record below the ceiling
	// that spec.Aborted marks — removal work exists even without drops.
	abortedKeyed []bool
	// stripKeyed[i]: segment i holds a keyed data record below StripBelow
	// that still carries headers — strip work MAY exist (see stamp).
	stripKeyed []bool
}

// mergeDigests streams all digests' keyed sections in key order and marks
// superseded copies (and expired tombstones) in per-segment drop bitsets.
// Only records at or below the ceiling participate — an undecided record
// above it can neither shadow a committed value nor be dropped.
func (c *compactCleaner) mergeDigests(spec CleanSpec, segments []*segment,
	digests []*keyDigest) (*mergeResult, error) {

	res := &mergeResult{
		drops:        make([]*dropSet, len(segments)),
		abortedKeyed: make([]bool, len(segments)),
		stripKeyed:   make([]bool, len(segments)),
	}
	drop := func(i int, off int64) {
		if res.drops[i] == nil {
			res.drops[i] = newDropSet(segments[i].BaseOffset, segments[i].NextOffset())
		}
		res.drops[i].set(off)
	}

	its := make([]*digestIter, len(digests))
	all := make([]*digestIter, len(digests))
	defer func() {
		// Release sidecar handles before any segment rewrite: refreshDigest
		// renames over .keys files and Windows refuses that while open.
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
		all[i] = it
		its[i] = it
		if !it.next() {
			if err := it.err(); err != nil {
				return nil, err
			}
			its[i] = nil // empty keyed section
		}
	}

	var (
		now         = timestamp()
		stripActive = spec.StripBelow > 0 && len(spec.StripHeaders) > 0
		gcActive    = spec.TombstoneRetention > 0
		// scratch: participants of the current key across segments
		partSeg []int
		partRec []digestRec
		minKey  []byte
	)
	for {
		// Find the smallest current key across iterators.
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
		// Copy: an iterator's key is only valid until its next(), and the
		// gather loop below advances the very iterator minKey came from.
		minKey = append(minKey[:0], its[minIdx].key...)

		// Gather every segment's records for this key; compute the latest
		// decided, non-aborted copy while noting per-segment work signals.
		partSeg = partSeg[:0]
		partRec = partRec[:0]
		var (
			latestOff int64 = -1
			latestIdx int
			latestRec digestRec
		)
		for i, it := range its {
			if it == nil || !bytes.Equal(it.key, minKey) {
				continue
			}
			for _, r := range it.recs {
				if stripActive && r.offset < spec.StripBelow && r.flags&digestFlagHasHeaders != 0 {
					res.stripKeyed[i] = true
				}
				if r.offset > spec.Ceiling {
					continue
				}
				if spec.Aborted != nil && spec.Aborted(r.offset) {
					// Aborted copies never shadow a committed value at any
					// decided offset; the pass removes them only below the
					// ceiling (the record AT the ceiling is always retained).
					if r.offset < spec.Ceiling {
						res.abortedKeyed[i] = true
					}
					continue
				}
				partSeg = append(partSeg, i)
				partRec = append(partRec, r)
				if r.offset > latestOff {
					latestOff, latestIdx, latestRec = r.offset, i, r
				}
			}
			if !it.next() {
				if err := it.err(); err != nil {
					return nil, err
				}
				its[i] = nil
			}
		}
		if latestOff < 0 {
			continue
		}
		// Superseded copies strictly below the ceiling are dropped (the
		// record AT the ceiling is always the latest for its key).
		for j, segIdx := range partSeg {
			if off := partRec[j].offset; off != latestOff && off < spec.Ceiling {
				drop(segIdx, off)
			}
		}
		// The surviving copy itself: an expired tombstone vanishes.
		if gcActive && latestRec.flags&digestFlagTombstone != 0 &&
			latestOff < spec.TombstoneGCBelow && latestRec.ts > 0 &&
			latestRec.ts < now-int64(spec.TombstoneRetention) {
			drop(latestIdx, latestOff)
		}
	}
	return res, nil
}

// canSkip reports whether a rewrite of segment i would provably keep every
// record unchanged, letting the pass keep the original file without reading
// it. Any doubt returns false — the segment then goes through cleanSegment,
// whose scan refreshes the digest (and its strip stamp) so the doubt is
// resolved for future passes.
func (c *compactCleaner) canSkip(spec CleanSpec, d *keyDigest, m *mergeResult, i int) bool {
	if m.drops[i] != nil && m.drops[i].count > 0 {
		return false
	}
	if m.abortedKeyed[i] {
		return false
	}
	stripActive := spec.StripBelow > 0 && len(spec.StripHeaders) > 0
	if stripActive {
		// Markers below the strip boundary are removed by the pass.
		for _, off := range d.control {
			if off < spec.StripBelow {
				return false
			}
		}
		// Data records below the boundary that still carry headers might
		// carry the strip targets — only a scan (recorded in the stamp) can
		// prove otherwise.
		if m.stripKeyed[i] && !d.stripStampCovers(spec.StripBelow, spec.StripHeaders) {
			return false
		}
		for _, r := range d.unkeyed {
			if r.offset < spec.StripBelow && r.flags&digestFlagHasHeaders != 0 &&
				!d.stripStampCovers(spec.StripBelow, spec.StripHeaders) {
				return false
			}
		}
	}
	if spec.Aborted != nil {
		for _, r := range d.unkeyed {
			if r.offset < spec.Ceiling && spec.Aborted(r.offset) {
				return false
			}
		}
	}
	return true
}

// loadOrBuildDigests returns a digest per segment: persisted sidecars for
// sealed segments (built and installed when missing or stale), an in-memory
// one for the active tail. Builds run on the cleaner's worker pool.
func (c *compactCleaner) loadOrBuildDigests(segments []*segment) ([]*keyDigest, error) {
	// Build concurrency is capped at 2, NOT MaxGoroutines: each build holds a
	// transient per-segment key map, and the post-restart first clean can owe
	// digests for every segment the catch-up burst sealed — 10 concurrent
	// ~40MB maps measured >1GB on a 12h soak. Cleans no longer block appends
	// (Stream.cleanMu split), so slower builds cost nothing on the commit
	// path; peak memory is what matters.
	buildConc := 2
	if c.MaxGoroutines < buildConc {
		buildConc = c.MaxGoroutines
	}
	var (
		digests = make([]*keyDigest, len(segments))
		errs    = make([]error, len(segments))
		wg      sync.WaitGroup
		sem     = make(chan struct{}, buildConc)
	)
	for i, seg := range segments {
		sealed := i < len(segments)-1
		if sealed {
			if d := loadKeyDigest(seg); d != nil {
				digests[i] = d
				continue
			}
		}
		wg.Add(1)
		go func(i int, seg *segment, persist bool) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			d, err := buildKeyDigest(seg)
			if err != nil {
				errs[i] = err
				return
			}
			digests[i] = d
			if persist {
				if werr := writeKeyDigest(seg, d); werr != nil {
					slog.Warn("key digest write failed; will rebuild next clean",
						slog.String("name", c.Name), slog.String("err", werr.Error()))
				} else if ld := loadKeyDigest(seg); ld != nil {
					// Swap to the streaming form so the merge doesn't retain
					// this build's keyed bytes (post-restart first cleans can
					// owe a digest for every catch-up segment at once).
					digests[i] = ld
				}
			}
		}(i, seg, sealed)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return digests, nil
}

// disposition classifies one message under a CleanSpec.
type disposition int

const (
	dispRetain disposition = iota // copy verbatim
	dispRemove                    // drop from the log
	dispStrip                     // retain, rewritten without StripHeaders
)

// classify decides a message's fate. drops must have been produced by
// mergeDigests under the SAME spec: it holds superseded copies and expired
// tombstones (aborted/control/nil-key/above-ceiling records never enter it,
// so they can never shadow a committed value).
func (c *compactCleaner) classify(spec CleanSpec, offset int64, msg SerializedMessage,
	drops *dropSet) disposition {

	// At or above the ceiling: possibly undecided — always retained.
	if offset >= spec.Ceiling {
		return dispRetain
	}
	attrs := msg.Attributes()
	if attrs&AttrControl != 0 {
		// Markers below the decided boundary are only removable when the
		// surviving records they governed are stripped to plain records in
		// the same pass — otherwise a reader would buffer those records
		// waiting for a marker that no longer exists (or worse, release
		// them on a LATER transaction's marker).
		if offset < spec.StripBelow && len(spec.StripHeaders) > 0 {
			return dispRemove
		}
		return dispRetain
	}
	if spec.Aborted != nil && spec.Aborted(offset) {
		// Aborted data records are invisible to every reader forever.
		return dispRemove
	}
	key := msg.Key()
	if key == nil {
		// Unkeyed data cannot be compacted; strip it if it is decided.
		if offset < spec.StripBelow && len(spec.StripHeaders) > 0 {
			return dispStrip
		}
		return dispRetain
	}
	if drops.get(offset) {
		// Superseded by a newer copy of the key, or an expired tombstone.
		return dispRemove
	}
	if offset < spec.StripBelow && len(spec.StripHeaders) > 0 {
		return dispStrip
	}
	return dispRetain
}

// cleanBlockTarget is the uncompressed size a rewrite accumulates before
// flushing one block. The append path writes a block per message set — small
// commits make sub-KB blocks, and a segment's blockRef index (and its sparse
// index, and zstd's ratio, and the open-time header walk) all scale with the
// block COUNT. Run 22 measured 18.6M ~140-byte blocks holding ~900MB of
// blockRefs; rewrites consolidate them ~1000x.
const cleanBlockTarget = 256 << 10

func (c *compactCleaner) cleanSegment(spec CleanSpec, seg *segment, drops *dropSet,
	epochCache *leaderEpochCache) (*segment, int, error) {

	cleaned, err := seg.Cleaned()
	if err != nil {
		return nil, 0, err
	}
	var (
		ss = newSegmentScanner(seg)
		// A consolidation pass rewrites even byte-identical content: the
		// rewrite's value is the block layout itself, so convergence must not
		// discard it (it would re-rewrite and re-discard every tick).
		consolidating = seg.needsBlockConsolidation()
		removed       = 0
		stripped      = 0
		stripActive   = spec.StripBelow > 0 && len(spec.StripHeaders) > 0
		// Does any surviving record still carry a strip-target header (i.e.
		// sits at/above StripBelow with one present)? Decides how far the
		// refreshed digest's strip stamp may reach.
		residualStrippable = false
		// Retained message sets accumulate here and flush as one consolidated
		// block (one WriteMessageSet call): concatenated message sets are a
		// valid message-set sequence, and entriesForMessageSet computes every
		// logical position from the flush-time segment position.
		blockBuf []byte
	)
	flush := func() error {
		if len(blockBuf) == 0 {
			return nil
		}
		entries := entriesForMessageSet(cleaned.Position(), blockBuf)
		if err := cleaned.WriteMessageSet(blockBuf, entries); err != nil {
			return err
		}
		blockBuf = blockBuf[:0]
		return nil
	}
	for ms, _, err := ss.Scan(); err == nil; ms, _, err = ss.Scan() {
		var (
			offset      = ms.Offset()
			leaderEpoch = ms.LeaderEpoch()
			msg         = ms.Message()
		)
		disp := c.classify(spec, offset, msg, drops)
		if disp == dispRemove {
			removed++
			continue
		}
		out := []byte(ms)
		if disp == dispStrip {
			sf, changed, err := stripFrame(ms, spec.StripHeaders)
			if err != nil {
				return nil, removed, err
			}
			if changed {
				out = sf
				stripped++
			}
		} else if stripActive && msg.Attributes()&AttrControl == 0 &&
			offset >= spec.StripBelow && hasAnyHeader(msg, spec.StripHeaders) {
			residualStrippable = true
		}
		blockBuf = append(blockBuf, out...)
		if len(blockBuf) >= cleanBlockTarget {
			if err := flush(); err != nil {
				return nil, removed, err
			}
		}
		// Maintain start offset for each new leader epoch.
		if leaderEpoch > epochCache.LastLeaderEpoch() {
			if err := epochCache.Assign(leaderEpoch, offset); err != nil {
				return nil, removed, err
			}
		}
	}
	if err := flush(); err != nil {
		return nil, removed, err
	}

	if cleaned.IsEmpty() {
		// If the new segment is empty, remove it along with the old one.
		return nil, removed, cleanupEmptySegment(cleaned, seg)
	}
	// After either outcome the segment's digest is refreshed with a strip
	// stamp recording what this scan proved, so the NEXT pass can skip the
	// segment without reading it. MaxInt64 = no surviving record anywhere
	// carries a strip target; else verified only below this pass's boundary.
	stamp := int64(-1)
	var stampHdrs []string
	if stripActive {
		stampHdrs = spec.StripHeaders
		if residualStrippable {
			stamp = spec.StripBelow
		} else {
			stamp = int64(math.MaxInt64)
		}
	}
	// CONVERGENCE: a pass that changed nothing keeps the ORIGINAL segment —
	// no rewrite install, no fsync. Without this every clean rewrote and
	// fsynced the ENTIRE decided prefix every cadence tick; on a large
	// steady-state log that is gigabytes of writes every few minutes, and
	// the commit path's own fsyncs queue behind the storm (measured as
	// multi-second commit stalls). Compacted+stripped segments reach this
	// fixed point after one pass — and the refreshed digest lets later
	// passes prove it without the scan this pass just paid.
	if removed == 0 && stripped == 0 && !consolidating {
		if err := cleaned.Delete(); err != nil {
			return nil, 0, err
		}
		c.refreshDigest(seg, stamp, stampHdrs)
		return seg, 0, nil
	}
	// The rewrite may hold the ONLY remaining copy of latest-per-key data;
	// make it durable before it replaces the source segment.
	if err := cleaned.Sync(); err != nil {
		return nil, removed, err
	}
	// Otherwise replace the old segment with the compacted one.
	if err = cleaned.Replace(seg); err != nil {
		return nil, removed, err
	}
	c.refreshDigest(cleaned, stamp, stampHdrs)
	return cleaned, removed, nil
}

// refreshDigest rebuilds and persists a segment's key digest after a clean
// pass scanned it, carrying the strip stamp the scan established. Best-effort:
// a failure only costs the next clean a scan.
func (c *compactCleaner) refreshDigest(seg *segment, stamp int64, stampHdrs []string) {
	d, err := buildKeyDigest(seg)
	if err == nil {
		d.stripVerifiedBelow = stamp
		d.stripHdrs = stampHdrs
		err = writeKeyDigest(seg, d)
	}
	if err != nil {
		slog.Warn("key digest refresh failed; will rebuild next clean",
			slog.String("name", c.Name), slog.String("err", err.Error()))
	}
}

// hasAnyHeader reports whether the message carries any of the given headers.
func hasAnyHeader(msg SerializedMessage, hdrs []string) bool {
	have := msg.Headers()
	if len(have) == 0 {
		return false
	}
	for _, h := range hdrs {
		if _, ok := have[h]; ok {
			return true
		}
	}
	return false
}

// stripFrame re-encodes the message inside a frame without the given header
// keys, preserving the frame's offset, timestamp, and leader epoch and the
// message's magic byte, attributes, key, and value. Returns changed=false
// (and no allocation) when none of the headers are present.
func stripFrame(ms messageSet, headers []string) ([]byte, bool, error) {
	msg := ms.Message()
	have := msg.Headers()
	found := false
	for _, h := range headers {
		if _, ok := have[h]; ok {
			found = true
			break
		}
	}
	if !found {
		return nil, false, nil
	}
	kept := make(map[string][]byte, len(have))
	for k, v := range have {
		kept[k] = v
	}
	for _, h := range headers {
		delete(kept, h)
	}
	data, err := encode(&Message{
		MagicByte:  msg.MagicByte(),
		Attributes: msg.Attributes(),
		Key:        msg.Key(),
		Value:      msg.Value(),
		Headers:    kept,
	})
	if err != nil {
		return nil, false, err
	}
	frame := make([]byte, msgSetHeaderLen+len(data))
	encoding.PutUint64(frame[offsetPos:], uint64(ms.Offset()))
	encoding.PutUint64(frame[timestampPos:], uint64(ms.Timestamp()))
	encoding.PutUint64(frame[leaderEpochPos:], ms.LeaderEpoch())
	encoding.PutUint32(frame[sizePos:], uint32(len(data)))
	copy(frame[msgSetHeaderLen:], data)
	return frame, true, nil
}

func cleanupEmptySegment(new, old *segment) error {
	// Delete the new segment if it's empty.
	if err := new.Delete(); err != nil {
		return err
	}
	// Also delete the old segment since it's been compacted. Set the replaced
	// flag since this is in the read path. Its digest goes with it.
	old.Lock()
	old.replaced = true
	old.Unlock()
	removeKeyDigest(old)
	return old.Delete()
}
