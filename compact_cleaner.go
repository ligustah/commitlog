package commitlog

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"math"
	"sort"
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
	// cache is needed to invalidate an offloaded segment's index entry when a
	// rewrite supersedes the object it describes.
	cache *RemoteIndexCache
	// commitTier publishes a manifest naming one segment's freshly uploaded
	// objects. It is the commit point for a rewrite of an offloaded segment, and
	// it belongs to the log rather than to the cleaner: only the log can build a
	// manifest, because a manifest describes every segment and the cleaner sees
	// one at a time.
	commitTier func(baseOffset int64, meta offloadMeta) error
	// superseded collects the store objects this pass replaced. They are NOT
	// deleted here: a reader that opened a segment before its rewrite holds a
	// backing over the old key and is entitled to finish. Each entry carries
	// that backing, and the log queues them for a later pass to reclaim once
	// nothing holds it — see pendingReclaim and commitLog.drainReclaim.
	superseded []pendingReclaim
}

// NewCompactCleaner returns a new cleaner which performs log compaction by
// rewriting segments such that they contain only the last message for a given
// key.
func newCompactCleaner(opts compactCleanerOptions) *compactCleaner {
	if opts.MaxGoroutines == 0 {
		opts.MaxGoroutines = defaultCompactMaxGoroutines
	}
	return &compactCleaner{compactCleanerOptions: opts}
}

// CompactSpec performs log compaction under a CleanSpec: segments up to but
// excluding the active (last) segment are rewritten so that, below the spec's
// Ceiling, only the latest message per key survives. With a transactional
// spec it additionally removes aborted records, garbage-collects expired
// tombstones, drops control markers below StripBelow, and strips the
// transactional headers off surviving decided records.
//
// The log's leader epoch cache is NOT this function's business. A clean removes
// records without renumbering them or changing when a leadership began, so the
// live cache stays true and the caller only has to raise its floor; see the
// ClearEarliest call in commitLog.CleanWithSpec.
func (c *compactCleaner) CompactSpec(spec CleanSpec, segments []*segment) ([]*segment,
	int64, error) {

	if len(segments) <= 1 {
		return segments, -1, nil
	}

	slog.Debug("Compacting log", slog.String("name", c.Name))
	before := time.Now()
	compacted, removed, verified, err := c.compact(spec, segments)
	if err == nil {
		slog.Debug("Finished compacting log",
			slog.String("name", c.Name),
			slog.Int("removed", removed),
			slog.Int("before", len(segments)),
			slog.Int("after", len(compacted)),
			slog.Duration("duration", time.Since(before)),
		)

	}

	return compacted, verified, errors.Wrap(err, "failed to compact log")

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
	int, int64, error) {

	// Latest-per-key is computed by a streaming merge over per-segment sorted
	// key digests (persistent sidecars for sealed segments, in-memory for the
	// active one) into per-segment drop bitsets. Segments whose digest proves
	// the pass would change nothing are kept without reading a single record.
	digests, err := c.loadOrBuildDigests(segments)
	if err != nil {
		return nil, 0, -1, err
	}
	merged, err := c.mergeDigests(spec, segments, digests)
	if err != nil {
		return nil, 0, -1, err
	}

	var (
		compacted = make([]*segment, 0, len(segments))
		removed   = 0
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

	// Write new segments. Skip the last segment since we will not compact it.
	// TODO: Join segments that are below the bytes limit.
	//
	// Classify every sealed segment, then SPEND the rewrite budget in
	// DROP-DENSITY order — most droppable records first. Both the choice of
	// what to rewrite and the order the deadline is consumed in follow
	// density: a budget that merely selected densely but executed in offset
	// order still spent its seconds oldest-first whenever the deadline cut
	// mid-pass.
	type segDisposition int
	const (
		keepProtected segDisposition = iota
		keepConverged
		wantRewrite
	)
	n := len(segments) - 1
	disp := make([]segDisposition, n)
	for i, seg := range segments[:n] {
		switch {
		case horizon > 0 && seg.LastWriteTime() > horizon:
			disp[i] = keepProtected
		case c.canSkip(spec, digests[i], merged, i) && !seg.needsBlockConsolidation():
			disp[i] = keepConverged
		default:
			disp[i] = wantRewrite
		}
	}
	order := make([]int, 0, n)
	for i := range disp {
		if disp[i] == wantRewrite {
			order = append(order, i)
		}
	}
	dropCount := func(i int) int {
		if merged.drops[i] == nil {
			return 0
		}
		return merged.drops[i].count
	}
	// Two removals are order-sensitive, because each takes out a record that
	// GOVERNS older ones, and the budget can stop this loop between any two
	// segments:
	//
	//   - an expired tombstone shadows every earlier copy of its key. Remove it
	//     while a segment holding one of those copies is unrewritten and that
	//     copy becomes latest-per-key on the next pass: the DELETED VALUE COMES
	//     BACK, permanently, since nothing supersedes it any more.
	//   - a control marker decides the records of its transaction, which sit at
	//     LOWER offsets. Remove it while those records are still carrying their
	//     transactional headers and a reader buffers them waiting for a marker
	//     that no longer exists — or releases them on a LATER transaction's
	//     marker (see classify).
	//
	// Both governing records sit at a segment index >= everything they govern,
	// so one rule covers both: segments performing either removal go LAST, in
	// ascending order. Everything else keeps density ordering, which is where
	// nearly every segment lands.
	//
	// A governed record is then either in phase 1 — rewritten before its
	// governor, or the cut lands before phase 2 and the governor survives to
	// keep governing it — or in phase 2 at a lower index, which ascending order
	// already visits first.
	lateRewrite := func(i int) bool {
		if merged.gcSegs[i] {
			return true
		}
		if !(spec.StripBelow > 0 && len(spec.StripHeaders) > 0) {
			return false
		}
		for _, off := range digests[i].control {
			if off < spec.StripBelow {
				return true
			}
		}
		return false
	}
	late := make([]bool, n)
	for i := range late {
		late[i] = lateRewrite(i)
	}
	sort.SliceStable(order, func(a, b int) bool {
		la, lb := late[order[a]], late[order[b]]
		if la != lb {
			return !la // order-insensitive segments first
		}
		if la {
			return order[a] < order[b] // never overtake what you govern
		}
		return dropCount(order[a]) > dropCount(order[b])
	})

	// Rewrite phase, density order, one shared budget and block accumulator.
	// Epoch assignments are only COLLECTED here (the cache requires
	// ascending-offset feeding, which the offset-order assembly below does).
	// One budget for local disk and ONE PER TIER, drawn on by where the
	// segment's bytes are. A tiered rewrite downloads the object, rewrites it
	// and uploads the result, where a local one touches local disk only — so a
	// single shared wall-clock budget lets one slow remote rewrite consume the
	// pass and starve local compaction while local debt grows.
	//
	// Per tier rather than one for all of them because the tiers in a chain are
	// not alike either: a rewrite in a fast nearby store and one in a cold
	// archive differ by as much as local and remote do, and a caller that gives
	// the archive a small budget should not thereby shrink the hot tier's.
	// A tier with no entry in TierBudgets falls back to RewriteBudget, so a
	// caller that sets nothing sees exactly the previous behaviour.
	budget := newRewriteBudget(spec.maxRewrites, spec.RewriteBudget)
	tierBudgets := make(map[string]*rewriteBudget)
	budgetFor := func(tier string) *rewriteBudget {
		if b, ok := tierBudgets[tier]; ok {
			return b
		}
		d, ok := spec.TierBudgets[tier]
		if !ok || d == 0 {
			d = spec.RewriteBudget
		}
		b := newRewriteBudget(spec.maxRewrites, d)
		tierBudgets[tier] = b
		return b
	}
	bw := &blockWriter{}
	sc := newBlockCache() // one decode-buffer pair for the whole pass
	rewritten := make([]*segment, n)
	didRewrite := make([]bool, n)
	// Skipping for budget is only safe in the order-INSENSITIVE phase. A late
	// segment removes a record that governs older ones, and may only do so if
	// everything it governs was rewritten in this same pass — so once any
	// earlier segment has been skipped, no governor may be rewritten at all.
	// Without this, giving the two tiers independent budgets would quietly
	// reintroduce the orphaning the ordering rule exists to prevent: a governed
	// segment skipped for want of tier budget, its governor rewritten anyway
	// because the local budget still had room.
	skipped := false
	for _, i := range order {
		b := budget
		tiered := segments[i].isOffloaded()
		if tiered {
			b = budgetFor(segments[i].tier)
		}
		// A read-only tier is not a budget: no rewrite of a tiered segment happens at
		// all, because for a caller that does not own tier writes a single one
		// is corruption of shared storage rather than wasted work. It still
		// counts as skipped, so a governor of records left in the tier is not
		// removed ahead of them.
		// Read bare, like isOffloaded just above it: both are set when the
		// segment attaches to a store and not touched again, and the pass holds
		// no segment lock here.
		if tiered && spec.skipTiers[segments[i].tier] {
			skipped = true
			continue
		}
		if late[i] && (skipped || !b.allow()) {
			break // never remove a governor ahead of what it governs
		}
		if !b.allow() {
			skipped = true
			continue // this tier is spent; the other may still have room
		}
		cleaned, msgsRemoved, err := c.cleanSegment(spec, segments[i], merged.drops[i], bw, sc)
		if err != nil {
			return nil, 0, -1, err
		}
		b.note()
		rewritten[i], didRewrite[i] = cleaned, true
		removed += msgsRemoved
	}

	// Assembly phase, offset order: build the segment list and advance the
	// verified floor over the consecutive verified prefix.
	for i, seg := range segments[:n] {
		segEnd := seg.NextOffset() - 1
		switch {
		case disp[i] == keepProtected:
			// Within the protected horizon — keep whole (headers and abort
			// markers intact, so the floor chain breaks here).
			verifiedChain = false
			compacted = append(compacted, seg)
			continue
		case disp[i] == keepConverged:
			// Digest proves a rewrite would keep every record byte-for-byte.
			compacted = append(compacted, seg)
			if verifiedChain {
				verified = segEnd
			}
			continue
		case !didRewrite[i]:
			// Deferred: the budget went to denser segments. Drops/strips/
			// consolidation wait for a later tick, which re-derives them from
			// the digests — the budget is what lets a short-lived process pay
			// down an arbitrarily large debt a slice at a time.
			verifiedChain = false
			compacted = append(compacted, seg)
			continue
		}
		if rewritten[i] != nil {
			compacted = append(compacted, rewritten[i])
		}
		if verifiedChain {
			verified = segEnd
		}
	}

	// Add the last segment back in to the compacted list.
	last := segments[len(segments)-1]
	compacted = append(compacted, last)

	// Stripping applies to offsets strictly below StripBelow, so the record
	// AT StripBelow keeps its headers; a spec without strip semantics
	// verifies nothing.
	if verified > spec.StripBelow-1 {
		verified = spec.StripBelow - 1
	}
	return compacted, removed, verified, nil
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
	// gcSegs[i]: segment i holds an expired tombstone this pass removes. Unlike
	// every other drop that one removes a key's NEWEST copy, so the rewrite
	// order has to keep it from outrunning the copies it shadows (see compact).
	gcSegs []bool
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
		gcSegs:       make([]bool, len(segments)),
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
				if r.offset > spec.ceiling {
					continue
				}
				if spec.Aborted != nil && spec.Aborted(r.offset) {
					// Aborted copies never shadow a committed value at any
					// decided offset; the pass removes them only below the
					// ceiling (the record AT the ceiling is always retained).
					if r.offset < spec.ceiling {
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
			if off := partRec[j].offset; off != latestOff && off < spec.ceiling {
				drop(segIdx, off)
			}
		}
		// The surviving copy itself: an expired tombstone vanishes.
		if gcActive && latestRec.flags&digestFlagTombstone != 0 &&
			latestOff < spec.TombstoneGCBelow && latestRec.ts > 0 &&
			latestRec.ts < now-int64(spec.TombstoneRetention) {
			drop(latestIdx, latestOff)
			res.gcSegs[latestIdx] = true
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
			if r.offset < spec.ceiling && spec.Aborted(r.offset) {
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
			d, err := buildKeyDigest(seg, newBlockCache())
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
	if offset >= spec.ceiling {
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

// blockWriter accumulates retained message sets and flushes them as
// cleanBlockTarget-sized blocks (one WriteMessageSet call each): concatenated
// message sets are a valid sequence, and entriesForMessageSet computes every
// logical position from the flush-time segment position. One writer is shared
// across a whole pass so the scratch buffer is grown once.
type blockWriter struct {
	seg *segment
	buf []byte
}

func (w *blockWriter) reset(seg *segment) {
	w.seg = seg
	w.buf = w.buf[:0]
}

func (w *blockWriter) add(ms []byte) error {
	w.buf = append(w.buf, ms...)
	if len(w.buf) >= cleanBlockTarget {
		return w.flush()
	}
	return nil
}

func (w *blockWriter) flush() error {
	if len(w.buf) == 0 {
		return nil
	}
	entries := entriesForMessageSet(w.seg.Position(), w.buf)
	if err := w.seg.WriteMessageSet(w.buf, entries); err != nil {
		return err
	}
	w.buf = w.buf[:0]
	return nil
}

func (c *compactCleaner) cleanSegment(spec CleanSpec, seg *segment, drops *dropSet,
	bw *blockWriter, sc *blockCache) (*segment, int, error) {

	cleaned, err := seg.Cleaned()
	if err != nil {
		return nil, 0, err
	}
	bw.reset(cleaned)
	var (
		ss = newSegmentScannerCache(seg, sc)
		// A consolidation pass rewrites even byte-identical content: the
		// rewrite's value is the block layout itself, so convergence must not
		// discard it (it would re-rewrite and re-discard every tick).
		consolidating = seg.needsBlockConsolidation()
		removed       = 0
		stripped      = 0
		// Records this pass declined to re-sign because they failed their own
		// CRC. They are carried through untouched, not dropped.
		corrupt     = 0
		stripActive = spec.StripBelow > 0 && len(spec.StripHeaders) > 0
		// Does any surviving record still carry a strip-target header (i.e.
		// sits at/above StripBelow with one present)? Decides how far the
		// refreshed digest's strip stamp may reach.
		residualStrippable = false
	)
	defer ss.Close()
	for {
		ms, _, err := ss.Scan()
		if err != nil {
			// The rewrite is about to REPLACE this segment, so a scan that
			// stopped early would install a copy missing everything past the
			// damage and then delete the original — silently, reporting success.
			// The partial copy is thrown away and the source left exactly as it
			// is.
			if !errors.Is(err, io.EOF) {
				cleaned.Delete()
				return nil, 0, fmt.Errorf("%w: rewrite of segment %d: %w",
					ErrSegmentUnreadable, seg.BaseOffset, err)
			}
			break
		}
		var (
			offset = ms.Offset()
			msg    = ms.Message()
		)
		disp := c.classify(spec, offset, msg, drops)
		if disp == dispRemove {
			removed++
			continue
		}
		out := []byte(ms)
		if disp == dispStrip {
			sf, changed, err := stripFrame(ms, spec.StripHeaders)
			switch {
			case errors.Is(err, ErrCorruptRecord):
				// Copy it verbatim instead, keeping its failing checksum. The
				// record stays exactly as damaged as it was and every reader
				// still gets ErrCorruptRecord from it — the honest outcome, and
				// the one a rewrite would have taken away.
				//
				// Not a failed clean, deliberately: the cleaner runs unattended
				// on a timer, so returning an error here wedges compaction AND
				// retention behind one bad record until someone intervenes,
				// turning a single unreadable record into a full disk. Declining
				// to rewrite it costs nothing by comparison.
				corrupt++
				// It keeps its strip-target headers, so the digest's strip stamp
				// must not go on to claim this segment has none left below the
				// boundary — a later pass would trust that and skip the scan.
				residualStrippable = true
				slog.Warn("record failed its CRC; copied without stripping rather than re-signing it",
					slog.String("name", c.Name), slog.Int64("offset", offset),
					slog.String("err", err.Error()))
			case err != nil:
				return nil, removed, err
			case changed:
				out = sf
				stripped++
			}
		} else if stripActive && msg.Attributes()&AttrControl == 0 &&
			offset >= spec.StripBelow && hasAnyHeader(msg, spec.StripHeaders) {
			residualStrippable = true
		}
		if err := bw.add(out); err != nil {
			return nil, removed, err
		}
	}
	if err := bw.flush(); err != nil {
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
	// no rewrite install, no fsync. Compacted+stripped segments reach this
	// fixed point after one pass, and the refreshed digest lets later passes
	// prove it without the scan this pass just paid. (Consolidation passes
	// are exempt: their value IS the rewrite.)
	if removed == 0 && stripped == 0 && !consolidating {
		if err := cleaned.Delete(); err != nil {
			return nil, 0, err
		}
		c.refreshDigest(seg, stamp, stampHdrs, sc)
		return seg, 0, nil
	}
	// The rewrite may hold the ONLY remaining copy of latest-per-key data;
	// make it durable before it replaces the source segment.
	if err := cleaned.Sync(); err != nil {
		return nil, removed, err
	}
	// Install the rewrite. An offloaded segment cannot take the local path:
	// Replace renames over the source's local files, and an offloaded segment
	// has none. It instead becomes the current objects of that segment,
	// and the segment object itself carries on — so the caller keeps the SAME
	// segment rather than the working copy, which is only the vehicle.
	if seg.isOffloaded() {
		meta, reclaim, err := seg.uploadReplacement(cleaned)
		if err != nil {
			return nil, removed, err
		}
		// The commit, between the upload and the swap. Until the manifest names
		// the new objects the segment goes on serving the one it supersedes, so
		// a failure here leaves a rewrite that simply did not happen.
		if err := c.commitTier(seg.BaseOffset, meta); err != nil {
			return nil, removed, err
		}
		if err := seg.swapReplacement(cleaned, meta); err != nil {
			return nil, removed, err
		}
		c.superseded = append(c.superseded, reclaim...)
		if err := cleaned.Delete(); err != nil { // the local vehicle is done
			return nil, removed, err
		}
		c.refreshDigest(seg, stamp, stampHdrs, sc)
		return seg, removed, nil
	}
	if err = cleaned.Replace(seg); err != nil {
		return nil, removed, err
	}
	c.refreshDigest(cleaned, stamp, stampHdrs, sc)
	return cleaned, removed, nil
}

// rewriteBudget bounds how much rewrite debt one pass pays down. Time is
// the operative bound (it encodes "finish inside the process's expected
// lifetime" and self-adjusts to segment size and disk speed); maxRewrites is
// an unexported deterministic seam for tests. At least one rewrite always
// proceeds so debt drains even under a pathologically small budget.
type rewriteBudget struct {
	max      int
	deadline time.Time
	spent    int
}

func newRewriteBudget(max int, d time.Duration) *rewriteBudget {
	b := &rewriteBudget{max: max}
	if d > 0 {
		b.deadline = time.Now().Add(d)
	}
	return b
}

func (b *rewriteBudget) allow() bool {
	if b.max > 0 && b.spent >= b.max {
		return false
	}
	return b.deadline.IsZero() || b.spent == 0 || !time.Now().After(b.deadline)
}

func (b *rewriteBudget) note() { b.spent++ }

// consolidateSegments is the block-layout maintenance pass for logs WITHOUT
// compaction: sealed segments whose block index tripped
// needsBlockConsolidation are rewritten verbatim (every record kept, offsets
// and leader epochs unchanged) with cleanBlockTarget-sized blocks, under the
// same budget semantics as compaction rewrites. The active (last) segment is
// never touched. Non-compacted logs never earn a recovery floor (nothing is
// stripped), so the pass reports no verified boundary by design.
func consolidateSegments(segments []*segment, maxRewrites int, budgetDur time.Duration) ([]*segment, error) {
	if len(segments) <= 1 {
		return segments, nil
	}
	out := make([]*segment, 0, len(segments))
	budget := newRewriteBudget(maxRewrites, budgetDur)
	bw := &blockWriter{}
	sc := newBlockCache() // one decode-buffer pair for the whole pass
	for _, seg := range segments[:len(segments)-1] {
		if !seg.needsBlockConsolidation() || !budget.allow() {
			out = append(out, seg)
			continue
		}
		cleaned, err := seg.Cleaned()
		if err != nil {
			return nil, err
		}
		bw.reset(cleaned)
		ss := newSegmentScannerCache(seg, sc)
		defer ss.Close()
		for ms, _, err := ss.Scan(); err == nil; ms, _, err = ss.Scan() {
			if err := bw.add(ms); err != nil {
				return nil, err
			}
		}
		if err := bw.flush(); err != nil {
			return nil, err
		}
		if err := cleaned.Sync(); err != nil {
			return nil, err
		}
		if err := cleaned.Replace(seg); err != nil {
			return nil, err
		}
		out = append(out, cleaned)
		budget.note()
	}
	return append(out, segments[len(segments)-1]), nil
}

// refreshDigest rebuilds and persists a segment's key digest after a clean
// pass scanned it, carrying the strip stamp the scan established. Best-effort:
// a failure only costs the next clean a scan.
func (c *compactCleaner) refreshDigest(seg *segment, stamp int64, stampHdrs []string, sc *blockCache) {
	d, err := buildKeyDigest(seg, sc)
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
	// Verify BEFORE re-encoding. This function recomputes the CRC over whatever
	// bytes it is handed, so re-encoding a record that is already damaged
	// LAUNDERS it: the corruption is signed by a fresh, valid checksum, and every
	// later read — including the CRC-verifying one — reports the record as sound.
	// The evidence that it was ever damaged is destroyed by the rewrite, and
	// cannot be recovered afterwards.
	//
	// Every other frame the cleaner writes is copied verbatim, so a corrupt
	// record keeps its failing checksum and stays detectable. This path is the
	// only one that can certify a lie, which is why the check belongs here rather
	// than at whatever read eventually trips over it.
	if want, got := msg.Crc(), crc32.Checksum(msg[4:], crc32cTable); want != got {
		return nil, false, errors.Wrapf(ErrCorruptRecord,
			"record at offset %d: expected CRC 0x%08x, got 0x%08x", ms.Offset(), want, got)
	}
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
	// The header's checksum, over the four fields just written. This is the only
	// place besides newMessageSetFromProto that builds a frame by hand, and
	// forgetting it here would write records no reader accepts — the whole suite
	// went red exactly once for that reason.
	encoding.PutUint32(frame[headerCrcPos:], headerCrc(frame))
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
