package commitlog

import (
	"log/slog"
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
	return c.CompactSpec(spec, segments)
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
	*leaderEpochCache, error) {

	if len(segments) <= 1 {
		return segments, nil, nil
	}

	slog.Debug("Compacting log", slog.String("name", c.Name))
	before := time.Now()
	compacted, epochCache, removed, err := c.compact(spec, segments)
	if err == nil {
		slog.Debug("Finished compacting log %s",
			slog.String("name", c.Name),
			slog.Int("removed", removed),
			slog.Int("before", len(segments)),
			slog.Int("after", len(compacted)),
			slog.Duration("duration", time.Since(before)),
		)

	}

	return compacted, epochCache, errors.Wrap(err, "failed to compact log")

}

type keyOffset struct {
	sync.RWMutex
	offset int64
}

func (k *keyOffset) set(offset int64) {
	k.Lock()
	if offset > k.offset {
		k.offset = offset
	}
	k.Unlock()
}

func (k *keyOffset) get() int64 {
	k.RLock()
	defer k.RUnlock()
	return k.offset
}

func (c *compactCleaner) compact(spec CleanSpec, segments []*segment) ([]*segment,
	*leaderEpochCache, int, error) {

	// Compact messages up to the last segment or the spec ceiling, whichever
	// is first, by scanning keys and retaining only the latest.
	var (
		compacted  = make([]*segment, 0, len(segments))
		epochCache = newLeaderEpochCacheNoFile(c.Name)
		removed    = 0
		keyOffsets = c.scanKeys(spec, segments)
	)

	// A protected compaction horizon: a segment whose newest write is within
	// MinAge is kept intact. keyOffsets still records the latest offset per key
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
	for _, seg := range segments[:len(segments)-1] {
		if horizon > 0 && seg.LastWriteTime() > horizon {
			// Within the protected horizon — keep whole.
			compacted = append(compacted, seg)
			continue
		}
		cleaned, msgsRemoved, err := c.cleanSegment(spec, seg, keyOffsets, epochCache)
		if err != nil {
			return nil, nil, 0, err
		}
		if cleaned != nil {
			compacted = append(compacted, cleaned)
		}
		removed += msgsRemoved
	}

	// Add the last segment back in to the compacted list.
	last := segments[len(segments)-1]
	compacted = append(compacted, last)

	// Maintain start offset for each new leader epoch for the last segment.
	ss := newSegmentScanner(last)
	for ms, _, err := ss.Scan(); err == nil; ms, _, err = ss.Scan() {
		leaderEpoch := ms.LeaderEpoch()
		if leaderEpoch > epochCache.LastLeaderEpoch() {
			if err := epochCache.Assign(leaderEpoch, ms.Offset()); err != nil {
				return nil, nil, 0, err
			}
		}
	}

	return compacted, epochCache, removed, nil
}

// disposition classifies one message under a CleanSpec.
type disposition int

const (
	dispRetain disposition = iota // copy verbatim
	dispRemove                    // drop from the log
	dispStrip                     // retain, rewritten without StripHeaders
)

// classify decides a message's fate. keyOffsets must have been built by
// scanKeys under the SAME spec (aborted/control/nil-key/above-ceiling records
// are absent from it, so they can never shadow a committed value).
func (c *compactCleaner) classify(spec CleanSpec, offset int64, msg SerializedMessage,
	ts int64, keyOffsets *sync.Map, now int64) disposition {

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
	latest, ok := keyOffsets.Load(string(key))
	if !ok || offset != latest.(*keyOffset).get() {
		// Superseded by a newer copy of the key (or, !ok, unreachable for a
		// scanned record — treat conservatively as superseded only when a
		// newer copy is known).
		if ok {
			return dispRemove
		}
		return dispRetain
	}
	// Latest copy of its key. Expired tombstone → the key vanishes.
	if attrs&AttrTombstone != 0 && spec.TombstoneRetention > 0 &&
		offset < spec.TombstoneGCBelow && ts > 0 &&
		ts < now-int64(spec.TombstoneRetention) {
		return dispRemove
	}
	if offset < spec.StripBelow && len(spec.StripHeaders) > 0 {
		return dispStrip
	}
	return dispRetain
}

func (c *compactCleaner) cleanSegment(spec CleanSpec, seg *segment, keyOffsets *sync.Map,
	epochCache *leaderEpochCache) (*segment, int, error) {

	cleaned, err := seg.Cleaned()
	if err != nil {
		return nil, 0, err
	}
	var (
		ss       = newSegmentScanner(seg)
		removed  = 0
		stripped = 0
		now      = timestamp()
	)
	for ms, _, err := ss.Scan(); err == nil; ms, _, err = ss.Scan() {
		var (
			offset      = ms.Offset()
			leaderEpoch = ms.LeaderEpoch()
			msg         = ms.Message()
		)
		disp := c.classify(spec, offset, msg, ms.Timestamp(), keyOffsets, now)
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
		}
		entries := entriesForMessageSet(cleaned.Position(), out)
		if err := cleaned.WriteMessageSet(out, entries); err != nil {
			return nil, removed, err
		}
		// Maintain start offset for each new leader epoch.
		if leaderEpoch > epochCache.LastLeaderEpoch() {
			if err := epochCache.Assign(leaderEpoch, offset); err != nil {
				return nil, removed, err
			}
		}
	}

	if cleaned.IsEmpty() {
		// If the new segment is empty, remove it along with the old one.
		return nil, removed, cleanupEmptySegment(cleaned, seg)
	}
	// CONVERGENCE: a pass that changed nothing keeps the ORIGINAL segment —
	// no rewrite install, no fsync. Without this every clean rewrote and
	// fsynced the ENTIRE decided prefix every cadence tick; on a large
	// steady-state log that is gigabytes of writes every few minutes, and
	// the commit path's own fsyncs queue behind the storm (measured as
	// multi-second commit stalls). Compacted+stripped segments reach this
	// fixed point after one pass.
	if removed == 0 && stripped == 0 {
		if err := cleaned.Delete(); err != nil {
			return nil, 0, err
		}
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
	return cleaned, removed, nil
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

// scanKeys builds the latest-offset-per-key map under the spec: control
// records, nil-key records, aborted records, and records at or above the
// ceiling never enter the map — so none of them can shadow (and thereby
// delete) a committed value.
func (c *compactCleaner) scanKeys(spec CleanSpec, segments []*segment) *sync.Map {
	var (
		wg            sync.WaitGroup
		keyOffsets    = new(sync.Map)
		numGoroutines = c.MaxGoroutines
		segmentC      = make(chan *segment, len(segments))
	)
	if numGoroutines > len(segments) {
		numGoroutines = len(segments)
	}

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go c.scanSegments(spec, segmentC, &wg, keyOffsets)
	}

	for _, seg := range segments {
		segmentC <- seg
	}
	close(segmentC)

	wg.Wait()
	return keyOffsets
}

func (c *compactCleaner) scanSegments(spec CleanSpec, ch <-chan *segment, wg *sync.WaitGroup, keyOffsets *sync.Map) {
	for seg := range ch {
		ss := newSegmentScanner(seg)
		for ms, _, err := ss.Scan(); err == nil; ms, _, err = ss.Scan() {
			offset := ms.Offset()
			if offset > spec.Ceiling {
				// Offsets within a segment are ordered; the rest of this
				// segment is above the ceiling too. The record AT the
				// ceiling is decided (ceiling = LSO / HW) and participates,
				// matching the pre-spec scan boundary exactly.
				break
			}
			msg := ms.Message()
			if msg.Attributes()&AttrControl != 0 {
				continue
			}
			if spec.Aborted != nil && spec.Aborted(offset) {
				continue
			}
			key := msg.Key()
			if key == nil {
				continue
			}
			curr, loaded := keyOffsets.LoadOrStore(
				string(key), &keyOffset{offset: offset})
			if loaded {
				curr.(*keyOffset).set(offset)
			}
		}
	}
	wg.Done()
}

func cleanupEmptySegment(new, old *segment) error {
	// Delete the new segment if it's empty.
	if err := new.Delete(); err != nil {
		return err
	}
	// Also delete the old segment since it's been compacted. Set the replaced
	// flag since this is in the read path.
	old.Lock()
	old.replaced = true
	old.Unlock()
	return old.Delete()
}
