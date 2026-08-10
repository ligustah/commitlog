package commitlog

import (
	"sort"

	"github.com/pkg/errors"
)

// TierObject describes where one segment's bytes live in a SegmentStore, and
// everything needed to place that segment without reading the object.
//
// It is the log's tier bookkeeping, and the tier manifest is a list of these.
// Keeping it IN the store is what makes a change of owner work: the new owner
// reads the manifest and knows what its predecessor uploaded, so it can serve
// those objects, avoid uploading a second copy of the same bytes, and reclaim
// them.
//
// The alternative — the new owner scanning the store and inferring what is
// current — is not sound in general: every upload allocates its own key, so a
// store can hold several objects claiming one base offset (a rewrite's
// predecessor, an upload orphaned by a crash before its publish) with nothing in
// the objects themselves saying which is current. Both cases are invisible to a
// scan.
//
// It is also the form the type is exported in, so a caller that moves ownership
// can hand the state over directly rather than through the store.
type TierObject struct {
	// BaseOffset identifies the segment. It is the key the caller matches on.
	BaseOffset int64
	// LogKey is the store object holding the segment's log bytes.
	LogKey string
	// IndexKey is the store object holding its index, empty when the index is
	// kept on local disk.
	IndexKey string
	// BlocksKey is the store object holding its block table, empty exactly when
	// BlockMode is false. Written at offload so that neither opening the tier
	// nor reading from it has to rebuild the table by walking the object.
	BlocksKey string
	// FirstOffset and LastOffset bound the records in the object, for offset
	// routing without reading it.
	FirstOffset int64
	LastOffset  int64
	// FirstWriteTime and LastWriteTime bound their timestamps, for time
	// routing and age-based retention.
	FirstWriteTime int64
	LastWriteTime  int64
	// Position is the segment's logical size; PhysPosition the object's byte
	// size, which differ under block compression.
	Position     int64
	PhysPosition int64
	// BlockMode records whether the object is block-compressed, since it
	// cannot be read correctly without knowing.
	BlockMode bool
}

// tierObject is meta's inverse: the same ten fields, plus the base offset that
// identifies which segment they describe. offloadMeta is what a segment knows
// about its own objects and TierObject is what the manifest says about them, so
// the two carry the same facts and only the direction differs.
func (m offloadMeta) tierObject(baseOffset int64) TierObject {
	return TierObject{
		BaseOffset:     baseOffset,
		LogKey:         m.LogKey,
		IndexKey:       m.IndexKey,
		BlocksKey:      m.BlocksKey,
		FirstOffset:    m.FirstOffset,
		LastOffset:     m.LastOffset,
		FirstWriteTime: m.FirstWriteTime,
		LastWriteTime:  m.LastWriteTime,
		Position:       m.Position,
		PhysPosition:   m.PhysPosition,
		BlockMode:      m.BlockMode,
	}
}

func (o TierObject) meta() offloadMeta {
	return offloadMeta{
		LogKey:         o.LogKey,
		IndexKey:       o.IndexKey,
		BlocksKey:      o.BlocksKey,
		FirstOffset:    o.FirstOffset,
		LastOffset:     o.LastOffset,
		FirstWriteTime: o.FirstWriteTime,
		LastWriteTime:  o.LastWriteTime,
		Position:       o.Position,
		PhysPosition:   o.PhysPosition,
		BlockMode:      o.BlockMode,
	}
}

// tierState returns this log's own view of its tier: one entry per offloaded
// segment, taken from the segments themselves rather than from the manifest.
//
// This is what writeTierManifest publishes. It reads the SEGMENTS because they
// hold the keys actually open for reading, which is what makes the published
// manifest describe reality rather than intent.
func (l *commitLog) tierState() ([]TierObject, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var out []TierObject
	for _, s := range l.segments {
		s.RLock()
		if s.store != nil {
			out = append(out, TierObject{
				BaseOffset:     s.BaseOffset,
				LogKey:         s.storeKey,
				IndexKey:       s.indexKey,
				BlocksKey:      s.blocksKey,
				FirstOffset:    s.firstOffset,
				LastOffset:     s.lastOffset,
				FirstWriteTime: s.firstWriteTime,
				LastWriteTime:  s.lastWriteTime,
				Position:       s.position,
				PhysPosition:   s.physPosition,
				BlockMode:      s.blockMode,
			})
		}
		s.RUnlock()
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BaseOffset < out[j].BaseOffset })
	return out, nil
}

// tierWritable reports whether this log may write to its SegmentStore.
func (l *commitLog) tierWritable() bool {
	l.tierMu.RLock()
	defer l.tierMu.RUnlock()
	return !l.tierReadOnly
}

// SetTierReadOnly grants or withdraws this log's right to write to its
// SegmentStore. See the interface doc.
func (l *commitLog) SetTierReadOnly(readOnly bool) {
	l.tierMu.Lock()
	l.tierReadOnly = readOnly
	l.tierMu.Unlock()
}

// errTierReadOnly is returned by anything that would write to a store this log
// does not own.
var errTierReadOnly = errors.New(
	"commitlog: this log's tier is read-only — it does not own writes to the store")

// pendingReclaim is one store object a rewrite superseded, waiting to be
// deleted.
//
// pin is the backing that was serving the object at the moment it was
// superseded, or nil where the object had no long-lived holder. The object is
// deletable once nothing still holds the pin: a reader that took the backing
// before the swap is reading the OLD object, and deleting it underneath them
// turns a rewrite into a read error.
type pendingReclaim struct {
	key string
	pin *storeBacking
}

// queueReclaim takes ownership of objects a rewrite superseded. They are
// deleted by a later drainReclaim, never here — see the type doc.
func (l *commitLog) queueReclaim(entries []pendingReclaim) {
	if len(entries) == 0 {
		return
	}
	l.tierMu.Lock()
	l.reclaim = append(l.reclaim, entries...)
	l.tierMu.Unlock()
}

// drainReclaim deletes the superseded objects no reader still holds, and keeps
// the rest for a later pass.
//
// Called at the START of a clean pass, which is what makes it safe on two
// counts that a delete at the point of rewrite cannot satisfy:
//
//   - Readers. A scan that took a backing before the rewrite is still on the old
//     object. By the next pass most are long gone, and one that is not simply
//     waits another pass — the queue is not a deadline.
//   - The manifest. Deleting an object the manifest still names leaves a
//     dangling reference: a reader that trusts the manifest opens a key that is
//     gone. The pass that queued these entries republished the manifest over the
//     NEW keys before returning, so by now nothing names them. If that publish
//     failed the queue is not drained at all (see tierManifestStale) — a crash
//     or an error between the two leaves an orphan, which UnreferencedObjects
//     reports and which costs storage rather than correctness.
//
// Errors are not returned. A store that refuses a delete has cost this log some
// space, not its consistency, and failing a clean — which may have just made a
// retention deadline — because a deletion of already-dead bytes did not land
// would be the worse trade. The entry stays queued and the next pass retries.
func (l *commitLog) drainReclaim() {
	if l.SegmentStore == nil {
		return
	}
	l.tierMu.Lock()
	if l.tierReadOnly || l.tierManifestStale || len(l.reclaim) == 0 {
		l.tierMu.Unlock()
		return
	}
	queued := l.reclaim
	l.reclaim = nil
	l.tierMu.Unlock()

	kept := make([]pendingReclaim, 0, len(queued))
	// Checked here, deleted on the next line, with no lock spanning the two —
	// the read-then-act shape that has caused this package's worst bugs. It is
	// safe, and the reason is worth stating so the next sweep does not re-derive
	// it: for a SUPERSEDED backing, refs can only fall.
	//
	// A reader takes a backing in exactly one place — acquireBacking, called only
	// by newSegmentScannerCache, under the segment's READ lock. swapReplacement
	// swaps s.backing to the new object under the segment's WRITE lock, so no
	// acquire can interleave with the swap, and afterwards the field names the
	// new object: nothing can reach the old one to acquire it again. Every entry
	// in this queue was put there BY that swap.
	//
	// So a zero here is not a lull that might end — it is terminal. Whereas
	// referenced() on a backing a segment is still serving means nothing, which
	// is what its own doc warns about.
	for _, e := range queued {
		if e.pin != nil && e.pin.referenced() {
			kept = append(kept, e)
			continue
		}
		if err := l.SegmentStore.Delete(e.key); err != nil {
			kept = append(kept, e)
		}
	}

	l.tierMu.Lock()
	l.reclaim = append(kept, l.reclaim...)
	l.tierMu.Unlock()
}

// DeleteStoreObjects removes objects from the SegmentStore. See the interface
// doc.
func (l *commitLog) DeleteStoreObjects(keys []string) ([]string, error) {
	if l.SegmentStore == nil || len(keys) == 0 {
		return nil, nil
	}
	if !l.tierWritable() {
		return nil, errTierReadOnly
	}
	deleted := make([]string, 0, len(keys))
	for _, key := range keys {
		if err := l.SegmentStore.Delete(key); err != nil {
			return deleted, errors.Wrapf(err, "delete %s", key)
		}
		deleted = append(deleted, key)
	}
	return deleted, nil
}

// UnreferencedObjects lists store objects nothing this log can see names. See
// the interface doc — in particular, what "unreferenced" is judged from.
func (l *commitLog) UnreferencedObjects() ([]string, error) {
	if l.SegmentStore == nil {
		return nil, nil
	}
	keys, err := l.SegmentStore.List()
	if err != nil {
		return nil, errors.Wrap(err, "list store")
	}

	// Live means named by the STORE'S MANIFEST or read by one of this log's
	// segments — the union, because each alone is wrong in a different way.
	//
	// The manifest alone would miss an object this log is reading but has not
	// yet republished: a rewrite installs new objects and then writes the
	// manifest, and in between the segment is on a key the manifest does not
	// name yet.
	//
	// This log's segments alone would miss everything ANOTHER process
	// offloaded since this one opened, which on a shared store is exactly the
	// data that must not be collected. That is the difference between
	// "unreferenced by me" and "unreferenced", and getting it wrong deletes
	// data a live peer is serving.
	//
	// The manifest and the descriptor are never garbage. Nothing references
	// either — they are what the store says ABOUT itself rather than what it
	// holds — so a rule built only from references collects both. The manifest
	// is what makes the tier readable; the descriptor is what makes it
	// identifiable, and collecting it leaves a log that refuses its own next
	// open, since a log that exists with no descriptor is a refusal.
	referenced := make(map[string]bool, len(keys))
	referenced[manifestKey] = true
	referenced[descriptorKey] = true
	if manifest, err := readTierManifest(l.SegmentStore); err == nil {
		for _, o := range manifest {
			if o.LogKey != "" {
				referenced[o.LogKey] = true
			}
			if o.IndexKey != "" {
				referenced[o.IndexKey] = true
			}
		}
	} else {
		// Refuse rather than under-report. A manifest that exists but cannot be
		// read tells us nothing about what is live, and a garbage list built
		// without it would name objects the tier still depends on.
		return nil, errors.Wrap(err, "read tier manifest")
	}
	l.mu.RLock()
	for _, s := range l.segments {
		s.RLock()
		if s.storeKey != "" {
			referenced[s.storeKey] = true
		}
		if s.indexKey != "" {
			referenced[s.indexKey] = true
		}
		s.RUnlock()
	}
	l.mu.RUnlock()

	var orphans []string
	for _, key := range keys {
		if !referenced[key] {
			orphans = append(orphans, key)
		}
	}
	sort.Strings(orphans)
	return orphans, nil
}
