package commitlog

import (
	"sort"

	"github.com/pkg/errors"
)

// StoreObject names one object in one tier. It is what the orphan sweep
// reports and what the delete takes back, because a bare key does not say which
// store to look in once a log has more than one — and the sweep's whole subject
// is objects no manifest names, so nothing else could resolve it.
type StoreObject struct {
	Tier string
	Key  string
}

// TierObject describes where one segment's bytes live in a tier's store, and
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
	// Tier names the store holding this object. A log with one store writes
	// defaultTierName here; it is never empty, because an empty name cannot be
	// told apart from a field that was never written.
	Tier string
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
	// MovedFrom names the tier this segment was moved OUT of, and is set only
	// while that tier may still claim it. It is the one thing that makes an
	// interrupted move recoverable.
	//
	// A move commits by publishing the destination's manifest and releases by
	// publishing the source's, in that order — the same rule offload follows,
	// because the reverse loses a segment entirely on a crash. Between the two
	// Puts both tiers claim the segment, which is exactly the state
	// mergeTierManifests refuses. Without this field a crash in that window
	// leaves a log that will not open, produced by a routine background move.
	//
	// So the destination says where it came from, and the merge drops the
	// source's stale claim. The refusal survives for what it was written for:
	// two tiers claiming a segment with nothing to say why is still two stores
	// attached to one log, and is still refused. The marker is cleared by
	// republishing the destination once the source has let go, so it exists
	// only across the window it describes.
	//
	// Omitted when empty so an ordinary manifest is unchanged by its existence.
	MovedFrom string `json:",omitempty"`
}

// tierObject is meta's inverse: the same ten fields, plus the base offset that
// identifies which segment they describe. offloadMeta is what a segment knows
// about its own objects and TierObject is what the manifest says about them, so
// the two carry the same facts and only the direction differs.
func (m offloadMeta) tierObject(baseOffset int64, tier string) TierObject {
	return TierObject{
		BaseOffset:     baseOffset,
		Tier:           tier,
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
				Tier:           s.tier,
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

// readOnlyTiers seeds the ownership map from the configured chain. A name
// absent from it is writable, so a log with no tiers gets an empty map rather
// than a special case.
func readOnlyTiers(tiers []Tier) map[string]bool {
	m := make(map[string]bool, len(tiers))
	for _, t := range tiers {
		if t.ReadOnly {
			m[t.Name] = true
		}
	}
	return m
}

// tierWritable reports whether this log may write to the named tier.
func (l *commitLog) tierWritable(name string) bool {
	l.tierMu.RLock()
	defer l.tierMu.RUnlock()
	return !l.tierReadOnly[name]
}

// readOnlyTierSet is the set of tiers this log may not write to, taken once so
// a pass sees one consistent answer: a flag flipped halfway through would
// otherwise let a pass skip a tier's retention and rewrite its segments in the
// same run.
func (l *commitLog) readOnlyTierSet() map[string]bool {
	l.tierMu.RLock()
	defer l.tierMu.RUnlock()
	m := make(map[string]bool, len(l.tierReadOnly))
	for name, ro := range l.tierReadOnly {
		if ro {
			m[name] = true
		}
	}
	return m
}

// SetTierReadOnly grants or withdraws this log's right to write to ONE tier.
// See the interface doc.
//
// An unknown name is an error rather than a no-op: a caller handing over
// ownership of a tier it has misnamed would be told nothing, and would go on
// believing it had stopped writing to a store it is still writing to. That is
// the failure the single-writer contract exists to prevent.
func (l *commitLog) SetTierReadOnly(tier string, readOnly bool) error {
	if _, err := l.storeForTier(tier); err != nil {
		return err
	}
	l.tierMu.Lock()
	l.tierReadOnly[tier] = readOnly
	l.tierMu.Unlock()
	return nil
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
	// tier is the store the key lives in, carried rather than resolved later:
	// by the time this drains, the segment that superseded the object may have
	// moved on, and the object's home is a fact about the object.
	tier string
	key  string
	pin  *storeBacking
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
	if !l.hasTier() {
		return
	}
	l.tierMu.Lock()
	if l.tierManifestStale || len(l.reclaim) == 0 {
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
		if !l.tierWritable(e.tier) {
			// Not this process's to delete. Kept queued: ownership moves, and
			// the pass that has it will find the entry still here.
			kept = append(kept, e)
			continue
		}
		store, err := l.storeForTier(e.tier)
		if err != nil {
			// The queue named a tier this log no longer has. Keeping the entry
			// is the same trade as a failed delete: it costs space, and the
			// alternative — dropping it — would silently forget an object
			// nothing else will ever name.
			kept = append(kept, e)
			continue
		}
		if err := store.Delete(e.key); err != nil {
			kept = append(kept, e)
		}
	}

	l.tierMu.Lock()
	l.reclaim = append(kept, l.reclaim...)
	l.tierMu.Unlock()
}

// DeleteStoreObjects removes objects from their tiers. See the interface doc.
func (l *commitLog) DeleteStoreObjects(objs []StoreObject) ([]StoreObject, error) {
	if !l.hasTier() || len(objs) == 0 {
		return nil, nil
	}
	deleted := make([]StoreObject, 0, len(objs))
	for _, o := range objs {
		if !l.tierWritable(o.Tier) {
			return deleted, errors.Wrapf(errTierReadOnly, "tier %s", o.Tier)
		}
		store, err := l.storeForTier(o.Tier)
		if err != nil {
			return deleted, err
		}
		if err := store.Delete(o.Key); err != nil {
			return deleted, errors.Wrapf(err, "delete %s from tier %s", o.Key, o.Tier)
		}
		deleted = append(deleted, o)
	}
	return deleted, nil
}

// UnreferencedObjects lists store objects nothing this log can see names. See
// the interface doc — in particular, what "unreferenced" is judged from.
func (l *commitLog) UnreferencedObjects() ([]StoreObject, error) {
	if !l.hasTier() {
		return nil, nil
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
	referenced := make(map[string]bool)
	referenced[manifestKey] = true
	referenced[descriptorKey] = true
	for _, t := range l.Tiers {
		manifest, err := readTierManifest(t.Store)
		if err != nil {
			// Refuse rather than under-report. A manifest that exists but cannot
			// be read tells us nothing about what is live, and a garbage list
			// built without it would name objects the tier still depends on.
			return nil, errors.Wrapf(err, "read tier manifest for tier %s", t.Name)
		}
		for _, o := range manifest {
			if o.LogKey != "" {
				referenced[o.LogKey] = true
			}
			if o.IndexKey != "" {
				referenced[o.IndexKey] = true
			}
			// A block-compressed segment has three objects, not two. Its table is
			// the map from logical offsets to compressed blocks, and the local one
			// went with the local file at offload — deliberately, since it
			// describes bytes that no longer exist. So a table collected here is
			// not rebuildable: the log bytes stay intact and stop being readable.
			if o.BlocksKey != "" {
				referenced[o.BlocksKey] = true
			}
		}
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
		if s.blocksKey != "" {
			referenced[s.blocksKey] = true
		}
		s.RUnlock()
	}
	l.mu.RUnlock()

	// Listed per tier and compared against the WHOLE live set rather than that
	// tier's slice of it. Keys are allocated per upload and never reused, so a
	// key live in one tier cannot be garbage in another — and a key that somehow
	// appeared in two places is a fault to leave alone, not one to collect half
	// of.
	var orphans []StoreObject
	for _, t := range l.Tiers {
		keys, err := t.Store.List()
		if err != nil {
			return nil, errors.Wrapf(err, "list tier %s", t.Name)
		}
		for _, key := range keys {
			if !referenced[key] {
				orphans = append(orphans, StoreObject{Tier: t.Name, Key: key})
			}
		}
	}
	sort.Slice(orphans, func(i, j int) bool {
		if orphans[i].Tier != orphans[j].Tier {
			return orphans[i].Tier < orphans[j].Tier
		}
		return orphans[i].Key < orphans[j].Key
	})
	return orphans, nil
}
