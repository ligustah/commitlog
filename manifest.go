package commitlog

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"sort"

	"github.com/pkg/errors"
)

// manifestKey is the object describing what a log's tier holds. One per store,
// which is one per log — a store is scoped to a single log, so the name needs
// no qualifier and stays predictable enough to fetch without a listing.
const manifestKey = "manifest"

// manifestVersion is the format the writer emits, and the only one a reader
// accepts. Refusing an unknown version rather than guessing at its layout is
// the point of carrying the field.
//
// A `>` comparison would also accept version 0, which is what an absent field
// decodes to, so any JSON object that happened to parse would be read as a
// manifest. Equality is the whole integrity check on this file.
//
// Version 2 adds BlocksKey. A version 1 manifest is refused rather than adapted:
// its block-compressed entries name no block table, and the only way to serve
// them would be to rebuild each table by walking its object — the cost the key
// exists to remove. Nothing is deployed against version 1, so there is nothing
// to migrate; a store written by an older build is re-offloaded, not converted.
//
// Version 3 adds Tier, naming the store an object lives in. It is the first step
// of multi-store tiering (docs/multi-store-tiering.md) and carries no behaviour
// yet: one tier is configurable, so every entry names defaultTierName. It goes in
// ahead of the capability so that the manifest a store is already carrying can
// describe itself once the second tier exists, rather than needing a second
// version bump at the moment it matters. Refused rather than adapted, for the
// same reason as version 1.
const manifestVersion = 3

// defaultTierName is the conventional name for the one tier of a single-store
// log. The library no longer writes it — every object records the name of the
// Tier it went into (Options.Tiers) — and it survives as the name the tests and
// simple callers use, and as the place the argument below is written down.
//
// A NAME rather than an empty string, and that is not cosmetic. An absent JSON
// field decodes to "", so an empty Tier would be indistinguishable from a
// manifest written by something that never set one — the same sentinel collision
// that made CleanSpec.Ceiling an int64 bug, where the zero value had to mean both
// "unset" and a real value a caller needs. A version 3 manifest must name its
// tier, and readTierManifest refuses one that does not.
const defaultTierName = "default"

// tierManifest is the store's own description of itself: which object holds
// which segment, and the offset and time ranges each covers.
//
// It exists because a tier that holds bytes it cannot describe is not
// self-contained. Without it the mapping from offset range to object would have
// to live beside the log, so the objects would be readable but uninterpretable
// on their own — a process that has the store and not the directory could not
// say what it was looking at, and the bookkeeping would have to be carried out
// of band by whoever has consensus. That is commitlog's own segment index, and
// it belongs with the segments.
//
// It is also the COMMIT POINT for the tier, written after the objects it names
// and before anything acts on them being committed: an object no manifest names
// was never committed, which makes a crash between an upload and its publish a
// recognisable orphan rather than an ambiguity, and local bytes are never
// dropped against an entry that is not yet published.
type tierManifest struct {
	Version  int          `json:"version"`
	Segments []TierObject `json:"segments"`
}

// writeTierManifest publishes the current set of offloaded segments, with any
// pending entries taking precedence over the log's own view of the same base
// offset.
//
// It rebuilds from the log's segments rather than patching an existing
// manifest. The set is small (one entry per offloaded segment), and a rebuild
// cannot drift: a patch has to be right about what changed, and every path that
// changes the tier — offload, rewrite, retention — would have to agree.
//
// A pending entry is an object that is uploaded and complete but that its
// segment has not switched to yet, and it exists because the publish is the
// COMMIT: it has to name the new object before anything acts on the commit
// having happened. A first offload cannot drop its local bytes until then, and a
// rewrite cannot stop serving the object it supersedes. So at the moment of the
// commit the log's own view and the tier's necessarily disagree, and the pending
// entry is the difference — which is why it overrides rather than adds. A
// republish after the segment set changes passes none.
//
// Caller must not hold l.mu, and must not hold the segment lock of any segment a
// pending entry describes: tierState reads every segment under its read lock.
func (l *commitLog) writeTierManifest(pending ...TierObject) error {
	if !l.hasTier() {
		return nil
	}
	objs, err := l.tierState()
	if err != nil {
		return err
	}
	if len(pending) > 0 {
		override := make(map[int64]TierObject, len(pending))
		for _, p := range pending {
			override[p.BaseOffset] = p
		}
		for i, o := range objs {
			if p, ok := override[o.BaseOffset]; ok {
				objs[i] = p
				delete(override, o.BaseOffset)
			}
		}
		for _, p := range pending {
			if _, ok := override[p.BaseOffset]; ok {
				objs = append(objs, p)
				delete(override, p.BaseOffset)
			}
		}
		sort.Slice(objs, func(i, j int) bool { return objs[i].BaseOffset < objs[j].BaseOffset })
	}
	// One manifest PER TIER, each naming only that tier's objects, because a
	// tier that holds bytes it cannot describe is not self-contained — which is
	// the principle ExportTierState/ImportTierState were removed to establish
	// (docs/tier-layering.md). A single manifest in the nearest tier would mean
	// a node adopting the archive alone found nothing to adopt, and losing the
	// nearest tier would lose the map to objects that are perfectly intact.
	//
	// The price is that two manifests can disagree about who owns an object.
	// That is representable, so the merge at open refuses it; see
	// mergeTierManifests.
	byTier := make(map[string][]TierObject, len(l.Tiers))
	for _, o := range objs {
		byTier[o.Tier] = append(byTier[o.Tier], o)
	}
	for _, t := range l.Tiers {
		// A tier this log does not own is not written to, manifest included: the
		// manifest is a claim about the store, and a process that does not own
		// the store has no business republishing what it holds.
		if !l.tierWritable(t.Name) {
			continue
		}
		body, err := json.Marshal(tierManifest{Version: manifestVersion, Segments: byTier[t.Name]})
		if err != nil {
			return errors.Wrapf(err, "encode tier manifest for tier %s", t.Name)
		}
		if err := t.Store.Put(manifestKey, bytes.NewReader(body), int64(len(body))); err != nil {
			return errors.Wrapf(err, "put tier manifest for tier %s", t.Name)
		}
	}
	return nil
}

// mergeTierManifests unions what each tier says it holds.
//
// A segment lives in exactly ONE tier, so two manifests naming the same base
// offset is not a state to resolve by picking one: it means two stores were
// attached to the same log, or a move was interrupted somewhere no crash should
// be able to leave it. Picking either would serve one tier's bytes and
// silently orphan the other's, and picking by order would make the answer
// depend on the caller's configuration rather than on the stores. So it is
// refused, and the refusal names both tiers.
//
// This is the cost of per-tier manifests, paid deliberately: option (a) in
// docs/multi-store-tiering.md made the disagreement unrepresentable, at the
// price of a tier that cannot describe itself.
func mergeTierManifests(perTier map[string][]TierObject) ([]TierObject, error) {
	owner := make(map[int64]string)
	var out []TierObject
	for tier, objs := range perTier {
		for _, o := range objs {
			if prev, ok := owner[o.BaseOffset]; ok {
				a, b := prev, tier
				if a > b {
					a, b = b, a
				}
				return nil, errors.Errorf(
					"commitlog: tiers %s and %s both claim segment %d; "+
						"one log's segments are in two stores",
					a, b, o.BaseOffset)
			}
			owner[o.BaseOffset] = tier
			out = append(out, o)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BaseOffset < out[j].BaseOffset })
	return out, nil
}

// readMergedTierManifest reads every tier's manifest and merges them.
func (l *commitLog) readMergedTierManifest() ([]TierObject, error) {
	if !l.hasTier() {
		return nil, nil
	}
	perTier := make(map[string][]TierObject, len(l.Tiers))
	for _, t := range l.Tiers {
		objs, err := readTierManifest(t.Store)
		if err != nil {
			return nil, err
		}
		perTier[t.Name] = objs
	}
	return mergeTierManifests(perTier)
}

// readTierManifest returns what the store says it holds, or nil when the store
// has no manifest, which means an empty tier.
func readTierManifest(store SegmentStore) ([]TierObject, error) {
	size, err := store.Size(manifestKey)
	if errors.Is(err, ErrObjectNotFound) {
		// Absent is not an error: a store with nothing offloaded has no
		// manifest. Only the store may say this, and only by saying it — any
		// other failure means we do not know what the tier holds, and "we do
		// not know" must not read as "nothing".
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "stat tier manifest")
	}
	if size <= 0 {
		// writeTierManifest always writes a JSON object, so a zero-length one
		// was not written by this package.
		return nil, errors.New("commitlog: tier manifest is empty")
	}
	body := make([]byte, size)
	if _, err := store.ReadAt(manifestKey, body, 0); err != nil {
		return nil, errors.Wrap(err, "read tier manifest")
	}
	var m tierManifest
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, errors.Wrap(err, "decode tier manifest")
	}
	if m.Version != manifestVersion {
		return nil, errors.Errorf(
			"commitlog: tier manifest is version %d, this build understands %d",
			m.Version, manifestVersion)
	}
	// A version 3 manifest names the tier of every object it describes. An entry
	// without one is not defaulted: see defaultTierName for why "" cannot be
	// allowed to mean "the only tier", and the key check below for why the whole
	// manifest is refused rather than the offending entry.
	for _, o := range m.Segments {
		if o.Tier == "" {
			return nil, errors.Errorf(
				"commitlog: tier manifest entry for base offset %d names no tier",
				o.BaseOffset)
		}
	}
	// The keys in here are the one part of the manifest that becomes an ACTION
	// rather than a description: they end up in s.storeKey and s.indexKey, and
	// segment.Delete hands those straight to store.Delete. A key naming a place
	// outside the store is therefore a delete outside the store, so it is refused
	// at the boundary rather than left to each SegmentStore implementation to
	// notice — FileSegmentStore does check now, but the interface has never
	// promised it and a store built on object storage has no reason to.
	//
	// The whole manifest is refused, not the offending entry. A manifest holding
	// a key this package could not have minted is not a manifest whose other
	// entries have been established as trustworthy, and adopting the rest would
	// bury the fact that something wrote it that should not have.
	for _, o := range m.Segments {
		if err := validStoreKey(o.LogKey); err != nil {
			return nil, errors.Wrapf(err,
				"commitlog: tier manifest segment %d names an invalid log object",
				o.BaseOffset)
		}
		// A block-compressed object without a block table is unreadable, and
		// there is deliberately no falling back to rebuilding one by walking the
		// object: that walk is the whole cost the table exists to remove, and a
		// silent fallback would turn its absence into a slow success nobody
		// notices. Refused where it arrives, like an unknown codec.
		if o.BlockMode != (o.BlocksKey != "") {
			return nil, errors.Errorf(
				"commitlog: tier manifest segment %d has BlockMode=%v and "+
					"BlocksKey=%q; a block-compressed object has a block table "+
					"and a raw one has none", o.BaseOffset, o.BlockMode, o.BlocksKey)
		}
		if o.BlocksKey != "" {
			if err := validStoreKey(o.BlocksKey); err != nil {
				return nil, errors.Wrapf(err,
					"commitlog: tier manifest segment %d names an invalid block table",
					o.BaseOffset)
			}
		}
		// An empty IndexKey is meaningful — it says the index stayed on local
		// disk — so it is the one value exempt from the check.
		if o.IndexKey == "" {
			continue
		}
		if err := validStoreKey(o.IndexKey); err != nil {
			return nil, errors.Wrapf(err,
				"commitlog: tier manifest segment %d names an invalid index object",
				o.BaseOffset)
		}
	}
	sort.Slice(m.Segments, func(i, j int) bool {
		return m.Segments[i].BaseOffset < m.Segments[j].BaseOffset
	})
	return m.Segments, nil
}

// TierManifest returns what the STORE says its tier holds, read from the store
// rather than from this log's local bookkeeping. See the interface doc.
func (l *commitLog) TierManifest() ([]TierObject, error) {
	return l.readMergedTierManifest()
}

// adoptTierManifest materialises segments this log does not have but the store's
// manifest describes, by opening them over their store objects.
//
// This is what makes a tier self-contained in practice: a process that has the
// store and an empty (or partial) log directory can open the log and reach the
// offloaded records, without being handed state by anyone.
//
// It only ADDS. A base offset the log already holds is left exactly as it is —
// the local segment wins, because it describes what this process has actually
// got, and an import is not the place to overrule that.
//
// Caller holds l.mu.
func (l *commitLog) adoptTierManifestLocked(objs []TierObject) (int, error) {
	if len(objs) == 0 {
		return 0, nil
	}
	have := make(map[int64]bool, len(l.segments))
	for _, s := range l.segments {
		have[s.BaseOffset] = true
	}

	adopted := 0
	for _, o := range objs {
		if have[o.BaseOffset] || o.LogKey == "" {
			continue
		}
		if o.IndexKey != "" && l.RemoteIndexCache == nil {
			return adopted, errors.Errorf(
				"commitlog: tier manifest segment %d has an offloaded index but no "+
					"RemoteIndexCache is configured", o.BaseOffset)
		}
		store, err := l.storeForTier(o.Tier)
		if err != nil {
			return adopted, err
		}
		meta := o.meta()
		seg, err := openOffloadedSegment(l.Path, o.BaseOffset, l.MaxSegmentBytes,
			l.Compression, store, o.Tier, meta, l.RemoteIndexCache)
		if err != nil {
			// The object is named but not there. That window is unavoidable —
			// the caller deletes superseded objects after a pass, and a crash
			// can land between the manifest that dropped them and the delete
			// that removed them — and the records are genuinely gone either
			// way, so refusing to open the whole log gains nothing. Skip it;
			// the next publish drops the entry.
			slog.Warn("commitlog: tier manifest names an unopenable object; skipping",
				slog.Int64("base_offset", o.BaseOffset),
				slog.String("key", o.LogKey),
				slog.String("err", err.Error()))
			continue
		}
		// A segment whose index was offloaded too is complete in the store. One
		// that kept its index LOCAL is not: this directory has never held that
		// index, so the segment would open with an empty one and read back as
		// though it had no records — present, described, and silently empty.
		//
		// Rebuild it from the object instead. That costs one pass over the
		// segment, which is a single request now that a sweep streams, and it is
		// what makes the tier genuinely self-contained rather than
		// self-contained only when the index happened to be offloaded as well.
		if o.IndexKey == "" {
			if err := seg.reconcileIndexTail(); err != nil {
				seg.Close()
				return adopted, errors.Wrapf(err,
					"rebuild index for manifest segment %d", o.BaseOffset)
			}
		}
		l.segments = append(l.segments, seg)
		adopted++
	}
	if adopted > 0 {
		sort.Slice(l.segments, func(i, j int) bool {
			return l.segments[i].BaseOffset < l.segments[j].BaseOffset
		})
	}
	return adopted, nil
}
