package commitlog

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"sort"
	"strings"

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
// Version 3 adds Tier, naming the store an object lives in. It went in ahead of
// multi-store tiering (docs/multi-store-tiering.md) so the manifest a store was
// already carrying could describe itself once the second tier existed, rather
// than needing a second version bump at the moment it mattered — and it now
// carries that behaviour: an entry names the tier its object is actually in, and
// publishTierManifests files entries by that name. Refused rather than adapted,
// for the same reason as version 1.
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
	return l.publishTierManifests(l.Tiers, nil, pending...)
}

// commitJoinedRun is the commit point of a tiered join: ONE manifest write that
// starts naming the joined result and stops naming every input it replaced.
//
// A join is the only operation whose publish changes the SET rather than one
// entry — it retires N base offsets and adds one — and it has to land as a
// single write, because a manifest that named some of the inputs and the result
// too would claim the same records twice, and one that named neither would lose
// them. It is one write for free: publishTierManifests rebuilds a whole manifest
// body and Puts it per tier, and a join's run never spans tiers.
//
// The two halves of that set reach the manifest differently, which is worth
// stating because it looks asymmetric. The result goes in as a PENDING entry, in
// the ordinary way and for the ordinary reason: its objects are uploaded but the
// segment has not switched to them yet. Its base offset is the run's lowest —
// the first input's — so it REPLACES that input rather than adding to it. The
// remaining inputs cannot be expressed that way, since a pending entry names an
// object and "stop naming this one" is not an object, so they are retired by
// base offset instead.
//
// Retiring rather than mutating the segments first, which is the route a move
// takes: swapTier may repoint before the commit because it repoints at objects
// that are real and complete, and a join has nowhere to repoint an input to —
// it is about to stop existing. Clearing their tier fields ahead of the write
// would leave a failed publish holding segments the log still serves but no
// longer believes are offloaded, and something would have to roll that back.
// Everything here is pre-commit or post-commit, and nothing in between.
func (l *commitLog) commitJoinedRun(retired []int64, joined TierObject) error {
	return l.publishTierManifests(l.Tiers, retired, joined)
}

// writeOneTierManifest publishes ONE tier's manifest and leaves every other
// tier's alone.
//
// The mover needs it and nothing else does. A move commits by publishing the
// destination's manifest and releases by publishing the source's, in that
// order — and writing both at once would make that order an accident of how
// the caller listed Options.Tiers, which is exactly the kind of dependence the
// merge at open refuses to have.
//
// A name this log has no tier for is refused. This is the call that RELEASES a
// source after a move has committed, so a name that quietly matched nothing
// would publish no manifest, report success, and leave both tiers claiming the
// segment for good — the state MovedFrom exists to make survivable, made
// permanent instead.
func (l *commitLog) writeOneTierManifest(tier string, pending ...TierObject) error {
	if !l.hasTier() {
		return nil
	}
	t, err := l.tierByName(tier)
	if err != nil {
		return err
	}
	return l.publishTierManifests([]Tier{t}, nil, pending...)
}

// publishTierManifests rebuilds and publishes the manifests of the given tiers,
// skipping any this log does not own.
//
// retiring names base offsets this write stops describing, applied after the
// pending overrides. It is the partner of pending and describes the same
// instant — the moment of a commit, when the log's own view and the tier's
// necessarily disagree — from the other side: pending is an object that is real
// before its segment says so, retiring is a segment whose object stops being the
// tier's before the segment says so. Only commitJoinedRun passes one; see there
// for why a join cannot express it as an override.
func (l *commitLog) publishTierManifests(tiers []Tier, retiring []int64, pending ...TierObject) error {
	if !l.hasTier() {
		return nil
	}
	objs, err := l.tierState()
	if err != nil {
		return err
	}
	// Refused rather than resolved either way. A base offset in both sets is a
	// caller who has confused which of a join's inputs SURVIVES — the result
	// keeps the run's lowest base offset, and that input is the one not retired
	// — and both readings of the overlap publish a manifest that is wrong: drop
	// the pending entry and the result is unnamed, keep it and an input the
	// caller believes is gone is named by the result's own objects.
	if len(retiring) > 0 && len(pending) > 0 {
		for _, p := range pending {
			for _, base := range retiring {
				if p.BaseOffset == base {
					return errors.Errorf(
						"commitlog: tier manifest publish both adds and retires base "+
							"offset %d", base)
				}
			}
		}
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
		// Whatever the walk above did not consume names a base offset the tier
		// state does not hold yet, so it is an addition. Taken from the map rather
		// than by walking `pending` again and asking the map whether each entry
		// survived: that phrasing needed a second delete to keep two pending
		// entries with one base offset from being appended twice, and stated the
		// deduplication as a side effect of a lookup. The map already holds one
		// entry per base offset. Its iteration order does not reach the output —
		// the sort below is total over these, since every remaining base offset is
		// distinct.
		for _, p := range override {
			objs = append(objs, p)
		}
		sort.Slice(objs, func(i, j int) bool { return objs[i].BaseOffset < objs[j].BaseOffset })
	}
	// Applied after the overrides, and to the rebuilt list rather than to a
	// tier's slice: base offsets are unique across a log, so a retiring entry
	// needs no tier of its own to be unambiguous.
	if len(retiring) > 0 {
		gone := make(map[int64]bool, len(retiring))
		for _, base := range retiring {
			gone[base] = true
		}
		kept := objs[:0]
		for _, o := range objs {
			if !gone[o.BaseOffset] {
				kept = append(kept, o)
			}
		}
		objs = kept
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
	for _, t := range tiers {
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
// attached to the same log. Picking either would serve one tier's bytes and
// silently orphan the other's, and picking by order would make the answer
// depend on the caller's configuration rather than on the stores. So it is
// refused, and the refusal names both tiers.
//
// The ONE exception is an interrupted move, and it is not an exception to the
// principle: the destination's entry says which tier it was moved out of, so
// the answer still comes from what the stores say rather than from how the
// caller listed them. See TierObject.MovedFrom for why the window exists at
// all. Anything else — two claims with no marker, a marker naming a tier that
// is not the other claimant, three tiers claiming one segment — is refused as
// before.
//
// This is the cost of per-tier manifests, paid deliberately: option (a) in
// docs/multi-store-tiering.md made the disagreement unrepresentable, at the
// price of a tier that cannot describe itself.
func mergeTierManifests(perTier map[string][]TierObject) ([]TierObject, error) {
	// Every claim first, then resolve: a decision made as the claims arrive
	// would depend on map iteration order, which is the very thing this
	// function must not depend on.
	claims := make(map[int64][]tierClaim)
	for tier, objs := range perTier {
		for _, o := range objs {
			claims[o.BaseOffset] = append(claims[o.BaseOffset], tierClaim{tier: tier, obj: o})
		}
	}
	out := make([]TierObject, 0, len(claims))
	for base, cs := range claims {
		if len(cs) == 1 {
			out = append(out, cs[0].obj)
			continue
		}
		won, err := resolveInterruptedMove(base, cs)
		if err != nil {
			return nil, err
		}
		out = append(out, won)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BaseOffset < out[j].BaseOffset })
	return out, nil
}

// resolveInterruptedMove picks the winner among two tiers claiming one segment,
// and only when the claims themselves explain the disagreement.
//
// The destination of a move carries MovedFrom naming the source, so a pair
// where exactly one claim names the other tier is a move that committed and did
// not get to release. Both stores hold complete objects at that moment, so
// either would serve the records — what matters is that every process picks the
// SAME one, and that the choice comes from the stores.
//
// Deliberately narrow. Two claims with no marker is the fault this refusal was
// written for. Both claiming to have moved from the other is not a state a move
// can produce, so it is a fault too. More than two claims likewise: a move has
// one source and one destination.
func resolveInterruptedMove(base int64, claims []tierClaim) (TierObject, error) {
	refuse := func() (TierObject, error) {
		names := make([]string, 0, len(claims))
		for _, c := range claims {
			names = append(names, c.tier)
		}
		// Sorted, because map iteration decides which claim was seen first and
		// an error message that changes between runs is one nobody can match on.
		sort.Strings(names)
		return TierObject{}, errors.Errorf(
			"commitlog: tiers %s both claim segment %d; "+
				"one log's segments are in two stores",
			strings.Join(names, " and "), base)
	}
	if len(claims) != 2 {
		return refuse()
	}
	a, b := claims[0], claims[1]
	switch {
	case a.obj.MovedFrom == b.tier && b.obj.MovedFrom != a.tier:
		return a.obj, nil
	case b.obj.MovedFrom == a.tier && a.obj.MovedFrom != b.tier:
		return b.obj, nil
	default:
		return refuse()
	}
}

// tierClaim is one tier's manifest saying it holds a segment. The tier is the
// store the entry was READ from rather than the Tier the entry records: a
// manifest copied between stores (CopyTier) names the tier it was written in,
// and where an object actually is has to come from where it was found.
type tierClaim struct {
	tier string
	obj  TierObject
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

// adoptTierManifestLocked materialises segments this log does not have but the store's
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
//
// It builds a NEW segment array rather than appending to and sorting the live
// one, which is the rule segmentsSnapshot() states for everyone who changes the
// set: readers index a snapshot without holding l.mu, so an element written in
// place is a data race against every one of them. sort.Slice swaps elements in
// place — this is the one caller that was breaking that rule, and it was safe
// only because it happens to run inside open(), before there is a log to hold a
// reader. That is a fact about the schedule, not about the function, and the
// first maintenance path to adopt a manifest published by another process would
// have taken it away silently.
func (l *commitLog) adoptTierManifestLocked(objs []TierObject) (adopted int, err error) {
	if len(objs) == 0 {
		return 0, nil
	}
	have := make(map[int64]bool, len(l.segments))
	for _, s := range l.segments {
		have[s.BaseOffset] = true
	}

	next := make([]*segment, len(l.segments), len(l.segments)+len(objs))
	copy(next, l.segments)
	// Published on every exit, error paths included, because that is what the
	// direct appends this replaces did: a segment opened before the failure is
	// already holding a file handle and a mapping, and the only thing that
	// releases them is the log closing the segments it knows about.
	defer func() {
		if adopted > 0 {
			sort.Slice(next, func(i, j int) bool {
				return next[i].BaseOffset < next[j].BaseOffset
			})
			l.segments = next
		}
	}()
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
		// that kept its index LOCAL is not, when this directory has never held
		// that index: the segment would open with an empty one and read back as
		// though it had no records — present, described, and silently empty.
		//
		// Rebuild it from the object. That costs one pass over the segment, which
		// is a single request now that a sweep streams, and it is what makes the
		// tier genuinely self-contained rather than self-contained only when the
		// index happened to be offloaded as well.
		//
		// Unconditional on purpose, and it is not the cost it looks like. The
		// walk starts at the last indexed frame's end and runs while that is
		// below the segment's size, so an index that already describes the object
		// — which is every segment of an ordinary reopen, since offloading
		// removes the local .log and keeps the .index — executes the loop body
		// zero times and reads nothing. Guarding it on "does the local index
		// match the manifest" was tried and reverted: it bought no I/O, and it
		// added a second, weaker answer to a question this walk already answers
		// exactly.
		if o.IndexKey == "" {
			// No floor: an offloaded segment is sealed and its backing is
			// remote, so there is no torn tail to discard here — the walk only
			// rebuilds the index from an object written in one shot.
			if err := seg.reconcileIndexTail(-1); err != nil {
				seg.Close()
				return adopted, errors.Wrapf(err,
					"rebuild index for manifest segment %d", o.BaseOffset)
			}
		}
		next = append(next, seg)
		adopted++
	}
	return adopted, nil
}
