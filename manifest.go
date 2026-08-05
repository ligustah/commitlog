package commitlog

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
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
// Only ONE version has ever existed, so "not this one" and "newer than this
// one" describe the same set of files today — but they do not describe the same
// set of BUGS. A `>` comparison also accepts version 0, which is what an absent
// field decodes to, so any JSON object that happened to parse was read as a
// manifest. That is the whole integrity check on this file.
const manifestVersion = 1

// tierManifest is the store's own description of itself: which object holds
// which segment, and the offset and time ranges each covers.
//
// It exists because a tier that holds bytes it cannot describe is not
// self-contained. Without it the mapping from offset range to object lives only
// in local marker files beside the log, so the objects are readable but
// uninterpretable on their own — a process that has the store and not the
// directory cannot say what it is looking at, and the bookkeeping has to be
// carried out of band by whoever has consensus. That is commitlog's own segment
// index, and it belongs with the segments.
//
// The manifest is written AFTER the objects it names, so it is the commit point
// for the tier exactly as the local marker is for the log directory: an object
// that no manifest names was never committed, which makes a crash between an
// upload and its manifest a recognisable orphan rather than an ambiguity.
type tierManifest struct {
	Version  int          `json:"version"`
	Segments []TierObject `json:"segments"`
}

// writeTierManifest publishes the current set of offloaded segments.
//
// It rebuilds from the log's segments rather than patching an existing
// manifest. The set is small (one entry per offloaded segment), and a rebuild
// cannot drift: a patch has to be right about what changed, and every path that
// changes the tier — offload, rewrite, retention — would have to agree.
//
// Caller must not hold l.mu.
func (l *commitLog) writeTierManifest() error {
	if l.SegmentStore == nil || !l.tierWritable() {
		return nil
	}
	objs, err := l.tierState()
	if err != nil {
		return err
	}
	body, err := json.Marshal(tierManifest{Version: manifestVersion, Segments: objs})
	if err != nil {
		return errors.Wrap(err, "encode tier manifest")
	}
	if err := l.SegmentStore.Put(manifestKey, bytes.NewReader(body), int64(len(body))); err != nil {
		return errors.Wrap(err, "put tier manifest")
	}
	return nil
}

// readTierManifest returns what the store says it holds, or nil when the store
// has no manifest, which means an empty tier.
func readTierManifest(store SegmentStore) ([]TierObject, error) {
	size, err := store.Size(manifestKey)
	if err != nil {
		// Absent is not an error: a store with nothing offloaded has no
		// manifest.
		return nil, nil
	}
	if size <= 0 {
		return nil, nil
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
	if l.SegmentStore == nil {
		return nil, nil
	}
	return readTierManifest(l.SegmentStore)
}

// adoptTierManifest materialises segments this log does not have but the store's
// manifest describes, by writing their offload markers and opening them.
//
// This is what makes a tier self-contained in practice: a process that has the
// store and an empty (or partial) log directory can open the log and reach the
// offloaded records, without being handed state by anyone.
//
// It only ADDS. A base offset the log already holds is left exactly as it is —
// local bytes and local markers win, because they describe what this process
// has actually got, and an import is not the place to overrule that.
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
		meta := o.meta()
		body, err := json.Marshal(meta)
		if err != nil {
			return adopted, errors.Wrap(err, "encode offload marker")
		}
		seg, err := openOffloadedSegment(l.Path, o.BaseOffset, l.MaxSegmentBytes,
			l.Compression, l.SegmentStore, meta, l.RemoteIndexCache)
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
		// Written only once the segment opens and its index is usable, so a
		// store object that cannot actually be read does not leave a marker
		// behind claiming it can.
		markerPath := seg.offloadMarkerPath()
		if err := os.WriteFile(markerPath, body, 0o644); err != nil {
			seg.Close()
			return adopted, errors.Wrap(err, "write offload marker")
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
