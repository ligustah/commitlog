package commitlog

import (
	"sort"

	"github.com/pkg/errors"
)

// TierObject describes where one segment's bytes live in a SegmentStore, and
// everything needed to place that segment without reading the object.
//
// It is the log's tier bookkeeping in transferable form. That bookkeeping is
// kept LOCALLY (an .offloaded marker per segment), which is fine while one
// process owns a store and fatal when ownership moves: the new owner holds no
// markers for anything its predecessor uploaded, so it cannot read those
// objects through the log, cannot avoid uploading a second copy of the same
// bytes, and cannot ever reclaim them.
//
// Handing it over explicitly is what makes a change of owner work. The
// alternative — the new owner scanning the store and inferring what is current
// — is not sound in general: a generation is derived from the writer's own
// local marker, so a store that has been through a bad handover can hold two
// objects for one base offset with nothing to order them, and a crash between
// an upload and its marker leaves an object that was never committed. Both are
// invisible to a scan.
//
// So the state travels with the ownership decision, from whatever makes that
// decision. It is the same division as CleanSpec's Ceiling: the log honours a
// decision it cannot itself make.
type TierObject struct {
	// BaseOffset identifies the segment. It is the key the caller matches on.
	BaseOffset int64
	// LogKey is the store object holding the segment's log bytes.
	LogKey string
	// IndexKey is the store object holding its index, empty when the index is
	// kept on local disk.
	IndexKey string
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

func tierObjectFromMeta(baseOffset int64, m offloadMeta) TierObject {
	return TierObject{
		BaseOffset:     baseOffset,
		LogKey:         m.LogKey,
		IndexKey:       m.IndexKey,
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
		FirstOffset:    o.FirstOffset,
		LastOffset:     o.LastOffset,
		FirstWriteTime: o.FirstWriteTime,
		LastWriteTime:  o.LastWriteTime,
		Position:       o.Position,
		PhysPosition:   o.PhysPosition,
		BlockMode:      o.BlockMode,
	}
}

// ExportTierState returns this log's tier bookkeeping. See the interface doc.
func (l *commitLog) ExportTierState() ([]TierObject, error) {
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

// ImportTierState installs tier bookkeeping the caller says is current. See the
// interface doc.
func (l *commitLog) ImportTierState(objs []TierObject) (int, error) {
	if len(objs) == 0 {
		return 0, nil
	}
	if l.SegmentStore == nil {
		return 0, errors.New("commitlog: cannot import tier state without a SegmentStore")
	}
	// Importing writes markers and drops local bytes, but touches no object, so
	// a read-only tier can still import: that is exactly what a process does
	// when it is told what a store holds without being given the right to
	// change it.

	// Validated in full BEFORE anything is applied. Import mutates the read
	// path and drops local bytes, so a batch that is half-applied and then
	// rejected would leave the log in a state the caller never described and
	// cannot name in order to correct.
	sorted := make([]TierObject, len(objs))
	copy(sorted, objs)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].BaseOffset < sorted[j].BaseOffset
	})
	seen := make(map[int64]bool, len(sorted))
	for _, o := range sorted {
		switch {
		case o.LogKey == "":
			return 0, errors.Errorf("commitlog: tier state for %d has no log key", o.BaseOffset)
		case seen[o.BaseOffset]:
			return 0, errors.Errorf(
				"commitlog: tier state names segment %d twice; only one object can be current",
				o.BaseOffset)
		// An empty segment is (-1, -1). Anything else must be a real range, and
		// a "last" below a "first" is checked whichever side is negative —
		// leaving a hole for a negative last would admit (5, -1), which routes
		// reads to a segment that claims to hold nothing and start at 5.
		case o.LastOffset < 0 && o.FirstOffset >= 0,
			o.LastOffset >= 0 && o.FirstOffset > o.LastOffset:
			return 0, errors.Errorf(
				"commitlog: tier state for %d has first offset %d above last offset %d",
				o.BaseOffset, o.FirstOffset, o.LastOffset)
		case o.IndexKey != "" && l.RemoteIndexCache == nil:
			return 0, errors.Errorf(
				"commitlog: tier state for %d has an offloaded index but no RemoteIndexCache is configured",
				o.BaseOffset)
		}
		seen[o.BaseOffset] = true

		// The objects must actually be there. A marker naming a missing object
		// is not discovered until a read reaches for it, which turns a bad
		// import into a failure somewhere else entirely.
		size, err := l.SegmentStore.Size(o.LogKey)
		if err != nil {
			return 0, errors.Wrapf(err, "tier state for %d names a missing object %s",
				o.BaseOffset, o.LogKey)
		}
		// And they must be the size the state claims, because that size is what
		// bounds every read of the object — the store is asked for bytes, not
		// for records. Importing drops the local copy, so a state that
		// understates the object hides its tail with nothing left to compare
		// against, and one that overstates it reads past the end. The store had
		// to be asked for the size anyway to know the object exists, so this
		// costs nothing beyond using the answer.
		if o.PhysPosition != size {
			return 0, errors.Errorf(
				"commitlog: tier state for %d claims object %s is %d bytes but it is %d",
				o.BaseOffset, o.LogKey, o.PhysPosition, size)
		}
		if o.IndexKey != "" {
			if _, err := l.SegmentStore.Size(o.IndexKey); err != nil {
				return 0, errors.Wrapf(err, "tier state for %d names a missing index object %s",
					o.BaseOffset, o.IndexKey)
			}
		}
	}

	l.cleanMu.Lock()
	defer l.cleanMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()

	byBase := make(map[int64]*segment, len(l.segments))
	for _, s := range l.segments {
		byBase[s.BaseOffset] = s
	}
	// The active segment is still being appended to, so it has no business
	// being replaced by an object.
	var active *segment
	if len(l.segments) > 0 {
		active = l.segments[len(l.segments)-1]
	}

	applied := 0
	for _, o := range sorted {
		seg, ok := byBase[o.BaseOffset]
		if !ok {
			// Nothing local to attach to. Creating a segment out of the object
			// would extend the log's offset range with records it has never
			// held, and the segment list must stay contiguous for readers, so
			// this is refused rather than guessed at.
			return applied, errors.Errorf(
				"commitlog: tier state names segment %d, which this log does not have",
				o.BaseOffset)
		}
		if seg == active {
			return applied, errors.Errorf(
				"commitlog: tier state names segment %d, which is the active segment",
				o.BaseOffset)
		}

		seg.Lock()
		switch {
		case seg.store != nil && seg.storeKey == o.LogKey:
			// Already pointing at exactly this object.
			seg.Unlock()
			continue
		case seg.store != nil:
			// Pointing at a different object: the caller's state wins. The old
			// keys join this log's lineage, so they can be reclaimed later —
			// this log is the one that stopped referencing them.
			// The objects it stops referencing become garbage, findable through
			// UnreferencedObjects — nothing else knows they were dropped.
			if err := seg.replaceOffloadedTargetLocked(o.meta(), l.RemoteIndexCache); err != nil {
				seg.Unlock()
				return applied, errors.Wrapf(err, "repoint segment %d", o.BaseOffset)
			}
		default:
			// A local segment whose bytes are already in the store. Its records
			// must be the ones the object holds; otherwise dropping the local
			// copy would swap a reader's data for something else.
			if seg.lastOffset != o.LastOffset || seg.firstOffset != o.FirstOffset {
				seg.Unlock()
				return applied, errors.Errorf(
					"commitlog: tier state for %d covers offsets %d-%d but the local "+
						"segment holds %d-%d",
					o.BaseOffset, o.FirstOffset, o.LastOffset,
					seg.firstOffset, seg.lastOffset)
			}
			if err := seg.attachOffloadedLocked(l.SegmentStore, o.meta(), l.RemoteIndexCache); err != nil {
				seg.Unlock()
				return applied, errors.Wrapf(err, "adopt segment %d", o.BaseOffset)
			}
		}
		seg.Unlock()
		applied++
	}

	return applied, nil
}
