package commitlog

import (
	"sort"
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
// — is not sound in general: every upload allocates its own key, so a store can
// hold several objects claiming one base offset (a rewrite's predecessor, an
// upload orphaned by a crash before its marker) with nothing in the store
// saying which is current. Both cases are invisible to a scan.
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
