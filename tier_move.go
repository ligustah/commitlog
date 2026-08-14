package commitlog

// Moving a segment between tiers is the second hop: local disk to the nearest
// tier is offload (see OffloadBefore), and everything below it is here.
//
// The caller names the destination and commitlog moves the bytes. Deliberately
// no clock: descent between stores is a policy question about cost and
// durability that only the caller has the context for, and a log that decided
// WHEN would be deciding it without that context — the same reason it runs no
// automatic cleaner on these logs.

import (
	"github.com/pkg/errors"
)

// moveSegment moves one offloaded segment's objects from the tier it is in into
// dst, and repoints the segment at them.
//
// The order below is the whole of the safety argument, and it is the same order
// offload already follows:
//
//  1. copy the objects, which commits nothing — unreferenced bytes in dst are a
//     recognisable orphan, and the sweep collects them;
//  2. publish dst's manifest naming them, which IS the commit;
//  3. repoint the segment, so reads come from dst;
//  4. publish src's manifest without them, which releases the source.
//
// Between 2 and 4 both tiers claim the segment. That state is readable rather
// than fatal because the entry published at 2 says which tier it came out of —
// see TierObject.MovedFrom and resolveInterruptedMove. The reverse order has no
// such repair: releasing the source first leaves a segment no manifest names,
// and the objects still in dst are indistinguishable from garbage.
//
// The superseded source objects are returned for the caller to queue, not
// deleted here. A reader that took the source backing before step 3 is still
// reading it, and the pass's reclaim queue is what waits for them.
//
// Caller holds cleanMu and must not hold l.mu.
func (l *commitLog) moveSegment(s *segment, dst Tier) ([]pendingReclaim, error) {
	s.RLock()
	src := s.tier
	srcStore := s.store
	meta := s.offloadMetaLocked()
	s.RUnlock()

	if srcStore == nil {
		return nil, errors.Errorf(
			"commitlog: segment %d is not offloaded, so there is no tier to move it "+
				"out of; local bytes reach a tier through OffloadBefore", s.BaseOffset)
	}
	if src == dst.Name {
		return nil, nil
	}
	// Both ends, before anything is copied. A move writes to the destination
	// AND to the source's manifest, so a log that does not own either end
	// cannot do it — and finding that out after copying a segment's bytes would
	// have paid the whole cost of the move for nothing.
	if !l.tierWritable(src) {
		return nil, errors.Wrapf(errTierReadOnly,
			"tier %s holds segment %d and releasing it is a write", src, s.BaseOffset)
	}
	if !l.tierWritable(dst.Name) {
		return nil, errors.Wrapf(errTierReadOnly, "tier %s is the move destination", dst.Name)
	}

	// A key is minted for an object that EXISTS, and the three are decided the
	// same way. This used to mint all three unconditionally and then blank two of
	// them back when the source had none — which states the rule as its own
	// exception, twice, and left the second retraction bare while the first
	// carried the reason. A sidecar the source does not have is not copied below,
	// so a key minted for it would name an object nothing ever wrote.
	//
	// The index is the case worth naming: when it is absent from the store it is
	// on local disk and stays there, and it describes positions in bytes the copy
	// reproduces exactly — so it is still true of the destination object.
	logKey, indexKey, blocksKey := newStoreKeys(s.BaseOffset)
	moved := meta
	moved.LogKey = logKey
	if meta.IndexKey != "" {
		moved.IndexKey = indexKey
	}
	if meta.BlocksKey != "" {
		moved.BlocksKey = blocksKey
	}

	if err := copyObjectAs(srcStore, dst.Store, meta.LogKey, moved.LogKey); err != nil {
		return nil, errors.Wrapf(err, "copy segment %d into tier %s", s.BaseOffset, dst.Name)
	}
	if meta.IndexKey != "" {
		if err := copyObjectAs(srcStore, dst.Store, meta.IndexKey, moved.IndexKey); err != nil {
			return nil, errors.Wrapf(err, "copy segment %d index into tier %s", s.BaseOffset, dst.Name)
		}
	}
	if meta.BlocksKey != "" {
		if err := copyObjectAs(srcStore, dst.Store, meta.BlocksKey, moved.BlocksKey); err != nil {
			return nil, errors.Wrapf(err, "copy segment %d block table into tier %s", s.BaseOffset, dst.Name)
		}
	}

	// The commit. MovedFrom is what makes the window between here and the
	// release below survivable.
	entry := moved.tierObject(s.BaseOffset, dst.Name)
	entry.MovedFrom = src
	if err := l.writeOneTierManifest(dst.Name, entry); err != nil {
		return nil, errors.Wrapf(err, "publish tier %s manifest for segment %d", dst.Name, s.BaseOffset)
	}

	superseded, err := s.swapTier(dst.Store, dst.Name, moved)
	if err != nil {
		// The segment did not repoint, so it is still serving the source's
		// objects and the source's manifest still names them. Leaving dst's
		// manifest claiming the segment is the recoverable side: the marker
		// resolves it back to dst at open, and dst's objects are complete.
		return superseded, errors.Wrapf(err, "repoint segment %d at tier %s", s.BaseOffset, dst.Name)
	}

	// The release. Rebuilt from the log's segments, which now say dst, so the
	// source's manifest simply stops naming this segment.
	if err := l.writeOneTierManifest(src); err != nil {
		return superseded, errors.Wrapf(err, "release segment %d from tier %s", s.BaseOffset, src)
	}
	return superseded, nil
}

// swapTier repoints an offloaded segment at objects in a different store.
//
// Nothing about the RECORDS changes — a move is a byte-for-byte copy — so the
// offsets, timestamps, positions and block table this segment already holds
// stay exactly as they are. Only where its bytes are read from changes.
//
// Returns the source objects this supersedes, with the backing that was serving
// the log object pinned: a reader that took it before this call is reading the
// SOURCE, and deleting it underneath them turns a move into a read error.
func (s *segment) swapTier(store SegmentStore, tier string, meta offloadMeta) ([]pendingReclaim, error) {
	s.Lock()
	defer s.Unlock()
	if s.closed {
		return nil, ErrSegmentClosed
	}
	if s.store == nil {
		return nil, errors.New("commitlog: segment is not offloaded")
	}

	oldBacking, _ := s.backing.(*storeBacking)
	// Note whose tier these name: s.tier, the tier the segment is leaving. The
	// caller has the destination, and the objects being reclaimed are the ones
	// left behind in the source. See supersededObjectsLocked.
	superseded := s.supersededObjectsLocked(oldBacking)

	sb := newStoreBackingSize(store, meta.LogKey, meta.PhysPosition)
	// Past here the segment IS the destination's objects, so anything still
	// able to serve the source has to be cleared.
	if old, ok := s.backing.(*storeBacking); ok {
		old.Invalidate()
	}
	if s.indexKey != "" && s.indexCache != nil {
		s.indexCache.Invalidate(s.indexKey)
	}
	s.backing = sb
	s.store = store
	s.tier = tier
	s.storeKey = meta.LogKey
	s.indexKey = meta.IndexKey
	s.blocksKey = meta.BlocksKey
	return superseded, nil
}

// movePlaced moves every segment the spec placed in a tier other than the one
// it is in, and returns the source objects to reclaim.
//
// A placement naming a tier that is not configured is an error and nothing
// moves, because a spec whose intent cannot be honoured must fail loudly rather
// than be partially applied — the same rule CleanSpec.Ceiling follows.
//
// A placement naming a base offset this log has no OFFLOADED segment for is
// skipped, and that asymmetry is deliberate. A tier name is a caller's
// configuration and a typo in it is always wrong. A base offset is a fact about
// the log at the moment the caller looked, and retention deletes segments
// between that look and this pass — erroring would make every caller's map go
// stale the instant it was built. A segment that is still local is skipped for
// the same reason it is not an error: local bytes reach the nearest tier
// through LocalRetentionAge, and placement governs the hops below that.
func (l *commitLog) movePlaced(placement map[int64]string) ([]pendingReclaim, error) {
	if len(placement) == 0 {
		return nil, nil
	}
	dests := make(map[int64]Tier, len(placement))
	for base, name := range placement {
		t, err := l.tierByName(name)
		if err != nil {
			return nil, errors.Errorf(
				"commitlog: CleanSpec.TierPlacement puts segment %d in tier %q, "+
					"which is not in Options.Tiers", base, name)
		}
		dests[base] = t
	}

	l.mu.RLock()
	segments := make([]*segment, len(l.segments))
	copy(segments, l.segments)
	l.mu.RUnlock()

	var superseded []pendingReclaim
	for _, s := range segments {
		dst, ok := dests[s.BaseOffset]
		if !ok {
			continue
		}
		s.RLock()
		offloaded, already := s.isOffloaded(), s.tier == dst.Name
		s.RUnlock()
		if !offloaded || already {
			continue
		}
		objs, err := l.moveSegment(s, dst)
		superseded = append(superseded, objs...)
		if err != nil {
			return superseded, err
		}
	}
	return superseded, nil
}
