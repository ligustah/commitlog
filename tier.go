package commitlog

import (
	"time"

	"github.com/pkg/errors"
)

// Tier names one store in a log's chain.
//
// A log's segments descend from local disk into the chain, and every object a
// log publishes records the tier holding it (TierObject.Tier). That name is how
// a manifest entry resolves back to a store, which is why a tier has an
// identity at all rather than just a position.
//
// Matched by NAME rather than by position because a caller that edits its chain
// renumbers every position after the edit, and a renumber must not silently
// redirect bytes into a store the caller did not mean.
type Tier struct {
	// Name is the tier's stable identity. It is recorded in every object this
	// log publishes and must not be empty: "" is what an absent field decodes
	// to, so a tier named "" cannot be told apart from one that was never
	// named. See defaultTierName.
	Name string
	// Store holds this tier's objects. The caller scopes it per log (a
	// directory or object-store prefix per stream), since object keys are
	// derived from the bare base offset.
	Store SegmentStore

	// Max* bound the segments whose bytes are in THIS tier, separately from
	// local disk and from every other tier. Retention is per tier because
	// descent is: a segment over one tier's budget has left that tier rather
	// than been deleted, and the record is gone only when the last tier in the
	// chain runs out of room for it.
	//
	// Zero keeps everything in this tier. A log with no tiers has no offloaded
	// segments, so none of this ever applies to it.
	MaxBytes    int64
	MaxMessages int64
	MaxAge      time.Duration

	// ReadOnly opens this tier without the right to write to it: no offload
	// into it, no rewrite of a segment it holds, no manifest, no descriptor,
	// no delete.
	//
	// Per tier because ownership is. A node can own the tier it writes and not
	// the archive below it, and one flag for the whole chain would make it
	// choose between offloading nothing and claiming a store it does not own.
	// Use SetTierReadOnly when ownership moves.
	ReadOnly bool
}

// validateTiers checks a chain.
//
// It used to refuse more than one tier outright, because until a segment could
// be PLACED, a caller who configured an archive below its hot tier got one that
// silently never received anything — told its data was somewhere it is not.
// CleanSpec.TierPlacement is what removed that, so the refusal went with it.
func validateTiers(tiers []Tier) error {
	seen := make(map[string]bool, len(tiers))
	for _, t := range tiers {
		if t.Name == "" {
			return errors.New("commitlog: a tier in Options.Tiers has no Name")
		}
		// Names are how everything resolves a tier — a manifest entry, a
		// placement, a reclaim, a handover. Two tiers sharing one would make
		// every one of those answer for whichever was listed first, so the
		// caller's second store would be unreachable in exactly the way the
		// old length-1 refusal existed to prevent.
		if seen[t.Name] {
			return errors.Errorf(
				"commitlog: Options.Tiers names tier %q twice; a tier's name is how "+
					"an object, a placement and a handover find its store", t.Name)
		}
		seen[t.Name] = true
		if t.Store == nil {
			return errors.Errorf("commitlog: tier %q in Options.Tiers has no Store", t.Name)
		}
		// Negatives, for the reason every other option refuses them: zero is
		// the unset value, so a negative is not a smaller budget, it is a
		// caller who computed one and got it wrong.
		if t.MaxBytes < 0 || t.MaxMessages < 0 || t.MaxAge < 0 {
			return errors.Errorf(
				"commitlog: tier %q has a negative retention limit; zero means "+
					"no limit", t.Name)
		}
	}
	return nil
}

// hasTier reports whether this log has anywhere to offload to. It replaces the
// nil check on the single store field that preceded it, and reads the same way
// at every call site: a log with no tier does not offload, does not rewrite a
// tiered segment, and publishes no manifest.
func (l *commitLog) hasTier() bool {
	return len(l.Tiers) > 0
}

// primaryTier is the tier a segment leaving local disk descends into: the first
// in the chain. Named rather than indexed at the call sites because with a
// chain longer than one this is a decision, not a lookup, and step 3 gives it
// one to make.
func (l *commitLog) primaryTier() (Tier, bool) {
	if len(l.Tiers) == 0 {
		return Tier{}, false
	}
	return l.Tiers[0], true
}

// primaryStore is primaryTier's store, or nil. For the call sites that only
// ever wanted somewhere to write.
func (l *commitLog) primaryStore() SegmentStore {
	if t, ok := l.primaryTier(); ok {
		return t.Store
	}
	return nil
}

// storeForTier resolves the tier an object names to the store holding it.
//
// An unknown name is an ERROR, not a fallback to the primary tier. A manifest
// that names a tier this process has not been given describes objects it cannot
// reach, and answering with the nearest store would read one tier's bytes under
// another tier's keys — the failure the name exists to prevent.
func (l *commitLog) storeForTier(name string) (SegmentStore, error) {
	for _, t := range l.Tiers {
		if t.Name == name {
			return t.Store, nil
		}
	}
	return nil, errors.Errorf(
		"commitlog: an object names tier %q, which is not in Options.Tiers", name)
}
