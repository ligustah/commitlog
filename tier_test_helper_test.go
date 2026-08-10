package commitlog

// oneTier is the chain a single-store test log has: the conventional default
// name over the store it was given.
//
// A helper rather than a literal at every fixture because the NAME is not what
// those tests are about — they are about a log that has somewhere to offload
// to. The tests that are about the name construct Tier directly, which is also
// what keeps this from hiding a change in the type.
func oneTier(store SegmentStore) []Tier {
	return []Tier{{Name: defaultTierName, Store: store}}
}

// orphanKeys drops the tier from a sweep result.
//
// For assertions that predate tiers having names and are about WHICH OBJECTS
// are garbage, not which store holds them. Kept as an explicit unwrap rather
// than left to a Contains over a []StoreObject: a require.NotContains of a
// string against a slice of structs passes unconditionally, so the conversion
// had to be visible at every one of these call sites or they would all have
// gone quiet at once.
func orphanKeys(objs []StoreObject) []string {
	out := make([]string, 0, len(objs))
	for _, o := range objs {
		out = append(out, o.Key)
	}
	return out
}

// oneTierReadOnly is oneTier with this log's ownership of it set. Ownership is
// per tier now, so a test that wants a follower says so on the tier rather
// than on the log.
func oneTierReadOnly(store SegmentStore, readOnly bool) []Tier {
	tiers := oneTier(store)
	tiers[0].ReadOnly = readOnly
	return tiers
}
