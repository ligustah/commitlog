package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Each tier describes only its own objects, and open unions them.
//
// The union is what makes a tier individually portable: a node handed the
// archive alone finds a manifest describing exactly what the archive holds,
// rather than a map that lives in a store it was not given.
func TestTheTiersMergeIntoOneView(t *testing.T) {
	merged, err := mergeTierManifests(map[string][]TierObject{
		"hot": {
			{BaseOffset: 200, Tier: "hot", LogKey: "b"},
			{BaseOffset: 300, Tier: "hot", LogKey: "c"},
		},
		"cold": {
			{BaseOffset: 0, Tier: "cold", LogKey: "a"},
		},
	})
	require.NoError(t, err)
	require.Len(t, merged, 3)

	// Ordered by base offset across tiers, not grouped by tier: everything
	// downstream treats this as the log's segment list.
	require.Equal(t, []int64{0, 200, 300},
		[]int64{merged[0].BaseOffset, merged[1].BaseOffset, merged[2].BaseOffset})
	require.Equal(t, "cold", merged[0].Tier)
	require.Equal(t, "hot", merged[1].Tier)
}

// Two tiers claiming one segment is refused, not resolved.
//
// This is the price of per-tier manifests, and it is paid on purpose: one
// manifest naming every object made the disagreement unrepresentable, at the
// cost of a tier that cannot describe itself. Here it is representable, so it
// has to be checked.
//
// Picking a winner would serve one tier's bytes and silently orphan the
// other's. Picking by configuration order would make the answer depend on how
// the caller happened to list its stores rather than on what the stores say —
// so the same two stores would resolve differently in two processes, which is
// worse than not opening at all.
func TestTwoTiersClaimingOneSegmentIsRefused(t *testing.T) {
	_, err := mergeTierManifests(map[string][]TierObject{
		"hot":  {{BaseOffset: 100, Tier: "hot", LogKey: "a"}},
		"cold": {{BaseOffset: 100, Tier: "cold", LogKey: "b"}},
	})
	require.Error(t, err, "one segment in two tiers must not be resolved by picking one")
	require.Contains(t, err.Error(), "100")
	// Both tiers named, and in a stable order — map iteration decides which is
	// found second, and an error message that changes between runs is one
	// nobody can match on.
	require.Contains(t, err.Error(), "cold and hot")
}

// A log with one tier merges to exactly that tier's manifest. The single-tier
// path is the one every caller is on today, so the merge must be a no-op for
// it rather than a new thing that can go wrong.
func TestOneTierMergesToItself(t *testing.T) {
	l, _, _ := tieredLog(t)

	merged, err := l.TierManifest()
	require.NoError(t, err)
	require.NotEmpty(t, merged)

	state, err := l.tierState()
	require.NoError(t, err)
	require.Equal(t, state, merged)
}
