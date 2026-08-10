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

// A move that committed and did not get to release is resolved, not refused.
//
// The publish order is destination-then-source, because the reverse leaves a
// segment named by nothing on a crash. That order means both tiers claim the
// segment between the two Puts — the exact state above — and a crash there
// would otherwise produce a log that will not open, from a routine background
// move. The destination says which tier it came out of, so the source's claim
// is known to be the stale one.
func TestAnInterruptedMoveResolvesToTheDestination(t *testing.T) {
	merged, err := mergeTierManifests(map[string][]TierObject{
		"hot":  {{BaseOffset: 100, Tier: "hot", LogKey: "old"}},
		"cold": {{BaseOffset: 100, Tier: "cold", LogKey: "new", MovedFrom: "hot"}},
	})
	require.NoError(t, err, "a move that crashed after its commit must still open")
	require.Len(t, merged, 1)
	require.Equal(t, "cold", merged[0].Tier)
	require.Equal(t, "new", merged[0].LogKey,
		"the destination's objects are the committed ones")
}

// The resolution is narrow on purpose: it reads what the stores say about a
// move, and says nothing about any other disagreement.
func TestOnlyAMoveResolvesADoubleClaim(t *testing.T) {
	t.Run("a marker naming a tier that is not the other claimant", func(t *testing.T) {
		_, err := mergeTierManifests(map[string][]TierObject{
			"hot":  {{BaseOffset: 100, Tier: "hot", LogKey: "a"}},
			"cold": {{BaseOffset: 100, Tier: "cold", LogKey: "b", MovedFrom: "archive"}},
		})
		require.Error(t, err, "a move out of a third tier does not explain these two claims")
		require.Contains(t, err.Error(), "cold and hot")
	})

	t.Run("both claiming to have moved from the other", func(t *testing.T) {
		_, err := mergeTierManifests(map[string][]TierObject{
			"hot":  {{BaseOffset: 100, Tier: "hot", LogKey: "a", MovedFrom: "cold"}},
			"cold": {{BaseOffset: 100, Tier: "cold", LogKey: "b", MovedFrom: "hot"}},
		})
		require.Error(t, err, "no move produces two destinations")
	})

	t.Run("three tiers claiming one segment", func(t *testing.T) {
		_, err := mergeTierManifests(map[string][]TierObject{
			"hot":     {{BaseOffset: 100, Tier: "hot", LogKey: "a"}},
			"cold":    {{BaseOffset: 100, Tier: "cold", LogKey: "b", MovedFrom: "hot"}},
			"archive": {{BaseOffset: 100, Tier: "archive", LogKey: "c"}},
		})
		require.Error(t, err, "a move has one source and one destination")
		require.Contains(t, err.Error(), "archive and cold and hot")
	})

	t.Run("a marker naming the tier it is already in", func(t *testing.T) {
		_, err := mergeTierManifests(map[string][]TierObject{
			"hot":  {{BaseOffset: 100, Tier: "hot", LogKey: "a", MovedFrom: "hot"}},
			"cold": {{BaseOffset: 100, Tier: "cold", LogKey: "b"}},
		})
		require.Error(t, err, "a tier cannot be the source of a move into itself")
	})
}
