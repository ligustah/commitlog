package commitlog

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// Every object a manifest describes names the tier holding it.
//
// Step 1 of docs/multi-store-tiering.md. It carries no behaviour yet — one store
// is configurable, so the answer is always defaultTierName — and it goes in
// ahead of the capability so that a store already carrying a manifest can
// describe itself once a second tier exists, rather than needing a second
// version bump at the moment that starts to matter.
func TestAManifestNamesTheTierOfEveryObject(t *testing.T) {
	l, _, _ := tieredLog(t)

	manifest, err := l.TierManifest()
	require.NoError(t, err)
	require.NotEmpty(t, manifest, "the fixture offloaded nothing, so this proves nothing")

	for _, o := range manifest {
		require.Equal(t, defaultTierName, o.Tier,
			"segment %d names no tier, or the wrong one", o.BaseOffset)
	}

	// The log's own view agrees. tierState builds TierObjects from the segments
	// and tierObject builds them from offloadMeta; both are published, so a tier
	// set in one and not the other would write a manifest that changes shape
	// depending on which path last touched it.
	state, err := l.tierState()
	require.NoError(t, err)
	require.Equal(t, state, manifest)
}

// An entry naming no tier is refused, and the whole manifest with it.
//
// "" is what an absent JSON field decodes to, so allowing it to mean "the only
// tier" would make a manifest written by something that never set the field
// indistinguishable from one that meant the default. That is the sentinel
// collision CleanSpec.Ceiling was an int64 bug for — a zero value forced to mean
// both "unset" and a real value a caller needs. A version 3 manifest names its
// tier or it is not a version 3 manifest.
//
// Refusing the whole file rather than the entry follows the key check beside it:
// a manifest with one bad entry is not a manifest with one segment missing, it
// is a file this build cannot vouch for.
func TestAManifestEntryWithNoTierIsRefused(t *testing.T) {
	l, store, _ := tieredLog(t)

	objs, err := l.TierManifest()
	require.NoError(t, err)
	require.NotEmpty(t, objs)

	// Hand-write a manifest identical to the real one but for the tier name.
	objs[0].Tier = ""
	body, err := json.Marshal(tierManifest{Version: manifestVersion, Segments: objs})
	require.NoError(t, err)
	require.NoError(t, store.Put(manifestKey, bytes.NewReader(body), int64(len(body))))

	_, err = readTierManifest(store)
	require.Error(t, err, "a manifest entry with no tier must not be read as the default")
	require.Contains(t, err.Error(), "names no tier")
}

// The version bump is a refusal, not a translation. Nothing is deployed against
// version 2, so a store written by an older build is re-offloaded rather than
// converted — the same call version 1 got when BlocksKey landed.
func TestAVersionTwoManifestIsRefused(t *testing.T) {
	l, store, _ := tieredLog(t)

	objs, err := l.TierManifest()
	require.NoError(t, err)
	body, err := json.Marshal(tierManifest{Version: 2, Segments: objs})
	require.NoError(t, err)
	require.NoError(t, store.Put(manifestKey, bytes.NewReader(body), int64(len(body))))

	_, err = readTierManifest(store)
	require.Error(t, err)
	require.Contains(t, err.Error(), "version 2")
}
