package commitlog

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A chain this build cannot honour is refused at New, not accepted and half
// used.
//
// Step 2 of docs/multi-store-tiering.md carries the plumbing for a chain while
// still enforcing one tier. A caller who configures an archive below its hot
// tier and gets one that silently never receives anything has been told its
// data is somewhere it is not — which is worse than being told the build cannot
// do it yet, and indistinguishable from working until the first time anyone
// looks in the archive.
func TestATierChainThisBuildCannotHonourIsRefused(t *testing.T) {
	dir := tempDir(t)
	hot, err := NewFileSegmentStore(filepath.Join(dir, "hot"))
	require.NoError(t, err)
	cold, err := NewFileSegmentStore(filepath.Join(dir, "cold"))
	require.NoError(t, err)

	t.Run("more than one tier", func(t *testing.T) {
		l, err := New(Options{Path: tempDir(t), MaxSegmentBytes: 1024, Tiers: []Tier{
			{Name: "hot", Store: hot},
			{Name: "cold", Store: cold},
		}})
		require.Error(t, err)
		require.Nil(t, l)
		require.Contains(t, err.Error(), "one")
	})

	t.Run("a tier with no name", func(t *testing.T) {
		l, err := New(Options{Path: tempDir(t), MaxSegmentBytes: 1024, Tiers: []Tier{
			{Store: hot},
		}})
		require.Error(t, err, "an unnamed tier cannot be told from one that was never named")
		require.Nil(t, l)
		require.Contains(t, err.Error(), "Name")
	})

	t.Run("a tier with no store", func(t *testing.T) {
		l, err := New(Options{Path: tempDir(t), MaxSegmentBytes: 1024, Tiers: []Tier{
			{Name: "hot"},
		}})
		require.Error(t, err)
		require.Nil(t, l)
		require.Contains(t, err.Error(), "Store")
	})

	t.Run("one named tier with a store", func(t *testing.T) {
		l, err := New(Options{Path: tempDir(t), MaxSegmentBytes: 1024, Tiers: []Tier{
			{Name: "hot", Store: hot},
		}})
		require.NoError(t, err)
		require.NoError(t, l.Close())
	})
}

// An object naming a tier this log has not been given is an error, not a read
// from the nearest store.
//
// This is the first thing TierObject.Tier is actually FOR. Answering with the
// primary tier would read one store's bytes under another store's keys — a key
// is allocated per upload, so the object almost certainly is not there, and the
// error the caller gets names a missing object rather than a misconfigured
// chain. The distinction matters when the missing object is the whole archive.
func TestAnObjectNamingAnUnconfiguredTierIsRefused(t *testing.T) {
	l, store, _ := tieredLog(t)

	objs, err := l.TierManifest()
	require.NoError(t, err)
	require.NotEmpty(t, objs)
	for i := range objs {
		objs[i].Tier = "cold"
	}
	body, err := json.Marshal(tierManifest{Version: manifestVersion, Segments: objs})
	require.NoError(t, err)
	require.NoError(t, store.Put(manifestKey, bytes.NewReader(body), int64(len(body))))

	// A fresh log over the same store, so every segment comes from the manifest
	// rather than from this process's own memory.
	reopened, err := New(Options{
		Path:             tempDir(t),
		MaxSegmentBytes:  64,
		Tiers:            oneTier(store),
		DisableAutoClean: true,
		AdoptOptions:     true,
	})
	require.Error(t, err, "a manifest naming an unconfigured tier must not open")
	require.Nil(t, reopened)
	require.Contains(t, err.Error(), "cold")
}

// The sweep says which store each orphan is in, and the delete takes that back.
//
// A bare key stopped being enough the moment a log could have two stores, and
// orphans are precisely the objects no manifest names — so nothing else could
// resolve one afterwards. The pair has to round-trip.
func TestTheOrphanSweepNamesTheTierAndTheDeleteTakesItBack(t *testing.T) {
	l, store, _ := tieredLog(t)

	orphan, _, _ := newStoreKeys(999)
	require.NoError(t, store.Put(orphan, strings.NewReader("x"), 1))

	garbage, err := l.UnreferencedObjects()
	require.NoError(t, err)
	require.Contains(t, garbage, StoreObject{Tier: defaultTierName, Key: orphan},
		"the sweep must name the tier holding each orphan, not just its key")

	deleted, err := l.DeleteStoreObjects(garbage)
	require.NoError(t, err)
	require.ElementsMatch(t, garbage, deleted)

	keys, err := store.List()
	require.NoError(t, err)
	require.NotContains(t, keys, orphan)
}

// A delete addressed to a tier this log does not have is refused rather than
// applied to the one it does. The write-side twin of the read refusal above,
// and the reason DeleteStoreObjects takes a tier at all.
func TestADeleteNamingAnUnconfiguredTierIsRefused(t *testing.T) {
	l, store, _ := tieredLog(t)

	orphan, _, _ := newStoreKeys(999)
	require.NoError(t, store.Put(orphan, strings.NewReader("x"), 1))

	deleted, err := l.DeleteStoreObjects([]StoreObject{{Tier: "cold", Key: orphan}})
	require.Error(t, err)
	require.Empty(t, deleted)

	keys, err := store.List()
	require.NoError(t, err)
	require.Contains(t, keys, orphan, "a refused delete must not have happened")
}

// Every offloaded object records the tier it went into, taken from the
// configured chain rather than from a constant.
func TestAnOffloadRecordsTheTierItWentInto(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	l, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  64,
		Tiers:            []Tier{{Name: "warm", Store: store}},
		DisableAutoClean: true,
	})
	t.Cleanup(cleanup)

	var last int64
	for i := 0; i < 24; i++ {
		offs, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("padding value")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)
	n, err := l.OffloadBefore(last)
	require.NoError(t, err)
	require.Positive(t, n)

	manifest, err := l.TierManifest()
	require.NoError(t, err)
	require.NotEmpty(t, manifest)
	for _, o := range manifest {
		require.Equal(t, "warm", o.Tier,
			"segment %d recorded the wrong tier", o.BaseOffset)
	}
}
