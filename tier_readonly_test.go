package commitlog

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// readOnlyFixture builds a log holding records and a store, with the tier in
// whichever mode the test wants.
func readOnlyFixture(t *testing.T, readOnly bool) (*commitLog, *FileSegmentStore, int64) {
	t.Helper()
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	l, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  64,
		Tiers:            oneTierReadOnly(store, readOnly),
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
	return l, store, last
}

// A read-only tier is the whole ownership mechanism, so what it must guarantee
// is simple and absolute: the log touches the store not at all. Asserted as
// ZERO objects rather than "fewer", because a single write into a store this
// process does not own is the corruption the contract exists to prevent — a
// reduced count would be no guarantee at all.
func TestReadOnlyTierWritesNothing(t *testing.T) {
	l, store, last := readOnlyFixture(t, true)

	n, err := l.OffloadBefore(last)
	require.NoError(t, err, "not owning the tier is not an error")
	require.Zero(t, n, "a read-only tier must not offload")

	hw := l.HighWatermark()
	_, err = l.CleanWithSpec(CleanSpec{
		Ceiling: At(hw + 1), TombstoneGCBelow: hw + 1,
	})
	require.NoError(t, err)
	l.tierMu.Lock()
	require.Empty(t, l.reclaim, "a pass that rewrote nothing supersedes nothing")
	l.tierMu.Unlock()

	require.NoError(t, l.Clean())

	keys, err := store.List()
	require.NoError(t, err)
	require.Empty(t, keys, "nothing may have been written to the store")
}

// Deleting is refused outright rather than silently skipped. A caller that
// believes it reclaimed an object and did not would keep paying for it while
// its own accounting said otherwise.
func TestReadOnlyTierRefusesDeletes(t *testing.T) {
	l, store, _ := readOnlyFixture(t, true)

	// Something a previous owner left behind.
	key, _, _ := newStoreKeys(1)
	require.NoError(t, store.Put(key, strings.NewReader("x"), 1))

	_, err := l.DeleteStoreObjects([]StoreObject{{Tier: defaultTierName, Key: key}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "read-only")

	keys, err := store.List()
	require.NoError(t, err)
	require.Contains(t, keys, key, "a refused delete must not have happened")
}

// Read-only is about WRITES. A process that does not own the tier still has to
// serve reads through it, or a follower could not read its own log.
func TestReadOnlyTierStillReadsThroughTheStore(t *testing.T) {
	owner, store, last := readOnlyFixture(t, false)
	n, err := owner.OffloadBefore(last)
	require.NoError(t, err)
	require.Positive(t, n)
	want := readFrom(t, owner)
	require.NotEmpty(t, want)

	state, err := owner.TierManifest()
	require.NoError(t, err)
	require.NotEmpty(t, state)

	// A second process over the same records and store, owning nothing.
	dir := tempDir(t)
	follower, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  64,
		Tiers:            oneTierReadOnly(store, true),
		DisableAutoClean: true,
	})
	defer cleanup()
	// It appends nothing: everything it serves comes from the tier it adopted
	// at open. That IS the property under test — a process with no local data
	// and no right to write still reads the log.
	follower.SetHighWatermark(last)

	// It already knows what the store holds — the manifest told it at open,
	// without any grant to write. That is the property that matters: a process
	// owning nothing can still learn the tier.
	known, err := follower.TierManifest()
	require.NoError(t, err)
	require.Equal(t, state, known)

	// It serves what the TIER holds, which is the offloaded segments — the
	// origin's active segment was never offloaded, so the follower is a proper
	// prefix rather than a copy.
	got := readFrom(t, follower)
	require.NotEmpty(t, got,
		"a process that does not own the tier still reads through it")
	require.Less(t, len(got), len(want), "the active segment is not in the tier")
	for off, val := range got {
		require.Equal(t, want[off], val, "offset %d", off)
	}

	keys, err := store.List()
	require.NoError(t, err)
	// Counting only segment objects. The manifest and the descriptor are the
	// tier describing itself — what it holds and what it is — and the owner
	// wrote both; neither is something the follower added.
	objects := segmentObjectCount(keys)
	require.Equal(t, len(state), objects, "and wrote nothing doing so")
}

// The handover: ownership moves by the previous owner going read-only before
// the next comes out of it. Both directions have to take effect immediately for
// that to be expressible at all.
func TestSetTierReadOnlyTakesEffectBothWays(t *testing.T) {
	l, store, last := readOnlyFixture(t, true)

	n, err := l.OffloadBefore(last)
	require.NoError(t, err)
	require.Zero(t, n)

	// Ownership arrives.
	require.NoError(t, l.SetTierReadOnly(defaultTierName, false))
	n, err = l.OffloadBefore(last)
	require.NoError(t, err)
	require.Positive(t, n, "the tier must be writable once ownership arrives")

	keys, err := store.List()
	require.NoError(t, err)
	require.NotEmpty(t, keys)
	objs := make([]StoreObject, 0, len(keys))
	for _, k := range keys {
		objs = append(objs, StoreObject{Tier: defaultTierName, Key: k})
	}

	// And leaves again.
	require.NoError(t, l.SetTierReadOnly(defaultTierName, true))
	_, err = l.DeleteStoreObjects(objs)
	require.Error(t, err, "withdrawing ownership must take effect at once")

	after, err := store.List()
	require.NoError(t, err)
	require.ElementsMatch(t, keys, after)
}

// A handover to a tier that does not exist is refused.
//
// This is the one call in the API whose whole purpose is to STOP writing, so a
// silent no-op is the worst possible answer: a caller that misnames the tier it
// is handing over is told nothing and goes on believing it stopped writing to a
// store it is still writing to. That is exactly the two-writer situation the
// single-writer contract exists to prevent, arrived at by a typo.
func TestSetTierReadOnlyRefusesAnUnknownTier(t *testing.T) {
	l, _, _ := readOnlyFixture(t, false)

	err := l.SetTierReadOnly("archive", true)
	require.Error(t, err, "a handover to a tier this log has no store for must not be silent")
	require.Contains(t, err.Error(), "archive")

	// And the tier it does have is untouched by the refusal.
	require.True(t, l.tierWritable(defaultTierName))
}

// A batch naming a tier this log may not write to deletes NOTHING, whichever
// position that object holds in the caller's slice.
//
// The refusal is documented as outright, and it has to be: deleting as it went
// made what survived depend on the ORDER of the slice. An operator sees an
// error either way, but a retry after fixing the ownership then removes a
// different remainder than the first attempt implied — the worst property
// available to an unfenced tool whose whole job is destroying data.
func TestDeleteStoreObjectsRefusesTheWholeBatchWhateverItsOrder(t *testing.T) {
	l, hot, _, _ := chainLog(t)

	manifest, err := l.TierManifest()
	require.NoError(t, err)
	require.NotEmpty(t, manifest, "the fixture needs an offloaded object to spare")
	live := StoreObject{Tier: "hot", Key: manifest[0].LogKey}

	require.NoError(t, l.SetTierReadOnly("cold", true))
	stillThere := func() {
		t.Helper()
		_, err := hot.Size(live.Key)
		require.NoError(t, err, "the whole batch is refused, so %s is untouched", live.Key)
	}

	// Read-only tier, then a tier that is not configured at all: both are
	// answers this log gives BEFORE it deletes anything.
	for _, bad := range []StoreObject{
		{Tier: "cold", Key: "some-cold-object"},
		{Tier: "archive", Key: "some-object"},
		{Tier: "", Key: "some-object"},
	} {
		for _, batch := range [][]StoreObject{{live, bad}, {bad, live}} {
			deleted, err := l.DeleteStoreObjects(batch)
			require.Error(t, err, "tier %q", bad.Tier)
			require.Empty(t, deleted, "a refused batch reports nothing deleted")
			stillThere()
		}
	}

	// The same object in the tier this log does own still goes.
	deleted, err := l.DeleteStoreObjects([]StoreObject{live})
	require.NoError(t, err)
	require.Equal(t, []StoreObject{live}, deleted)
}
