package commitlog

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The property the manifest exists for: a tier describes itself. Everything the
// store holds is discoverable FROM the store, with no reference to the log
// directory that produced it.
func TestTierManifestDescribesTheStore(t *testing.T) {
	l, store, _ := tieredLog(t)

	manifest, err := l.TierManifest()
	require.NoError(t, err)
	require.NotEmpty(t, manifest, "an offloaded tier must describe itself")

	// It agrees with the log's own view, and covers every offloaded segment.
	state, err := l.tierState()
	require.NoError(t, err)
	require.Equal(t, state, manifest)

	// Every object it names exists, and every non-manifest object is the
	// manifest itself. Nothing in the store is unaccounted for.
	named := map[string]bool{manifestKey: true}
	for _, o := range manifest {
		require.Positive(t, o.PhysPosition)
		require.LessOrEqual(t, o.FirstOffset, o.LastOffset)
		size, err := store.Size(o.LogKey)
		require.NoError(t, err)
		require.Equal(t, o.PhysPosition, size)
		named[o.LogKey] = true
		if o.IndexKey != "" {
			named[o.IndexKey] = true
		}
	}
	keys, err := store.List()
	require.NoError(t, err)
	for _, k := range keys {
		require.True(t, named[k], "object %s is in the store but not the manifest", k)
	}
}

// The case that motivated it: a process holding the STORE and nothing else opens
// the log and reaches the offloaded records. No local markers, no state handed
// over by anyone.
func TestALogOpensATierItHasNoLocalStateFor(t *testing.T) {
	origin, store, last := tieredLog(t)
	want := readFrom(t, origin)
	require.NotEmpty(t, want)

	manifest, err := origin.TierManifest()
	require.NoError(t, err)
	require.NotEmpty(t, manifest)

	// A brand-new log directory over the same store. It has never offloaded
	// anything and holds no markers.
	fresh, cleanup := setupWithOptions(t, Options{
		Path:             tempDir(t),
		MaxSegmentBytes:  64,
		SegmentStore:     store,
		DisableAutoClean: true,
	})
	defer cleanup()

	adopted, err := fresh.TierManifest()
	require.NoError(t, err)
	require.Equal(t, manifest, adopted,
		"the store must describe itself to a log that has never written to it")

	// And the records are actually reachable, not merely described. A fresh log
	// starts with nothing committed, so commit what the tier holds first.
	fresh.SetHighWatermark(last)
	got := readFrom(t, fresh)
	require.NotEmpty(t, got, "a log that adopted a tier must be able to read it")
	for off, val := range got {
		if wantVal, ok := want[off]; ok {
			require.Equal(t, wantVal, val, "offset %d", off)
		}
	}

	require.LessOrEqual(t, fresh.OldestOffset(), last)
}

// A stale manifest is worse than none, because it names objects a reader then
// fails to open. So it is republished whenever the tier changes underneath it.
func TestTierManifestFollowsTheTier(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	l, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  128,
		Compact:          true,
		SegmentStore:     store,
		DisableAutoClean: true,
	})
	defer cleanup()

	var last int64
	for i := 0; i < 30; i++ {
		offs, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("value padding")}})
		require.NoError(t, err)
		last = offs[0]
	}
	for _, k := range []string{"pad0", "pad1"} {
		offs, err := l.Append([]*Message{{Key: []byte(k), Value: []byte("p")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)

	n, err := l.OffloadBefore(last)
	require.NoError(t, err)
	require.Positive(t, n)

	before, err := l.TierManifest()
	require.NoError(t, err)
	require.NotEmpty(t, before)

	// A compaction pass rewrites offloaded segments onto new objects.
	hw := l.HighWatermark()
	_, err = l.CleanWithSpec(CleanSpec{
		Ceiling: hw + 1, TombstoneGCBelow: hw + 1,
	})
	require.NoError(t, err)

	l.tierMu.Lock()
	superseded := make([]string, 0, len(l.reclaim))
	for _, e := range l.reclaim {
		superseded = append(superseded, e.key)
	}
	l.tierMu.Unlock()
	require.NotEmpty(t, superseded, "the fixture must have rewritten something")

	after, err := l.TierManifest()
	require.NoError(t, err)
	require.NotEqual(t, before, after, "the manifest must follow the rewrite")

	// Crucially it names none of the superseded objects — those are exactly
	// what a reader would fail on, and it is also what makes them safe to
	// reclaim on the next pass.
	for _, o := range after {
		require.NotContains(t, superseded, o.LogKey)
		require.NotContains(t, superseded, o.IndexKey)
	}

	// And it still agrees with what the log is actually reading.
	state, err := l.tierState()
	require.NoError(t, err)
	require.Equal(t, state, after)
}

// An object the manifest does not name was never committed — the crash between
// an upload and its manifest. It must be identifiable as garbage rather than
// leaving a reader to guess which of two objects is current.
func TestObjectsOutsideTheManifestAreGarbage(t *testing.T) {
	l, store, _ := tieredLog(t)

	// An upload that never made it into a manifest.
	orphan, _ := newStoreKeys(999)
	require.NoError(t, store.Put(orphan, strings.NewReader("x"), 1))

	manifest, err := l.TierManifest()
	require.NoError(t, err)
	for _, o := range manifest {
		require.NotEqual(t, orphan, o.LogKey)
	}

	orphans, err := l.UnreferencedObjects()
	require.NoError(t, err)
	require.Contains(t, orphans, orphan,
		"an object no manifest names must be reclaimable")
}

// A read-only tier must not write the manifest — it is a store write like any
// other, and a process that does not own the tier has no business republishing
// what it holds.
func TestReadOnlyTierDoesNotWriteTheManifest(t *testing.T) {
	l, store, last := readOnlyFixture(t, true)

	n, err := l.OffloadBefore(last + 1)
	require.NoError(t, err)
	require.Zero(t, n)

	require.NoError(t, l.Clean())

	keys, err := store.List()
	require.NoError(t, err)
	require.Empty(t, keys, "a read-only tier must write nothing, manifest included")
}

// The question a shared-tier collector has to have answered: does this call
// name objects ANOTHER live process is serving? It must not, or garbage
// collection deletes a peer's data.
//
// This is what the manifest buys. Judged by local segments alone the answer
// would be yes, because a process only knows the objects it adopted or wrote.
func TestUnreferencedObjectsSparesAPeersObjects(t *testing.T) {
	origin, store, last := tieredLog(t)

	// A second log over the same store, opened BEFORE the peer offloads more,
	// so it cannot have adopted what comes next.
	peer, cleanup := setupWithOptions(t, Options{
		Path:             tempDir(t),
		MaxSegmentBytes:  64,
		SegmentStore:     store,
		DisableAutoClean: true,
	})
	defer cleanup()

	// The origin offloads further segments the peer has never seen.
	for i := 0; i < 24; i++ {
		offs, err := origin.Append([]*Message{{Key: []byte("k"), Value: []byte("padding value")}})
		require.NoError(t, err)
		last = offs[0]
	}
	origin.SetHighWatermark(last)
	n, err := origin.OffloadBefore(last + 1)
	require.NoError(t, err)
	require.Positive(t, n, "the origin must have offloaded something new")

	live, err := origin.tierState()
	require.NoError(t, err)
	require.NotEmpty(t, live)

	// The peer's own view is stale — it holds fewer segments than the tier now
	// has, which is precisely the situation that used to be dangerous.
	peerState, err := peer.tierState()
	require.NoError(t, err)
	require.Less(t, len(peerState), len(live), "the peer's view must be behind")

	orphans, err := peer.UnreferencedObjects()
	require.NoError(t, err)
	for _, o := range live {
		require.NotContains(t, orphans, o.LogKey,
			"an object the tier's manifest names must never be called garbage")
		if o.IndexKey != "" {
			require.NotContains(t, orphans, o.IndexKey)
		}
	}
}
