package commitlog

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The point of the stamp: two writers that each believe they own tier writes
// address DIFFERENT objects. Without it both read generation N from their own
// local marker, both compute N+1, and one silently overwrites the other — with
// no error to either and the loser possibly holding the current data.
func TestWriterStampMakesKeysDisjoint(t *testing.T) {
	a := segmentStoreKey(42, 0, "nodeA")
	b := segmentStoreKey(42, 0, "nodeB")
	require.NotEqual(t, a, b, "two writers must not address the same object")

	// Including at the same generation, which is the case that matters: the
	// generation is derived per-writer, so a collision there is the norm rather
	// than the exception.
	require.NotEqual(t, segmentStoreKey(42, 3, "nodeA"), segmentStoreKey(42, 3, "nodeB"))
	require.NotEqual(t, segmentIndexStoreKey(42, 3, "nodeA"), segmentIndexStoreKey(42, 3, "nodeB"))

	// An empty writer keeps the key a single-writer log always had.
	require.Equal(t, "00000000000000000042.log", segmentStoreKey(42, 0, ""))
}

// The stamp has to come back out of the key exactly as it went in, because the
// fence compares the parsed value against the current identity. A stamp that
// round-trips short refuses the writer its OWN objects.
func TestWriterStampRoundTrips(t *testing.T) {
	for _, w := range []string{"nodeA", "epoch-17", "a", "n_1", strings.Repeat("x", 64)} {
		require.Equal(t, w, storeKeyWriter(segmentStoreKey(42, 0, w)), "log key for %q", w)
		require.Equal(t, w, storeKeyWriter(segmentIndexStoreKey(42, 7, w)), "index key for %q", w)
	}

	// Unstamped keys, old and new, carry no writer.
	require.Empty(t, storeKeyWriter("00000000000000000042.log"))
	require.Empty(t, storeKeyWriter("00000000000000000042.g3.log"))
	require.Empty(t, storeKeyWriter(""))

	// The parse is positional, so a key from some other naming scheme whose
	// components happen to start with 'w' is not mistaken for a stamp — which
	// would fence a delete that should have been allowed.
	require.Empty(t, storeKeyWriter("00000000000000000042.warm.log"))
}

// A writer id that cannot survive the key format is refused where it is
// supplied, not silently mangled into a fence that no longer works. A dotted
// hostname is the realistic case.
func TestInvalidWriterIdIsRefused(t *testing.T) {
	bad := []string{"node.1", "host.dc.example.com", "a/b", "with space", strings.Repeat("x", 65)}
	for _, id := range bad {
		require.False(t, validTierWriter(id), "%q must not be accepted", id)

		_, err := New(Options{Path: tempDir(t), TierWriter: id})
		require.Error(t, err, "New must refuse %q", id)
		require.Contains(t, err.Error(), "invalid TierWriter")
	}

	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t)})
	defer cleanup()
	for _, id := range bad {
		require.Error(t, l.SetTierWriter(id), "SetTierWriter must refuse %q", id)
	}
	require.NoError(t, l.SetTierWriter("nodeA"))
	require.NoError(t, l.SetTierWriter(""), "unsetting is legal — it means unstamped")

	// A spec-level override skips both entry points above, so it is checked too.
	_, _, err := l.CleanWithSpec(CleanSpec{Ceiling: 0, TierWriter: "node.1"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid CleanSpec.TierWriter")
}

// The other half of C9: a writer must not delete an object it does not own. A
// stale writer acting on its lagging view of ownership would otherwise remove
// objects the current writer is serving, and unlike a clobbered upload there is
// nothing left to recover.
func TestDeleteStoreObjectsFencesForeignKeys(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	l, cleanup := setupWithOptions(t, Options{
		Path: dir, SegmentStore: store, TierWriter: "nodeA",
	})
	defer cleanup()

	var (
		mine    = segmentStoreKey(1, 0, "nodeA")
		theirs  = segmentStoreKey(2, 0, "nodeB")
		legacy  = segmentStoreKey(3, 0, "")
		content = strings.NewReader("x")
	)
	for _, k := range []string{mine, theirs, legacy} {
		content.Reset("x")
		require.NoError(t, store.Put(k, content, 1))
	}

	// Someone else's object: refused, and still there afterwards.
	_, err = l.DeleteStoreObjects([]string{theirs})
	require.Error(t, err)
	require.Contains(t, err.Error(), "written by \"nodeB\"")
	keys, err := store.List()
	require.NoError(t, err)
	require.Contains(t, keys, theirs, "a fenced delete must not have happened")

	// Its own, and an unstamped one that predates any identity.
	deleted, err := l.DeleteStoreObjects([]string{mine, legacy})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{mine, legacy}, deleted)

	keys, err = store.List()
	require.NoError(t, err)
	require.NotContains(t, keys, mine)
	require.NotContains(t, keys, legacy)

	// Idempotent: deleting what is already gone is not an error, because a
	// caller retrying after a partial failure must not be forced to distinguish.
	_, err = l.DeleteStoreObjects([]string{mine})
	require.NoError(t, err)
}

// Losing ownership must not cost readability. Keys are resolved from the
// marker verbatim and never recomputed, so objects written under one identity
// stay readable under the next — which is what lets the stamp change at all.
func TestReadsSurviveAWriterChange(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	l, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  64,
		SegmentStore:     store,
		TierWriter:       "nodeA",
		DisableAutoClean: true,
	})
	defer cleanup()

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

	before := readFrom(t, l)
	require.NotEmpty(t, before)

	// Ownership moves to this process under a new identity.
	require.NoError(t, l.SetTierWriter("nodeB"))

	require.Equal(t, before, readFrom(t, l),
		"objects written under the previous identity must stay readable")
}

// Fencing turns a corruption bug into a storage leak, which is better but still
// a bug. The objects a rewrite supersedes are reported so the owner can reclaim
// them rather than pay for them forever.
func TestUnreferencedObjectsFindsSupersededOnes(t *testing.T) {
	l, store, seg := offloadedFixture(t, nil)

	// Nothing is garbage yet: every object is the one its segment is reading.
	orphans, err := l.UnreferencedObjects()
	require.NoError(t, err)
	require.Empty(t, orphans, "a freshly offloaded log has nothing to reclaim")

	fresh := freshLocalSegment(t, l, seg)
	superseded, err := seg.ReplaceOffloaded(fresh, "", nil)
	require.NoError(t, err)
	require.NotEmpty(t, superseded)

	// The superseded object is still in the store — deliberately, since a
	// reader that opened the segment first is entitled to finish — so it is now
	// exactly the garbage this reports.
	orphans, err = l.UnreferencedObjects()
	require.NoError(t, err)
	require.Subset(t, orphans, superseded,
		"an object no segment reads any more must be reclaimable")

	// And the live object is not in the list, which is the half that matters:
	// reclaiming it would break every reader of the log.
	seg.RLock()
	live := seg.storeKey
	seg.RUnlock()
	require.NotContains(t, orphans, live)

	deleted, err := l.DeleteStoreObjects(orphans)
	require.NoError(t, err)
	require.ElementsMatch(t, orphans, deleted)

	keys, err := store.List()
	require.NoError(t, err)
	require.Contains(t, keys, live, "reclaiming garbage must not touch live data")
	require.NotEmpty(t, readFrom(t, l), "and the log still reads")
}

// Retention must keep working across an identity change, and this is the test
// that says so, because the obvious design does not.
//
// Fencing a HELD segment against the current identity looks like the same
// defence as fencing a key from a listing, but it is not. After ownership
// moves, every segment already in the tier carries the previous identity's
// stamp — so a fenced retention pass can never drop its oldest segment again
// and the tier grows without bound. That is strictly worse than the orphaned
// object the fence was protecting against.
//
// What entitles the log here is not the stamp but the marker: the segment's own
// offload marker names the object.
func TestRetentionDropsASegmentStampedByAPreviousIdentity(t *testing.T) {
	_, store, seg := offloadedFixture(t, nil)

	seg.RLock()
	key := seg.storeKey
	seg.RUnlock()
	require.NotEmpty(t, key)

	// The object in the tier was written before ownership moved.
	seg.Lock()
	seg.storeWriter = "nodeA"
	seg.Unlock()

	require.NoError(t, deleteSegment(seg),
		"retention must still drop a segment the log holds")

	keys, err := store.List()
	require.NoError(t, err)
	require.NotContains(t, keys, key, "its object must go with it")
}

// An object written under an identity this log no longer holds cannot be
// deleted through the fence — which is the leak UnreferencedObjects exists to
// make visible. Worth pinning, because it is the surprising consequence of
// fencing: a node cannot clean up after its own previous epoch.
func TestObjectsFromAPreviousIdentityNeedReclaiming(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	l, cleanup := setupWithOptions(t, Options{
		Path: dir, SegmentStore: store, TierWriter: "epoch1",
	})
	defer cleanup()

	old := segmentStoreKey(9, 0, "epoch1")
	require.NoError(t, store.Put(old, strings.NewReader("x"), 1))

	// Same node, next epoch.
	require.NoError(t, l.SetTierWriter("epoch2"))

	_, err = l.DeleteStoreObjects([]string{old})
	require.Error(t, err, "the fence cannot tell a previous self from a rival")

	// It is visible as garbage, which is what makes the leak bounded.
	orphans, err := l.UnreferencedObjects()
	require.NoError(t, err)
	require.Contains(t, orphans, old)
}

// The two reclamation calls have to compose ACROSS an identity change, which is
// the case that actually happens in production. A rewrite supersedes the object
// the PREVIOUS identity wrote, so a fence applied to superseded keys refuses the
// caller the very keys the pass just handed it — not once, but on every rewrite
// from the failover onwards.
//
// A superseded object is one this log's own marker named until the rewrite
// replaced it, so its lineage is not in doubt and the fence must not apply.
func TestReclamationComposesAfterAWriterChange(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	l, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  128,
		Compact:          true,
		SegmentStore:     store,
		TierWriter:       "nodeA",
		DisableAutoClean: true,
	})
	defer cleanup()

	// One key many times, so the sealed segments are full of superseded copies
	// and a later pass has something to rewrite.
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
	require.Positive(t, n, "the tier must hold objects stamped by the first identity")

	// Ownership moves to this process under a new identity BEFORE anything is
	// compacted, so everything the next pass supersedes was written by the
	// previous identity — which is the situation after any real failover.
	require.NoError(t, l.SetTierWriter("nodeB"))

	hw := l.HighWatermark()
	_, superseded, err := l.CleanWithSpec(CleanSpec{
		Ceiling: hw + 1, TombstoneGCBelow: hw + 1,
	})
	require.NoError(t, err)

	var stale []string
	for _, k := range superseded {
		if storeKeyWriter(k) == "nodeA" {
			stale = append(stale, k)
		}
	}
	require.NotEmpty(t, stale,
		"the fixture must supersede at least one object the previous identity wrote")

	deleted, err := l.DeleteStoreObjects(superseded)
	require.NoError(t, err,
		"what a pass supersedes, the caller must be able to delete — otherwise "+
			"every rewrite after a failover leaks an object permanently")
	require.ElementsMatch(t, superseded, deleted)

	keys, err := store.List()
	require.NoError(t, err)
	for _, k := range stale {
		require.NotContains(t, keys, k)
	}
	require.NotEmpty(t, readFrom(t, l), "and the log still reads")
}

// The escape hatch for objects the fence would otherwise strand for good: ones
// written under an identity this process no longer holds and did not supersede
// in its current lifetime — a crash between a rewrite and the delete of what it
// replaced. Only the caller can assert the old identity is finished, so it has
// to say so explicitly.
func TestAdoptTierWritersAllowsReclaimingAPreviousIdentity(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	l, cleanup := setupWithOptions(t, Options{
		Path: dir, SegmentStore: store, TierWriter: "epoch2",
	})
	defer cleanup()

	stranded := segmentStoreKey(9, 0, "epoch1")
	require.NoError(t, store.Put(stranded, strings.NewReader("x"), 1))

	_, err = l.DeleteStoreObjects([]string{stranded})
	require.Error(t, err, "an unadopted identity stays fenced")
	require.Contains(t, err.Error(), "adopt it")

	require.Error(t, l.AdoptTierWriters("bad.id"), "adoption validates like any id")
	require.Error(t, l.AdoptTierWriters(""), "the unstamped case is not an identity")

	require.NoError(t, l.AdoptTierWriters("epoch1"))
	deleted, err := l.DeleteStoreObjects([]string{stranded})
	require.NoError(t, err)
	require.Equal(t, []string{stranded}, deleted)

	// Adoption is specific: it does not open the fence generally.
	other := segmentStoreKey(10, 0, "epoch3")
	require.NoError(t, store.Put(other, strings.NewReader("x"), 1))
	_, err = l.DeleteStoreObjects([]string{other})
	require.Error(t, err, "adopting one identity must not adopt every identity")
}
