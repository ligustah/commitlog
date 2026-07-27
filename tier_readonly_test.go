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
		SegmentStore:     store,
		TierReadOnly:     readOnly,
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
	_, superseded, err := l.CleanWithSpec(CleanSpec{
		Ceiling: hw + 1, TombstoneGCBelow: hw + 1,
	})
	require.NoError(t, err)
	require.Empty(t, superseded)

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
	key := segmentStoreKey(1, 0)
	require.NoError(t, store.Put(key, strings.NewReader("x"), 1))

	_, err := l.DeleteStoreObjects([]string{key})
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

	state, err := owner.ExportTierState()
	require.NoError(t, err)
	require.NotEmpty(t, state)

	// A second process over the same records and store, owning nothing.
	dir := tempDir(t)
	follower, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  64,
		SegmentStore:     store,
		TierReadOnly:     true,
		DisableAutoClean: true,
	})
	defer cleanup()
	for i := 0; i < 24; i++ {
		_, err := follower.Append([]*Message{{Key: []byte("k"), Value: []byte("padding value")}})
		require.NoError(t, err)
	}
	follower.SetHighWatermark(last)

	// Importing is a local operation — it writes markers and drops local bytes,
	// but touches no object — so a process that owns nothing can still be told
	// what the store holds.
	applied, err := follower.ImportTierState(state)
	require.NoError(t, err, "a read-only tier must still be able to learn what is there")
	require.Equal(t, len(state), applied)

	require.Equal(t, want, readFrom(t, follower),
		"a process that does not own the tier still reads through it")

	keys, err := store.List()
	require.NoError(t, err)
	require.Len(t, keys, len(state), "and wrote nothing doing so")
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
	l.SetTierReadOnly(false)
	n, err = l.OffloadBefore(last)
	require.NoError(t, err)
	require.Positive(t, n, "the tier must be writable once ownership arrives")

	keys, err := store.List()
	require.NoError(t, err)
	require.NotEmpty(t, keys)

	// And leaves again.
	l.SetTierReadOnly(true)
	_, err = l.DeleteStoreObjects(keys)
	require.Error(t, err, "withdrawing ownership must take effect at once")

	after, err := store.List()
	require.NoError(t, err)
	require.ElementsMatch(t, keys, after)
}
