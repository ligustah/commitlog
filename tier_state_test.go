package commitlog

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// tieredLog builds a log with some sealed segments offloaded under the given
// writer identity, and returns it with its store.
func tieredLog(t *testing.T, writer string) (*commitLog, *FileSegmentStore, int64) {
	t.Helper()
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	l, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  64,
		SegmentStore:     store,
		TierWriter:       writer,
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
	require.Positive(t, n, "the fixture needs offloaded segments")
	return l, store, last
}

// The state has to carry everything needed to place a segment without reading
// its object — including the writer and generation, so the receiving log can
// allocate the NEXT generation on a rewrite instead of colliding with what is
// already there.
func TestExportTierStateDescribesEveryOffloadedSegment(t *testing.T) {
	l, _, _ := tieredLog(t, "nodeA")

	state, err := l.ExportTierState()
	require.NoError(t, err)
	require.NotEmpty(t, state)

	for _, o := range state {
		require.NotEmpty(t, o.LogKey)
		require.Equal(t, "nodeA", o.Writer, "the identity must survive the handover")
		require.Equal(t, 0, o.Generation)
		require.Equal(t, o.LogKey, segmentStoreKey(o.BaseOffset, o.Generation, o.Writer),
			"the key must be the one its base offset, generation and writer name")
		require.Positive(t, o.PhysPosition)
		require.LessOrEqual(t, o.FirstOffset, o.LastOffset)
	}

	// Only offloaded segments appear: the active one is still being written.
	l.mu.RLock()
	active := l.segments[len(l.segments)-1].BaseOffset
	l.mu.RUnlock()
	for _, o := range state {
		require.NotEqual(t, active, o.BaseOffset)
	}
}

// The case C10 exists for: a process that did not upload the objects takes over,
// holding the same records locally but no markers. Importing the previous
// owner's state must make those objects its own — readable through the log, and
// not uploaded a second time.
func TestImportTierStateAdoptsObjectsWrittenByAnotherProcess(t *testing.T) {
	old, store, last := tieredLog(t, "nodeA")
	state, err := old.ExportTierState()
	require.NoError(t, err)
	require.NotEmpty(t, state)
	want := readFrom(t, old)
	require.NotEmpty(t, want)

	// A second log over the same records and the same store, which never
	// offloaded anything: the successor.
	dir := tempDir(t)
	successor, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  64,
		SegmentStore:     store,
		TierWriter:       "nodeB",
		DisableAutoClean: true,
	})
	defer cleanup()
	for i := 0; i < 24; i++ {
		_, err := successor.Append([]*Message{{Key: []byte("k"), Value: []byte("padding value")}})
		require.NoError(t, err)
	}
	successor.SetHighWatermark(last)

	before, err := successor.ExportTierState()
	require.NoError(t, err)
	require.Empty(t, before, "the successor starts with no tier bookkeeping at all")

	n, err := successor.ImportTierState(state)
	require.NoError(t, err)
	require.Equal(t, len(state), n)

	// It now reads those records THROUGH the store, and gets the same data.
	require.Equal(t, want, readFrom(t, successor))

	after, err := successor.ExportTierState()
	require.NoError(t, err)
	require.Equal(t, state, after, "the state must survive the round trip intact")

	// Checked against the KEY as well, not only against the other side of the
	// round trip: export and import share the code that carries the identity,
	// so a round trip that lost it on both sides would still compare equal.
	for _, o := range after {
		require.Equal(t, storeKeyWriter(o.LogKey), o.Writer,
			"the exported identity must agree with the key it names")
		require.Equal(t, "nodeA", o.Writer)
	}

	// And it did not upload a second copy of anything.
	keys, err := store.List()
	require.NoError(t, err)
	for _, k := range keys {
		require.Equal(t, "nodeA", storeKeyWriter(k),
			"adoption must not have written new objects")
	}
}

// Having adopted them, the successor can also reclaim them — which is the other
// half of the problem, since objects it cannot name are objects it pays for
// forever.
func TestImportedObjectsBecomeReclaimable(t *testing.T) {
	old, store, last := tieredLog(t, "nodeA")
	state, err := old.ExportTierState()
	require.NoError(t, err)

	dir := tempDir(t)
	successor, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  64,
		SegmentStore:     store,
		TierWriter:       "nodeB",
		DisableAutoClean: true,
	})
	defer cleanup()
	for i := 0; i < 24; i++ {
		_, err := successor.Append([]*Message{{Key: []byte("k"), Value: []byte("padding value")}})
		require.NoError(t, err)
	}
	successor.SetHighWatermark(last)
	_, err = successor.ImportTierState(state)
	require.NoError(t, err)

	// Nothing is garbage while the segments read it.
	orphans, err := successor.UnreferencedObjects()
	require.NoError(t, err)
	require.Empty(t, orphans)

	// Retention drops a tiered segment, and its object goes with it — even
	// though the object carries the PREVIOUS identity's stamp.
	successor.mu.RLock()
	var tiered *segment
	for _, s := range successor.segments {
		if s.isOffloaded() {
			tiered = s
			break
		}
	}
	successor.mu.RUnlock()
	require.NotNil(t, tiered)
	key := tiered.storeKey

	require.NoError(t, deleteSegment(tiered))
	keys, err := store.List()
	require.NoError(t, err)
	require.NotContains(t, keys, key)
}

// The check that matters most: adopting an object means DROPPING the local
// bytes for those records, so if the object covers something other than what
// the segment holds, the import would swap a reader's data underneath it. That
// is silent — the records are still there, just different ones — so it has to be
// refused rather than detected later.
func TestImportTierStateRefusesAnObjectCoveringOtherRecords(t *testing.T) {
	old, store, last := tieredLog(t, "nodeA")
	state, err := old.ExportTierState()
	require.NoError(t, err)
	require.NotEmpty(t, state)

	dir := tempDir(t)
	successor, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  64,
		SegmentStore:     store,
		TierWriter:       "nodeB",
		DisableAutoClean: true,
	})
	defer cleanup()
	for i := 0; i < 24; i++ {
		_, err := successor.Append([]*Message{{Key: []byte("k"), Value: []byte("padding value")}})
		require.NoError(t, err)
	}
	successor.SetHighWatermark(last)

	// Self-consistent, and names an object that really exists — it simply does
	// not describe the records the local segment holds.
	bent := make([]TierObject, len(state))
	copy(bent, state)
	bent[0].LastOffset--
	require.LessOrEqual(t, bent[0].FirstOffset, bent[0].LastOffset)

	_, err = successor.ImportTierState(bent)
	require.Error(t, err)
	require.Contains(t, err.Error(), "but the local segment holds")

	// The refusal must leave the segment local and readable, not half-adopted.
	tier, err := successor.ExportTierState()
	require.NoError(t, err)
	require.Empty(t, tier, "a refused adoption must not have attached anything")
	require.NotEmpty(t, readFrom(t, successor))
}

// Import is refused unless it can be applied exactly as described. Each of
// these would otherwise corrupt the read path in a way the caller could not see
// from the return value.
func TestImportTierStateRefusesStateItCannotApply(t *testing.T) {
	l, store, _ := tieredLog(t, "nodeA")
	state, err := l.ExportTierState()
	require.NoError(t, err)
	require.NotEmpty(t, state)

	l.mu.RLock()
	active := l.segments[len(l.segments)-1].BaseOffset
	l.mu.RUnlock()

	bend := func(f func(o *TierObject)) []TierObject {
		out := make([]TierObject, len(state))
		copy(out, state)
		f(&out[0])
		return out
	}

	cases := []struct {
		name  string
		objs  []TierObject
		match string
	}{
		{"missing object", bend(func(o *TierObject) {
			o.LogKey = segmentStoreKey(o.BaseOffset, 9, "nodeA")
		}), "missing object"},
		{"no log key", bend(func(o *TierObject) { o.LogKey = "" }), "no log key"},
		{"unusable writer", bend(func(o *TierObject) { o.Writer = "node.1" }), "unusable writer"},
		{"offsets inverted", bend(func(o *TierObject) {
			o.FirstOffset = o.LastOffset + 1
		}), "above last offset"},
		{"empty range with a real start", bend(func(o *TierObject) {
			o.LastOffset = -1
		}), "above last offset"},
		{"segment not held", bend(func(o *TierObject) { o.BaseOffset = 9999 }), "does not have"},
		{"the active segment", bend(func(o *TierObject) { o.BaseOffset = active }), "active segment"},
		{"same segment twice", append(append([]TierObject{}, state...), state[0]), "twice"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := l.ImportTierState(tc.objs)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.match)
		})
	}

	// After every rejection the log is unchanged and still readable.
	after, err := l.ExportTierState()
	require.NoError(t, err)
	require.Equal(t, state, after, "a refused import must change nothing")
	require.NotEmpty(t, readFrom(t, l))

	keys, err := store.List()
	require.NoError(t, err)
	require.NotEmpty(t, keys)
}

// A batch that is valid up front but fails partway must not report success for
// what it managed. The offsets check fires per segment, so a second entry that
// disagrees with its local segment stops the import mid-batch.
func TestImportTierStateReportsWhatItApplied(t *testing.T) {
	l, _, _ := tieredLog(t, "nodeA")
	state, err := l.ExportTierState()
	require.NoError(t, err)

	// Importing a log's own state is a no-op: every segment already points at
	// exactly those objects.
	n, err := l.ImportTierState(state)
	require.NoError(t, err)
	require.Zero(t, n, "re-importing the current state must change nothing")
}

// Importing needs a store, and an empty import is not an error — a caller
// replicating tier state should not have to special-case a log with none.
func TestImportTierStateEdgeCases(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t)})
	defer cleanup()

	n, err := l.ImportTierState(nil)
	require.NoError(t, err)
	require.Zero(t, n)

	state, err := l.ExportTierState()
	require.NoError(t, err)
	require.Empty(t, state, "a log with no store has no tier bookkeeping")

	_, err = l.ImportTierState([]TierObject{{BaseOffset: 0, LogKey: "x"}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "without a SegmentStore")
}
