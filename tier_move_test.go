package commitlog

import (
	"io"
	"path/filepath"
	"sync/atomic"

	"github.com/pkg/errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// chainLog is a log over a two-tier chain with everything offloaded into the
// hot tier, which is where a segment starts before anyone places it lower.
func chainLog(t *testing.T) (l *commitLog, hot, cold *FileSegmentStore, last int64) {
	t.Helper()
	dir := tempDir(t)
	hot, err := NewFileSegmentStore(filepath.Join(dir, "hot"))
	require.NoError(t, err)
	cold, err = NewFileSegmentStore(filepath.Join(dir, "cold"))
	require.NoError(t, err)

	l, cleanup := setupWithOptions(t, Options{
		Path:            dir,
		MaxSegmentBytes: 64,
		Tiers: []Tier{
			{Name: "hot", Store: hot},
			{Name: "cold", Store: cold},
		},
		DisableAutoClean: true,
	})
	t.Cleanup(cleanup)

	for i := 0; i < 24; i++ {
		offs, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("padding value")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)

	n, err := l.OffloadBefore(last)
	require.NoError(t, err)
	require.Positive(t, n, "the fixture needs offloaded segments")
	return l, hot, cold, last
}

// requireNoSegmentsIn asserts a tier holds no segment objects. Its descriptor
// and its (empty) manifest are there from open and are not segment bytes: every
// tier gets both, so that a node handed one store alone can say which log it
// belongs to and what it holds.
func requireNoSegmentsIn(t *testing.T, store *FileSegmentStore) {
	t.Helper()
	objs, err := readTierManifest(store)
	require.NoError(t, err)
	require.Empty(t, objs, "the tier's manifest must name nothing")

	keys, err := store.List()
	require.NoError(t, err)
	for _, k := range keys {
		require.Contains(t, []string{manifestKey, descriptorKey}, k,
			"nothing but the tier's own bookkeeping may be in the store")
	}
}

// tierOf answers which tier the manifests say a segment is in.
func tierOf(t *testing.T, l *commitLog, base int64) string {
	t.Helper()
	objs, err := l.TierManifest()
	require.NoError(t, err)
	for _, o := range objs {
		if o.BaseOffset == base {
			return o.Tier
		}
	}
	return ""
}

// A placed segment moves, and the log keeps reading.
//
// The point of the second hop: the caller names the destination and commitlog
// moves the bytes. Reads must not notice — the segment is the same records,
// served from a different store.
func TestAPlacedSegmentMovesAndTheLogStillReads(t *testing.T) {
	l, hot, cold, _ := chainLog(t)

	before := readFrom(t, l)
	objs, err := l.TierManifest()
	require.NoError(t, err)
	require.NotEmpty(t, objs)
	base := objs[0].BaseOffset
	require.Equal(t, "hot", tierOf(t, l, base))

	_, err = l.CleanWithSpec(CleanSpec{TierPlacement: map[int64]string{base: "cold"}})
	require.NoError(t, err)

	require.Equal(t, "cold", tierOf(t, l, base), "the manifests must agree the segment moved")
	require.Equal(t, before, readFrom(t, l),
		"a move is a byte-for-byte copy; the records are the same records")

	// The destination holds the objects, and the source's manifest has let go.
	coldKeys, err := cold.List()
	require.NoError(t, err)
	require.NotEmpty(t, coldKeys)
	hotObjs, err := readTierManifest(hot)
	require.NoError(t, err)
	for _, o := range hotObjs {
		require.NotEqual(t, base, o.BaseOffset, "the source manifest must release the segment")
	}
}

// The marker exists only across the window it describes. Once the source has
// let go, republishing the destination clears it — otherwise a later move back
// would find a stale marker and resolve in favour of the wrong tier.
func TestACompletedMoveLeavesNoMarker(t *testing.T) {
	l, _, cold, _ := chainLog(t)

	objs, err := l.TierManifest()
	require.NoError(t, err)
	base := objs[0].BaseOffset

	_, err = l.CleanWithSpec(CleanSpec{TierPlacement: map[int64]string{base: "cold"}})
	require.NoError(t, err)

	coldObjs, err := readTierManifest(cold)
	require.NoError(t, err)
	var found bool
	for _, o := range coldObjs {
		if o.BaseOffset != base {
			continue
		}
		found = true
		require.Empty(t, o.MovedFrom,
			"a move nobody is still resolving must not keep saying where it came from")
	}
	require.True(t, found, "the destination manifest must name the moved segment")
}

// A moved segment survives a restart, read from the tier it was placed in.
// This is the whole point of publishing the manifests: the placement is a fact
// about the stores, not about this process.
func TestAMovedSegmentReopensFromItsNewTier(t *testing.T) {
	l, hot, cold, last := chainLog(t)

	objs, err := l.TierManifest()
	require.NoError(t, err)
	base := objs[0].BaseOffset
	before := readFrom(t, l)

	_, err = l.CleanWithSpec(CleanSpec{TierPlacement: map[int64]string{base: "cold"}})
	require.NoError(t, err)
	path := l.Path
	require.NoError(t, l.Close())

	reopened, err := New(Options{
		Path:            path,
		MaxSegmentBytes: 64,
		Tiers: []Tier{
			{Name: "hot", Store: hot},
			{Name: "cold", Store: cold},
		},
		DisableAutoClean: true,
	})
	require.NoError(t, err)
	defer reopened.Close()
	reopened.SetHighWatermark(last)

	rl := reopened.(*commitLog)
	require.Equal(t, "cold", tierOf(t, rl, base))
	require.Equal(t, before, readFrom(t, rl))
}

// The source objects are reclaimed, but only after a manifest has stopped
// naming them — and never in the same pass that superseded them, because a
// reader that took the source backing before the swap is still on it.
func TestAMoveReclaimsTheSourceObjectsOnALaterPass(t *testing.T) {
	l, hot, cold, _ := chainLog(t)

	objs, err := l.TierManifest()
	require.NoError(t, err)
	base := objs[0].BaseOffset
	var sourceKeys []string
	for _, o := range objs {
		if o.BaseOffset == base {
			sourceKeys = append(sourceKeys, o.LogKey)
			if o.IndexKey != "" {
				sourceKeys = append(sourceKeys, o.IndexKey)
			}
			if o.BlocksKey != "" {
				sourceKeys = append(sourceKeys, o.BlocksKey)
			}
		}
	}
	require.NotEmpty(t, sourceKeys)

	_, err = l.CleanWithSpec(CleanSpec{TierPlacement: map[int64]string{base: "cold"}})
	require.NoError(t, err)

	// Still there: queued, not deleted.
	after, err := hot.List()
	require.NoError(t, err)
	for _, k := range sourceKeys {
		require.Contains(t, after, k, "the source object is pinned until a later pass")
	}

	// A second pass drains the queue.
	_, err = l.CleanWithSpec(CleanSpec{})
	require.NoError(t, err)
	after, err = hot.List()
	require.NoError(t, err)
	for _, k := range sourceKeys {
		require.NotContains(t, after, k, "the superseded source object is this log's garbage")
	}

	coldKeys, err := cold.List()
	require.NoError(t, err)
	require.NotEmpty(t, coldKeys, "reclaiming the source must not touch the destination")
}

// A placement naming a tier that is not configured is refused and NOTHING
// moves. A spec whose intent cannot be honoured must fail loudly rather than be
// partially applied — the same rule CleanSpec.Ceiling follows.
func TestAPlacementNamingAnUnconfiguredTierIsRefused(t *testing.T) {
	l, _, cold, _ := chainLog(t)

	objs, err := l.TierManifest()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(objs), 2, "the fixture needs more than one offloaded segment")

	_, err = l.CleanWithSpec(CleanSpec{TierPlacement: map[int64]string{
		objs[0].BaseOffset: "cold",
		objs[1].BaseOffset: "archive",
	}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "archive")

	// The valid half did not move either: the check runs before any bytes do.
	require.Equal(t, "hot", tierOf(t, l, objs[0].BaseOffset),
		"a partially applied placement is worse than a refused one")
	requireNoSegmentsIn(t, cold)
}

// A placement for a base offset the log has no offloaded segment for is
// skipped, not refused — a caller's map is computed from a snapshot and
// retention deletes segments between that look and this pass.
func TestAPlacementForASegmentThatIsNotThereIsSkipped(t *testing.T) {
	l, _, cold, _ := chainLog(t)

	objs, err := l.TierManifest()
	require.NoError(t, err)
	base := objs[0].BaseOffset

	_, err = l.CleanWithSpec(CleanSpec{TierPlacement: map[int64]string{
		base:      "cold",
		999999999: "cold", // no such segment
	}})
	require.NoError(t, err, "a stale base offset is a race the caller cannot avoid")
	require.Equal(t, "cold", tierOf(t, l, base), "the placements that do apply still apply")

	coldKeys, err := cold.List()
	require.NoError(t, err)
	require.NotEmpty(t, coldKeys)
}

// Moving into a tier this log does not own is refused before any bytes are
// copied. Finding out afterwards would have paid the entire cost of the move
// for nothing, and would have written to a store another process owns.
func TestAMoveIntoATierThisLogDoesNotOwnIsRefused(t *testing.T) {
	l, _, cold, _ := chainLog(t)
	require.NoError(t, l.SetTierReadOnly("cold", true))

	objs, err := l.TierManifest()
	require.NoError(t, err)
	base := objs[0].BaseOffset

	_, err = l.CleanWithSpec(CleanSpec{TierPlacement: map[int64]string{base: "cold"}})
	require.ErrorIs(t, err, errTierReadOnly)

	require.Equal(t, "hot", tierOf(t, l, base))
	requireNoSegmentsIn(t, cold)
}

// And moving OUT of a tier this log does not own, which is the half that is
// easy to miss: the copy into the destination would be legitimate, but the
// release is a write to the source's manifest.
func TestAMoveOutOfATierThisLogDoesNotOwnIsRefused(t *testing.T) {
	l, _, cold, _ := chainLog(t)
	require.NoError(t, l.SetTierReadOnly("hot", true))

	objs, err := l.TierManifest()
	require.NoError(t, err)
	base := objs[0].BaseOffset

	_, err = l.CleanWithSpec(CleanSpec{TierPlacement: map[int64]string{base: "cold"}})
	require.ErrorIs(t, err, errTierReadOnly)

	require.Equal(t, "hot", tierOf(t, l, base))
	requireNoSegmentsIn(t, cold)
}

// haltingStore stops accepting writes on command, so a test can stand where a
// crash would and look at what the stores say.
type haltingStore struct {
	*FileSegmentStore
	halted atomic.Bool
}

func (s *haltingStore) Put(key string, r io.Reader, size int64) error {
	if s.halted.Load() {
		return errors.New("store halted")
	}
	return s.FileSegmentStore.Put(key, r, size)
}

// A move interrupted between its commit and its release reopens, and reopens on
// the destination.
//
// This is the failure the whole MovedFrom mechanism exists for, and it is the
// one a design can most easily get wrong: the publish order has to be
// destination-then-source, because the reverse leaves a segment named by
// nothing — and that order means a crash in between leaves BOTH tiers claiming
// it, which is what the merge at open refuses. Without the marker, a routine
// background move would produce a log that will not open.
//
// The source store is halted after the destination's manifest has landed, which
// is exactly where a process dying would leave the stores.
func TestAMoveInterruptedAfterItsCommitStillOpens(t *testing.T) {
	dir := tempDir(t)
	hotFile, err := NewFileSegmentStore(filepath.Join(dir, "hot"))
	require.NoError(t, err)
	cold, err := NewFileSegmentStore(filepath.Join(dir, "cold"))
	require.NoError(t, err)
	hot := &haltingStore{FileSegmentStore: hotFile}

	tiers := []Tier{{Name: "hot", Store: hot}, {Name: "cold", Store: cold}}
	l, err := New(Options{
		Path: dir, MaxSegmentBytes: 64, Tiers: tiers, DisableAutoClean: true,
	})
	require.NoError(t, err)

	var last int64
	for i := 0; i < 24; i++ {
		offs, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("padding value")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)
	before := readFrom(t, l)
	n, err := l.OffloadBefore(last)
	require.NoError(t, err)
	require.Positive(t, n)

	objs, err := l.TierManifest()
	require.NoError(t, err)
	base := objs[0].BaseOffset

	// The copy into cold and cold's manifest both land; hot's release cannot.
	hot.halted.Store(true)
	_, err = l.CleanWithSpec(CleanSpec{TierPlacement: map[int64]string{base: "cold"}})
	require.Error(t, err, "the release did not land, and the caller has to hear about it")
	require.NoError(t, l.Close())

	// Both stores now claim the segment, which is the state a crash leaves.
	hotObjs, err := readTierManifest(hotFile)
	require.NoError(t, err)
	coldObjs, err := readTierManifest(cold)
	require.NoError(t, err)
	require.True(t, namesSegment(hotObjs, base), "the source has not let go")
	require.True(t, namesSegment(coldObjs, base), "and the destination has committed")

	reopened, err := New(Options{
		Path: dir, MaxSegmentBytes: 64,
		Tiers:            []Tier{{Name: "hot", Store: hotFile}, {Name: "cold", Store: cold}},
		DisableAutoClean: true,
	})
	require.NoError(t, err, "a crashed move must not leave a log that cannot be opened")
	defer reopened.Close()
	reopened.SetHighWatermark(last)

	rl := reopened.(*commitLog)
	require.Equal(t, "cold", tierOf(t, rl, base),
		"the destination committed, so the destination is where the segment is")
	require.Equal(t, before, readFrom(t, rl), "and every record is still readable")
}

// namesSegment reports whether a manifest claims a base offset.
func namesSegment(objs []TierObject, base int64) bool {
	for _, o := range objs {
		if o.BaseOffset == base {
			return true
		}
	}
	return false
}
