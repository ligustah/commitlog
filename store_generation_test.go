package commitlog

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Generation 0 keeps the original un-suffixed key, and every later generation
// gets a distinct one. The first half is a compatibility requirement — objects
// uploaded before generations existed carry those keys — and the second is the
// whole point: a rewrite must not be able to land on the key a reader is
// already reading.
func TestSegmentStoreKeyGenerations(t *testing.T) {
	require.Equal(t, "00000000000000000042.log", segmentStoreKey(42, 0, ""))
	require.Equal(t, "00000000000000000042.index", segmentIndexStoreKey(42, 0, ""))

	seen := map[string]bool{}
	for gen := 0; gen < 8; gen++ {
		k := segmentStoreKey(42, gen, "")
		require.False(t, seen[k], "generation %d reused key %s", gen, k)
		seen[k] = true

		ik := segmentIndexStoreKey(42, gen, "")
		require.False(t, seen[ik], "generation %d reused index key %s", gen, ik)
		seen[ik] = true
	}

	// Distinct base offsets must not collide with a generation of another.
	require.NotEqual(t, segmentStoreKey(42, 1, ""), segmentStoreKey(43, 0, ""))
}

// A marker written before generations existed has no generation field. It must
// read back as generation 0 — the generation its keys already encode — rather
// than failing to parse or defaulting to something that would send a reader at
// a key that does not exist.
func TestOffloadMarkerWithoutGenerationReadsAsZero(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "0"+offloadedSuffix)

	legacy := `{"log_key":"00000000000000000000.log","first_offset":0,` +
		`"last_offset":9,"position":100,"phys_position":100}`
	require.NoError(t, os.WriteFile(path, []byte(legacy), 0666))

	meta, err := readOffloadMarker(path)
	require.NoError(t, err)
	require.Equal(t, 0, meta.Generation)
	require.Equal(t, "00000000000000000000.log", meta.LogKey)
	require.Equal(t, segmentStoreKey(0, meta.Generation, ""), meta.LogKey,
		"a legacy marker's key must be exactly the generation-0 key")
}

// A v1 marker is the raw log key rather than JSON. It must keep working, and
// report generation 0.
func TestOffloadMarkerV1ReadsAsZero(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "0"+offloadedSuffix)
	require.NoError(t, os.WriteFile(path, []byte("00000000000000000000.log"), 0666))

	meta, err := readOffloadMarker(path)
	require.NoError(t, err)
	require.Equal(t, 0, meta.Generation)
	require.Equal(t, "00000000000000000000.log", meta.LogKey)
	require.Empty(t, meta.IndexKey, "a v1 marker keeps its index local")
}

// The generation round-trips through the marker, and is omitted entirely at 0
// so markers written now stay byte-compatible with what an older reader
// expects.
func TestOffloadMarkerGenerationRoundTrips(t *testing.T) {
	encoded, err := json.Marshal(offloadMeta{
		LogKey: segmentStoreKey(7, 3, ""), Generation: 3, LastOffset: 9,
	})
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"generation":3`)

	var back offloadMeta
	require.NoError(t, json.Unmarshal(encoded, &back))
	require.Equal(t, 3, back.Generation)
	require.Equal(t, segmentStoreKey(7, 3, ""), back.LogKey)

	zero, err := json.Marshal(offloadMeta{LogKey: segmentStoreKey(7, 0, "")})
	require.NoError(t, err)
	require.NotContains(t, string(zero), "generation",
		"generation 0 must not appear, so markers stay as they were")
}

// An offloaded segment records the generation it opened, so a rewrite can
// allocate the next one and a reader can tell which objects it is holding.
func TestOffloadedSegmentCarriesItsGeneration(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	l, cleanup := setupWithOptions(t, Options{
		Path:            dir,
		MaxSegmentBytes: 64, // roll, so there are sealed segments to offload
		SegmentStore:    store,
	})
	defer cleanup()

	var last int64
	for i := 0; i < 20; i++ {
		offs, err := l.Append([]*Message{{Value: []byte("padding value")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)

	n, err := l.OffloadBefore(last)
	require.NoError(t, err)
	require.Positive(t, n, "some sealed segment must have offloaded")

	l.mu.RLock()
	defer l.mu.RUnlock()
	offloaded := 0
	for _, s := range l.segments {
		s.RLock()
		isOff, gen, key := s.store != nil, s.storeGen, s.storeKey
		s.RUnlock()
		if !isOff {
			continue
		}
		offloaded++
		require.Equal(t, 0, gen, "a first offload is generation 0")
		require.Equal(t, segmentStoreKey(s.BaseOffset, gen, ""), key,
			"the segment's key must be the one its generation names")
	}
	require.Positive(t, offloaded)
}
