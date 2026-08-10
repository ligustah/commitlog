package commitlog

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ligustah/commitlog/compress"
)

// blockTieredLog is the tiered fixture with block compression on, so the store
// holds block tables as well as logs and indexes. tieredLog writes raw segments
// and therefore has no BlocksKey anywhere — which is why nothing had ever asked
// what the orphan sweep thinks of one.
func blockTieredLog(t *testing.T) (*commitLog, *FileSegmentStore, int64) {
	t.Helper()
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	l, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  1 << 12,
		Compression:      compress.Snappy,
		SegmentStore:     store,
		DisableAutoClean: true,
	})
	t.Cleanup(cleanup)

	var last int64
	for i := 0; i < 400; i++ {
		offs, err := l.Append([]*Message{{Value: []byte("padding value for the block")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)

	n, err := l.OffloadBefore(last)
	require.NoError(t, err)
	require.Positive(t, n, "the fixture needs offloaded segments")
	return l, store, last
}

// A block table the manifest names is not garbage.
//
// UnreferencedObjects builds its live set from the manifest's LogKey and
// IndexKey and from each segment's storeKey and indexKey. A block-compressed
// offloaded segment has a THIRD object — its block table, under BlocksKey — and
// nothing named it. So the sweep reported every live block table as
// unreferenced, and the documented use of this call is to hand its result
// straight to DeleteStoreObjects.
//
// Deleting one does not corrupt anything; it removes the only copy of the map
// from logical offsets to compressed blocks. The local table went with the
// local file at offload (deliberately: it describes bytes that no longer
// exist), so the segment cannot rebuild it, and every read of that segment
// fails at "size block table". The bytes are intact and unreachable.
func TestABlockTableIsNotGarbage(t *testing.T) {
	l, _, _ := blockTieredLog(t)

	manifest, err := l.TierManifest()
	require.NoError(t, err)

	var tables []string
	for _, o := range manifest {
		if o.BlocksKey != "" {
			tables = append(tables, o.BlocksKey)
		}
	}
	require.NotEmpty(t, tables,
		"the fixture offloaded no block-compressed segment, so this proves nothing")

	orphans, err := l.UnreferencedObjects()
	require.NoError(t, err)
	for _, key := range tables {
		require.NotContains(t, orphans, key,
			"the block table of a segment the manifest names was reported as garbage")
	}
}

// The same object, kept alive by the other half of the live set.
//
// A rewrite installs fresh objects and publishes the manifest afterwards, so in
// between there is a block table this log is reading that no manifest names.
// That window is what the segment loop exists for, and it covered storeKey and
// indexKey but not blocksKey — so the two halves of the live set had the same
// hole and neither could cover for the other.
//
// Staged by publishing a manifest that omits the segment rather than by racing
// a rewrite: the state under test is "the log holds keys the manifest does not
// name", and that is reachable directly.
func TestABlockTableTheLogIsReadingIsNotGarbage(t *testing.T) {
	l, store, _ := blockTieredLog(t)

	manifest, err := l.TierManifest()
	require.NoError(t, err)
	require.NotEmpty(t, manifest)

	var dropped TierObject
	kept := make([]TierObject, 0, len(manifest))
	for _, o := range manifest {
		if o.BlocksKey != "" && dropped.BlocksKey == "" {
			dropped = o
			continue
		}
		kept = append(kept, o)
	}
	require.NotEmpty(t, dropped.BlocksKey, "the fixture offloaded no block table")

	body, err := json.Marshal(tierManifest{Version: manifestVersion, Segments: kept})
	require.NoError(t, err)
	require.NoError(t, store.Put(manifestKey, bytes.NewReader(body), int64(len(body))))

	orphans, err := l.UnreferencedObjects()
	require.NoError(t, err)
	require.NotContains(t, orphans, dropped.BlocksKey,
		"a block table the log is reading was reported as garbage because no "+
			"manifest names it yet")
	// Its siblings prove the fixture reached the segment loop at all rather than
	// the manifest still naming the object by another route.
	require.NotContains(t, orphans, dropped.LogKey)
	require.NotContains(t, orphans, dropped.IndexKey)
}
