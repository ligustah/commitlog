package commitlog

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// A store key names an object inside the store. Nothing enforced that.
//
// FileSegmentStore.objectPath joins the key onto its directory and says so in a
// comment: "Keys are log-relative segment identifiers (no separators), so the
// join stays within dir." That holds for keys this package MINTS — newStoreKeys
// emits "%020d.<rand><suffix>". It does not hold for keys that arrive from
// OUTSIDE, and the tier manifest is exactly that: readTierManifest decodes
// LogKey and IndexKey out of an object in the store, adoptTierManifestLocked
// writes them into a local offload marker, and openOffloadedSegment lands them
// in s.storeKey / s.indexKey. From there segment.Delete calls
// store.Delete(s.storeKey), which for FileSegmentStore is an os.Remove of the
// joined path.
//
// So a manifest naming "../../x" removes a file the store does not own. This is
// hardening rather than a live exploit — a writer that can put a manifest in the
// store can already put objects in it — but "corrupt the log you already control"
// and "delete a path outside the store directory" are different powers, and only
// the first is one the store is entitled to.
//
// The manifest is where the check belongs: refuse a key this package would never
// have minted, at the point it enters.
func TestATierManifestNamingAKeyOutsideTheStoreIsRefused(t *testing.T) {
	dir := tempDir(t)
	defer remove(t, dir)

	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	put := func(m tierManifest) {
		body, err := json.Marshal(m)
		require.NoError(t, err)
		require.NoError(t, store.Put(manifestKey, bytes.NewReader(body), int64(len(body))))
	}

	// A manifest this package could have written round-trips.
	logKey, indexKey := newStoreKeys(0)
	put(tierManifest{Version: manifestVersion, Segments: []TierObject{{
		BaseOffset: 0, LogKey: logKey, IndexKey: indexKey,
	}}})
	objs, err := readTierManifest(store)
	require.NoError(t, err)
	require.Len(t, objs, 1)

	for _, bad := range []string{
		"../escape",
		"a/b",
		`a\b`,
		"/etc/passwd",
		"..",
		"",
	} {
		put(tierManifest{Version: manifestVersion, Segments: []TierObject{{
			BaseOffset: 0, LogKey: bad,
		}}})
		_, err := readTierManifest(store)
		require.Errorf(t, err, "manifest LogKey %q was accepted", bad)

		// The index key is the same kind of value and reaches the same Delete.
		put(tierManifest{Version: manifestVersion, Segments: []TierObject{{
			BaseOffset: 0, LogKey: logKey, IndexKey: bad,
		}}})
		_, err = readTierManifest(store)
		if bad == "" {
			// An empty IndexKey is meaningful: the index stayed on local disk.
			require.NoError(t, err)
			continue
		}
		require.Errorf(t, err, "manifest IndexKey %q was accepted", bad)
	}
}

// The other route into s.storeKey. A manifest becomes a marker, but a marker is
// also written directly by offloadTo, and openOffloadedSegment reads it either
// way — so validating only the manifest would leave the rule true of one path
// and not the other.
//
// A marker lives in the log's own directory, so this is weaker than the manifest
// case: anyone who can write here can already delete the log's segments. It is
// checked so that "a store key names an object inside the store" holds wherever
// a store key comes from, rather than in the one place it was noticed.
func TestAnOffloadMarkerNamingAKeyOutsideTheStoreIsRefused(t *testing.T) {
	dir := tempDir(t)
	defer remove(t, dir)

	// v2 marker: JSON.
	path := filepath.Join(dir, "00000000000000000000"+offloadedSuffix)
	body, err := json.Marshal(offloadMeta{LogKey: "../escape"})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, body, 0o644))
	_, err = readOffloadMarker(path)
	require.Error(t, err, "a marker naming a path outside the store was accepted")

	// v1 marker: the raw key as the whole file. Same rule.
	require.NoError(t, os.WriteFile(path, []byte("../escape"), 0o644))
	_, err = readOffloadMarker(path)
	require.Error(t, err, "a legacy marker naming a path outside the store was accepted")

	// A key this package would have minted still round-trips, in both formats.
	logKey, indexKey := newStoreKeys(0)
	body, err = json.Marshal(offloadMeta{LogKey: logKey, IndexKey: indexKey})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, body, 0o644))
	meta, err := readOffloadMarker(path)
	require.NoError(t, err)
	require.Equal(t, logKey, meta.LogKey)

	require.NoError(t, os.WriteFile(path, []byte(logKey), 0o644))
	meta, err = readOffloadMarker(path)
	require.NoError(t, err)
	require.Equal(t, logKey, meta.LogKey)
	require.Empty(t, meta.IndexKey, "a legacy marker keeps its index local")
}

// The consequence the check above prevents, stated on its own so it cannot be
// mistaken for a theoretical concern: a key with a traversal in it removes a
// file the store never held.
func TestAFileSegmentStoreKeyCannotReachOutsideItsDirectory(t *testing.T) {
	root := tempDir(t)
	defer remove(t, root)

	outside := filepath.Join(root, "outside.txt")
	require.NoError(t, os.WriteFile(outside, []byte("not the store's"), 0o644))

	store, err := NewFileSegmentStore(filepath.Join(root, "store"))
	require.NoError(t, err)

	err = store.Delete("../outside.txt")
	require.Error(t, err, "a traversing key was accepted by the store")

	_, statErr := os.Stat(outside)
	require.NoError(t, statErr, "a file outside the store directory was deleted")
}
