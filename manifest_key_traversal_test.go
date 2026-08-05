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

	path := filepath.Join(dir, "00000000000000000000"+offloadedSuffix)
	body, err := json.Marshal(offloadMeta{LogKey: "../escape"})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, body, 0o644))
	_, err = readOffloadMarker(path)
	require.Error(t, err, "a marker naming a path outside the store was accepted")

	// The index key is the second way out, and it is optional — empty means the
	// index stayed local — so it needs its own case rather than riding along.
	body, err = json.Marshal(offloadMeta{LogKey: "log", IndexKey: "../escape"})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, body, 0o644))
	_, err = readOffloadMarker(path)
	require.Error(t, err, "a marker whose INDEX key left the store was accepted")

	// A key this package would have minted still round-trips.
	logKey, indexKey := newStoreKeys(0)
	body, err = json.Marshal(offloadMeta{LogKey: logKey, IndexKey: indexKey})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, body, 0o644))
	meta, err := readOffloadMarker(path)
	require.NoError(t, err)
	require.Equal(t, logKey, meta.LogKey)
	require.Equal(t, indexKey, meta.IndexKey)
}

// The marker used to have two layouts: JSON, or the bare log key. Which one you
// got was decided by whether the first byte was '{' — so every file that was not
// JSON took the second branch, including files that were not markers at all.
//
// Nothing has written the bare layout since the marker started carrying the
// segment's boundaries, so the branch existed only to read data no one has. What
// it actually did was launder corruption: a truncated write, a half-flushed
// file, a marker filled with NULs — all of them satisfy "does not start with
// '{'", so all of them opened as a segment whose store key was the garbage.
//
// This is the same shape as the epoch checkpoint's signed parse: the parse is
// the only integrity check the file gets, so a parse that accepts anything is no
// check at all. One layout, and a marker that is not it is an error.
func TestAMarkerThatIsNotJSONIsRefused(t *testing.T) {
	dir := tempDir(t)
	defer remove(t, dir)

	path := filepath.Join(dir, "00000000000000000000"+offloadedSuffix)
	logKey, _ := newStoreKeys(0)

	// Each of these was previously accepted as a log key, silently.
	for name, body := range map[string][]byte{
		"the bare log key":  []byte(logKey),
		"empty":             {},
		"NUL-filled":        make([]byte, 64),
		"truncated JSON":    []byte(`{"log_key":"` + logKey),
		"unrelated content": []byte("not a marker at all\n"),
	} {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, os.WriteFile(path, body, 0o644))
			_, err := readOffloadMarker(path)
			require.Error(t, err, "a marker that is not JSON was read as a store key")
		})
	}
}

// The manifest's version field is the only integrity check the manifest gets,
// and until now it checked in one direction: refuse anything NEWER than this
// build writes.
//
// One version has ever existed, so that reads as complete. It is not. An absent
// version field decodes to 0, and 0 is not newer than 1, so any JSON object
// that parsed was accepted as a manifest — including one with no segments at
// all, which is indistinguishable from "the tier is empty" and would quietly
// unpublish every offloaded segment the store holds.
//
// The rule that matches what the field is FOR is equality.
func TestAManifestThatIsNotThisVersionIsRefused(t *testing.T) {
	dir := tempDir(t)
	defer remove(t, dir)

	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	put := func(v any) {
		body, err := json.Marshal(v)
		require.NoError(t, err)
		require.NoError(t, store.Put(manifestKey, bytes.NewReader(body), int64(len(body))))
	}

	logKey, _ := newStoreKeys(0)
	segs := []TierObject{{BaseOffset: 0, LogKey: logKey}}

	// The version this build writes is the one it reads.
	put(tierManifest{Version: manifestVersion, Segments: segs})
	objs, err := readTierManifest(store)
	require.NoError(t, err)
	require.Len(t, objs, 1)

	// A manifest with no version field at all — which is what decodes to 0.
	noVersion := struct {
		Segments []TierObject
	}{Segments: segs}

	for name, body := range map[string]any{
		"no version field": noVersion,
		"version 0":        tierManifest{Version: 0, Segments: segs},
		"a newer version":  tierManifest{Version: manifestVersion + 1, Segments: segs},
		"an empty object":  struct{}{},
	} {
		t.Run(name, func(t *testing.T) {
			put(body)
			_, err := readTierManifest(store)
			require.Error(t, err, "a manifest this build cannot vouch for was accepted")
		})
	}
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
