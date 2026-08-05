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
// LogKey and IndexKey out of an object in the store and openOffloadedSegment
// lands them in s.storeKey / s.indexKey. From there segment.Delete calls
// store.Delete(s.storeKey), which for FileSegmentStore is an os.Remove of the
// joined path.
//
// So a manifest naming "../../x" removes a file the store does not own. This is
// hardening rather than a live exploit — a writer that can put a manifest in the
// store can already put objects in it — but "corrupt the log you already control"
// and "delete a path outside the store directory" are different powers, and only
// the first is one the store is entitled to.
//
// The manifest is where the check belongs, and it is the only place it needs to
// be: the manifest is the sole record of an offload, so every store key a
// segment holds came through here. Refuse a key this package would never have
// minted, at the point it enters.
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
