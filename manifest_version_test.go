package commitlog

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// The layout that existed before TierObject.Records is refused, not adapted.
//
// This is the bump the field needed and did not get. Records is the count a
// cold tiered segment answers from — messageCountLocked's third arm reads the
// manifest entry and nothing else, because an index-offloaded segment has no
// local index and no resident block table. An absent JSON field decodes to 0,
// so reading a pre-Records manifest at face value would have every offloaded
// segment report holding nothing, and applyTotalLimit sums exactly those
// numbers against MaxLogMessages. A running total that never climbs never
// reaches the ceiling, which switches the limit off over the tier without
// anything going wrong out loud.
//
// That is the same defect as the over-count this field was added to fix — a
// term in a budget that is not measuring what the budget bounds — pointed the
// other way, and the other way is the one nothing notices: over-deleting loses
// records a caller asked to keep, under-deleting just quietly keeps paying for
// a tier that was supposed to be capped.
//
// The version is written out as a LITERAL rather than manifestVersion-1. The
// claim is about a specific layout — the one with no Records in it — and a
// relative expression would follow the constant forward and stop being about
// that layout at the next bump, which is how a version test ages into asserting
// only that some number is refused.
func TestTheManifestLayoutBeforeRecordsIsRefused(t *testing.T) {
	dir := tempDir(t)
	defer remove(t, dir)

	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	logKey, _, _ := newStoreKeys(0)
	// Valid in every other respect: it names its tier, its key is one this
	// package would mint, and its offsets are sound. The version is the only
	// thing wrong with it, so a reader that accepted it would be accepting it
	// for the entries' sake — which is precisely the adaptation being refused.
	body, err := json.Marshal(tierManifest{
		Version: 3,
		Segments: []TierObject{{
			BaseOffset:  0,
			Tier:        defaultTierName,
			LogKey:      logKey,
			FirstOffset: 0,
			LastOffset:  9,
		}},
	})
	require.NoError(t, err)
	require.NoError(t, store.Put(manifestKey, bytes.NewReader(body), int64(len(body))))

	_, err = readTierManifest(store)
	require.Error(t, err,
		"a manifest written before Records existed was accepted; every segment in it "+
			"would report holding zero records to the MaxLogMessages budget")
	require.Contains(t, err.Error(), "version 3",
		"the error must name the version found")
}

// And the count survives the manifest, which is the whole reason the version
// moved.
//
// Stated separately because the refusal above passes just as well against a
// build that dropped Records from the wire entirely: refusing the old layout
// and never writing the new field would satisfy it. This is the half that says
// the round trip carries the number.
func TestATierManifestCarriesTheRecordCount(t *testing.T) {
	dir := tempDir(t)
	defer remove(t, dir)

	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	logKey, _, _ := newStoreKeys(0)
	body, err := json.Marshal(tierManifest{
		Version: manifestVersion,
		Segments: []TierObject{{
			BaseOffset:  0,
			Tier:        defaultTierName,
			LogKey:      logKey,
			FirstOffset: 0,
			LastOffset:  99,
			Records:     42,
		}},
	})
	require.NoError(t, err)
	require.NoError(t, store.Put(manifestKey, bytes.NewReader(body), int64(len(body))))

	objs, err := readTierManifest(store)
	require.NoError(t, err)
	require.Len(t, objs, 1)
	require.EqualValues(t, 42, objs[0].Records,
		"the manifest lost the record count; the span between the offsets is 100, "+
			"which is the answer this field exists to stop being given")
}
