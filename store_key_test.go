package commitlog

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Every upload gets its own key. This is the property the whole tier design
// rests on, and it is asserted directly rather than inferred from a counter,
// because the counter is exactly what it replaced: a generation is read from
// local state, and local state is what a crash, a restart or a second process
// leaves stale.
func TestStoreKeysAreUniquePerUpload(t *testing.T) {
	const n = 500
	seen := make(map[string]bool, 2*n)
	for i := 0; i < n; i++ {
		logKey, idxKey := newStoreKeys(42)

		require.False(t, seen[logKey], "log key reused: %s", logKey)
		require.False(t, seen[idxKey], "index key reused: %s", idxKey)
		seen[logKey], seen[idxKey] = true, true

		// The log and index of one upload are distinct objects but share the
		// upload's identity, so a marker naming one implies the other.
		require.NotEqual(t, logKey, idxKey)
		require.Equal(t,
			strings.TrimSuffix(logKey, logSuffix),
			strings.TrimSuffix(idxKey, indexSuffix),
			"a single upload's objects must share a stem")
	}
	require.Len(t, seen, 2*n)
}

// Keys stay grouped and sorted by base offset, which is what lets a store
// listing be read by a human and lets objects for one segment be found
// together.
func TestStoreKeysLeadWithTheBaseOffset(t *testing.T) {
	logKey, idxKey := newStoreKeys(42)
	require.True(t, strings.HasPrefix(logKey, "00000000000000000042."), logKey)
	require.True(t, strings.HasPrefix(idxKey, "00000000000000000042."), idxKey)
	require.True(t, strings.HasSuffix(logKey, logSuffix))
	require.True(t, strings.HasSuffix(idxKey, indexSuffix))

	// Zero-padded, so lexical order is offset order — a listing of a large log
	// is otherwise interleaved nonsense.
	low, _ := newStoreKeys(9)
	high, _ := newStoreKeys(10)
	require.Less(t, low, high)
}

// A retry after an ambiguous failure must not address the object the first
// attempt may still be writing. This is the case a deterministic key cannot
// handle: the original request may be in flight, and nothing can tell.
func TestRetryingAnUploadUsesADifferentKey(t *testing.T) {
	first, firstIdx := newStoreKeys(7)
	retry, retryIdx := newStoreKeys(7)

	require.NotEqual(t, first, retry,
		"a retry must not race the attempt it is retrying")
	require.NotEqual(t, firstIdx, retryIdx)
}

// A rewrite lands on a new object and leaves the old one for the caller to
// remove, so a reader that opened the segment first is never disturbed.
func TestRewriteDoesNotReuseTheKeyBeingRead(t *testing.T) {
	l, store, seg := offloadedFixture(t, nil)

	seg.RLock()
	before := seg.storeKey
	seg.RUnlock()

	fresh := freshLocalSegment(t, l, seg)
	superseded := replaceOffloaded(t, seg, fresh)

	seg.RLock()
	after := seg.storeKey
	seg.RUnlock()

	require.NotEqual(t, before, after, "the rewrite must not land on the live key")
	require.Len(t, superseded, 1)
	require.Equal(t, before, superseded[0].key)

	// Both objects exist: the old one is removed by a later pass, once no reader
	// can still be on it.
	keys, err := store.List()
	require.NoError(t, err)
	require.Contains(t, keys, before)
	require.Contains(t, keys, after)
}
