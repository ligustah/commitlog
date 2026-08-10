package commitlog

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// flakyStore answers one key with a TRANSIENT failure — the shape of a timeout
// or a refused connection, not of an absence. Everything else it forwards.
//
// The distinction is the whole point. A store is allowed to say "there is no
// object here" and the log acts on that answer; it is not allowed to say
// "something went wrong" and have the log hear the same thing.
type flakyStore struct {
	*FileSegmentStore
	failKey string
	failing atomic.Bool
}

func (s *flakyStore) Size(key string) (int64, error) {
	if key == s.failKey && s.failing.Load() {
		return 0, errors.New("store unreachable")
	}
	return s.FileSegmentStore.Size(key)
}

func flakyOpts(t *testing.T, store *flakyStore) Options {
	t.Helper()
	return Options{
		Path:                      tempDir(t),
		MaxSegmentBytes:           64,
		Tiers:                     oneTier(store),
		DisableAutoClean:          true,
		Compact:                   true,
		CompactMinAge:             time.Hour,
		CompactTombstoneRetention: 24 * time.Hour,
	}
}

// A store that cannot answer has not answered "no".
//
// Absence used to be inferred from Size failing, for want of an exists() on the
// interface. Both places that did it act on absence in a way that is only safe
// when the absence is real, so the inference turned a bad minute for the store
// into a permanent, silent wrong answer.
func TestAStoreThatCannotAnswerIsNotANewLog(t *testing.T) {
	backing, _ := tieredCompactedLog(t)
	store := &flakyStore{FileSegmentStore: backing, failKey: descriptorKey}
	store.failing.Store(true)

	// A log is NEW when the store has no descriptor, and a new log records the
	// settings it was handed without checking them against anything. Reaching
	// that state through a failed read is the bug v0.52.0 fixed, arriving by a
	// different door.
	isNew, err := logIsNew(flakyOpts(t, store))
	require.NoError(t, err)
	require.False(t, isNew,
		"a store that failed to answer must not be read as a store with no descriptor")

	_, err = readStoreDescriptor(store)
	require.Error(t, err)
	require.NotErrorIs(t, err, os.ErrNotExist,
		"a transient failure must not be reported as absence")

	// And the open refuses rather than adopting the tier on the caller's word.
	_, err = New(flakyOpts(t, store))
	require.Error(t, err)
}

// The same inference, in the manifest reader, with a worse ending: an empty
// tier is not a harmless answer. It means this log holds no offloaded segments,
// so the next publish rebuilds the manifest without them and every object they
// named becomes garbage.
func TestAStoreThatCannotAnswerIsNotAnEmptyTier(t *testing.T) {
	backing, _ := tieredCompactedLog(t)
	store := &flakyStore{FileSegmentStore: backing, failKey: manifestKey}
	store.failing.Store(true)

	objs, err := readTierManifest(store)
	require.Error(t, err, "a store that failed to answer must not read as an empty tier")
	require.Nil(t, objs)

	_, err = New(flakyOpts(t, store))
	require.Error(t, err, "a log must not open over a tier it could not read")
}

// UnreferencedObjects already refuses to build a garbage list without the
// manifest — the reasoning is written beside the code. That refusal was
// unreachable, because the manifest reader turned the failure into a
// legitimate-looking empty manifest before it ever got there.
//
// Named for the consequence rather than the branch: what this protects is the
// collect-then-DeleteStoreObjects cycle the API documents.
func TestAStoreThatCannotAnswerYieldsNoGarbageList(t *testing.T) {
	backing, last := tieredCompactedLog(t)
	store := &flakyStore{FileSegmentStore: backing, failKey: manifestKey}

	l, cleanup := setupWithOptions(t, flakyOpts(t, store))
	t.Cleanup(cleanup)
	l.SetHighWatermark(last)

	// Healthy first: the objects the tier holds are not garbage.
	garbage, err := l.UnreferencedObjects()
	require.NoError(t, err)
	require.Empty(t, garbage, "the fixture's objects are all live")

	// Now the store stops answering. The honest result is an error, not a list
	// naming every object the tier depends on.
	store.failing.Store(true)
	garbage, err = l.UnreferencedObjects()
	require.Error(t, err)
	require.Empty(t, garbage)
}
