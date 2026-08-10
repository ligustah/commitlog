package commitlog

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// tieredCompactedLog builds a log whose records live in a store, with compaction
// settings worth disagreeing about, and returns the store plus the last offset.
//
// The local directory is left in place; the tests below open SEPARATE directories
// over the same store, which is the shape that matters — a process that has the
// store and not the directory.
func tieredCompactedLog(t *testing.T) (*FileSegmentStore, int64) {
	t.Helper()
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	opts := Options{
		Path:                      dir,
		MaxSegmentBytes:           64,
		Tiers:                     oneTier(store),
		DisableAutoClean:          true,
		Compact:                   true,
		CompactMinAge:             time.Hour,
		CompactTombstoneRetention: 24 * time.Hour,
	}
	l, cleanup := setupWithOptions(t, opts)
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
	return store, last
}

// adoptingOpts is what a second process passes: its own empty directory, the
// same store, and whatever settings it believes the log has.
func adoptingOpts(t *testing.T, store *FileSegmentStore) Options {
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

// The descriptor lives where the log's DATA lives. For a store-backed log that
// is the store, beside the manifest — not the local directory.
//
// This is the fact the two tests below rest on, stated on its own so a failure
// says which half broke.
func TestAStoreBackedLogPublishesItsDescriptorToTheStore(t *testing.T) {
	store, _ := tieredCompactedLog(t)

	keys, err := store.List()
	require.NoError(t, err)
	require.Contains(t, keys, descriptorKey,
		"a log with a store must publish what it IS beside what it HOLDS")

	got, err := readStoreDescriptor(store)
	require.NoError(t, err)
	require.True(t, got.Compact)
	require.Equal(t, time.Hour, got.CompactMinAge)
	require.Equal(t, 24*time.Hour, got.CompactTombstoneRetention)

	// And a log with NO store still keeps it in its directory, because there is
	// no catalog to be authoritative and the directory is all there is.
	dir := tempDir(t)
	l, err := New(compactedOpts(dir))
	require.NoError(t, err)
	require.NoError(t, l.Close())
	_, err = os.Stat(filepath.Join(dir, descriptorFileName))
	require.NoError(t, err, "a log with no store keeps its descriptor local")
}

// The bug this whole change exists for.
//
// A node adopting a tier has a store full of segments and an EMPTY directory.
// Newness used to be decided by reading that directory for .log/.offloaded
// files, so the adopting node called itself new — and a new log skips the
// descriptor check entirely and records whatever it was passed. The one moment a
// process picks up a log it did not create is the one moment its retention
// settings were never compared against the log's.
//
// The consequence is not an error message. Compact and CompactTombstoneRetention
// decide what gets DELETED, and the zero values mean no protection rather than
// "disabled", so a process that adopts a compacted log while passing an empty
// config starts applying a retention policy the log was never created with — to
// data it did not write.
func TestAdoptingATierIsCheckedAgainstTheLogsOwnSettings(t *testing.T) {
	store, _ := tieredCompactedLog(t)

	for name, tune := range map[string]func(*Options){
		"an empty config": func(o *Options) {
			o.Compact = false
			o.CompactMinAge = 0
			o.CompactTombstoneRetention = 0
		},
		"compaction off":      func(o *Options) { o.Compact = false },
		"a shorter min age":   func(o *Options) { o.CompactMinAge = time.Minute },
		"a shorter tombstone": func(o *Options) { o.CompactTombstoneRetention = time.Minute },
	} {
		t.Run(name, func(t *testing.T) {
			opts := adoptingOpts(t, store)
			tune(&opts)
			_, err := New(opts)
			require.ErrorIs(t, err, ErrDescriptorMismatch,
				"a node adopting this tier was never checked against it")
		})
	}
}

// The other half, and the reason the first attempt at this was reverted: making
// the adopting node "not new" must not turn adoption itself into a refusal.
//
// It nearly does. An adopting node has no local descriptor either, so a rule
// that says "not new" while still reading the descriptor from the DIRECTORY
// sends every adoption into the has-no-descriptor refusal — correct-looking, and
// wrong for exactly the case the change was meant to fix. The store has to be
// both the thing that answers "does this log exist" and the thing that answers
// "what is it", or the two disagree.
func TestAdoptingATierWithMatchingSettingsOpens(t *testing.T) {
	store, last := tieredCompactedLog(t)

	l, err := New(adoptingOpts(t, store))
	require.NoError(t, err, "a node whose settings AGREE must be able to adopt the tier")
	defer l.Close() // nolint: errcheck

	// And it really adopted it, rather than opening an empty log that happens
	// not to have errored.
	adopted, err := l.TierManifest()
	require.NoError(t, err)
	require.NotEmpty(t, adopted, "the store must describe itself to a log that has never written to it")

	l.SetHighWatermark(last)
	require.LessOrEqual(t, l.OldestOffset(), last)
}

// A follower does not own the tier and does not write to it — and a descriptor
// is a claim about the log, not part of it, so declining to publish one is right
// where declining to write segment data would not be.
//
// The check still applies to it. Being read-only is not permission to disagree.
func TestAReadOnlyTierIsCheckedButPublishesNothing(t *testing.T) {
	store, _ := tieredCompactedLog(t)

	before, err := store.List()
	require.NoError(t, err)

	opts := adoptingOpts(t, store)
	opts.Tiers[0].ReadOnly = true
	opts.CompactMinAge = time.Minute
	_, err = New(opts)
	require.ErrorIs(t, err, ErrDescriptorMismatch,
		"read-only is about writes, not about being exempt from the check")

	opts = adoptingOpts(t, store)
	opts.Tiers[0].ReadOnly = true
	l, err := New(opts)
	require.NoError(t, err)
	require.NoError(t, l.Close())

	after, err := store.List()
	require.NoError(t, err)
	require.ElementsMatch(t, before, after,
		"a read-only tier must not have written to the store")
}

// Garbage collection is judged by what REFERENCES an object, and nothing
// references the descriptor — it is what the store says about itself, not part
// of what it holds. So a rule built only from references collects it, and the
// log then refuses its own next open: a log that exists with no descriptor is a
// refusal, by design.
//
// The manifest has always been exempt for the same reason. The descriptor is the
// second object of that kind, and the first one added since the rule was written.
func TestTheDescriptorIsNeverGarbage(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	opts := Options{
		Path:                      dir,
		MaxSegmentBytes:           64,
		Tiers:                     oneTier(store),
		DisableAutoClean:          true,
		Compact:                   true,
		CompactMinAge:             time.Hour,
		CompactTombstoneRetention: 24 * time.Hour,
	}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()

	var last int64
	for i := 0; i < 24; i++ {
		offs, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("padding value")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)
	n, err := l.OffloadBefore(last)
	require.NoError(t, err)
	require.Positive(t, n)

	garbage, err := l.UnreferencedObjects()
	require.NoError(t, err)
	require.NotContains(t, orphanKeys(garbage), descriptorKey,
		"the descriptor was collected as garbage")
	require.NotContains(t, orphanKeys(garbage), manifestKey)

	// Run the collection the caller is told to run, then prove the log still
	// opens — which is the consequence, not the list.
	deleted, err := l.DeleteStoreObjects(garbage)
	require.NoError(t, err)
	require.Len(t, deleted, len(garbage))
	require.NoError(t, l.Close())

	reopened, err := New(opts)
	require.NoError(t, err, "collecting garbage must not cost the log its identity")
	require.NoError(t, reopened.Close())
}

// A read-only process over a store that has no descriptor has nothing to be
// checked against and no right to publish one. It opens rather than refusing,
// because refusing would make a follower depend on its leader having reached a
// particular line of code first.
func TestAReadOnlyTierOpensAStoreWithNoDescriptor(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	opts := adoptingOpts(t, store)
	opts.Tiers[0].ReadOnly = true
	l, err := New(opts)
	require.NoError(t, err)
	require.NoError(t, l.Close())

	keys, err := store.List()
	require.NoError(t, err)
	require.NotContains(t, keys, descriptorKey)
}
