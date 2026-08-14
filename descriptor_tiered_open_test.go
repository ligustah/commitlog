package commitlog

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// A log whose bytes are in a store cannot be opened without one, and
// AdoptOptions does not change that.
//
// The refusal itself is old; what is new is that it is STATED. It used to be a
// side effect of a tiered log writing its descriptor only to its tiers, so
// opening the directory alone found no descriptor and refused for that reason
// instead. Writing the local copy — which is what makes InspectIdentity work on
// a broker's copy of a tiered partition — took the refusal away with it, and
// nothing went red until a full suite ran, because no line of code anywhere
// said what the rule was.
//
// The AdoptOptions half is the part that is strictly stronger than what was
// lost. The old refusal came from the missing-descriptor branch, which
// AdoptOptions is explicitly allowed through — so a caller that adopts on every
// open, as durable_streams does because its settings come from a catalog rather
// than a config file, never had this protection at all. Adopting is a statement
// about POLICY: "I know what this log is, record it." Where the bytes are is
// not policy, and adopting does not relocate them.
func TestATieredLogRefusesToOpenWithoutItsStore(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	logDir := filepath.Join(dir, "log")
	opts := Options{
		Name: "tiered", Path: logDir, MaxSegmentBytes: 512,
		Tiers: oneTier(store),
	}
	l, err := New(opts)
	require.NoError(t, err)
	for range 30 {
		appendMsg(t, l, "padding-value-to-roll-segments-zzzzzzzzzzzzzzz")
	}
	n, err := l.OffloadBefore(l.NewestOffset())
	require.NoError(t, err)
	require.Positive(t, n, "nothing was offloaded, so the directory still holds the whole log")
	require.NoError(t, l.Close())

	for name, adopt := range map[string]bool{
		"plain":         false,
		"with adoption": true,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := New(Options{
				Name: "tiered", Path: logDir, MaxSegmentBytes: 512,
				AdoptOptions: adopt,
			})
			require.Error(t, err,
				"a log with segments in a store opened without one; its local segments are "+
					"the TAIL, so this presents a truncated log as a complete one")
			require.ErrorIs(t, err, ErrDescriptorMismatch)
			require.Contains(t, err.Error(), "no Tiers were supplied",
				"the error must say what is missing, not just that something disagrees")
		})
	}
}

// The other direction still opens, which is what keeps the refusal from being a
// blanket one.
//
// A log that lives entirely in its directory, opened as it always was: nothing
// about it is store-backed and nothing should ask for a store. Stated because
// the cheap way to write the check above — refuse whenever the descriptor and
// the options disagree about tiers — would have caught this too, and a caller
// whose plain logs stopped opening would find that out in production.
func TestAPlainLogStillOpensWithNoStore(t *testing.T) {
	dir := tempDir(t)
	opts := Options{Name: "plain", Path: filepath.Join(dir, "log"), MaxSegmentBytes: 512}

	l, err := New(opts)
	require.NoError(t, err)
	appendMsg(t, l, "a record")
	require.NoError(t, l.Close())

	l2, err := New(opts)
	require.NoError(t, err, "a log that was never tiered must reopen exactly as before")
	require.NoError(t, l2.Close())

	// And the descriptor says so rather than being silent about it: an absent
	// answer is what the version bump exists to stop being interpreted.
	d, err := readDescriptor(opts.Path)
	require.NoError(t, err)
	require.False(t, d.Tiered)
}

// Adopting a plain log INTO a tier is not refused.
//
// This is the direction the check is deliberately blind to, and it has to stay
// that way: attaching a store to an existing local log is how a log becomes
// tiered in the first place. It reaches a different branch entirely —
// loadDescriptor reads the nearest TIER when Tiers are set, never the local
// file — so the stored `tiered=false` is not even consulted. Asserted anyway,
// because "unreachable" is a claim about code that changes.
func TestAPlainLogCanBeAdoptedIntoATier(t *testing.T) {
	dir := tempDir(t)
	logDir := filepath.Join(dir, "log")

	l, err := New(Options{Name: "adopted", Path: logDir, MaxSegmentBytes: 512})
	require.NoError(t, err)
	appendMsg(t, l, "a record")
	require.NoError(t, l.Close())

	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)
	l2, err := New(Options{
		Name: "adopted", Path: logDir, MaxSegmentBytes: 512,
		Tiers: oneTier(store), AdoptOptions: true,
	})
	require.NoError(t, err, "attaching a store to a local log is how a log becomes tiered")
	require.NoError(t, l2.Close())

	// The tier is the authority, and it now says the log is store-backed.
	stored, err := readStoreDescriptor(store)
	require.NoError(t, err)
	require.True(t, stored.Tiered)
}

// A descriptor written before the field existed is refused, not read as
// "not tiered".
//
// The version line is the whole reason that is possible. Without the bump, a v1
// file — which every tiered log's STORE copy was, before this — would decode
// with Tiered false and open a store-backed log as a complete local one, which
// is the exact failure the field was added to prevent. Refusing on the version
// is what turns "this predates the field" into a fact rather than a default.
func TestADescriptorFromBeforeTheTieredFieldIsRefused(t *testing.T) {
	// Rendered by hand at the OLD version, since this build cannot write one.
	// The body is otherwise a descriptor this package would have produced.
	const v1 = "1\n" +
		"compact=false\n" +
		"compact_min_age=0s\n" +
		"compact_tombstone_retention=0s\n" +
		"compression=none\n" +
		"max_segment_bytes=512\n"

	_, err := parseDescriptor(strings.NewReader(v1))
	require.Error(t, err, "a descriptor with no tiered line was read as a log with no store")
	require.Contains(t, errors.Cause(err).Error(), "unsupported descriptor version 1")
}
