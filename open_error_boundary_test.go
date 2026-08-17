package commitlog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Every way New refuses the caller's Options carries ErrInvalidOptions.
//
// The point is not any single refusal — each of these was already an error, and
// each already said something useful. The point is the BOUNDARY. A caller that
// opens on a retry loop has to sort New's failures into "retrying might help"
// and "retrying is wasted", and the only thing it can sort on is the sentinel.
// So the rule has to hold for all of them or for none of them: seven refusals
// with no sentinel are seven cases where a correct classifier gives the wrong
// answer, and which way it is wrong depends on which default the caller picked.
//
// Both defaults were observed in real callers. One treats an unrecognised error
// as transient, and spins on an empty Path until its budget runs out. The other
// treats it as permanent, and gives up on a disk that was briefly busy. Neither
// is a bug in the caller.
//
// The cases below are every refusal reachable before New touches the
// filesystem — hack/openerrors.sh is what stops an eighth being added without
// one, since a test can only check the cases someone remembered to write.
func TestEveryOptionsRefusalFromNewIsPermanent(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	for name, opts := range map[string]Options{
		"no path": {},
		"unknown codec": {
			Path: tempDir(t), Compression: 9,
		},
		"negative option": {
			Path: tempDir(t), MaxSegmentBytes: -1,
		},
		"tier with no name": {
			Path: tempDir(t), Tiers: []Tier{{Store: store}},
		},
		"tier with no store": {
			Path: tempDir(t), Tiers: []Tier{{Name: "hot"}},
		},
		"two tiers with one name": {
			Path: tempDir(t), Tiers: []Tier{{Name: "hot", Store: store}, {Name: "hot", Store: store}},
		},
		"tier with a negative limit": {
			Path: tempDir(t), Tiers: []Tier{{Name: "hot", Store: store, MaxBytes: -1}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			l, err := New(opts)
			if l != nil {
				t.Cleanup(func() { _ = l.Close() })
			}
			require.Error(t, err)
			require.ErrorIs(t, err, ErrInvalidOptions,
				"a retrying caller cannot tell this from a busy disk")
		})
	}
}

// ErrLogLocked is the one sentinel from New that does NOT mean stop, so it must
// not acquire ErrInvalidOptions on the way out.
//
// Asserted because the tidy direction is the wrong one. "Every sentinel New
// returns means permanent" is a simpler rule than the one that actually holds,
// and a lock held by a process that is about to exit is precisely the condition
// a retry loop exists for. Nothing else in the package would notice the
// difference: the error still names the directory, still fails the open, and
// still reads correctly to a human.
func TestTheLockedDirectoryRefusalIsNotAnOptionsRefusal(t *testing.T) {
	dir := tempDir(t)
	first, err := New(Options{Path: dir})
	require.NoError(t, err)
	defer first.Close() // nolint: errcheck

	second, err := New(Options{Path: dir})
	if second != nil {
		t.Cleanup(func() { _ = second.Close() })
	}
	require.ErrorIs(t, err, ErrLogLocked)
	require.NotErrorIs(t, err, ErrInvalidOptions,
		"the Options are fine — the directory is busy, which is the one condition "+
			"at open that clears on its own")
}

// And the other side of the boundary: an environment failure carries no
// commitlog sentinel at all, so "unrecognised" is a meaningful class rather
// than a hole the Options refusals also fall into.
//
// A path that exists as a FILE is the cheapest way to make the directory work
// fail for a reason that is nothing to do with the caller's values.
func TestAnEnvironmentFailureAtOpenCarriesNoSentinel(t *testing.T) {
	occupied := filepath.Join(tempDir(t), "not-a-directory")
	require.NoError(t, os.WriteFile(occupied, []byte("x"), 0o600))

	l, err := New(Options{Path: occupied})
	if l != nil {
		t.Cleanup(func() { _ = l.Close() })
	}
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrInvalidOptions,
		"Options.Path is a perfectly good value; the filesystem is what said no")
	require.NotErrorIs(t, err, ErrLogLocked)
}
