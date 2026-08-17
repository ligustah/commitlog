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

// The same rule, one door over: a caller-supplied argument that is wrong is as
// permanent as a wrong Option, and must say so with the same sentinel.
//
// These two were found by taking the interface doc's retryability rule
// literally — "a commitlog sentinel means permanent; anything else is an OS or
// store error and may be transient" — and looking for a refusal that breaks it.
// Both did, on exported surface, and both are exactly the ErrInvalidOptions
// class arriving through a different door than New.
//
// That is the third path the same defect reached, after New's Options (#309)
// and a corrupt block header refusing an open (#312). The pattern is what makes
// it worth a test rather than a fix: the rule is easy to state, easy to believe
// already true, and false at whichever refusal nobody re-read.
func TestACallersBadArgumentIsAsPermanentAsABadOption(t *testing.T) {
	l, cleanup := setup(t)
	defer cleanup()
	defer l.Close() // nolint: errcheck

	// maxBytes is the caller's, and no environment change makes zero positive.
	_, err := l.ReadMessageSet(0, 0)
	require.ErrorIs(t, err, ErrInvalidOptions,
		"a retrying caller cannot tell a bad maxBytes from a busy disk")
	require.Contains(t, err.Error(), "maxBytes must be positive",
		"the error must still name what the caller got wrong")

	// CopyTier is a package function rather than a method, which is the only
	// reason it was missed: nothing about it is reachable from New, so neither
	// hack/openerrors.sh nor any Options test could see it.
	require.ErrorIs(t, CopyTier(nil, nil), ErrInvalidOptions)

	// And the boundary holds in the other direction — a store that exists but
	// is empty is not the caller's mistake, so it must NOT claim to be.
	dir := tempDir(t)
	src, err := NewFileSegmentStore(filepath.Join(dir, "src"))
	require.NoError(t, err)
	dst, err := NewFileSegmentStore(filepath.Join(dir, "dst"))
	require.NoError(t, err)
	if cerr := CopyTier(src, dst); cerr != nil {
		require.NotErrorIs(t, cerr, ErrInvalidOptions,
			"both arguments are exactly what the caller meant; whatever went "+
				"wrong here is about the stores, not the call")
	}
}
