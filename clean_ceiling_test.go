package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A Ceiling of zero means "compact nothing", and it must be reachable.
//
// It was not. The field was an int64 and the pass read `if spec.Ceiling <= 0 {
// spec.Ceiling = l.HighWatermark() }`, so zero — the narrowest bound a caller
// can ask for — arrived as the widest one there is. The caller that needs it is
// a transactional one whose oldest open transaction begins at offset 0: every
// record in the log is undecided, and the answer it got was that every record
// was compactable. Nothing in the spec was wrong; the sentinel ate it.
//
// This is the same shape as RetentionFloor, which is a *int64 for the same
// reason and says so in its own doc. Both are bounds whose zero value is a real
// offset, so neither can spare its zero to mean "unset".
func TestACeilingOfZeroCompactsNothing(t *testing.T) {
	l, app := specLog(t)

	// Three copies of one key: latest-per-key compaction would leave one.
	app(&Message{Key: []byte("k"), Value: []byte("v1")})
	app(&Message{Key: []byte("k"), Value: []byte("v2")})
	app(&Message{Key: []byte("k"), Value: []byte("v3")})
	app(&Message{Key: []byte("pad"), Value: []byte("pad")}) // active segment padding

	before := readAllMsgs(t, l)
	requireCleanOK(t, l, CleanSpec{Ceiling: bound(0)})
	after := readAllMsgs(t, l)

	require.Equal(t, before, after, "a Ceiling of 0 bounds compaction at offset 0, "+
		"so every record is at or above it and must survive verbatim")
}

// Nil is the one value that means "no bound supplied", and it keeps the
// behaviour every non-transactional caller already had: compact up to the high
// watermark, where everything below is decided by definition.
func TestNoCeilingCompactsToTheHighWatermark(t *testing.T) {
	l, app := specLog(t)

	app(&Message{Key: []byte("k"), Value: []byte("v1")})
	app(&Message{Key: []byte("k"), Value: []byte("v2")})
	offLatest := app(&Message{Key: []byte("k"), Value: []byte("v3")})
	app(&Message{Key: []byte("pad"), Value: []byte("pad")})

	requireCleanOK(t, l, CleanSpec{})

	got := readAllMsgs(t, l)
	var copies []int64
	for off, m := range got {
		if string(m.Key()) == "k" {
			copies = append(copies, off)
		}
	}
	require.Equal(t, []int64{offLatest}, copies,
		"with no Ceiling the pass compacts to the high watermark, "+
			"leaving one copy per key")
}

// A negative Ceiling is not a policy this log disagrees with, it is a value that
// cannot mean anything: offsets are non-negative. It is refused rather than
// clamped, because clamping is exactly how the old sentinel turned a caller's
// narrowest bound into the widest one.
func TestANegativeCeilingIsRefused(t *testing.T) {
	l, app := specLog(t)
	app(&Message{Key: []byte("k"), Value: []byte("v1")})

	for _, off := range []int64{-1, -42} {
		verified, err := l.CleanWithSpec(CleanSpec{Ceiling: bound(off)})
		require.Errorf(t, err, "Ceiling %d was accepted", off)
		require.Equalf(t, int64(-1), verified, "Ceiling %d returned a floor", off)
	}
}
