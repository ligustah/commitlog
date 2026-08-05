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
// This is the same shape as RetentionFloor, which is a Bound for the same
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
	requireCleanOK(t, l, CleanSpec{Ceiling: At(0)})
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

// A NEGATIVE Ceiling is legitimate, and this test exists because a first draft
// of this change refused it as obviously meaningless.
//
// It is not meaningless. HighWatermark returns -1 for "nothing is committed
// yet", and `Ceiling: At(l.HighWatermark())` is what callers write — so -1
// arrives whenever a log is cleaned before anything is committed. It means what
// every ceiling means: retain everything at or above it. Since every offset is
// above -1, that is "compact nothing", which is exactly right for a log with no
// committed data.
//
// The lesson is the one docs/layering.md already states: Ceiling is an input
// this layer must trust. Even its sign turns out to be a fact the caller has and
// this layer does not.
func TestACeilingBelowEveryOffsetIsLegitimate(t *testing.T) {
	l, app := specLog(t)

	app(&Message{Key: []byte("k"), Value: []byte("v1")})
	app(&Message{Key: []byte("k"), Value: []byte("v2")})
	app(&Message{Key: []byte("pad"), Value: []byte("pad")})

	before := readAllMsgs(t, l)
	for _, off := range []int64{-1, -42} {
		_, err := l.CleanWithSpec(CleanSpec{Ceiling: At(off)})
		require.NoErrorf(t, err, "Ceiling %d was refused", off)
	}
	require.Equal(t, before, readAllMsgs(t, l),
		"a ceiling below every offset compacts nothing")
}
