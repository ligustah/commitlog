package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// StripBelow ABOVE Ceiling is a legitimate spec, and this is the regression test
// for the release that refused it.
//
// v0.76.0 added a check that StripBelow must not exceed a supplied Ceiling, on
// the reasoning that the two describe one boundary from opposite sides and so
// could not disagree. Two things were wrong with that. Ceiling is not a claim
// about decidedness — it bounds COMPACTION, and a caller has reasons to hold it
// down other than the LSO: durable_streams builds both fields equal at the LSO
// and then lowers Ceiling ALONE to pin records a lagging consumer group has not
// read yet. And the pass could not have done harm with the pairing regardless,
// because classify returns dispRetain for `offset >= spec.ceiling` before it
// considers stripping at all.
//
// So the refusal protected against something that could not happen, while
// rejecting every pass on a stream that had a decided transaction and a slow
// group at the same time. What this pins is the behaviour that makes the spec
// worth writing: the ceiling wins where the two overlap, and StripBelow still
// does its job below it.
func TestAStripBelowAboveTheCeilingIsHonoured(t *testing.T) {
	l, app := specLog(t)
	txHdrs := func() map[string][]byte {
		return map[string][]byte{
			"pid":   {0, 0, 0, 0, 0, 0, 0, 7},
			"epoch": {0, 0, 0, 1},
			"seq":   {0, 0, 0, 0, 0, 0, 0, 3},
			"app":   []byte("keep-me"),
		}
	}

	// Below the consumer floor: decided, unpinned, and so strippable.
	offLow := app(&Message{Key: []byte("low"), Value: []byte("v"), Headers: txHdrs()})
	// At and above it: two copies of one key. The FIRST is superseded by the
	// second, so it is exactly the record compaction would drop if the ceiling
	// were not holding it — which is what the lagging group is protected from.
	offPinnedOld := app(&Message{Key: []byte("k"), Value: []byte("v1"), Headers: txHdrs()})
	offPinnedNew := app(&Message{Key: []byte("k"), Value: []byte("v2"), Headers: txHdrs()})
	app(&Message{Key: []byte("pad"), Value: []byte("pad")})

	// The consumer floor pins from offPinnedOld up. StripBelow stays at the LSO,
	// above all of them, because every one of these records is decided.
	ceiling := offPinnedOld
	stripBelow := l.HighWatermark()
	require.Greater(t, stripBelow, ceiling,
		"the fixture must put StripBelow above Ceiling or it tests nothing")

	_, err := l.CleanWithSpec(CleanSpec{
		Ceiling:      At(ceiling),
		StripBelow:   stripBelow,
		StripHeaders: []string{"pid", "epoch", "seq"},
	})
	require.NoError(t, err,
		"a StripBelow above the ceiling is what a caller pinning records for a "+
			"lagging consumer ends up writing; it must not be refused")

	got := readAllMsgs(t, l)

	// Pinned: the superseded copy is still here. Without the ceiling holding it
	// down, latest-per-key would have taken it.
	require.Contains(t, got, offPinnedOld,
		"the ceiling did not pin the superseded copy; a lagging consumer would "+
			"have lost a record it had not read")
	require.Equal(t, "v1", string(got[offPinnedOld].Value()))
	require.Contains(t, got, offPinnedNew)

	// And verbatim, headers included: at or above the ceiling classify retains
	// before it considers stripping, so StripBelow does not reach up here. This
	// is the half the refusal got backwards — it believed these records were
	// being stripped, which is why it thought there was anything to prevent.
	for _, off := range []int64{offPinnedOld, offPinnedNew} {
		require.Contains(t, got[off].Headers(), "pid",
			"record %d sits at or above the ceiling and must be retained "+
				"verbatim, StripBelow notwithstanding", off)
	}

	// Below the ceiling StripBelow still applies, so the spec is not merely being
	// tolerated — it is doing its job on the range it can reach.
	require.Contains(t, got, offLow)
	lowHdrs := got[offLow].Headers()
	require.NotContains(t, lowHdrs, "pid",
		"a decided record below the ceiling should still lose its transactional "+
			"headers; the spec was accepted but did nothing")
	require.NotContains(t, lowHdrs, "epoch")
	require.NotContains(t, lowHdrs, "seq")
	require.Equal(t, "keep-me", string(lowHdrs["app"]),
		"a non-transactional header must survive the strip")
}
