package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// StripBelow above Ceiling is refused, and the pass does not run.
//
// The two fields are one caller's account of one boundary. Ceiling says records
// at or above it MAY BE UNDECIDED and are retained verbatim; StripBelow says
// records below it ARE DECIDED and no longer need their transactional
// bookkeeping. Set StripBelow higher and the range between them is described
// both ways at once — and the pass acts on the second: mergeDigests marks a
// record for stripping on `r.offset < spec.StripBelow` BEFORE it consults the
// ceiling, so the records the ceiling was set to protect keep their offsets and
// lose their pid/epoch/seq headers. Those headers are how the caller decides the
// transaction they belong to, so an undecided record that loses them cannot be
// decided by anyone, ever.
//
// This is the invariant docs/layering.md asked for by name — "if a cheap
// invariant on Ceiling ever becomes available, it belongs at the top of
// CleanWithSpec" — and it is cheap because it compares two numbers the caller
// supplied together, with nothing about the log involved.
func TestAStripBelowAboveTheCeilingIsRefused(t *testing.T) {
	l, app := specLog(t)

	app(&Message{Key: []byte("k"), Value: []byte("first")})
	app(&Message{Key: []byte("k"), Value: []byte("second")})
	app(&Message{Key: []byte("pad"), Value: []byte("pad")})

	before := readAllMsgs(t, l)
	hw := l.HighWatermark()

	_, err := l.CleanWithSpec(CleanSpec{
		Ceiling:      At(hw),
		StripBelow:   hw + 1,
		StripHeaders: []string{"pid", "epoch", "seq"},
	})
	require.Error(t, err, "a StripBelow above the ceiling must be refused")
	require.ErrorContains(t, err, "StripBelow",
		"the error must name the field the caller has to fix")

	// Refused BEFORE the pass, not partway through it: a spec rejected after
	// records were rewritten would be the worst of both, an error the caller has
	// to handle and a log that changed anyway.
	require.Equal(t, before, readAllMsgs(t, l), "the refused pass still ran")
}

// The refusal is about StripBelow EXCEEDING the ceiling, and this holds it there.
//
// Every caller in this repo and in durable_streams passes the two equal —
// `Ceiling: At(hw), StripBelow: hw` — because the LSO is one boundary and both
// fields describe it. A refusal written one notch too wide (`>=`) rejects exactly
// that spec, which is the normal one, and reads as stricter while being simply
// broken. The equal case is the case that has to keep working.
func TestAStripBelowAtTheCeilingStillRuns(t *testing.T) {
	l, app := specLog(t)

	app(&Message{Key: []byte("k"), Value: []byte("first")})
	app(&Message{Key: []byte("k"), Value: []byte("second")})
	app(&Message{Key: []byte("pad"), Value: []byte("pad")})

	hw := l.HighWatermark()
	_, err := l.CleanWithSpec(CleanSpec{
		Ceiling:      At(hw),
		StripBelow:   hw,
		StripHeaders: []string{"pid", "epoch", "seq"},
	})
	require.NoError(t, err,
		"StripBelow equal to Ceiling is the spec every transactional caller "+
			"passes; it describes one boundary from both sides and contradicts "+
			"nothing")
}

// A StripBelow that strips NOTHING is not judged against the ceiling either.
//
// The contradiction the refusal catches is only reachable where stripping
// happens, and stripping needs both fields: mergeDigests gates on
// `spec.StripBelow > 0 && len(spec.StripHeaders) > 0`, and classify gates marker
// removal on the same pair. With no headers named, a StripBelow above the ceiling
// removes nothing from anything and contradicts nothing.
//
// This is the arm the first version of the refusal did not have, and its absence
// was not a theoretical gap. StripBelow's zero value means "no stripping", not
// "strip below offset 0"; HighWatermark returns -1 for "nothing committed yet";
// `Ceiling: At(l.HighWatermark())` is what callers write. So an unset StripBelow
// of 0 sat above a legitimate ceiling of -1 and the pass was refused —
// TestACeilingBelowEveryOffsetIsLegitimate went red across every CI platform.
// Reading a field the caller never set as a value they wrote down is the same
// laundering the TierBudgets refusal exists to stop, committed by the check
// written to stop it.
func TestAStripBelowWithoutStripHeadersIsNotJudged(t *testing.T) {
	l, app := specLog(t)

	app(&Message{Key: []byte("k"), Value: []byte("first")})
	app(&Message{Key: []byte("pad"), Value: []byte("pad")})

	hw := l.HighWatermark()
	_, err := l.CleanWithSpec(CleanSpec{
		Ceiling:    At(hw),
		StripBelow: hw + 1,
		// No StripHeaders: nothing is stripped, so nothing above the ceiling can
		// lose the bookkeeping the ceiling protects.
	})
	require.NoError(t, err,
		"a StripBelow with no StripHeaders strips nothing and cannot contradict "+
			"the ceiling, but was refused anyway")
}

// A spec with no Ceiling is not judged against StripBelow at all.
//
// With the field unset the bound is the log's own high watermark, resolved
// inside the pass and free to move between any check here and any use of it
// there — so a refusal on that pairing would be a race wearing an invariant's
// clothes. What the refusal above compares is two numbers the caller wrote down
// together, which disagree or do not, and a spec that supplies only one of them
// has said nothing to contradict.
func TestAStripBelowWithNoCeilingIsNotJudged(t *testing.T) {
	l, app := specLog(t)

	app(&Message{Key: []byte("k"), Value: []byte("first")})
	app(&Message{Key: []byte("pad"), Value: []byte("pad")})

	// Far above anything this log holds, which is what would trip the refusal if
	// it reached for the high watermark to stand in for the missing ceiling.
	_, err := l.CleanWithSpec(CleanSpec{
		StripBelow:   l.HighWatermark() + 1000,
		StripHeaders: []string{"pid", "epoch", "seq"},
	})
	require.NoError(t, err,
		"a spec with no Ceiling was refused against the high watermark, which "+
			"moves under the pass and cannot be checked here")
}
