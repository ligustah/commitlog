package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A Ceiling and a running automatic cleaner cannot both be honoured, so the
// combination is refused instead of documented.
//
// The two settings do not merely disagree. A ceiling protects every record at
// or above it because those records may be undecided; the automatic pass has no
// spec at all, bounds itself at the high watermark, and so compacts exactly
// those records on its own timer. The caller's ceiling is honoured on every
// pass the caller drives and ignored on every pass it does not, which looks
// like a working system until an undecided record is missing — and by then the
// record is gone and the pass that took it left no trace.
//
// The hazard is not hypothetical: it is what the v0.60.1 clean-at-open change
// surfaced. Two tests in this package drove CleanWithSpec with a ceiling while
// a spec-less pass ran underneath them, and they had been passing only because
// the automatic pass had never once fired before the assertions — NewTicker
// does not fire until t+interval, and nothing ran a pass at open. Making the
// log clean at open turned that latent misconfiguration into a failure. Every
// caller outside this repo had the same latent misconfiguration available.
func TestACeilingOnAnAutoCleaningLogIsRefused(t *testing.T) {
	newLog := func(t *testing.T, disable bool) *commitLog {
		l, cleanup := setupWithOptions(t, Options{
			Path:             tempDir(t),
			MaxSegmentBytes:  64,
			Compact:          true,
			DisableAutoClean: disable,
		})
		t.Cleanup(cleanup)
		offs, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("v")}})
		require.NoError(t, err)
		l.SetHighWatermark(offs[0])
		return l
	}

	t.Run("a ceiling with the cleaner running is an error", func(t *testing.T) {
		l := newLog(t, false)

		verified, err := l.CleanWithSpec(CleanSpec{Ceiling: At(l.HighWatermark())})

		require.Error(t, err)
		require.Contains(t, err.Error(), "DisableAutoClean",
			"the error must name the option that fixes it, not just the problem")
		require.Zero(t, verified, "a refused clean verified nothing")
	})

	// At(0) is the narrowest ceiling there is and the one a caller whose oldest
	// open transaction begins at offset 0 must pass. It is also the zero offset,
	// so a guard testing the offset rather than whether a bound was supplied
	// would wave it through — the same sentinel mistake the field itself once
	// made. See TestACeilingOfZeroCompactsNothing.
	t.Run("a ceiling of zero is still a ceiling", func(t *testing.T) {
		l := newLog(t, false)

		_, err := l.CleanWithSpec(CleanSpec{Ceiling: At(0)})

		require.Error(t, err, "At(0) is a supplied bound, not an absent one")
	})

	t.Run("the same spec is accepted with the cleaner off", func(t *testing.T) {
		l := newLog(t, true)

		_, err := l.CleanWithSpec(CleanSpec{Ceiling: At(l.HighWatermark())})

		require.NoError(t, err)
	})

	// The guard must cost the non-transactional caller nothing. A spec with no
	// ceiling wants the high watermark, which is what the automatic pass uses
	// too, so the two agree and there is nothing to refuse.
	t.Run("a spec without a ceiling is unaffected", func(t *testing.T) {
		l := newLog(t, false)

		_, err := l.CleanWithSpec(CleanSpec{StripBelow: l.HighWatermark()})

		require.NoError(t, err)
	})
}
