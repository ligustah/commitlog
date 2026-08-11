package commitlog

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Every option defaulted by a test for zero refuses a negative value.
//
// Zero is the unset value, so the arm that supplies the default is a test for
// zero — and a test for zero reads as "the caller supplied a number" for every
// value that is not exactly the zero value. A negative therefore passes the
// check that exists to catch a missing one and lands somewhere that cannot
// cope. Measured, before this was refused:
//
//	HWCheckpointInterval  panic: non-positive interval for NewTicker
//	CleanerInterval       panic: non-positive interval for NewTicker
//	MaxSegmentBytes       no panic — the probe never returned
//
// None of those happen at the call that set the option. Two are panics on
// background tickers, where there is no caller left to hand an error to, and
// the third is a hang. That is why the answer is refusal at New rather than a
// clamp: a clamp keeps the caller's mistake and hides it.
//
// Table-driven on purpose. The defect is one defect in three places, and the
// next option defaulted this way should be added here rather than discovered.
//
// CleanRewriteBudget is deliberately absent. It is defaulted the same way, but
// a negative one MEANS something there — no budget at all, which is what every
// spec-less pass had before budgets existed — and TestTheAutomaticCleanIsBounded
// asserts it survives the zero-means-default rule. "Defaulted by a test for
// zero" is what makes an option worth checking here; it is not what makes a
// negative wrong.
func TestNegativeOptionsAreRefused(t *testing.T) {
	for name, mut := range map[string]func(*Options){
		"MaxSegmentBytes":      func(o *Options) { o.MaxSegmentBytes = -1 },
		"HWCheckpointInterval": func(o *Options) { o.HWCheckpointInterval = -time.Second },
		"CleanerInterval":      func(o *Options) { o.CleanerInterval = -time.Second },
		// Not one of the three — zero already means "never offload" here, so a
		// negative is not an unset value reaching a default. It is a horizon in
		// the future, which makes every sealed segment older than it and
		// offloads the whole log on the first pass.
		"LocalRetentionAge": func(o *Options) { o.LocalRetentionAge = -time.Second },
	} {
		t.Run(name, func(t *testing.T) {
			opts := Options{Path: tempDir(t), Compact: true}
			mut(&opts)
			l, err := New(opts)
			if err == nil {
				l.Close()
			}
			require.Error(t, err, "New accepted a negative %s", name)
			require.Contains(t, err.Error(), name,
				"the error must name the option the caller got wrong")
		})
	}
}

// Zero still means "use the default", which is the whole reason these checks
// have to be < 0 and not != 0. Without this, refusing negatives could be
// "implemented" by refusing everything unset, and every caller using a default
// would break.
func TestAZeroOptionStillTakesTheDefault(t *testing.T) {
	l, err := New(Options{Path: tempDir(t), Compact: true})
	require.NoError(t, err)
	t.Cleanup(func() { l.Close() })

	cl := l.(*commitLog)
	require.Equal(t, int64(defaultMaxSegmentBytes), cl.Options.MaxSegmentBytes)
	require.Equal(t, defaultHWCheckpointInterval, cl.Options.HWCheckpointInterval)
	require.Equal(t, defaultCleanerInterval, cl.Options.CleanerInterval)
	require.Equal(t, cl.Options.CleanerInterval, cl.Options.CleanRewriteBudget)
	require.Positive(t, cl.Options.CleanRewriteBudget,
		"an unset budget must default to the tick, not to unbounded")
}
