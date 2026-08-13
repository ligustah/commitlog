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
		// concurrencyBudget defaults on `v <= 0`, so these were the same defect
		// in a fourth and fifth place — but reachable by FOLLOWING THE
		// DOCUMENTATION rather than by fumbling a subtraction. The sibling
		// CoalesceBytes knobs are described in the same Options paragraph and
		// that paragraph teaches that a negative is meaningful and powerful
		// here, so a caller who reads it and asks for the analogous extreme
		// silently received 8 or 64 instead.
		"PrefixReadConcurrency":     func(o *Options) { o.PrefixReadConcurrency = -1 },
		"PrefixReadTierConcurrency": func(o *Options) { o.PrefixReadTierConcurrency = -1 },
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

// The other side of the asymmetry, pinned so it is not "tidied up" into
// consistency. Four PrefixRead knobs sit in one Options paragraph and only two
// of them refuse a negative, which looks like an oversight and is not: a
// negative CoalesceBytes is the documented way to say "never coalesce, one
// request per isolated record", a behaviour a caller can want and cannot
// otherwise express, while a negative Concurrency has no defensible meaning —
// the analogous extreme is unbounded on one reading and serial on the other.
//
// Without this test, adding the two CoalesceBytes fields to the refusal list
// above reads as finishing the job, and the only thing that would go red is a
// cost test in another file that does not mention New.
func TestANegativeCoalesceBudgetIsAValueNotAMistake(t *testing.T) {
	l, err := New(Options{
		Path: tempDir(t), Compact: true,
		PrefixReadCoalesceBytes:     -1,
		PrefixReadTierCoalesceBytes: -1,
	})
	require.NoError(t, err, "a negative coalesce budget means 'never coalesce', not a mistake")
	t.Cleanup(func() { l.Close() })

	// And it resolves to a zero-byte budget rather than to the default, which
	// is the behaviour the sign is there to express.
	require.Equal(t, int64(0), coalesceBudget(-1, defaultPrefixReadCoalesceBytes))
	require.Equal(t, int64(defaultPrefixReadCoalesceBytes),
		coalesceBudget(0, defaultPrefixReadCoalesceBytes))
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
