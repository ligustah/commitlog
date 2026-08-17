package commitlog

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// negativeIsAValue names the signed numeric Options where a negative is a value
// the caller can mean, with the meaning. Everything else must be REFUSED.
//
// An allowlist and not a denylist, because those rot in opposite directions.
// The list of refused options is the one that has to grow with the struct, and
// forgetting to grow it is silent — that is how PrefixReadConcurrency,
// MaxSegmentAge and the three retention limits each shipped able to launder a
// negative. This list only grows when someone gives a negative a MEANING, which
// is a thing they are already thinking about at the time.
var negativeIsAValue = map[string]string{
	"CleanRewriteBudget": "a negative budget means no budget at all, which is " +
		"what every spec-less pass had before one existed",
	"PrefixReadCoalesceBytes": "the documented way to say never coalesce, one " +
		"request per isolated record",
	"PrefixReadTierCoalesceBytes": "as PrefixReadCoalesceBytes, for the tier",
}

// Every signed numeric option has an opinion about negatives, found by
// REFLECTION over Options rather than by a list kept beside it.
//
// The table in New is an enumeration of fields, maintained by hand, one struct
// away from the thing it enumerates. Twice in one day it turned out to be
// missing an entry, and neither omission was visible: the option was accepted,
// the default arm swallowed it, and the log ran on a value nobody meant. A test
// built from the same hand-written list cannot see that — it checks the entries
// the list already has.
//
// So the fields come from the struct. Adding a numeric option to Options and
// not deciding about negatives now fails HERE, at the point of adding it, with
// the two ways to resolve it named in the failure. That is the whole mechanism:
// the decision becomes unavoidable rather than remembered.
func TestEveryNumericOptionDecidesAboutNegatives(t *testing.T) {
	ot := reflect.TypeOf(Options{})
	checked := 0
	for i := range ot.NumField() {
		field := ot.Field(i)
		switch field.Type.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			// Signed only. An unsigned option cannot hold a negative, and
			// compress.Codec is a byte — so the codec is excluded by what it is
			// rather than by being remembered.
		default:
			continue
		}
		checked++
		t.Run(field.Name, func(t *testing.T) {
			opts := Options{Name: "negatives", Path: tempDir(t)}
			reflect.ValueOf(&opts).Elem().FieldByName(field.Name).SetInt(-1)

			l, err := New(opts)
			if l != nil {
				t.Cleanup(func() { _ = l.Close() })
			}
			if why, ok := negativeIsAValue[field.Name]; ok {
				require.NoError(t, err,
					"Options.%s is on the negative-is-a-value list (%s) but New refused it",
					field.Name, why)
				return
			}
			require.Error(t, err,
				"Options.%s accepted -1. Either refuse it in New's table, or add it to "+
					"negativeIsAValue with what a negative MEANS. Accepting it without a "+
					"meaning is how a value nobody meant reaches a default arm.", field.Name)
			require.Contains(t, err.Error(), "must not be negative",
				"Options.%s was refused for some OTHER reason, so this test says nothing "+
					"about negatives for it", field.Name)
			require.ErrorIs(t, err, ErrInvalidOptions,
				"a caller opening on a retry loop reads an unsentineled refusal as "+
					"an environment problem and spins on Options that will never change")
		})
	}
	// Reflection finding nothing is a passing test. See how many fields it is
	// actually reaching, so a change to the Kind switch above cannot quietly
	// turn this into a loop over zero options.
	require.GreaterOrEqual(t, checked, 15,
		"only %d signed numeric Options fields were reached; the Kind filter is wrong", checked)
}

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
		// The same failure as MaxSegmentBytes, one field away in Options and
		// missed for as long: CheckSplit disables rolling on `logRollTime == 0`,
		// so a negative reaches `timestamp()-firstWriteTime >= int64(...)`, which
		// is true for anything a clock can produce, and every append rolls.
		"MaxSegmentAge": func(o *Options) { o.MaxSegmentAge = -time.Second },
		// The retention three, which fail worse: noRetentionLimits() asked `== 0`
		// while the apply gates ask `> 0`, so a negative was "configured" to one
		// and "skip" to the others. The log grew unbounded while the debug line
		// reported the policy it was about to ignore.
		"MaxLogBytes":    func(o *Options) { o.MaxLogBytes = -1 },
		"MaxLogMessages": func(o *Options) { o.MaxLogMessages = -1 },
		"MaxLogAge":      func(o *Options) { o.MaxLogAge = -time.Second },
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
			require.ErrorIs(t, err, ErrInvalidOptions)
		})
	}
}

// The deleteCleaner's own consistency, checked where New cannot reach.
//
// noRetentionLimits() decides whether the pass runs at all; the three gates in
// cleanLocal decide whether each limit applies. They must agree about every
// value, because a value the first calls "configured" and the second calls
// "skip" is a pass that does the walk, logs the policy, and enforces nothing.
//
// Asserted against the cleaner rather than through New on purpose: New now
// refuses a negative, so routing this through New would test the refusal a
// second time and never reach the disagreement. A deleteCleaner is built
// directly in tests and takes Retention as a plain struct, so the boundary
// check is not a promise this type can rely on.
func TestTheCleanerAgreesWithItselfAboutANegativeLimit(t *testing.T) {
	for name, mut := range map[string]func(*deleteCleanerOptions){
		"Bytes":    func(o *deleteCleanerOptions) { o.Retention.Bytes = -1 },
		"Messages": func(o *deleteCleanerOptions) { o.Retention.Messages = -1 },
		"Age":      func(o *deleteCleanerOptions) { o.Retention.Age = -time.Second },
	} {
		t.Run(name, func(t *testing.T) {
			opts := deleteCleanerOptions{Path: "agree"}
			mut(&opts)
			c := &deleteCleaner{deleteCleanerOptions: opts}
			require.True(t, c.noRetentionLimits(),
				"a negative %s is not applied by cleanLocal's `> 0` gate, so it must "+
					"not count as a configured limit here — otherwise the pass runs "+
					"and enforces nothing", name)
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
