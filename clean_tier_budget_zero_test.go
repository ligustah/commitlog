package commitlog

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A named tier with a budget of 0 is refused, and the pass does not run.
//
// It is the "default arm launders bad input" shape: budgetFor read
// `!ok || d == 0`, so the fallback branch a caller reaches by saying NOTHING was
// also the branch reached by saying 0 — and a value the caller wrote down became
// unreachable. Which reading was lost depends on what they meant, and both are
// wrong here: 0 is unbounded on RewriteBudget one field up, and an unbounded
// tiered rewrite consuming the whole pass is the case TierBudgets exists to
// prevent; unset is already spelled by leaving the tier out of the map.
//
// Refused rather than resolved, matching the Ceiling/DisableAutoClean refusal
// ten lines above it in CleanWithSpec: a spec that cannot be honoured fails
// loudly instead of being reinterpreted into one that can.
func TestATierBudgetOfZeroIsRefused(t *testing.T) {
	l, store, before := tieredJoinFixture(t, 120)

	pre := liveSegments(l)

	_, err := l.CleanWithSpec(CleanSpec{
		TierBudgets: map[string]time.Duration{joinTierName: 0},
	})
	require.Error(t, err, "a tier budget of 0 must be refused")
	require.ErrorContains(t, err, joinTierName,
		"the error must name the tier the caller has to fix")

	// Refused BEFORE the pass, not partway through it. A spec rejected after
	// segments moved would be the worst of both: an error the caller must handle
	// and a log that changed anyway.
	require.Equal(t, len(pre), len(liveSegments(l)), "the refused pass still ran")
	require.Equal(t, before, readAllMsgs(t, l))
	published := store.published()
	require.Empty(t, published, "the refused pass published a manifest")
}

// The refusal is about the VALUE, not about the map, and this is what holds it
// to that scope.
//
// A refusal written one notch too wide — firing on any entry, or on any tier the
// log does not have — reads as stricter and is not: absence from TierBudgets is
// the documented way to say "use RewriteBudget here", so refusing it would break
// the caller who spelled the default correctly. Mutating the condition to
// `d >= 0` is what this catches, and it catches it while the test above still
// passes, which is why both exist.
//
// Note what it does NOT assert: that an absent tier's budget ends up equal to
// RewriteBudget. Nothing observable distinguishes that from an unbounded one on
// a pass this small — the difference is a deadline neither reaches — and a test
// that cannot fail for the reason it names is worse than no test.
func TestATierAbsentFromTierBudgetsStillRuns(t *testing.T) {
	l, _, before := tieredJoinFixture(t, 120)

	_, err := l.CleanWithSpec(CleanSpec{
		TierBudgets:   map[string]time.Duration{"a-tier-this-log-does-not-have": time.Second},
		RewriteBudget: time.Second,
	})
	require.NoError(t, err, "a tier absent from TierBudgets falls back to RewriteBudget")
	require.Equal(t, before, readAllMsgs(t, l))
}
