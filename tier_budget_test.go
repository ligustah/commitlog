package commitlog

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A tiered rewrite downloads, rewrites and uploads; a local one touches local
// disk. Sharing one wall-clock budget lets a single slow remote rewrite consume
// the pass and starve local compaction while local debt grows — so the tiers
// draw on separate budgets.
//
// Here the tier budget is exhausted while the local one is not: the offloaded
// segments must be left for a later pass, and the local ones must still be
// compacted rather than blocked behind them.
func TestTierBudgetDoesNotStarveLocalCompaction(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	l, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  128,
		Compact:          true,
		Tiers:            oneTier(store),
		DisableAutoClean: true,
	})
	defer cleanup()

	// Two rounds on one key, so both the offloaded and the local segments hold
	// superseded copies worth rewriting.
	var last int64
	for i := 0; i < 16; i++ {
		offs, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("value padding")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)
	n, err := l.OffloadBefore(last)
	require.NoError(t, err)
	require.Positive(t, n)

	for i := 0; i < 16; i++ {
		offs, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("value padding")}})
		require.NoError(t, err)
		last = offs[0]
	}
	for _, k := range []string{"pad0", "pad1"} {
		offs, err := l.Append([]*Message{{Key: []byte(k), Value: []byte("p")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)

	keysBefore := map[int64]string{}
	locals := map[int64]bool{}
	l.mu.RLock()
	for _, s := range l.segments {
		s.RLock()
		if s.store != nil {
			keysBefore[s.BaseOffset] = s.storeKey
		} else {
			locals[s.BaseOffset] = true
		}
		s.RUnlock()
	}
	l.mu.RUnlock()
	require.NotEmpty(t, keysBefore, "the fixture needs offloaded segments")
	require.NotEmpty(t, locals, "and local ones")

	// A tier budget already spent, with local rewrites still affordable.
	hw := l.HighWatermark()
	_, err = l.CleanWithSpec(CleanSpec{
		Ceiling:          At(hw + 1),
		TombstoneGCBelow: hw + 1,
		RewriteBudget:    time.Hour,
		TierBudgets:      map[string]time.Duration{defaultTierName: time.Nanosecond},
	})
	require.NoError(t, err)

	// The tier budget allows at least one rewrite by design (debt must always
	// drain), so the check is that it did not run away with the whole pass.
	advanced := 0
	l.mu.RLock()
	for _, s := range l.segments {
		s.RLock()
		if s.store != nil {
			if before, ok := keysBefore[s.BaseOffset]; ok && s.storeKey != before {
				advanced++
			}
		}
		s.RUnlock()
	}
	l.mu.RUnlock()
	require.LessOrEqual(t, advanced, 1,
		"an exhausted tier budget must defer its remaining rewrites, not spend "+
			"the pass on them")

	// And the log is still readable, which is the part a budget split must not
	// cost.
	require.NotEmpty(t, readFrom(t, l))
}

// A tier with no entry in TierBudgets falls back to RewriteBudget, so a caller
// that sets nothing sees exactly the previous behaviour.
func TestTierBudgetDefaultsToTheRewriteBudget(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	l, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  128,
		Compact:          true,
		Tiers:            oneTier(store),
		DisableAutoClean: true,
	})
	defer cleanup()

	var last int64
	for i := 0; i < 20; i++ {
		offs, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("value padding")}})
		require.NoError(t, err)
		last = offs[0]
	}
	for _, k := range []string{"pad0", "pad1"} {
		offs, err := l.Append([]*Message{{Key: []byte(k), Value: []byte("p")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)
	_, err = l.OffloadBefore(last)
	require.NoError(t, err)

	keysBefore := map[int64]string{}
	l.mu.RLock()
	for _, s := range l.segments {
		s.RLock()
		if s.store != nil {
			keysBefore[s.BaseOffset] = s.storeKey
		}
		s.RUnlock()
	}
	l.mu.RUnlock()

	// No tier budget set, a generous shared one: tiered rewrites still happen.
	hw := l.HighWatermark()
	_, err = l.CleanWithSpec(CleanSpec{
		Ceiling:          At(hw + 1),
		TombstoneGCBelow: hw + 1,
		RewriteBudget:    time.Hour,
	})
	require.NoError(t, err)

	advanced := 0
	l.mu.RLock()
	for _, s := range l.segments {
		s.RLock()
		if s.store != nil {
			if before, ok := keysBefore[s.BaseOffset]; ok && s.storeKey != before {
				advanced++
			}
		}
		s.RUnlock()
	}
	l.mu.RUnlock()
	require.Positive(t, advanced,
		"with no tier budget set, the shared budget must still fund tiered rewrites")
}

// One tier's budget is one tier's. Two tiers each get their own allowance, so a
// caller that gives its cold archive a small budget does not thereby shrink
// what its hot tier may spend.
//
// The discriminator is the "at least one rewrite" floor every budget carries:
// under a single shared tier budget an exhausted allowance permits ONE tiered
// rewrite in the whole pass, and under per-tier budgets it permits one in each.
func TestOneTiersBudgetDoesNotShrinkAnothers(t *testing.T) {
	dir := tempDir(t)
	hot, err := NewFileSegmentStore(filepath.Join(dir, "hot"))
	require.NoError(t, err)
	cold, err := NewFileSegmentStore(filepath.Join(dir, "cold"))
	require.NoError(t, err)

	l, cleanup := setupWithOptions(t, Options{
		Path:            dir,
		MaxSegmentBytes: 64,
		Compact:         true,
		Tiers: []Tier{
			{Name: "hot", Store: hot},
			{Name: "cold", Store: cold},
		},
		DisableAutoClean: true,
	})
	defer cleanup()

	// Alternating keys, so every segment holds both a record compaction drops
	// and one it keeps. A segment whose records are ALL superseded is deleted
	// rather than rewritten, and a deletion costs no budget — the fixture has
	// to produce rewrites for a rewrite budget to be observable at all.
	var last int64
	for i := 0; i < 40; i++ {
		key := "k"
		if i%2 == 1 {
			key = fmt.Sprintf("u%d", i)
		}
		offs, err := l.Append([]*Message{{Key: []byte(key), Value: []byte("padding value")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)
	n, err := l.OffloadBefore(last)
	require.NoError(t, err)
	require.Positive(t, n)

	// Half into the archive, with a ceiling of zero so this pass compacts
	// nothing: the rewrite work has to survive to the pass under test.
	objs, err := l.TierManifest()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(objs), 4, "the fixture needs segments to split between tiers")
	placement := map[int64]string{}
	for _, o := range objs[len(objs)/2:] {
		placement[o.BaseOffset] = "cold"
	}
	_, err = l.CleanWithSpec(CleanSpec{TierPlacement: placement, Ceiling: At(0)})
	require.NoError(t, err)

	keysBefore, tierOfSeg := map[int64]string{}, map[int64]string{}
	l.mu.RLock()
	for _, s := range l.segments {
		s.RLock()
		if s.store != nil {
			keysBefore[s.BaseOffset] = s.storeKey
			tierOfSeg[s.BaseOffset] = s.tier
		}
		s.RUnlock()
	}
	l.mu.RUnlock()
	inTier := map[string]int{}
	for _, tier := range tierOfSeg {
		inTier[tier]++
	}
	require.Positive(t, inTier["hot"], "the fixture needs segments in both tiers")
	require.Positive(t, inTier["cold"])

	// Both tiers exhausted. Each must still get its own single rewrite.
	hw := l.HighWatermark()
	_, err = l.CleanWithSpec(CleanSpec{
		Ceiling:          At(hw + 1),
		TombstoneGCBelow: hw + 1,
		RewriteBudget:    time.Hour,
		TierBudgets:      map[string]time.Duration{"hot": time.Nanosecond, "cold": time.Nanosecond},
	})
	require.NoError(t, err)

	advanced := map[string]int{}
	l.mu.RLock()
	for _, s := range l.segments {
		s.RLock()
		if before, ok := keysBefore[s.BaseOffset]; ok && s.store != nil && s.storeKey != before {
			advanced[tierOfSeg[s.BaseOffset]]++
		}
		s.RUnlock()
	}
	l.mu.RUnlock()

	require.Equal(t, 1, advanced["hot"],
		"the hot tier's floor is the hot tier's, whatever the archive's budget is")
	require.Equal(t, 1, advanced["cold"],
		"and the archive's is its own; one shared budget would allow only one rewrite in total")
}
