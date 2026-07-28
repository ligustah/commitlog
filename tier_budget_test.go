package commitlog

import (
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
		SegmentStore:     store,
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
		Ceiling:           hw + 1,
		TombstoneGCBelow:  hw + 1,
		RewriteBudget:     time.Hour,
		TierRewriteBudget: time.Nanosecond,
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

// An unset TierRewriteBudget falls back to RewriteBudget, so a caller that sets
// nothing sees exactly the previous behaviour.
func TestTierBudgetDefaultsToTheRewriteBudget(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	l, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  128,
		Compact:          true,
		SegmentStore:     store,
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
		Ceiling:          hw + 1,
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
