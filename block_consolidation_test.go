package commitlog

import (
	"context"
	"fmt"
	"testing"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// The append path writes one block per message set, so small commits make
// sub-KB blocks; a clean must consolidate a sealed segment's blocks toward
// cleanBlockTarget — even when the pass removes nothing (the convergence
// fast-path must not discard a consolidation rewrite, and the digest skip
// must not bypass it) — while keeping every record byte-identical.
func TestCleanConsolidatesTinyBlocks(t *testing.T) {
	opts := Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 256 << 10,
		Compact:         true,
		Compression:     compress.Zstd,
	}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()

	// ~3000 unique-key appends of ~150B each: several sealed segments, one
	// tiny block per append, and NO compactable duplicates — the pass is a
	// pure consolidation.
	for i := 0; i < 8000; i++ {
		_, err := l.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("key-%06d", i)),
			Value: []byte(fmt.Sprintf("value-%06d-%s", i, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")),
		}})
		require.NoError(t, err)
	}
	l.SetHighWatermark(l.NewestOffset())

	blockStats := func() (segs, blocks int) {
		l.mu.RLock()
		defer l.mu.RUnlock()
		for _, s := range l.segments[:len(l.segments)-1] {
			s.RLock()
			segs++
			blocks += len(s.blocks)
			s.RUnlock()
		}
		return
	}
	readAll := func() map[string]string {
		// A committed reader BLOCKS at the tail waiting for appends, so read
		// the exact expected count rather than reading to error.
		r, err := l.NewReader(0, true)
		require.NoError(t, err)
		out := map[string]string{}
		headers := make([]byte, 28)
		for i := 0; i < 8000; i++ {
			msg, _, _, _, err := r.ReadMessage(context.Background(), headers)
			require.NoError(t, err)
			out[string(msg.Key())] = string(msg.Value())
		}
		return out
	}

	segsBefore, blocksBefore := blockStats()
	require.Greater(t, segsBefore, 1, "need sealed segments")
	require.Greater(t, blocksBefore, segsBefore*1000, "appends must have made tiny blocks")
	before := readAll()
	require.Len(t, before, 8000)

	require.NoError(t, l.Clean())
	_, blocksAfter := blockStats()
	require.Less(t, blocksAfter, blocksBefore/50, "clean must consolidate blocks (before %d, after %d)", blocksBefore, blocksAfter)

	after := readAll()
	require.Equal(t, before, after, "consolidation must not change any record")

	// Random point seeks land correctly through the coarser sparse index.
	for _, off := range []int64{0, 1, 999, 1500, 2500, 7998} {
		r, err := l.NewReader(off, true)
		require.NoError(t, err)
		headers := make([]byte, 28)
		msg, gotOff, _, _, err := r.ReadMessage(context.Background(), headers)
		require.NoError(t, err)
		require.Equal(t, off, gotOff)
		require.Equal(t, fmt.Sprintf("key-%06d", off), string(msg.Key()))
	}

	// A second clean converges: consolidated segments are skipped, not
	// re-rewritten (needsBlockConsolidation is false at the new layout).
	_, blocksAgain := blockStats()
	require.NoError(t, l.Clean())
	_, blocksFinal := blockStats()
	require.Equal(t, blocksAgain, blocksFinal, "second clean must be a no-op")
}

// The verified floor returned by CleanWithSpec must cover exactly the sealed,
// rewritten/converged prefix — never the active segment (whose records keep
// headers and abort markers below the LSO; trusting an LSO floor there is how
// soak runs 23/24 lost their abort watermark and diverged).
func TestCleanVerifiedFloor(t *testing.T) {
	opts := Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 4 << 10,
		Compact:         true,
	}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()

	for i := 0; i < 400; i++ {
		_, err := l.Append([]*Message{{
			Key:     []byte(fmt.Sprintf("key-%06d", i)),
			Value:   []byte(fmt.Sprintf("value-%06d", i)),
			Headers: map[string][]byte{"pid": {1}, "seq": {byte(i)}},
		}})
		require.NoError(t, err)
	}
	hw := l.NewestOffset()
	l.SetHighWatermark(hw)

	l.mu.RLock()
	activeBase := l.segments[len(l.segments)-1].BaseOffset
	l.mu.RUnlock()
	require.Greater(t, activeBase, int64(0), "need sealed segments")

	// Strip everything decided: the floor must reach the end of the sealed
	// prefix and STOP there, even though StripBelow (≈ an LSO at the tip)
	// reaches into the active segment.
	floor, err := l.CleanWithSpec(CleanSpec{
		Ceiling: hw, StripBelow: hw, StripHeaders: []string{"pid", "epoch", "seq"},
	})
	require.NoError(t, err)
	require.Equal(t, activeBase-1, floor,
		"floor must cover exactly the sealed prefix (activeBase %d)", activeBase)

	// A converged second pass proves the same floor via digest skips.
	floor2, err := l.CleanWithSpec(CleanSpec{
		Ceiling: hw, StripBelow: hw, StripHeaders: []string{"pid", "epoch", "seq"},
	})
	require.NoError(t, err)
	require.Equal(t, floor, floor2)

	// No strip semantics ⇒ nothing verified.
	floor3, err := l.CleanWithSpec(CleanSpec{Ceiling: hw})
	require.NoError(t, err)
	require.Equal(t, int64(-1), floor3)
}
