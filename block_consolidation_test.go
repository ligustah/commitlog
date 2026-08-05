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
		r, err := l.NewReader(From(0), Uncommitted(), Follow())
		require.NoError(t, err)
		out := map[string]string{}
		headers := make([]byte, HeaderBufferLen)
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
		r, err := l.NewReader(From(off), Uncommitted(), Follow())
		require.NoError(t, err)
		headers := make([]byte, HeaderBufferLen)
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
		Ceiling: At(hw), StripBelow: hw, StripHeaders: []string{"pid", "epoch", "seq"},
	})
	require.NoError(t, err)
	require.Equal(t, activeBase-1, floor,
		"floor must cover exactly the sealed prefix (activeBase %d)", activeBase)

	// A converged second pass proves the same floor via digest skips.
	floor2, err := l.CleanWithSpec(CleanSpec{
		Ceiling: At(hw), StripBelow: hw, StripHeaders: []string{"pid", "epoch", "seq"},
	})
	require.NoError(t, err)
	require.Equal(t, floor, floor2)

	// No strip semantics ⇒ nothing verified.
	floor3, err := l.CleanWithSpec(CleanSpec{Ceiling: At(hw)})
	require.NoError(t, err)
	require.Equal(t, int64(-1), floor3)
}

// A block-mode segment must roll at maxSegmentBlocks even when far below its
// byte cap: per-append tiny blocks otherwise accumulate millions of live
// blockRefs in the ACTIVE segment (which no clean ever rewrites).
func TestSegmentRollsAtBlockCap(t *testing.T) {
	opts := Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 1 << 30, // byte cap far out of reach
		Compact:         true,
		Compression:     compress.Zstd,
	}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()

	for i := 0; i < maxSegmentBlocks+64; i++ {
		_, err := l.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("k%06d", i)),
			Value: []byte("v"),
		}})
		require.NoError(t, err)
	}
	l.mu.RLock()
	nSegs := len(l.segments)
	activeBlocks := len(l.segments[len(l.segments)-1].blocks)
	l.mu.RUnlock()
	require.GreaterOrEqual(t, nSegs, 2, "block cap must have rolled the segment")
	require.Less(t, activeBlocks, maxSegmentBlocks, "active segment must be under the cap")
}

// The consolidation veto must key on block COUNT vs the target layout, not
// an average-size floor: multi-KB logical batches (view-output shape) once
// dodged a size-floor veto while segments carried 16k blocks each.
func TestConsolidationVetoOnBatchShape(t *testing.T) {
	opts := Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 1 << 30,
		Compact:         true,
		Compression:     compress.Zstd,
	}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()

	// ~4KB logical batches: one block each, avg block size well above any
	// size floor, but 16k of them per segment (the block cap rolls it).
	batch := make([]*Message, 8)
	n := 0
	for i := 0; i < maxSegmentBlocks+512; i++ {
		for j := range batch {
			n++
			batch[j] = &Message{
				Key:   []byte(fmt.Sprintf("k%07d", n)),
				Value: []byte(fmt.Sprintf("v%0500d", n)),
			}
		}
		_, err := l.Append(batch)
		require.NoError(t, err)
	}
	l.SetHighWatermark(l.NewestOffset())

	l.mu.RLock()
	sealed := l.segments[0]
	blocksBefore := len(sealed.blocks)
	l.mu.RUnlock()
	require.GreaterOrEqual(t, blocksBefore, maxSegmentBlocks, "need a full sealed segment")
	require.True(t, sealed.needsBlockConsolidation(), "batch-shaped segment must trip the veto")

	require.NoError(t, l.Clean())
	l.mu.RLock()
	blocksAfter := len(l.segments[0].blocks)
	l.mu.RUnlock()
	require.Less(t, blocksAfter, blocksBefore/10, "clean must consolidate batch-shaped blocks (before %d after %d)", blocksBefore, blocksAfter)
}

// A budgeted clean must consolidate exactly its slice per pass and converge
// over several passes — the incremental-clean contract that lets a
// short-lived process pay down a large debt.
func TestIncrementalCleanBudget(t *testing.T) {
	opts := Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 256 << 10,
		Compact:         true,
		Compression:     compress.Zstd,
	}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()

	for i := 0; i < 20000; i++ {
		_, err := l.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("key-%06d", i)),
			Value: []byte(fmt.Sprintf("value-%06d-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", i)),
		}})
		require.NoError(t, err)
	}
	hw := l.NewestOffset()
	l.SetHighWatermark(hw)
	spec := CleanSpec{
		Ceiling: At(hw), StripBelow: hw, StripHeaders: []string{"pid", "epoch", "seq"},
		maxRewrites: 2,
	}

	debt := func() int {
		n := 0
		l.mu.RLock()
		for _, s := range l.segments[:len(l.segments)-1] {
			if s.needsBlockConsolidation() {
				n++
			}
		}
		l.mu.RUnlock()
		return n
	}
	require.Greater(t, debt(), 4, "need more debt segments than one budget")

	prev := debt()
	passes := 0
	var lastFloor int64 = -2
	for debt() > 0 && passes < 50 {
		floor, err := l.CleanWithSpec(spec)
		require.NoError(t, err)
		passes++
		d := debt()
		require.LessOrEqual(t, prev-d, 2+1, "one pass must not consolidate more than its budget")
		require.GreaterOrEqual(t, floor, lastFloor, "verified floor must never regress across budgeted passes")
		prev, lastFloor = d, floor
	}
	require.Zero(t, debt(), "budgeted passes must converge to zero debt (after %d passes)", passes)
	require.Greater(t, passes, 2, "convergence should have taken multiple passes")
}

// Non-compacted block-mode logs (the daemon's view output streams) must
// still consolidate their tiny blocks on Clean: content, offsets and read
// results identical, block count collapsed.
func TestConsolidationOnlyPassForNonCompactedLog(t *testing.T) {
	opts := Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 256 << 10,
		Compact:         false, // the daemon's uncompacted view-stream shape
		Compression:     compress.Zstd,
	}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()

	for i := 0; i < 12000; i++ {
		_, err := l.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("key-%06d", i%64)),
			Value: []byte(fmt.Sprintf("value-%06d-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", i)),
		}})
		require.NoError(t, err)
	}
	hw := l.NewestOffset()
	l.SetHighWatermark(hw)

	counts := func() (int, int) {
		cs := l.SegmentBlockCounts()
		tot := 0
		for _, n := range cs[:len(cs)-1] {
			tot += n
		}
		return len(cs) - 1, tot
	}
	segs, before := counts()
	require.Greater(t, segs, 1)
	require.Greater(t, before, 8000, "need tiny-block debt")

	readN := func() map[int64]string {
		r, err := l.NewReader(From(0), Uncommitted(), Follow())
		require.NoError(t, err)
		out := map[int64]string{}
		headers := make([]byte, HeaderBufferLen)
		for i := 0; i < 12000; i++ {
			msg, off, _, _, err := r.ReadMessage(context.Background(), headers)
			require.NoError(t, err)
			out[off] = string(SerializedMessage(msg).Key()) + "|" + string(SerializedMessage(msg).Value())
		}
		return out
	}
	beforeRecs := readN()

	// Budgeted passes drain the debt; every record survives verbatim.
	for pass := 0; pass < 30; pass++ {
		_, err := l.CleanWithSpec(CleanSpec{maxRewrites: 2})
		require.NoError(t, err)
		if _, n := counts(); n < before/10 {
			break
		}
	}
	_, after := counts()
	require.Less(t, after, before/10, "consolidation-only pass must fire on non-compacted logs (before %d after %d)", before, after)
	require.Equal(t, beforeRecs, readN(), "records must be byte-identical after consolidation")
}

// A clean pass's scans (digest builds, rewrites, consolidation) must not
// populate the per-segment block caches: each populated cache retains a
// decode-buffer pair for the segment's lifetime, so cache-routed scans cost
// O(segments) heap per pass (run 32's ~500MB-1GB transients). Scans carry
// their own cache; the segments' stay cold until a real reader arrives.
func TestCleanScansLeaveSegmentCachesCold(t *testing.T) {
	opts := Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 64 << 10,
		Compact:         true,
		Compression:     compress.Zstd,
	}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()

	for i := 0; i < 3000; i++ {
		_, err := l.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("key-%06d", i%300)), // duplicates => real rewrites
			Value: []byte(fmt.Sprintf("value-%06d-abcdefghijklmnopqrstuvwxyz", i)),
		}})
		require.NoError(t, err)
	}
	l.SetHighWatermark(l.NewestOffset())

	_, err := l.CleanWithSpec(CleanSpec{Ceiling: At(l.NewestOffset())})
	require.NoError(t, err)

	l.mu.RLock()
	defer l.mu.RUnlock()
	require.Greater(t, len(l.segments), 1)
	for _, s := range l.segments {
		s.cache.mu.Lock()
		start, dataLen, rawLen := s.cache.start, len(s.cache.data), len(s.cache.raw)
		s.cache.mu.Unlock()
		require.Equal(t, int64(-1), start, "segment %d cache populated by clean scan", s.BaseOffset)
		require.Zero(t, dataLen, "segment %d cache holds decode buffer", s.BaseOffset)
		require.Zero(t, rawLen, "segment %d cache holds raw buffer", s.BaseOffset)
	}
}
