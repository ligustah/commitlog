package commitlog

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ligustah/commitlog/compress"
)

// prefix_read.go reasons about a scattered read as a choice between reading a
// contiguous span and addressing each record, and prices it in the bytes between
// the wanted records. On a BLOCK-COMPRESSED segment neither half of that holds:
// a record cannot be addressed, only its whole block can, and the bytes between
// two records in one block are not transferred separately from the ones that
// are. The unit of both cost and work is the block.
//
// These two tests pin the consequence from both sides. The first is the one that
// was wrong: where nothing can be skipped, no budget can buy anything. The
// second is its guard: where whole blocks CAN be skipped, the budget must still
// do what it says, or the fix would have quietly turned the setting off.

// blockCostLog builds a tiered, block-compressed log whose blocks each hold
// `batch` records, one in every `every` of them carrying a key under "want:".
//
// Records are appended in BATCHES because that is what makes a block a block:
// the append path writes one block per WriteMessageSet, so a per-record append
// would give a per-record block and there would be no inside-a-block gap for
// anything to be wrong about. A batching producer is also the shape the tier
// actually sees — a per-append block is consolidated away by the first clean
// that rewrites the segment (see needsBlockConsolidation).
func blockCostLog(t *testing.T, batches, batch, every int) (*commitLog, *costStore, int64) {
	t.Helper()
	dir := tempDir(t)
	fs, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)
	store := &costStore{FileSegmentStore: fs}

	l, cleanup := setupWithOptions(t, Options{
		Path: dir,
		// Several blocks per segment, and several segments: a segment holding
		// one block would make "skip a block" and "skip a segment" the same
		// decision and neither test could tell them apart.
		MaxSegmentBytes:  256 << 10,
		Compression:      compress.Zstd,
		Compact:          true,
		Tiers:            oneTier(store),
		DisableAutoClean: true,
	})
	t.Cleanup(cleanup)

	n := 0
	for b := 0; b < batches; b++ {
		msgs := make([]*Message, 0, batch)
		for i := 0; i < batch; i++ {
			m := &Message{
				Key:   []byte(fmt.Sprintf("other:%06d", n)),
				Value: make([]byte, 200),
			}
			if n%every == 0 {
				m.Key = []byte(fmt.Sprintf("want:%06d", n))
				m.Value = []byte("hit")
			}
			msgs = append(msgs, m)
			n++
		}
		offs, err := l.Append(msgs)
		require.NoError(t, err)
		l.SetHighWatermark(offs[len(offs)-1])
	}

	// Digests, for the same reason costLog needs them: without one a prefix read
	// scans and filters instead of planning runs, and no coalesce budget applies.
	requireCleanOK(t, l, CleanSpec{Ceiling: At(l.HighWatermark())})

	bound := l.ActiveSegmentBase() - 1
	moved, err := l.OffloadBefore(l.ActiveSegmentBase())
	require.NoError(t, err)
	require.NotZero(t, moved)
	return l, store, bound
}

// blockLayout reports the offloaded segments' block shape, and asserts the
// premise every test here stands on: that they are block-compressed at all. A
// codec that declined to compress would leave raw blocks, which is fine — a raw
// block is still a block and still the unit of transfer — but NO blocks would
// mean these tests were measuring the ordinary path under a compressed name.
func blockLayout(t *testing.T, l *commitLog, bound int64) (blocks int, minLogical int64) {
	t.Helper()
	minLogical = -1
	for _, seg := range l.segmentsSnapshot() {
		if seg.BaseOffset > bound {
			continue
		}
		require.NoError(t, seg.ensureBlocksLoaded())
		seg.RLock()
		require.True(t, seg.blockMode, "segment %d is not block-compressed", seg.BaseOffset)
		for _, b := range seg.blocks {
			blocks++
			if minLogical < 0 || b.logicalLen < minLogical {
				minLogical = b.logicalLen
			}
		}
		seg.RUnlock()
	}
	require.NotZero(t, blocks, "no blocks: the fixture is not on the block path")
	return blocks, minLogical
}

// measure returns the requests and bytes one full prefix read costs at a given
// tier coalesce budget.
func measurePrefixCost(t *testing.T, l *commitLog, store *costStore, opts []ReadOption, want []readRec, budget int64) (int, int64) {
	t.Helper()
	l.Options.PrefixReadTierCoalesceBytes = budget
	store.reset()
	r, err := l.NewReader(opts...)
	require.NoError(t, err)
	requireRecsEq(t, want, drainReader(t, r), fmt.Sprintf("budget=%d", budget))
	return store.totals()
}

// When every block holds a wanted record, a run boundary can skip NOTHING: the
// blocks are contiguous in the file, so the physical bytes between one hit's
// block and the next hit's block are zero. Splitting there does not avoid a
// transfer, it repeats one — each run gets its own single-entry blockCache
// (fetchRuns), so the same block is fetched and decompressed once per run that
// touches it.
//
// So the cost must be FLAT across every budget, including "never coalesce".
// That is the assertion, and it is deliberately equality rather than a bound:
// the claim is not that a small budget is cheap, it is that on this shape the
// budget has nothing to decide.
func TestPrefixReadOverBlocksThatAllHoldHitsIsBudgetIndependent(t *testing.T) {
	// 200 records per block, a hit every 30, so ~6 hits per block spaced about
	// 6KB apart — comfortably wider than the 4KB tier default, which is what
	// makes today's planner split inside the block rather than at its edge.
	l, store, bound := blockCostLog(t, 24, 200, 30)
	opts := []ReadOption{KeyPrefix([]byte("want:")), Until(bound)}
	spec, err := l.resolve(opts)
	require.NoError(t, err)
	want := scanFiltered(t, l, spec)
	require.NotEmpty(t, want)

	blocks, minLogical := blockLayout(t, l, bound)
	t.Logf("%d hits over %d blocks, smallest block %d logical bytes", len(want), blocks, minLogical)
	require.Greater(t, len(want), blocks,
		"need more hits (%d) than blocks (%d), or some block holds none and this "+
			"is measuring the skippable shape instead", len(want), blocks)

	// One read before measuring: the first touch of an offloaded block segment
	// fetches its block table, and that request belongs to opening the segment,
	// not to the budget under test.
	measurePrefixCost(t, l, store, opts, want, 4<<10)

	budgets := []int64{-1, 1 << 10, 4 << 10, 16 << 10, 64 << 10, 1 << 20}
	baseReqs, baseBytes := measurePrefixCost(t, l, store, opts, want, budgets[0])
	t.Logf("%12s %10s %12s", "budget", "requests", "bytes")
	t.Logf("%12s %10d %12d", "none", baseReqs, baseBytes)
	for _, b := range budgets[1:] {
		reqs, bytes := measurePrefixCost(t, l, store, opts, want, b)
		t.Logf("%12d %10d %12d", b, reqs, bytes)
		require.Equal(t, baseReqs, reqs,
			"budget %d issued %d requests where the smallest budget issued %d; "+
				"with a hit in every block there is nothing for a budget to skip, "+
				"so a difference here is the same block being fetched more than once",
			b, reqs, baseReqs)
		require.Equal(t, baseBytes, bytes,
			"budget %d transferred %d bytes where the smallest budget transferred %d",
			b, bytes, baseBytes)
	}
}

// The other side. When the hits are far enough apart that WHOLE blocks lie
// between them, skipping is real again and the budget must still buy what it
// claims. Without this, "never split inside a block" could be satisfied by never
// splitting at all.
//
// Asserted on REQUESTS alone, not on the requests-for-bytes trade the raw path
// makes. A run costs exactly one store request — one Stream, opened at the run's
// first record and closed at its last — so requests is the axis the budget
// governs and the axis a store bills hardest. Bytes do not move the way the raw
// model predicts: measured here, the 4MB budget transferred FEWER bytes than the
// 1KB one (15207 against 20289) rather than more, because a merged run replaces
// two block fetches with one stream rather than adding the span between them.
// Asserting a trade that this path does not make would be asserting the raw
// segment's economics under a block segment's name — the same mistake the
// planner itself was making.
func TestPrefixReadOverBlocksWithGapsStillHonoursTheBudget(t *testing.T) {
	// A hit every 900 records against 200-record blocks: most blocks hold none,
	// so consecutive hits are separated by entire blocks of physical bytes.
	l, store, bound := blockCostLog(t, 40, 200, 900)
	opts := []ReadOption{KeyPrefix([]byte("want:")), Until(bound)}
	spec, err := l.resolve(opts)
	require.NoError(t, err)
	want := scanFiltered(t, l, spec)
	require.NotEmpty(t, want)

	blocks, minLogical := blockLayout(t, l, bound)
	t.Logf("%d hits over %d blocks, smallest block %d logical bytes", len(want), blocks, minLogical)
	require.Greater(t, blocks, len(want),
		"need more blocks (%d) than hits (%d), or no block is skippable and this "+
			"is measuring the dense shape instead", blocks, len(want))

	measurePrefixCost(t, l, store, opts, want, 4<<10)

	small, smallBytes := measurePrefixCost(t, l, store, opts, want, 1<<10)
	large, largeBytes := measurePrefixCost(t, l, store, opts, want, 4<<20)
	t.Logf("1KB budget: %d requests, %d bytes", small, smallBytes)
	t.Logf("4MB budget: %d requests, %d bytes", large, largeBytes)
	require.Less(t, large, small,
		"the largest budget saved no requests, so the budget is doing nothing on "+
			"a shape where whole blocks can be skipped")
}
