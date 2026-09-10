package commitlog

import (
	"fmt"
	"os"
	"testing"

	"github.com/ligustah/commitlog/blockv3"
	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// keyed are n keyed messages with no timestamp, epoch 1, valued so the batch
// clears compressMinBlock only when n is large.
func keyed(prefix string, n int, val string) []*Message {
	msgs := make([]*Message, n)
	for i := range msgs {
		msgs[i] = &Message{Key: []byte(fmt.Sprintf("%s%02d", prefix, i)), Value: []byte(val), LeaderEpoch: 1}
	}
	return msgs
}

// v3BlocksOf are the version-3 blocks of the segment holding offset, keyed by
// first offset.
func v3BlocksOf(t *testing.T, l *commitLog, offset int64) map[int64][]byte {
	t.Helper()
	_, blocks, err := l.ReadBlocks(offset, 1<<30)
	require.NoError(t, err)
	out := map[int64][]byte{}
	for _, b := range blocks {
		if b.Data[1] == blockv3.Version {
			out[b.FirstOffset] = b.Data
		}
	}
	return out
}

func blockCountOf(seg *segment) int {
	seg.RLock()
	defer seg.RUnlock()
	return len(seg.blocks)
}

// A compaction pass carries a version-3 block that still bears its identity,
// or is a control block, byte for byte when it would keep every record in it;
// a block it strips or thins goes the way its records go, and what survives of
// a control block is never merged with its neighbours.
func TestACleanCarriesIdentityBearingBlocksWhole(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path: tempDir(t), MaxSegmentBytes: 1 << 20, Compression: compress.Snappy,
		BlockFormat: blockv3.Version, Compact: true, DisableAutoClean: true,
	})
	defer cleanup()
	meta := func(pid uint64) BatchMeta { return BatchMeta{ProducerID: pid, ProducerEpoch: 1, Nonce: pid} }
	marker := func(pid uint64, n int) ([]int64, error) {
		msgs := make([]*Message, n)
		for i := range msgs {
			msgs[i] = &Message{Value: []byte("commit"), Attributes: AttrControl, LeaderEpoch: 1}
		}
		return l.AppendBatch(BatchMeta{ProducerID: pid, ProducerEpoch: 1}, msgs)
	}
	// A: identity-bearing, below the strip boundary — stripped, re-blocked.
	_, err := l.AppendBatch(meta(7), keyed("a", 40, "old"))
	require.NoError(t, err)
	// B: two markers straddling the boundary — the first is removed, the
	// second survives on its own.
	_, err = marker(7, 2)
	require.NoError(t, err)
	// C: identity-bearing, every record kept — verbatim.
	_, err = l.AppendBatch(meta(9), keyed("c", 40, "kept"))
	require.NoError(t, err)
	// D: a control block above the boundary — verbatim.
	_, err = marker(9, 1)
	require.NoError(t, err)
	// E: identity-less, thinned by F — merged.
	_, err = l.AppendBatch(BatchMeta{}, keyed("e", 40, "old"))
	require.NoError(t, err)
	// F: identity-bearing, supersedes ten of E's keys, itself kept whole.
	_, err = l.AppendBatch(meta(11), keyed("e", 10, "new"))
	require.NoError(t, err)
	require.Equal(t, int64(132), l.NewestOffset())
	require.NoError(t, l.split(l.activeSegment()))
	l.SetHighWatermark(l.NewestOffset())

	before := v3BlocksOf(t, l, 0)
	require.Len(t, before, 6)
	beforeRecords := readFrom(t, l)

	requireCleanOK(t, l, CleanSpec{
		Ceiling: At(l.HighWatermark()), StripBelow: 41,
		StripHeaders: []string{hdrProducerID, hdrProducerEpoch, hdrSequence, hdrNonce},
	})

	after := v3BlocksOf(t, l, 0)
	require.Equal(t, map[int64][]byte{42: before[42], 82: before[82], 123: before[123]}, after,
		"C, D and F pass whole and untouched; A, B and E are rewritten")
	got := readFrom(t, l)
	for off := int64(0); off <= 132; off++ {
		switch {
		case off == 40, off >= 83 && off < 93:
			require.NotContains(t, got, off, "offset %d is removed", off)
		default:
			require.Equal(t, beforeRecords[off], got[off], "offset %d", off)
		}
	}
	ids := identities(t, l)
	byOffset := map[int64]identityOf{}
	for _, id := range ids {
		byOffset[id.offset] = id
	}
	require.False(t, byOffset[5].hasIdentity, "A was stripped")
	require.True(t, byOffset[41].hasIdentity, "the surviving marker keeps its identity")
	require.Equal(t, uint64(9), byOffset[50].pid, "C keeps its identity")
	require.Equal(t, int32(8), byOffset[50].seq)
	require.Equal(t, uint64(11), byOffset[125].pid)

	_, blocks, err := l.ReadBlocks(0, 1<<30)
	require.NoError(t, err)
	var shape []string
	for _, b := range blocks {
		shape = append(shape, fmt.Sprintf("v%d@%d+%d", b.Data[1], b.FirstOffset, b.Records))
	}
	require.Equal(t, []string{
		"v2@0+40",  // A, stripped
		"v2@41+1",  // B's survivor, alone
		"v3@42+40", // C
		"v3@82+1",  // D
		"v2@93+30", // E's survivors
		"v3@123+10",
	}, shape)
}

// consolidationLog holds two sealed segments of tiny version-3 blocks: the
// first mixes identity-less blocks with a few identity-bearing and control
// ones, the second is identity-bearing throughout.
func consolidationLog(t *testing.T, path string) (*commitLog, func()) {
	t.Helper()
	l, cleanup := setupWithOptions(t, Options{
		Path: path, MaxSegmentBytes: 1 << 20, Compression: compress.Zstd,
		BlockFormat: blockv3.Version, DisableAutoClean: true,
	})
	for i := 0; i < 1100; i++ {
		_, err := l.AppendBatch(BatchMeta{}, keyed("s", 1, fmt.Sprint(i)))
		require.NoError(t, err)
		if i%100 == 0 {
			_, err := l.AppendBatch(BatchMeta{ProducerID: 7, ProducerEpoch: 1, BaseSequence: int32(i)}, keyed("p", 1, "x"))
			require.NoError(t, err)
			_, err = l.AppendBatch(BatchMeta{ProducerID: 7, ProducerEpoch: 1},
				[]*Message{{Value: []byte("commit"), Attributes: AttrControl}})
			require.NoError(t, err)
		}
	}
	require.NoError(t, l.split(l.activeSegment()))
	for i := 0; i < 1100; i++ {
		_, err := l.AppendBatch(BatchMeta{ProducerID: 9, ProducerEpoch: 1, BaseSequence: int32(i)}, keyed("q", 1, "y"))
		require.NoError(t, err)
	}
	require.NoError(t, l.split(l.activeSegment()))
	_, err := l.Append(keyed("t", 1, "tail"))
	require.NoError(t, err)
	l.SetHighWatermark(l.NewestOffset())
	return l, cleanup
}

// Consolidation merges the blocks it may — identity-less ones — and leaves the
// rest byte for byte; a segment with nothing to merge is not rewritten at all.
func TestAConsolidationMergesStrippedBlocksOnly(t *testing.T) {
	l, cleanup := consolidationLog(t, tempDir(t))
	defer cleanup()
	seg0, seg1 := l.segments[0], l.segments[1]
	require.True(t, seg0.needsBlockConsolidation())
	require.False(t, seg1.needsBlockConsolidation(), "identity-bearing blocks cannot be merged, so there is nothing to schedule")
	require.Equal(t, 1122, blockCountOf(seg0))

	before0, before1 := v3BlocksOf(t, l, 0), v3BlocksOf(t, l, seg1.BaseOffset)
	require.Len(t, before0, 1122)
	require.Len(t, before1, 1100)
	beforeRecords := readFrom(t, l)

	require.NoError(t, l.Clean())
	require.Same(t, seg1, l.segments[1], "not rewritten")
	require.Equal(t, before1, v3BlocksOf(t, l, seg1.BaseOffset))
	seg0 = l.segments[0]
	require.Less(t, blockCountOf(seg0), 60, "1100 identity-less blocks merge into a few")
	after0 := v3BlocksOf(t, l, 0)
	require.Len(t, after0, 22, "the 11 identity-bearing and 11 control blocks pass whole")
	for off, data := range after0 {
		require.Equal(t, before0[off], data, "block at %d", off)
	}
	require.False(t, seg0.needsBlockConsolidation(), "converged")
	require.Equal(t, beforeRecords, readFrom(t, l))
}

// The flags a rewrite decides by survive a reopen, whether the block table
// comes back from its sidecar or from a walk of the file.
func TestBlockFlagsSurviveAReopen(t *testing.T) {
	for _, walk := range []bool{false, true} {
		t.Run(fmt.Sprintf("walk=%t", walk), func(t *testing.T) {
			path := tempDir(t)
			l, _ := consolidationLog(t, path)
			require.NoError(t, l.Close())
			if walk {
				for _, seg := range l.segments {
					require.NoError(t, os.RemoveAll(localBlockTablePath(seg)))
				}
			}
			reopened, err := New(Options{
				Path: path, MaxSegmentBytes: 1 << 20, Compression: compress.Zstd,
				BlockFormat: blockv3.Version, DisableAutoClean: true,
			})
			require.NoError(t, err)
			defer reopened.Close()
			rl := reopened.(*commitLog)
			require.True(t, rl.segments[0].needsBlockConsolidation())
			require.False(t, rl.segments[1].needsBlockConsolidation())
		})
	}
}
