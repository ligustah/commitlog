package commitlog

import (
	"fmt"
	"testing"

	"github.com/ligustah/commitlog/blockv3"
	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// A log that never compacts still strips: a spec naming StripBelow runs the
// pass minus the key removals, so every record survives — one key repeated
// 1100 times included — with its identity gone, the marker below the boundary
// removed, and the once identity-bearing blocks consolidated.
func TestAStripOnlyPassOnANonCompactedLog(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path: tempDir(t), MaxSegmentBytes: 1 << 20, Compression: compress.Zstd,
		BlockFormat: blockv3.Version, DisableAutoClean: true,
	})
	defer cleanup()
	require.False(t, l.Compact)
	_, err := l.AppendBatch(BatchMeta{ProducerID: 9, ProducerEpoch: 1},
		[]*Message{{Value: []byte("commit"), Attributes: AttrControl}})
	require.NoError(t, err)
	for i := 0; i < 1100; i++ {
		_, err := l.AppendBatch(BatchMeta{ProducerID: 9, ProducerEpoch: 1, BaseSequence: int32(i), Nonce: 5},
			[]*Message{{Key: []byte("k"), Value: []byte(fmt.Sprint(i))}})
		require.NoError(t, err)
	}
	require.NoError(t, l.split(l.activeSegment()))
	boundary := l.NewestOffset() + 1
	_, err = l.AppendBatch(BatchMeta{ProducerID: 9, ProducerEpoch: 1, BaseSequence: 1100},
		[]*Message{{Key: []byte("k"), Value: []byte("tail")}})
	require.NoError(t, err)
	l.SetHighWatermark(l.NewestOffset())
	seg0 := l.segments[0]
	require.False(t, seg0.needsBlockConsolidation(), "identity-bearing: nothing to consolidate yet")
	before := readFrom(t, l)

	spec := CleanSpec{StripBelow: boundary,
		StripHeaders: []string{hdrProducerID, hdrProducerEpoch, hdrSequence, hdrNonce}}
	verified, err := l.CleanWithSpec(spec)
	require.NoError(t, err)
	require.Equal(t, boundary-1, verified, "the pass verified the whole sealed prefix")

	got := readFrom(t, l)
	delete(before, 0)
	require.Equal(t, before, got, "every data record survives, the marker below the boundary does not")
	for _, id := range identities(t, l) {
		require.Equal(t, id.offset >= boundary, id.hasIdentity, "offset %d", id.offset)
	}
	seg0 = l.segments[0]
	require.Less(t, blockCountOf(seg0), 30, "stripped, the blocks merge")

	// Converged: a second pass rewrites nothing and reads only the active
	// tail, whose digest is rebuilt in memory every pass.
	scans := segmentScans.Load()
	_, err = l.CleanWithSpec(spec)
	require.NoError(t, err)
	require.Same(t, seg0, l.segments[0])
	require.LessOrEqual(t, segmentScans.Load()-scans, int64(1))
}
