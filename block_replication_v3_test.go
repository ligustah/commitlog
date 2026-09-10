package commitlog

import (
	"testing"

	"github.com/ligustah/commitlog/blockv3"
	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// A version-3 stream replicates as its blocks: the follower stores the
// leader's version-3 blocks byte for byte, header included, beside the
// version-2 ones, and every reader answers the same on both logs.
func TestAVersion3LogReplicatesByteForByte(t *testing.T) {
	src, cleanupSrc := v3Log(t, compress.Snappy)
	defer cleanupSrc()
	appendBatches(t, src, 5)
	_, err := src.AppendBatch(batchMeta, batchMsgs(40))
	require.NoError(t, err)
	_, err = src.AppendBatch(BatchMeta{ProducerID: 42, ProducerEpoch: 3},
		[]*Message{{Value: []byte("commit"), Attributes: AttrControl, LeaderEpoch: 1}})
	require.NoError(t, err)
	_, err = src.AppendBatch(BatchMeta{}, batchMsgs(3))
	require.NoError(t, err)
	stripped := v3Header(compress.None)
	stripped.Flags, stripped.TxNonce = blockv3.FlagStripped, 0
	recs := v3Records(3, "y")
	recs[1].OffsetDelta, recs[2].OffsetDelta = 4, 9
	_, err = src.AppendPreframed(producerBlock(t, stripped, recs))
	require.NoError(t, err)
	appendBatches(t, src, 2)

	// Into a version-3 follower, the follower may not have switched yet: a
	// block is the leader's word, whatever the follower's option says.
	for name, dst := range map[string]*commitLog{"version 3": nil, "version 2": nil} {
		var cleanup func()
		if name == "version 3" {
			dst, cleanup = v3Log(t, compress.Snappy)
		} else {
			dst, cleanup = blockLog(t, compress.Zstd)
		}
		replicateBlocks(t, src, dst, 1<<20)
		require.Equal(t, physicalBytes(t, src), physicalBytes(t, dst), name)
		require.Equal(t, identities(t, src), identities(t, dst), name)
		require.Equal(t, readFrom(t, src), readFrom(t, dst), name)
		cleanup()
	}

	// A raw follower takes the framing.
	raw, cleanupRaw := setupWithOptions(t, Options{Path: tempDir(t), MaxSegmentBytes: 1 << 20})
	defer cleanupRaw()
	replicateBlocks(t, src, raw, 1<<20)
	require.False(t, raw.activeSegment().BlockMode())
	require.Equal(t, identities(t, src), identities(t, raw))
}

func TestAppendBlockRefusesABadVersion3Block(t *testing.T) {
	src, cleanupSrc := v3Log(t, compress.Snappy)
	defer cleanupSrc()
	_, err := src.AppendBatch(batchMeta, batchMsgs(40))
	require.NoError(t, err)
	_, blocks, err := src.ReadBlocks(0, 1<<20)
	require.NoError(t, err)
	require.Len(t, blocks, 1)
	sound := blocks[0]

	dst, cleanupDst := v3Log(t, compress.Snappy)
	defer cleanupDst()
	_, err = dst.AppendBlock(sound)
	require.NoError(t, err)
	tail := dst.NewestOffset()
	damaged := func(mutate func(d []byte)) Block {
		b := sound
		b.Data = append([]byte(nil), sound.Data...)
		mutate(b.Data)
		return b
	}
	gapped := v3Records(3, "x")
	gapped[2].OffsetDelta = 3
	cases := map[string]Block{
		"at or below the tail": sound,
		"a corrupt payload":    damaged(func(d []byte) { d[len(d)-1] ^= 0xff }),
		"a corrupt header":     damaged(func(d []byte) { d[40] ^= 0xff }),
		"a hole in an identity-bearing block": {
			Data: producerBlock(t, v3Header(compress.None), gapped), Records: 3},
	}
	for name, block := range cases {
		_, err := dst.AppendBlock(block)
		require.ErrorIs(t, err, ErrMessageSetRefused, name)
		require.Equal(t, tail, dst.NewestOffset(), name)
	}
}
