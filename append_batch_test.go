package commitlog

import (
	"testing"

	"github.com/ligustah/commitlog/blockv3"
	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// v3Log opens a log writing version-3 blocks under codec.
func v3Log(t *testing.T, codec compress.Codec) (*commitLog, func()) {
	t.Helper()
	return setupWithOptions(t, Options{
		Path: tempDir(t), MaxSegmentBytes: 1 << 20, Compression: codec,
		BlockFormat: blockv3.Version, DisableAutoClean: true,
	})
}

var batchMeta = BatchMeta{ProducerID: 42, ProducerEpoch: 3, BaseSequence: 500, Nonce: 99}

// batchMsgs are n compressible messages with no timestamp, epoch 1.
func batchMsgs(n int) []*Message {
	msgs := compressibleMsgs(n)
	for _, m := range msgs {
		m.Timestamp = 0
	}
	return msgs
}

// identityOf is the part of a record's metadata a batch decides, under either
// block format.
type identityOf struct {
	offset                 int64
	pid                    uint64
	epoch                  uint32
	seq                    int32
	nonce                  uint64
	hasIdentity            bool
	attributes             int8
	timestamp, leaderEpoch uint64
}

func identities(t *testing.T, l *commitLog) []identityOf {
	t.Helper()
	var out []identityOf
	for _, m := range metadataFrom(t, l) {
		out = append(out, identityOf{
			m.Offset, m.ProducerID, m.ProducerEpoch, m.Sequence, m.Nonce, m.HasIdentity,
			m.Attributes, uint64(m.Timestamp), m.LeaderEpoch,
		})
	}
	return out
}

// One accessor for both formats: a batch appended to a log writing version 3
// and the same batch appended to one writing version 2 read back identically,
// identity and all, while only the former stored a version-3 block.
func TestAppendBatchReadsTheSameUnderEitherFormat(t *testing.T) {
	v3, cleanup3 := v3Log(t, compress.Snappy)
	defer cleanup3()
	v2, cleanup2 := blockLog(t, compress.Snappy)
	defer cleanup2()

	batch := func() []*Message {
		msgs := batchMsgs(40)
		msgs[7].Attributes = AttrTombstone
		msgs[7].Headers = map[string][]byte{"trace": []byte("t")}
		for _, m := range msgs {
			m.Timestamp = 1_700_000_000_000
		}
		return msgs
	}
	for _, l := range []*commitLog{v3, v2} {
		appendBatches(t, l, 2)
		offs, err := l.AppendBatch(batchMeta, batch())
		require.NoError(t, err)
		require.Equal(t, int64(2), offs[0])
		require.Len(t, offs, 40)
		marker := &Message{Value: []byte("commit"), Attributes: AttrControl, LeaderEpoch: 1}
		_, err = l.AppendBatch(BatchMeta{ProducerID: 42, ProducerEpoch: 3}, []*Message{marker})
		require.NoError(t, err)
		require.NotZero(t, marker.Timestamp, "the append clock is written back")
		marker.Timestamp = 0
	}

	require.Equal(t, readFrom(t, v2), readFrom(t, v3))
	got3, got2 := identities(t, v3), identities(t, v2)
	require.Len(t, got3, 43)
	for i := range got3 {
		got3[i].timestamp, got2[i].timestamp = 0, 0 // the marker's clock reading differs
	}
	require.Equal(t, got2, got3)
	require.Equal(t, identityOf{offset: 9, pid: 42, epoch: 3, seq: 507, nonce: 99, hasIdentity: true,
		attributes: AttrTombstone, leaderEpoch: 1}, got3[9])
	require.Equal(t, identityOf{offset: 42, pid: 42, epoch: 3, hasIdentity: true,
		attributes: AttrControl, leaderEpoch: 1}, got3[42], "a control batch with no nonce")

	_, blocks, err := v3.ReadBlocks(2, 1<<20)
	require.NoError(t, err)
	require.Equal(t, blockv3.Version, blocks[0].Data[1], "the version-3 log stored a version-3 block")
	h, err := blockv3.DecodeHeader(blocks[0].Data)
	require.NoError(t, err)
	require.Equal(t, compress.Snappy, h.Codec, "40 compressible records clear compressMinBlock")
	require.Equal(t, blockv3.FlagTransactional, h.Flags)
	require.Equal(t, int64(1_700_000_000_000), h.BaseTimestamp)
	require.Equal(t, blockv3.Version, blocks[1].Data[1])
	h, err = blockv3.DecodeHeader(blocks[1].Data)
	require.NoError(t, err)
	require.Equal(t, compress.None, h.Codec, "one small record is stored raw, as appendBlock would")
	require.Equal(t, blockv3.FlagControl, h.Flags)
	_, blocks, err = v2.ReadBlocks(2, 1<<20)
	require.NoError(t, err)
	require.Equal(t, BlockFormatVersion, blocks[0].Data[1], "the version-2 log stored headers on records")
}

// Under version 3 a log with no codec is still block-framed, and Append
// keeps writing version-2 blocks beside the batches.
func TestVersion3WithNoCodecIsBlockFramed(t *testing.T) {
	l, cleanup := v3Log(t, compress.None)
	defer cleanup()
	appendBatches(t, l, 3)
	_, err := l.AppendBatch(batchMeta, batchMsgs(2))
	require.NoError(t, err)
	require.NoError(t, l.Sync(l.NewestOffset()))

	f, err := ClassifySegment(l.activeSegment().logPath())
	require.NoError(t, err)
	require.True(t, f.Blocked)
	require.True(t, f.Readable())
	seg, err := InspectSegment(l.activeSegment().logPath())
	require.NoError(t, err)
	blocks, err := seg.Blocks()
	require.NoError(t, err)
	require.Len(t, blocks, 2)
	require.Equal(t, BlockFormatVersion, blocks[0].Version)
	require.Equal(t, blockv3.Version, blocks[1].Version)
	require.Equal(t, uint32(2), blocks[1].Records)
	var offsets []int64
	require.NoError(t, seg.Records(func(r RecordInfo) error {
		require.True(t, r.CRCValid)
		offsets = append(offsets, r.Offset)
		return nil
	}))
	require.Equal(t, []int64{0, 1, 2, 3, 4}, offsets, "the inspector walks both formats")
	require.Len(t, identities(t, l), 5)
}

func TestAppendBatchRefusalsWriteNothing(t *testing.T) {
	l, cleanup := v3Log(t, compress.None)
	defer cleanup()
	appendBatches(t, l, 1)
	tail := l.NewestOffset()

	with := func(f func(msgs []*Message)) []*Message {
		msgs := batchMsgs(3)
		f(msgs)
		return msgs
	}
	cases := map[string][]*Message{
		"empty":                     {},
		"a reserved header":         with(func(m []*Message) { m[1].Headers = map[string][]byte{"seq": {1}} }),
		"two timestamps":            with(func(m []*Message) { m[1].Timestamp, m[2].Timestamp = 6, 7 }),
		"two epochs":                with(func(m []*Message) { m[0].LeaderEpoch = 2 }),
		"control and data together": with(func(m []*Message) { m[2].Attributes = AttrControl }),
	}
	for name, msgs := range cases {
		_, err := l.AppendBatch(batchMeta, msgs)
		require.ErrorIs(t, err, ErrMessageSetRefused, name)
		require.Equal(t, tail, l.NewestOffset(), name)
	}
	l2, cleanup2 := blockLog(t, compress.None)
	defer cleanup2()
	_, err := l2.AppendBatch(batchMeta, cases["a reserved header"])
	require.ErrorIs(t, err, ErrMessageSetRefused, "reserved under version 2 as well")
	require.Equal(t, int64(-1), l2.NewestOffset())
}

// producerBlock is what a producer hands AppendPreframed: built elsewhere, with
// whatever it thought the base offset, epoch and timestamp were.
func producerBlock(t *testing.T, h blockv3.Header, recs []blockv3.Record) []byte {
	t.Helper()
	h.BaseOffset, h.LeaderEpoch, h.BaseTimestamp = 9_000, 77, 5
	block, err := blockv3.EncodeBlock(h, recs)
	require.NoError(t, err)
	return block
}

// A pre-framed block lands with its header rewritten to the log's tail, epoch
// and clock, and its payload untouched.
func TestAppendPreframedRewritesOnlyTheHeader(t *testing.T) {
	l, cleanup := v3Log(t, compress.Snappy)
	defer cleanup()
	require.NoError(t, l.NewLeaderEpoch(3))
	appendBatches(t, l, 5)

	block := producerBlock(t, v3Header(compress.Zstd), v3Records(40, "x"))
	payload := append([]byte(nil), block[blockv3.HeaderLen:]...)
	offs, err := l.AppendPreframed(block)
	require.NoError(t, err)
	require.Len(t, offs, 40)
	require.Equal(t, int64(5), offs[0])
	require.Equal(t, int64(44), l.NewestOffset())

	h, err := blockv3.DecodeHeader(block)
	require.NoError(t, err)
	require.Equal(t, int64(5), h.BaseOffset, "rewritten in place")
	require.Equal(t, uint64(3), h.LeaderEpoch)
	require.NotEqual(t, int64(5), h.BaseTimestamp)
	require.Equal(t, int64(42), h.ProducerID)
	require.Equal(t, compress.Zstd, h.Codec, "the producer's codec, not the log's")
	_, blocks, err := l.ReadBlocks(5, 1<<20)
	require.NoError(t, err)
	require.Equal(t, block, blocks[0].Data, "stored verbatim")
	require.Equal(t, payload, blocks[0].Data[blockv3.HeaderLen:])

	got := identities(t, l)
	require.Len(t, got, 45)
	require.Equal(t, identityOf{offset: 17, pid: 42, epoch: 3, seq: 512, nonce: 99, hasIdentity: true,
		timestamp: uint64(h.BaseTimestamp), leaderEpoch: 3}, got[17])

	// A stripped block may have holes; its records are its own offsets.
	stripped := v3Header(compress.None)
	stripped.Flags, stripped.TxNonce = blockv3.FlagStripped, 0
	recs := v3Records(3, "y")
	recs[1].OffsetDelta, recs[2].OffsetDelta = 4, 9
	offs, err = l.AppendPreframed(producerBlock(t, stripped, recs))
	require.NoError(t, err)
	require.Equal(t, []int64{45, 49, 54}, offs)
	require.False(t, identities(t, l)[46].hasIdentity)

	require.NoError(t, l.Close())
	reopened, err := New(Options{
		Path: l.Path, MaxSegmentBytes: 1 << 20, Compression: compress.Snappy, BlockFormat: blockv3.Version,
	})
	require.NoError(t, err)
	defer reopened.Close()
	require.Equal(t, got, identities(t, reopened.(*commitLog))[:45])
}

func TestAppendPreframedRefusalsWriteNothing(t *testing.T) {
	l, cleanup := v3Log(t, compress.Snappy)
	defer cleanup()
	appendBatches(t, l, 1)
	tail := l.NewestOffset()

	sound := producerBlock(t, v3Header(compress.Snappy), v3Records(40, "x"))
	damaged := func(mutate func(b []byte)) []byte {
		b := append([]byte(nil), sound...)
		mutate(b)
		return b
	}
	gapped := v3Records(3, "x")
	gapped[2].OffsetDelta = 3
	noFlag := v3Header(compress.None)
	noFlag.Flags = 0
	v2, _, err := l.ReadBlocks(0, 1<<20)
	require.NoError(t, err)
	cases := map[string][]byte{
		"a version-2 message set":             v2,
		"a corrupt payload":                   damaged(func(b []byte) { b[len(b)-1] ^= 0xff }),
		"a corrupt header":                    damaged(func(b []byte) { b[40] ^= 0xff }),
		"a hole in an identity-bearing block": producerBlock(t, v3Header(compress.None), gapped),
		"a nonce without the flag":            producerBlock(t, noFlag, v3Records(2, "x")),
	}
	for name, block := range cases {
		_, err := l.AppendPreframed(block)
		require.ErrorIs(t, err, ErrMessageSetRefused, name)
		require.Equal(t, tail, l.NewestOffset(), name)
	}

	v2Log, cleanup2 := blockLog(t, compress.Snappy)
	defer cleanup2()
	_, err = v2Log.AppendPreframed(sound)
	require.ErrorIs(t, err, ErrMessageSetRefused, "a log writing version 2 takes no version-3 block")
}

// A log opened under version 3 over a raw active segment writes the framing
// into it, so the switch never fails an append.
func TestAVersion3AppendIntoARawSegmentWritesFraming(t *testing.T) {
	path := tempDir(t)
	raw, err := New(Options{Path: path, MaxSegmentBytes: 1 << 20})
	require.NoError(t, err)
	appendBatches(t, raw.(*commitLog), 2)
	require.NoError(t, raw.Close())

	l, cleanup := setupWithOptions(t, Options{Path: path, MaxSegmentBytes: 1 << 20, BlockFormat: blockv3.Version})
	defer cleanup()
	require.False(t, l.activeSegment().BlockMode())
	_, err = l.AppendBatch(batchMeta, batchMsgs(2))
	require.NoError(t, err)
	_, err = l.AppendPreframed(producerBlock(t, v3Header(compress.None), v3Records(2, "x")))
	require.NoError(t, err)
	got := identities(t, l)
	require.Len(t, got, 6)
	require.True(t, got[2].hasIdentity)
	require.Equal(t, int32(501), got[5].seq)
	head, blocks, err := l.ReadBlocks(0, 1<<20)
	require.NoError(t, err)
	require.Empty(t, blocks)
	require.NotEmpty(t, head)
}

func TestBlockFormatIsValidatedAtOpen(t *testing.T) {
	_, err := New(Options{Path: tempDir(t), BlockFormat: 9})
	require.ErrorIs(t, err, ErrInvalidOptions)
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t)})
	defer cleanup()
	require.Equal(t, BlockFormatVersion, l.BlockFormat, "zero means this build's default")
}
