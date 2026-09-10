package commitlog

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/ligustah/commitlog/blockv3"
	"github.com/ligustah/commitlog/compress"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// blockLog opens a log whose appends are stored as blocks under codec.
func blockLog(t *testing.T, codec compress.Codec) (*commitLog, func()) {
	t.Helper()
	return setupWithOptions(t, Options{
		Path: tempDir(t), MaxSegmentBytes: 1 << 20, Compression: codec, DisableAutoClean: true,
	})
}

// appendBatches appends one message set per batch and returns the offsets of
// every record. Batches of 40 compressible records clear compressMinBlock and
// are stored compressed; a batch of one small record is stored raw.
func appendBatches(t *testing.T, l *commitLog, sizes ...int) []int64 {
	t.Helper()
	var offs []int64
	for _, n := range sizes {
		o, err := l.Append(compressibleMsgs(n))
		require.NoError(t, err)
		offs = append(offs, o...)
	}
	return offs
}

// replicateBlocks drives dst from src the way a follower would: ReadBlocks,
// AppendMessageSet for the head, AppendBlock for each block, resume after the
// last offset appended. maxBytes is the fetch budget.
func replicateBlocks(t *testing.T, src, dst *commitLog, maxBytes int) {
	t.Helper()
	for next := int64(0); next <= src.NewestOffset(); {
		head, blocks, err := src.ReadBlocks(next, maxBytes)
		require.NoError(t, err)
		require.True(t, len(head) > 0 || len(blocks) > 0, "a follower must always make progress")
		if len(head) > 0 {
			offs, err := dst.AppendMessageSet(head)
			require.NoError(t, err)
			next = offs[len(offs)-1] + 1
		}
		for _, b := range blocks {
			offs, err := dst.AppendBlock(b)
			require.NoError(t, err, "a block the source stores must be appendable verbatim")
			require.Len(t, offs, b.Records)
			require.Equal(t, b.FirstOffset, offs[0])
			next = offs[len(offs)-1] + 1
		}
	}
}

// physicalBytes is the segment file's bytes, which for a block segment are the
// blocks exactly as stored.
func physicalBytes(t *testing.T, l *commitLog) []byte {
	t.Helper()
	require.NoError(t, l.Sync(l.NewestOffset()))
	b, err := os.ReadFile(l.activeSegment().logPath())
	require.NoError(t, err)
	return b
}

// The contract that matters: a block ReadBlocks hands out is a block AppendBlock
// stores byte for byte, and every reader answers the same on both logs.
func TestReadBlocksRoundTripsIntoAnotherLogByteForByte(t *testing.T) {
	src, cleanupSrc := blockLog(t, compress.Snappy)
	defer cleanupSrc()
	dst, cleanupDst := blockLog(t, compress.Snappy)
	defer cleanupDst()

	appendBatches(t, src, 40, 1, 40, 40, 1)
	src.SetHighWatermark(src.NewestOffset())

	replicateBlocks(t, src, dst, 1<<20)
	dst.SetHighWatermark(dst.NewestOffset())

	require.Equal(t, src.NewestOffset(), dst.NewestOffset())
	require.Equal(t, readFrom(t, src), readFrom(t, dst))

	wantSet, err := src.ReadMessageSet(0, 1<<20)
	require.NoError(t, err)
	gotSet, err := dst.ReadMessageSet(0, 1<<20)
	require.NoError(t, err)
	require.Equal(t, wantSet, gotSet, "the logical framing must be identical")

	last, err := src.LastOffsetForLeaderEpoch(AtEpoch(1))
	require.NoError(t, err)
	gotLast, err := dst.LastOffsetForLeaderEpoch(AtEpoch(1))
	require.NoError(t, err)
	require.Equal(t, last, gotLast, "the leader-epoch history must follow the records")

	off, err := src.EarliestOffsetAfterTimestamp(20)
	require.NoError(t, err)
	gotOff, err := dst.EarliestOffsetAfterTimestamp(20)
	require.NoError(t, err)
	require.Equal(t, off, gotOff)

	require.Equal(t, physicalBytes(t, src), physicalBytes(t, dst),
		"the replica must hold the leader's blocks verbatim")
}

// An offset inside a block yields the rest of that block as head — the logical
// framing a follower appends as a message set — and whole blocks from the next
// boundary on. Head and blocks together are exactly what ReadMessageSet would
// have returned.
func TestReadBlocksFromInsideABlockYieldsHeadThenWholeBlocks(t *testing.T) {
	src, cleanup := blockLog(t, compress.Snappy)
	defer cleanup()
	appendBatches(t, src, 40, 40, 40)

	const mid = 50 // inside the second block, which holds 40..79
	head, blocks, err := src.ReadBlocks(mid, 1<<20)
	require.NoError(t, err)
	require.NotEmpty(t, head)
	require.Len(t, blocks, 1)
	require.Equal(t, int64(80), blocks[0].FirstOffset)
	require.Equal(t, int64(119), blocks[0].LastOffset)
	require.Equal(t, 40, blocks[0].Records)

	wantHead, err := src.ReadMessageSet(mid, len(head))
	require.NoError(t, err)
	require.Equal(t, wantHead, head, "head is the logical framing from the offset to the block's end")

	logical, err := compress.Snappy.DecompressInto(nil, blocks[0].Data[blockHeaderLen:])
	require.NoError(t, err)
	require.Equal(t, blocks[0].LogicalLen, len(logical))
	want, err := src.ReadMessageSet(mid, 1<<20)
	require.NoError(t, err)
	require.Equal(t, want, append(head, logical...),
		"head plus the blocks' framing is what ReadMessageSet returns for the same range")

	// At a boundary there is no head.
	head, blocks, err = src.ReadBlocks(40, 1<<20)
	require.NoError(t, err)
	require.Empty(t, head)
	require.Len(t, blocks, 2)
	require.Equal(t, int64(40), blocks[0].FirstOffset)
	require.Equal(t, int64(79), blocks[0].LastOffset)
	require.Equal(t, src.NewestOffset(), blocks[1].LastOffset)
}

// maxBytes is a budget, not a cut: the first unit comes back whole however
// small the budget, and nothing after it comes back partial.
func TestReadBlocksReturnsTheFirstUnitWholeAndNothingPartial(t *testing.T) {
	src, cleanup := blockLog(t, compress.Snappy)
	defer cleanup()
	appendBatches(t, src, 40, 40, 40)

	head, blocks, err := src.ReadBlocks(0, 1)
	require.NoError(t, err)
	require.Empty(t, head)
	require.Len(t, blocks, 1, "a budget below one block still yields that block")

	head, blocks, err = src.ReadBlocks(50, 1)
	require.NoError(t, err)
	require.NotEmpty(t, head, "a budget below the head still yields the head")
	require.Empty(t, blocks)

	_, all, err := src.ReadBlocks(0, 1<<20)
	require.NoError(t, err)
	require.Len(t, all, 3)
	exact := len(all[0].Data) + len(all[1].Data)
	_, blocks, err = src.ReadBlocks(0, exact)
	require.NoError(t, err)
	require.Len(t, blocks, 2, "a budget that fits two blocks yields exactly two")
	_, blocks, err = src.ReadBlocks(0, exact-1)
	require.NoError(t, err)
	require.Len(t, blocks, 1, "one byte short of the second block leaves it out")

	_, blocks, err = src.ReadBlocks(50, len(head))
	require.NoError(t, err)
	require.Empty(t, blocks, "a budget the head fills exactly has no room for a block")
}

// A block the source stored raw is appended raw, and a destination whose own
// codec differs keeps each block's codec — so one segment holds blocks under
// several codecs and reads all of them.
func TestAppendBlockKeepsEachBlocksCodec(t *testing.T) {
	src, cleanupSrc := blockLog(t, compress.Snappy)
	defer cleanupSrc()
	dst, cleanupDst := blockLog(t, compress.Zstd)
	defer cleanupDst()

	appendBatches(t, src, 1, 40, 1)
	replicateBlocks(t, src, dst, 1<<20)

	dseg := dst.activeSegment()
	dseg.RLock()
	codecs := []compress.Codec{}
	for _, b := range dseg.blocks {
		codecs = append(codecs, b.codec)
	}
	dseg.RUnlock()
	require.Equal(t, []compress.Codec{compress.None, compress.Snappy, compress.None}, codecs,
		"raw stays raw and snappy stays snappy in a zstd log")

	// The destination's OWN appends still use its codec, beside the copied ones.
	_, err := dst.Append(compressibleMsgs(40))
	require.NoError(t, err)
	require.Equal(t, readFrom(t, src), func() map[int64]string {
		got := readFrom(t, dst)
		for off := range got {
			if off > src.NewestOffset() {
				delete(got, off)
			}
		}
		return got
	}())
	require.Equal(t, physicalBytes(t, src), physicalBytes(t, dst)[:len(physicalBytes(t, src))])
}

// Every refusal AppendMessageSet makes, AppendBlock makes, plus the ones a
// block header adds — and each writes nothing.
func TestAppendBlockRefusalsWriteNothing(t *testing.T) {
	src, cleanupSrc := blockLog(t, compress.Snappy)
	defer cleanupSrc()
	appendBatches(t, src, 40, 40, 1)
	_, blocks, err := src.ReadBlocks(0, 1<<20)
	require.NoError(t, err)
	require.Len(t, blocks, 3)
	raw := blocks[2]
	require.Equal(t, byte(compress.None), raw.Data[2], "fixture: the one-record block is stored raw")

	dst, cleanupDst := blockLog(t, compress.Snappy)
	defer cleanupDst()
	_, err = dst.AppendBlock(blocks[0])
	require.NoError(t, err)
	tail := dst.NewestOffset()
	size := dst.activeSegment().PhysicalSize()

	damaged := func(mutate func(d []byte) []byte) Block {
		b := blocks[1]
		b.Data = mutate(append([]byte(nil), b.Data...))
		return b
	}
	cases := map[string]struct {
		block Block
		want  error
	}{
		"at or below the tail": {blocks[0], ErrMessageSetRefused},
		"bad magic":            {damaged(func(d []byte) []byte { d[0] ^= 0xff; return d }), ErrMessageSetRefused},
		"a format version this build does not write": {
			damaged(func(d []byte) []byte { d[1] = blockv3.Version + 1; return d }), ErrBlockFormat},
		"shorter than the header promises":    {damaged(func(d []byte) []byte { return d[:len(d)-1] }), ErrMessageSetRefused},
		"longer than the header promises":     {damaged(func(d []byte) []byte { return append(d, 0) }), ErrMessageSetRefused},
		"a payload that does not decode":      {damaged(func(d []byte) []byte { d[blockHeaderLen] ^= 0xff; return d }), ErrMessageSetRefused},
		"a record count the payload belies":   {damaged(func(d []byte) []byte { d[11]++; return d }), ErrMessageSetRefused},
		"a logical length the payload belies": {damaged(func(d []byte) []byte { d[3]++; return d }), ErrMessageSetRefused},
		"no header at all":                    {Block{Data: []byte{1, 2, 3}}, ErrMessageSetRefused},
		// A raw block's payload copies through the decode untouched, so a
		// physical length the header understates is caught by the length check
		// ALONE — and without it the block is stored under a header that
		// misframes it, so the next open parses the following header a byte
		// early.
		"a physical length the header understates, raw": {func() Block {
			b := raw
			b.Data = append([]byte(nil), b.Data...)
			b.Data[7]--
			return b
		}(), ErrMessageSetRefused},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := dst.AppendBlock(c.block)
			require.ErrorIs(t, errors.Cause(err), c.want, "%v", err)
			require.Equal(t, tail, dst.NewestOffset(), "a refused block moves nothing")
			require.Equal(t, size, dst.activeSegment().PhysicalSize(), "a refused block writes nothing")
		})
	}

	// And the undamaged blocks still go in afterwards.
	for _, b := range blocks[1:] {
		_, err = dst.AppendBlock(b)
		require.NoError(t, err)
	}
	require.Equal(t, src.NewestOffset(), dst.NewestOffset())
}

// A message set that ends inside a frame is refused, not written as a prefix —
// and not a panic. The size field sets the reach of the parse, and a set cut
// short by a transport used to slice past its own end inside the library.
func TestAMessageSetCutInsideAFrameIsRefusedNotPanicked(t *testing.T) {
	src, cleanupSrc := blockLog(t, compress.None)
	defer cleanupSrc()
	_, err := src.Append([]*Message{{Value: []byte("one")}, {Value: []byte("two")}})
	require.NoError(t, err)
	ms, err := src.ReadMessageSet(0, 1<<20)
	require.NoError(t, err)

	dst, cleanupDst := blockLog(t, compress.None)
	defer cleanupDst()
	for _, cut := range []int{len(ms) - 1, len(ms) - msgSetHeaderLen - 2, msgSetHeaderLen + 1} {
		_, err := dst.AppendMessageSet(ms[:cut])
		require.ErrorIs(t, errors.Cause(err), ErrMessageSetRefused, "cut at %d", cut)
		require.Equal(t, int64(-1), dst.NewestOffset(), "a refused set writes nothing (cut at %d)", cut)
	}
	_, err = dst.AppendMessageSet(ms)
	require.NoError(t, err)
}

// Over an offloaded segment the blocks come off the store as they are, with no
// decode. Proven by making a decode impossible: the object's payload bytes are
// damaged after the offload, and ReadBlocks must still return them — exactly
// as damaged — where ReadMessageSet cannot.
func TestReadBlocksOverATieredSegmentIssuesNoDecode(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)
	l, cleanup := setupWithOptions(t, Options{
		Path: dir, MaxSegmentBytes: 1 << 14, Compression: compress.Snappy,
		Tiers: oneTier(store), DisableAutoClean: true,
	})
	defer cleanup()

	var last int64
	for i := 0; i < 8; i++ {
		offs, err := l.Append(compressibleMsgs(40))
		require.NoError(t, err)
		last = offs[len(offs)-1]
	}
	l.SetHighWatermark(last)
	n, err := l.OffloadBefore(last)
	require.NoError(t, err)
	require.Positive(t, n, "fixture: at least one sealed segment offloaded")

	seg := l.segmentsSnapshot()[0]
	require.True(t, seg.isOffloaded(), "fixture: the first segment is in the store")

	// Damage the first block's payload in the store, header untouched — before
	// any read, since the store backing keeps a prefetch window and a read that
	// warmed it would serve the undamaged bytes from there.
	path := filepath.Join(dir, "store", seg.storeKey)
	obj, err := os.ReadFile(path)
	require.NoError(t, err)
	for i := blockHeaderLen; i < blockHeaderLen+8; i++ {
		obj[i] ^= 0xff
	}
	require.NoError(t, os.WriteFile(path, obj, 0o644))

	_, err = l.ReadMessageSet(0, 1<<20)
	require.Error(t, err, "the damaged payload cannot decode, which is what makes the next assertion mean something")

	head, blocks, err := l.ReadBlocks(0, 1<<20)
	require.NoError(t, err, "a block read does not decode, so damage it cannot see does not stop it")
	require.Empty(t, head)
	require.Equal(t, int64(seg.MessageCount()), func() (n int64) {
		for _, b := range blocks {
			n += int64(b.Records)
		}
		return n
	}(), "every block of the segment came back")
	require.Equal(t, obj[:len(blocks[0].Data)], blocks[0].Data, "the bytes are the store's, as they are")
	require.Equal(t, obj[len(blocks[0].Data):len(blocks[0].Data)+len(blocks[1].Data)], blocks[1].Data)
}

// A log restarted after AppendBlock reads the verbatim block like any other:
// the open-time header walk and the tail reconciliation see one block.
func TestALogReopenedAfterAppendBlockReadsItBack(t *testing.T) {
	src, cleanupSrc := blockLog(t, compress.Snappy)
	defer cleanupSrc()
	appendBatches(t, src, 40, 1, 40)
	want := readFrom(t, src)

	path := tempDir(t)
	opts := Options{Path: path, MaxSegmentBytes: 1 << 20, Compression: compress.Snappy, DisableAutoClean: true}
	dst, err := New(opts)
	require.NoError(t, err)
	replicateBlocks(t, src, dst.(*commitLog), 1<<20)
	require.NoError(t, dst.Close())

	reopened, err := New(opts)
	require.NoError(t, err)
	defer reopened.Close()
	require.Equal(t, src.NewestOffset(), reopened.NewestOffset())
	require.Equal(t, want, readFrom(t, reopened))

	// And the reopened log keeps appending after the copied blocks.
	offs, err := reopened.Append([]*Message{{Value: []byte("after")}})
	require.NoError(t, err)
	require.Equal(t, src.NewestOffset()+1, offs[0])
}

// A segment with no block layout at either end: a raw source yields everything
// as head; a raw destination stores a block's framing as a message set.
func TestBlocksAcrossALegacySegmentAtEitherEnd(t *testing.T) {
	raw, cleanupRaw := blockLog(t, compress.None)
	defer cleanupRaw()
	appendBatches(t, raw, 40, 40)

	head, blocks, err := raw.ReadBlocks(0, 1<<20)
	require.NoError(t, err)
	require.Empty(t, blocks, "a raw segment has no blocks to hand out")
	want, err := raw.ReadMessageSet(0, 1<<20)
	require.NoError(t, err)
	require.Equal(t, want, head)

	src, cleanupSrc := blockLog(t, compress.Snappy)
	defer cleanupSrc()
	appendBatches(t, src, 40, 1)
	_, blocks, err = src.ReadBlocks(0, 1<<20)
	require.NoError(t, err)
	require.Len(t, blocks, 2)

	dst, cleanupDst := blockLog(t, compress.None)
	defer cleanupDst()
	for _, b := range blocks {
		offs, err := dst.AppendBlock(b)
		require.NoError(t, err, "a raw log takes the block's framing instead")
		require.Len(t, offs, b.Records)
	}
	require.Equal(t, readFrom(t, src), readFrom(t, dst))
	srcSet, err := src.ReadMessageSet(0, 1<<20)
	require.NoError(t, err)
	require.True(t, bytes.Equal(srcSet, physicalBytes(t, dst)),
		"a raw destination's file is the logical framing")
}
