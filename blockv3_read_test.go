package commitlog

import (
	"context"
	"hash/crc32"
	"os"
	"testing"
	"time"

	"github.com/ligustah/commitlog/blockv3"
	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// v3Records builds n records with a repeated schema, so a batch compresses.
func v3Records(n int, prefix string) []blockv3.Record {
	recs := make([]blockv3.Record, n)
	for i := range recs {
		recs[i] = blockv3.Record{
			OffsetDelta: uint32(i),
			Key:         []byte("k"),
			Value:       []byte(`{"operation":"insert","relation":"catalog","after":` + prefix + string(rune('a'+i%26)) + `}`),
			Headers:     map[string][]byte{"trace": []byte("t")},
		}
	}
	return recs
}

// writeV3 stores a version-3 block on l's active segment as the log would once
// it writes them: the header's log-owned fields set to the tail, the framing
// its records decode to sized and indexed. Returns the block as stored.
func writeV3(t *testing.T, l *commitLog, h blockv3.Header, recs []blockv3.Record) []byte {
	t.Helper()
	seg := l.activeSegment()
	h.BaseOffset = seg.NextOffset()
	block, err := blockv3.EncodeBlock(h, recs)
	require.NoError(t, err)
	stored, err := blockv3.DecodeHeader(block)
	require.NoError(t, err)
	framing, err := decodeV3Block(block, nil)
	require.NoError(t, err)
	entries := entriesForMessageSet(seg.Position(), framing)
	require.NoError(t, seg.WriteBlock(block, stored.Codec, int64(len(framing)), entries))
	return block
}

func v3Header(codec compress.Codec) blockv3.Header {
	return blockv3.Header{
		Codec: codec, Flags: blockv3.FlagTransactional,
		LeaderEpoch: 1, BaseTimestamp: 1_700_000_000_000,
		ProducerID: 42, ProducerEpoch: 3, BaseSequence: 500, TxNonce: 99,
	}
}

// metadataFrom reads every record's metadata from oldest to newest.
func metadataFrom(t *testing.T, l *commitLog) []MessageMetadata {
	t.Helper()
	r, err := l.NewReader(From(0), Uncommitted())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out []MessageMetadata
	hdr := make([]byte, HeaderBufferLen)
	for {
		// A fresh payload buffer per record: Raw and Headers alias it, and the
		// comparison below keeps neither.
		m, _, err := r.ReadMessageMetadata(ctx, hdr, make([]byte, 0, 1024))
		if err != nil {
			break
		}
		m.Headers = nil
		m.Raw = nil
		out = append(out, m)
	}
	return out
}

// A version-3 block reads as its records, beside version-2 blocks in the same
// segment: same offsets, timestamps, epochs and values a reader gets from any
// other record, with the block's producer identity on each one.
func TestAVersion3BlockReadsAsItsRecords(t *testing.T) {
	l, cleanup := blockLog(t, compress.Snappy)
	defer cleanup()

	appendBatches(t, l, 5)
	block := writeV3(t, l, v3Header(compress.Snappy), v3Records(40, "x"))
	stripped := v3Header(compress.Zstd)
	stripped.Flags = blockv3.FlagStripped
	writeV3(t, l, stripped, v3Records(3, "y"))
	control := v3Header(compress.None)
	control.Flags = blockv3.FlagControl
	writeV3(t, l, control, []blockv3.Record{{Value: []byte("commit"), Attributes: blockv3.AttrTombstone}})
	appendBatches(t, l, 2)

	require.Equal(t, int64(50), l.NewestOffset(), "5 + 40 + 3 + 1 + 2 records, contiguous")
	got := readFrom(t, l)
	require.Len(t, got, 51)
	require.Contains(t, got[5], `"after":xa}`)
	require.Contains(t, got[44], `"after":xn}`, "record 39 of the block is at offset 5+39")
	require.Contains(t, got[45], `"after":ya}`)
	require.Equal(t, "commit", got[48])

	meta := metadataFrom(t, l)
	require.Len(t, meta, 51)
	require.False(t, meta[0].HasIdentity, "a record appended without identity headers has none")
	for i := 5; i < 45; i++ {
		m := meta[i]
		require.True(t, m.HasIdentity, "offset %d", i)
		require.Equal(t, uint64(42), m.ProducerID)
		require.Equal(t, uint32(3), m.ProducerEpoch)
		require.Equal(t, int32(500+i-5), m.Sequence, "sequence is by index from the block's base")
		require.Equal(t, uint64(99), m.Nonce)
		require.Equal(t, int64(1_700_000_000_000), m.Timestamp)
		require.Equal(t, uint64(1), m.LeaderEpoch)
		require.Equal(t, int64(i), m.Offset)
	}
	require.False(t, meta[45].HasIdentity, "a stripped block carries no identity")
	require.Equal(t, AttrControl|AttrTombstone, meta[48].Attributes, "control from the block, tombstone from the record")
	require.True(t, meta[48].HasIdentity)
	require.Equal(t, uint64(0), meta[48].Nonce, "not transactional: no nonce")

	// The user headers survive beside the synthesized identity.
	r, err := l.NewReader(From(5), Uncommitted())
	require.NoError(t, err)
	msg, _, _, _, err := r.ReadMessage(context.Background(), make([]byte, HeaderBufferLen))
	require.NoError(t, err)
	hdrs := SerializedMessage(msg).Headers()
	require.Equal(t, []byte("t"), hdrs["trace"])
	require.Len(t, hdrs["pid"], 8)
	require.Len(t, hdrs["seq"], 4)

	// And the block itself is what ReadBlocks hands a replica.
	_, blocks, err := l.ReadBlocks(5, 1<<20)
	require.NoError(t, err)
	require.Equal(t, block, blocks[0].Data)
	require.Equal(t, int64(5), blocks[0].FirstOffset)
	require.Equal(t, int64(44), blocks[0].LastOffset)
	require.Equal(t, 40, blocks[0].Records)
}

// What a version-3 block decodes to is appendable to a log that has never
// heard of version 3: the framing is the log's own.
func TestAVersion3BlocksFramingReplicatesToAVersion2Log(t *testing.T) {
	src, cleanupSrc := blockLog(t, compress.Snappy)
	defer cleanupSrc()
	writeV3(t, src, v3Header(compress.Snappy), v3Records(40, "x"))

	dst, cleanupDst := blockLog(t, compress.None)
	defer cleanupDst()
	ms, err := src.ReadMessageSet(0, 1<<20)
	require.NoError(t, err)
	offs, err := dst.AppendMessageSet(ms)
	require.NoError(t, err)
	require.Len(t, offs, 40)
	require.Equal(t, readFrom(t, src), readFrom(t, dst))
	require.Equal(t, metadataFrom(t, src), metadataFrom(t, dst), "identity travels in the headers")
}

// A reopened segment reads its version-3 blocks back: from the sidecar with no
// decode when it was sealed, by decoding each block once when it was not.
func TestAReopenedSegmentReadsVersion3BlocksFromTableOrWalk(t *testing.T) {
	path := tempDir(t)
	opts := Options{Path: path, MaxSegmentBytes: 1 << 14, Compression: compress.Snappy, DisableAutoClean: true}
	l, err := New(opts)
	require.NoError(t, err)
	cl := l.(*commitLog)
	for range 3 {
		writeV3(t, cl, v3Header(compress.Snappy), v3Records(40, "x"))
	}
	require.Len(t, cl.segmentsSnapshot(), 1, "fixture: all three blocks in one segment")
	// A version-2 append past MaxSegmentBytes rolls the segment, which seals
	// the first and writes its table.
	appendBatches(t, cl, 40)
	require.Len(t, cl.segmentsSnapshot(), 2, "fixture: the append rolled")
	want := readFrom(t, cl)
	require.NoError(t, l.Close())

	reopened, err := New(opts)
	require.NoError(t, err)
	rl := reopened.(*commitLog)
	segs := rl.segmentsSnapshot()
	require.Zero(t, segs[0].blocksWalked, "a sealed segment's table spares the decode")
	require.Equal(t, want, readFrom(t, rl))
	require.NoError(t, reopened.Close())
}

// A reopen without the sidecar decodes the blocks and reaches the same layout.
func TestAnUnsealedSegmentDecodesItsVersion3BlocksOnceAtOpen(t *testing.T) {
	path := tempDir(t)
	opts := Options{Path: path, MaxSegmentBytes: 1 << 20, Compression: compress.Snappy, DisableAutoClean: true}
	l, err := New(opts)
	require.NoError(t, err)
	cl := l.(*commitLog)
	writeV3(t, cl, v3Header(compress.Snappy), v3Records(40, "x"))
	appendBatches(t, cl, 3)
	writeV3(t, cl, v3Header(compress.None), v3Records(2, "y"))
	want := readFrom(t, cl)
	seg := cl.activeSegment()
	layout := append([]blockRef(nil), seg.blocks...)
	require.NoError(t, l.Close())
	// Close seals the active segment and writes its table; without it the open
	// has to walk.
	require.NoError(t, os.Remove(localBlockTablePath(seg)))

	reopened, err := New(opts)
	require.NoError(t, err)
	defer reopened.Close()
	rl := reopened.(*commitLog)
	seg = rl.activeSegment()
	require.Equal(t, 3, seg.blocksWalked, "no table for the active segment: every block walked")
	require.Equal(t, layout, seg.blocks, "the walk reaches the layout the writer recorded")
	require.Equal(t, want, readFrom(t, rl))
	offs, err := rl.Append([]*Message{{Value: []byte("after")}})
	require.NoError(t, err)
	require.Equal(t, int64(45), offs[0])
}

// A version-3 block cut by a crash is discarded at open like a version-2 one;
// a version-3 block that is all there and damaged refuses the open, like a
// corrupt version-2 header.
func TestATornVersion3TailIsDiscardedAndADamagedOneRefused(t *testing.T) {
	for _, tc := range []struct {
		name string
		// cutFromHeader cuts the file this many bytes into the second block's
		// header; cutFromEnd cuts this many bytes off the end; damage flips a
		// payload byte instead.
		cutIntoHeader int
		cutFromEnd    int
		damage        bool
		opens         bool
		newest        int64
	}{
		{name: "cut inside the payload", cutFromEnd: 10, opens: true, newest: 39},
		{name: "cut inside the header", cutIntoHeader: 5, opens: true, newest: 39},
		{name: "payload damaged", damage: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := tempDir(t)
			opts := Options{Path: path, MaxSegmentBytes: 1 << 20, Compression: compress.Snappy, DisableAutoClean: true}
			l, err := New(opts)
			require.NoError(t, err)
			cl := l.(*commitLog)
			writeV3(t, cl, v3Header(compress.Snappy), v3Records(40, "x"))
			second := writeV3(t, cl, v3Header(compress.Snappy), v3Records(40, "y"))
			seg := cl.activeSegment()
			logPath := seg.logPath()
			require.NoError(t, l.Close())
			// Close sealed the segment and wrote its table; without it the open
			// walks the blocks, which is the path under test. (With it, the cut
			// cases reach the walk anyway through the table's size check, and
			// the damaged case is refused later, by the tail reconciliation.)
			require.NoError(t, os.Remove(localBlockTablePath(seg)))

			f, err := os.ReadFile(logPath)
			require.NoError(t, err)
			switch {
			case tc.cutIntoHeader > 0:
				f = f[:len(f)-len(second)+tc.cutIntoHeader]
			case tc.cutFromEnd > 0:
				f = f[:len(f)-tc.cutFromEnd]
			case tc.damage:
				f[len(f)-3] ^= 0xff
			}
			require.NoError(t, os.WriteFile(logPath, f, 0o644))

			reopened, err := New(opts)
			if !tc.opens {
				require.Error(t, err)
				require.ErrorIs(t, err, blockv3.ErrCorrupt)
				return
			}
			require.NoError(t, err)
			defer reopened.Close()
			require.Equal(t, tc.newest, reopened.NewestOffset())
		})
	}
}

// The block table carries each block's format, and a table from the previous
// release — no format byte — still decodes, with every block version 2.
func TestTheBlockTableCarriesEachBlocksFormat(t *testing.T) {
	blocks := []blockRef{
		{logicalLen: 100, physLen: 60, codec: compress.Snappy, records: 3, version: BlockFormatVersion},
		{logicalStart: 100, logicalLen: 900, physStart: 60, physLen: 300, codec: compress.Zstd, records: 40, version: blockv3.Version},
	}
	mixed := encodeBlockTable(blocks)
	require.Equal(t, byte(blockTableVersion), mixed[1])
	got, err := decodeBlockTable(mixed)
	require.NoError(t, err)
	require.Equal(t, blocks, got)

	// Only version-2 blocks: the previous release's layout, so a rollback
	// still opens every segment sealed since the upgrade.
	v2Only := encodeBlockTable(blocks[:1])
	require.Equal(t, byte(2), v2Only[1])
	require.Len(t, v2Only, blockTableHeaderLen+blockTableEntryLenV2+4)
	got, err = decodeBlockTable(v2Only)
	require.NoError(t, err)
	require.Equal(t, blocks[:1], got)

	v2 := make([]byte, blockTableHeaderLen+blockTableEntryLenV2+4)
	v2[0], v2[1] = blockTableMagic, 2
	encoding.PutUint32(v2[2:], 1)
	encoding.PutUint32(v2[6:], 100)
	encoding.PutUint32(v2[10:], 60)
	v2[14] = byte(compress.Snappy)
	encoding.PutUint32(v2[15:], 3)
	encoding.PutUint32(v2[19:], crc32.ChecksumIEEE(v2[:19]))
	got, err = decodeBlockTable(v2)
	require.NoError(t, err)
	require.Equal(t, blocks[:1], got)

	bad := encodeBlockTable([]blockRef{{logicalLen: 1, physLen: 20, records: 1, version: 7}})
	_, err = decodeBlockTable(bad)
	require.ErrorIs(t, err, ErrBlockTableFormat)
}

// The synthesized message is the encoder's message: same fields, valid CRC.
func TestSynthesizedFramingMatchesTheEncoder(t *testing.T) {
	m := &Message{Attributes: AttrTombstone, Key: []byte("k"), Value: []byte("v"),
		Headers: map[string][]byte{"a": []byte("1"), "b": {}}}
	want, err := encode(m)
	require.NoError(t, err)
	got, err := encodeV1Message(nil, AttrTombstone, m.Key, m.Value, []string{"a", "b"}, m.Headers)
	require.NoError(t, err)
	require.Len(t, got, len(want))
	sm := SerializedMessage(got)
	require.True(t, sm.crcMatches())
	require.Equal(t, SerializedMessage(want).Key(), sm.Key())
	require.Equal(t, SerializedMessage(want).Value(), sm.Value())
	require.Equal(t, SerializedMessage(want).Attributes(), sm.Attributes())
	require.Equal(t, SerializedMessage(want).Headers(), sm.Headers())
}
