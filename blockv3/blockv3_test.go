package blockv3

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"hash/crc32"
	"strings"
	"testing"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

func sampleRecords(n int) []Record {
	schema := `{"operation":"insert","relation":"catalog","schema":[{"key":true,"name":"product_id"}],"after":`
	recs := make([]Record, n)
	for i := range recs {
		recs[i] = Record{
			OffsetDelta: uint32(i),
			Key:         []byte("k" + strings.Repeat("0", i%3)),
			Value:       []byte(schema + strings.Repeat("x", i) + "}"),
			Headers:     map[string][]byte{"trace": []byte("t1"), "app": []byte("a")},
		}
	}
	recs[0].Attributes = AttrTombstone
	return recs
}

func sampleHeader(codec compress.Codec) Header {
	return Header{
		Codec: codec, Flags: FlagTransactional,
		BaseOffset: 1000, LeaderEpoch: 7, BaseTimestamp: 1_700_000_000_000_000_000,
		ProducerID: 42, ProducerEpoch: 3, BaseSequence: 500, TxNonce: 0xdeadbeef,
	}
}

// The layout durable_streams encodes against, byte for byte: every field at
// its documented offset, big-endian, with the header CRC over bytes 0..72.
func TestHeaderLayoutIsTheDocumentedOne(t *testing.T) {
	h := sampleHeader(compress.Zstd)
	h.Records, h.LastOffsetDelta, h.UncompressedLen, h.CompressedLen, h.PayloadCRC = 9, 8, 1234, 567, 0xabcdef01
	b := EncodeHeader(nil, h)
	require.Len(t, b, HeaderLen)
	be := binary.BigEndian
	require.Equal(t, byte(0xC1), b[0])
	require.Equal(t, byte(3), b[1])
	require.Equal(t, byte(compress.Zstd), b[2])
	require.Equal(t, FlagTransactional, b[3])
	require.Equal(t, uint64(1000), be.Uint64(b[4:]))
	require.Equal(t, uint64(7), be.Uint64(b[12:]))
	require.Equal(t, uint64(1_700_000_000_000_000_000), be.Uint64(b[20:]))
	require.Equal(t, uint64(42), be.Uint64(b[28:]))
	require.Equal(t, uint32(3), be.Uint32(b[36:]))
	require.Equal(t, uint32(500), be.Uint32(b[40:]))
	require.Equal(t, uint64(0xdeadbeef), be.Uint64(b[44:]))
	require.Equal(t, uint32(9), be.Uint32(b[52:]))
	require.Equal(t, uint32(8), be.Uint32(b[56:]))
	require.Equal(t, uint32(1234), be.Uint32(b[60:]))
	require.Equal(t, uint32(567), be.Uint32(b[64:]))
	require.Equal(t, uint32(0xabcdef01), be.Uint32(b[68:]))
	require.Equal(t, crc32.Checksum(b[:72], crc32.MakeTable(crc32.Castagnoli)), be.Uint32(b[72:]))

	got, err := DecodeHeader(b)
	require.NoError(t, err)
	require.Equal(t, h, got)
}

func TestBlockRoundTripsUnderEveryCodec(t *testing.T) {
	for _, codec := range []compress.Codec{compress.None, compress.Snappy, compress.S2, compress.Zstd} {
		t.Run(codec.String(), func(t *testing.T) {
			recs := sampleRecords(50)
			block, err := EncodeBlock(sampleHeader(codec), recs)
			require.NoError(t, err)

			h, got, err := DecodeBlock(block)
			require.NoError(t, err)
			require.Equal(t, codec, h.Codec, "50 records of repeated schema compress under every codec")
			require.Equal(t, uint32(50), h.Records)
			require.Equal(t, uint32(49), h.LastOffsetDelta)
			require.Equal(t, HeaderLen+int(h.CompressedLen), len(block))
			require.Equal(t, recs, got)
		})
	}
}

// A payload the codec cannot shrink is stored raw, and says so.
func TestAnIncompressiblePayloadIsStoredRaw(t *testing.T) {
	v := make([]byte, 4096)
	_, err := rand.Read(v)
	require.NoError(t, err)
	block, err := EncodeBlock(sampleHeader(compress.Snappy), []Record{{Value: v}})
	require.NoError(t, err)
	h, recs, err := DecodeBlock(block)
	require.NoError(t, err)
	require.Equal(t, compress.None, h.Codec)
	require.Equal(t, h.UncompressedLen, h.CompressedLen)
	require.Equal(t, v, recs[0].Value)
}

// nil and empty are different fields and survive the trip as such.
func TestNilAndEmptyFieldsAreDistinct(t *testing.T) {
	recs := []Record{
		{OffsetDelta: 0, Key: nil, Value: []byte{}},
		{OffsetDelta: 1, Key: []byte{}, Value: nil},
		{OffsetDelta: 2, Key: nil, Value: nil, Headers: map[string][]byte{"h": {}}},
	}
	block, err := EncodeBlock(sampleHeader(compress.None), recs)
	require.NoError(t, err)
	_, got, err := DecodeBlock(block)
	require.NoError(t, err)
	require.Nil(t, got[0].Key)
	require.NotNil(t, got[0].Value)
	require.Empty(t, got[0].Value)
	require.NotNil(t, got[1].Key)
	require.Empty(t, got[1].Key)
	require.Nil(t, got[1].Value)
	require.NotNil(t, got[2].Headers["h"], "a present empty header value")
	require.Empty(t, got[2].Headers["h"])
}

// Headers encode in key order, so the same record always encodes to the same
// bytes whatever order the map is built in.
func TestRecordEncodingIsDeterministic(t *testing.T) {
	a := Record{Headers: map[string][]byte{"b": []byte("2"), "a": []byte("1"), "c": []byte("3")}}
	b := Record{Headers: map[string][]byte{"c": []byte("3"), "a": []byte("1"), "b": []byte("2")}}
	require.Equal(t, EncodeRecords(nil, []Record{a}), EncodeRecords(nil, []Record{b}))
}

// The hops: the log rewrites its three fields, a mirror restamps identity,
// compaction strips it. Each touches the header only, and the payload CRC still
// verifies afterwards.
func TestRewriteRestampAndStripTouchOnlyTheHeader(t *testing.T) {
	block, err := EncodeBlock(sampleHeader(compress.Zstd), sampleRecords(10))
	require.NoError(t, err)
	payload := append([]byte(nil), block[HeaderLen:]...)

	require.NoError(t, Rewrite(block, 5000, 9, 123))
	h, err := VerifyPayload(block)
	require.NoError(t, err)
	require.Equal(t, int64(5000), h.BaseOffset)
	require.Equal(t, uint64(9), h.LeaderEpoch)
	require.Equal(t, int64(123), h.BaseTimestamp)
	require.Equal(t, int64(42), h.ProducerID, "rewrite leaves identity alone")

	require.NoError(t, Restamp(block, 77, 1, 9000, 0))
	h, err = VerifyPayload(block)
	require.NoError(t, err)
	require.Equal(t, int64(77), h.ProducerID)
	require.Equal(t, int32(1), h.ProducerEpoch)
	require.Equal(t, int32(9000), h.BaseSequence)
	require.Equal(t, uint64(0), h.TxNonce)
	require.Zero(t, h.Flags&FlagTransactional, "no nonce, not transactional")
	require.Equal(t, int64(5000), h.BaseOffset, "restamp leaves the log's fields alone")

	require.NoError(t, Restamp(block, 77, 1, 9000, 5))
	h, err = VerifyPayload(block)
	require.NoError(t, err)
	require.NotZero(t, h.Flags&FlagTransactional)

	require.NoError(t, Strip(block))
	h, err = VerifyPayload(block)
	require.NoError(t, err)
	require.NotZero(t, h.Flags&FlagStripped)
	require.Zero(t, h.Flags&FlagTransactional)
	require.Zero(t, h.ProducerID)
	require.Zero(t, h.BaseSequence)
	require.Zero(t, h.TxNonce)

	require.NoError(t, Restamp(block, 1, 1, 1, 0))
	h, err = VerifyPayload(block)
	require.NoError(t, err)
	require.Zero(t, h.Flags&FlagStripped, "a restamp makes sequence-by-index true again")

	require.Equal(t, payload, block[HeaderLen:], "no hop touched the payload")
	_, recs, err := DecodeBlock(block)
	require.NoError(t, err)
	require.Len(t, recs, 10)
}

// Every check the header makes possible, refused with the sentinel a caller
// branches on: format for another version, corrupt for damage.
func TestDamageIsRefusedWithTheRightSentinel(t *testing.T) {
	good, err := EncodeBlock(sampleHeader(compress.Snappy), sampleRecords(10))
	require.NoError(t, err)
	damaged := func(f func(b []byte) []byte) []byte { return f(append([]byte(nil), good...)) }
	reseal := func(b []byte) []byte { sealHeader(b); return b }

	cases := map[string]struct {
		block []byte
		want  error
	}{
		"short":               {good[:HeaderLen-1], ErrCorrupt},
		"magic":               {damaged(func(b []byte) []byte { b[0] = 0; return reseal(b) }), ErrFormat},
		"version":             {damaged(func(b []byte) []byte { b[1] = 2; return reseal(b) }), ErrFormat},
		"header crc":          {damaged(func(b []byte) []byte { b[4] ^= 1; return b }), ErrCorrupt},
		"codec":               {damaged(func(b []byte) []byte { b[2] = 9; return reseal(b) }), ErrCorrupt},
		"zero records":        {damaged(func(b []byte) []byte { binary.BigEndian.PutUint32(b[52:], 0); return reseal(b) }), ErrCorrupt},
		"payload crc":         {damaged(func(b []byte) []byte { b[HeaderLen+3] ^= 1; return b }), ErrCorrupt},
		"payload too short":   {good[:len(good)-1], ErrCorrupt},
		"payload too long":    {damaged(func(b []byte) []byte { return append(b, 0) }), ErrCorrupt},
		"records overclaimed": {damaged(func(b []byte) []byte { binary.BigEndian.PutUint32(b[52:], 11); return reseal(b) }), ErrCorrupt},
		"records underclaimed": {damaged(func(b []byte) []byte {
			binary.BigEndian.PutUint32(b[52:], 9)
			return reseal(b)
		}), ErrCorrupt},
		"last delta disagrees": {damaged(func(b []byte) []byte { binary.BigEndian.PutUint32(b[56:], 3); return reseal(b) }), ErrCorrupt},
		"uncompressed len": {damaged(func(b []byte) []byte {
			binary.BigEndian.PutUint32(b[60:], binary.BigEndian.Uint32(b[60:])+1)
			return reseal(b)
		}), ErrCorrupt},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, err := DecodeBlock(c.block)
			require.ErrorIs(t, err, c.want, "%v", err)
		})
	}
}

// A record that lies about its own lengths cannot reach past the payload.
func TestAMalformedRecordCannotReadPastThePayload(t *testing.T) {
	payload := EncodeRecords(nil, sampleRecords(3))
	for i := range payload {
		p := append([]byte(nil), payload...)
		p[i] ^= 0xff
		_, err := DecodeRecords(p, 3) // must return, never panic
		if err == nil {
			// A flip that lands in a value byte is not detectable here; the
			// block CRC is what catches it. Whatever decoded must still be
			// three records.
			recs, _ := DecodeRecords(p, 3)
			require.Len(t, recs, 3)
		}
	}
	_, err := DecodeRecords(payload[:len(payload)-1], 3)
	require.ErrorIs(t, err, ErrCorrupt)
	_, err = DecodeRecords(append(payload, 1), 3)
	require.ErrorIs(t, err, ErrCorrupt)
}

// An empty block is not a block.
func TestABlockHoldsAtLeastOneRecord(t *testing.T) {
	_, err := EncodeBlock(sampleHeader(compress.None), nil)
	require.Error(t, err)
}

func TestDecodedFieldsAliasTheBufferTheyCameFrom(t *testing.T) {
	block, err := EncodeBlock(sampleHeader(compress.None), []Record{{Value: []byte("hello")}})
	require.NoError(t, err)
	_, recs, err := DecodeBlock(block)
	require.NoError(t, err)
	idx := bytes.Index(block, []byte("hello"))
	require.Equal(t, &block[idx], &recs[0].Value[0], "a raw payload's fields point into the block")
}
