// Package blockv3 encodes and decodes version-3 blocks: a fixed 76-byte
// uncompressed header carrying everything a hop rewrites or checks — offsets,
// epoch, timestamp, producer identity, the payload's CRC — over a compressed
// payload of delta-encoded records. A block is compressed once by its producer
// and copied verbatim by every hop after; the log assigns offsets by rewriting
// the header alone.
//
// It depends on compress only, so a client or a mirror can build and re-stamp
// blocks without linking the log.
package blockv3

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"

	"github.com/ligustah/commitlog/compress"
)

const (
	// Magic is the first byte of every block, shared with earlier versions.
	Magic byte = 0xC1
	// Version is the value of the header's version byte.
	Version byte = 3
	// HeaderLen is the fixed header length in bytes.
	HeaderLen = 76

	// FlagControl marks a block of transactional control records.
	FlagControl byte = 1 << 0
	// FlagTransactional says TxNonce is present.
	FlagTransactional byte = 1 << 1
	// FlagStripped says producer identity has been removed by compaction, so
	// sequence-by-index no longer holds.
	FlagStripped byte = 1 << 2

	// AttrTombstone marks a record as a key's terminal record.
	AttrTombstone byte = 0x02
)

// Header field offsets.
const (
	offMagic           = 0
	offVersion         = 1
	offCodec           = 2
	offFlags           = 3
	offBaseOffset      = 4
	offLeaderEpoch     = 12
	offBaseTimestamp   = 20
	offProducerID      = 28
	offProducerEpoch   = 36
	offBaseSequence    = 40
	offTxNonce         = 44
	offRecords         = 52
	offLastOffsetDelta = 56
	offUncompressedLen = 60
	offCompressedLen   = 64
	offPayloadCRC      = 68
	offHeaderCRC       = 72
)

var (
	// ErrFormat reports a block whose magic or version is not this package's.
	ErrFormat = errors.New("blockv3: not a version-3 block")
	// ErrCorrupt reports a block whose bytes fail a check the header makes
	// possible: a CRC, a length, a record that does not parse.
	ErrCorrupt = errors.New("blockv3: corrupt block")
)

var (
	encoding = binary.BigEndian
	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

// Header is a block's fixed header, decoded.
type Header struct {
	Codec compress.Codec
	Flags byte
	// BaseOffset, LeaderEpoch and BaseTimestamp are the appending log's; see
	// Rewrite.
	BaseOffset    int64
	LeaderEpoch   uint64
	BaseTimestamp int64
	// ProducerID, ProducerEpoch, BaseSequence and TxNonce are the producer's;
	// see Restamp. Record i has sequence BaseSequence+i while FlagStripped is
	// clear.
	ProducerID    int64
	ProducerEpoch int32
	BaseSequence  int32
	TxNonce       uint64
	// Records is how many records the payload holds; LastOffsetDelta is the
	// last record's OffsetDelta.
	Records         uint32
	LastOffsetDelta uint32
	// UncompressedLen and CompressedLen size the payload; PayloadCRC is
	// CRC-32C over its compressed bytes.
	UncompressedLen uint32
	CompressedLen   uint32
	PayloadCRC      uint32
}

// Record is one record of a block's payload.
type Record struct {
	Attributes byte
	// OffsetDelta is the record's offset relative to the header's BaseOffset.
	OffsetDelta uint32
	// Key and Value are nil when absent; an empty, non-nil slice is a present
	// empty field.
	Key   []byte
	Value []byte
	// Headers are the record's user headers, encoded in key order.
	Headers map[string][]byte
}

// EncodeHeader appends h as HeaderLen bytes to dst, header CRC included.
func EncodeHeader(dst []byte, h Header) []byte {
	var b [HeaderLen]byte
	b[offMagic] = Magic
	b[offVersion] = Version
	b[offCodec] = byte(h.Codec)
	b[offFlags] = h.Flags
	encoding.PutUint64(b[offBaseOffset:], uint64(h.BaseOffset))
	encoding.PutUint64(b[offLeaderEpoch:], h.LeaderEpoch)
	encoding.PutUint64(b[offBaseTimestamp:], uint64(h.BaseTimestamp))
	encoding.PutUint64(b[offProducerID:], uint64(h.ProducerID))
	encoding.PutUint32(b[offProducerEpoch:], uint32(h.ProducerEpoch))
	encoding.PutUint32(b[offBaseSequence:], uint32(h.BaseSequence))
	encoding.PutUint64(b[offTxNonce:], h.TxNonce)
	encoding.PutUint32(b[offRecords:], h.Records)
	encoding.PutUint32(b[offLastOffsetDelta:], h.LastOffsetDelta)
	encoding.PutUint32(b[offUncompressedLen:], h.UncompressedLen)
	encoding.PutUint32(b[offCompressedLen:], h.CompressedLen)
	encoding.PutUint32(b[offPayloadCRC:], h.PayloadCRC)
	sealHeader(b[:])
	return append(dst, b[:]...)
}

// DecodeHeader reads the header at the front of b, verifying magic, version,
// codec and the header CRC. It does not look at the payload; see VerifyPayload.
func DecodeHeader(b []byte) (Header, error) {
	if len(b) < HeaderLen {
		return Header{}, fmt.Errorf("%w: %d bytes, header is %d", ErrCorrupt, len(b), HeaderLen)
	}
	if b[offMagic] != Magic {
		return Header{}, fmt.Errorf("%w: magic 0x%02x", ErrFormat, b[offMagic])
	}
	if b[offVersion] != Version {
		return Header{}, fmt.Errorf("%w: version %d", ErrFormat, b[offVersion])
	}
	if want, got := encoding.Uint32(b[offHeaderCRC:]), headerCRC(b); want != got {
		return Header{}, fmt.Errorf("%w: header CRC 0x%08x, computed 0x%08x", ErrCorrupt, want, got)
	}
	codec := compress.Codec(b[offCodec])
	if !codec.Valid() {
		return Header{}, fmt.Errorf("%w: codec %d", ErrCorrupt, b[offCodec])
	}
	h := Header{
		Codec:           codec,
		Flags:           b[offFlags],
		BaseOffset:      int64(encoding.Uint64(b[offBaseOffset:])),
		LeaderEpoch:     encoding.Uint64(b[offLeaderEpoch:]),
		BaseTimestamp:   int64(encoding.Uint64(b[offBaseTimestamp:])),
		ProducerID:      int64(encoding.Uint64(b[offProducerID:])),
		ProducerEpoch:   int32(encoding.Uint32(b[offProducerEpoch:])),
		BaseSequence:    int32(encoding.Uint32(b[offBaseSequence:])),
		TxNonce:         encoding.Uint64(b[offTxNonce:]),
		Records:         encoding.Uint32(b[offRecords:]),
		LastOffsetDelta: encoding.Uint32(b[offLastOffsetDelta:]),
		UncompressedLen: encoding.Uint32(b[offUncompressedLen:]),
		CompressedLen:   encoding.Uint32(b[offCompressedLen:]),
		PayloadCRC:      encoding.Uint32(b[offPayloadCRC:]),
	}
	if h.Records == 0 {
		return Header{}, fmt.Errorf("%w: header claims no records", ErrCorrupt)
	}
	return h, nil
}

// Rewrite sets the fields the appending log owns — base offset, leader epoch,
// base timestamp — in the header at the front of block, in place, and reseals
// the header CRC. The payload is untouched. block must start with a header
// DecodeHeader accepts.
func Rewrite(block []byte, baseOffset int64, leaderEpoch uint64, baseTimestamp int64) error {
	if _, err := DecodeHeader(block); err != nil {
		return err
	}
	encoding.PutUint64(block[offBaseOffset:], uint64(baseOffset))
	encoding.PutUint64(block[offLeaderEpoch:], leaderEpoch)
	encoding.PutUint64(block[offBaseTimestamp:], uint64(baseTimestamp))
	sealHeader(block)
	return nil
}

// Restamp gives the block a producer's identity, in place, and reseals the
// header CRC: FlagTransactional follows whether nonce is non-zero, and
// FlagStripped is cleared, since the records are once again sequence-by-index
// from baseSequence. The payload is untouched.
func Restamp(block []byte, producerID int64, producerEpoch, baseSequence int32, nonce uint64) error {
	if _, err := DecodeHeader(block); err != nil {
		return err
	}
	encoding.PutUint64(block[offProducerID:], uint64(producerID))
	encoding.PutUint32(block[offProducerEpoch:], uint32(producerEpoch))
	encoding.PutUint32(block[offBaseSequence:], uint32(baseSequence))
	encoding.PutUint64(block[offTxNonce:], nonce)
	flags := block[offFlags] &^ (FlagTransactional | FlagStripped)
	if nonce != 0 {
		flags |= FlagTransactional
	}
	block[offFlags] = flags
	sealHeader(block)
	return nil
}

// Strip removes the block's producer identity, in place: identity fields
// zeroed, FlagStripped set, FlagTransactional cleared, header CRC resealed.
func Strip(block []byte) error {
	if _, err := DecodeHeader(block); err != nil {
		return err
	}
	for i := offProducerID; i < offRecords; i++ {
		block[i] = 0
	}
	block[offFlags] = block[offFlags]&^FlagTransactional | FlagStripped
	sealHeader(block)
	return nil
}

// VerifyPayload checks that block is exactly a header plus the payload the
// header describes, and that the payload's CRC matches — without decompressing
// it. It returns the header.
func VerifyPayload(block []byte) (Header, error) {
	h, err := DecodeHeader(block)
	if err != nil {
		return Header{}, err
	}
	if want := HeaderLen + int(h.CompressedLen); len(block) != want {
		return Header{}, fmt.Errorf("%w: block is %d bytes, header promises %d", ErrCorrupt, len(block), want)
	}
	if got := crc32.Checksum(block[HeaderLen:], crcTable); got != h.PayloadCRC {
		return Header{}, fmt.Errorf("%w: payload CRC 0x%08x, computed 0x%08x", ErrCorrupt, h.PayloadCRC, got)
	}
	return h, nil
}

// EncodeRecords appends recs to dst in payload form, uncompressed.
func EncodeRecords(dst []byte, recs []Record) []byte {
	var scratch []byte
	for _, r := range recs {
		scratch = encodeRecord(scratch[:0], r)
		dst = binary.AppendUvarint(dst, uint64(len(scratch)))
		dst = append(dst, scratch...)
	}
	return dst
}

func encodeRecord(dst []byte, r Record) []byte {
	dst = append(dst, r.Attributes)
	dst = binary.AppendUvarint(dst, uint64(r.OffsetDelta))
	dst = appendBytes(dst, r.Key)
	dst = appendBytes(dst, r.Value)
	dst = binary.AppendUvarint(dst, uint64(len(r.Headers)))
	keys := make([]string, 0, len(r.Headers))
	for k := range r.Headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		dst = binary.AppendUvarint(dst, uint64(len(k)))
		dst = append(dst, k...)
		v := r.Headers[k]
		dst = binary.AppendUvarint(dst, uint64(len(v)))
		dst = append(dst, v...)
	}
	return dst
}

// appendBytes writes a varint length (-1 for nil) then the bytes.
func appendBytes(dst, b []byte) []byte {
	if b == nil {
		return binary.AppendVarint(dst, -1)
	}
	dst = binary.AppendVarint(dst, int64(len(b)))
	return append(dst, b...)
}

// DecodeRecords parses n records from an uncompressed payload. The returned
// records' Key, Value and header values alias payload.
func DecodeRecords(payload []byte, n uint32) ([]Record, error) {
	recs := make([]Record, 0, n)
	c := cursor{buf: payload}
	for i := range n {
		body := c.bytes(c.uvarint())
		if c.err != nil {
			return nil, fmt.Errorf("%w: record %d: %v", ErrCorrupt, i, c.err)
		}
		r, err := decodeRecord(body)
		if err != nil {
			return nil, fmt.Errorf("%w: record %d: %v", ErrCorrupt, i, err)
		}
		recs = append(recs, r)
	}
	if c.pos != len(payload) {
		return nil, fmt.Errorf("%w: %d bytes after the last record", ErrCorrupt, len(payload)-c.pos)
	}
	return recs, nil
}

func decodeRecord(body []byte) (Record, error) {
	c := cursor{buf: body}
	var r Record
	r.Attributes = c.byte()
	r.OffsetDelta = uint32(c.uvarint())
	r.Key = c.field()
	r.Value = c.field()
	if n := c.uvarint(); n > 0 {
		r.Headers = make(map[string][]byte, n)
		for i := uint64(0); i < n && c.err == nil; i++ {
			k := c.bytes(c.uvarint())
			v := c.bytes(c.uvarint())
			r.Headers[string(k)] = v
		}
	}
	if c.err != nil {
		return Record{}, c.err
	}
	if c.pos != len(body) {
		return Record{}, fmt.Errorf("%d bytes past the record's fields", len(body)-c.pos)
	}
	return r, nil
}

// cursor is a bounds-checked walk over a byte slice: the first read that
// would leave the buffer latches an error the rest of the walk propagates.
type cursor struct {
	buf []byte
	pos int
	err error
}

func (c *cursor) fail(err error) {
	if c.err == nil {
		c.err = err
	}
}

func (c *cursor) byte() byte {
	if c.err != nil || c.pos >= len(c.buf) {
		c.fail(errors.New("short read"))
		return 0
	}
	b := c.buf[c.pos]
	c.pos++
	return b
}

func (c *cursor) uvarint() uint64 {
	if c.err != nil {
		return 0
	}
	v, n := binary.Uvarint(c.buf[c.pos:])
	if n <= 0 {
		c.fail(errors.New("bad uvarint"))
		return 0
	}
	c.pos += n
	return v
}

func (c *cursor) varint() int64 {
	if c.err != nil {
		return 0
	}
	v, n := binary.Varint(c.buf[c.pos:])
	if n <= 0 {
		c.fail(errors.New("bad varint"))
		return 0
	}
	c.pos += n
	return v
}

func (c *cursor) bytes(n uint64) []byte {
	if c.err != nil {
		return nil
	}
	if n > uint64(len(c.buf)-c.pos) {
		c.fail(fmt.Errorf("length %d exceeds the %d bytes left", n, len(c.buf)-c.pos))
		return nil
	}
	b := c.buf[c.pos : c.pos+int(n) : c.pos+int(n)]
	c.pos += int(n)
	return b
}

// field reads a varint-length-prefixed field: -1 is nil, 0 is present and
// empty.
func (c *cursor) field() []byte {
	n := c.varint()
	switch {
	case c.err != nil:
		return nil
	case n == -1:
		return nil
	case n < -1:
		c.fail(fmt.Errorf("field length %d", n))
		return nil
	}
	b := c.bytes(uint64(n))
	if b == nil && c.err == nil {
		return []byte{}
	}
	return b
}

// EncodeBlock builds a whole block: recs encoded and compressed under h.Codec
// — stored raw under compress.None when the codec does not shrink them — with
// Records, LastOffsetDelta, the lengths and both CRCs filled in from the
// payload. The remaining header fields are taken from h as given.
func EncodeBlock(h Header, recs []Record) ([]byte, error) {
	if len(recs) == 0 {
		return nil, errors.New("blockv3: a block holds at least one record")
	}
	logical := EncodeRecords(nil, recs)
	payload := logical
	if h.Codec != compress.None {
		if c := h.Codec.Compress(logical); len(c) < len(logical) {
			payload = c
		} else {
			h.Codec = compress.None
		}
	}
	h.Records = uint32(len(recs))
	h.LastOffsetDelta = recs[len(recs)-1].OffsetDelta
	h.UncompressedLen = uint32(len(logical))
	h.CompressedLen = uint32(len(payload))
	h.PayloadCRC = crc32.Checksum(payload, crcTable)
	out := make([]byte, 0, HeaderLen+len(payload))
	out = EncodeHeader(out, h)
	return append(out, payload...), nil
}

// DecodeBlock verifies block and decodes its payload. The returned records
// alias the decompressed buffer, which is fresh, or block itself when the
// payload is stored raw.
func DecodeBlock(block []byte) (Header, []Record, error) {
	h, err := VerifyPayload(block)
	if err != nil {
		return Header{}, nil, err
	}
	payload := block[HeaderLen:]
	if h.Codec != compress.None {
		if payload, err = h.Codec.DecompressInto(nil, payload); err != nil {
			return Header{}, nil, fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
	}
	if len(payload) != int(h.UncompressedLen) {
		return Header{}, nil, fmt.Errorf("%w: payload decodes to %d bytes, header promises %d",
			ErrCorrupt, len(payload), h.UncompressedLen)
	}
	recs, err := DecodeRecords(payload, h.Records)
	if err != nil {
		return Header{}, nil, err
	}
	if last := recs[len(recs)-1].OffsetDelta; last != h.LastOffsetDelta {
		return Header{}, nil, fmt.Errorf("%w: last offset delta %d in the payload, %d in the header",
			ErrCorrupt, last, h.LastOffsetDelta)
	}
	return h, recs, nil
}

func headerCRC(b []byte) uint32 { return crc32.Checksum(b[:offHeaderCRC], crcTable) }

func sealHeader(b []byte) { encoding.PutUint32(b[offHeaderCRC:], headerCRC(b)) }
