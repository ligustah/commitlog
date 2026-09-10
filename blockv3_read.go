package commitlog

import (
	"fmt"
	"hash/crc32"
	"sort"

	"github.com/ligustah/commitlog/blockv3"
	"github.com/pkg/errors"
)

// The identity headers a version-3 block's records carry when read. They are
// the names durable_streams writes on version-2 records, so one accessor
// reads both formats.
const (
	hdrProducerID    = "pid"
	hdrProducerEpoch = "epoch"
	hdrSequence      = "seq"
	hdrNonce         = "nonce"
)

// decodeV3Block turns a whole version-3 block — header and payload as stored —
// into the log's logical framing: version-2 message-set frames, one per record,
// with the block's offsets, timestamp and epoch on each frame and its producer
// identity as headers. dst is reused when it has room. The result is what the
// log's logical byte space holds for this block, so its length is the block's
// logical length and the frames are what every reader, repair and rewrite in
// the package already walks.
//
// Deterministic by construction — headers in key order, records in payload
// order — so the length a write computed and the length a decode measures are
// one number, and a re-decode yields the same bytes.
func decodeV3Block(block, dst []byte) ([]byte, error) {
	h, recs, err := blockv3.DecodeBlock(block)
	if err != nil {
		return nil, err
	}
	return synthesizeFraming(dst[:0], h, recs)
}

// synthesizeFraming appends the version-2 frames for a decoded version-3 block
// to dst.
func synthesizeFraming(dst []byte, h blockv3.Header, recs []blockv3.Record) ([]byte, error) {
	var (
		attrs   int8
		msg     []byte
		keys    []string
		idHdrs  map[string][]byte
		stamped = h.Flags&blockv3.FlagStripped == 0
	)
	if h.Flags&blockv3.FlagControl != 0 {
		attrs = AttrControl
	}
	if stamped {
		idHdrs = map[string][]byte{
			hdrProducerID:    be64(uint64(h.ProducerID)),
			hdrProducerEpoch: be32(uint32(h.ProducerEpoch)),
		}
		if h.Flags&blockv3.FlagTransactional != 0 {
			idHdrs[hdrNonce] = be64(h.TxNonce)
		}
	}
	for i, r := range recs {
		headers := r.Headers
		if stamped {
			headers = make(map[string][]byte, len(r.Headers)+3)
			for k, v := range r.Headers {
				headers[k] = v
			}
			for k, v := range idHdrs {
				headers[k] = v
			}
			headers[hdrSequence] = be32(uint32(h.BaseSequence + int32(i)))
		}
		keys = keys[:0]
		for k := range headers {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		msg, err := encodeV1Message(msg[:0], attrs|int8(r.Attributes&blockv3.AttrTombstone), r.Key, r.Value, keys, headers)
		if err != nil {
			return nil, errors.Wrapf(err, "record %d", i)
		}
		hdr := len(dst)
		dst = encoding.AppendUint64(dst, uint64(h.BaseOffset+int64(r.OffsetDelta)))
		dst = encoding.AppendUint64(dst, uint64(h.BaseTimestamp))
		dst = encoding.AppendUint64(dst, h.LeaderEpoch)
		dst = encoding.AppendUint32(dst, uint32(len(msg)))
		dst = encoding.AppendUint32(dst, headerCrc(dst[hdr:]))
		dst = append(dst, msg...)
	}
	return dst, nil
}

// encodeV1Message appends one message in the log's wire form — CRC, magic,
// attributes, key, value, headers — with headers written in the order of keys.
// It is Message.Encode with a fixed header order, so the bytes are the same for
// the same input.
func encodeV1Message(dst []byte, attrs int8, key, value []byte, keys []string, headers map[string][]byte) ([]byte, error) {
	start := len(dst)
	dst = append(dst, 0, 0, 0, 0) // CRC, filled below
	dst = append(dst, 0, byte(attrs))
	dst = appendV1Bytes(dst, key)
	dst = appendV1Bytes(dst, value)
	if len(keys) > 1<<15-1 {
		return nil, fmt.Errorf("%d headers", len(keys))
	}
	dst = encoding.AppendUint16(dst, uint16(len(keys)))
	for _, k := range keys {
		if len(k) > 1<<15-1 {
			return nil, fmt.Errorf("header key of %d bytes", len(k))
		}
		dst = encoding.AppendUint16(dst, uint16(len(k)))
		dst = append(dst, k...)
		dst = appendV1Bytes(dst, headers[k])
	}
	encoding.PutUint32(dst[start:], crc32.Checksum(dst[start+4:], crc32cTable))
	return dst, nil
}

func appendV1Bytes(dst, b []byte) []byte {
	if b == nil {
		return encoding.AppendUint32(dst, 0xffffffff)
	}
	dst = encoding.AppendUint32(dst, uint32(len(b)))
	return append(dst, b...)
}

func be64(v uint64) []byte { return encoding.AppendUint64(nil, v) }
func be32(v uint32) []byte { return encoding.AppendUint32(nil, v) }

// identityFromHeaders reads the producer identity a record's headers carry —
// the same four names whether the record was appended with them (version 2) or
// synthesized from a version-3 block header.
func identityFromHeaders(headers map[string][]byte) (pid uint64, epoch uint32, seq int32, nonce uint64, ok bool) {
	p, hasPID := headers[hdrProducerID]
	e, hasEpoch := headers[hdrProducerEpoch]
	s, hasSeq := headers[hdrSequence]
	if !hasPID || !hasEpoch || !hasSeq || len(p) != 8 || len(e) != 4 || len(s) != 4 {
		return 0, 0, 0, 0, false
	}
	if n := headers[hdrNonce]; len(n) == 8 {
		nonce = encoding.Uint64(n)
	}
	return encoding.Uint64(p), encoding.Uint32(e), int32(encoding.Uint32(s)), nonce, true
}
