package commitlog

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"

	"github.com/pkg/errors"
)

const (
	offsetPos      = 0
	timestampPos   = 8
	leaderEpochPos = 16
	sizePos        = 24
	// headerCrcPos holds a CRC32 over the 28 bytes before it.
	//
	// The record's own checksum covers its PAYLOAD, which left the frame header
	// — a record's identity — unprotected. A damaged offset field was reported as
	// fact: FuzzCorruptFrameHeaderIsNeverServedAsTruth produced offset 7 carrying
	// record 0's value, CRC passing, in a log holding 0..15. A reader could
	// reject an offset outside its segment's range (55a1d11) and nothing more,
	// because there was no checksum to contradict one inside it.
	//
	// Four bytes per record buys identity the same protection the value already
	// had. This is a FORMAT CHANGE: a segment written before it has 28-byte
	// headers and will not read.
	headerCrcPos    = 28
	msgSetHeaderLen = 32
)

// HeaderBufferLen is the LENGTH the headersBuf argument to Reader.ReadMessage
// and Reader.ReadMessageMetadata must have at least. Capacity does not do: the
// header is read into the slice, so make([]byte, 0, HeaderBufferLen) is refused.
// Longer is accepted and behaves identically — only the first HeaderBufferLen
// bytes are written.
//
// Exported because it was previously a bare "28" in a doc comment, and a
// consumer duly wrote make([]byte, 28) against it. A number copied out of prose
// is the same mistake as a magic byte copied into another repo: correct until it
// isn't, and silent when it stops being.
const HeaderBufferLen = msgSetHeaderLen

type messageSet []byte

// headerCrc is the checksum over a frame header's first 28 bytes — offset,
// timestamp, leader epoch and payload size.
func headerCrc(hdr []byte) uint32 {
	return crc32.Checksum(hdr[:headerCrcPos], crc32cTable)
}

// storedHeaderCrc reads the checksum a frame header carries.
func storedHeaderCrc(hdr []byte) uint32 {
	return encoding.Uint32(hdr[headerCrcPos:])
}

func entriesForMessageSet(basePos int64, ms []byte) []*entry {
	entries := []*entry{}
	if len(ms) <= msgSetHeaderLen {
		return entries
	}
	var n int64
	for len(ms) > 0 {
		var (
			relPos      = n
			m           = messageSet(ms)
			offset      = m.Offset()
			timestamp   = m.Timestamp()
			leaderEpoch = m.LeaderEpoch()
			size        = m.Size()
		)
		entries = append(entries, &entry{
			Offset:      offset,
			Timestamp:   timestamp,
			LeaderEpoch: leaderEpoch,
			Position:    basePos + relPos,
			Size:        size + msgSetHeaderLen,
		})
		n += msgSetHeaderLen + int64(size)
		ms = ms[msgSetHeaderLen+size:]
	}
	return entries
}

func newMessageSetFromProto(baseOffset, basePos int64, msgs []*Message) (
	messageSet, []*entry, error) {

	var (
		buf     = new(bytes.Buffer)
		entries = make([]*entry, len(msgs))
		n       int32
	)
	for i, m := range msgs {
		data, err := encode(m)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "encode message at index %d", i)
		}
		var (
			len    = int32(len(data))
			relPos = int64(n)
			offset = int64(i) + baseOffset
		)

		if err := binary.Write(buf, encoding, uint64(offset)); err != nil {
			return nil, nil, err
		}
		n += 8
		if err := binary.Write(buf, encoding, uint64(m.Timestamp)); err != nil {
			return nil, nil, err
		}
		n += 8
		if err := binary.Write(buf, encoding, m.LeaderEpoch); err != nil {
			return nil, nil, err
		}
		n += 8
		if err := binary.Write(buf, encoding, uint32(len)); err != nil {
			return nil, nil, err
		}
		n += 4
		// The header's own checksum, over the 28 bytes just written. Taken from
		// the buffer rather than recomputed from the locals, so it is a checksum
		// of what will actually be on disk.
		framed := buf.Bytes()
		hdrStart := buf.Len() - headerCrcPos
		if err := binary.Write(buf, encoding, headerCrc(framed[hdrStart:])); err != nil {
			return nil, nil, err
		}
		n += 4
		if _, err := buf.Write(data); err != nil {
			return nil, nil, err
		}
		n += len
		entries[i] = &entry{
			Offset:      offset,
			Timestamp:   m.Timestamp,
			LeaderEpoch: m.LeaderEpoch,
			Position:    basePos + relPos,
			Size:        len + msgSetHeaderLen,
		}
	}
	return buf.Bytes(), entries, nil
}

// MessageMetadata is the result of a header-only read — offset, attributes,
// and headers without CRC validation.
type MessageMetadata struct {
	Offset      int64
	Timestamp   int64
	LeaderEpoch uint64
	Attributes  int8
	// Headers values are subslices of Raw and share its lifetime.
	Headers map[string][]byte
	// Raw is the full message (Key() and Value() work), BORROWED rather than
	// owned: it points into the payloadBuf passed to ReadMessageMetadata, which
	// the next call overwrites. Copy anything kept past that call, and note that
	// these bytes have not been CRC-checked. See ReadMessageMetadata.
	Raw SerializedMessage
}

// payloadCursor is a bounds-checked walk over a record's payload. Every read
// goes through take, and the first one that would leave the buffer latches an
// error that the rest of the walk then propagates — so a parse can be written
// straight through without a length check between every field.
type payloadCursor struct {
	buf []byte
	n   int64
	err error
}

func (c *payloadCursor) take(n int64) []byte {
	if c.err != nil {
		return nil
	}
	// The overflow arm is not decoration: these lengths come off the wire, and
	// c.n+n on a large one wraps to a value that passes a naive bounds check.
	if n < 0 || c.n+n < c.n || c.n+n > int64(len(c.buf)) {
		c.err = errors.Errorf(
			"wants %d bytes at position %d of a %d-byte record", n, c.n, len(c.buf))
		return nil
	}
	b := c.buf[c.n : c.n+n]
	c.n += n
	return b
}

func (c *payloadCursor) uint16() uint16 {
	b := c.take(2)
	if b == nil {
		return 0
	}
	return encoding.Uint16(b)
}

func (c *payloadCursor) uint32() uint32 {
	b := c.take(4)
	if b == nil {
		return 0
	}
	return encoding.Uint32(b)
}

// parseHeadersAfterValue skips CRC, magic, attributes, key, and value to
// extract message headers from the raw serialized form.
//
// Every index is checked, because this is the one parse in the package that runs
// on bytes no checksum has vouched for. ReadMessageMetadata does not validate the
// payload CRC — that is the trade it exists to make — and the frame header's CRC
// covers the record's IDENTITY, not its contents. So the length fields that
// decide how far this reaches are exactly the fields nothing verifies, and an
// unchecked index here is a panic in the caller's process on a single flipped
// bit. It was exactly that: a key length of 1<<20 in a 51-byte record indexed
// straight off the end.
func parseHeadersAfterValue(buf []byte) (map[string][]byte, error) {
	c := &payloadCursor{buf: buf, n: 6}
	// Key then value: a 4-byte length, -1 for absent, followed by that many
	// bytes. Anything else negative is a damaged length, not an absent field.
	for range 2 {
		size := int32(c.uint32())
		switch {
		case size == -1:
		case size < 0:
			return nil, errors.Errorf("declares a length of %d", size)
		default:
			c.take(int64(size))
		}
	}
	numHeaders := c.uint16()
	headers := make(map[string][]byte, numHeaders)
	for range int(numHeaders) {
		key := c.take(int64(c.uint16()))
		value := c.take(int64(c.uint32()))
		if c.err != nil {
			break
		}
		headers[string(key)] = value
	}
	if c.err != nil {
		return nil, c.err
	}
	return headers, nil
}

func (ms messageSet) Offset() int64 {
	return int64(encoding.Uint64(ms[offsetPos : offsetPos+8]))
}

func (ms messageSet) Timestamp() int64 {
	return int64(encoding.Uint64(ms[timestampPos : timestampPos+8]))
}

func (ms messageSet) LeaderEpoch() uint64 {
	return encoding.Uint64(ms[leaderEpochPos : leaderEpochPos+8])
}

func (ms messageSet) Size() int32 {
	return int32(encoding.Uint32(ms[sizePos : sizePos+4]))
}

func (ms messageSet) Message() SerializedMessage {
	if len(ms) <= msgSetHeaderLen {
		return nil
	}
	size := ms.Size()
	return SerializedMessage(ms[msgSetHeaderLen : msgSetHeaderLen+size])
}
