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

// parseHeadersAfterValue skips CRC, magic, attributes, key, and value to
// extract message headers from the raw serialized form.
func parseHeadersAfterValue(buf []byte) map[string][]byte {
	// Key length at offset 6
	keyLen := int32(encoding.Uint32(buf[6:10]))
	keyEnd := int32(10)
	if keyLen != -1 {
		keyEnd += keyLen
	}
	// Value length at keyEnd
	valLen := int32(encoding.Uint32(buf[keyEnd : keyEnd+4]))
	valEnd := keyEnd + 4
	if valLen != -1 {
		valEnd += valLen
	}
	n := valEnd
	numHeaders := encoding.Uint16(buf[n:])
	n += 2
	headers := make(map[string][]byte, numHeaders)
	for i := uint16(0); i < numHeaders; i++ {
		keySize := encoding.Uint16(buf[n:])
		n += 2
		key := string(buf[n : n+int32(keySize)])
		n += int32(keySize)
		valueSize := encoding.Uint32(buf[n:])
		n += 4
		value := buf[n : n+int32(valueSize)]
		n += int32(valueSize)
		headers[key] = value
	}
	return headers
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
