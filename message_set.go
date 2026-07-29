package commitlog

import (
	"bytes"
	"encoding/binary"

	"github.com/pkg/errors"
)

const (
	offsetPos       = 0
	timestampPos    = 8
	leaderEpochPos  = 16
	sizePos         = 24
	msgSetHeaderLen = 28
)

// HeaderBufferLen is the capacity the headersBuf argument to
// Reader.ReadMessage and Reader.ReadMessageMetadata must have.
//
// Exported because it was previously a bare "28" in a doc comment, and a
// consumer duly wrote make([]byte, 28) against it. A number copied out of prose
// is the same mistake as a magic byte copied into another repo: correct until it
// isn't, and silent when it stops being.
const HeaderBufferLen = msgSetHeaderLen

type messageSet []byte

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

func newMessageSetFromProto(baseOffset, basePos int64, msgs []*Message, concurrencyControl bool) (
	messageSet, []*entry, error) {

	// When concurrency control is enabled, messages shall be processed on by one
	if concurrencyControl && len(msgs) > 1 {
		return nil, nil, errors.Errorf(
			"commitlog: concurrency control processes one message at a time, got a batch of %d",
			len(msgs))
	}

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

		// Check expected offset for concurrency in case of Optimistic Concurrency Control
		if concurrencyControl && m.Offset != -1 {
			if offset != m.Offset {
				return nil, nil, ErrIncorrectOffset
			}
		}

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
