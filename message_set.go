package commitlog

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/pkg/errors"
)

const (
	offsetPos       = 0
	timestampPos    = 8
	leaderEpochPos  = 16
	sizePos         = 24
	msgSetHeaderLen = 28
)

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
		panic(fmt.Errorf("Concurrency Control is enabled, unable to process a batch of messages"))
	}

	var (
		buf     = new(bytes.Buffer)
		entries = make([]*entry, len(msgs))
		n       int32
	)
	for i, m := range msgs {
		data, err := encode(m)
		if err != nil {
			panic(err)
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

// readMessage reads a single message from the reader or blocks until one is
// available. It returns the Message in addition to its offset, timestamp, and
// leader epoch. This may return uncommitted messages if the reader was created
// with the uncommitted flag set to true.
func readMessage(ctx context.Context, reader contextReader, headersBuf []byte) (SerializedMessage, int64, int64, uint64, error) {
	if _, err := reader.Read(ctx, headersBuf); err != nil {
		return nil, 0, 0, 0, errors.Wrap(err, "failed to read message headers")
	}
	var (
		offset      = int64(encoding.Uint64(headersBuf[offsetPos:]))
		timestamp   = int64(encoding.Uint64(headersBuf[timestampPos:]))
		leaderEpoch = encoding.Uint64(headersBuf[leaderEpochPos:])
		size        = encoding.Uint32(headersBuf[sizePos:])
		buf         = make([]byte, int(size))
	)
	if _, err := reader.Read(ctx, buf); err != nil {
		return nil, 0, 0, 0, errors.Wrap(err, "failed to ready message payload")
	}
	m := SerializedMessage(buf)
	// Check the CRC on the message.
	crc := m.Crc()
	if c := crc32.Checksum(m[4:], crc32cTable); crc != c {
		// If the CRC doesn't match, data on disk is corrupted which means the
		// server is in an unrecoverable state.
		panic(fmt.Errorf("Read corrupted data, expected CRC: 0x%08x, got: 0x%08x", crc, c))
	}
	return m, offset, timestamp, leaderEpoch, nil
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

// readMessageMetadata reads a message from the log, parses headers and
// attributes, and returns them without CRC-validating the payload. The
// payloadBuf slice is reused across calls to avoid per-message allocations.
// Callers should pass the returned slice back on the next call.
//
// This is intended for metadata-only scans (LSO rebuild, offset tracking)
// where the value bytes are not needed and full deserialization is wasteful.
func readMessageMetadata(ctx context.Context, reader contextReader, hdrBuf []byte, payloadBuf []byte) (MessageMetadata, []byte, error) {
	if _, err := reader.Read(ctx, hdrBuf); err != nil {
		return MessageMetadata{}, payloadBuf, errors.Wrap(err, "failed to read message headers")
	}
	var (
		offset      = int64(encoding.Uint64(hdrBuf[offsetPos:]))
		timestamp   = int64(encoding.Uint64(hdrBuf[timestampPos:]))
		leaderEpoch = encoding.Uint64(hdrBuf[leaderEpochPos:])
		size        = encoding.Uint32(hdrBuf[sizePos:])
	)
	if cap(payloadBuf) < int(size) {
		payloadBuf = make([]byte, int(size))
	}
	buf := payloadBuf[:int(size)]
	if _, err := reader.Read(ctx, buf); err != nil {
		return MessageMetadata{}, payloadBuf, errors.Wrap(err, "failed to read message payload")
	}
	return MessageMetadata{
		Offset:      offset,
		Timestamp:   timestamp,
		LeaderEpoch: leaderEpoch,
		Attributes:  int8(buf[5]),
		Headers:     parseHeadersAfterValue(buf),
		Raw:         SerializedMessage(buf),
	}, payloadBuf, nil
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
