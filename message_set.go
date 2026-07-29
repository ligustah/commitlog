package commitlog

import (
	"bytes"
	"context"
	"encoding/binary"
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

// maxPayloadChunk bounds how far a frame's declared size is TRUSTED before any
// of it has been read.
//
// The size field is not covered by any checksum — the CRC lives inside the
// payload it describes — so a torn or damaged frame can declare any length up to
// 4GiB, and allocating that up front hands an out-of-memory kill to whoever
// embedded this log. Found by FuzzTornLogServesOnlyAPrefix, which truncates a
// segment and leaves the reader parsing a size out of whatever bytes follow: the
// fuzzing worker died with "terminated unexpectedly" rather than any assertion.
//
// So the payload is read in chunks and the buffer grows only as bytes actually
// ARRIVE. A frame claiming 4GiB of a file that holds a hundred bytes now costs
// one chunk and an error, instead of the process.
const maxPayloadChunk = 1 << 20

// readPayload fills size bytes from reader, growing into reuse when it can. It
// never allocates more than one chunk beyond what has already been read, so a
// bogus size costs a chunk rather than the address space.
func readPayload(ctx context.Context, reader contextReader, size int, reuse []byte) ([]byte, error) {
	buf := reuse[:0]
	for len(buf) < size {
		want := size - len(buf)
		if want > maxPayloadChunk {
			want = maxPayloadChunk
		}
		have := len(buf)
		if cap(buf) < have+want {
			grown := make([]byte, have, have+want)
			copy(grown, buf)
			buf = grown
		}
		buf = buf[:have+want]
		if _, err := reader.Read(ctx, buf[have:]); err != nil {
			return reuse[:0], err
		}
	}
	return buf, nil
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
	)
	buf, err := readPayload(ctx, reader, int(size), nil)
	if err != nil {
		return nil, 0, 0, 0, errors.Wrap(err, "failed to ready message payload")
	}
	m := SerializedMessage(buf)
	// The frame's size field is not covered by any checksum — the CRC lives
	// INSIDE the payload it describes — so a torn or damaged frame can claim a
	// length too short to hold that checksum. Reading it would index past the
	// end: Crc() takes m[0:4], and on a size of 0 that panicked out of the
	// caller's process, which is exactly what a log embedded in someone else's
	// binary must not do.
	//
	// Anything this short cannot be a record. encode never emits fewer than a
	// checksum, a magic byte, an attribute byte and two length prefixes, so
	// refusing here rejects only frames that were already impossible, and every
	// longer malformation is left for the CRC below to catch.
	if len(m) < 4 {
		return nil, 0, 0, 0, errors.Wrapf(ErrCorruptRecord,
			"record at offset %d: frame claims %d bytes, too short to hold a checksum", offset, len(m))
	}
	// Check the CRC on the message. Returned, not panicked: see ErrCorruptRecord
	// for why a library embedded in someone else's process must not take it down
	// over a record the caller could have skipped.
	crc := m.Crc()
	if c := crc32.Checksum(m[4:], crc32cTable); crc != c {
		return nil, 0, 0, 0, errors.Wrapf(ErrCorruptRecord,
			"record at offset %d: expected CRC 0x%08x, got 0x%08x", offset, crc, c)
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
	// Chunked for the same reason as readMessage: an unchecksummed size field
	// must not turn a torn frame into a 4GiB allocation. See maxPayloadChunk.
	buf, err := readPayload(ctx, reader, int(size), payloadBuf)
	if err != nil {
		return MessageMetadata{}, payloadBuf, errors.Wrap(err, "failed to read message payload")
	}
	payloadBuf = buf
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
