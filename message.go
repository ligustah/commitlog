package commitlog

import (
	"hash/crc32"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// Attribute bits for Message.Attributes.
const (
	// AttrControl marks a transactional control record (a commit/abort
	// marker written by a transactional layer such as durable_streams).
	AttrControl int8 = 0x01
	// AttrTombstone marks a key's terminal record. Compaction may remove a
	// latest-per-key tombstone entirely — the key vanishes from the log —
	// once it is older than the tombstone retention (CleanSpec or
	// Options.CompactTombstoneRetention). The payload is otherwise a normal
	// record; readers are unaffected by the bit.
	AttrTombstone int8 = 0x02
)

// Message is the object that gets serialized and written to the log.
type Message struct {
	Crc        int32
	MagicByte  int8
	Attributes int8
	Key        []byte
	Value      []byte
	Headers    map[string][]byte

	// Framing fields: carried alongside a message in memory rather than written
	// by Encode, because the log frames them itself. Both are INPUT to Append —
	// a caller stamping its own timestamp and epoch — and only one of them is
	// ever written back:
	//
	//   - Timestamp: a zero one is filled in with the append's clock reading,
	//     ON THE CALLER'S Message, so a caller that leaves it zero can read the
	//     stamp back afterwards. A non-zero one is used as given.
	//   - LeaderEpoch: read only. Nothing writes it back.
	//
	// Offsets are NOT here. They come from Append's return value, and there used
	// to be an Offset field that looked like it carried them: it was the
	// caller's expected offset for an optimistic-concurrency check that no
	// caller ever enabled, nothing in the log ever wrote it, and it sat in a
	// block whose doc claimed the log filled these in on the way out. Removed in
	// v0.70.0 — see CHANGELOG.md.
	Timestamp   int64
	LeaderEpoch uint64
}

// Encode the Message into the packetEncoder.
func (m *Message) Encode(e packetEncoder) error {
	e.Push(&crcField{})
	e.PutInt8(m.MagicByte)
	e.PutInt8(m.Attributes)
	if err := e.PutBytes(m.Key); err != nil {
		return err
	}
	if err := e.PutBytes(m.Value); err != nil {
		return err
	}
	e.PutInt16(int16(len(m.Headers)))
	for key, header := range m.Headers {
		if err := e.PutString(key); err != nil {
			return err
		}
		if err := e.PutBytes(header); err != nil {
			return err
		}
	}
	e.Pop()
	return nil
}

// crcField is used to perform a CRC32 check on a message.
type crcField struct {
	StartOffset int
}

// SaveOffset sets the position to fill the CRC digest.
func (f *crcField) SaveOffset(in int) {
	f.StartOffset = in
}

// ReserveSize sets the number of bytes to reserve for the CRC digest.
func (f *crcField) ReserveSize() int {
	return 4
}

// Fill sets the CRC digest.
func (f *crcField) Fill(curOffset int, buf []byte) error {
	crc := crc32.Checksum(buf[f.StartOffset+4:curOffset], crc32cTable)
	encoding.PutUint32(buf[f.StartOffset:], crc)
	return nil
}

// SerializedMessage is a serialized message read from the log.
type SerializedMessage []byte

// Crc returns the CRC32 digest of the message.
func (m SerializedMessage) Crc() uint32 {
	return encoding.Uint32(m)
}

// crcMatches reports whether the message's stored checksum matches its bytes,
// answering false for a frame too short to hold one rather than indexing past
// the end.
//
// This REPORTS; it does not guard. The read paths keep their own explicit check
// because they must refuse the record and say why, and because a guard that
// answers a bool is easy to call and forget to act on. See ErrCorruptRecord.
func (m SerializedMessage) crcMatches() bool {
	if len(m) < 4 {
		return false
	}
	return m.Crc() == crc32.Checksum(m[4:], crc32cTable)
}

// MagicByte returns the byte used for encoding protocol version detection.
func (m SerializedMessage) MagicByte() int8 {
	return int8(m[4])
}

// Attributes returns the byte used for message flags.
func (m SerializedMessage) Attributes() int8 {
	return int8(m[5])
}

// Key returns the message key.
func (m SerializedMessage) Key() []byte {
	start, end, size := m.keyOffsets()
	if size == -1 {
		return nil
	}
	return m[start+4 : end]
}

// Value returns the message value.
func (m SerializedMessage) Value() []byte {
	start, end, size := m.valueOffsets()
	if size == -1 {
		return nil
	}
	return m[start+4 : end]
}

// Headers returns the message headers map.
func (m SerializedMessage) Headers() map[string][]byte {
	var (
		_, valueEnd, _ = m.valueOffsets()
		n              = valueEnd
		numHeaders     = encoding.Uint16(m[n:])
		headers        = make(map[string][]byte, numHeaders)
	)
	n += 2
	for i := uint16(0); i < numHeaders; i++ {
		keySize := encoding.Uint16(m[n:])
		n += 2
		key := string(m[n : n+int32(keySize)])
		n += int32(keySize)
		valueSize := encoding.Uint32(m[n:])
		n += 4
		value := m[n : n+int32(valueSize)]
		n += int32(valueSize)
		headers[key] = value
	}
	return headers
}

func (m SerializedMessage) keyOffsets() (start, end, size int32) {
	return m.fieldOffsets(6)
}

func (m SerializedMessage) valueOffsets() (start, end, size int32) {
	_, keyEnd, _ := m.keyOffsets()
	return m.fieldOffsets(keyEnd)
}

// fieldOffsets locates the length-prefixed field beginning at start: a 4-byte
// size, then that many bytes.
//
// -1 is ABSENT, and is the whole reason this is one function rather than the
// two identical ones it replaces. A size of -1 is not a length to skip over —
// nothing follows it — so the field ends at start+4, and a copy that added the
// size anyway would move `end` backwards by four bytes and hand the caller a
// slice into the middle of the previous field. The key and the value are the
// same wire shape, and there is no version of this format where one of them
// spells absent differently from the other.
func (m SerializedMessage) fieldOffsets(start int32) (int32, int32, int32) {
	size := int32(encoding.Uint32(m[start:]))
	end := start + 4
	if size != -1 {
		end += size
	}
	return start, end, size
}
