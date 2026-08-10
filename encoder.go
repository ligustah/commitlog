package commitlog

import (
	"encoding/binary"
	"errors"
	"math"
)

var (
	// encoding is the byte order to use for internal disk serialization.
	encoding = binary.BigEndian

	errInvalidStringLength    = errors.New("invalid string length")
	errInvalidByteSliceLength = errors.New("invalid byteslice length")
)

// packetEncoder is used to serialize an object.
//
// It carries exactly what the record format writes and nothing else. It began
// as a Kafka wire-protocol encoder — bools, nullable strings, int32/int64/string
// arrays, raw byte runs — and the log serializes none of those, so every one of
// them was a method two implementations had to carry and no encode path could
// reach. A format that gains a field gains the method back; a set of methods
// that outruns the format only makes the two disagree about which one is
// authoritative.
type packetEncoder interface {
	PutInt8(in int8)
	PutInt16(in int16)
	PutBytes(in []byte) error
	PutString(in string) error
	Push(pe pushEncoder)
	Pop()
}

// pushEncoder is used to push an operation onto the stack to perform later
// once serialized bytes are filled.
type pushEncoder interface {
	SaveOffset(in int)
	ReserveSize() int
	Fill(curOffset int, buf []byte) error
}

// encoder is a struct that can be serialized.
type encoder interface {
	Encode(e packetEncoder) error
}

// encode serializes the struct to bytes.
func encode(e encoder) ([]byte, error) {
	lenEnc := new(lenEncoder)
	err := e.Encode(lenEnc)
	if err != nil {
		return nil, err
	}

	b := make([]byte, lenEnc.Length)
	byteEnc := newByteEncoder(b)
	err = e.Encode(byteEnc)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// lenEncoder is a packetEncoder that tracks the running length of serialized
// bytes.
type lenEncoder struct {
	Length int
}

// PutInt8 increments length for an int8.
func (e *lenEncoder) PutInt8(in int8) {
	e.Length++
}

// PutInt16 increments length for an int16.
func (e *lenEncoder) PutInt16(in int16) {
	e.Length += 2
}

// PutBytes increments length for a size-prefixed byte array.
func (e *lenEncoder) PutBytes(in []byte) error {
	e.Length += 4
	if in == nil {
		return nil
	}
	if len(in) > math.MaxInt32 {
		return errInvalidByteSliceLength
	}
	e.Length += len(in)
	return nil
}

// PutString increments length for a string.
func (e *lenEncoder) PutString(in string) error {
	e.Length += 2
	if len(in) > math.MaxInt16 {
		return errInvalidStringLength
	}
	e.Length += len(in)
	return nil
}

// Push increments length based on the pushEncoder's reserved size.
func (e *lenEncoder) Push(pe pushEncoder) {
	e.Length += pe.ReserveSize()
}

// Pop is a no-op.
func (e *lenEncoder) Pop() {}

// byteEncoder is a packetEncoder that serializes data into a byte slice.
type byteEncoder struct {
	b     []byte
	off   int
	stack []pushEncoder
}

// NewByteEncoder creates a new ByteEncoder with the given backing
// pre-allocated byte slice.
func newByteEncoder(b []byte) *byteEncoder {
	return &byteEncoder{b: b}
}

// PutInt8 serializes an int8.
func (e *byteEncoder) PutInt8(in int8) {
	e.b[e.off] = byte(in)
	e.off++
}

// PutInt16 serializes an int16.
func (e *byteEncoder) PutInt16(in int16) {
	encoding.PutUint16(e.b[e.off:], uint16(in))
	e.off += 2
}

// putInt32 serializes an int32. Not on packetEncoder: nothing encodes a bare
// int32, it is the size prefix PutBytes writes.
func (e *byteEncoder) putInt32(in int32) {
	encoding.PutUint32(e.b[e.off:], uint32(in))
	e.off += 4
}

// PutBytes serializes a size-prefixed byte slice.
func (e *byteEncoder) PutBytes(in []byte) error {
	if in == nil {
		e.putInt32(-1)
		return nil
	}
	e.putInt32(int32(len(in)))
	copy(e.b[e.off:], in)
	e.off += len(in)
	return nil
}

// PutString serializes a size-prefixed string.
func (e *byteEncoder) PutString(in string) error {
	e.PutInt16(int16(len(in)))
	copy(e.b[e.off:], in)
	e.off += len(in)
	return nil
}

// Push adds the given pushEncoder to the stack and saves the current offset
// position.
func (e *byteEncoder) Push(pe pushEncoder) {
	pe.SaveOffset(e.off)
	e.off += pe.ReserveSize()
	e.stack = append(e.stack, pe)
}

// Pop the stack and run the popped pushEncoder on the serialized data.
func (e *byteEncoder) Pop() {
	// this is go's ugly pop pattern (the inverse of append)
	pe := e.stack[len(e.stack)-1]
	e.stack = e.stack[:len(e.stack)-1]
	pe.Fill(e.off, e.b)
}
