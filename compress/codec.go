// Package compress provides the block-compression codecs used by the commitlog
// segment layer. A codec turns a message-set (a batch of framed messages) into a
// compressed block on disk and back; compression is transparent to the log's
// logical byte space, so offsets, the index, and message framing are unchanged.
//
// Larger batches compress dramatically better because the per-record redundancy
// (repeated schemas, keys, field names) dedups across the whole batch — see the
// codec/batch-size comparison in bench_test.go.
package compress

import (
	"fmt"

	gsnappy "github.com/golang/snappy"
	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
)

// Codec identifies a compression algorithm. The value is stored in each block
// header, so it must be stable on disk.
type Codec byte

const (
	// None stores blocks uncompressed (the default; also used per-block when a
	// batch does not compress smaller than raw).
	None Codec = 0
	// Snappy is fast with a modest, batch-size-insensitive ratio.
	Snappy Codec = 1
	// S2 is a faster, higher-ratio Snappy successor.
	S2 Codec = 2
	// Zstd gives by far the best ratio, especially on larger batches, at a still
	// high throughput — the recommended choice for an IO-bound log.
	Zstd Codec = 3
)

var (
	// Bounded encoder/decoder state: the library defaults size their lane
	// pools by GOMAXPROCS with multi-MB windows — hundreds of MB of standing
	// memory on a many-core box (measured as a ~750MB daemon baseline in the
	// sqlcdc soak the day zstd was enabled). Commitlog blocks are small; a
	// 1MB window loses nothing, and 2/4 lanes keep encode/decode concurrent
	// enough for an IO-bound log.
	zstdEnc, _ = zstd.NewWriter(nil,
		zstd.WithEncoderConcurrency(2),
		zstd.WithWindowSize(1<<20))
	zstdDec, _ = zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(4),
		zstd.WithDecoderLowmem(true))
)

// Compress returns the compressed form of src. It never mutates src.
func (c Codec) Compress(src []byte) []byte {
	switch c {
	case Snappy:
		return gsnappy.Encode(nil, src)
	case S2:
		return s2.Encode(nil, src)
	case Zstd:
		return zstdEnc.EncodeAll(src, nil)
	default:
		return src
	}
}

// Decompress returns the decompressed form of src.
func (c Codec) Decompress(src []byte) ([]byte, error) {
	switch c {
	case Snappy:
		return gsnappy.Decode(nil, src)
	case S2:
		return s2.Decode(nil, src)
	case Zstd:
		return zstdDec.DecodeAll(src, nil)
	case None:
		return src, nil
	default:
		return nil, fmt.Errorf("compress: unknown codec %d", c)
	}
}

// Valid reports whether c is a known codec.
func (c Codec) Valid() bool { return c <= Zstd }

func (c Codec) String() string {
	switch c {
	case None:
		return "none"
	case Snappy:
		return "snappy"
	case S2:
		return "s2"
	case Zstd:
		return "zstd"
	default:
		return fmt.Sprintf("codec(%d)", byte(c))
	}
}

// Parse maps a codec name to a Codec.
func Parse(s string) (Codec, error) {
	switch s {
	case "", "none":
		return None, nil
	case "snappy":
		return Snappy, nil
	case "s2":
		return S2, nil
	case "zstd":
		return Zstd, nil
	default:
		return None, fmt.Errorf("compress: unknown codec %q", s)
	}
}
