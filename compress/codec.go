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
	// memory on a many-core box (measured as a ~750MB baseline in a
	// long-running soak the day zstd was enabled). Commitlog blocks are small; a
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
//
// The default arm stores src raw, and unlike Decompress it cannot report an
// unknown codec — there is no error to return. That is safe only because a codec
// is checked where it ENTERS: commitlog.New refuses one Valid rejects, so a
// Codec that reaches here is one of the four below. It was not always: an unknown
// codec used to be accepted, written into every block header, and refused on the
// way back out by parseBlockHeader, which is the read path saying no to what the
// write path had already stored.
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
	return c.DecompressInto(nil, src)
}

// DecompressInto decompresses src, appending to dst (which may be nil or a
// recycled buffer with its length reset). Scan-heavy paths — compaction
// rewrites read every block of every segment exactly once — reuse one
// scratch buffer per scanner instead of allocating a fresh output per block:
// a 12h soak's clean over a ~1200-segment stream showed ~276MB of dead
// DecodeAll buffers awaiting GC in a single anomaly heap capture.
func (c Codec) DecompressInto(dst, src []byte) ([]byte, error) {
	switch c {
	case Snappy:
		return gsnappy.Decode(dst, src)
	case S2:
		return s2.Decode(dst, src)
	case Zstd:
		return zstdDec.DecodeAll(src, dst[:0])
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
