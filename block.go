package commitlog

import (
	"fmt"
	"sync"

	"github.com/ligustah/commitlog/compress"
)

// Block compression stores each written message-set as a self-describing block
// in the segment file, while the log's *logical* byte space — the offsets, the
// offset index, and the message framing — is unchanged. A block header records
// the codec and the uncompressed/compressed lengths so the logical positions can
// be mapped to physical file positions on read and rebuilt on open.
//
// The header's magic byte disambiguates a compressed segment from a legacy raw
// one: a raw segment begins with a message-set whose first byte is the high byte
// of a big-endian offset (0x00 for any realistic offset), which can never be the
// magic.
const (
	blockMagic     byte = 0xC1
	blockHeaderLen      = 10 // magic(1) + codec(1) + uncompressedLen(4) + compressedLen(4)
)

// blockRef indexes one on-disk block: its position in the logical (uncompressed)
// byte space and in the physical (on-disk) file.
type blockRef struct {
	logicalStart int64          // uncompressed start position
	logicalLen   int64          // uncompressed length
	physStart    int64          // physical file offset of the block header
	physLen      int64          // header + compressed payload length
	codec        compress.Codec // codec of this block (None when stored raw)
}

func (b blockRef) payloadStart() int64 { return b.physStart + blockHeaderLen }
func (b blockRef) payloadLen() int64   { return b.physLen - blockHeaderLen }

// encodeBlockHeader writes a block header for a payload with the given codec and
// lengths.
func encodeBlockHeader(codec compress.Codec, uncompressedLen, compressedLen uint32) []byte {
	hdr := make([]byte, blockHeaderLen)
	hdr[0] = blockMagic
	hdr[1] = byte(codec)
	encoding.PutUint32(hdr[2:], uncompressedLen)
	encoding.PutUint32(hdr[6:], compressedLen)
	return hdr
}

// parseBlockHeader reads a block header, returning the codec and lengths.
func parseBlockHeader(hdr []byte) (codec compress.Codec, uncompressedLen, compressedLen uint32, err error) {
	if len(hdr) < blockHeaderLen {
		return 0, 0, 0, fmt.Errorf("commitlog: short block header (%d bytes)", len(hdr))
	}
	if hdr[0] != blockMagic {
		return 0, 0, 0, fmt.Errorf("commitlog: bad block magic 0x%02x", hdr[0])
	}
	codec = compress.Codec(hdr[1])
	if !codec.Valid() {
		return 0, 0, 0, fmt.Errorf("commitlog: unknown block codec %d", hdr[1])
	}
	return codec, encoding.Uint32(hdr[2:]), encoding.Uint32(hdr[6:]), nil
}

// blockCache memoizes the most recently decompressed block so sequential reads
// (the buffered reader fills 64 KB at a time from a possibly larger block) don't
// re-decompress it. Guarded by its own mutex since reads hold only a read lock.
// A cache may be shared ACROSS segments (a scan's cache walks many), so the
// entry is keyed by (seg, physStart), not physStart alone — two segments'
// blocks can share a physical start.
type blockCache struct {
	mu    sync.Mutex
	seg   *segment
	start int64 // physStart of the cached block, -1 when empty
	data  []byte
	// raw is the recycled compressed-payload read buffer. Both raw and
	// data are OWNED by the cache and overwritten on displacement; callers
	// receive copies made under mu (segment.blockCopyInto), never these
	// slices.
	raw []byte
}

func newBlockCache() *blockCache { return &blockCache{start: -1} }

func (c *blockCache) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seg = nil
	c.start = -1
	c.data = nil
}
