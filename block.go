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
	blockMagic byte = 0xC1
	// BlockFormatVersion is the block-segment layout version, carried in
	// every block header so a segment DESCRIBES ITSELF. A magic byte alone
	// only answers "is this a block?"; it cannot answer "is this a block I
	// understand?", which is what a reader must know before it starts
	// applying data.
	//
	// This exists so startup can PROBE each component's own bytes instead
	// of consulting a side manifest. A manifest is a second source of
	// truth and can disagree with what it describes — restore a mixed
	// backup and it claims one version while the segments hold another.
	// Bytes cannot lie that way.
	BlockFormatVersion byte = 1
	blockHeaderLen          = 11 // magic(1) + version(1) + codec(1) + uncompressedLen(4) + compressedLen(4)
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
	hdr[1] = BlockFormatVersion
	hdr[2] = byte(codec)
	encoding.PutUint32(hdr[3:], uncompressedLen)
	encoding.PutUint32(hdr[7:], compressedLen)
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
	// Clean cutover: pre-version segments are not supported. Refusing here
	// is the point — the alternative is reading a layout we do not
	// understand and corrupting state before anyone notices.
	if v := hdr[1]; v != BlockFormatVersion {
		return 0, 0, 0, fmt.Errorf("%w: block format version %d, this build writes %d",
			ErrBlockFormat, v, BlockFormatVersion)
	}
	codec = compress.Codec(hdr[2])
	if !codec.Valid() {
		return 0, 0, 0, fmt.Errorf("commitlog: unknown block codec %d", hdr[2])
	}
	return codec, encoding.Uint32(hdr[3:]), encoding.Uint32(hdr[7:]), nil
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
