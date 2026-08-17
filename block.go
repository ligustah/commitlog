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
// The header's magic byte disambiguates a compressed segment from a raw one
// (what a None codec writes): a raw segment begins with a message-set whose
// first byte is the high byte
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
	//
	// v2 added the record count. v1 is not read: see parseBlockHeader.
	BlockFormatVersion byte = 2
)

// blockHeaderLen is magic(1) + version(1) + codec(1) + uncompressedLen(4) +
// compressedLen(4) + records(4).
//
// Deliberately untyped and deliberately not in the group above: it is a LENGTH,
// not a header field, and it is added to int64 file offsets throughout. Sitting
// under `blockMagic byte` it read as though it inherited `byte` — it does not,
// because it has its own value — and a reader who believed that would conclude
// the arithmetic in payloadStart could not compile.
const blockHeaderLen = 15

// blockRef indexes one on-disk block: its position in the logical (uncompressed)
// byte space and in the physical (on-disk) file.
type blockRef struct {
	logicalStart int64          // uncompressed start position
	logicalLen   int64          // uncompressed length
	physStart    int64          // physical file offset of the block header
	physLen      int64          // header + compressed payload length
	codec        compress.Codec // codec of this block (None when stored raw)
	// records is how many messages this block holds, stored rather than
	// derived. A block's logical extent gives its BYTES and its index anchor
	// gives its FIRST OFFSET, and neither answers how many records are inside:
	// the offsets a block covers are contiguous only until a compaction pass
	// drops records from it, after which the span between the first and the
	// last counts the holes as well. See segment.MessageCount.
	records int64
}

func (b blockRef) payloadStart() int64 { return b.physStart + blockHeaderLen }
func (b blockRef) payloadLen() int64   { return b.physLen - blockHeaderLen }

// encodeBlockHeader writes a block header for a payload with the given codec,
// lengths and record count.
func encodeBlockHeader(codec compress.Codec, uncompressedLen, compressedLen, records uint32) []byte {
	hdr := make([]byte, blockHeaderLen)
	hdr[0] = blockMagic
	hdr[1] = BlockFormatVersion
	hdr[2] = byte(codec)
	encoding.PutUint32(hdr[3:], uncompressedLen)
	encoding.PutUint32(hdr[7:], compressedLen)
	encoding.PutUint32(hdr[11:], records)
	return hdr
}

// parseBlockHeader reads a block header, returning the codec, lengths and
// record count.
//
// Every refusal but ONE carries ErrSegmentUnreadable, and the exception is the
// whole reason the sentinel is chosen here rather than by a caller. A header
// that is entirely present and wrong means the bytes on this replica are
// damaged: a caller with a peer to copy from can act on that, and retrying the
// same call cannot fix it — which is what ErrSegmentUnreadable says and what
// its five read-path sites already mean by it.
//
// Until this, none of that reached an opener. scanBlocks refuses the open on a
// corrupt header, and that error travelled to New with no sentinel at all — so
// a caller applying the rule on New's doc comment ("a commitlog sentinel means
// permanent; anything else is an OS or store error and may be transient")
// retried forever on bytes that will never parse. See #312; it is #309's defect
// reached by a different door.
//
// The exception is the version byte. ErrBlockFormat means another BUILD wrote
// these bytes — they are exactly what its writer meant, and copying from a peer
// returns the same bytes back. Filing that as unreadable aims an operator at
// restoring a replica when the remedy is running the right binary.
//
// Chosen HERE, where the class is known, and not by an `errors.Is` arm in
// scanBlocks — an arm there would be textually identical to the one deleted
// from that function earlier the same day for buying nothing, while doing the
// opposite work. Deciding at the refusal needs no arm anywhere.
func parseBlockHeader(hdr []byte) (codec compress.Codec, uncompressedLen, compressedLen, records uint32, err error) {
	if len(hdr) < blockHeaderLen {
		return 0, 0, 0, 0, fmt.Errorf("%w: short block header (%d bytes)",
			ErrSegmentUnreadable, len(hdr))
	}
	if hdr[0] != blockMagic {
		return 0, 0, 0, 0, fmt.Errorf("%w: bad block magic 0x%02x",
			ErrSegmentUnreadable, hdr[0])
	}
	// Clean cutover: pre-version segments are not supported. Refusing here
	// is the point — the alternative is reading a layout we do not
	// understand and corrupting state before anyone notices.
	if v := hdr[1]; v != BlockFormatVersion {
		return 0, 0, 0, 0, fmt.Errorf("%w: block format version %d, this build writes %d",
			ErrBlockFormat, v, BlockFormatVersion)
	}
	codec = compress.Codec(hdr[2])
	if !codec.Valid() {
		return 0, 0, 0, 0, fmt.Errorf("%w: unknown block codec %d",
			ErrSegmentUnreadable, hdr[2])
	}
	// A block with no records cannot exist — write() refuses an empty message
	// set before a byte is appended — so a zero here is a field nobody wrote.
	// It is refused rather than treated as "unknown" for the reason
	// MessageCount gives: a count that silently reads low is a retention walk
	// that deletes what it was asked to keep, and there is no value of this
	// field that means "ask someone else".
	//
	// NOT ErrBlockFormat, though it was. That sentinel says "this build cannot
	// read this FORMAT", which is a whole-store fact a caller acts on at
	// startup — and reaching this line means the version byte was one this
	// build DOES write, which is the only reason the record count was read at
	// all. The message said so out loud: "unsupported block format version:
	// block header claims no records". A zero here is damage in one header, and
	// filing damage under the version sentinel tells an operator to go looking
	// for a build mismatch that does not exist.
	if r := encoding.Uint32(hdr[11:]); r == 0 {
		return 0, 0, 0, 0, fmt.Errorf("%w: block header claims no records",
			ErrSegmentUnreadable)
	}
	return codec, encoding.Uint32(hdr[3:]), encoding.Uint32(hdr[7:]), encoding.Uint32(hdr[11:]), nil
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
	// receive copies made under mu (segment.blockCopyIntoCache), never these
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
