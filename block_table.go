package commitlog

import (
	"hash/crc32"

	"github.com/ligustah/commitlog/compress"
	"github.com/pkg/errors"
)

// blocksSuffix names the object holding a segment's block table.
const blocksSuffix = ".blocks"

const (
	blockTableMagic   = 0x42 // 'B'
	blockTableVersion = 1
	// blockTableHeaderLen is magic, version, and the block count.
	blockTableHeaderLen = 1 + 1 + 4
	// blockTableEntryLen is one block: its uncompressed length, its physical
	// length (header included), and its codec.
	blockTableEntryLen = 4 + 4 + 1
)

// ErrBlockTableFormat means the object holding a block table is not one.
var ErrBlockTableFormat = errors.New("commitlog: not a block table")

// The block table is a segment's map from logical position to a byte range in
// the object, and it is the one thing a tier manifest entry does not carry. It
// is written to the store at offload, beside the log and index objects, so that
// opening a tier and reading from it both cost what the manifest says they cost.
//
// The alternative was to rebuild it by walking the object's block header chain,
// which is a read of the whole object — 22MB across a 22-segment snappy tier,
// measured. Doing that at open made a reopen download the entire tier before
// serving anything; deferring it to the first read only moved the same download
// behind the first record anyone asked for. Persisting it removes the walk
// rather than rescheduling it: the fetch is a few KB, once, for a segment
// somebody actually reads.
//
// It is not in the manifest itself for a reason of scale. The manifest is read
// WHOLE on every open, and one entry per segment is bounded by the tier's
// segment count; block tables are bounded by its total block count, which is
// three orders larger. Inlining them would put the tier's whole block census on
// the boot path, which is the cost this exists to remove.
//
// Only the per-block LENGTHS are stored. Starts are the running sums of the
// lengths before them, exactly as scanBlocks accumulates them while walking, so
// there is no way for a start to disagree with the lengths around it — an
// inconsistency the format simply cannot express is better than one a reader has
// to check for.
func encodeBlockTable(blocks []blockRef) []byte {
	buf := make([]byte, blockTableHeaderLen+len(blocks)*blockTableEntryLen+4)
	buf[0] = blockTableMagic
	buf[1] = blockTableVersion
	encoding.PutUint32(buf[2:], uint32(len(blocks)))
	at := blockTableHeaderLen
	for _, b := range blocks {
		encoding.PutUint32(buf[at:], uint32(b.logicalLen))
		encoding.PutUint32(buf[at+4:], uint32(b.physLen))
		buf[at+8] = byte(b.codec)
		at += blockTableEntryLen
	}
	encoding.PutUint32(buf[at:], crc32.ChecksumIEEE(buf[:at]))
	return buf
}

// decodeBlockTable reads back what encodeBlockTable wrote, rebuilding each
// block's start positions by accumulation.
//
// Every failure here is refused rather than repaired. A block table that does
// not decode cannot be approximated: a wrong table maps logical offsets onto the
// wrong bytes, so the segment answers reads with plausible garbage instead of an
// error. There is no fallback to walking the object either — that walk is
// precisely the cost this removes, and a silent fallback would hide the failure
// behind a slow success and put the tier back on the boot path.
func decodeBlockTable(buf []byte) ([]blockRef, error) {
	if len(buf) < blockTableHeaderLen+4 {
		return nil, errors.Wrapf(ErrBlockTableFormat, "block table is %d bytes", len(buf))
	}
	if buf[0] != blockTableMagic {
		return nil, errors.Wrapf(ErrBlockTableFormat, "magic 0x%02x", buf[0])
	}
	if buf[1] != blockTableVersion {
		return nil, errors.Wrapf(ErrBlockTableFormat, "version %d, want %d",
			buf[1], blockTableVersion)
	}
	n := int(encoding.Uint32(buf[2:]))
	want := blockTableHeaderLen + n*blockTableEntryLen + 4
	if len(buf) != want {
		return nil, errors.Wrapf(ErrBlockTableFormat,
			"%d blocks need %d bytes, object is %d", n, want, len(buf))
	}
	body := buf[:want-4]
	if got, exp := crc32.ChecksumIEEE(body), encoding.Uint32(buf[want-4:]); got != exp {
		return nil, errors.Wrapf(ErrBlockTableFormat, "crc %08x, want %08x", got, exp)
	}
	blocks := make([]blockRef, 0, n)
	var logical, phys int64
	at := blockTableHeaderLen
	for i := 0; i < n; i++ {
		uLen := int64(encoding.Uint32(body[at:]))
		pLen := int64(encoding.Uint32(body[at+4:]))
		codec := compress.Codec(body[at+8])
		if pLen < blockHeaderLen {
			return nil, errors.Wrapf(ErrBlockTableFormat,
				"block %d is %d bytes, shorter than a header", i, pLen)
		}
		blocks = append(blocks, blockRef{
			logicalStart: logical,
			logicalLen:   uLen,
			physStart:    phys,
			physLen:      pLen,
			codec:        codec,
		})
		logical += uLen
		phys += pLen
		at += blockTableEntryLen
	}
	return blocks, nil
}

// blockTableExtent is the logical and physical size the table accounts for. Both
// are the segment's position and physPosition, so a table that disagrees with
// the manifest entry beside it is a mismatch worth catching at the point the two
// meet rather than at the read that trips over it.
func blockTableExtent(blocks []blockRef) (logical, phys int64) {
	if len(blocks) == 0 {
		return 0, 0
	}
	last := blocks[len(blocks)-1]
	return last.logicalStart + last.logicalLen, last.physStart + last.physLen
}
