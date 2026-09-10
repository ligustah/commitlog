package commitlog

import (
	"hash/crc32"

	"github.com/ligustah/commitlog/blockv3"
	"github.com/ligustah/commitlog/compress"
	"github.com/pkg/errors"
)

// blocksSuffix names the object holding a segment's block table.
const blocksSuffix = ".blocks"

const (
	blockTableMagic = 0x42 // 'B'
	// blockTableVersion 2 added the per-block record count, alongside
	// BlockFormatVersion 2 adding it to the header the table summarises; 4
	// added each block's format version and, for a version-3 block, its
	// flags — so a table can describe a segment holding version-3 blocks,
	// whose logical length is NOT in their header, and say which of them a
	// rewrite may merge without reading one. (3, the same without the flags,
	// was never written outside this package's tests and is refused.)
	blockTableVersion = 4
	// blockTableHeaderLen is magic, version, and the block count.
	blockTableHeaderLen = 1 + 1 + 4
	// blockTableEntryLen is one block: its uncompressed length, its physical
	// length (header included), its codec, its record count, its format
	// version and its flags.
	blockTableEntryLen = 4 + 4 + 1 + 4 + 1 + 1
	// blockTableEntryLenV2 is an entry of a version-2 table, which has no
	// format byte: every block it describes is BlockFormatVersion.
	blockTableEntryLenV2 = 4 + 4 + 1 + 4
)

// ErrBlockTableFormat means the object holding a block table is not one.
var ErrBlockTableFormat = errors.New("commitlog: not a block table")

// maxBlockTableBytes is the largest table a segment of phys physical bytes could
// possibly need, and it exists to bound an allocation made from a size the STORE
// reported and nothing has verified.
//
// Derived rather than picked, the same argument maxDescriptorBytes makes. The
// table's layout is fixed-width, and every block occupies at least
// blockHeaderLen physical bytes in the object, so a segment of phys bytes holds
// at most phys/blockHeaderLen blocks. A size past this cannot decode whatever
// else is true of it — decodeBlockTable requires an EXACT length and would refuse
// it — so refusing early costs nothing that could have succeeded, and refuses it
// before the bytes are allocated rather than after.
func maxBlockTableBytes(phys int64) int64 {
	return blockTableHeaderLen + (phys/blockHeaderLen)*blockTableEntryLen + 4
}

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
//
// A table describing only version-2 blocks is written in the version-2 layout,
// which the previous release reads. The format and flag bytes exist for
// version-3 blocks, and a segment holding none has nothing to say with them —
// while a table the previous release refuses would make a rollback fail to
// open every segment sealed since the upgrade, over fields that carried no
// information.
func encodeBlockTable(blocks []blockRef) []byte {
	version, entryLen := byte(2), blockTableEntryLenV2
	for _, b := range blocks {
		if b.version != BlockFormatVersion {
			version, entryLen = blockTableVersion, blockTableEntryLen
			break
		}
	}
	buf := make([]byte, blockTableHeaderLen+len(blocks)*entryLen+4)
	buf[0] = blockTableMagic
	buf[1] = version
	encoding.PutUint32(buf[2:], uint32(len(blocks)))
	at := blockTableHeaderLen
	for _, b := range blocks {
		encoding.PutUint32(buf[at:], uint32(b.logicalLen))
		encoding.PutUint32(buf[at+4:], uint32(b.physLen))
		buf[at+8] = byte(b.codec)
		encoding.PutUint32(buf[at+9:], uint32(b.records))
		if version == blockTableVersion {
			buf[at+13] = b.version
			buf[at+14] = b.flags
		}
		at += entryLen
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
	// Version 2 tables are read: they were written by the previous release for
	// segments holding only version-2 blocks, and a tier full of them must not
	// walk every object because the entry grew a byte.
	entryLen := blockTableEntryLen
	switch buf[1] {
	case blockTableVersion:
	case 2:
		entryLen = blockTableEntryLenV2
	default:
		return nil, errors.Wrapf(ErrBlockTableFormat, "version %d, want %d",
			buf[1], blockTableVersion)
	}
	n := int(encoding.Uint32(buf[2:]))
	want := blockTableHeaderLen + n*entryLen + 4
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
		records := int64(encoding.Uint32(body[at+9:]))
		version, flags := byte(BlockFormatVersion), byte(0)
		if entryLen == blockTableEntryLen {
			version, flags = body[at+13], body[at+14]
		}
		if pLen < blockHeaderLen {
			return nil, errors.Wrapf(ErrBlockTableFormat,
				"block %d is %d bytes, shorter than a header", i, pLen)
		}
		// Zero is refused for the reason parseBlockHeader refuses it in the
		// header this entry summarises: no block holds no records, so a zero is
		// a field nobody wrote, and the table is the ONLY source a tiered
		// segment consults — a zero accepted here becomes a segment that
		// reports fewer records than it holds to the retention walk.
		if records == 0 {
			return nil, errors.Wrapf(ErrBlockTableFormat,
				"block %d claims no records", i)
		}
		// A format this build does not read. Zero included: the field is
		// written from a blockRef, and a blockRef whose version was never set
		// describes a block nothing can decode.
		if version != BlockFormatVersion && version != blockv3.Version {
			return nil, errors.Wrapf(ErrBlockTableFormat,
				"block %d has format version %d", i, version)
		}
		blocks = append(blocks, blockRef{
			logicalStart: logical,
			logicalLen:   uLen,
			physStart:    phys,
			physLen:      pLen,
			codec:        codec,
			records:      records,
			version:      version,
			flags:        flags,
		})
		logical += uLen
		phys += pLen
		at += entryLen
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

// sumBlockRecords is how many messages a block table accounts for.
//
// A sum and not a stored total, deliberately: the table already refuses to
// decode unless its length and CRC match exactly, so the per-block counts either
// all arrive or none do, and a separate total could only ever disagree with
// them. It is the same argument the format makes about start positions — an
// inconsistency the format cannot express beats one a reader has to check for.
func sumBlockRecords(blocks []blockRef) int64 {
	var n int64
	for _, b := range blocks {
		n += b.records
	}
	return n
}
