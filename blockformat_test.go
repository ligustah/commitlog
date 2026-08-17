package commitlog

import (
	"errors"
	"strings"
	"testing"

	"github.com/ligustah/commitlog/compress"
)

// A block header must carry a version, and a reader must REFUSE a version
// it does not write. A magic byte alone only answers "is this a block?" —
// not "is this a block I understand?", which is what a reader has to know
// before it starts applying data.
func TestBlockHeaderCarriesVersionAndRefusesOthers(t *testing.T) {
	hdr := encodeBlockHeader(compress.Zstd, 1234, 567, 42)
	if got := hdr[1]; got != BlockFormatVersion {
		t.Fatalf("header version byte = %d, want %d", got, BlockFormatVersion)
	}
	codec, ulen, clen, records, err := parseBlockHeader(hdr)
	if err != nil {
		t.Fatalf("round-trip: %v", err)
	}
	if codec != compress.Zstd || ulen != 1234 || clen != 567 || records != 42 {
		t.Fatalf("round-trip mismatch: codec=%v ulen=%d clen=%d records=%d",
			codec, ulen, clen, records)
	}

	// A future version must be refused, not misread. Clean cutover: there
	// is deliberately no compatibility path.
	future := append([]byte(nil), hdr...)
	future[1] = BlockFormatVersion + 1
	if _, _, _, _, err := parseBlockHeader(future); !errors.Is(err, ErrBlockFormat) {
		t.Fatalf("newer block version accepted (err=%v) — a reader that guesses at an unknown layout corrupts state", err)
	}

	// A pre-version segment (version byte where the codec used to sit)
	// must also be refused rather than silently parsed as some codec.
	legacy := append([]byte(nil), hdr...)
	legacy[1] = byte(compress.None)
	if _, _, _, _, err := parseBlockHeader(legacy); !errors.Is(err, ErrBlockFormat) {
		t.Fatalf("pre-version segment accepted (err=%v)", err)
	}
}

// A v1 header is 11 bytes and a v2 header is 15, so a v1 segment read by this
// build does not merely lose the record count — every block boundary after the
// first is four bytes off. The version byte is what stops that walk before it
// starts, which is worth pinning separately from the round trip above: the
// header this constructs is the one an earlier release actually wrote.
func TestAV1BlockHeaderIsRefusedRatherThanMisread(t *testing.T) {
	v1 := []byte{
		0xC1,                   // magic
		1,                      // BlockFormatVersion as it was
		byte(compress.Snappy),  // codec
		0x00, 0x00, 0x04, 0xD2, // uncompressedLen 1234
		0x00, 0x00, 0x02, 0x37, // compressedLen 567
		// and nothing else: a v1 header ended here.
	}
	if len(v1) != 11 {
		t.Fatalf("this fixture is meant to be the 11-byte v1 header, got %d", len(v1))
	}
	// Padded to the length a v2 parse demands, so the refusal below is the
	// VERSION byte doing its job and not a short-header check standing in for
	// it. Those four bytes are whatever followed in the file — the next block's
	// magic and version, here.
	v1 = append(v1, 0xC1, 1, byte(compress.Snappy), 0x00)

	if _, _, _, _, err := parseBlockHeader(v1); !errors.Is(err, ErrBlockFormat) {
		t.Fatalf("a v1 block header was accepted by a v2 reader (err=%v): every "+
			"block boundary after this one would be four bytes off", err)
	}
}

// A block always holds at least one record — write() refuses an empty message
// set before a byte is appended — so a zero in that field is a header nobody
// filled in, and it must not be read as "this block is empty".
//
// The direction is the whole point. A count that reads LOW makes a retention
// walk believe the log is under its message limit, so it keeps segments it was
// asked to drop; a count that reads low on EVERY block makes MessageCount
// answer 0 for the segment, and a log that reports no records is one nothing
// can trim at all.
// It is refused as DAMAGE, not as a format problem. This asserted
// errors.Is(err, ErrBlockFormat) when the field was added, and that was wrong
// in a way the error text spelled out: "unsupported block format version:
// block header claims no records". The version byte is correct — that is the
// only reason the record count was read at all. See ErrBlockFormat's doc for
// why the boundary is one a caller acts on rather than a tidy-up.
func TestABlockHeaderClaimingNoRecordsIsRefused(t *testing.T) {
	hdr := encodeBlockHeader(compress.Snappy, 100, 80, 0)
	_, _, _, _, err := parseBlockHeader(hdr)
	if err == nil {
		t.Fatal("a block header claiming zero records was accepted")
	}
	if !strings.Contains(err.Error(), "claims no records") {
		t.Fatalf("refused by a different check than the record count: %v", err)
	}
	if errors.Is(err, ErrBlockFormat) {
		t.Fatalf("filed under ErrBlockFormat, which tells a caller another "+
			"build wrote this store; the version byte here is this build's own "+
			"(err=%v)", err)
	}
	// Not merely "some other error": this refusal reaches an opener, where a
	// caller sorts permanent from transient on the sentinel alone. Without one
	// it reads as a busy disk and gets retried forever. See #312.
	if !errors.Is(err, ErrSegmentUnreadable) {
		t.Fatalf("a zero record count is damage on this replica and must say so, "+
			"or a retrying opener cannot tell it from a transient fault (err=%v)", err)
	}
}
