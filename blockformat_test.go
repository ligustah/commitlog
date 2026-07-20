package commitlog

import (
	"errors"
	"testing"

	"github.com/ligustah/commitlog/compress"
)

// A block header must carry a version, and a reader must REFUSE a version
// it does not write. A magic byte alone only answers "is this a block?" —
// not "is this a block I understand?", which is what a reader has to know
// before it starts applying data.
func TestBlockHeaderCarriesVersionAndRefusesOthers(t *testing.T) {
	hdr := encodeBlockHeader(compress.Zstd, 1234, 567)
	if got := hdr[1]; got != BlockFormatVersion {
		t.Fatalf("header version byte = %d, want %d", got, BlockFormatVersion)
	}
	codec, ulen, clen, err := parseBlockHeader(hdr)
	if err != nil {
		t.Fatalf("round-trip: %v", err)
	}
	if codec != compress.Zstd || ulen != 1234 || clen != 567 {
		t.Fatalf("round-trip mismatch: codec=%v ulen=%d clen=%d", codec, ulen, clen)
	}

	// A future version must be refused, not misread. Clean cutover: there
	// is deliberately no compatibility path.
	future := append([]byte(nil), hdr...)
	future[1] = BlockFormatVersion + 1
	if _, _, _, err := parseBlockHeader(future); !errors.Is(err, ErrBlockFormat) {
		t.Fatalf("newer block version accepted (err=%v) — a reader that guesses at an unknown layout corrupts state", err)
	}

	// A pre-version segment (version byte where the codec used to sit)
	// must also be refused rather than silently parsed as some codec.
	legacy := append([]byte(nil), hdr...)
	legacy[1] = byte(compress.None)
	if _, _, _, err := parseBlockHeader(legacy); !errors.Is(err, ErrBlockFormat) {
		t.Fatalf("pre-version segment accepted (err=%v)", err)
	}
}
