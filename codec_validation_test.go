package commitlog

import (
	"testing"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// A codec the package does not know is refused when it arrives, not when the
// records written under it are read back.
//
// It used to be accepted all the way through. Compress has no error to return,
// so its default arm stored the batch raw; the block header recorded the unknown
// codec byte; and parseBlockHeader — the only place Valid() was ever called —
// refused it on the way back. The write path accepted precisely what the read
// path rejects.
//
// The reopen is the part worth keeping honest about. It is not that the segment
// reads back badly: the descriptor records the codec BY NAME, an unknown one
// renders as "codec(9)", and compress.Parse refuses that. So a log configured
// this way took appends, closed cleanly, and could never be opened again — the
// records were lost to a value that was wrong before the first one was written.
func TestAnUnknownCompressionCodecIsRefusedAtOpen(t *testing.T) {
	for _, bad := range []compress.Codec{4, 9, 200, 255} {
		dir := tempDir(t)
		l, err := New(Options{Path: dir, MaxSegmentBytes: 4096, Compression: bad})
		require.Errorf(t, err, "codec %d was accepted", byte(bad))
		require.Nil(t, l)
	}

	// The four real ones still open.
	for _, good := range []compress.Codec{compress.None, compress.Snappy, compress.S2, compress.Zstd} {
		dir := tempDir(t)
		l, err := New(Options{Path: dir, MaxSegmentBytes: 4096, Compression: good})
		require.NoErrorf(t, err, "codec %s was refused", good)
		require.NoError(t, l.Close())
	}
}
