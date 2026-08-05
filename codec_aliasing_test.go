package commitlog

import (
	"bytes"
	"testing"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// DecompressInto puts the result in dst, for every codec.
//
// One did not. None returned src itself, so the "decompressed" bytes were the
// caller's compressed-payload buffer — which in the block path is a RECYCLED
// read buffer, refilled by the next block. Nothing was wrong with the bytes at
// the moment they were returned; they simply stopped being those bytes later.
//
// decodeBlock knew, and carried a pointer-identity check against exactly this —
// comparing &data[0] to &raw[0] and copying when they matched. That check is the
// tell: a contract that holds for three of four codecs is not a contract, it is
// something every caller has to rediscover, and the one that forgets gets a bug
// with no bad value anywhere in it.
//
// The copy is the same copy the caller was already making when it noticed. It
// just happens somewhere the caller cannot forget it.
func TestDecompressIntoNeverAliasesItsInput(t *testing.T) {
	payload := bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog "), 64)

	for _, c := range []compress.Codec{compress.None, compress.Snappy, compress.S2, compress.Zstd} {
		// A recycled scratch buffer, as the scan paths use.
		src := append([]byte(nil), c.Compress(payload)...)
		dst := make([]byte, 0, len(payload))

		got, err := c.DecompressInto(dst, src)
		require.NoErrorf(t, err, "codec %s", c)
		require.Equalf(t, payload, got, "codec %s: wrong bytes back", c)

		// Refill the source buffer, as the next block's read would.
		for i := range src {
			src[i] = 0xEE
		}
		require.Equalf(t, payload, got, "codec %s: the result aliased its input "+
			"and changed when the input buffer was reused", c)
	}
}
