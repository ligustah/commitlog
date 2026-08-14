package commitlog

import (
	"fmt"
	"hash/crc32"
	"os"
	"testing"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// Both tests here make the SAME claim about two formats: that the version
// constant moved when the layout did.
//
// That is a different claim from "the version line is checked", which every
// format already had covered, and the difference is what #300 cost. TierObject
// gained a Records field and manifestVersion stayed at 3, so a v0.88.0 manifest
// decoded with Records absent — which JSON gives as zero — and a retention
// budget that sums record counts stopped reaching its ceiling at all. The check
// was there the whole time. The number was not.
//
// A test for the second claim must be FALSIFIED BY MOVING THE CONSTANT, and
// that rules out two fixtures that look like they would do:
//
//   - a version poked to an arbitrary wrong value (9, 0xFF) is refused whatever
//     the constant says, so it tests the first claim only
//   - a version written as `theVersion - 1` moves WITH the constant, so
//     neutralizing the constant changes the fixture too and nothing goes red
//
// Both had to be written literally. That is why these say `1` where a relative
// expression would read better, and why each carries a comment saying so —
// a later tidy-up that "removes the magic number" disarms the test silently.
//
// hack/formatversion.sh is why these exist as a set rather than one at a time:
// it enumerates the version constants that gate a refusal and requires each to
// be named by a guard, so a format added later cannot skip this quietly.
// BlockFormatVersion needs nothing here — TestAV1BlockHeaderIsRefusedRatherThan
// Misread already builds a literal 11-byte v1 header and is falsified correctly.

// A v1 block table is refused, and refused BY ITS VERSION.
//
// The assertion on the message is load-bearing, not decoration. A v1 entry is 9
// bytes against v2's 13, so a v1 table also fails decodeBlockTable's exact-length
// check — and both refusals are ErrBlockTableFormat, so require.ErrorIs cannot
// tell them apart. Naming the version error is what makes this fail when the
// constant stops moving: with blockTableVersion at 1 the fixture sails past the
// version check and dies on the length instead, which ErrorIs alone would accept.
func TestAV1BlockTableIsRefusedByItsVersion(t *testing.T) {
	// One block, laid out as v1 wrote it: logicalLen(4) + physLen(4) + codec(1),
	// with no record count. Hand-built on purpose — the point is a table this
	// build's encoder can no longer produce.
	const v1EntryLen = 9
	body := make([]byte, blockTableHeaderLen+v1EntryLen)
	body[0] = blockTableMagic
	body[1] = 1 // blockTableVersion as it was — a LITERAL, see the note above
	encoding.PutUint32(body[2:], 1)
	at := blockTableHeaderLen
	encoding.PutUint32(body[at:], 100)              // logicalLen
	encoding.PutUint32(body[at+4:], blockHeaderLen) // physLen
	body[at+8] = byte(compress.None)

	// A correct CRC over the v1 body, so nothing but the version or the length
	// can be what refuses this. A fixture that fails its own checksum would be
	// refused before either check and would prove nothing about them.
	v1 := append(body, 0, 0, 0, 0)
	encoding.PutUint32(v1[len(body):], crc32.ChecksumIEEE(body))
	require.Len(t, v1, blockTableHeaderLen+v1EntryLen+4,
		"this fixture is meant to be a one-block v1 table")

	_, err := decodeBlockTable(v1)
	require.ErrorIs(t, err, ErrBlockTableFormat, "a v1 block table was accepted")
	require.Contains(t, err.Error(), fmt.Sprintf("version 1, want %d", blockTableVersion),
		"the refusal must be the VERSION check. A v1 entry is 9 bytes against v2's "+
			"13, so this table fails the length check too, and both are "+
			"ErrBlockTableFormat — if the length is what caught it, the version "+
			"constant is free to stop moving and nothing here would notice")
}

// A digest sidecar from an older build is IGNORED, not misread.
//
// The digest is a cache, so a version mismatch is a soft failure by design:
// loadKeyDigest returns nil and the caller rebuilds. That is the right answer,
// and it is also why this had no test — nothing goes red when a soft path stops
// working. It gets slower, or worse, it starts succeeding on bytes it should
// not have understood, and a prefix read then answers from a map of a layout
// that is not the one on disk.
//
// Both directions are asserted. Without the positive control, a reader that
// rejected every digest would satisfy this — and "the digest is silently never
// loaded" is exactly the regression a best-effort cache invites.
func TestADigestFromAnOlderVersionIsIgnored(t *testing.T) {
	l, app := specLog(t)
	for i := 0; i < 6; i++ {
		app(&Message{Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("v")})
	}
	l.mu.RLock()
	seg := l.segments[0]
	l.mu.RUnlock()

	d, err := buildKeyDigest(seg, newBlockCache())
	require.NoError(t, err)
	require.NoError(t, writeKeyDigest(seg, d))

	require.NotNil(t, loadKeyDigest(seg),
		"the digest this build just wrote must load, or the negative below is "+
			"satisfied by a reader that rejects everything")

	// The version byte sits after the 4-byte magic, and is written as a LITERAL
	// 1 rather than digestVersion-1: a relative value moves with the constant,
	// so neutralizing the constant would move the fixture with it and this test
	// would pass either way. See the note at the top of this file.
	//
	// The trailing CRC is recomputed because the checksum check sits ABOVE the
	// version check in loadKeyDigest — leave it stale and the file is refused
	// before the version is ever read.
	raw, err := os.ReadFile(digestPath(seg))
	require.NoError(t, err)
	raw[4] = 1
	body := raw[:len(raw)-4]
	encoding.PutUint32(raw[len(raw)-4:], crc32.ChecksumIEEE(body))
	require.NoError(t, os.WriteFile(digestPath(seg), raw, 0666))

	require.Nil(t, loadKeyDigest(seg),
		"a digest written at version 1 was loaded. Its layout is not this "+
			"build's, so every offset read out of it lands somewhere the writer "+
			"did not put it")
}
