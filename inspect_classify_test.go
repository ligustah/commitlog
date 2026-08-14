package commitlog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// The cheap answer and the expensive one must agree, for both framings.
//
// This is the assertion that makes ClassifySegment safe to adopt: a consumer
// replacing InspectSegment with it at boot is trusting that nothing about the
// answer changed except its cost. Both route through isBlockFramed so they
// cannot drift, and this is what would notice if someone gave one of them its
// own copy of the magic byte again.
func TestClassifySegmentAgreesWithTheFullInspector(t *testing.T) {
	for name, codec := range map[string]compress.Codec{
		"blocked": compress.Zstd,
		"flat":    compress.None,
	} {
		t.Run(name, func(t *testing.T) {
			dir, _ := buildInspectableLog(t, codec, 200)
			path := onlyLogFile(t, dir)

			full, err := InspectSegment(path)
			require.NoError(t, err)
			cheap, err := ClassifySegment(path)
			require.NoError(t, err)

			require.Equal(t, full.Format(), cheap,
				"the header-only classifier disagreed with the whole-file inspector")
			require.Equal(t, full.Blocked(), cheap.Blocked)
			require.True(t, cheap.Readable(),
				"this build wrote the fixture, so it must be able to read it")
		})
	}
}

// The fixture has to actually produce both framings, or the test above is
// comparing two agreeing answers about nothing.
//
// Stated separately rather than folded in, because "flat" is the case that
// silently stops being exercised: if compress.None ever started block-framing,
// both sides of the comparison would still agree and the flat path would go
// untested forever.
func TestTheClassifierFixtureProducesBothFramings(t *testing.T) {
	blockedDir, _ := buildInspectableLog(t, compress.Zstd, 200)
	blocked, err := ClassifySegment(onlyLogFile(t, blockedDir))
	require.NoError(t, err)
	require.True(t, blocked.Blocked, "the zstd fixture produced no block-framed segment")
	require.Equal(t, BlockFormatVersion, blocked.Version)

	flatDir, _ := buildInspectableLog(t, compress.None, 200)
	flat, err := ClassifySegment(onlyLogFile(t, flatDir))
	require.NoError(t, err)
	require.False(t, flat.Blocked, "the uncompressed fixture produced block framing")
}

// The body is not read, which is the entire reason this exists.
//
// Proven by making the body unreadable: a valid header followed by three bytes
// where a block claims 1024. Anything that parses the body fails here — Blocks
// does, and that is asserted so this cannot pass by the body being fine after
// all. ClassifySegment answers anyway, because it never looked.
func TestClassifySegmentDoesNotReadTheBody(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "00000000000000000000.log")

	// A header claiming 1024 payload bytes, then a body of three. Built by the
	// writer for the reason the sibling fixture gives: hand-laid bytes stop being
	// a WELL-FORMED header the moment the layout changes, and this one's whole
	// job is to be sound everywhere except the length it claims.
	hdr := encodeBlockHeader(compress.Zstd, 1024, 1024, 4)
	require.NoError(t, os.WriteFile(path, append(hdr, 1, 2, 3), 0666))

	got, err := ClassifySegment(path)
	require.NoError(t, err, "classification must not depend on the body being intact")
	require.True(t, got.Blocked)
	require.Equal(t, BlockFormatVersion, got.Version)
	require.True(t, got.Readable())

	// The other half of the proof.
	f, err := InspectSegment(path)
	require.NoError(t, err)
	_, err = f.Blocks()
	require.Error(t, err,
		"the fixture's body must be unparseable, or this proves nothing about the body")
}

// Blocks and Records must give the SAME verdict on the same bytes.
//
// They did not. A block header claiming more payload than the file holds was
// refused by Records and reported as a healthy block by Blocks, which added the
// claimed length to its cursor, stepped past the end, and let the loop condition
// end the walk with a nil error. Truncation — a short write, a partial upload, a
// download cut off mid-object — is the likeliest thing an inspector gets aimed
// at, and "this file is fine" is the one answer that sends the investigation
// somewhere else.
//
// Asserted as AGREEMENT rather than as "Blocks errors", because the defect was
// never that one call lacked a check. It was that the package shipped two
// readers of one format that disagreed, which is the exact failure the note at
// the top of inspect.go describes between repos.
func TestBlocksAndRecordsAgreeOnATruncatedPayload(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "00000000000000000000.log")

	// A block claiming 1024 payload bytes, with three bytes behind it. Built by
	// the writer rather than by hand: the fixture's point is a header that is
	// WELL FORMED and describes more than is there, so a hand-laid one stops
	// being that the moment the layout changes — it becomes a short header, and
	// the parse fails one check earlier than the one under test.
	hdr := encodeBlockHeader(compress.Zstd, 1024, 1024, 4)
	require.NoError(t, os.WriteFile(path, append(hdr, 1, 2, 3), 0666))

	f, err := InspectSegment(path)
	require.NoError(t, err)

	blocks, blocksErr := f.Blocks()
	recordsErr := f.Records(func(RecordInfo) error { return nil })

	require.Error(t, recordsErr, "the fixture must be unreadable, or this proves nothing")
	require.Error(t, blocksErr,
		"Blocks called a file sound that Records could not read: %d blocks, no error",
		len(blocks))
	require.Contains(t, blocksErr.Error(), "claims 1024 payload bytes",
		"the error must name what the header claimed")
	require.Contains(t, blocksErr.Error(), "file holds 3",
		"and what was actually there")

	// The blocks parsed before the bad one still come back — an inspector hands
	// over what it did read along with the reason it stopped.
	require.Len(t, blocks, 1, "the overrunning block is still described, then refused")
	require.Zero(t, blocks[0].FileOffset)
}

// An empty segment is a segment, not damage.
//
// A log that has just been created has one, and a boot probe that called it
// corrupt would refuse to start on a healthy directory.
func TestClassifySegmentOnAnEmptyFileIsFlat(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "00000000000000000000.log")
	require.NoError(t, os.WriteFile(path, nil, 0666))

	got, err := ClassifySegment(path)
	require.NoError(t, err, "an empty segment must not be an error")
	require.False(t, got.Blocked)
	require.True(t, got.Readable())
}

// A magic byte with no version byte behind it is refused, NOT reported as
// version 0.
//
// Version 0 is a value the caller cannot distinguish from a real version byte
// that happened to be 0, so answering with it would be answering a question the
// bytes never settled — the same laundering as a default arm that fires on a
// value nobody supplied. There is no version in this file, so the honest answer
// is that there is no answer.
func TestClassifySegmentRefusesAMagicWithNoVersion(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "00000000000000000000.log")
	require.NoError(t, os.WriteFile(path, []byte{blockMagic}, 0666))

	_, err := ClassifySegment(path)
	require.Error(t, err, "a truncated header must not be reported as version 0")
	require.Contains(t, err.Error(), "no version byte")

	// A one-byte file that is NOT the magic is just a flat segment as far as
	// framing goes; whatever else is wrong with it is not this call's to say.
	other := filepath.Join(dir, "00000000000000000001.log")
	require.NoError(t, os.WriteFile(other, []byte{0x42}, 0666))
	got, err := ClassifySegment(other)
	require.NoError(t, err)
	require.False(t, got.Blocked)
}

// An unrecognised version comes back, it does not error.
//
// This is what a consumer probing a foreign directory is asking for: it needs
// to learn that the format is one it cannot read, and which one it is. Refusing
// would withhold exactly the fact that makes the answer useful, and would push
// the caller back to reading the byte itself — which is the copy this API
// exists to retire.
func TestAnUnknownBlockVersionIsReportedNotRefused(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "00000000000000000000.log")
	hdr := []byte{blockMagic, 9, byte(compress.Zstd), 0, 0, 0x04, 0x00, 0, 0, 0x72, 0x23}
	require.NoError(t, os.WriteFile(path, append(hdr, make([]byte, 64)...), 0666))

	got, err := ClassifySegment(path)
	require.NoError(t, err, "an unreadable version is a fact to report, not an error")
	require.True(t, got.Blocked)
	require.Equal(t, byte(9), got.Version, "the caller needs to know WHICH version")
	require.False(t, got.Readable(),
		"this build writes %d, so version 9 must not claim to be readable",
		BlockFormatVersion)
}
