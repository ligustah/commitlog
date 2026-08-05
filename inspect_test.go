package commitlog

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// buildInspectableLog writes a log and closes it, returning its dir and what it
// wrote, so a test can read the files back with nothing holding them open.
func buildInspectableLog(t *testing.T, codec compress.Codec, n int) (string, map[int64][]byte) {
	t.Helper()
	dir := tempDir(t)
	l, err := New(Options{
		Path:            dir,
		MaxSegmentBytes: 1 << 20, // one segment, so a test can name it
		Compact:         true,
		Compression:     codec,
	})
	require.NoError(t, err)
	cl := l.(*commitLog)

	want := map[int64][]byte{}
	var last int64
	for i := 0; i < n; i++ {
		val := []byte(fmt.Sprintf("inspect-%03d-payload-payload-payload", i))
		offs, aerr := cl.Append([]*Message{{
			Key: []byte(fmt.Sprintf("k:%03d", i)), Value: val,
		}})
		require.NoError(t, aerr)
		want[offs[0]] = val
		last = offs[0]
	}
	cl.SetHighWatermark(last)
	require.NoError(t, cl.Close())
	return dir, want
}

func onlyLogFile(t *testing.T, dir string) string {
	t.Helper()
	logs, err := filepath.Glob(filepath.Join(dir, "*.log"))
	require.NoError(t, err)
	require.Len(t, logs, 1, "fixture should produce exactly one segment")
	return logs[0]
}

// The whole point: a consumer can read every record out of a closed directory
// without opening the log, and gets back exactly what was written.
func TestInspectSegmentReadsEveryRecord(t *testing.T) {
	dir, want := buildInspectableLog(t, compress.None, 25)

	f, err := InspectSegment(onlyLogFile(t, dir))
	require.NoError(t, err)

	got := map[int64][]byte{}
	require.NoError(t, f.Records(func(r RecordInfo) error {
		require.True(t, r.CRCValid, "offset %d reported a bad CRC in an undamaged file", r.Offset)
		got[r.Offset] = append([]byte(nil), r.Value...)
		return nil
	}))
	require.Equal(t, want, got, "inspection disagreed with what was written")
}

// A damaged record is REPORTED, not refused. An inspector that errored here
// would be useless for the job it exists for — looking at a file precisely
// because something is suspected wrong with it.
func TestInspectSegmentReportsCorruptionRatherThanRefusing(t *testing.T) {
	dir, want := buildInspectableLog(t, compress.None, 25)
	path := onlyLogFile(t, dir)

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	marker := []byte("inspect-005-payload")
	idx := bytesIndex(raw, marker)
	require.GreaterOrEqual(t, idx, 0, "marker not found raw on disk")
	raw[idx+8] ^= 0xFF
	require.NoError(t, os.WriteFile(path, raw, 0666))

	f, err := InspectSegment(path)
	require.NoError(t, err)

	var bad, seen int
	require.NoError(t, f.Records(func(r RecordInfo) error {
		seen++
		if !r.CRCValid {
			bad++
		}
		return nil
	}))
	require.Equal(t, len(want), seen, "a damaged record must not truncate the walk")
	require.Equal(t, 1, bad, "exactly the damaged record should report an invalid CRC")
}

// Block-framed files decompress on the way through, and Blocks() describes the
// physical layout the other consumers were reverse-engineering by hand.
func TestInspectSegmentWalksBlocks(t *testing.T) {
	dir, want := buildInspectableLog(t, compress.Zstd, 200)
	path := onlyLogFile(t, dir)

	f, err := InspectSegment(path)
	require.NoError(t, err)
	// REQUIRED, not skipped-if-absent. A skip here would read as a pass forever
	// the day the fixture stopped producing blocks, and this test would be
	// guarding nothing — the exact shape of the vacuous tests this suite has
	// already produced.
	require.True(t, f.Blocked(), "fixture produced no block-framed segment, so this proves nothing")

	blocks, err := f.Blocks()
	require.NoError(t, err)
	require.NotEmpty(t, blocks)
	require.Zero(t, blocks[0].FileOffset, "the first block starts at byte 0")
	for _, b := range blocks {
		require.True(t, b.Codec.Valid(), "block at %d has an invalid codec", b.FileOffset)
	}

	got := map[int64][]byte{}
	require.NoError(t, f.Records(func(r RecordInfo) error {
		got[r.Offset] = append([]byte(nil), r.Value...)
		return nil
	}))
	require.Equal(t, want, got, "records read through blocks disagreed with what was written")
}

// A header this build cannot read must say which version it found and which it
// writes, and must say the SAME thing whichever direction the mismatch runs.
//
// It did not used to. At position 0 the error was wrapped with a sentence
// asserting the file predated v0.15.0 — inferred from the symptom, since that
// layout's codec byte lands where the version byte now is. A segment from a
// newer build produces the identical symptom, and got told the identical wrong
// story: the shape of error that sends someone looking for a writer that does
// not exist, which is what the sentence was added to prevent.
func TestInspectSegmentNamesBothBlockFormatVersions(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "00000000000000000000.log")

	// Version 9 — no build has ever written it, and it stands in for either
	// direction, since the parse compares rather than orders.
	hdr := []byte{blockMagic, 9, byte(compress.Zstd), 0, 0, 0x04, 0x00, 0, 0, 0x72, 0x23}
	require.NoError(t, os.WriteFile(path, append(hdr, make([]byte, 64)...), 0666))

	f, err := InspectSegment(path)
	require.NoError(t, err)
	require.True(t, f.Blocked(), "the magic byte should still identify it as block-framed")

	_, err = f.Blocks()
	require.Error(t, err)
	require.ErrorIs(t, err, ErrBlockFormat)
	require.Contains(t, err.Error(), "block format version 9", "the error must name what it found")
	require.Contains(t, err.Error(), fmt.Sprintf("this build writes %d", BlockFormatVersion),
		"the error must name what this build writes")
	require.NotContains(t, err.Error(), "predates",
		"the error must not guess at which older layout produced the mismatch")
}

func bytesIndex(hay, needle []byte) int {
	for i := 0; i+len(needle) <= len(hay); i++ {
		match := true
		for j := range needle {
			if hay[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}
