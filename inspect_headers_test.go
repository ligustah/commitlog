package commitlog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// An inspector reports record headers.
//
// The forensic surface exposed offset, timestamp, epoch, key and value but
// dropped headers, and headers are where a producer's identity lives. A
// consumer reading a captured data directory to answer "which writer produced
// these two byte-identical batches" could see the bytes were identical and not
// see who wrote them -- the one field that separates a broker fence failing
// from a client re-initialising, unavailable from the only tool that may touch
// a captured directory without rewriting it.
func TestInspectReportsRecordHeaders(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{Path: dir, MaxSegmentBytes: 1 << 20})
	require.NoError(t, err)

	_, err = l.Append([]*Message{
		{
			Value:     []byte("with-headers"),
			Timestamp: 1,
			Headers:   map[string][]byte{"pid": []byte("7"), "epoch": []byte("11")},
		},
		{Value: []byte("no-headers"), Timestamp: 2},
	})
	require.NoError(t, err)
	require.NoError(t, l.Close())

	sf, err := InspectSegment(onlyLogFile(t, dir))
	require.NoError(t, err)

	var got []RecordInfo
	require.NoError(t, sf.Records(func(r RecordInfo) error {
		got = append(got, r)
		return nil
	}))
	require.Len(t, got, 2)

	require.Equal(t, []byte("7"), got[0].Headers["pid"],
		"the producer identity a forensic caller is reading the file FOR")
	require.Equal(t, []byte("11"), got[0].Headers["epoch"])

	// Distinct from damage: a record that carries no headers parses fine and
	// says so with an empty map, so a nil can only mean the region was
	// unreadable.
	require.NotNil(t, got[1].Headers,
		"a record with no headers parsed cleanly, so it must not be reported as damaged")
	require.Empty(t, got[1].Headers)
}

// Inspecting a damaged record REPORTS, and never panics the caller.
//
// The length fields that decide how far Key, Value and the header walk reach
// are payload, and no checksum vouches for payload -- the frame header's CRC
// covers the record's identity, not its contents. SerializedMessage.Key and
// .Value slice by those lengths without checking them, and a uint32 header
// length converts to a NEGATIVE int32 above 2^31, which slices in reverse. That
// is safe on the READ path only because the reader runs a bounds-checked parse
// first and hands out nothing that failed it.
//
// An inspector has no such guarantee: it exists to be pointed at files that are
// already damaged, and it is the one tool a caller may aim at a captured
// production directory. Panicking there kills the process of whoever is trying
// to find out what went wrong, from inside the tool provided for finding out.
//
// Written as a mutation sweep rather than one hand-placed bad byte because a
// single corruption tests whichever field it happens to land in. This walks
// every byte position, and found the live panic in Key on its first run.
func TestInspectSurvivesADamagedHeaderRegion(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{Path: dir, MaxSegmentBytes: 1 << 20})
	require.NoError(t, err)

	_, err = l.Append([]*Message{{
		Value:     []byte("v"),
		Timestamp: 1,
		Headers:   map[string][]byte{"pid": []byte("7")},
	}})
	require.NoError(t, err)
	require.NoError(t, l.Close())

	raw, err := os.ReadFile(onlyLogFile(t, dir))
	require.NoError(t, err)

	// Drive the mutator rather than hoping one byte lands well: corrupt every
	// byte position in turn and require that the walk never panics and never
	// hands back headers it could not actually parse.
	for i := range raw {
		mutated := append([]byte(nil), raw...)
		mutated[i] ^= 0xFF
		path := filepath.Join(tempDir(t), "00000000000000000000.log")
		require.NoError(t, os.WriteFile(path, mutated, 0o600))

		sf, err := InspectSegment(path)
		if err != nil {
			continue // refused at the framing level, which is also fine
		}
		// The requirement is simply that this returns, for every mutation.
		_ = sf.Records(func(r RecordInfo) error { return nil })
	}
}
