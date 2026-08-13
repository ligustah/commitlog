package commitlog

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// A bufReader must never answer "end of data" to a question that is not about
// data running out.
//
// Prompted by durable_streams, who found that v0.41.0 and earlier let a
// cancellation reach them as io.EOF and, because two of their scan loops break
// on io.EOF as an ordinary end, returned PARTIAL results as complete answers.
// Their generalisation is what this test pins: an end-of-data signal doubling as
// a failure signal makes a consumer stop early and report success.
//
// v0.42.0 fixed the cancellation. This is the sweep of the rest of the package,
// and bufReader held the remaining cases. All three were unreachable today,
// which is exactly why they needed writing down rather than leaving: nothing can
// fail if the condition never arises, so the next person to make one reachable
// gets a silent truncation instead of a test failure.
func TestBufReaderNeverOriginatesEndOfData(t *testing.T) {
	// Unpositioned: no segment was ever given. That is a bug in this package,
	// not a drained log — and answering io.EOF would make a reader that was
	// never positioned indistinguishable from one that read everything.
	var b bufReader
	n, err := b.Read(make([]byte, 32))
	require.Zero(t, n)
	require.Error(t, err)
	require.NotErrorIs(t, err, io.EOF,
		"an unpositioned bufReader reported the log as fully read")
	require.ErrorIs(t, err, errBufReaderUnpositioned)
}

// The end-of-data that bufReader DOES report must be the backing's own, passed
// through unchanged — so a real drain still ends a read normally.
//
// The pairing matters: the test above would also pass if bufReader had stopped
// reporting io.EOF at all, which would hang every reader in the package at the
// end of a segment. This is the other half of the claim.
func TestBufReaderForwardsTheBackingsEndOfData(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t)})
	defer l.Close() // nolint: errcheck
	defer cleanup()

	offsets, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("v")}})
	require.NoError(t, err)
	l.SetHighWatermark(offsets[0])

	seg := l.segmentsSnapshot()[0]
	var b bufReader
	b.reset(seg, 0)

	// Read the whole segment, then keep reading past it.
	buf := make([]byte, seg.Position())
	n, err := b.Read(buf)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)

	// Past the end: this is a genuine end of data and must still say so, because
	// every read loop in this package advances segments on exactly this signal.
	_, err = b.Read(make([]byte, 32))
	require.ErrorIs(t, err, io.EOF,
		"reading past the end of a segment must still report io.EOF")
}
