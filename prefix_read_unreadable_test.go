package commitlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A damaged segment must be reported as damaged, whichever route reads it.
//
// This is the same claim TestKeyPrefixRefusesRecordsThatFailCRC makes about
// ErrCorruptRecord, for the other sentinel. The digest-planned KeyPrefix path
// used to return a wrapped io.EOF when the segment ended before an offset its
// own digest names, so:
//
//	sequential path: ErrSegmentUnreadable — "damaged, copy from a peer"
//	KeyPrefix path : io.EOF               — "the segment ended"
//
// The same bytes, on the same log. ErrSegmentUnreadable exists so a replica can
// tell damage apart from a failure worth retrying, and that advice is no less
// true when a digest happened to plan the read.
//
// io.EOF is the ordinary value here, which is what made this easy to miss.
// Every other scan loop in the package runs to the end of a segment, so EOF is
// how it stops; collectRun stops when it has collected the offsets the digest
// promised, so EOF means those records are not there.
//
// Damage is done UNDER A LIVE LOG on purpose, the way
// TestScannerCorruptHeader's fixture does it: damage to a closed log is met by
// open(), which never puts a damaged sealed segment in front of a read path.
func TestAPrefixReadReportsADamagedSegmentAsUnreadable(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{
		Path:            dir,
		MaxSegmentBytes: 256,
		Compact:         true,
		// The cleaner would meet the damage on its own timer and rewrite or
		// refuse the segment underneath the read this test is about.
		DisableAutoClean: true,
	})
	require.NoError(t, err)
	cl := l.(*commitLog)
	defer cl.Close() // nolint: errcheck

	var last int64
	for i := range 60 {
		offs, err := cl.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("want:%03d", i)),
			Value: []byte(fmt.Sprintf("payload-%03d-xxxxxxxx", i)),
		}})
		require.NoError(t, err)
		last = offs[0]
	}
	cl.SetHighWatermark(last)

	require.Greater(t, len(cl.segments), 3, "need a sealed segment to damage")
	victim := cl.segments[len(cl.segments)/2]

	// One pass, to persist the digests. Sealing does not write them — only the
	// compaction pass and the join do — and every key here is unique, so this
	// rewrites nothing and the segment it damages below is the one it just
	// described. Driven explicitly because DisableAutoClean is set, for the
	// reason given there.
	require.NoError(t, cl.Clean())

	// The digest has to be on disk and has to be the thing that plans the read.
	// Without it, prefix_source falls back to BUILDING one, which scans the
	// segment itself and returns ErrSegmentUnreadable from keydigest.go — the
	// assertion below would pass without collectRun having been reached at all.
	// That is why the message is checked too.
	digest := filepath.Join(dir, fmt.Sprintf(fileFormat, victim.BaseOffset, keysSuffix))
	require.FileExists(t, digest,
		"the sealed segment has no persisted digest, so this would not be a digest-planned read")

	// Truncate the victim's log so its tail records are past the end of the
	// file while its digest still names them. Length rather than content: a
	// flipped byte is the CRC test's fixture and lands on ErrCorruptRecord,
	// which is already covered. This is the other failure — the record is not
	// there at all — and it is the one that arrived as io.EOF.
	logPath := filepath.Join(dir, fmt.Sprintf(fileFormat, victim.BaseOffset, logFileSuffix))
	info, err := os.Stat(logPath)
	require.NoError(t, err)
	require.NoError(t, os.Truncate(logPath, info.Size()/2))

	// The victim's LAST record: comfortably inside the half just removed, and
	// named by the digest that survived.
	wantOffset := victim.NextOffset() - 1
	require.GreaterOrEqual(t, wantOffset, victim.BaseOffset, "victim segment is empty")
	key := fmt.Sprintf("want:%03d", wantOffset)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	r, err := cl.NewReader(KeyPrefix([]byte(key)), Until(last))
	require.NoError(t, err)

	msg, _, _, _, err := r.ReadMessage(ctx, make([]byte, HeaderBufferLen))
	require.Error(t, err,
		"a KeyPrefix read served offset %d from a file that no longer reaches it", wantOffset)
	require.Nil(t, msg, "a refused record must not also be handed to the caller")
	require.ErrorIs(t, err, ErrSegmentUnreadable,
		"the sequential route calls these bytes damaged; this route must agree")
	require.Contains(t, err.Error(), "prefix read",
		"the failure came from somewhere other than collectRun, so this test says "+
			"nothing about the prefix path's own error contract")
}

// The other arm of the same refusal: bytes that are THERE and do not parse,
// rather than bytes that are gone.
//
// Both arms were one `errors.Wrapf(err, ...)` before, so covering only the
// io.EOF one would leave the sentinel on a line nothing reaches — and this is
// the arm that already looked like an error, which is exactly why it is the
// easier one to leave carrying no sentinel.
//
// Damage is 0xA5 over a frame header, the fixture TestScannerCorruptHeader
// uses, and NOT over a payload: a flipped payload byte leaves the frame
// parseable and lands on ErrCorruptRecord, which
// TestKeyPrefixRefusesRecordsThatFailCRC already covers. What is under test
// here is the scan failing outright.
func TestAPrefixReadReportsAnUnparseableFrameAsUnreadable(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{
		Path:             dir,
		MaxSegmentBytes:  256,
		Compact:          true,
		DisableAutoClean: true,
	})
	require.NoError(t, err)
	cl := l.(*commitLog)
	defer cl.Close() // nolint: errcheck

	var last int64
	for i := range 60 {
		offs, err := cl.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("want:%03d", i)),
			Value: []byte(fmt.Sprintf("payload-%03d-xxxxxxxx", i)),
		}})
		require.NoError(t, err)
		last = offs[0]
	}
	cl.SetHighWatermark(last)

	require.Greater(t, len(cl.segments), 3, "need a sealed segment to damage")
	victim := cl.segments[len(cl.segments)/2]
	require.NoError(t, cl.Clean())

	digest := filepath.Join(dir, fmt.Sprintf(fileFormat, victim.BaseOffset, keysSuffix))
	require.FileExists(t, digest,
		"the sealed segment has no persisted digest, so this would not be a digest-planned read")

	// In place and leaving the file's length alone, so the index and the digest
	// both stay valid and the damage is invisible to everything but the scan.
	logPath := filepath.Join(dir, fmt.Sprintf(fileFormat, victim.BaseOffset, logFileSuffix))
	f, err := os.OpenFile(logPath, os.O_WRONLY, 0o644)
	require.NoError(t, err)
	garbage := make([]byte, 64)
	for i := range garbage {
		garbage[i] = 0xA5
	}
	_, err = f.WriteAt(garbage, 16)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// A record in the damaged stretch, named by the digest that survived: the
	// segment's SECOND, so the run starts at or before the garbage rather than
	// seeking past it.
	wantOffset := victim.BaseOffset + 1
	require.Greater(t, victim.NextOffset(), wantOffset, "victim segment too short")
	key := fmt.Sprintf("want:%03d", wantOffset)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	r, err := cl.NewReader(KeyPrefix([]byte(key)), Until(last))
	require.NoError(t, err)

	msg, _, _, _, err := r.ReadMessage(ctx, make([]byte, HeaderBufferLen))
	require.Error(t, err, "a KeyPrefix read served offset %d out of garbage", wantOffset)
	require.Nil(t, msg, "a refused record must not also be handed to the caller")
	require.ErrorIs(t, err, ErrSegmentUnreadable,
		"the sequential route calls these bytes damaged; this route must agree")
}
