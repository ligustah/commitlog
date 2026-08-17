package commitlog

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ErrCorruptRecord's remedy list offers "skip the record and carry on" first.
// These two tests exist because that was true of most of the sentinel's
// producers and false for one of them, and a caller holding the error could not
// tell which it had.
//
// Found by sqlcdc asking the question directly — their walstat wanted the skip
// and stopped rather than risk spinning — one release after the remedy was
// written down. Nothing in the suite had ever called ReadMessage AGAIN after a
// refusal, so the affordance the docs promised was never exercised in either
// direction.

// buildTwentyRecordLog writes 20 records into a single segment, closes the log,
// and returns the directory, the options that reopen it, the last offset, and the
// path of the one .log file. Callers damage that file and reopen.
//
// One segment on purpose: both tests locate a frame by walking from position 0,
// which is only meaningful if every record is in the file they are walking.
func buildTwentyRecordLog(t *testing.T, marker string) (Options, int64, string) {
	t.Helper()
	dir := tempDir(t)
	opts := Options{Path: dir, MaxSegmentBytes: 1 << 20}

	l, err := New(opts)
	require.NoError(t, err)
	cl := l.(*commitLog)

	var last int64
	for i := 0; i < 20; i++ {
		value := fmt.Sprintf("payload-%03d-xxxxxxxx", i)
		if i == 5 && marker != "" {
			value = marker
		}
		offs, err := cl.Append([]*Message{{
			Key: []byte(fmt.Sprintf("k:%03d", i)), Value: []byte(value),
		}})
		require.NoError(t, err)
		last = offs[0]
	}
	cl.SetHighWatermark(last)
	require.NoError(t, cl.Close())

	logs, err := filepath.Glob(filepath.Join(dir, "*.log"))
	require.NoError(t, err)
	require.Len(t, logs, 1, "fixture assumes a single segment; frame walking below depends on it")
	return opts, last, logs[0]
}

// frameStart walks n frames from the start of the file and returns the byte
// offset where frame n begins. It asserts every header it steps over passes its
// own CRC, so a fixture that has stopped looking like a chain of raw frames
// fails here rather than silently damaging the wrong bytes.
func frameStart(t *testing.T, data []byte, n int) int {
	t.Helper()
	pos := 0
	for i := 0; i < n; i++ {
		require.LessOrEqual(t, pos+msgSetHeaderLen, len(data), "ran out of file walking to frame %d", n)
		hdr := messageSet(data[pos : pos+msgSetHeaderLen])
		require.Equal(t, storedHeaderCrc(hdr), headerCrc(hdr),
			"frame %d does not carry a valid header CRC; fixture is not a chain of raw frames", i)
		pos += msgSetHeaderLen + int(hdr.Size())
	}
	require.LessOrEqual(t, pos+msgSetHeaderLen, len(data), "frame %d starts past the end of the file", n)
	return pos
}

// A payload failure is skippable, and the sentinel now says so by NOT being the
// frame-header one.
//
// The existing corrupt-record test stops at the error, which is where the gap
// was: it proved the read refuses the record and never asked whether the caller
// can act on the remedy it is offered. This reads on and requires every
// remaining record.
func TestAPayloadFailureCanBeSkippedAndTheErrorSaysSo(t *testing.T) {
	const marker = "SEQUENTIAL-005-ZZZZ"
	opts, last, logPath := buildTwentyRecordLog(t, marker)

	data, err := os.ReadFile(logPath)
	require.NoError(t, err)
	idx := bytes.Index(data, []byte(marker))
	require.GreaterOrEqual(t, idx, 0, "the marker value was not found raw on disk")
	// Inside the payload, so the header still verifies and the failure is the
	// payload CRC — the whole frame is consumed before it is raised.
	data[idx+11] = 'Q'
	require.NoError(t, os.WriteFile(logPath, data, 0666))

	l2, err := New(opts)
	require.NoError(t, err)
	cl2 := l2.(*commitLog)
	defer cl2.Close() // nolint: errcheck
	cl2.SetHighWatermark(last)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	r, err := cl2.NewReader(From(0), Uncommitted())
	require.NoError(t, err)
	hdr := make([]byte, HeaderBufferLen)

	var served int
	var readErr error
	for i := 0; i < 20; i++ {
		if _, _, _, _, err := r.ReadMessage(ctx, hdr); err != nil {
			readErr = err
			break
		}
		served++
	}
	require.ErrorIs(t, readErr, ErrCorruptRecord)
	// The distinction under test: this is the skippable kind. If this line ever
	// flips, the assertions below are testing a cascade rather than a skip.
	require.NotErrorIs(t, readErr, ErrCorruptFrameHeader,
		"a payload CRC failure must not report itself as a frame-header failure")
	require.Equal(t, 5, served, "the records before the corrupt one must still be served")

	// Now the part that was never exercised: carry on. Records 6..19 are intact
	// and the read is positioned at frame 6, so all fourteen must come back in
	// order. A reader left mid-record cannot do this.
	var resumed []int64
	for i := 0; i < 14; i++ {
		_, off, _, _, err := r.ReadMessage(ctx, hdr)
		require.NoError(t, err, "skipping the corrupt record did not resume cleanly (got %d of 14)", i)
		resumed = append(resumed, off)
	}
	want := []int64{6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}
	require.Equal(t, want, resumed,
		"skip-and-carry-on must resume at the record after the corrupt one, with none missing or repeated")
}

// A frame-header failure is the one that must not be skipped, and it now says
// that too.
//
// It reports ErrCorruptFrameHeader while STILL matching ErrCorruptRecord, so an
// existing arm that only asks "is this damage" is unaffected — that is the whole
// reason the new sentinel wraps rather than replaces.
//
// The second half pins the reason the distinction is needed rather than just
// asserting the name: reading on lands mid-record and reports intact records as
// corrupt, so a tool that skips here over-counts damage instead of spinning.
func TestAFrameHeaderFailureIsDistinguishableAndMustNotBeSkipped(t *testing.T) {
	opts, last, logPath := buildTwentyRecordLog(t, "")

	data, err := os.ReadFile(logPath)
	require.NoError(t, err)
	pos := frameStart(t, data, 5)
	// Damage a field the header CRC covers, and do NOT recompute it. Flipping the
	// offset field leaves a header that is structurally fine and fails its
	// checksum — which is the real shape: nothing rewrites the CRC when a byte
	// rots.
	data[pos+offsetPos] ^= 0xFF
	forged := messageSet(data[pos : pos+msgSetHeaderLen])
	require.NotEqual(t, storedHeaderCrc(forged), headerCrc(forged),
		"the damaged header still passes its CRC, so this test would prove nothing")
	require.NoError(t, os.WriteFile(logPath, data, 0666))

	l2, err := New(opts)
	require.NoError(t, err)
	cl2 := l2.(*commitLog)
	defer cl2.Close() // nolint: errcheck
	cl2.SetHighWatermark(last)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	r, err := cl2.NewReader(From(0), Uncommitted())
	require.NoError(t, err)
	hdr := make([]byte, HeaderBufferLen)

	var served int
	var readErr error
	for i := 0; i < 20; i++ {
		if _, _, _, _, err := r.ReadMessage(ctx, hdr); err != nil {
			readErr = err
			break
		}
		served++
	}
	require.Equal(t, 5, served, "the records before the damaged header must still be served")
	require.ErrorIs(t, readErr, ErrCorruptFrameHeader)
	// Every arm written against the old sentinel keeps working. This is the
	// compatibility claim the new sentinel makes, and it is cheap to assert.
	require.ErrorIs(t, readErr, ErrCorruptRecord,
		"ErrCorruptFrameHeader must still satisfy errors.Is for ErrCorruptRecord")

	// Why it must not be skipped. The payload was never read and its length was
	// never verified, so the read is positioned msgSetHeaderLen bytes into a
	// record. The next read interprets payload bytes as a header, which fails its
	// CRC on data that is perfectly intact — damage in ONE record reported as a
	// run of them.
	_, _, _, _, nextErr := r.ReadMessage(ctx, hdr)
	require.ErrorIs(t, nextErr, ErrCorruptFrameHeader,
		"reading on from a frame-header failure should land mid-record, not on a clean frame; "+
			"if this resumes cleanly the sentinel is over-warning and the docs should say so")
}
