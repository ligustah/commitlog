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

// A corrupted offset that lands on ANOTHER offset in the same segment is served,
// and this pins that.
//
// It asserts the behaviour it would rather not have, on purpose. The frame header
// carries no checksum: the CRC covers the message payload only, so nothing
// contradicts an offset field that has been changed to a plausible value.
// readOne can reject an offset outside the segment's range — that is the fix in
// 55a1d11 — and cannot reject one inside it.
//
// Found by FuzzCorruptFrameHeaderIsNeverServedAsTruth in four seconds of active
// fuzzing, after its seeds had all passed. The seeds happened to produce only
// out-of-range offsets, which flattered the fix; the fuzzer immediately produced
// the in-range case. The commit that shipped the fix already said it caught only
// the former, so the target had been left asserting more than the code delivers.
// This test is where that difference now lives, instead of in a comment nobody
// runs.
//
// WHAT WOULD CHANGE IT: a checksum over the 28-byte frame header. That is a
// format change — every existing segment would fail it — so it is a decision
// about compatibility, not a guard to add. If someone makes it, THIS TEST SHOULD
// FAIL, and that failure is the signal to delete it rather than to weaken it.
func TestFrameHeaderOffsetSwapWithinASegmentIsUndetectable(t *testing.T) {
	dir := tempDir(t)
	opts := Options{
		Path:                 dir,
		MaxSegmentBytes:      1 << 20, // one segment, so every offset is in range
		Compact:              true,
		DisableAutoClean:     true,
		HWCheckpointInterval: time.Hour,
		CleanerInterval:      time.Hour,
	}
	l, err := New(opts)
	require.NoError(t, err)
	cl := l.(*commitLog)

	const records = 16
	var last int64
	for i := 0; i < records; i++ {
		offs, aerr := cl.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("k:%02d", i)),
			Value: []byte(fmt.Sprintf("frame-%02d-value", i)),
		}})
		require.NoError(t, aerr)
		last = offs[0]
	}
	cl.SetHighWatermark(last)
	require.NoError(t, cl.Close())

	logs, gerr := filepath.Glob(filepath.Join(dir, "*.log"))
	require.NoError(t, gerr)
	require.Len(t, logs, 1)
	path := logs[0]
	raw, rerr := os.ReadFile(path)
	require.NoError(t, rerr)
	require.NotEqual(t, blockMagic, raw[0], "fixture must not be block-framed")

	// The first frame starts at byte 0. Rewrite the low byte of its offset
	// field from 0 to 7 — an offset that genuinely exists in this segment, which
	// is exactly why it cannot be caught.
	require.Zero(t, raw[offsetPos+7], "expected the first record to be at offset 0")
	raw[offsetPos+7] = 7
	require.NoError(t, os.WriteFile(path, raw, 0666))

	l2, err := New(opts)
	require.NoError(t, err)
	cl2 := l2.(*commitLog)
	defer cl2.Close() // nolint: errcheck
	cl2.SetHighWatermark(last)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := cl2.NewReader(From(0), Uncommitted())
	require.NoError(t, err)

	msg, off, _, _, rerr2 := r.ReadMessage(ctx, make([]byte, HeaderBufferLen))
	require.NoError(t, rerr2, "an in-range offset is not detectable, so the read succeeds")
	require.Equal(t, int64(7), off,
		"the fabricated offset is reported as fact — this is the limitation")
	require.Equal(t, "frame-00-value", string(msg.Value()),
		"and it carries record 0's value, under record 7's identity")
	require.True(t, msg.crcMatches(),
		"the payload checksum still passes: it protects the value, not the identity")
}
