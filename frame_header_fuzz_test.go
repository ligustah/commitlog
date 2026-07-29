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

// The frame header is NOT covered by any checksum, and this is what that costs.
//
// A record's CRC spans the message payload only. The 28 bytes in front of it —
// offset, timestamp, leader epoch, payload size — are unprotected, so a damaged
// header is not detectable the way a damaged value is. The reader has no way to
// know that the offset it is about to report was ever the offset that was
// written there.
//
// The other two corruption targets cannot reach this. FuzzCorruptedRecord damages
// bytes INSIDE the value, where the CRC catches it. FuzzTornLog changes the file's
// length, which desynchronises framing rather than lying within it. Neither
// rewrites a header field in place and leaves everything else plausible.
//
// The invariant is about agreement between a record's identity and its content:
//
//	a record served at offset N holds the value that was written at offset N,
//	or the read fails.
//
// A read is free to fail, panic-free, at any point. What it may not do is report
// a record under an offset that belonged to a different record, because a caller
// that resumes from a reported offset would then resume from the wrong place —
// and unlike a bad value, nothing downstream can detect it.
//
// This target is expected to find LESS than the other two, and that is the
// result: the size field is bounded by later reads, and the CRC catches the
// payload. If it ever fails, the finding is that an unchecksummed field reached a
// caller as truth.
// STATUS: this target FOUND the gap it was written for, and the gap is now
// closed. It failed on its first seed — a corrupted offset field was served as
// truth, observed as offsets 72057594037927939 and 71 in a log holding 0..15 —
// and readOne now cross-checks a record's offset against the range of the
// segment it was found in.
//
// What that check can and cannot do: it catches an offset outside the segment's
// range, not one swapped with another record INSIDE it. The header carries no
// checksum to make that detectable and adding one would change the format, so
// the remaining exposure is stated rather than implied.
func FuzzCorruptFrameHeaderIsNeverServedAsTruth(f *testing.F) {

	// Seeds: (which frame, which byte of its 28-byte header, xor mask).
	f.Add([]byte{3, 0, 0x01})  // offset field, low bit
	f.Add([]byte{3, 24, 0x01}) // size field, low bit
	f.Add([]byte{0, 8, 0xFF})  // timestamp
	f.Add([]byte{5, 16, 0x80}) // leader epoch
	f.Add([]byte{7, 7, 0x40})  // offset field, high byte
	f.Add([]byte{2, 26, 0x10}) // size field, high half

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 3 {
			t.Skip()
		}
		var (
			pick = int(data[0])
			at   = int(data[1]) % msgSetHeaderLen
			mask = data[2]
		)
		if mask == 0 {
			t.Skip()
		}

		dir := tempDir(t)
		opts := Options{
			Path:                 dir,
			MaxSegmentBytes:      1 << 20, // one segment, contiguous frames
			Compact:              true,
			DisableAutoClean:     true,
			HWCheckpointInterval: time.Hour,
			CleanerInterval:      time.Hour,
		}
		l, err := New(opts)
		require.NoError(t, err)
		cl := l.(*commitLog)

		const records = 16
		want := make(map[int64][]byte, records)
		var last int64
		for i := 0; i < records; i++ {
			val := []byte(fmt.Sprintf("frame-%02d-value", i))
			offs, aerr := cl.Append([]*Message{{
				Key: []byte(fmt.Sprintf("k:%02d", i)), Value: val,
			}})
			require.NoError(t, aerr)
			want[offs[0]] = val
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
		if len(raw) == 0 || raw[0] == blockMagic {
			t.Skip() // block-framed: headers are not at file offsets here
		}

		// Walk the frames to find where each header starts. Framing is
		// size-prefixed, so this is the only way to address the Nth header.
		var starts []int64
		for pos := int64(0); pos+msgSetHeaderLen <= int64(len(raw)); {
			starts = append(starts, pos)
			size := int64(messageSet(raw[pos:]).Size())
			if size < 0 {
				break
			}
			pos += msgSetHeaderLen + size
		}
		if len(starts) == 0 {
			t.Skip()
		}
		target := starts[pick%len(starts)]
		raw[target+int64(at)] ^= mask
		require.NoError(t, os.WriteFile(path, raw, 0666))

		l2, oerr := New(opts)
		if oerr != nil {
			return // refusing to open is a fine answer
		}
		cl2 := l2.(*commitLog)
		defer cl2.Close() // nolint: errcheck
		cl2.SetHighWatermark(last)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		oldest := cl2.OldestOffset()
		if oldest < 0 {
			return
		}
		r, nerr := cl2.NewReader(From(oldest), Uncommitted())
		if nerr != nil {
			return
		}

		// A panic is a failure in its own right — a damaged header must not take
		// the caller's process down, and the short-frame guard exists because one
		// did.
		var panicked any
		func() {
			defer func() { panicked = recover() }()
			hdr := make([]byte, HeaderBufferLen)
			for i := 0; i < records+8; i++ {
				msg, off, _, _, readErr := r.ReadMessage(ctx, hdr)
				if readErr != nil {
					return
				}
				expect, known := want[off]
				if !known {
					t.Errorf("served offset %d, which was never written", off)
					return
				}
				if string(msg.Value()) != string(expect) {
					t.Errorf(
						"offset %d served the value written at a DIFFERENT offset: got %q, want %q",
						off, msg.Value(), expect)
					return
				}
			}
		}()
		require.Nil(t, panicked, "a damaged frame header panicked instead of returning an error")
	})
}
