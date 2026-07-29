package commitlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A log torn off mid-write must lose records, never invent them.
//
// This is the crash shape: a process dies with a frame half on disk, so the file
// ends in the middle of a record. Recovery is entitled to DROP that tail — the
// records were never acknowledged — but a partial frame must not come back as
// data, and the records before it must be exactly what was written.
//
// Distinct from the other two corruption targets, and deliberately so. They
// damage bytes IN PLACE, leaving every length and offset intact so that only a
// checksum can notice. This changes the file's shape instead: the framing itself
// is now a lie, which is what exercises recovery rather than verification.
//
// The invariant:
//
//	what a torn log serves is a PREFIX of what was appended, record for record,
//	or the read fails. Never a partial record, never a reordered one, never a
//	record that was never written.
//
// A truncation anywhere is allowed, not just in the active segment: tearing a
// SEALED segment is a torn middle rather than a torn tail, which recovery does
// not promise to repair. Reads over it are free to fail — the invariant only
// forbids answering wrongly, and says nothing about answering at all.
func FuzzTornLogServesOnlyAPrefix(f *testing.F) {
	// Seeds: (which .log file, truncation point as a fraction of its length).
	f.Add([]byte{9, 200}) // last segment, most of the way through
	f.Add([]byte{9, 250}) // last segment, a sliver off the end
	f.Add([]byte{0, 128}) // first segment, halfway — a torn MIDDLE
	f.Add([]byte{4, 30})  // an early segment, cut short
	f.Add([]byte{9, 0})   // truncated to nothing

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 2 {
			t.Skip()
		}
		var (
			pick = int(data[0])
			frac = int(data[1])
		)

		dir := tempDir(t)
		opts := Options{
			Path:                 dir,
			MaxSegmentBytes:      256,
			Compact:              true,
			DisableAutoClean:     true,
			HWCheckpointInterval: time.Hour,
			CleanerInterval:      time.Hour,
		}
		l, err := New(opts)
		require.NoError(t, err)
		cl := l.(*commitLog)

		const records = 24
		// want[i] is the value appended at the i'th offset, so a served record
		// can be checked against its POSITION, not just its existence.
		want := make([][]byte, 0, records)
		var last int64
		for i := 0; i < records; i++ {
			val := []byte(fmt.Sprintf("torn-%03d-payload", i))
			offs, aerr := cl.Append([]*Message{{
				Key: []byte(fmt.Sprintf("k:%03d", i)), Value: val,
			}})
			require.NoError(t, aerr)
			want = append(want, val)
			last = offs[0]
		}
		cl.SetHighWatermark(last)
		require.NoError(t, cl.Close())

		logs, gerr := filepath.Glob(filepath.Join(dir, "*.log"))
		require.NoError(t, gerr)
		if len(logs) == 0 {
			t.Skip()
		}
		sort.Strings(logs)
		path := logs[pick%len(logs)]
		info, serr := os.Stat(path)
		require.NoError(t, serr)
		if info.Size() == 0 {
			t.Skip()
		}
		// Cut it short. frac/256 of the way through, so the fuzzer can land on
		// a frame boundary, inside a header, or inside a payload.
		cut := info.Size() * int64(frac) / 256
		require.NoError(t, os.Truncate(path, cut))

		// Reopening may legitimately fail on a badly mangled log; that is a
		// refusal, not a wrong answer.
		l2, oerr := New(opts)
		if oerr != nil {
			return
		}
		cl2 := l2.(*commitLog)
		defer cl2.Close() // nolint: errcheck
		cl2.SetHighWatermark(last)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		oldest := cl2.OldestOffset()
		if oldest < 0 {
			return // nothing survived, which is allowed
		}
		r, nerr := cl2.NewReader(From(oldest), Uncommitted())
		if nerr != nil {
			return
		}

		hdr := make([]byte, 28)
		for i := 0; i < records+8; i++ {
			msg, off, _, _, readErr := r.ReadMessage(ctx, hdr)
			if readErr != nil {
				return // stopping early is the expected outcome of a tear
			}
			require.GreaterOrEqual(t, off, int64(0), "served a negative offset")
			require.Less(t, off, int64(records),
				"served offset %d, but only %d records were ever appended", off, records)
			require.Equal(t, want[off], msg.Value(),
				"offset %d came back with bytes that were never written there", off)
		}
	})
}
