package commitlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// A crash mid-append leaves the last thing written half on disk. That is the
// ordinary unclean shutdown, and what a log owes its owner afterwards is
// everything that was written BEFORE the torn write — above all, the sealed
// segments, which were complete and fsynced long before the machine died and
// have nothing to do with the record that was in flight.
//
// Raw segments answer that: the tail is a frame whose checksum fails, and the
// log stops there. A block-compressed segment is walked differently — the file
// is a chain of block headers, each giving the next one's position — and a
// truncated tail means the walk reads a header out of the missing bytes or
// arrives past the end. Both are errors, and an error opening ONE segment is an
// error opening the log: every sealed segment under it becomes unreachable
// because the newest block did not finish being written.
//
// Cut at many depths, because they fail differently: inside the final block's
// payload, inside its header, and at the boundary where the header is whole and
// the payload it promises is not.
func TestATornTailNeverCostsTheSealedSegments(t *testing.T) {
	for _, codec := range []compress.Codec{compress.None, compress.Snappy, compress.Zstd} {
		t.Run(fmt.Sprintf("codec=%d", codec), func(t *testing.T) {
			tornTailNeverCostsTheSealedSegments(t, codec)
		})
	}
}

func tornTailNeverCostsTheSealedSegments(t *testing.T, codec compress.Codec) {
	// One pristine log, built once and copied per cut, so every cut starts from
	// the same bytes and a cut that heals cannot flatter the next one.
	template := tempDir(t)
	l, err := New(Options{
		Path:            template,
		MaxSegmentBytes: 1024,
		Compression:     codec,
	})
	require.NoError(t, err)

	const records = 400
	for i := range records {
		offs, err := l.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("k:%04d", i)),
			Value: []byte(fmt.Sprintf("v:%08d:%s", i, strings.Repeat("x", 32))),
		}})
		require.NoError(t, err)
		require.EqualValues(t, i, offs[0], "offsets must be dense here")
		l.SetHighWatermark(offs[0])
	}
	require.NoError(t, l.Close())

	activeBase, activeLog, activeSize := activeSegment(t, template)
	require.Positive(t, activeBase,
		"every record landed in the first segment, so there is no sealed one to lose")
	t.Logf("active segment starts at %d, its log is %d bytes", activeBase, activeSize)

	// Cut depths chosen to land in different parts of the final block: a byte or
	// two off the payload, a slice deep enough to eat into the header, and
	// enough to remove the last block whole.
	cuts := []int64{1, 2, 3, 5, 9, 17, 33, 65}
	ctx := context.Background()
	headers := make([]byte, HeaderBufferLen)
	for _, cut := range cuts {
		if cut >= activeSize {
			continue
		}
		t.Run(fmt.Sprintf("cut=%d", cut), func(t *testing.T) {
			dir := tempDir(t)
			copyDir(t, template, dir)
			require.NoError(t, os.Truncate(
				filepath.Join(dir, filepath.Base(activeLog)), activeSize-cut))

			reopened, err := New(Options{
				Path:            dir,
				MaxSegmentBytes: 1024,
				Compression:     codec,
			})
			require.NoError(t, err,
				"a torn tail on the ACTIVE segment made the whole log unopenable, "+
					"taking every sealed segment under it with it")
			defer reopened.Close()

			rd, err := reopened.NewReader(From(reopened.OldestOffset()))
			require.NoError(t, err, "a recovered log cannot be read from its own "+
				"oldest offset %d (newest %d)",
				reopened.OldestOffset(), reopened.NewestOffset())

			// Everything below the active segment was complete before the crash
			// and must come back, in order, unaltered. Above it, anything from
			// nothing to all of it is lawful — that is the write that was in
			// flight.
			want := int64(0)
			for want < activeBase {
				msg, offset, _, _, err := rd.ReadMessage(ctx, headers)
				require.NoError(t, err,
					"the sealed segments stopped being readable at offset %d, "+
						"below the active segment's base %d", want, activeBase)
				require.Equal(t, want, offset, "a record from a sealed segment is missing")
				require.Equal(t,
					fmt.Sprintf("v:%08d:%s", offset, strings.Repeat("x", 32)),
					string(msg.Value()),
					"the record at offset %d came back altered", offset)
				want++
			}

			// The rest is the torn region. Whatever it serves must still be a
			// record that was written, at an increasing offset — a truncation
			// must not turn into a shorter record or a repeat.
			//
			// Any error ends it. A torn tail is the one place the log is
			// entitled to stop with something other than io.EOF — it has half a
			// record and no honest way to describe it — and raw segments do
			// exactly that, reporting the frame that will not resolve. What is
			// NOT lawful is serving something out of those bytes.
			tail := int64(0)
			var endedWith error
			prev := activeBase - 1
			for {
				msg, offset, _, _, err := rd.ReadMessage(ctx, headers)
				if err != nil {
					endedWith = err
					break
				}
				require.Greater(t, offset, prev, "the torn tail repeated an offset")
				prev = offset
				require.Equal(t,
					fmt.Sprintf("v:%08d:%s", offset, strings.Repeat("x", 32)),
					string(msg.Value()),
					"the torn tail served a record that was never written, at "+
						"offset %d", offset)
				tail++
			}
			t.Logf("cut %d byte(s): %d sealed record(s) intact, %d more from the "+
				"torn segment, ended with %v", cut, activeBase, tail, endedWith)

			// And it must be able to carry on. A log that reopens but cannot be
			// appended to is only half recovered.
			offs, err := reopened.Append([]*Message{{
				Key:   []byte("k:after"),
				Value: []byte("v:after"),
			}})
			require.NoError(t, err, "a recovered log refused to accept new records")
			require.Greater(t, offs[0], prev,
				"an append after recovery reused an offset the log had served")
			reopened.SetHighWatermark(offs[0])

			// Read the whole log AGAIN, now that something has been written to it.
			// This is the assertion that matters most, and the one an
			// append-and-stop test cannot make: if the torn bytes were left on
			// disk, the append lands after them (the handle is O_APPEND) and a
			// reader walks into the remnant, so the records that came back
			// perfectly a moment ago are all behind an error. The log is fine
			// until it is written to, and destroyed by the first write — which is
			// worse than refusing to open, because nothing marks the transition.
			rd2, err := reopened.NewReader(From(reopened.OldestOffset()))
			require.NoError(t, err, "the log stopped being readable after an append")
			for want := int64(0); want < activeBase; want++ {
				msg, offset, _, _, err := rd2.ReadMessage(ctx, headers)
				require.NoError(t, err,
					"an append after recovery cost the sealed segments: the read "+
						"failed at offset %d, below the active segment's base %d",
					want, activeBase)
				require.Equal(t, want, offset, "a sealed record went missing after the append")
				require.Equal(t,
					fmt.Sprintf("v:%08d:%s", offset, strings.Repeat("x", 32)),
					string(msg.Value()),
					"the record at offset %d came back altered after the append", offset)
			}
			// And the appended record must be reachable — it is the proof the
			// remnant is not merely skipped but gone, since a reader that stops
			// at the tear would never arrive at anything written past it.
			for {
				_, offset, _, _, err := rd2.ReadMessage(ctx, headers)
				require.NoError(t, err,
					"the read stopped at offset %d before reaching the record "+
						"appended after recovery at %d", prev, offs[0])
				if offset == offs[0] {
					break
				}
			}
		})
	}
}

// activeSegment reports the highest-numbered segment in a log directory: its
// base offset, its log file path, and that file's size.
func activeSegment(t *testing.T, dir string) (int64, string, int64) {
	t.Helper()
	logs, err := filepath.Glob(filepath.Join(dir, "*"+logFileSuffix))
	require.NoError(t, err)
	require.NotEmpty(t, logs)
	sort.Slice(logs, func(i, j int) bool {
		return baseOffsetOf(t, logs[i]) < baseOffsetOf(t, logs[j])
	})
	active := logs[len(logs)-1]
	fi, err := os.Stat(active)
	require.NoError(t, err)
	return baseOffsetOf(t, active), active, fi.Size()
}

func baseOffsetOf(t *testing.T, logPath string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(
		strings.TrimSuffix(filepath.Base(logPath), logFileSuffix), 10, 64)
	require.NoError(t, err)
	return n
}

// copyDir copies every regular file in src into dst, one level deep, which is
// all a log directory ever is.
func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	entries, err := os.ReadDir(src)
	require.NoError(t, err)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(src, e.Name()))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dst, e.Name()), b, 0o644))
	}
}
