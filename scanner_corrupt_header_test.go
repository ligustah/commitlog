package commitlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Damage in one segment must not kill the process.
//
// The scan reads a frame header and sizes its payload allocation from the
// header's size field. That field is covered by the header's CRC, and the read
// path has checked it since the CRC existed — but the SCAN did not, so a size
// read out of damaged bytes went straight into make(), where a length out of
// range is not an error but a panic. Unrecoverable in the caller, fatal to every
// unrelated log in the same binary, and raised from routine maintenance rather
// than from a read anyone is waiting on.
//
// Which is the same failure family as a torn tail costing the sealed segments,
// one floor up: there, damage in one segment cost the log; here it cost the
// process.
//
// The paths that reach it are the ones that walk a segment in order to REWRITE
// it — the two truncations and compaction — because an ordinary read resolves
// through the index and stops at the damaged frame with a CRC error, correctly.
func TestDamageInOneSegmentDoesNotKillTheProcess(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(l *commitLog, minOffset int64) error
		// What the call is not entitled to remove. Each of these three removes
		// records on purpose, so "nothing disappeared" asks the wrong question;
		// what they must not do is drop a record on the side of the boundary
		// they were told to keep, or drop anything at all on a pass that failed
		// and therefore installed nothing.
		mustKeep func(offset, minOffset int64, err error) bool
	}{
		{
			"TruncateBefore",
			func(l *commitLog, minOffset int64) error { return l.TruncateBefore(minOffset) },
			func(offset, minOffset int64, _ error) bool { return offset >= minOffset },
		},
		{
			"Truncate",
			func(l *commitLog, minOffset int64) error { return l.Truncate(minOffset) },
			func(offset, minOffset int64, _ error) bool { return offset < minOffset },
		},
		{
			"Clean",
			func(l *commitLog, _ int64) error { return l.Clean() },
			func(_, _ int64, err error) bool { return err != nil },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := tempDir(t)
			l, cleanup := setupWithOptions(t, Options{
				Path:            dir,
				MaxSegmentBytes: 1024,
				Compact:         true,
			})
			defer cleanup()
			for i := range 200 {
				offs, err := l.Append([]*Message{{
					Key:   []byte(fmt.Sprintf("k:%d", i%8)),
					Value: []byte(fmt.Sprintf("v:%08d:%s", i, strings.Repeat("x", 32))),
				}})
				require.NoError(t, err)
				l.SetHighWatermark(offs[0])
			}

			// A sealed segment in the middle: one the truncations will have to
			// REWRITE rather than delete whole, with segments on both sides of
			// it, and not the one still being written to.
			require.Greater(t, len(l.segments), 3, "need a sealed segment to damage")
			victim := l.segments[len(l.segments)/2]
			// The boundary lands inside the victim, so both truncations rewrite
			// exactly the segment that is about to stop being readable. Left to
			// the middle of the log by offset, whether they did would be luck.
			minOffset := victim.BaseOffset + 2
			require.Greater(t, victim.NextOffset(), minOffset, "victim segment too short")

			// In place, under a log that is already open, and leaving the file's
			// length alone. That detail is load-bearing: damage on a CLOSED log is
			// caught when it opens, which never gets a damaged sealed segment in
			// front of the rewrite paths. Corrupting underneath a live process is
			// what does.
			path := filepath.Join(dir, fmt.Sprintf(fileFormat, victim.BaseOffset, logFileSuffix))
			f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
			require.NoError(t, err)
			garbage := make([]byte, 32)
			for i := range garbage {
				garbage[i] = 0xA5
			}
			_, err = f.WriteAt(garbage, 200)
			require.NoError(t, err)
			require.NoError(t, f.Close())

			// What is readable BEFORE maintenance runs. Maintenance is entitled
			// to remove records deliberately, and this test asks about none of
			// that — only that it does not take away, as a side effect of
			// meeting damage, something it was still serving a moment ago.
			before := readableOffsets(t, l)
			require.NotEmpty(t, before)

			// Whatever this returns, it must RETURN. An error is a fine answer —
			// the log says it cannot read something and the caller decides. A
			// panic is not an answer at all.
			err = tc.run(l, minOffset)
			t.Logf("%s over a damaged segment returned: %v", tc.name, err)

			// And it must be an error. All three of these walk the damaged
			// segment in order to replace it, and a walk that gives up early
			// looks exactly like one that reached the end — so reporting success
			// here means having quietly dropped everything past the damage and
			// deleted the file that still held it. Saying so is the whole
			// difference between damage and loss.
			require.ErrorIs(t, err, ErrSegmentUnreadable,
				"%s met unreadable bytes and called it done", tc.name)

			// A rewrite that walks a segment to copy it forward and stops early
			// installs a copy missing everything past the damage, then deletes
			// the original. That is how a scan error becomes data loss: the pass
			// reports success, and records that were being served a moment
			// earlier are gone with the file that held them. Whether a record
			// survives is not the scanner's judgement to make.
			lost := before
			for offset := range readableOffsets(t, l) {
				delete(lost, offset)
			}
			for offset := range lost {
				if !tc.mustKeep(offset, minOffset, err) {
					delete(lost, offset)
				}
			}
			require.Empty(t, lost,
				"%d record(s) readable before %s were gone after it, without it "+
					"saying so: %v", len(lost), tc.name, sortedKeys(lost))

			// And the log is still usable afterwards, rather than left in a state
			// where the next call is the one that dies.
			_, err = l.Append([]*Message{{Key: []byte("k:after"), Value: []byte("v:after")}})
			require.NoError(t, err, "the log refused writes after meeting the damage")
		})
	}
}

// readableOffsets returns every offset the log will actually serve from a
// sequential read, stopping at the first thing it will not. Uncommitted so the
// high watermark plays no part, and errors are the end of the walk rather than a
// failure — meeting the damage is the point.
func readableOffsets(t *testing.T, l *commitLog) map[int64]struct{} {
	t.Helper()
	oldest := l.OldestOffset()
	if oldest < 0 {
		return map[int64]struct{}{}
	}
	r, err := l.NewReader(From(oldest), Uncommitted())
	require.NoError(t, err)
	var (
		out  = map[int64]struct{}{}
		hdr  = make([]byte, HeaderBufferLen)
		ctx  = context.Background()
		seen = 0
	)
	for seen < 10_000 {
		_, offset, _, _, err := r.ReadMessage(ctx, hdr)
		if err != nil {
			break
		}
		out[offset] = struct{}{}
		seen++
	}
	return out
}

func sortedKeys(m map[int64]struct{}) []int64 {
	out := make([]int64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
