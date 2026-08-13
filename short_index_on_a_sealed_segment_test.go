package commitlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// Nothing repairs a SHORT index on a SEALED segment, and a short index does not
// merely cost a scan — it makes the records it fails to describe unreadable.
//
// setupIndex takes lastOffset straight from the index's last entry, so a
// segment whose index stops short of its log reports a lastOffset lower than
// the log actually holds. The records past that point are in the file and the
// log will not serve them.
//
// The repair exists — reconcileIndexTail appends entries for exactly the frames
// past the last indexed one — but it runs on the ACTIVE segment (commitlog.go)
// and on adopted tiers (manifest.go), and on nothing else. setupIndex's own
// rebuild fires only on indexOvershootsLog, the OPPOSITE direction: an index
// that reaches PAST its log. An index BEHIND its log is documented right there
// as "ordinary ... and reconcileIndexTail fills it in", which is true of the
// active segment and of no other.
//
// This truncates the index by hand rather than crashing a process mid-roll,
// because the fixture has to be the state, not one route to it. A seal() whose
// flush failed, an mmap page that never reached the platter, and a torn write
// all leave the same file behind.
//
// The pristine read is the control and it is not decoration: without it a
// reader loop that stops early for its own reasons reports the same failure,
// and the count would be evidence of nothing.
func TestAShortIndexOnASealedSegmentHidesRecords(t *testing.T) {
	dir := tempDir(t)
	opts := Options{Path: dir, MaxSegmentBytes: 256, DisableAutoClean: true}

	l, err := New(opts)
	require.NoError(t, err)

	const records = 60
	for i := 0; i < records; i++ {
		_, err := l.Append([]*Message{{Value: []byte("record-" + strconv.Itoa(i))}})
		require.NoError(t, err)
	}
	require.Greater(t, len(l.(*commitLog).Segments()), 2,
		"the fixture needs a sealed segment that is not the last")
	newest := l.NewestOffset()
	require.NoError(t, l.Close())

	require.Equal(t, records, readEveryOffset(t, opts, newest),
		"the control: this log reads back whole, so the loop below stopping early "+
			"is about the damage and not about the loop")

	truncateFirstIndexByOneEntry(t, dir)

	require.Equal(t, records, readEveryOffset(t, opts, newest),
		"a sequential read from 0 lost records to ONE missing index entry in a "+
			"sealed segment; the bytes are still in the log file and no open "+
			"reconciles them, so the loss is permanent")
}

// truncateFirstIndexByOneEntry removes the last entry from the FIRST segment's
// index. That segment is sealed and it is not the last, which is the state
// nothing used to look at.
//
// By hand rather than by crashing a process mid-roll, because the fixture has to
// be the state and not one route to it: a seal() whose flush failed, an mmap page
// that never reached the platter, and a torn write all leave this file behind.
func truncateFirstIndexByOneEntry(t *testing.T, dir string) {
	t.Helper()
	idxPath := filepath.Join(dir, fmt.Sprintf(fileFormat, int64(0), indexFileSuffix))
	st, err := os.Stat(idxPath)
	require.NoError(t, err)
	require.Greater(t, st.Size(), int64(entryWidth),
		"the first segment must hold several entries, or truncating one empties it")
	require.NoError(t, os.Truncate(idxPath, st.Size()-entryWidth))
}

// readEveryOffset opens the log, reads from 0 until the reader stops, and
// returns how many messages came out.
func readEveryOffset(t *testing.T, opts Options, newest int64) int {
	t.Helper()
	l, err := New(opts)
	require.NoError(t, err)
	defer l.Close()

	l.SetHighWatermark(newest)
	r, err := l.NewReader(From(0), Uncommitted())
	require.NoError(t, err)
	headers := make([]byte, HeaderBufferLen)
	var n int
	for {
		_, off, _, _, err := r.ReadMessage(context.Background(), headers)
		if err != nil {
			return n
		}
		n++
		if off >= newest {
			return n
		}
	}
}
