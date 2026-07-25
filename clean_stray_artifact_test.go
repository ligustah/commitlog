package commitlog

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// segFiles returns the sorted base names of every file in the log directory.
func segFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out
}

// Deleting a segment whose log backing is already closed must still remove both
// files. Closing the two halves is not independent: skipping the index close
// after a failed backing close leaves the index mapped, and a mapped file
// cannot be unlinked on Windows — so the segment becomes permanently
// undeletable and every later maintenance pass repeats the same failure.
func TestDeleteSegmentWithAlreadyClosedBacking(t *testing.T) {
	dir := tempDir(t)
	seg := createSegment(t, dir, 0, 1024)
	logPath, idxPath := seg.logPath(), seg.indexPath()

	// A maintenance pass running outside the log mutex already closed the log
	// file; the segment does not know it yet.
	require.NoError(t, seg.backing.Close())

	require.NoError(t, seg.Delete(), "deleting a half-closed segment must succeed")
	require.False(t, exists(logPath), "log file must be gone")
	require.False(t, exists(idxPath), "index file must be gone")
}

// A kill mid-compaction leaves the rewrite's working copy behind: the cleaner
// writes "<base>.log.cleaned"/"<base>.index.cleaned" and only then renames them
// over the source segment, so a process killed before the rename leaves both
// artifacts on disk. Reopening skips them (open() matches only ".log"), which
// is intended — they carry no committed data. The question this pins down is
// what the FIRST maintenance pass after that restart does with them: Cleaned()
// opens the segment's working copy with O_CREATE|O_APPEND, so a leftover file
// is REUSED rather than started empty, and the pass would append the rewrite
// after the dead pass's bytes and rename that over live data.
func TestCleanAfterInterruptedRewrite(t *testing.T) {
	dir := tempDir(t)
	l, _ := setupWithOptions(t, Options{
		Path:            dir,
		MaxSegmentBytes: 64, // roll constantly: every message a sealed segment
		Compact:         true,
	})

	// Two values for one key plus padding, so a compaction pass has something to
	// remove (the superseded value) and must rewrite the segment holding it.
	for _, m := range []*Message{
		{Key: []byte("k"), Value: []byte("v1")},
		{Key: []byte("k"), Value: []byte("v2")},
		{Key: []byte("pad"), Value: []byte("pad")},
	} {
		offs, err := l.Append([]*Message{m})
		require.NoError(t, err)
		l.SetHighWatermark(offs[0])
	}
	want := readAllMsgs(t, l)
	require.NoError(t, l.Close())

	// Simulate the kill: drop the working copies the interrupted rewrite would
	// have left next to the oldest sealed segment. Non-empty and not a valid
	// message frame, exactly like a partially-written rewrite.
	logs, err := filepath.Glob(filepath.Join(dir, "*"+logSuffix))
	require.NoError(t, err)
	require.NotEmpty(t, logs)
	sort.Strings(logs)
	stem := strings.TrimSuffix(filepath.Base(logs[0]), logSuffix)
	require.NoError(t, os.WriteFile(filepath.Join(dir, stem+logSuffix+cleanedSuffix),
		[]byte{0xEE, 0xEE, 0xEE, 0xEE}, 0666))
	require.NoError(t, os.WriteFile(filepath.Join(dir, stem+indexSuffix+cleanedSuffix),
		make([]byte, entryWidth), 0666))
	t.Logf("after simulated kill: %v", segFiles(t, dir))

	// Restart and run the first maintenance pass.
	l2, cleanup2 := setupWithOptions(t, Options{
		Path:            dir,
		MaxSegmentBytes: 64,
		Compact:         true,
	})
	defer cleanup2()

	err = l2.Clean()
	t.Logf("first clean after restart: err=%v", err)
	t.Logf("after first clean: %v", segFiles(t, dir))
	require.NoError(t, err, "the first maintenance pass after a kill must succeed")

	// Whatever the pass did, every committed record must still be readable.
	got := readAllMsgs(t, l2)
	for off, m := range want {
		if string(m.Key()) == "k" && string(m.Value()) == "v1" {
			continue // legitimately compactable: superseded by v2
		}
		require.Contains(t, got, off, "record at %s must survive the pass",
			strconv.FormatInt(off, 10))
		require.Equal(t, m.Value(), got[off].Value())
	}
}
