package commitlog

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// An index with no log bytes anywhere is removed at open.
//
// It is the residue of a segment whose .log was collected while its .index was
// not — a retention pass that died between the two removals. Left in place it is
// not inert: open() names segments by the base offset in the file name, so the
// next segment to reach that base offset finds a populated index sitting under
// the name it is about to write, describing records that no longer exist.
func TestAnIndexWithNoLogAndNoManifestEntryIsRemovedAtOpen(t *testing.T) {
	dir := tempDir(t)

	l, err := New(Options{Path: dir, MaxSegmentBytes: 1 << 16})
	require.NoError(t, err)
	offs, err := l.Append([]*Message{{Value: []byte("v")}})
	require.NoError(t, err)
	l.SetHighWatermark(offs[0])
	require.NoError(t, l.Close())

	orphan := filepath.Join(dir, fmt.Sprintf(fileFormat, 9999, indexFileSuffix))
	require.NoError(t, os.WriteFile(orphan, make([]byte, 64), 0o644))

	reopened, err := New(Options{Path: dir, MaxSegmentBytes: 1 << 16})
	require.NoError(t, err)
	t.Cleanup(func() { reopened.Close() })

	require.NoFileExists(t, orphan,
		"an index with neither a .log beside it nor a manifest entry survived "+
			"open, under the name a future segment at that base offset will use")
}

// An offloaded segment's index has no .log beside it and must NOT be removed.
//
// This is the whole reason the orphan check consults the manifest rather than
// the directory alone. Offloading drops the local log and, when no remote index
// cache is configured, deliberately keeps the local index — so "index with no
// log" is the NORMAL resting state of a tiered segment, not damage. Removing it
// costs a download of the index object on the next read of that segment, and
// the check that prevents it is one map lookup against the manifest read at the
// top of open().
func TestAnOffloadedSegmentsIndexSurvivesOpen(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	opts := Options{
		Path:             dir,
		MaxSegmentBytes:  1 << 12,
		SegmentStore:     store,
		DisableAutoClean: true,
	}
	l, err := New(opts)
	require.NoError(t, err)

	var last int64
	for i := 0; i < 400; i++ {
		offs, err := l.Append([]*Message{{Value: []byte("padding value for a segment")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)

	n, err := l.(*commitLog).OffloadBefore(last)
	require.NoError(t, err)
	require.Positive(t, n, "the fixture needs an offloaded segment")

	// The offloaded segments are exactly the ones whose .log is gone. Take the
	// state from the directory rather than from the count, so the assertion below
	// names files that really are in this position.
	stranded := strandedIndexes(t, dir)
	require.NotEmpty(t, stranded,
		"offloading should have left at least one index with no log beside it")

	require.NoError(t, l.Close())

	reopened, err := New(opts)
	require.NoError(t, err)
	t.Cleanup(func() { reopened.Close() })

	for _, p := range stranded {
		require.FileExists(t, p,
			"open removed an offloaded segment's local index as an orphan; the "+
				"manifest names that base offset")
	}
}

// strandedIndexes returns the .index files in dir with no .log beside them.
func strandedIndexes(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	logs := make(map[string]bool, len(entries))
	for _, e := range entries {
		if n := e.Name(); filepath.Ext(n) == logFileSuffix {
			logs[n[:len(n)-len(logFileSuffix)]] = true
		}
	}
	var out []string
	for _, e := range entries {
		n := e.Name()
		if filepath.Ext(n) != indexFileSuffix {
			continue
		}
		if stem := n[:len(n)-len(indexFileSuffix)]; !logs[stem] {
			out = append(out, filepath.Join(dir, n))
		}
	}
	return out
}
