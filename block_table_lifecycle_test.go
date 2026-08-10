package commitlog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

func sidecarCount(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	n := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == blocksSuffix {
			n++
		}
	}
	return n
}

// Offloading a segment takes its local block table with it.
//
// The attach drops the local .log and .index once the manifest naming the store
// objects is published, and the block table describes exactly those local bytes.
// Left behind it is not merely litter: it is a table for a file that no longer
// exists, sitting under the name a future segment at that base offset would use.
// The offloaded segment's table lives in the store under blocksKey.
func TestOffloadingASegmentRemovesItsLocalBlockTable(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	l, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  1 << 14,
		Compression:      compress.Snappy,
		Tiers:            oneTier(store),
		DisableAutoClean: true,
	})
	t.Cleanup(cleanup)

	var last int64
	for i := 0; i < 400; i++ {
		offs, err := l.Append([]*Message{{Value: []byte("padding value for the block")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)

	before := sidecarCount(t, dir)
	require.Positive(t, before,
		"sealed block-compressed segments should have persisted their tables")

	n, err := l.OffloadBefore(last)
	require.NoError(t, err)
	require.Positive(t, n, "a sealed segment must have offloaded")

	after := sidecarCount(t, dir)
	require.Less(t, after, before,
		"offloading left %d local block tables behind, describing log files it "+
			"had just deleted", after)
}

// Installing a rewrite drops the block table of the segment it replaces.
//
// Replace renames the rewrite's files over the source's, so the source's table
// ends up beside bytes it does not describe — and the reopen at the end of
// Replace runs initPositions, which would read it. The size check backing this
// up refuses a table accounting for a different number of bytes, but a rewrite
// that dropped nothing can land on the same size, and a table believed on that
// evidence maps logical offsets onto the wrong records. So the removal is
// explicit rather than left to the check.
func TestInstallingARewriteDropsTheReplacedBlockTable(t *testing.T) {
	dir := tempDir(t)

	source, err := newSegment(dir, 0, 1<<20, true, "", compress.Snappy)
	require.NoError(t, err)
	ms, entries, err := newMessageSetFromProto(0, 0,
		[]*Message{{Value: []byte("original")}}, false)
	require.NoError(t, err)
	require.NoError(t, source.WriteMessageSet(ms, entries))
	source.Seal()

	sidecar := localBlockTablePath(source)
	require.FileExists(t, sidecar, "sealing should have persisted the source's table")

	rewrite, err := newWorkingSegment(dir, 0, 1<<20, cleanedSuffix, compress.Snappy)
	require.NoError(t, err)
	rms, rentries, err := newMessageSetFromProto(0, 0,
		[]*Message{{Value: []byte("rewritten and a good deal longer")}}, false)
	require.NoError(t, err)
	require.NoError(t, rewrite.WriteMessageSet(rms, rentries))

	require.NoError(t, rewrite.Replace(source))

	require.NoFileExists(t, sidecar,
		"the replaced segment's block table survived the install, beside bytes "+
			"it does not describe")

	// The installed segment still reads, which is what the removal must not cost.
	var got entry
	require.NoError(t, rewrite.Index.ReadEntryAtFileOffset(&got, 0))
	require.Equal(t, int64(0), got.Offset)
}
