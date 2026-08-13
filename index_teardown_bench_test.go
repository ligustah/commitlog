package commitlog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Which of closeIndex's steps actually costs the 8ms per segment that
// BenchmarkCloseCleanLog measures?
//
// closeIndex(durable) does three things to an index this process opened and
// never wrote to — syncMmap (FlushViewOfFile over the mapping, then
// FlushFileBuffers on the handle), unmap, and shrink (SetEndOfFile) — and they
// have very different implications, so the total is not a number worth acting
// on:
//
//   - If the cost is the SHRINK, it is free to remove. A sealed index on disk is
//     already shrunk, so InitializePosition sets position to the content end and
//     the file is already exactly that size — the truncate is a no-op that still
//     pays a syscall. The position==size metric below reports 1 to prove that is
//     the case being measured and not a real truncate.
//   - If the cost is the FSYNC, it is not free at all. dirtyIndex starts true for
//     every segment opened from disk, deliberately: a predecessor that crashed
//     may have left index bytes in the page cache, and nothing repairs a SHORT
//     index on a SEALED segment (open() reconciles the active segment only, and
//     manifest.go the adopted ones). Removing that flush needs a way to know the
//     predecessor shut down cleanly.
//
// ORDER MATTERS, and getting it wrong is how the first version of this
// benchmark lied. Shrink on a still-mapped index unmaps, truncates and REMAPS —
// three syscalls, and it measured 1.98ms, indistinguishable from the fsync.
// closeIndex unmaps FIRST and shrinks after, so the mapping is already nil and
// shrink is the bare truncate. Each case below therefore runs the real
// sequence, and "unmap only" is the floor every other case is measured against
// rather than an idle baseline.
//
// Measured (windows, Core Ultra 9 285K, IDLE box, two runs):
//
//	unmap only               3.39 / 3.20 ms
//	unmap+truncate           2.87 / 2.65 ms      truncate: nothing, inside noise
//	sync+unmap               5.13 / 5.84 ms      fsync: about +2ms
//	durable (all three)      5.41 / 4.31 ms
//
// So the no-op truncate costs nothing worth chasing and the unmap — which is
// not optional, the handle cannot close under a live mapping — is the single
// largest step. On an idle box the fsync is the SMALLER of the two.
//
// It is still the one worth removing, and the reason is variance rather than
// the mean. The same benchmark on a box with a neighbour working the disk put
// every syncing case at 36-50ms and every non-syncing case at ~3ms:
// FlushFileBuffers flushes the DEVICE cache, so it pays for whatever else is
// dirty on the volume, while the unmap keeps paying only for this file. A
// shutdown that fsyncs once per segment therefore has a cost with no upper
// bound in the thing it is supposedly measuring, and that is what closing a
// 336-segment log under load runs into.
func BenchmarkIndexTeardownParts(b *testing.B) {
	// One modest log is enough: the question is per-file syscall cost, not
	// scaling. Sealed and shrunk, which is the state a reopened segment is in.
	opts := benchBlockLog(b, 4000, 1<<16)
	path := firstSealedIndexPath(b, opts.Path)

	// Named for what closeIndex would be doing, not for the primitives:
	// "durable" is the current path, "unmap only" is CloseDiscarding.
	for _, part := range []string{"durable (sync+unmap+truncate)", "sync+unmap", "unmap+truncate", "unmap only"} {
		b.Run(part, func(b *testing.B) {
			var samePos bool
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				idx, err := newIndex(options{path: path, baseOffset: 0})
				if err != nil {
					b.Fatal(err)
				}
				if _, err := idx.InitializePosition(); err != nil {
					b.Fatal(err)
				}
				// Proves the truncate under test is the no-op one.
				samePos = idx.position == idx.size
				b.StartTimer()

				if strings.HasPrefix(part, "durable") || part == "sync+unmap" {
					_ = idx.Sync()
				}
				// Unmap by hand, in closeIndex's order, so the truncate below
				// sees the same nil mapping the real path gives it.
				idx.mapMu.Lock()
				_ = unmapFile(idx.mmap)
				idx.mmap = nil
				idx.mapMu.Unlock()
				if strings.HasPrefix(part, "durable") || part == "unmap+truncate" {
					_ = idx.file.Truncate(idx.position)
				}

				b.StopTimer()
				// The mapping is already released above; this is the handle.
				if err := idx.file.Close(); err != nil {
					b.Fatal(err)
				}
				idx.closed = true
				b.StartTimer()
			}
			b.StopTimer()
			b.ReportMetric(boolMetric(samePos), "position==size")
		})
	}
}

func boolMetric(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

// firstSealedIndexPath returns the index of the log's FIRST segment, which is
// sealed and shrunk in a closed log — the active segment's index is neither, and
// would measure a real truncate instead of the no-op one under test.
func firstSealedIndexPath(b *testing.B, dir string) string {
	b.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(b, err)
	var names []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), indexFileSuffix) {
			names = append(names, e.Name())
		}
	}
	require.Greater(b, len(names), 1,
		"the fixture must have rolled, or the only index is the active one")
	// ReadDir sorts by filename and segment files are zero-padded base offsets,
	// so the first entry is the oldest segment.
	return filepath.Join(dir, names[0])
}
