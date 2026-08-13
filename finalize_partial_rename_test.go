package commitlog

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// Finalize installs a trim with TWO renames, and stopping between them must not
// leave the first one standing.
//
// Publishing a working copy means clearing its suffix, and dropIfUnpublished
// reads that suffix to decide whether the copy still needs dropping. A Finalize
// that renamed the log and then failed on the index returns with the log at its
// FINAL name and the suffix still set — so the caller's defer concludes "not
// published", deletes by the suffixed paths, removes the index it can still see
// and leaves the log it cannot. An orphan .log at the trim's base offset with
// nothing beside it.
//
// Sibling of TestAFailedTruncateBeforeDropsTheTrimItBuilt, which blocks the LOG
// name and so fails on the first rename, where there is nothing to undo. This
// one blocks the INDEX name specifically to get past the first rename and stop
// on the second — the only ordering in which the rollback exists.
//
// Note what it does NOT assert: that the next open cannot cope. It can — the
// orphan overlaps the boundary segment that this failed call left in place, and
// resolveSegmentOverlaps drops the contained one. That is exactly why this is
// worth asserting directly. A failed operation whose cleanup depends on a later
// recovery pass noticing is the thing the suffix rule exists to avoid, and it
// would go on passing every test that only checks the log still reads.
func TestAHalfDoneFinalizeLeavesNoOrphanLog(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{
		Name: "finalize-partial", Path: dir, MaxSegmentBytes: 64 << 20,
		DisableAutoClean: true,
	})
	require.NoError(t, err)
	cl := l.(*commitLog)
	defer cl.Close() // nolint: errcheck

	app := func(n int) {
		for i := range n {
			_, err := cl.Append([]*Message{{Value: []byte(strconv.Itoa(i))}})
			require.NoError(t, err)
		}
	}
	// Segments based at 0, 4 and 8; the cut at 2 lands inside the first, so it
	// is the boundary and is trimmed to a new base of 2.
	app(4)
	require.NoError(t, cl.split(cl.activeSegment()))
	app(4)
	require.NoError(t, cl.split(cl.activeSegment()))
	app(4)

	cl.mu.RLock()
	segments := append([]*segment(nil), cl.segments...)
	cl.mu.RUnlock()
	require.Len(t, segments, 3)
	require.EqualValues(t, 3, segments[0].LastOffset(),
		"the cut must land INSIDE the first segment, or no trim is built")

	finalLog := filepath.Join(dir, fmt.Sprintf(fileFormat, int64(2), logSuffix))
	finalIdx := filepath.Join(dir, fmt.Sprintf(fileFormat, int64(2), indexSuffix))

	// Block ONLY the index name. The log rename then succeeds and the index
	// rename fails, which is the window under test.
	require.NoError(t, os.MkdirAll(finalIdx, 0755))

	require.Error(t, cl.TruncateBefore(2),
		"the fixture must fail the trim, or it asserts nothing about a failed one")

	strays, err := filepath.Glob(filepath.Join(dir, "*"+trimmedSuffix))
	require.NoError(t, err)
	require.Empty(t, strays, "the failed trim left its working copy behind")

	// The one this test exists for: the log must not be sitting at its final
	// name with no index beside it.
	_, err = os.Stat(finalLog)
	require.True(t, os.IsNotExist(err),
		"the log was renamed to its final name and left there: the trim's suffix "+
			"still said unpublished, so the disposal deleted the suffixed index "+
			"and could not see this file")
}
