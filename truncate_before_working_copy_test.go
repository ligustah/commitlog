package commitlog

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// TruncateBefore builds a working copy too, and it was the one path building one
// with no test that it disposes of it.
//
// The other four rewrite paths publish with Replace; this one publishes with
// Finalize, which renames the suffix off in place rather than over a source. The
// discriminator is the same either way — a segment still carrying its suffix has
// not been published — which is why segment.dropIfUnpublished covers this site
// as well, and why this test exists to say so.
//
// The sabotage is a DIRECTORY sitting on the name Finalize wants to rename onto.
// A rename onto a directory fails on every platform this runs on, it fails on
// the FIRST of Finalize's two renames so nothing has moved when it does, and it
// needs no fault injection into the segment itself — which matters here, because
// the boundary segment has to stay perfectly readable for the trim to be built
// at all. Sabotaging the segment would stop the trim from existing, and a test
// that never builds a working copy cannot show it being dropped.
func TestAFailedTruncateBeforeDropsTheTrimItBuilt(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{
		Name: "truncate-before-drop", Path: dir, MaxSegmentBytes: 64 << 20,
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
	// Three segments based at 0, 4 and 8. The cut at 2 lands inside the first, so
	// it is the boundary and gets trimmed to a new base of 2.
	app(4)
	require.NoError(t, cl.split(cl.activeSegment()))
	app(4)
	require.NoError(t, cl.split(cl.activeSegment()))
	app(4)

	cl.mu.RLock()
	segments := append([]*segment(nil), cl.segments...)
	cl.mu.RUnlock()
	require.Len(t, segments, 3)
	require.EqualValues(t, 0, segments[0].BaseOffset)
	require.EqualValues(t, 3, segments[0].LastOffset(),
		"the cut must land INSIDE the first segment, or no trim is built and "+
			"this test asserts nothing")

	// The name Finalize renames the trimmed log onto, occupied by a directory.
	blocked := filepath.Join(dir, fmt.Sprintf(fileFormat, int64(2), logSuffix))
	require.NoError(t, os.MkdirAll(blocked, 0755))

	err = cl.TruncateBefore(2)
	require.Error(t, err,
		"the fixture must fail the trim, or it asserts nothing about a failed one")

	strays, err := filepath.Glob(filepath.Join(dir, "*"+trimmedSuffix))
	require.NoError(t, err)
	require.Empty(t, strays,
		"the failed truncate-before left its trim behind: it is open, mapped, "+
			"and nothing names it")
}
