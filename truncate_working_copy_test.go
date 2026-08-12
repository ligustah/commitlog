package commitlog

import (
	"errors"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// A truncation that fails on the way to installing its replacement does not
// strand it.
//
// Truncate builds the replacement first, deliberately: until the first Delete
// every failure can be returned with the log exactly as it was found. But the
// deletes come next, and a failure THERE returned with the replacement built,
// open, and reachable by nothing — Replace had not run, so it is not in
// l.segments and the source does not link to it. Its handle and its index
// mapping were then held for the life of the process, with its .truncated files
// left in the directory; on Windows that is a directory that cannot be removed
// after a Close that reported success.
//
// Same shape as the abandoned compaction rewrite (see
// TestAFailedCompactionPassPublishesTheRewritesItInstalled), reached from the
// other side: there the segment was already installed and had to be PUBLISHED,
// here it is not installed yet and has to be DROPPED. Which one applies is
// decided by whether the rename has happened, and nothing else.
func TestAFailedTruncateDropsTheReplacementItBuilt(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{
		Name: "truncate-drop", Path: dir, MaxSegmentBytes: 64 << 20,
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
	// Three segments: the cut lands inside the first, so the second and third are
	// deleted whole and the first is rewritten.
	app(4)
	require.NoError(t, cl.split(cl.activeSegment()))
	app(4)
	require.NoError(t, cl.split(cl.activeSegment()))
	app(4)

	cl.mu.RLock()
	segments := append([]*segment(nil), cl.segments...)
	cl.mu.RUnlock()
	require.Len(t, segments, 3)

	// The sabotage: the first segment the delete loop reaches refuses to close,
	// so its Delete fails — after the replacement for segment 0 has been built
	// and before anything installs it.
	boom := errors.New("this segment will not close")
	victim := segments[1]
	victim.Lock()
	real := victim.backing
	victim.backing = refusingBacking{err: boom}
	victim.Unlock()
	defer real.Close() // nolint: errcheck

	require.ErrorIs(t, cl.Truncate(2), boom,
		"the fixture must fail the truncation, or it asserts nothing about a "+
			"failed one")

	strays, err := filepath.Glob(filepath.Join(dir, "*"+truncatedSuffix))
	require.NoError(t, err)
	require.Empty(t, strays,
		"the failed truncation left its replacement behind: it is open, mapped, "+
			"and nothing names it")
}
