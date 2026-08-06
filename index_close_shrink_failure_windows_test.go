//go:build windows

package commitlog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// A failed shrink during Close must not cost the handle.
//
// closeIndex is written around one rule, stated in its own comment: report the
// failure, but never before releasing the mapping AND the handle, because a
// mapped or open index file cannot be unlinked on Windows and the segment then
// becomes permanently undeletable. The rule was applied to the flush and to
// nothing else — the shrink after it returned early, so a refused SetEndOfFile
// left the index unmapped, the handle open and the index marked OPEN, which is
// the same wedge one step later.
//
// The refusal is the real one: a second view of the same file makes
// SetEndOfFile fail, which is the whole reason shrink unmaps first.
func TestAFailedShrinkOnCloseStillReleasesTheHandle(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "0.index")

	idx, err := newIndex(options{path: path, bytes: 4096, baseOffset: 0})
	require.NoError(t, err)
	idx.position = 0
	require.NoError(t, idx.writeEntries(indexEntries(4, 0)))

	blocker, err := os.OpenFile(path, os.O_RDWR, 0666)
	require.NoError(t, err)
	blockerMap, err := mmapFile(blocker)
	require.NoError(t, err)

	// Close flushes, unmaps, and then shrinks — and the shrink is what the
	// blocker refuses.
	closeErr := idx.Close()

	require.NoError(t, unmapFile(blockerMap))
	require.NoError(t, blocker.Close())

	// The fixture is only meaningful if the shrink actually failed. If Windows
	// ever stops refusing SetEndOfFile under a second view, this test proves
	// nothing and must say so rather than passing.
	require.Error(t, closeErr,
		"the second view did not block the shrink, so this test never "+
			"exercised the failure path it exists for")

	require.True(t, idx.closed, "the index must not be left marked open")
	require.NoError(t, os.Remove(path),
		"the index file must be removable after a failed close: the handle was "+
			"still open, so this segment could never be deleted")
}
