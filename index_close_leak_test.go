package commitlog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// A failed flush must not cost the mapping. Returning early from Close on a
// sync error leaves the index mapped, the handle open and the index marked
// open — and a mapped file cannot be unlinked on Windows, so the segment
// becomes permanently undeletable and every later maintenance pass fails the
// same way. Losing the unflushed tail is recoverable; leaking the mapping is
// not.
func TestIndexCloseReleasesMappingWhenFlushFails(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "0.index")
	idx, err := newIndex(options{path: path, bytes: 4096, baseOffset: 0})
	require.NoError(t, err)
	idx.position = 0
	require.NoError(t, idx.writeEntries(indexEntries(4, 0)))

	// Make the flush fail the way a lost handle does: close the file out from
	// under the index, so syncMmap's file sync returns os.ErrClosed.
	require.NoError(t, idx.file.Close())

	err = idx.Close()
	require.Error(t, err, "Close must report the failed flush")

	// The point of the fix: despite that failure the file is releasable.
	// Checked as a bool, not require.Nil: a failure would otherwise dump the
	// entire mapping into the test output.
	require.True(t, idx.mmap == nil,
		"the mapping must be released even when the flush fails")
	require.True(t, idx.closed, "the index must not be left marked open")
	require.NoError(t, os.Remove(path),
		"the index file must be removable after a failed close")
}

// Close stays idempotent after a failed flush — a second call must not report
// the failure again or try to unmap twice.
func TestIndexCloseIdempotentAfterFailedFlush(t *testing.T) {
	dir := tempDir(t)
	idx, err := newIndex(options{
		path: filepath.Join(dir, "0.index"), bytes: 4096, baseOffset: 0,
	})
	require.NoError(t, err)
	idx.position = 0
	require.NoError(t, idx.writeEntries(indexEntries(4, 0)))
	require.NoError(t, idx.file.Close())

	require.Error(t, idx.Close())
	require.NoError(t, idx.Close(), "Close must stay idempotent")
}
