//go:build windows

package commitlog

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// A shrink whose truncate fails leaves the index READABLE.
//
// shrink on Windows has to unmap before it can truncate, because an open
// MapViewOfFile makes SetEndOfFile fail. That leaves a window in which the
// index has no mapping, and the truncate is exactly what can fail inside it —
// so the error path ran with idx.mmap already nil and returned straight out,
// leaving no mapping behind a non-zero position.
//
// That state is permanent. Nothing re-opens a segment's index once the log is
// running, so every subsequent read of the segment answers "corrupt index
// file" for the life of the process, and seal DISCARDS this error by design on
// the premise that a failed shrink costs a rebuilt index tail rather than data.
// The premise held only while the mapping survived the failure. sqlcdc reported
// it from production: position=275700, closed=false, 28 views wedged.
//
// The truncate is made to fail the way it really fails, by holding a second
// view of the same file open — which is the documented Windows behaviour the
// whole unmap/remap dance exists for, not a simulated error.
func TestAFailedShrinkLeavesTheIndexReadable(t *testing.T) {
	dir := tempDir(t)
	path := dir + "/shrink_fail.idx"

	idx, err := newIndex(options{path: path})
	require.NoError(t, err)
	defer idx.Close()

	_, err = idx.InitializePosition()
	require.NoError(t, err)
	require.NoError(t, idx.writeEntries([]*entry{
		{Offset: 0, Size: 4}, {Offset: 1, Size: 4}, {Offset: 2, Size: 4},
	}))

	// A second, independent view of the same file. While this is open, no
	// SetEndOfFile on the file can succeed, so the truncate inside shrink is
	// certain to fail — after shrink has already unmapped its own view.
	blocker, err := os.OpenFile(path, os.O_RDWR, 0666)
	require.NoError(t, err)
	blockerMap, err := mmapFile(blocker)
	require.NoError(t, err)

	shrinkErr := idx.Shrink()

	require.NoError(t, unmapFile(blockerMap))
	require.NoError(t, blocker.Close())

	// The fixture is only meaningful if the truncate actually failed. If a
	// future Windows or Go release stops rejecting SetEndOfFile under a second
	// view, this test proves nothing and must be told so rather than passing.
	require.Error(t, shrinkErr,
		"the second view did not block the truncate, so this test never "+
			"exercised the failure path it exists for")

	// The claim. A failed shrink may leave the file un-shrunk; it may not leave
	// the index unreadable.
	require.NotNil(t, idx.mmap,
		"shrink failed with the index left unmapped: every later read of this "+
			"segment reports a corrupt index, for the life of the process")

	var got entry
	require.NoError(t, idx.ReadEntryAtFileOffset(&got, entryWidth),
		"the index is unreadable after a failed shrink")
	require.Equal(t, int64(1), got.Offset,
		"the index is readable but no longer holds what it held")
}
