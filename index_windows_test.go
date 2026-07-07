//go:build windows

package commitlog

import (
	"os"
	"testing"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// TestIndexSyncWindows verifies that index.Sync() does not return the
// "FlushFileBuffers: The handle is invalid" error that gommap.Sync produces
// on Windows (it passes a file-mapping handle to FlushFileBuffers instead
// of the file handle).
func TestIndexSyncWindows(t *testing.T) {
	dir := tempDir(t)
	idx, err := newIndex(options{path: dir + "/test.idx"})
	require.NoError(t, err)
	defer idx.Close()

	_, err = idx.InitializePosition()
	require.NoError(t, err)

	require.NoError(t, idx.Sync(), "Sync must not fail with FlushFileBuffers error on Windows")
}

// TestIndexCloseWindows verifies that index.Close() succeeds on Windows and
// shrinks the file to the number of written entries.  The sequence is:
//
//	sync → unmap → shrink (SetEndOfFile) → file.Close
//
// Previously this failed because (a) gommap.Sync called FlushFileBuffers
// with a file-mapping handle (ERROR_INVALID_HANDLE) and (b) SetEndOfFile
// failed with ERROR_USER_MAPPED_FILE while a MapViewOfFile was still open.
func TestIndexCloseWindows(t *testing.T) {
	dir := tempDir(t)
	idx, err := newIndex(options{path: dir + "/close_test.idx"})
	require.NoError(t, err)

	_, err = idx.InitializePosition()
	require.NoError(t, err)

	require.NoError(t, idx.writeEntries([]*entry{{Offset: 0, Size: 4}}))
	require.NoError(t, idx.Close(), "Close must succeed on Windows")

	// The file must be shrunk to exactly one entry.
	fi, err := os.Stat(dir + "/close_test.idx")
	require.NoError(t, err)
	require.Equal(t, int64(entryWidth), fi.Size(),
		"index file must be shrunk to the number of written entries on Close")
}

// TestIndexShrinkWhileMmappedWindows verifies that Shrink() (called during
// Seal) works on Windows even while a MapViewOfFile is still active.
// On Windows, SetEndOfFile fails with ERROR_USER_MAPPED_FILE if any view is
// open; our Windows implementation unmaps, truncates, then remaps.
func TestIndexShrinkWhileMmappedWindows(t *testing.T) {
	dir := tempDir(t)

	// Case 1: empty index — Shrink should truncate to 0 and leave mmap nil.
	idx, err := newIndex(options{path: dir + "/shrink_empty.idx"})
	require.NoError(t, err)
	defer idx.Close()

	_, err = idx.InitializePosition()
	require.NoError(t, err)
	require.Equal(t, int64(0), idx.position)

	require.NoError(t, idx.Shrink(),
		"Shrink on empty index must not return ERROR_USER_MAPPED_FILE")

	fi, err := idx.file.Stat()
	require.NoError(t, err)
	require.Equal(t, int64(0), fi.Size(),
		"empty index must be truncated to 0 bytes by Shrink")

	// After Shrink-to-zero, reads must return EOF without panicking on nil mmap.
	_, readErr := idx.ReadAt(make([]byte, entryWidth), 0)
	require.Error(t, readErr, "ReadAt on a zero-size index must return an error")

	// Case 2: non-empty index — Shrink should truncate to position bytes and remap.
	idx2, err := newIndex(options{path: dir + "/shrink_nonempty.idx"})
	require.NoError(t, err)
	defer idx2.Close()

	_, err = idx2.InitializePosition()
	require.NoError(t, err)

	require.NoError(t, idx2.writeEntries([]*entry{
		{Offset: 1, Size: 8},
		{Offset: 2, Size: 8},
	}))
	pos := idx2.position // 2 * entryWidth

	require.NoError(t, idx2.Shrink(),
		"Shrink on non-empty index must not fail on Windows")

	fi2, err := idx2.file.Stat()
	require.NoError(t, err)
	require.Equal(t, pos, fi2.Size(),
		"non-empty index must be truncated to exactly position bytes")

	// The remapped index must still be readable.
	var e entry
	require.NoError(t, idx2.ReadEntryAtLogOffset(&e, 0),
		"remapped index must be readable after Shrink")
	require.Equal(t, int64(1), e.Offset)
}

// TestIndexExpandWindows verifies that the index can grow past its initial
// allocation (triggering re-mmap) and that entries before and after the
// expansion boundary are all readable.  The unmapping of the old mmap must
// happen before the new mmap is created to prevent address-collision in
// gommap's handleMap on Windows.
func TestIndexExpandWindows(t *testing.T) {
	dir := tempDir(t)

	// bytes: entryWidth means room for exactly one entry; the second write forces expansion.
	idx, err := newIndex(options{path: dir + "/expand.idx", bytes: entryWidth})
	require.NoError(t, err)
	defer idx.Close()

	_, err = idx.InitializePosition()
	require.NoError(t, err)

	first := &entry{Offset: 10, Timestamp: 100, Position: 0, Size: 16}
	second := &entry{Offset: 11, Timestamp: 200, Position: 16, Size: 24}
	require.NoError(t, idx.writeEntries([]*entry{first, second}),
		"write spanning an mmap expansion must not panic or corrupt the index")

	var got entry
	require.NoError(t, idx.ReadEntryAtLogOffset(&got, 0))
	require.Equal(t, first.Offset, got.Offset)
	require.Equal(t, first.Size, got.Size)

	require.NoError(t, idx.ReadEntryAtLogOffset(&got, 1))
	require.Equal(t, second.Offset, got.Offset)
	require.Equal(t, second.Size, got.Size)
}

// TestSegmentCloseWindows verifies the full segment close path on Windows:
// log.Close() + index.Close() must both succeed, and the temp directory must
// be removable afterward (no open file handles or mmap views remaining).
func TestSegmentCloseWindows(t *testing.T) {
	dir := tempDir(t)

	s, err := newSegment(dir, 0, 512, true, "", compress.None)
	require.NoError(t, err)

	ms, entries, err := newMessageSetFromProto(0, 0,
		[]*Message{{Value: []byte("hello windows")}}, false)
	require.NoError(t, err)
	require.NoError(t, s.WriteMessageSet(ms, entries))

	require.NoError(t, s.Close(),
		"segment.Close() must succeed on Windows (previously crashed with FlushFileBuffers error)")

	// After Close, the directory must be deletable (no open handles or mappings).
	require.NoError(t, os.RemoveAll(dir),
		"temp directory must be removable after segment.Close() on Windows")
}
