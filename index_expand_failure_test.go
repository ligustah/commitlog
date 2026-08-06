package commitlog

import (
	"os"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/tysonmote/gommap"
)

// An index whose remap fails during expansion stays COHERENT: size never claims
// more than the mapping covers.
//
// writeAt grows the file, unmaps, and maps again. It used to record the new
// size the moment the file grew — but the mapping is what the next write copies
// into, and both the unmap and the map can fail in between. size then described
// a file rather than a mapping, and since the only test for expansion is
// `offset+pSize >= idx.size`, the next write concluded the room was already
// there and sliced straight past the end of the mapping. That is a panic inside
// a library, which takes the caller's process with it — strictly worse than the
// wedge on the Windows shrink path, and this one is not Windows-only.
//
// The mapping is made to fail through the mmapFile var rather than by asking
// the OS for something it will refuse: the only reliable real refusal is a
// mapping big enough to be a hazard in a test.
func TestAFailedRemapLeavesTheIndexCoherent(t *testing.T) {
	dir := tempDir(t)

	// Small enough that a handful of entries forces an expansion.
	idx, err := newIndex(options{path: dir + "/expand_fail.idx", bytes: 4 * entryWidth})
	require.NoError(t, err)
	defer idx.Close()

	_, err = idx.InitializePosition()
	require.NoError(t, err)
	require.NoError(t, idx.writeEntries([]*entry{
		{Offset: 0, Size: 4}, {Offset: 1, Size: 4},
	}))

	real := mmapFile
	mmapFile = func(*os.File) (gommap.MMap, error) {
		return nil, errors.New("injected: the OS refused to map the file")
	}
	// Enough entries to run past the pre-allocated size and force the expansion
	// whose remap now fails.
	expandErr := idx.writeEntries([]*entry{
		{Offset: 2, Size: 4}, {Offset: 3, Size: 4}, {Offset: 4, Size: 4},
	})
	mmapFile = real

	// The fixture is only meaningful if the expansion was actually attempted.
	require.Error(t, expandErr,
		"the write never expanded the index, so this test never exercised the "+
			"failure path it exists for")

	// The claim. Before, this append asked idx.size, was told the room was
	// already there, skipped the expansion it needed, and sliced a mapping that
	// was never rebuilt — a panic raised inside a library, in the caller's
	// goroutine, for a transient refusal by the OS.
	require.NotPanics(t, func() {
		err = idx.writeEntries([]*entry{{Offset: 5, Size: 4}})
	}, "appending after a failed remap panicked inside the library")
	require.NoError(t, err, "the index did not recover on the next append")

	// The recovered index still holds what it held before the failure.
	var got entry
	require.NoError(t, idx.ReadEntryAtFileOffset(&got, entryWidth))
	require.Equal(t, int64(1), got.Offset)
}
