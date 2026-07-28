package commitlog

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Shrinking an EMPTY index must leave it writable and readable.
//
// On Windows shrink() must unmap before truncating, because an open view blocks
// SetEndOfFile. It then remaps — but only `if remap && idx.position > 0`. For an
// empty index that guard is false, so the mapping is left nil while idx.size
// still describes the pre-allocated file that no longer exists.
//
// Nothing fails at that point, which is what makes it dangerous:
//
//   - the next writeAt sees offset+len < the STALE size, so it does not expand
//     or remap;
//   - it then copies into idx.mmap[offset:], and slicing a NIL slice at [0:] is
//     perfectly legal Go — the copy silently writes NOTHING while position
//     still advances. The index is now lying about entries it does not hold.
//   - the first read of one of those entries passes the position guard and
//     indexes a nil mapping: "slice bounds out of range capacity 0".
//
// So the panic is the second symptom. The first is silent index corruption,
// and a reader that never reads those offsets would never learn of it.
//
// Reported by sqlcdc against v0.36.1: a second reader on a live log with
// SearchTimestamp probing high offsets, panicking in findEntry → index.ReadAt.
func TestIndexShrinkOnEmptyIndexKeepsItUsable(t *testing.T) {
	dir := tempDir(t)
	idx, err := newIndex(options{path: filepath.Join(dir, "00000000000000000000.index"), bytes: 1024})
	require.NoError(t, err)
	t.Cleanup(func() { idx.Close() }) // nolint: errcheck

	// A freshly created index is all zeros, so this is the real path a segment
	// takes: position resolves to 0 — the index is empty.
	_, err = idx.InitializePosition()
	require.NoError(t, err)
	require.Zero(t, idx.Position(), "a fresh index must be empty")

	require.NoError(t, idx.Shrink(), "shrinking an empty index must succeed")

	// Everything below is what a live segment does next.
	want := &entry{Offset: 0, Position: 4242, Size: 17, Timestamp: 987654321}
	require.NoError(t, idx.writeEntries([]*entry{want}),
		"an index that was shrunk while empty must still accept entries")

	require.Equal(t, int64(entryWidth), idx.Position(),
		"position must reflect the entry just written")

	var got entry
	require.NoError(t, idx.ReadEntryAtFileOffset(&got, 0),
		"reading back the entry must not fail")
	require.Equal(t, want.Position, got.Position,
		"the entry read back must be the one written — a silent no-op write reads back as zeros")
	require.Equal(t, want.Size, got.Size)
	require.Equal(t, want.Timestamp, got.Timestamp)
}

// The same shape through a segment: roll a segment whose index is empty, then
// keep using the log. This is the path the reporter actually hit.
func TestSegmentSealWithEmptyIndexStaysReadable(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 64,
		Compact:         true,
	})
	defer cleanup()

	for i := 0; i < 40; i++ {
		offs, err := l.Append([]*Message{{
			Key: []byte("k"), Value: []byte("some padding value here"),
		}})
		require.NoError(t, err)
		l.SetHighWatermark(offs[0])
	}

	// Probe every offset, including past the end — SearchTimestamp-style
	// probing of high offsets is how the reporter reached the bad index.
	for _, seg := range l.Segments() {
		for off := seg.BaseOffset; off <= seg.NextOffset()+2; off++ {
			_, _ = seg.findEntry(off) // must not panic; an error is fine
		}
	}
}
