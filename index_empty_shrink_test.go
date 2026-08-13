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

// Closing a log whose active segment is EMPTY, then reopening it.
//
// Read the name literally: this asserts the reopen works, and nothing more. It
// does NOT cover the empty-shrink defect above, and two attempts to make it do
// so both passed with that fix reverted — which is how the honest limit in the
// v0.38.1 changelog entry was established.
//
// The reason is worth keeping: a shrink on an empty index leaves a bad state
// only IN MEMORY, and closing the log discards it. On reopen, newIndex finds a
// zero-length file, re-allocates and maps it fresh, so the corruption never
// survives the close it would have to survive to be observed here. Reaching the
// defect needs the index to be USED after an empty shrink within one process,
// and seal() — its only caller — marks the segment sealed, after which nothing
// writes to it.
//
// So no production path to that defect has been demonstrated. The unit test
// above reaches it by calling Shrink then writeEntries directly, which is a
// sequence seal() does not produce. The fix stands as defence against a latent
// inconsistency, not as a proven live bug — stated here because the earlier
// version of this test claimed the coverage it did not have.
func TestReopenAfterSealingAnEmptyIndexIsUsable(t *testing.T) {
	dir := tempDir(t)
	opts := Options{Path: dir, MaxSegmentBytes: 256, Compact: true}

	cl, err := New(opts)
	require.NoError(t, err)
	l := cl.(*commitLog)

	// Fill exactly up to a roll, so the ACTIVE segment is left empty: its
	// index has no entries, and closing the log seals and shrinks it.
	var lastOff int64
	for i := 0; i < 200; i++ {
		offs, err := l.Append([]*Message{{
			Key: []byte("k"), Value: []byte("padding to force segment rolls"),
		}})
		require.NoError(t, err)
		l.SetHighWatermark(offs[0])
		lastOff = offs[0]
	}
	// Roll to a fresh segment and leave it untouched.
	segs := l.segmentsSnapshot()
	active := segs[len(segs)-1]
	require.NoError(t, l.split(active))
	segs = l.segmentsSnapshot()
	fresh := segs[len(segs)-1]
	require.NotEqual(t, active.BaseOffset, fresh.BaseOffset, "expected a fresh segment")
	require.Zero(t, fresh.Index.Position(), "the fresh active segment's index must be empty")

	require.NoError(t, l.Close()) // seals the empty segment: shrink on an empty index

	// Reopen and use it: this is where a mishandled empty shrink surfaces.
	l2, err := New(opts)
	require.NoError(t, err)
	defer l2.Close() // nolint: errcheck

	require.Equal(t, lastOff, l2.NewestOffset(), "reopened log lost its tail")

	// Reads must work, including probes past the end and by timestamp — the
	// shapes the reporter was exercising.
	r, err := l2.NewReader(From(l2.OldestOffset()), Uncommitted())
	require.NoError(t, err)
	require.NotEmpty(t, drainReader(t, r), "reopened log returned no records")

	_, err = l2.EarliestOffsetAfterTimestamp(0)
	require.NoError(t, err)
	_, err = l2.EarliestOffsetAfterTimestamp(timestamp() + 3600_000)
	require.NoError(t, err)

	// And it must still accept writes.
	offs, err := l2.Append([]*Message{{Key: []byte("after"), Value: []byte("reopen")}})
	require.NoError(t, err)
	l2.SetHighWatermark(offs[0])
	require.Equal(t, lastOff+1, offs[0])
}
