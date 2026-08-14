package commitlog

import (
	"testing"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// seal must not clear dirtyIndex on a flush that failed.
//
// dirtyIndex means "the index bytes are on stable storage". seal used to set it
// false immediately after an Index.Sync() whose error is deliberately discarded,
// so a failed flush left a segment ASSERTING durability it did not have. Nothing
// caught that, because closeSegment fsyncs every index again unconditionally on
// the way out and re-did the work the lie would have skipped.
//
// That cover is exactly what the shutdown-cost work removes: once close honours
// this mark, a single failed seal is the difference between a sealed segment
// whose index is durable and one whose index is permanently short — and nothing
// repairs a short index on a SEALED segment, since open() reconciles the active
// segment alone (commitlog.go) and tier adoption (manifest.go). So the mark has
// to be true before anything is allowed to trust it.
//
// The failure is induced by closing the real index rather than substituting a
// double for it: a fake would have to be installed in place of *index, and every
// path here is written against the concrete type. Closing it makes Sync fail for
// the same reason a disk error would — the flush did not happen.
func TestSealKeepsTheDirtyMarkWhenTheFlushFails(t *testing.T) {
	dir := tempDir(t)
	seg, err := newSegment(dir, 0, 1<<30, compress.Snappy)
	require.NoError(t, err)
	t.Cleanup(func() { seg.Close() })

	writeSet(t, seg, compressibleMsgs(10))
	seg.Lock()
	defer seg.Unlock()
	require.True(t, seg.dirtyIndex,
		"the fixture must leave the index dirty or the assertion below is vacuous")

	// Break the real thing: after this, Sync returns ErrSegmentClosed.
	require.NoError(t, seg.Index.Close())
	require.Error(t, seg.Index.Sync(), "the fixture must make the flush fail")

	seg.seal()

	require.True(t, seg.dirtyIndex,
		"seal cleared the dirty mark after a flush that failed; a caller trusting "+
			"it would skip the fsync that keeps this sealed segment's index whole")
	require.True(t, seg.sealed, "the segment must still be sealed either way")
}

// The ordinary path still clears it, or the mark would be useless in the other
// direction — every close would fsync every index forever and the mark would
// never be able to say "already durable".
func TestSealClearsTheDirtyMarkWhenTheFlushSucceeds(t *testing.T) {
	dir := tempDir(t)
	seg, err := newSegment(dir, 0, 1<<30, compress.Snappy)
	require.NoError(t, err)
	t.Cleanup(func() { seg.Close() })

	writeSet(t, seg, compressibleMsgs(10))
	seg.Lock()
	defer seg.Unlock()
	require.True(t, seg.dirtyIndex)

	seg.seal()

	require.False(t, seg.dirtyIndex,
		"a successful seal must record that the index reached stable storage")
}
