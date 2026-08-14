package commitlog

import (
	"testing"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// Closing a segment whose index is already on stable storage must not fsync it
// again.
//
// closeSegment used to pass durable=true straight through, so every index was
// flushed on the way out regardless of whether this process had written to it.
// For a log that has been up long enough to roll segments that is one
// FlushFileBuffers per sealed segment, each of which flushes the DEVICE cache
// rather than just this file — measured at 6.3-6.6ms per segment, about 60% of
// a full open-and-close, and it bought nothing: seal() already flushed those
// bytes and nothing wrote to them afterwards.
//
// The counter is what makes the absence observable. A flush that is skipped and
// a flush that is performed leave byte-identical files behind, so without it
// there is no way to tell the two apart from outside — and a guard with nothing
// to go red against is not a guard.
func TestACleanIndexIsClosedWithoutAFlush(t *testing.T) {
	dir := tempDir(t)
	seg, err := newSegment(dir, 0, 1<<30, compress.Snappy)
	require.NoError(t, err)

	writeSet(t, seg, compressibleMsgs(10))

	seg.Lock()
	require.True(t, seg.dirtyIndex, "a written segment must be marked dirty")
	// seal is the ordinary way a segment's index reaches stable storage: it
	// happens on every roll. Do it the real way rather than by setting the flag,
	// so the test would notice seal quietly ceasing to flush.
	seg.seal()
	require.False(t, seg.dirtyIndex, "seal must record that the flush succeeded")

	before := seg.Index.flushes.Load()
	require.Greater(t, before, int64(0), "seal itself must have flushed")

	require.NoError(t, seg.closeSegment(true))
	seg.Unlock()

	require.Equal(t, before, seg.Index.flushes.Load(),
		"closing a segment whose index is already durable fsynced it again; that "+
			"is one device-cache flush per sealed segment at every shutdown, for "+
			"bytes that were already on disk")
}

// The other direction, so the skip cannot be widened into "never flush at
// close": a segment with unflushed index bytes MUST still be flushed on the way
// out. This is the active segment at every clean shutdown.
func TestADirtyIndexIsStillFlushedAtClose(t *testing.T) {
	dir := tempDir(t)
	seg, err := newSegment(dir, 0, 1<<30, compress.Snappy)
	require.NoError(t, err)

	writeSet(t, seg, compressibleMsgs(10))

	seg.Lock()
	defer seg.Unlock()
	require.True(t, seg.dirtyIndex)
	before := seg.Index.flushes.Load()

	require.NoError(t, seg.closeSegment(true))

	require.Greater(t, seg.Index.flushes.Load(), before,
		"a segment closed with an unflushed index must be flushed on the way out; "+
			"nothing repairs a short index on a sealed segment")
}

// And a caller about to unlink the files still flushes nothing, which is the
// case CloseDiscarding was added for.
func TestADiscardedCloseFlushesNothing(t *testing.T) {
	dir := tempDir(t)
	seg, err := newSegment(dir, 0, 1<<30, compress.Snappy)
	require.NoError(t, err)

	writeSet(t, seg, compressibleMsgs(10))

	seg.Lock()
	defer seg.Unlock()
	before := seg.Index.flushes.Load()

	require.NoError(t, seg.closeSegment(false))

	require.Equal(t, before, seg.Index.flushes.Load(),
		"durability work on bytes that are about to stop existing")
}
