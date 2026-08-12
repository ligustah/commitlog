//go:build windows

package commitlog

import (
	"os"
	"testing"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// A Replace that fails partway leaves its SOURCE readable.
//
// Replace closes both segments, renames the rewrite over the source, and brings
// the result back up — and it records the link that redirects readers from the
// source to the replacement only at the very end. A failure in between used to
// return with the source closed and unlinked.
//
// That is worse than an aborted pass. The caller publishes nothing on the way
// out, so l.segments is never swapped and the closed segment stays LIVE, where
// current() hands it to readers as usable because there is no link telling it
// otherwise. Every read of that segment then fails with ErrSegmentClosed for the
// life of the process — verbatim the symptom current() exists to eliminate.
//
// The rename is made to fail the way it really fails on Windows: a file held
// open without FILE_SHARE_DELETE cannot be renamed over.
func TestAFailedReplaceLeavesTheSourceReadable(t *testing.T) {
	dir := tempDir(t)

	source, err := newSegment(dir, 0, 1<<20, true, "", compress.None)
	require.NoError(t, err)
	ms, entries, err := newMessageSetFromProto(0, 0,
		[]*Message{{Value: []byte("original")}})
	require.NoError(t, err)
	require.NoError(t, source.WriteMessageSet(ms, entries))

	rewrite, err := newWorkingSegment(dir, 0, 1<<20, cleanedSuffix, compress.None)
	require.NoError(t, err)
	rms, rentries, err := newMessageSetFromProto(0, 0,
		[]*Message{{Value: []byte("rewritten")}})
	require.NoError(t, err)
	require.NoError(t, rewrite.WriteMessageSet(rms, rentries))

	// Hold the rename's TARGET open, so the rename inside Replace fails after it
	// has already closed the source.
	blocker, err := os.Open(source.logPath())
	require.NoError(t, err)

	replaceErr := rewrite.Replace(source)

	require.NoError(t, blocker.Close())

	// The fixture is only meaningful if the rename actually failed.
	require.Error(t, replaceErr,
		"the open handle did not block the rename, so this test never "+
			"exercised the failure path it exists for")

	// The claim. Nothing was published, so this segment is still the live one.
	require.False(t, source.closed,
		"the source segment was left closed: it is still in the published list, "+
			"so every read of it reports ErrSegmentClosed for the life of the process")

	var got entry
	require.NoError(t, source.Index.ReadEntryAtFileOffset(&got, 0),
		"the source segment's index is unreadable after a failed replace")
	require.Equal(t, int64(0), got.Offset,
		"the source segment is readable but no longer holds what it held")
}
