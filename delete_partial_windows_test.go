//go:build windows

package commitlog

import (
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// A Delete that fails must leave a log that still OPENS.
//
// os.RemoveAll records one error and carries on deleting the rest, so a single
// held file never stopped it removing the descriptor — and a directory with
// segments and no descriptor is a state readDescriptor refuses on every
// subsequent open, forever. The failure mode was not "the delete did not
// happen" but "the log can never be opened again", which no retry fixes.
// Reported by sqlcdc, which lost a view's name to it in a soak.
//
// The held file here is a sidecar rather than an index, because the log's own
// handles are gone by the time the removal runs and an index cannot be taken
// exclusively from outside while the log still holds it. What matters is only
// that ONE entry in the directory refuses to be deleted; which one it is is not
// the property.
func TestAFailedDeleteLeavesALogThatStillOpens(t *testing.T) {
	dir := tempDir(t)
	opts := Options{Name: "held", Path: dir, MaxSegmentBytes: 1 << 20}

	l, err := New(opts)
	require.NoError(t, err)
	_, err = l.Append([]*Message{{Key: []byte("k"), Value: []byte("v")}})
	require.NoError(t, err)
	require.NoError(t, l.PutSidecar("stubborn", []byte("held open")))

	h := openDenyAll(t, filepath.Join(dir, "stubborn"))
	err = l.Delete()
	require.Error(t, err, "a held entry must fail the delete, or this test proves nothing")

	// Released before reopening: the point is what Delete left behind, not
	// whether the handle is still there.
	require.NoError(t, syscall.CloseHandle(h))

	require.FileExists(t, filepath.Join(dir, descriptorFileName),
		"the descriptor is what says the log exists; a failed delete must not be "+
			"the thing that removes it")

	reopened, err := New(opts)
	require.NoError(t, err,
		"a delete that failed left a log that cannot be opened again — no retry fixes that")
	require.NoError(t, reopened.Delete(), "and the retry must now finish the job")
}
