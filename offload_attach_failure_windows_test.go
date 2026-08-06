//go:build windows

package commitlog

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// An offload whose LOCAL cleanup fails still leaves the segment offloaded.
//
// attachOffloadedLocked is the second half of an offload: the objects are in the
// store and the manifest naming them is already published, so all that is left
// is to drop the redundant local copies and swap the backing over to the object.
// It used to return early from each of those four steps.
//
// That return published a lie. The manifest said the segment's bytes were in the
// store; the in-memory segment had a closed local backing, no store, and was
// still in l.segments — so every read of it failed until a restart. The caller
// (OffloadBefore) aborts its pass on that error, so nothing put it right, and
// the abort itself made the returned count disagree with the manifest.
//
// Nothing below the point where the store backing is open can make staying local
// correct: the commit already happened, in the store. So the swap is
// unconditional and the cleanup error is reported alongside it.
//
// The failure is forced the way it really happens on Windows: a file held open
// without FILE_SHARE_DELETE cannot be removed.
func TestAFailedLocalCleanupStillLeavesTheSegmentOffloaded(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	l, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  64, // roll, so there is a sealed segment to offload
		SegmentStore:     store,
		DisableAutoClean: true,
	})
	t.Cleanup(cleanup)

	var last int64
	for i := 0; i < 24; i++ {
		offs, err := l.Append([]*Message{{Value: []byte("padding value")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)

	// The oldest sealed segment is the one OffloadBefore reaches first.
	l.mu.RLock()
	target := l.segments[0]
	l.mu.RUnlock()
	require.Less(t, target.LastOffset(), last, "the first segment must be sealed")

	// Hold its local log open, so the os.Remove inside the attach refuses after
	// the backing has already been closed.
	blocker, err := os.Open(target.logPath())
	require.NoError(t, err)

	n, offloadErr := l.OffloadBefore(last)

	require.NoError(t, blocker.Close())

	// The fixture is only meaningful if the removal actually failed.
	require.Error(t, offloadErr,
		"the open handle did not block the removal, so this test never "+
			"exercised the failure path it exists for")

	// The claim: the segment is offloaded, not stranded halfway.
	target.RLock()
	offloaded := target.store != nil
	target.RUnlock()
	require.True(t, offloaded,
		"the segment was left with a closed local backing and no store, against "+
			"a manifest entry that already named its objects")
	require.Equal(t, 1, n,
		"the returned count disagrees with the manifest, which names this segment")

	// And it reads, through the object rather than the file that could not go.
	r, err := l.NewReader(From(target.BaseOffset), Follow())
	require.NoError(t, err)
	msg, _, _, _, err := r.ReadMessage(context.Background(), make([]byte, HeaderBufferLen))
	require.NoError(t, err, "the offloaded segment is unreadable after a failed local cleanup")
	require.Equal(t, "padding value", string(msg.Value()))
}
