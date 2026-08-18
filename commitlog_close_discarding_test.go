package commitlog

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// discardableLog opens a log at path holding three records with a high
// watermark that is neither zero nor the default, so a reopen resuming from the
// checkpoint is distinguishable from one that did not find it.
func discardableLog(t *testing.T, path string) *commitLog {
	t.Helper()
	l, err := New(Options{Path: path, MaxSegmentBytes: 4096, DisableAutoClean: true})
	require.NoError(t, err)
	for i := range 3 {
		_, err := l.Append([]*Message{{Value: []byte("v" + strconv.Itoa(i))}})
		require.NoError(t, err)
	}
	l.SetHighWatermark(2)
	return l.(*commitLog)
}

// TestCloseDiscardingRefusesTheReopen is the whole point of the method, and the
// half a consumer asked for after reading the first draft: durable_streams
// reuses one t.TempDir() across subtests, so reopening a directory they closed
// discarding is their ORDINARY path. Documenting the hazard would have left it
// silent for exactly the caller who hits it most.
//
// Falsified by making CloseDiscarding call closeSegments: the reopen then
// succeeds and this fails at the ErrorIs. That mutation is the shape the method
// is most likely to decay into, since every other line of it matches Close.
func TestCloseDiscardingRefusesTheReopen(t *testing.T) {
	path := tempDir(t)
	l := discardableLog(t, path)
	require.NoError(t, l.CloseDiscarding())

	_, err := New(Options{Path: path, MaxSegmentBytes: 4096, DisableAutoClean: true})
	require.ErrorIs(t, err, ErrLogDiscarded)
	// The path is in the message, because a caller who reuses one directory
	// across subtests gets this error from a New they did not write.
	require.Contains(t, err.Error(), path)
}

// TestCloseKeepsTheReopen is the other side of the check above. Without it, a
// CloseDiscarding that poisoned the directory unconditionally — say, one whose
// marker write moved into a shared helper both closes call — passes every
// assertion above while breaking the ordinary close.
func TestCloseKeepsTheReopen(t *testing.T) {
	path := tempDir(t)
	l := discardableLog(t, path)
	require.NoError(t, l.Close())

	reopened, err := New(Options{Path: path, MaxSegmentBytes: 4096, DisableAutoClean: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	require.Equal(t, int64(2), reopened.HighWatermark(), "the checkpoint Close wrote")
}

// TestCloseDiscardingWritesTheMarkerNotAnOffset pins WHERE the refusal lives.
// The reopen test above passes for any hw file New cannot parse, including one
// left corrupt by accident, so on its own it does not show that the marker is
// what refuses.
func TestCloseDiscardingWritesTheMarkerNotAnOffset(t *testing.T) {
	path := tempDir(t)
	l := discardableLog(t, path)
	require.NoError(t, l.CloseDiscarding())

	b, err := os.ReadFile(filepath.Join(path, hwFileName))
	require.NoError(t, err)
	require.Equal(t, hwDiscardedMarker, string(b))
	_, parseErr := strconv.ParseInt(string(b), 10, 64)
	require.Error(t, parseErr, "the marker must not be readable as an offset")
}

// TestDiscardedIsDistinctFromDamage is the consumer's second requirement stated
// as a test: they need to tell "you discarded this" from "this is broken", and
// a sentinel is only distinct if something ELSE still lands on the generic
// error. Both halves are asserted, since a New that returned ErrLogDiscarded
// for every unparseable hw file would satisfy the first alone.
//
// The torn case is the one worth having. markDiscarded writes without a rename,
// so a crash mid-write can leave a prefix of the marker; matching the file
// WHOLE is what sends that to the parse error rather than reporting a
// deliberate discard the log never completed.
func TestDiscardedIsDistinctFromDamage(t *testing.T) {
	for _, tc := range []struct {
		name     string
		contents string
	}{
		{"garbage", "not a number at all"},
		{"torn marker", hwDiscardedMarker[:4]},
		{"empty", ""},
		{"marker with trailing newline", hwDiscardedMarker + "\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := tempDir(t)
			l := discardableLog(t, path)
			require.NoError(t, l.Close())
			require.NoError(t, os.WriteFile(filepath.Join(path, hwFileName), []byte(tc.contents), 0o666))

			_, err := New(Options{Path: path, MaxSegmentBytes: 4096, DisableAutoClean: true})
			require.Error(t, err)
			require.NotErrorIs(t, err, ErrLogDiscarded)
			require.Contains(t, err.Error(), "parse high watermark file failed")
		})
	}
}

// TestCloseDiscardingLeavesTheIndexUnshrunk is the only test here that can see
// the SEGMENT half of the method. Every assertion above is about the marker, so
// swapping segment.closeDiscarding() back to segment.close() — which is where
// the fsyncs this method exists to avoid actually live — passes all of them.
//
// Shrinking is the visible half of a durable index close: it truncates the
// mapping down to the entries in use, which the discarding close skips along
// with the flush. Measured rather than assumed, on a 3-record log: 60 bytes
// after Close (three 20-byte entries), the full 10MiB preallocation after
// CloseDiscarding. The assertion is stated as "much larger than the entries
// occupy" rather than as either literal, so it survives a change to the
// preallocation size or the entry width.
func TestCloseDiscardingLeavesTheIndexUnshrunk(t *testing.T) {
	sizeOfSoleIndex := func(t *testing.T, path string) int64 {
		t.Helper()
		matches, err := filepath.Glob(filepath.Join(path, "*"+indexFileSuffix))
		require.NoError(t, err)
		require.Len(t, matches, 1)
		fi, err := os.Stat(matches[0])
		require.NoError(t, err)
		return fi.Size()
	}

	durablePath := tempDir(t)
	require.NoError(t, discardableLog(t, durablePath).Close())
	shrunk := sizeOfSoleIndex(t, durablePath)
	require.Equal(t, int64(3*entryWidth), shrunk, "a durable close shrinks to the entries in use")

	discardedPath := tempDir(t)
	require.NoError(t, discardableLog(t, discardedPath).CloseDiscarding())
	require.Greater(t, sizeOfSoleIndex(t, discardedPath), shrunk*100,
		"a discarding close leaves the preallocated mapping alone")
}

// TestCloseDiscardingIsIdempotent covers the same ground closeSegments' own
// segmentsClosed check does. A second call must not report a failure, because
// a fixture's cleanup calling it after an explicit one is the obvious way to
// use it.
func TestCloseDiscardingIsIdempotent(t *testing.T) {
	path := tempDir(t)
	l := discardableLog(t, path)
	require.NoError(t, l.CloseDiscarding())
	require.NoError(t, l.CloseDiscarding())
	require.True(t, l.IsClosed())
}

// TestCloseDiscardingAfterCloseLeavesTheCheckpoint pins the order the
// idempotence above implies but does not show. discardSegments returns early on
// segmentsClosed, and the marker write sits INSIDE that guard — so a log
// already closed durably keeps the checkpoint Close wrote, and stays openable.
// Moving the marker write above the guard is a one-line change that would
// poison a directory whose contents were made durable on purpose.
func TestCloseDiscardingAfterCloseLeavesTheCheckpoint(t *testing.T) {
	path := tempDir(t)
	l := discardableLog(t, path)
	require.NoError(t, l.Close())
	require.NoError(t, l.CloseDiscarding())

	reopened, err := New(Options{Path: path, MaxSegmentBytes: 4096, DisableAutoClean: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	require.Equal(t, int64(2), reopened.HighWatermark())
}

// TestIsClosedIsTrueAfterEveryShutdown pins a doc claim rather than a defence.
// CommitLog.IsClosed used to say "whether Close has run", which was already
// loose about Delete and became loose about CloseDiscarding too; the corrected
// doc names all three, so it needs a test for the same reason the v0.93.2
// emptiness fix did — a doc naming specific behaviour invites an obvious
// cleanup that would make it false again.
//
// IsDeleted is asserted alongside because the two docs refer to each other:
// IsClosed says it does not distinguish, IsDeleted says it separates the last.
func TestIsClosedIsTrueAfterEveryShutdown(t *testing.T) {
	for _, tc := range []struct {
		name    string
		shut    func(l *commitLog) error
		deleted bool
	}{
		{"Close", func(l *commitLog) error { return l.Close() }, false},
		{"CloseDiscarding", func(l *commitLog) error { return l.CloseDiscarding() }, false},
		{"Delete", func(l *commitLog) error { return l.Delete() }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := discardableLog(t, tempDir(t))
			require.False(t, l.IsClosed(), "a live log")
			require.NoError(t, tc.shut(l))
			require.True(t, l.IsClosed())
			require.Equal(t, tc.deleted, l.IsDeleted())
		})
	}
}

// TestCloseDiscardingReleasesTheDirectory: the claim outlives the segments and
// is given back last on every close path. A CloseDiscarding that forgot the
// release would leave the name unopenable for the life of the process, which on
// this path is invisible — the reopen refuses either way, and refusing is what
// the test above asks for. Distinguished by opening a DIFFERENT log in the same
// directory after removing the marker.
func TestCloseDiscardingReleasesTheDirectory(t *testing.T) {
	path := tempDir(t)
	l := discardableLog(t, path)
	require.NoError(t, l.CloseDiscarding())
	require.NoError(t, os.Remove(filepath.Join(path, hwFileName)))

	reopened, err := New(Options{Path: path, MaxSegmentBytes: 4096, DisableAutoClean: true})
	require.NoError(t, err, "the directory lock must have been released")
	require.NoError(t, reopened.Close())
}
