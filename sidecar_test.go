package commitlog

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// A sidecar name is an ACTION, not a description of one.
//
// The name reaches os.Remove and an atomic write with nothing between it and
// the disk but filepath.Join, which CLEANS a traversal rather than refusing it.
// So "../../x" named a real file outside the log directory and RemoveSidecar
// deleted it; "00000000000000000000.index" named a live index and deleted that;
// "replication-offset-checkpoint" overwrote the high watermark. The contract
// existed the whole time — "the name must not collide with the log's own files"
// — as a sentence on the interface, which is advice to the caller and not a
// thing the log does.
//
// The escape is asserted by planting a file OUTSIDE the log directory and
// requiring it to survive, not by reading the error: an error that comes back
// while the file is already gone is the failure this test exists to catch.
func TestASidecarNameCannotReachOutsideTheLog(t *testing.T) {
	parent := tempDir(t)
	dir := filepath.Join(parent, "log")
	l, err := New(Options{Name: "sidecars", Path: dir, MaxSegmentBytes: 1 << 20})
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })

	bystander := filepath.Join(parent, "bystander")
	require.NoError(t, os.WriteFile(bystander, []byte("not the log's"), 0o600))

	for _, name := range []string{"../bystander", `..\bystander`, "sub/x", "..", "."} {
		require.ErrorIs(t, l.PutSidecar(name, []byte("x")), ErrInvalidSidecarName, "put %q", name)
		_, err := l.GetSidecar(name)
		require.ErrorIs(t, err, ErrInvalidSidecarName, "get %q", name)
		require.ErrorIs(t, l.RemoveSidecar(name), ErrInvalidSidecarName, "remove %q", name)
	}

	require.FileExists(t, bystander, "a sidecar name walked out of the log directory")
	b, err := os.ReadFile(bystander)
	require.NoError(t, err)
	require.Equal(t, "not the log's", string(b), "a sidecar write landed outside the log")
}

// A name the log owns is refused whatever the caller meant by it.
//
// Checked against the log's own constants rather than spelled-out strings: the
// point is that these names are the log's, and a test that re-types them would
// keep passing after a rename while the code it guards stopped matching.
func TestASidecarCannotNameOneOfTheLogsOwnFiles(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{Name: "own", Path: dir, MaxSegmentBytes: 1 << 20})
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })

	_, err = l.Append([]*Message{{Key: []byte("k"), Value: []byte("v")}})
	require.NoError(t, err)
	require.NoError(t, l.SyncAll())

	index := filepath.Join(dir, "00000000000000000000"+indexFileSuffix)
	require.FileExists(t, index, "the fixture needs a live index to try to delete")

	refused := []string{
		hwFileName,
		leaderEpochFileName,
		descriptorFileName,
		"00000000000000000000" + logFileSuffix,
		"00000000000000000000" + indexFileSuffix,
		"00000000000000000000" + keysSuffix,
		"00000000000000000000" + blocksSuffix,
		"anything" + cleanedSuffix,
		"anything" + truncatedSuffix,
		"anything" + trimmedSuffix,
		"anything" + tmpSuffix,
		// The stem is not what the log matches on: openLog scans by suffix and
		// fails the open outright on a .log whose stem is not an integer.
		"notes" + logFileSuffix,
		"",
	}
	for _, name := range refused {
		require.ErrorIs(t, l.PutSidecar(name, []byte("x")), ErrInvalidSidecarName, "put %q", name)
		_, err := l.GetSidecar(name)
		require.ErrorIs(t, err, ErrInvalidSidecarName, "get %q", name)
		require.ErrorIs(t, l.RemoveSidecar(name), ErrInvalidSidecarName, "remove %q", name)
	}

	require.FileExists(t, index, "RemoveSidecar deleted a live index")
	require.FileExists(t, filepath.Join(dir, descriptorFileName), "RemoveSidecar deleted the descriptor")

	// And the log still opens, which is the failure the .log stem would have
	// caused and the one no error return would have told anyone about.
	require.NoError(t, l.Close())
	reopened, err := New(Options{Name: "own", Path: dir, MaxSegmentBytes: 1 << 20})
	require.NoError(t, err, "the log did not reopen after the refused names")
	require.NoError(t, reopened.Close())
}

// The list of names the log owns is derived from a real log directory, not from
// the list the code keeps.
//
// logOwnedFileNames and logOwnedFileSuffixes are an enumeration, and an
// enumeration rots: the log gains a new kind of file, nobody thinks about the
// sidecar rule, and a client can name the new file from that day on. The two
// tests above cannot see that happen — they check the names the list already
// has. This one exercises the log until its directory is full of its own work
// and then requires that EVERY entry in it is refused, so the thing under test
// is what the log actually writes.
func TestEveryFileTheLogWritesIsRefusedAsASidecarName(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{
		Path:            dir,
		Name:            "derived",
		MaxSegmentBytes: 8 << 10,
		Compact:         true,
		Compression:     compress.Zstd,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })

	// Enough to roll several segments, so the sealed ones carry their derived
	// files (key digests, block tables) and not only a log and an index.
	for i := range 2000 {
		_, err := l.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("key-%04d", i%50)),
			Value: []byte(fmt.Sprintf("value-%04d-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", i)),
		}})
		require.NoError(t, err)
	}
	l.SetHighWatermark(l.NewestOffset())
	require.NoError(t, l.NewLeaderEpoch(7))
	require.NoError(t, l.Clean())
	require.NoError(t, l.SyncAll())

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	var (
		present  = map[string]bool{}
		suffixes = map[string]bool{}
	)
	for _, e := range entries {
		name := e.Name()
		present[name] = true
		if ext := filepath.Ext(name); ext != "" {
			suffixes[ext] = true
		}
		require.ErrorIs(t, checkSidecarName(name), ErrInvalidSidecarName,
			"the log writes %q into its own directory and a client may name it", name)
	}

	// What the fixture actually reached, stated rather than assumed. A log that
	// stopped writing one of these would leave the loop above passing on a
	// directory that no longer contains the file it was meant to prove refused.
	for _, want := range []string{hwFileName, leaderEpochFileName, descriptorFileName} {
		require.True(t, present[want], "the fixture never wrote %s, so nothing here refused it", want)
	}
	require.GreaterOrEqual(t, len(suffixes), 3,
		"only %v in the log directory; the fixture is too thin to say much about suffixes", suffixes)
}

// The ordinary case still works, end to end. GetSidecar and RemoveSidecar had
// no caller anywhere in the repo before this — not in the log, not in a test —
// so nothing said what they did, and a refusal added above them could have
// broken both without a single failure.
func TestASidecarRoundTrips(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{Name: "roundtrip", Path: dir, MaxSegmentBytes: 1 << 20})
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })

	_, err = l.GetSidecar("recovery-floor")
	require.True(t, os.IsNotExist(err), "an absent sidecar must satisfy os.IsNotExist, got %v", err)

	require.NoError(t, l.PutSidecar("recovery-floor", []byte("42")))
	b, err := l.GetSidecar("recovery-floor")
	require.NoError(t, err)
	require.Equal(t, "42", string(b))

	require.NoError(t, l.PutSidecar("recovery-floor", []byte("43")), "a sidecar must be overwritable")
	b, err = l.GetSidecar("recovery-floor")
	require.NoError(t, err)
	require.Equal(t, "43", string(b))

	require.NoError(t, l.RemoveSidecar("recovery-floor"))
	_, err = l.GetSidecar("recovery-floor")
	require.True(t, os.IsNotExist(err), "the sidecar outlived its removal")
	require.NoError(t, l.RemoveSidecar("recovery-floor"), "removing an absent sidecar is a no-op")
}
