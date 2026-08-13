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

	// The last three carry ClientSidecarPrefix, and they are the ones that keep
	// this test about traversal. Once a sidecar name has to carry the prefix,
	// every name above is refused for lacking it — so the traversal check could
	// be deleted outright and the first five lines would stay green. "client-"
	// is a prefix of "client-/../../bystander" too, and that one Cleans to a
	// file in the log directory's PARENT.
	for _, name := range []string{
		"../bystander", `..\bystander`, "sub/x", "..", ".",
		ClientSidecarPrefix + "/../../bystander",
		ClientSidecarPrefix + `\..\..\bystander`,
		ClientSidecarPrefix + "sub/x",
	} {
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

// A name the log owns is refused whatever the caller meant by it — now as one
// case of a wider rule: a name without ClientSidecarPrefix is refused, and no
// file the log writes carries the prefix.
//
// The log's own names are still spelled through its constants rather than
// re-typed, even though the check no longer consults them. What is under test
// is that THESE files cannot be named, and a test that re-typed them would keep
// passing after a rename while saying nothing about the files that exist.
//
// The plain names at the end of the fixture are the other half. Before the
// prefix they were all legal, including "recovery-floor" — the name this
// package's own doc comment offered as the example sidecar.
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
		// Ordinary names, refused because they are unclaimed rather than because
		// they are taken. "recovery-floor" was durable_streams' real sidecar and
		// this package's documented example, so if the prefix is ever weakened
		// to "refuse only what looks like the log's", this is the line that goes
		// green while the reservation quietly stops being one.
		"notes",
		"state.json",
		"recovery-floor",
		// The reservation with no name after it.
		ClientSidecarPrefix,
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

// The names the log owns are derived from a real log directory, not from a list
// the code keeps.
//
// This test outlived the lists it was written against. It existed because
// logOwnedFileNames and logOwnedFileSuffixes were an enumeration and an
// enumeration rots — the log gains a kind of file, nobody thinks about the
// sidecar rule, and a client can name the new file from that day on. The
// reserved prefix removed the lists, so the rot is gone; what the test proves
// now is the promise that replaced them, and it proves it in BOTH directions:
//
//   - no file the log writes can be named by a client (unchanged), and
//   - no file the log writes carries ClientSidecarPrefix.
//
// The second is the one that has to be checked against a real directory rather
// than argued. It is a promise about every file commitlog will ever add, and
// the only thing that can falsify it is commitlog adding one — at which point
// the client whose sidecar it collides with finds out by having its file
// overwritten, on a machine that is not this one.
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
		require.False(t, isClientSidecar(name),
			"the log writes %q, which carries the prefix reserved for its clients — "+
				"a client sidecar of that name is overwritten, and this scan skips it", name)
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

// A sidecar survives an open whatever it is called, which is the half of the
// reservation the refusal cannot deliver.
//
// checkSidecarName only governs names arriving through PutSidecar. It says
// nothing about what openLog does with the file once it is on disk, and openLog
// dispatches on SUFFIX. So the two names below were reachable only by refusing
// them at the door — and refusing them is what the prefix was supposed to stop
// needing, since it is exactly the enumerate-the-log's-suffixes list that rots.
//
// Both are planted directly rather than through PutSidecar. A client that wrote
// its sidecars under an older commitlog has files in that directory already,
// and this is what an upgrade finds. The stems are non-integer on purpose:
//
//   - "client-notes.log" fails strconv.Atoi in openLog, which RETURNS the
//     error, so the log does not open at all. The client's own file bricks it.
//   - "client-notes.index" has no matching .log and no manifest entry, so it is
//     an orphaned index and openLog DELETES it. That one is silent.
//
// The delete is why the assertion is on the file's contents surviving and not
// on the open returning nil: an open that succeeds having eaten the sidecar is
// the failure, and it looks like success from the caller's side.
func TestAClientSidecarSurvivesAnOpenWhateverItIsCalled(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{Name: "survives", Path: dir, MaxSegmentBytes: 1 << 20})
	require.NoError(t, err)
	_, err = l.Append([]*Message{{Key: []byte("k"), Value: []byte("v")}})
	require.NoError(t, err)
	require.NoError(t, l.Close())

	planted := map[string]string{
		ClientSidecarPrefix + "notes" + logFileSuffix:   "read as a segment",
		ClientSidecarPrefix + "notes" + indexFileSuffix: "removed as an orphan index",
		ClientSidecarPrefix + "state":                   "ordinary",
	}
	for name, body := range planted {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600))
	}

	reopened, err := New(Options{Name: "survives", Path: dir, MaxSegmentBytes: 1 << 20})
	require.NoError(t, err, "a client's own sidecar stopped the log from opening")
	t.Cleanup(func() { _ = reopened.Close() })

	for name, body := range planted {
		got, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err, "the open consumed the sidecar %q", name)
		require.Equal(t, body, string(got), "the open rewrote the sidecar %q", name)
	}

	// The log is still the log: the planted .log did not become a segment.
	require.Len(t, reopened.(*commitLog).segments, 1, "a sidecar was adopted as a segment")
	require.EqualValues(t, 0, reopened.NewestOffset(), "the log lost or gained records")
}

// A sidecar written before the first append does not make the log look old.
//
// logIsNew answers by looking for log bytes in the directory, and it decides
// whether the descriptor records what the caller created the log with or gets
// checked against it. A client that writes its config sidecar first — which is
// the natural order, since the config is what says how to create the log —
// would otherwise be creating a log that believes it already existed.
func TestASidecarWrittenBeforeTheFirstAppendLeavesTheLogNew(t *testing.T) {
	dir := tempDir(t)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, ClientSidecarPrefix+"config"+logFileSuffix), []byte("{}"), 0o600))

	isNew, err := logIsNew(Options{Path: dir})
	require.NoError(t, err)
	require.True(t, isNew, "a client sidecar counted as the log's own bytes")
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

	_, err = l.GetSidecar(ClientSidecarPrefix+"recovery-floor")
	require.True(t, os.IsNotExist(err), "an absent sidecar must satisfy os.IsNotExist, got %v", err)

	require.NoError(t, l.PutSidecar(ClientSidecarPrefix+"recovery-floor",[]byte("42")))
	b, err := l.GetSidecar(ClientSidecarPrefix+"recovery-floor")
	require.NoError(t, err)
	require.Equal(t, "42", string(b))

	require.NoError(t, l.PutSidecar(ClientSidecarPrefix+"recovery-floor",[]byte("43")), "a sidecar must be overwritable")
	b, err = l.GetSidecar(ClientSidecarPrefix+"recovery-floor")
	require.NoError(t, err)
	require.Equal(t, "43", string(b))

	require.NoError(t, l.RemoveSidecar(ClientSidecarPrefix+"recovery-floor"))
	_, err = l.GetSidecar(ClientSidecarPrefix+"recovery-floor")
	require.True(t, os.IsNotExist(err), "the sidecar outlived its removal")
	require.NoError(t, l.RemoveSidecar(ClientSidecarPrefix+"recovery-floor"), "removing an absent sidecar is a no-op")
}
