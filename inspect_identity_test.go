package commitlog

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A RECLAIMER JUDGES LOGS IT MUST NOT OPEN.
//
// InspectIdentity exists for durable_streams' periodic orphan pass, and its whole
// value is in the four answers being four rather than three: an unstamped log and
// a directory holding nothing look identical from a distance and demand opposite
// treatment. Unstamped and stale are indistinguishable by identity, so only one
// of them may ever be deleted — collapsing them destroys data, and collapsing
// them the other way leaks it forever.
//
// Each test below is one of the four, plus the property the whole thing rests on:
// that reading the answer does not disturb what it read.

func TestInspectIdentityReadsAStoredStamp(t *testing.T) {
	dir := tempDir(t)
	want := []byte("incarnation-7")
	l, err := New(Options{Name: "stamped", Path: dir, MaxSegmentBytes: 512, Identity: want})
	require.NoError(t, err)
	appendMsg(t, l, "a record, so the log is not merely a directory")
	require.NoError(t, l.Close())

	got, err := InspectIdentity(dir)
	require.NoError(t, err)
	require.True(t, got.Stored)
	require.Equal(t, want, got.Identity)
}

// A LOG THAT PREDATES THE FEATURE IS NOT AN ORPHAN.
//
// The answer the whole design is arranged around: a real log, no identity, and a
// nil error — because a pass that read this as "nothing here" would delete the
// copies that never had a chance to match.
func TestInspectIdentityReportsALogThatStoresNoStamp(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{Name: "unstamped", Path: dir, MaxSegmentBytes: 512})
	require.NoError(t, err)
	appendMsg(t, l, "a record, so the log is not merely a directory")
	require.NoError(t, l.Close())

	got, err := InspectIdentity(dir)
	require.NoError(t, err)
	require.False(t, got.Stored, "an unstamped log must be distinguishable from an absent one")
	require.Nil(t, got.Identity)
}

// NOTHING THERE IS ITS OWN ANSWER, and it is an error rather than a zero value,
// so a caller cannot reach it by forgetting to check one.
//
// Both shapes of nothing give the same sentinel on purpose. A directory with no
// descriptor is not a log — every log that has been through New has one — so
// splitting it from a missing directory would give the caller a distinction it
// cannot act on while blurring the one it must.
func TestInspectIdentityRefusesAPathWithNoLog(t *testing.T) {
	t.Run("no directory", func(t *testing.T) {
		_, err := InspectIdentity(filepath.Join(tempDir(t), "never-created"))
		require.ErrorIs(t, err, ErrNoLog)
	})
	t.Run("a directory holding no descriptor", func(t *testing.T) {
		dir := tempDir(t)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "00000000000000000000.log"),
			[]byte("bytes that are not a log"), 0o600))
		_, err := InspectIdentity(dir)
		require.ErrorIs(t, err, ErrNoLog)
	})
}

// A DAMAGED DESCRIPTOR IS "CANNOT JUDGE", NOT "NO IDENTITY".
//
// The fourth answer, and the one an over-eager simplification would lose: if a
// parse failure fell through to the zero LogIdentity with a nil error, every
// corrupt descriptor would read as an unstamped log — and every torn byte would
// become a permanent leak, since unstamped is the answer that forbids deletion.
// It must also not read as ErrNoLog, which is the answer that permits it.
func TestInspectIdentityReportsADamagedDescriptorAsAnError(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{Name: "damaged", Path: dir, MaxSegmentBytes: 512,
		Identity: []byte("incarnation-9")})
	require.NoError(t, err)
	appendMsg(t, l, "a record")
	require.NoError(t, l.Close())

	require.NoError(t, os.WriteFile(descriptorPath(dir),
		[]byte("this is not a descriptor\n"), 0o600))

	got, err := InspectIdentity(dir)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrNoLog,
		"a damaged descriptor read as ErrNoLog is a reclaimer being told it may delete")
	require.False(t, got.Stored)
}

// READING THE EVIDENCE DOES NOT ALTER IT.
//
// The property that makes this worth having at all rather than a New and a Close:
// the pass runs over copies it may be about to delete and over logs another
// process holds open, so it must take no lock, run no recovery and write nothing.
//
// Asserted two ways, because either alone is weak. The directory listing and the
// descriptor's bytes must be byte-identical afterwards — an adoption or a
// recovery would show up in one or the other — and the call must succeed while a
// live log holds the directory lock, which a New would not.
func TestInspectIdentityNeitherLocksNorWrites(t *testing.T) {
	dir := tempDir(t)
	want := []byte("incarnation-11")
	l, err := New(Options{Name: "held", Path: dir, MaxSegmentBytes: 512, Identity: want,
		// Both background loops silenced, because the assertion below is that
		// nothing changed on disk across the read and either of them landing in
		// that window would make this test flake on the log's own housekeeping
		// rather than on the thing under test. The checkpoint loop in particular
		// ticks every 5s by default and writes through a temp file, so a listing
		// can catch a name that belongs to neither state.
		DisableAutoClean:     true,
		HWCheckpointInterval: time.Hour,
	})
	require.NoError(t, err)
	defer l.Close()
	appendMsg(t, l, "a record")
	require.NoError(t, l.SyncAll())

	before, err := os.ReadFile(descriptorPath(dir))
	require.NoError(t, err)
	beforeNames := dirNames(t, dir)

	// While l is open and holding the directory lock. A New here would fail.
	got, err := InspectIdentity(dir)
	require.NoError(t, err, "the read must not need the directory lock")
	require.Equal(t, want, got.Identity)

	after, err := os.ReadFile(descriptorPath(dir))
	require.NoError(t, err)
	require.Equal(t, before, after, "the descriptor was rewritten by a read")
	require.Equal(t, beforeNames, dirNames(t, dir), "the read left something behind")
}

func dirNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// A tiered log's descriptor is in its STORE, and a path cannot reach one.
//
// Documented as local-only, so this pins that it fails LOUDLY rather than
// answering for a log it cannot see. The caller this was built for is judging
// this broker's local copy, so local is the right scope — but "local" has to mean
// ErrNoLog and not a confident Stored=false, which would be this function
// asserting something about bytes it never read.
func TestInspectIdentityDoesNotReachIntoATier(t *testing.T) {
	dir := tempDir(t)
	fs, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)
	logDir := filepath.Join(dir, "log")
	l, cleanup := setupWithOptions(t, Options{
		Name:            "tiered",
		Path:            logDir,
		MaxSegmentBytes: 512,
		Tiers:           oneTier(fs),
		Identity:        []byte("incarnation-13"),
	})
	defer cleanup()
	appendMsg(t, l, "a record")
	require.NoError(t, l.SyncAll())

	// The premise: the descriptor really is in the store and not on disk.
	_, err = os.Stat(descriptorPath(logDir))
	require.True(t, os.IsNotExist(err), "this log keeps a LOCAL descriptor, so the test proves nothing")

	_, err = InspectIdentity(logDir)
	require.ErrorIs(t, err, ErrNoLog)
}

// The identity survives a REOPEN by a caller that passes none, which is what
// makes a later inspection meaningful at all.
//
// Cheap here and not covered by the descriptor tests from this angle: those
// assert what an open sees, and a reclaimer reads the file. If a no-identity
// reopen erased the line, every log in a process that stamps at creation and not
// at restart would read as unstamped by the pass — and never be reclaimed.
func TestInspectIdentitySeesAStampAReopenDidNotErase(t *testing.T) {
	dir := tempDir(t)
	want := []byte("incarnation-17")
	l, err := New(Options{Name: "persisted", Path: dir, MaxSegmentBytes: 512, Identity: want})
	require.NoError(t, err)
	appendMsg(t, l, "a record")
	require.NoError(t, l.Close())

	again, err := New(Options{Name: "persisted", Path: dir, MaxSegmentBytes: 512})
	require.NoError(t, err)
	require.NoError(t, again.Close())

	got, err := InspectIdentity(dir)
	require.NoError(t, err)
	require.True(t, got.Stored)
	require.Equal(t, want, got.Identity)
}
