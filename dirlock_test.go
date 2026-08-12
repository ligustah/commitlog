package commitlog

import (
	stderrors "errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// The second open of a live log's directory is refused.
//
// This is the whole contract. There is deliberately no test of what two writers
// do to each other, because the point of the lock is that the state cannot be
// constructed any more — and a test that reached around the lock to build it
// would be asserting the behaviour of a configuration the log now refuses.
func TestASecondOpenOfALiveLogDirectoryIsRefused(t *testing.T) {
	dir := tempDir(t)

	first, err := New(Options{Path: dir, MaxSegmentBytes: 1 << 20})
	require.NoError(t, err)
	defer first.Close()

	second, err := New(Options{Path: dir, MaxSegmentBytes: 1 << 20})
	require.ErrorIs(t, err, ErrLogLocked)
	require.Nil(t, second, "a refused open must not hand back a log")

	// The refusal costs the holder nothing: the log that owns the directory is
	// still usable, which is what makes this a refusal of the newcomer rather
	// than a collision that breaks both.
	offsets, err := first.Append([]*Message{{Value: []byte("still writable")}})
	require.NoError(t, err)
	require.Len(t, offsets, 1)
}

// Closing gives the directory back, so an ordinary restart can reopen it. A
// lock that outlived its log would turn every clean shutdown into a directory
// nothing could open until the process exited.
func TestClosingALogReleasesItsDirectory(t *testing.T) {
	dir := tempDir(t)

	first, err := New(Options{Path: dir, MaxSegmentBytes: 1 << 20})
	require.NoError(t, err)
	_, err = first.Append([]*Message{{Value: []byte("before the restart")}})
	require.NoError(t, err)
	require.NoError(t, first.Close())

	second, err := New(Options{Path: dir, MaxSegmentBytes: 1 << 20})
	require.NoError(t, err, "a closed log must leave its directory openable")
	defer second.Close()
	require.Equal(t, int64(0), second.NewestOffset(), "the reopened log must hold the record")
}

// Delete removes the lock file along with everything else. It is the one case
// where the file cannot simply be left behind: a directory that still contains
// an entry has not been deleted, and on Windows a held handle would have made
// the removal fail outright.
func TestDeleteRemovesTheLockFileWithTheDirectory(t *testing.T) {
	dir := tempDir(t)

	l, err := New(Options{Path: dir, MaxSegmentBytes: 1 << 20})
	require.NoError(t, err)
	_, err = l.Append([]*Message{{Value: []byte("doomed")}})
	require.NoError(t, err)

	require.FileExists(t, filepath.Join(dir, lockFileName), "the lock file should exist while the log is open")
	require.NoError(t, l.Delete())

	_, err = os.Stat(dir)
	require.True(t, os.IsNotExist(err), "Delete must remove the directory, lock file and all")
}

// A sidecar cannot name the lock file. The lock is the newest member of the
// log's own file set, and the reason it has to be in the denylist is sharper
// than for the others: overwriting it does not corrupt data, it revokes the
// exclusion that keeps a second writer out.
func TestASidecarCannotNameTheDirectoryLock(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t), MaxSegmentBytes: 1 << 20})
	defer l.Close()
	defer cleanup()

	require.ErrorIs(t, l.PutSidecar(lockFileName, []byte("mine now")), ErrInvalidSidecarName)
	require.ErrorIs(t, l.RemoveSidecar(lockFileName), ErrInvalidSidecarName)
}

// The cross-process half, which is the one the in-process mutexes could never
// have covered and the one durable_streams reported. A second PROCESS opening
// the directory must be refused for the same reason a second open in this one
// is -- flock and the Windows share mode are both properties of the OS handle,
// not of this package's memory, which is what makes them able to say so.
func TestASecondProcessCannotOpenALiveLogDirectory(t *testing.T) {
	if os.Getenv("COMMITLOG_DIRLOCK_CHILD") != "" {
		// Running as the child: try to open the directory the parent holds and
		// report the verdict through the exit code.
		//
		// The success code is 7 and not 0 ON PURPOSE. A child whose -test.run
		// matched nothing runs no test at all and exits 0, so a parent checking
		// for 0 would pass without the child ever having tried to open
		// anything — the same vacuous-selection trap as `go test -run` with a
		// pattern that matches no test. Demanding a code the child can only
		// produce from inside this branch is what makes the assertion real.
		_, err := New(Options{Path: os.Getenv("COMMITLOG_DIRLOCK_DIR"), MaxSegmentBytes: 1 << 20})
		if err == nil {
			os.Exit(2) // opened a directory another process holds
		}
		if !stderrors.Is(err, ErrLogLocked) {
			os.Exit(3) // refused, but for the wrong reason
		}
		os.Exit(7)
	}

	dir := tempDir(t)
	l, err := New(Options{Path: dir, MaxSegmentBytes: 1 << 20})
	require.NoError(t, err)
	defer l.Close()

	cmd := exec.Command(os.Args[0], "-test.run", "^"+t.Name()+"$")
	cmd.Env = append(os.Environ(),
		"COMMITLOG_DIRLOCK_CHILD=1",
		"COMMITLOG_DIRLOCK_DIR="+dir,
	)
	out, runErr := cmd.CombinedOutput()

	var exit *exec.ExitError
	require.ErrorAs(t, runErr, &exit,
		"child must exit non-zero with the agreed code; output:\n%s", out)
	require.Equal(t, 7, exit.ExitCode(),
		"7 = refused with ErrLogLocked. 2 = it OPENED a directory this process holds. "+
			"3 = refused for some other reason. 0 = the child ran no test at all, "+
			"which is the failure this code exists to tell apart. Output:\n%s", out)
}
