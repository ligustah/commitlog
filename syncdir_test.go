package commitlog

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// syncDir has to WORK on the platform running it.
//
// This does not prove the durability — that needs a power cut, and no test in
// this repo can stage one. It proves the other thing, which is the risk the
// change actually introduced: fsync on a directory is a call some filesystems
// answer with an error, and it now sits on the return path of every atomic
// write the log makes. A platform or a CI filesystem where it fails would
// otherwise break the high watermark checkpoint, every sidecar and every
// file-store Put at once, and this test says so directly instead.
//
// unsupportedDirSync is what keeps that from being a refusal on a filesystem
// that never had the guarantee, so a red here means something worse than
// unsupported: the directory could not be opened, or the sync failed for a
// reason that is about this write.
func TestSyncDirWorksOnThisPlatform(t *testing.T) {
	dir := tempDir(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "x"), []byte("x"), 0o600))
	require.NoError(t, syncDir(dir), "fsync on a directory failed on this platform")
}

// And the write path it now returns through still round-trips. Every high
// watermark checkpoint goes through atomicWriteFileWithin, so the suite covers
// the wiring in bulk; this covers the shape the bulk cannot show — that the
// success return is the syncDir call and not a dropped error, and that a write
// into a directory that does not exist still FAILS.
func TestAnAtomicWriteReturnsThroughTheDirectorySync(t *testing.T) {
	dir := tempDir(t)
	path := filepath.Join(dir, "checkpoint")

	require.NoError(t, AtomicWriteFileWithRetry(path, bytes.NewReader([]byte("41"))))
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "41", string(b))

	require.NoError(t, AtomicWriteFileWithRetry(path, bytes.NewReader([]byte("42"))))
	b, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "42", string(b), "the replacement did not land")

	// A zero budget rather than the exported wrapper: the failure here is
	// permanent, and the retry loop would spend its whole waited-on budget
	// discovering that a directory which does not exist still does not exist.
	err = atomicWriteFileWithin(filepath.Join(dir, "missing", "checkpoint"), bytes.NewReader([]byte("x")), 0)
	require.Error(t, err, "a write into a directory that does not exist reported success")
}
