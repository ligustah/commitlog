//go:build !windows

package commitlog

import (
	stderrors "errors"
	"os"
	"syscall"

	"github.com/pkg/errors"
)

// syncDir makes a rename into dir durable.
//
// fsync on the renamed FILE is not enough and is not the same question: it
// makes the bytes durable, while the name that reaches them lives in the
// directory. A rename that has returned is visible to every later reader in
// this boot and can still be undone by a power cut, which is why POSIX makes
// the directory a separate fsync rather than an implied one.
//
// It matters where the rename IS the commit point. Everything the log finishes
// with a rename is in that position: the high watermark checkpoint a caller
// waited on through SyncAll or Close, a client's sidecar, an object published
// into a file-backed tier, the log's DESCRIPTOR, and the leader epoch
// checkpoint. For those, "the write returned" and "the write survives" have to
// be the same statement.
//
// The last two were absent from that list because they were absent from this
// path: both wrote through the atomic-file library directly instead of through
// AtomicWriteFileWithRetry, so they got the torn-write guarantee and not the
// durability one. A list of callers written out by hand does not notice a
// caller that never arrives, which is why the check that keeps it honest is a
// grep for the library rather than this sentence — see writeDescriptor.
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return errors.Wrap(err, "open directory to sync")
	}
	if err := f.Sync(); err != nil && !unsupportedDirSync(err) {
		_ = f.Close()
		return errors.Wrap(err, "sync directory")
	}
	return errors.Wrap(f.Close(), "close directory after sync")
}

// unsupportedDirSync reports the errors that mean this FILESYSTEM does not
// implement fsync on a directory, as distinct from this write having failed.
//
// The distinction is the whole point of naming them rather than swallowing
// everything: a filesystem that answers EINVAL never had the guarantee to give
// and never will, so failing the caller's write over it would refuse an
// operation that has already fully succeeded by every measure this build can
// take. Anything else — EIO, ENOSPC — is the write's problem and is returned.
// Some network and fuse mounts are the real cases; the open above still has to
// work, so a directory we genuinely cannot reach is still an error.
func unsupportedDirSync(err error) bool {
	return stderrors.Is(err, syscall.EINVAL) || stderrors.Is(err, syscall.ENOTSUP)
}
