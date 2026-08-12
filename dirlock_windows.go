//go:build windows

package commitlog

import (
	stderrors "errors"
	"path/filepath"
	"syscall"

	"github.com/pkg/errors"
)

// lockHandle is the raw file handle. It is not wrapped in an *os.File because
// the exclusion comes from how the handle was opened, not from anything done to
// it afterwards, and os.OpenFile cannot express that.
type lockHandle = syscall.Handle

// errSharingViolation is ERROR_SHARING_VIOLATION, which Go's syscall package
// does not name on Windows. It is the error a second opener gets, so it is the
// one that means "another log holds this directory" rather than a real failure.
const errSharingViolation = syscall.Errno(32)

func lockFilePath(dir string) string { return filepath.Join(dir, lockFileName) }

// acquireDirLock opens the lock file with a share mode of ZERO, which is the
// exclusion: Windows refuses any later open of a file that a live handle holds
// without sharing, and returns ERROR_SHARING_VIOLATION to the loser.
//
// Deliberately not LockFileEx, which would need golang.org/x/sys — this package
// has no such dependency and does not need one for a claim that share mode
// already expresses. Deliberately not os.OpenFile either: it opens with
// FILE_SHARE_READ|FILE_SHARE_WRITE, so two of them succeed and neither excludes
// anything.
//
// Windows closes handles when a process dies, by any means, so a crashed holder
// releases the directory the same way flock does on unix. Neither platform
// leaves a lock that needs clearing by hand, which is the whole reason this is a
// held handle and not a file whose existence means "locked".
func acquireDirLock(path string) (lockHandle, error) {
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return syscall.InvalidHandle, errors.Wrap(err, "log directory lock path")
	}
	h, err := syscall.CreateFile(
		p,
		syscall.GENERIC_READ|syscall.GENERIC_WRITE,
		0, // no sharing -- this is the lock
		nil,
		syscall.OPEN_ALWAYS,
		syscall.FILE_ATTRIBUTE_NORMAL,
		0,
	)
	if err != nil {
		if stderrors.Is(err, errSharingViolation) || stderrors.Is(err, syscall.ERROR_ACCESS_DENIED) {
			return syscall.InvalidHandle, errors.Wrapf(ErrLogLocked, "%s", path)
		}
		return syscall.InvalidHandle, errors.Wrap(err, "open log directory lock")
	}
	return h, nil
}

// releaseDirLock closes the handle, which is what drops the exclusion.
func releaseDirLock(h lockHandle) error {
	if h == syscall.InvalidHandle {
		return nil
	}
	return errors.Wrap(syscall.CloseHandle(h), "release log directory lock")
}
