//go:build !windows

package commitlog

import (
	stderrors "errors"
	"os"
	"path/filepath"
	"syscall"

	"github.com/pkg/errors"
)

// lockHandle is the open lock file. flock is attached to the open file
// description, so the descriptor has to stay open for the lock to stay held.
type lockHandle = *os.File

func lockFilePath(dir string) string { return filepath.Join(dir, lockFileName) }

// acquireDirLock takes an exclusive flock, failing immediately rather than
// waiting. LOCK_NB is the point: a caller that would rather wait can retry, and
// one that would rather not is not blocked indefinitely by a peer that is
// running normally and will hold this for its whole life.
//
// flock releases automatically when the process dies, including on SIGKILL, so
// a crashed process leaves the directory openable rather than needing an
// operator to remove a stale file. That is the property this has over a plain
// O_EXCL create, which cannot tell a live holder from a dead one's litter.
func acquireDirLock(path string) (lockHandle, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, errors.Wrap(err, "open log directory lock")
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if stderrors.Is(err, syscall.EWOULDBLOCK) {
			return nil, errors.Wrapf(ErrLogLocked, "%s", path)
		}
		return nil, errors.Wrap(err, "lock log directory")
	}
	return f, nil
}

// releaseDirLock closes the descriptor, which drops the flock with it. Closing
// is the release: an explicit LOCK_UN followed by a Close would unlock twice,
// and the Close alone cannot be skipped without leaking the descriptor.
func releaseDirLock(h lockHandle) error {
	if h == nil {
		return nil
	}
	return errors.Wrap(h.Close(), "release log directory lock")
}
