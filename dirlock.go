package commitlog

import (
	"github.com/pkg/errors"
)

// ErrLogLocked reports that another live commitLog holds this directory.
//
// It is a distinct sentinel because the two things a caller might do about it
// are opposite. A directory held by a peer that is still running is a
// configuration mistake — two brokers pointed at one data dir — and retrying
// makes it worse. A directory held by a process that has just died releases on
// its own, and there retrying is exactly right. Only the caller knows which it
// is looking at, so the log names the condition instead of guessing.
var ErrLogLocked = errors.New("commitlog: log directory is locked by another process")

// lockFileName is the log's own exclusion file. Leading dot so it sorts away
// from the segment files, and a name openLog's scan cannot mistake for one:
// that scan keys on the .log/.index suffixes and parses the stem as an offset.
const lockFileName = ".lock"

// dirLock is an exclusive claim on a log directory, held for the life of the
// log. The zero value holds nothing and releases cleanly, so a failure between
// acquiring and returning can release unconditionally.
type dirLock struct {
	path string
	h    lockHandle
	held bool
}

// lockLogDir takes the exclusive lock on dir, which must already exist.
//
// Why the log needs this at all, given it already has appendMu: appendMu is a
// claim over one process's memory, and every offset the log hands out is
// computed from state that lives there — the active segment's Position and
// NextOffset. A second process opens the same directory, builds its own copy of
// that state from the same files, and is then correct about a log that is no
// longer the one on disk. Both append at their own believed position into the
// same file, each overwriting frames the other wrote, and the frame CRCs catch
// it only on the way back out: "failed to read message headers", zero records
// readable, from two writers that both saw every append succeed.
//
// Nothing in the format can detect that after the fact. The frames are
// individually well-formed; what is wrong is that two of them were written to
// one span of the file. So the exclusion has to happen before the second writer
// exists, which makes it an open-time refusal rather than anything the append
// path could check.
//
// This is the LOCAL counterpart to the single-writer contract the CommitLog
// interface states for tiers — and the failure is much worse here. Two writers
// on a shared store produce a duplicate object and cost storage; two writers on
// a shared directory destroy the log.
//
// The lock is advisory in the sense that it binds commitlog processes only.
// Anything else writing into a log directory is outside what the log can defend
// against, and always was.
func lockLogDir(dir string) (*dirLock, error) {
	h, err := acquireDirLock(lockFilePath(dir))
	if err != nil {
		return nil, err
	}
	return &dirLock{path: dir, h: h, held: true}, nil
}

// release gives the lock back. Safe on a nil or already-released lock, because
// both callers of it (a failed open unwinding, and Close) can reach it without
// knowing whether the lock was ever taken.
//
// It does NOT remove the lock file. On Windows the file cannot be removed while
// the handle is open, and removing it after the handle closes reintroduces the
// window this exists to close: process B could take the lock on the file A is
// about to unlink, and then both would hold it. An empty file left behind costs
// a directory entry; Delete removes it with everything else.
func (d *dirLock) release() error {
	if d == nil || !d.held {
		return nil
	}
	d.held = false
	return releaseDirLock(d.h)
}
