//go:build !windows

package commitlog

import (
	"os"

	"github.com/pkg/errors"
	"github.com/tysonmote/gommap"
)

func syncMmap(mmap gommap.MMap, f *os.File) error {
	if err := f.Sync(); err != nil {
		return errors.Wrap(err, "file sync failed")
	}
	if len(mmap) > 0 {
		if err := mmap.Sync(gommap.MS_SYNC); err != nil {
			return errors.Wrap(err, "mmap sync failed")
		}
	}
	return nil
}

func (idx *index) shrink() error {
	if err := idx.file.Truncate(idx.position); err != nil {
		return err
	}
	// size tracks the FILE, so it follows the truncate. Leaving it at the
	// pre-allocated value made writeAt compare against a file that no longer
	// existed and skip the expansion it needed — harmless here only because
	// shrink runs at seal, after which nothing writes. Keeping it honest costs
	// nothing and removes the standing trap. (The Windows implementation has
	// the same line for a sharper reason; see there.)
	idx.size = idx.position
	return nil
}

// mapIndexFile maps the whole of f read-write.
//
// This used to run under a package-level mutex shared with the unmap side. The
// mutex was there for gommap's Windows internals — a package-level registry
// written without a lock — and this side of the build never needed it:
// gommap.MapAt is an fstat, an mmap syscall and a slice header built on a
// local, and UnsafeUnmap is a bare SYS_MUNMAP. Neither touches package state,
// so there was nothing for it to serialize. Windows no longer goes through
// gommap at all (see index_mmap_windows.go), which left the mutex with no
// justification on either platform.
func mapIndexFile(f *os.File) (gommap.MMap, error) {
	return gommap.Map(f.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
}

func unmapFile(m gommap.MMap) error {
	return m.UnsafeUnmap()
}
