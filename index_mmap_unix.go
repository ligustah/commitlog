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
