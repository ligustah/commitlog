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
	return idx.file.Truncate(idx.position)
}
