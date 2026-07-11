//go:build windows

package commitlog

import (
	"os"
	"syscall"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/tysonmote/gommap"
)

// syncMmap flushes dirty pages of the memory-mapped region to disk.
//
// gommap.Sync on Windows calls FlushFileBuffers with the handle returned by
// CreateFileMapping (a file-mapping object handle). FlushFileBuffers requires
// a file handle (from CreateFile), not a mapping handle, and returns
// ERROR_INVALID_HANDLE (Win32 error 6) when given the wrong handle type.
//
// We work around this by calling FlushViewOfFile directly to flush dirty mmap
// pages into the OS page cache, then delegating to f.Sync() which calls
// FlushFileBuffers with the correct file handle.
func syncMmap(mmap gommap.MMap, f *os.File) error {
	if len(mmap) > 0 {
		addr := uintptr(unsafe.Pointer(&mmap[0]))
		if err := syscall.FlushViewOfFile(addr, uintptr(len(mmap))); err != nil {
			return errors.Wrap(err, "FlushViewOfFile failed")
		}
	}
	if err := f.Sync(); err != nil {
		return errors.Wrap(err, "file sync failed")
	}
	return nil
}

// shrink truncates the index file to idx.position bytes.
//
// On Windows, an open MapViewOfFile prevents SetEndOfFile from succeeding.
// When the mmap is still active (called from Shrink/Seal), we unmap first,
// truncate, then remap at the smaller size so readers remain valid.
// When mmap is already nil (called from Close after UnsafeUnmap), we skip
// the unmap/remap and just truncate.
func (idx *index) shrink() error {
	remap := idx.mmap != nil
	if remap {
		if err := unmapFile(idx.mmap); err != nil {
			return errors.Wrap(err, "unmap failed during shrink")
		}
		idx.mmap = nil
	}
	if err := idx.file.Truncate(idx.position); err != nil {
		return errors.Wrap(err, "truncate failed during shrink")
	}
	if remap && idx.position > 0 {
		var err error
		idx.mmap, err = mmapFile(idx.file)
		if err != nil {
			return errors.Wrap(err, "remap failed after shrink")
		}
		idx.size = idx.position
	}
	return nil
}
