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
// The caller holds idx.mu exclusively. mapMu is taken on top of it for the
// unmap/remap, to exclude a flush — which pins the mapping WITHOUT mu.
func (idx *index) shrink() error {
	remap := idx.mmap != nil
	if remap {
		idx.mapMu.Lock()
		err := unmapFile(idx.mmap)
		idx.mmap = nil
		idx.mapMu.Unlock()
		if err != nil {
			return errors.Wrap(err, "unmap failed during shrink")
		}
	}
	if err := idx.file.Truncate(idx.position); err != nil {
		return errors.Wrap(err, "truncate failed during shrink")
	}
	// size tracks the FILE, so it must follow the truncate whether or not a
	// remap happens. Updating it only in the remap branch left an index shrunk
	// while EMPTY claiming the pre-allocated size of a file that no longer
	// existed, with a nil mapping — a state nothing rejected and everything
	// downstream misread. writeAt then compared against the stale size, decided
	// no expansion was needed, and copied into a nil mapping: slicing nil at
	// [0:] is legal Go, so the write silently did NOTHING while position still
	// advanced. The index then claimed entries it did not hold, and the first
	// read of one panicked indexing the nil mapping.
	idx.size = idx.position
	if remap && idx.position > 0 {
		idx.mapMu.Lock()
		mmap, err := mmapFile(idx.file)
		idx.mmap = mmap
		idx.mapMu.Unlock()
		if err != nil {
			return errors.Wrap(err, "remap failed after shrink")
		}
	}
	// A zero-length file cannot be mapped, so an empty index legitimately has
	// no mapping. That is now COHERENT rather than merely survivable: size is 0
	// too, so the next write expands the file and maps it before touching it.
	return nil
}
