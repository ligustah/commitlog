//go:build windows

package commitlog

import (
	stderrors "errors"
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
		// The unmap above already happened, so returning straight out here left
		// the index with no mapping and a non-zero position. Nothing re-opens a
		// segment's index on the read path, so every later read of that segment
		// reported a corrupt index for the LIFE OF THE PROCESS — and seal
		// discards this error by design, on the premise that a failed shrink
		// costs a rebuilt index tail rather than data. That premise was only
		// true while the mapping survived the failure.
		//
		// So put it back. The truncate failed, which means the file is still the
		// size idx.size already claims, and mapping it again restores exactly
		// the state shrink was called in. A failed shrink then costs an
		// un-shrunk file, which is what the caller was told it costs.
		return errors.Wrap(stderrors.Join(err, idx.restoreMapping(remap)),
			"truncate failed during shrink")
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
			// Same wedge as the truncate path, reached the other way: the file
			// is shrunk and coherent, but with no mapping and a non-zero
			// position every read calls it corrupt. One more attempt is worth
			// making before giving up, because what fails here is transient —
			// the file is intact and the size is already right.
			return errors.Wrap(stderrors.Join(err, idx.restoreMapping(remap)),
				"remap failed after shrink")
		}
	}
	// A zero-length file cannot be mapped, so an empty index legitimately has
	// no mapping. That is now COHERENT rather than merely survivable: size is 0
	// too, so the next write expands the file and maps it before touching it.
	return nil
}

// restoreMapping puts a mapping back after a step of shrink has failed, so that
// a failed shrink leaves the index usable rather than dead.
//
// It exists because of the one state this file must never end in: no mapping
// with a non-zero position. Nothing re-opens a segment's index once the log is
// running, so that state is permanent for the process, and every read of the
// segment answers "corrupt index file" from then on. sqlcdc hit it on a
// segment with position=275700 and closed=false, and it took out 28 views.
//
// It maps whatever the file currently IS. Both callers have already made
// idx.size agree with that — the truncate path because the truncate did not
// happen, the remap path because the truncate did — so there is no size to
// pass in and no way for the two to disagree.
//
// remap is false when shrink was called with no mapping to begin with (from
// Close, after UnsafeUnmap). There was nothing to restore and mapping the file
// again would be a leak, so this does nothing. A zero-length file cannot be
// mapped at all, and needs no mapping: size 0 with no mapping is coherent.
func (idx *index) restoreMapping(remap bool) error {
	if !remap || idx.size == 0 {
		return nil
	}
	idx.mapMu.Lock()
	mmap, err := mmapFile(idx.file)
	idx.mmap = mmap
	idx.mapMu.Unlock()
	if err != nil {
		return errors.Wrap(err, "restoring the mapping after a failed shrink")
	}
	return nil
}
