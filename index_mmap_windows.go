//go:build windows

package commitlog

import (
	stderrors "errors"
	"os"
	"reflect"
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

// mapIndexFile maps the whole of f read-write, without going through gommap.
//
// gommap's unmap on this platform calls FlushFileBuffers itself,
// unconditionally, under a package-level lock, before it releases anything. So
// every index teardown in the process paid an fsync and took it in series with
// every other one, whichever Close variant the caller chose — which is why
// Close, CloseFlushed and CloseDiscarding were indistinguishable here: the
// fsync they choose between is not the one that cost. Measured on 64 dirty
// 64KB mappings: 3.28ms per unmap through gommap against 26µs issued
// directly. durable_streams reported a 9m43s Coordinator.Close with
// FlushFileBuffers in the stack.
//
// gommap keeps an address-to-handle registry because its Sync flushes with the
// handle CreateFileMapping returned. Nothing here needs that handle again:
// syncMmap flushes through FlushViewOfFile and the *os.File, for the separate
// reason given above. A view holds its own reference to the section object and
// UnmapViewOfFile and CloseHandle may be called in either order, so the handle
// is closed as soon as the view exists. That leaves no registry to protect, no
// lock to take, and an unmap that is one syscall and no flush.
//
// A zero-length file cannot be mapped, here as under gommap: CreateFileMapping
// refuses a maximum size of zero on an empty file. Callers already treat "no
// mapping" as the coherent state for an empty index rather than mapping one.
func mapIndexFile(f *os.File) (gommap.MMap, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "sizing the index file to map")
	}
	size := fi.Size()
	h, err := syscall.CreateFileMapping(syscall.Handle(f.Fd()), nil,
		syscall.PAGE_READWRITE, uint32(size>>32), uint32(size&0xFFFFFFFF), nil)
	if err != nil {
		return nil, errors.Wrap(err, "CreateFileMapping failed")
	}
	addr, mapErr := syscall.MapViewOfFile(h, syscall.FILE_MAP_WRITE, 0, 0, uintptr(size))
	// Released whether or not the view was created: the view owns the section
	// from here, and if there is no view there is nothing to own. gommap leaks
	// the handle on the failure path.
	closeErr := syscall.CloseHandle(h)
	if mapErr != nil {
		return nil, errors.Wrap(mapErr, "MapViewOfFile failed")
	}
	if closeErr != nil {
		// The view is usable and only its section handle is unaccounted for, so
		// this throws away something that works. It is still the right way
		// round: closing a handle we just opened fails only if this file's idea
		// of what it owns is already wrong, and a caller told the mapping
		// failed retries or reports it, where one handed a mapping we cannot
		// account for carries it for the life of the process. Every caller of
		// mmapFile already has a failure path — that is what the seam in
		// index.go exists to exercise.
		return nil, errors.Wrap(stderrors.Join(closeErr, syscall.UnmapViewOfFile(addr)),
			"CloseHandle failed after mapping")
	}
	return viewSlice(addr, size), nil
}

// viewSlice describes the mapped view as a slice.
//
// The two linters this repo runs disagree about how to spell this, and there
// is no third spelling. go vet's unsafeptr check rejects
// unsafe.Slice((*byte)(unsafe.Pointer(addr)), n) because addr is a bare
// uintptr, and it runs on the Windows job, where this file compiles.
// staticcheck rejects reflect.SliceHeader as deprecated, and today it does not
// see this file at all because it runs on Linux — so the directive below is
// what stops that from being an accident that a later GOOS=windows lint run
// turns into a failure.
//
// What the deprecation is about does not apply here: a Data field written as a
// uintptr is invisible to the garbage collector, which matters when it points
// into the Go heap and nothing else keeps the object alive. This points at a
// view of a file mapping. The OS keeps it until UnmapViewOfFile, the collector
// has no claim on it, and gommap constructed its mappings exactly this way.
func viewSlice(addr uintptr, size int64) gommap.MMap {
	m := gommap.MMap{}
	//lint:ignore SA1019 see above: no vet-clean non-deprecated spelling exists.
	dh := (*reflect.SliceHeader)(unsafe.Pointer(&m))
	dh.Data = addr
	dh.Len = int(size)
	dh.Cap = dh.Len
	return m
}

// unmapFile releases a mapping made by mapIndexFile. It does NOT flush: a
// caller that wants the bytes durable calls Sync first, and one that does not
// is entitled to skip it — which was not true while gommap owned the unmap.
func unmapFile(m gommap.MMap) error {
	if len(m) == 0 {
		return nil
	}
	if err := syscall.UnmapViewOfFile(uintptr(unsafe.Pointer(&m[0]))); err != nil {
		return errors.Wrap(err, "UnmapViewOfFile failed")
	}
	return nil
}
