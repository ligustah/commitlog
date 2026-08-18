//go:build windows

package commitlog

import (
	"os"
	"syscall"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

// TestIndexMappingReleasesTheSectionHandle measures directly what the rest of
// the suite only infers.
//
// mapIndexFile closes the handle CreateFileMapping returned as soon as the view
// exists, on the documented rule that a view keeps its own reference to the
// section and the two may be released in either order. Dropping that CloseHandle
// is caught loudly — around forty tests fail, because an open section handle
// blocks SetEndOfFile and every shrink on the platform then fails. What they all
// report is "The requested operation cannot be performed on a file with a
// user-mapped section open", from inside a truncate, which names the symptom and
// not the cause; the first version of this test was itself misled by it and died
// in its own fixture setup before reaching an assertion.
//
// So this one holds no index and truncates nothing: it maps and unmaps a plain
// file and counts the process's kernel handles across the run. The bound is
// deliberately loose, because handle counts move for reasons unrelated to this
// test — runtime threads and timers, and the os.File opened and closed per
// iteration. A leak is one handle per iteration, and the iterations outnumber
// the slack by an order of magnitude, so nothing this bound could miss is the
// bug it looks for.
func TestIndexMappingReleasesTheSectionHandle(t *testing.T) {
	const iterations = 300

	path := tempDir(t) + "/handles.idx"
	require.NoError(t, os.WriteFile(path, make([]byte, 4*entryWidth), 0666))

	// Warm up first: the first mapping pulls in whatever the runtime needs to
	// do it, and counting that as growth would make the baseline the low point
	// of the run rather than its resting state.
	for i := 0; i < 10; i++ {
		mapAndUnmapOnce(t, path)
	}

	before := processHandleCount(t)
	for i := 0; i < iterations; i++ {
		mapAndUnmapOnce(t, path)
	}
	after := processHandleCount(t)

	require.Less(t, int(after)-int(before), iterations/10,
		"handle count grew from %d to %d over %d map/unmap cycles: the section "+
			"handle from CreateFileMapping is not being closed", before, after, iterations)
}

func mapAndUnmapOnce(t *testing.T, path string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0666)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()

	m, err := mapIndexFile(f)
	require.NoError(t, err)
	require.NotEmpty(t, m, "the fixture must be non-empty to be mappable")
	require.NoError(t, unmapFile(m))
}

// processHandleCount reports how many kernel handles this process holds.
// GetProcessHandleCount is not in syscall, so it is resolved by name.
func processHandleCount(t *testing.T) uint32 {
	t.Helper()
	proc := syscall.NewLazyDLL("kernel32.dll").NewProc("GetProcessHandleCount")
	self, err := syscall.GetCurrentProcess()
	require.NoError(t, err)
	var count uint32
	r, _, errno := proc.Call(uintptr(self), uintptr(unsafe.Pointer(&count)))
	require.NotZero(t, r, "GetProcessHandleCount failed: %v", errno)
	return count
}
