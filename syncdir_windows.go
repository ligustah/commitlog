//go:build windows

package commitlog

// syncDir is a no-op on Windows, and not because the guarantee is unavailable
// here — because it is already given. The rename underneath is the Win32
// ReplaceFile, whose metadata change NTFS records in its own journal before
// returning; there is no window in which the new name is visible and not yet
// recorded. There is also no handle to flush: a directory cannot be opened for
// FlushFileBuffers the way a file can, so the unix shape has nothing to map to.
//
// See the unix build for what this buys and where it is needed.
func syncDir(string) error { return nil }
