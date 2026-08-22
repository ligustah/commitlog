---
name: Dependency drift
interval: 7d
after: 3
paths: go.mod, go.sum
---
Review dependencies for updates that matter: security advisories first, then versions far enough behind
that a later jump would be painful. `go list -m -u all` shows what has moved.

Upgrade only what you can verify with the full suite in the same run, and run the fuzz targets briefly
alongside it for anything touching compression, mmap, or atomic file writes — this log's dependencies are
in the data path, not the periphery, so a subtle behaviour change is a corruption bug rather than a
compile error.

`gommap` is now used on non-Windows builds only, for `Map` and `UnsafeUnmap` — both thin syscall wrappers
touching no package state. Windows maps and unmaps through `CreateFileMapping`/`MapViewOfFile` in
`index_mmap_windows.go`, because gommap's unmap fsyncs unconditionally and that dominated teardown. So the
surface exposed to drift here is small, but read its changes rather than trusting the version number: the
type `index.mmap` is declared with is still gommap's. (It was pinned to a commit when this task was
written; it moved to the v0.0.3 release the same day, which is the newest tag that exists.)

Where the advisories actually land here: every dependency is a leaf this repo pins directly, and
`govulncheck` reports have so far been entirely STDLIB — fixed by the toolchain, not by go.mod. So the Go
version IS the dependency that matters most, and **nothing updates it on its own**. This line used to say
CI built with `go-version: stable` and therefore picked stdlib fixes up by itself; that stopped being true
on 2026-08-12, when the move to go1.27rc2 took CI off `stable` onto an explicit pin. Both workflows now
carry a hand-synced `GO_VERSION` matching go.mod's `toolchain` line, so moving it is part of THIS task and
happens nowhere else.

A prerelease pin is the sharp case, because a release candidate never receives the stdlib fix that
supersedes it — the fix ships in the release line instead. Check what the pin is, not just whether
`govulncheck` is quiet today: `go list -m -versions golang.org/toolchain` says what exists, since go.dev is
not reachable from this machine.

File anything needing a real migration as an idea instead of starting it here.
