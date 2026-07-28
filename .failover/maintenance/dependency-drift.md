---
name: Dependency drift
interval: 5d
after: 3
paths: go.mod, go.sum
---
Review dependencies for updates that matter: security advisories first, then versions far enough behind
that a later jump would be painful. `go list -m -u all` shows what has moved.

Upgrade only what you can verify with the full suite in the same run, and run the fuzz targets briefly
alongside it for anything touching compression, mmap, or atomic file writes — this log's dependencies are
in the data path, not the periphery, so a subtle behaviour change is a corruption bug rather than a
compile error.

`gommap` deserves particular care: it has no internal locking around its package-level handle registry, and
this repo works around that with `gommapMu` in index.go. Read its changes rather than trusting the version
number. (It was pinned to a commit when this task was written; it moved to the v0.0.3 release the same day,
which is the newest tag that exists.)

Where the advisories actually land here: every dependency is a leaf this repo pins directly, and
`govulncheck` reports have so far been entirely STDLIB — fixed by the toolchain, not by go.mod. CI builds
with `go-version: stable` so it picks those up on its own; a stale local toolchain does not. Check the Go
version before concluding the repo is exposed.

File anything needing a real migration as an idea instead of starting it here.
