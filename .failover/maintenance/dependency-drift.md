---
name: Dependency drift
interval: 30d
after: 8
paths: go.mod, go.sum
---
Review dependencies for updates that matter: security advisories first, then versions far enough behind
that a later jump would be painful. `go list -m -u all` shows what has moved.

Upgrade only what you can verify with the full suite in the same run, and run the fuzz targets briefly
alongside it for anything touching compression, mmap, or atomic file writes — this log's dependencies are
in the data path, not the periphery, so a subtle behaviour change is a corruption bug rather than a
compile error.

`gommap` deserves particular care: it is pinned to a commit rather than a release, it has no internal
locking around its handle registry, and this repo works around that. Read its changes rather than trusting
the version number.

File anything needing a real migration as an idea instead of starting it here.
