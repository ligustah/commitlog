# What opening a log costs

Standing requirement from the user: *"Can we please not require full stream scans
on startup for any reason?"*

This is the audit answering that, as of v0.79.1. Short version: **no path in
`New()` reads log bytes proportional to the data in the log.** Every walk that
exists is confined to one segment, conditional on a derived file being missing or
damaged, and self-healing — it persists what it rebuilt, so it is paid once ever
rather than once per open.

Written down because the question keeps being worth re-asking and re-deriving it
takes an hour. If you add work to the boot path, add a row here.

## Per-segment cost at open

| step | healthy cost | walks only when |
|---|---|---|
| directory listing | one `os.ReadDir` for the whole log | — |
| `initPositions` (raw) | `Size()` + a 1-byte magic read | never — position IS the file size |
| `initPositions` (block) | one sidecar read | the sidecar is absent or describes different bytes → `scanBlocks`, one read per block **of that segment**, then the table is written back |
| `setupIndex` | two index-entry reads | the index reaches PAST its log → `rebuildIndexFromLog` over that segment |
| `setupIndex` (block, extra) | scan of the FINAL block's frames | — (bounded by one block, always) |
| `reconcileIndexTail` | O(1); the loop body runs zero times | the index stops short of its log → reads only the missing tail |
| `adoptTierManifestLocked` | skips every base offset already held locally | — |
| `resolveSegmentOverlaps` | `BaseOffset`/`NextOffset` arithmetic, no log reads | — |
| key digests | **not loaded at open** — compaction path only | — |
| `RecoverTail` | **not called by `New()`** — it is public API the embedder drives | — |

## Why the two walks stay bounded

Both derived files are written back by the thing that rebuilt them, so a walk is
a one-time cost per segment across the life of the log, not a per-open cost:

- **Block table.** `newSegment` writes the sidecar whenever `blocksWalked > 0`,
  which covers the open that FOLLOWS a crash — the process that would have sealed
  the segment died, and without this the next open walks the same chain again.
- **Index.** `seal()` flushes the index at every roll. Its job is not correctness
  — `reconcileIndexTail` at open provides that since v0.79.0 — but BOUNDING: a
  crash leaves at most one segment (the active one) with a short index instead of
  every segment that rolled since the last sync.

That second point is worth stating plainly because the code used to justify the
seal-time fsync as *the only repair*, and that stopped being true in v0.79.0.
The fsync is still correct, for the other reason. **Deleting it is safe for the
data and unbounds the recovery.**

## What enforces this

Not documentation — these are the assertions that go red if the property breaks:

- `TestReopeningASealedBlockLogWalksNoBlockHeaders` — reopens a sealed
  block-compressed log and requires `blocksWalked == 0` across every segment.
- `TestAReopenedLogFlushesNoIndexAtClose` — requires every segment to arrive at
  close with `dirtyIndex == false`. That is the index half of the same claim:
  `reconcileIndexTail` clears the mark only when it appended nothing, and it
  appends nothing exactly when its loop body never ran. Wrote nothing ⟺ read
  nothing.
- `TestAWalkAtOpenPersistsTheTableItBuilt` — the walk is paid once.

## The one thing that is NOT O(1)

A log whose segments are block-compressed and whose sidecars were never written
— a log last touched by a process that crashed before any seal — walks each
segment's block chain on the first open, and only the first. That is a header
read per block, not a decompress, and it ends with the tables on disk. There is
no version of "recover a block log with no derived state" that reads less.
