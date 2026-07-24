# Changelog

A file-backed write-ahead log with block compression and transaction-aware
compaction. Extracted from [liftbridge-io/liftbridge](https://github.com/liftbridge-io/liftbridge)'s
internal commitlog package in June 2024; this changelog covers the standalone
library from that fork onward.

## v0.19.0 — 2026-07-24

- **Added**: `CommitLog.NewScanReader(offset)` — a reader for sweeping a static
  range that returns EOF when it drains rather than parking for appends that
  may never come. The readers from `NewReader` are *tailing* readers, so
  reaching the end of the data is not an end condition for them and a bounded
  sweep that expects to finish there hangs instead. That is not hypothetical:
  it is how `RecoverTail` could hang before v0.18.0, and a consumer hit the
  same shape independently in its own abort scan and lost its compactor to it.
  The reader already existed internally; this names it, because the consumer
  was reconstructing it in three separate places.

  Two contract details, both documented on the interface and pinned by tests
  because both bite: the terminating EOF is **wrapped** (compare with
  `errors.Is`, not `==`), and a start offset with no segment behind it is
  **refused at construction** with `ErrSegmentNotFound` rather than handed back
  as a reader that instantly ends — "this range was dropped by retention" and
  "this range held nothing" are different answers, and a rebuild that scanned
  no data at all should not look like one that found none.

  Minor rather than patch: it adds a method to the `CommitLog` interface, which
  breaks anything else implementing it.
- **Tests**: `FuzzOffloadCompactionRetention`, the offload analogue of
  `FuzzCompactionRecovery` — offload interleaved with compaction and retention
  across crash and reopen, asserting latest-per-key survival, read
  transparency, and that no store object outlives the segment it belonged to.
  Clean over a 6-minute sweep; wired into the per-push smoke matrix and the
  nightly deep sweep.
- **Tests**: the compaction fuzz oracle now asserts the marker/strip invariant
  that v0.18.2 fixed. It could not have caught that bug before — the harness
  drove the workload but never checked the property. Reverting the fix now
  fails on the existing seed corpus.

## v0.18.2 — 2026-07-24

- **Fixed (correctness)**: a clean that ran out of rewrite budget could orphan a
  transaction's records. `classify` may only remove a control marker below
  `StripBelow` because the records that marker governed are stripped to plain
  records in the *same pass* — otherwise a reader buffers them waiting for a
  marker that no longer exists, or releases them on a later transaction's
  marker. That promise spans segments while the budget applies rewrites one
  segment at a time, and a marker's segment can be denser than the segment
  holding its records, so density order rewrote it first. Second instance of the
  v0.18.1 hazard, found by generalising it: an expired tombstone and a control
  marker both *govern* records at lower offsets, so both now share one rule —
  segments performing either removal are rewritten last, in ascending order.
- **Fixed (Windows)**: the high-watermark checkpoint and `PutSidecar` could fail
  with `cannot replace ...: Access is denied` and take the process with them.
  `ReplaceFile` is refused while any other handle to the destination is open,
  which under a kill/restart cycle is routinely a process on its way out or a
  scanner that opened the file after the previous write. Both writers now retry
  on a bound (25 × 20ms); a real conflict never clears, so it still fails, and
  promptly. The payload is buffered before the first attempt — the writer
  consumes its reader, so an unbuffered retry would have replaced the checkpoint
  with an empty file.
- **Changed**: the tombstone-GC rewrite ordering from v0.18.1 now applies the
  minimum constraint instead of a blunt one. Only segments that actually drop a
  tombstone give up density ordering (they go last, ascending); every other
  segment keeps it. The safety property is unchanged — a segment dropping a
  tombstone is still rewritten after every segment holding a copy it shadows —
  and `TestCleanSpecBudgetedPassCannotResurrect` still fails when the
  comparator is reduced to plain density.

  Recorded honestly: this was motivated by a consumer reporting their
  integration suite going 77s → 202s on v0.18.1, which they have since
  **retracted** — re-measuring gave 65s, faster than the 77s that preceded the
  fix, and the 202s was seed variance. So v0.18.1's ordering cost nothing
  measurable and this change fixes no observed regression. It is kept because
  constraining only what safety requires is the better shape, not because the
  broad version was slow.

## v0.18.1 — 2026-07-24

- **Fixed (data loss)**: a clean that ran out of rewrite budget could bring a
  deleted value back. Tombstone GC is the only drop that removes a key's
  *newest* copy, so it is the only one whose rewrite order matters, and
  rewrites were spent in drop-density order. If the budget stopped the pass
  after rewriting the tombstone's segment but before a segment holding a copy
  it shadowed, the tombstone was gone and the older copy became latest-per-key
  on the next pass — permanently, since nothing superseded it any more. Live
  for any caller combining `CleanSpec.RewriteBudget` with tombstone retention.
  Passes that GC now spend the budget in ascending segment order: a GC'd
  tombstone always sits at a segment index at or above every copy it shadows,
  so a budget cut leaves either the shadowed copies already dropped or the
  tombstone still there to shadow them. Density ordering is unchanged for
  passes that do not GC. Found by an extended fuzz sweep; the crashing input
  ships as a corpus seed.
- **Tests**: `TestCleanSpecBystanderKeySurvives` and
  `TestCleanSpecCeilingAboveUndecidedLosesKey`. The existing spec tests each
  drive one drop path and assert what it removes; neither direction asserted
  that a record no path selects survives, nor that the drop paths can *compose*
  to remove a key entirely. They can — but only across two passes, and only when
  the earlier pass ran with a `Ceiling` above a still-undecided record. Within a
  single pass it cannot happen: `mergeDigests` excludes aborted offsets from
  candidacy before choosing the latest, so an aborted record never supersedes
  anything. The two subtests are the same records and the same abort, differing
  only in where the first pass's ceiling sat.
- **Docs**: `tla/README.md` now records that `Ceiling` is an *input* the specs
  assume to be the transactional LSO. A caller that advances one past an
  undecided record is outside the modelled state space, so no amount of model
  checking there will find that class — the specs prove the engine honours its
  contract, not that the contract is supplied correctly.
- Fixed a stray `%s` in the compaction debug log: `slog` does no format
  substitution, so it rendered literally.
- **CI**: GitHub Actions — test matrix (ubuntu/windows/macos), the declared
  `go 1.22` floor, race, gofmt/vet/`go mod verify`/actionlint, and a bounded
  fuzz sweep per target. Plus a `workflow_dispatch` TLA+ workflow that asserts
  each negative control violates its *own* named invariant, and dependabot for
  the action pins and Go modules.

## v0.18.0 — 2026-07-24

- **Fixed**: a crash could leave the active segment's log physically AHEAD of
  its index — the append path writes a log frame *before* its index entry, and
  `checkpointHW` fsyncs only the log backing. On reopen the segment took its
  tail from the stale index and under-reported it: `NewestOffset` was too low, a
  seek and a sequential uncommitted scan disagreed about which record an offset
  names, and the next `Append` landed on an existing un-indexed record, silently
  shadowing it. `reconcileIndexTail` now rebuilds the missing entries on open by
  scanning the log past the last indexed record, leaving a torn frame for
  `RecoverTail`. No-op when the index already covers the log.
- **Fixed**: `RecoverTail` could hang instead of healing. It forward-scanned
  with an uncommitted reader on `context.Background()`, which parks in
  `waitForData` forever once the readable bytes drain — so if the reconstructed
  LEO ever overshot the log on disk, recovery blocked rather than repairing.
  Recovery now scans a static tail through a no-wait reader that returns
  `io.EOF` the instant data drains, and treats the drain like a torn suffix.
  Existing guarantees unchanged: extend past a stale checkpoint via a CRC-good
  forward scan, drop a torn suffix, never recover below the checkpoint.
- **Tests**: seeded compaction property/fuzz harness and an offload
  crash-consistency fuzz harness (transparency, reopen, fault injection,
  index-cache race stress, and `RecoverTail` against a torn active tail).
- **Tests**: TLA+ specs for the core log, transaction-aware compaction, and
  tiered-storage offload, each TLC-verified with a deliberately broken variant
  to show the invariants discriminate. A fidelity audit against the
  implementation corrected the compaction ceiling boundary in the model.

## v0.17.0 — 2026-07-23

- **Added**: tiered storage, part two — the segment *index* is offloaded
  alongside its log, fetched read-through, and held in `RemoteIndexCache`, a
  process-wide LRU with pin counts so an index cannot be evicted out from under
  a live seek.

## v0.16.0 — 2026-07-23

- **Added**: tiered storage, part one — sealed segments can be offloaded to a
  `SegmentStore` (`OffloadBefore`, `FileSegmentStore`) and are read back
  transparently, so an offloaded segment is indistinguishable from a local one
  at the read API.
- Internal: segment log I/O routes through a `segmentBacking` seam, which is
  what makes the remote backing substitutable.

## v0.15.1 — 2026-07-23

- **Fixed**: `Delete()` could fail on Windows when a reader was concurrently
  mid-read — the reader still held the segment mmap/handle for a moment after
  the log's own segments were closed, so the immediate `os.RemoveAll` raced its
  release and failed with "being used by another process". The removal is now
  retried briefly (the reader releases as its `ReadMessage` observes the
  deletion); on Unix the first attempt still succeeds, so there is no added cost.
- **Tests**: fixed two flaky tests (surfaced only under full-suite scheduling on
  Windows) — `TestReaderLogDeleted` (the Delete race above, plus a stray
  `require` inside a goroutine) and `TestCompressedOperationalEquivalence` (its
  torture appended real-time timestamps and GC'd tombstones at nanosecond
  retention, making GC decisions depend on coarse timer-tick alignment; the
  torture now uses deterministic timestamps).

## v0.15.0 — 2026-07-20

- **Changed (format break, clean cutover)**: block headers carry a
  `BlockFormatVersion` byte, so a segment describes itself. A magic byte
  alone answers "is this a block?" but not "is this a block I
  understand?", which is what a reader must know before it applies data.
  Readers refuse an unrecognised version with `ErrBlockFormat`.
  Pre-version segments are NOT supported — there is deliberately no
  compatibility path, so an existing store must be rebuilt.
- Why: it lets a consumer PROBE each component's own bytes at startup
  instead of consulting a side manifest. A manifest is a second source of
  truth and can disagree with what it describes (restore a mixed backup
  and it claims one version while the segments hold another); bytes
  cannot lie that way.

## v0.14.1 — 2026-07-19

- **Fixed**: clean-pass scans and rewrite installs retained a zstd decode
  buffer pair in every segment's block cache — O(segments) heap per pass,
  observed as 1.0–1.5GB transients and a baseline that ratcheted with segment
  count. Scans now carry one pass-shared cache (entries keyed by
  `(segment, physStart)`), the open-time last-frame recovery decodes
  transiently, and segment caches fill only for real reader traffic.

## v0.14.0 — 2026-07-19

Simplification release; breaking API cleanup with no behavioral changes.

- **Breaking**: `CleanSpec.MaxRewrites` removed — `RewriteBudget` (time) is
  the one production rewrite bound.
- **Breaking**: the spec-less `Compact()` wrapper removed; `CompactSpec` is
  the entry point.
- **Breaking**: `Dir()` replaced by a first-class client-sidecar API:
  `PutSidecar` (atomic write) / `GetSidecar` / `RemoveSidecar`.
- **Changed**: the rewrite budget is now *spent* in drop-density order, not
  just allocated by it; epoch assignments are collected per rewrite and
  applied in ascending-offset assembly.
- **Changed**: shared `blockWriter` / `decodeBlock` / `rewriteBudget`
  internals; `digestIter` collapsed to a single reader path; a failed block
  decode now invalidates the block cache entry instead of leaving it
  serving scribbled data.

## v0.13.7 — 2026-07-18

- **Changed**: budgeted rewrites select candidates by drop density (most
  reclaimable segments first).
- **Fixed**: the block cache recycles its decode buffers on displacement
  instead of abandoning them to the GC (~276MB pending collection measured
  during one clean pass over a ~1200-segment log).

## v0.13.6 — 2026-07-18

- **Fixed**: digest-merge readers use 8KB buffers; the k-way merge holds one
  reader per segment, so the previous 64KB buffers multiplied to ~79MB
  across a large log.

## v0.13.5 — 2026-07-17

- **Added**: `CleanSpec.RewriteBudget` — time-bounded rewrites, so a clean
  pass always finishes inside a short-lived process's kill window while
  reclamation scales with inflow.

## v0.13.4 — 2026-07-17

- **Added**: consolidation-only maintenance pass for non-compacted
  block-mode logs (tiny-block cleanup without compaction semantics).

## v0.13.3 — 2026-07-17

- **Added**: incremental cleans — `CleanSpec.MaxRewrites` budgets how many
  segments one pass may rewrite (superseded by the time budget in v0.13.5,
  removed in v0.14.0).

## v0.13.2 — 2026-07-17

- **Fixed**: the consolidation veto keys on block count vs the target
  layout, not a size floor — logs with mid-size logical blocks were never
  consolidated.

## v0.13.1 — 2026-07-17

- **Added**: segments roll at 16k blocks regardless of byte size, so
  small-append workloads stop hoarding block index entries in the active
  segment (~316MB of blockRefs measured across active segments).

## v0.13.0 — 2026-07-16

- **Added**: `CleanWithSpec` returns the pass's **verified floor** — the
  highest offset at or below which the log provably holds no transactional
  headers, control markers, or aborted records. Callers persist it to bound
  open-time rebuild scans. (Replaces unsound LSO-based floors, which lost
  the abort watermark across restarts.)
- **Fixed**: high-watermark checkpoint failures warn and retry instead of
  killing the checkpoint loop.

## v0.12.8 — 2026-07-16

- **Changed**: appends below 4KB skip the codec and are stored raw;
  compression happens at clean time when tiny blocks are consolidated.

## v0.12.7 — 2026-07-16

- **Added**: cleans consolidate tiny per-append blocks into ~256KB blocks.
  Block count (blockRefs, sparse index, decompression overhead) scales with
  the append pattern otherwise — 18.6M ~140-byte blocks holding ~900MB of
  index metadata were measured before this pass existed.

## v0.12.6 — 2026-07-16

- **Changed**: the digest merge streams keyed sections from disk instead of
  materializing every segment's keyed section in memory.

## v0.12.5 — 2026-07-16

- **Added**: `Dir()` — the log's directory, for stream-level sidecar
  checkpoints (replaced by the sidecar API in v0.14.0).

## v0.12.4 — 2026-07-14

- **Fixed**: zstd encoder/decoder memory is bounded (window/concurrency
  limits); unbounded defaults dominated heap on many-log processes.

## v0.12.3 — 2026-07-14

- **Added**: raw-vs-zstd operational-equivalence torture test.
- **Fixed**: leader-epoch cache file-handle leak.

## v0.12.2 — 2026-07-14

- **Fixed**: `RecoverTail` recovers the log's real tail after a crash
  instead of truncating to the periodic HW checkpoint — truncation
  retroactively unwrote committed, already-served records ("checkpoint
  amputation"). Only a torn suffix (power loss mid-write) is truncated.

## v0.12.1 — 2026-07-14

- **Changed**: digest-build concurrency capped at 2 — each build holds a
  transient per-segment key map, and peak memory matters more than
  parallelism on post-restart catch-up cleans.

## v0.12.0 — 2026-07-13

- **Added**: key-digest sidecars (`<base>.keys`) — per-segment sorted key
  summaries that let compaction run as a streaming k-way merge with
  per-segment drop bitsets, instead of materializing a global
  latest-per-key map (>1GB transient on large logs). Converged segments are
  skipped without reading a single record.

## v0.11.3 — 2026-07-13

- **Added**: `ActiveSegmentBase` for clean-boundary queries (callers need
  the exact upper bound of what a pass can have removed).

## v0.11.2 — 2026-07-12

- **Fixed**: compaction converges — a pass that would change nothing keeps
  the original segment instead of rewriting it every clean.

## v0.11.1 — 2026-07-12

- **Added**: `CleanWithSpec` on the `CommitLog` interface.

## v0.11.0 — 2026-07-12

- **Added**: transaction-aware compaction via `CleanSpec` — caller-provided
  ceiling, aborted-record removal, tombstone GC with retention, control
  marker drops below `StripBelow`, and decide-and-strip of transactional
  headers on surviving records.

## v0.10.4 — 2026-07-11

- **Fixed**: two data races — log maintenance is serialized (`cleanMu`) and
  gommap syscalls no longer race segment swaps.

## v0.10.3 — 2026-07-10

- **Changed**: `Append` stamps `LogAppendTime` on messages without a
  timestamp.

## v0.10.2 — 2026-07-10

- **Fixed**: `OldestOffset` on live handles; retention survives partial
  deletion failure.

## v0.10.1 — 2026-07-10

- **Fixed**: committed-reader desync on messages larger than the 64KB read
  buffer.

## v0.10.0 — 2026-07-09

- **Added**: `SyncAll` — on-demand power-loss durability barrier (fsync
  every segment + checkpoint the high watermark).

## v0.9.0 — 2026-07-07

- **Added**: `CompactMinAge` — protected compaction horizon; recent
  segments keep their full per-record history.

## v0.8.0 — 2026-07-07

- **Added**: sparse per-block offset index for compressed segments.

## v0.7.0 — 2026-07-07

- **Added**: transparent block compression (snappy, s2, zstd) — segments
  store compressed blocks while the logical byte space (offsets, framing,
  index positions) stays uncompressed-stable.

## v0.6.0 — 2026-07-06

- **Fixed**: `Close`/`Delete` join the background loops before closing
  segments (Windows: open handles made reopening the same path fail).

## v0.5.0 — 2026-07-06

- **Added**: `ReadMessageMetadata` — zero-allocation metadata scan with a
  reusable buffer, skipping CRC validation.
- **Added**: 64KB buffered sequential reads (`bufReader`) to reduce
  `ReadAt` syscalls.

## v0.4.1 — 2026-07-05

- **Fixed**: `TruncateBefore` reader deadlock on unsealed trimmed segments.

## v0.4.0 — 2026-07-05

- **Added**: `TruncateBefore` — head (prefix) truncation with sealed-segment
  deletion and boundary-segment rewrite.

## v0.3.0 — 2026-07-04

- **Fixed**: Windows crash in index `Close`/`Sync` (`FlushFileBuffers` on an
  invalid handle).
- **Fixed**: the active segment is fsynced when checkpointing.

## v0.2.0 — 2024-06-26

- **Changed**: last liftbridge dependency removed; the library stands alone.

## v0.1.0 — 2024-06-18

- Initial release: the commitlog package extracted from liftbridge into a
  standalone module.
