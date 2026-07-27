# Changelog

A file-backed write-ahead log with block compression and transaction-aware
compaction. Extracted from [liftbridge-io/liftbridge](https://github.com/liftbridge-io/liftbridge)'s
internal commitlog package in June 2024; this changelog covers the standalone
library from that fork onward.

## v0.26.0 — 2026-07-27

- **Added**: `segment.ReplaceOffloaded` — installs a freshly-written local
  segment as the next generation of an offloaded one (C4). This is what lets a
  tiered segment be compacted at all.

  A local rewrite gets its atomicity from a rename over the same path. A store
  has no equivalent, since `Put` overwrites unconditionally and cannot be made
  conditional, so the generation is the substitute: the new bytes go to a key
  nothing is reading, and the **marker is the commit point** that decides which
  generation the segment is — the same role it already plays for a first
  offload. A crash before the marker leaves orphaned objects nothing points at;
  after it, the segment is the new generation.

  The caches that would otherwise keep serving the old bytes are invalidated
  between the commit and the swap: without that the rewrite would appear to
  succeed while reads continued to come from the pre-rewrite window.

  **Superseded keys are returned rather than deleted.** A reader that opened the
  segment beforehand holds a backing over the old key and is entitled to finish;
  deleting underneath it would turn a rewrite into a read error. It is also why
  deletion has to be explicit rather than implied by an overwrite — a rewrite
  that empties a segment leaves the old objects with nothing to overwrite them.

  Not yet wired into the cleaners; that is C6, which removes the exclusion
  keeping them away from offloaded segments.

## v0.25.0 — 2026-07-27

- **Added**: invalidation for both of the caches that outlive the objects they
  describe (C2 and C3 of the tiered-compaction work).

  `storeBacking.Invalidate()` drops the 1 MiB read-ahead window, which was
  previously held for the backing's lifetime with no way to clear it — so an
  object that changed under a live key kept being served from bytes cached
  before the change, indefinitely. Generation-stamped keys mean a rewrite
  normally writes a *new* object and the backing keeps reading the one it
  opened; this covers the cases where an object genuinely can change under a
  live key, such as a generation-0 object or a store managed from outside
  commitlog.

  `RemoteIndexCache.Invalidate(cacheKey)` drops a cached index so the next seek
  refetches. Eviction was LRU-only, so without this a cached index outlives the
  object it describes with no size pressure that would reliably remove it, and
  seeks keep resolving against a pre-rewrite layout.

  An entry a live seek is holding is **detached rather than closed** — it stops
  being findable at once, and the last release closes it — so invalidation never
  pulls an index out from under a reader. That required `release` to take the
  entry rather than its key, since a detached entry is no longer in the map and
  its pin still has to be dropped.

## v0.24.0 — 2026-07-27

- **Added**: offloaded store objects carry a **generation** in their key
  (`<base>.g<N>.log`), recorded in the `.offloaded` marker. First of the changes
  that let a tiered segment be rewritten; see `docs/tiered-compaction.md`.

  `SegmentStore.Put` overwrites unconditionally and has no compare-and-swap
  form, so rewriting an object in place would change it under a reader already
  reading it — and, where two processes share a tier, would lose one of their
  writes with no error to either. A rewrite therefore writes the NEXT generation
  to a NEW key, which makes both hazards observable rather than silent: a reader
  holds a key that cannot change, and two uploaders racing produce two distinct
  objects instead of one corrupted one.

  **Nothing bumps a generation yet** — this release only establishes the keying
  and threads it through offload, the marker and reopen.

  Compatible with objects already in a store: generation 0 keeps the original
  un-suffixed key, and the marker — which records the key verbatim and is the
  only thing that resolves it — reports 0 when the field is absent. Both the
  JSON and the older raw-key marker forms are covered by tests, and a marker
  written now omits the field at generation 0, so it stays byte-compatible.

## v0.23.0 — 2026-07-27

- **Breaking**: the module's `go` directive moves from **1.22 to 1.26**, and
  `klauspost/compress` from v1.18.0 to **v1.19.1**. These are one decision: every
  compress release after v1.18.0 raises its own minimum, so the floor and the
  compression dependency move together.

  **This is a crash fix, not drift.** compress v1.18.6 fixes an `s2.Decode`
  SIGSEGV on amd64 when the goroutine is async-preempted
  ([klauspost/compress#1097](https://github.com/klauspost/compress/issues/1097)),
  and `compress/codec.go` calls `s2.Decode` on every S2-compressed read — the
  bug sits directly in the read path. Nothing below `go 1.24` carries that fix:
  the v1.18.x patch line raises its directive at v1.18.2 and again at v1.18.5,
  so there is no "take the fix, keep the floor" option. On the way it also picks
  up CVE-2025-61728 and the retraction of v1.18.1's invalid flate encoding.

  **The floor now tracks the Go version this library is developed on**, rather
  than the minimum a dependency happens to force or the lowest known consumer.
  That settles the next dependency to raise its own minimum too. Consumers bump
  their directive when they next update; of the known ones, `sqlcdc` is already
  past this and `durable_streams` needs a matching bump.

- **Removed**: the `minver` CI job. It built with `go-version-file: go.mod` to
  keep the declared floor honest while that floor sat well below current Go.
  With the floor tracking the toolchain in use it installs the same Go as the
  existing `stable` matrix and no longer tests anything the matrix does not.

## v0.22.4 — 2026-07-27

- **Docs (contract)**: `TruncateBefore` promised `OldestOffset() >= minOffset`
  after the call. That is false, and the same comment said why two sentences
  later: the active segment is never rewritten, so a log whose records all still
  live in one active segment frees nothing and its oldest offset does not move.
  A caller gating retention on the floor being reached would wait forever — and
  one downstream already wrote that assertion and had it fail.

  The documented guarantee is now the directional one that actually holds:
  nothing at or above `minOffset` is ever discarded, reclamation is
  segment-granular and best-effort, and retention is unpoliced. Two tests pin
  both halves.

- **Docs (contract)**: `SyncAll` described itself as `Sync` plus a high-watermark
  checkpoint. Since `Sync` became log-bytes-only it does strictly more than
  that — it flushes indexes too — so a reader comparing the two was told the gap
  was smaller than it is.

- **Docs (contract)**: `Sync` suggested passing `NewestOffset()` as an
  alternative to the offset `Append` returned. Those are not equivalent: the
  tail advances with every append, so it is never covered by a flush already in
  flight and every caller ends up leading its own, silently defeating the
  coalescing the same comment advertises. It now says to pass the offset you
  were given, and why.

  All three found by a scheduled contract-drift pass.

## v0.22.3 — 2026-07-27

- **Fixed (corruption)**: `Truncate` could rebuild the boundary segment from a
  torn read, leaving a log that could not be read end to end. Before replacing
  that segment it SCANS it to copy the records below the cut, and that scan ran
  outside the segment's lock — so an append extending the segment mid-scan left
  the copy holding a partial frame. Reproduced in roughly one run in eight of a
  concurrent append-and-truncate test.

  `Truncate` now takes the append lock, as a segment roll does: both redefine
  where an append lands.

  The near-miss worth recording is that this path looks safe. `Truncate` calls
  `Delete` or `Replace` on the very segment the appender holds, and both take
  that segment's write lock — which orders the two WRITES, and says nothing
  about the scan that precedes them. `TruncateBefore` and `Clean` are genuinely
  safe here for a structural reason instead: neither ever rewrites the active
  segment, and a sealed segment takes no appends.

  Found by a scheduled concurrency sweep, not a report.

## v0.22.2 — 2026-07-27

- **Fixed (hang)**: `Sync(offset)` never returned for an offset the log no
  longer reaches. It waits until the durable watermark covers the offset, and
  that watermark comes from the log's tail — so once retention moved the tail
  below an offset the caller was still holding, the condition could never be
  met and the call spun fsyncs forever. Reachable through the public API:
  append, `Truncate` below it, then `Sync` the offset you were given.

  Those records are gone, so there is nothing left to make durable and the call
  now returns.

  Found while auditing whether the barrier's own tests could detect a broken
  watermark. They could not — they hung, which is how the missing termination
  guarantee surfaced.

## v0.22.1 — 2026-07-27

- **Fixed (performance)**: `Sync`'s coalescing barely coalesced. It flushed the
  instant it took leadership, so a committer arriving a microsecond later was
  not covered and had to lead a flush of its own — measured, **98% of concurrent
  committers led their own**, which is no batching at all. A consumer measured
  the result as 6.5× slower than the caller-side batching v0.22.0 told them to
  retire, flat from 4 writers up. They were right.

  The leader now holds the door open before flushing, for the duration of the
  PREVIOUS flush. That self-tunes: on a fast disk the wait is short, on a slow
  one it grows and the batches grow with it, which is where batching pays.

  Measured, commits per fsync on one log: **1 writer 1.0, 4 writers 0.26, 16
  writers 0.064, 64 writers 0.019** — 53 commits per fsync, against 0.68 before.

  Two cleverer variants were tried and both measured worse, which is why the
  simple one is in: skipping the wait when nobody joined the last flush is
  self-reinforcing (with no window nobody can arrive in time to join, so it
  never re-arms) and took 64 writers back to 0.42; decaying the window by half
  instead was unstable at high concurrency, landing at 0.167.

  The regression test's bar was also wrong — it asserted "fewer fsyncs than
  callers", which the barely-coalescing version passed comfortably.

## v0.22.0 — 2026-07-27

- **Breaking / Changed**: `Sync()` becomes `Sync(offset)` — "make the log
  durable through this offset" — and now coalesces. Pass an offset returned by
  `Append`, typically the last of a commit, or `NewestOffset()` for everything
  so far.

  **Concurrent callers share one fsync.** A caller whose offset a completed
  flush already covers returns without issuing another; one whose offset a flush
  in flight will cover waits for it. So N commits landing together cost one
  fsync rather than N, which is what makes per-commit durability affordable.
  Callers should NOT batch above this — the log is the only layer that knows
  what a given fsync actually covered.

  An offset is the right shape because callers already hold one from `Append`,
  and it gives a coverage predicate a segment-shaped call cannot express.

  Measured, 24 concurrent committers on one log: **0.75 fsyncs per commit** when
  each syncs the offset it was given, against 0.91 when each asks for the log's
  current tail (never covered by a flush in flight, so every caller leads one).
  The saving grows with fsync latency — on a fast NVMe the batches are small,
  and a consumer measured ~20 callers per fsync on a slower disk.

- **Changed**: `Sync` now flushes log bytes ONLY — not the index. An index
  behind its log is a state recovery already repairs, since the append path
  writes the log frame before the index entry and open rebuilds the missing
  tail. `SyncAll` still flushes both.

- **Fixed**: a segment's index is now flushed when it SEALS. Open rebuilds a
  short index tail for the active segment only, so a segment that rolled between
  syncs could keep a permanently short index that nothing would ever repair.
  Sealing is the last moment anything can fix it: one extra fsync per roll, off
  the hot path, which confines the unflushed index to the active segment that
  open already handles and makes an offset in a sealed segment durable by
  construction.

- **Fixed**: a segment roll could run concurrently with an append and leak the
  segment it built. The cleaner loop rolls on its own ticker, independently of
  any append, and `split` builds the new segment before swapping it in — but
  "refuse if the file already exists" and "create the file" are two steps, so
  two rollers could both end up holding a segment over the SAME files. The one
  that lost the compare-and-swap was discarded with a best-effort `Delete`,
  which closes and unlinks files the winner is actively using.

  On Windows that unlink fails, the error is swallowed, and the log is left with
  a handle and mapping nothing will ever close, so its directory can never be
  removed. On POSIX it is worse and quieter: unlinking an open file succeeds, so
  the live active segment's files are removed out from under it with no error
  anywhere.

  Rolls now take the same lock appends do, which is the honest relationship
  between them — a roll redefines where an append lands. Found by auditing for
  more instances of the read-then-write shape behind the concurrent-`Append`
  bug in v0.21.1, rather than from a report.

## v0.21.1 — 2026-07-26

- **Fixed (data loss)**: concurrent `Append` calls on one log could be handed
  the SAME offset, and their records written over the same byte range. An append
  reads the active segment's next offset and position, encodes a message set
  stamped with them, and only then takes the segment's write lock — so two
  appends racing on one log both read the same tail, were both stamped with it,
  and both wrote there. The loser's records were simply gone and the offset
  sequence held duplicates, with no error returned to either caller. Measured on
  the regression test: **32 concurrent appends left 3 readable records.**

  Reading the tail and writing to it is now one atomic step, for both `Append`
  and `AppendMessageSet`. The encoding has to sit inside that critical section
  because the offsets are baked into the encoded bytes, so appends serialize
  against each other — but not against fsyncs, which is the part that governs
  throughput and is why the sync path deliberately runs outside the segment
  lock.

  The bug is old, not new. It stayed invisible because callers tended to
  serialize their own writes; one that narrowed a coarse lock of its own started
  appending concurrently for the first time and lost records immediately.
  `ConcurrencyControl` is off by default and did not protect against this.

## v0.21.0 — 2026-07-26

- **Breaking / Added**: a log now records what it IS. Its compaction-defining
  settings are persisted to a `log-descriptor` sidecar in the log directory —
  human-readable, in the style of the existing `leader-epoch-checkpoint` and
  `replication-offset-checkpoint` — and checked against the `Options` passed on
  every open. If they disagree, the log refuses to open with
  `ErrDescriptorMismatch`.

  This closes a silent data-loss path. Compaction behaviour previously lived
  only in the `Options` a caller happened to pass, so reopening a directory with
  different — or absent — options quietly changed what got deleted. A downstream
  caller reopened a compacted log with no config; `CompactMinAge` and
  `CompactTombstoneRetention` both defaulted to zero, which means *no
  protection* rather than "disabled", so compaction ran unprotected and removed
  records their replay needed. Live was fine, only reopen broke, and nothing
  errored at any point. Preferring either side silently would have kept the
  failure invisible, which is the bug — so it is an error.

  `Compact`, `CompactMinAge` and `CompactTombstoneRetention` gate the open.
  `Compression` and `MaxSegmentBytes` are recorded to describe the log but never
  gate it: both can change safely on an existing log, since segments keep the
  format and size they were written with.

  **A log with no descriptor is the same error**, which is what makes this
  breaking — every log created before this has none. Silently adopting whatever
  the caller passes is precisely the behaviour being removed. Set the new
  `Options.AdoptOptions` to record the passed options as the log's descriptor
  instead of checking against it: one explicit opt-in covering both the
  migration of an existing log and a deliberate retune. A log being created is
  unaffected — it simply records what it was created with.

  A descriptor that exists but does not parse is corruption, not a migration,
  and is reported as itself rather than being overwritten. Unknown keys are
  ignored, so a descriptor written by a newer version stays readable.

## v0.20.0 — 2026-07-26

- **Fixed (performance)**: the index flush ran while holding the mutex that
  guards entry writes, so an append to a log blocked for the duration of that
  log's index flush — the same shape as the segment-level fix below, one layer
  down, and the limiter once that landed. A consumer's mutex profile at 64
  concurrent durable commits found it underneath their own coordinator lock.

  The mapping was the obstacle: the index remaps when it expands, and a flush
  must never walk a mapping being torn down. The mapping now has its own
  lifetime lock — shared by the flush, exclusive in the unmap/remap paths —
  while the metadata mutex is held only briefly. The flush pins the mapping
  before releasing the metadata mutex, in the same order the remap path takes
  them, so it cannot be unmapped in the gap.

  An entry written during a flush may or may not be covered by it, as at the
  segment level; the caller's next sync covers it.

  Measured: entry writes during continuous flushing go from ~8 per flush to
  ~477. Per commit of one record, `Sync` 2.02 ms → 1.65 ms and `SyncAll`
  5.33 ms → 4.45 ms; across 24 concurrent writers, 1.03 ms → 0.63 ms.
- **Fixed**: closing an index returned early when its flush failed, leaving the
  index mapped, its handle open and the index marked open. A mapped file cannot
  be unlinked on Windows, so a transient flush failure became a segment that
  could never be deleted and a maintenance pass that failed identically forever
  — the same failure mode as the segment close path below, one layer down.
  The mapping and handle are now released regardless, and the flush failure is
  reported after: losing an unflushed tail is recoverable, leaking the mapping
  is not.
- **Added**: `CommitLog.Sync()` — the durability primitive, for callers making a
  commit durable. It fsyncs log and index and stops there, where `SyncAll` also
  checkpoints the high watermark: a second fsync of the active segment plus an
  atomic rewrite of the checkpoint file, roughly three fsyncs and a rename where
  durability needs one. The checkpoint is only an optimization — recovery rides
  out a stale one — so a durability caller should not have to buy it. `SyncAll`
  keeps it for the promote path, whose observers must never see the log roll
  back.

  Segments now track whether anything has been appended since their last fsync,
  so neither entry point pays per-segment fsyncs for data already on stable
  storage; a `Sync` with nothing new is free rather than an fsync per segment.
  The mark starts set, because a segment opened from disk was written by a
  process whose flush state we cannot know. It is cleared *before* the fsync, so
  an append landing mid-flush is covered by the next sync rather than lost, and
  restored if the fsync fails, so a reported failure can never leave a segment
  looking durable when its data is still in OS buffers.

  Minor rather than patch: it adds a method to the `CommitLog` interface, which
  breaks anything else implementing it.
- **Fixed (performance)**: the fsync ran while holding the segment lock, and the
  append path needs that same lock — so no append could land while a sync was in
  flight. Those appends are exactly what forms a caller-side group commit's next
  batch, so batching that coalesces correctly in isolation collapsed one layer
  down: a consumer measured 1.7× end to end where coalescing should approach the
  single-fsync floor. The sync now snapshots what to flush under the lock,
  releases it, then fsyncs. An append landing mid-fsync is not covered by that
  fsync and waits for the next one — already the group-commit contract — and the
  segment stays dirty, so nothing is lost. The index is safe to flush outside the
  segment lock because it carries its own mutex for both writes and flushes,
  which is what keeps a flush off a mapping that remap-on-expand is tearing down.

  Measured per commit of one record: `Sync` 2.0 ms against `SyncAll` 5.3 ms, and
  1.0 ms per commit across 24 concurrent writers — below a single fsync, which
  is coalescing actually happening.
- **Fixed (data loss)**: a process killed mid-compaction left its half-written
  `.cleaned` working copy on disk. Reopening skips it — `open` matches only
  `.log` — so it survived to the next maintenance pass, where the working copy
  was reopened `O_CREATE|O_APPEND` and the new rewrite appended *after* the dead
  pass's bytes, then renamed over the live segment. The digest rebuild then
  panicked on the malformed leading frame, and the panic unwound leaving the
  source segment's index still mapped, which on Windows makes the file
  unremovable — so the segment became permanently undeletable, every restart's
  first maintenance pass failed, and the log grew without bound. A working copy
  now starts empty; it holds no committed data until its rename, so discarding a
  leftover is always safe.
- **Fixed**: the segment close path bailed out after a failed log close, skipping
  the index close and leaving the index mapped and the segment marked open —
  reaching that same undeletable state by a second route. Both halves are now
  closed before either failure is reported, and an already-closed half counts as
  success, matching the sync path: a maintenance pass can reach a segment another
  pass just closed, since rewrites run outside the log mutex.

## v0.19.1 — 2026-07-25

- **Fixed**: `CommitLog.SyncAll` aborted the whole sync — skipping the still-open
  active segment — when it hit a segment closed concurrently by `Clean`, which
  rewrites and closes segments OUTSIDE the log mutex. Such a segment is already
  durable, so `SyncAll` now skips it on `os.ErrClosed` and keeps syncing the rest.
  This surfaced downstream as a spurious `sync ...: file already closed`, exposed
  by durable_streams' shared-coordinator model (many transactional producers
  sharing one coordinator's txLog while maintenance runs); the
  per-producer-coordinator layout made the overlap rare.

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
