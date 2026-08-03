# Changelog

A file-backed write-ahead log with block compression and transaction-aware
compaction. Extracted from [liftbridge-io/liftbridge](https://github.com/liftbridge-io/liftbridge)'s
internal commitlog package in June 2024; this changelog covers the standalone
library from that fork onward.

## v0.50.1 — 2026-08-04

- **Fixed**: one read spanning a truncation could return the same offset twice,
  or go backwards. Regression in v0.50.0; **v0.50.0 should not be used**.

  A reader that finished a segment advanced with
  `findSegmentByBaseOffset(r.seg.BaseOffset+1)` — *the next segment whose base is
  above mine* — and reset to that segment's position zero. That query is wrong
  whenever a segment can be replaced by one with a **higher** base offset
  covering a **suffix** of the same range, which is exactly what
  `TruncateBefore`'s boundary trim is: a source holding `0..5` becomes a trim
  holding `4..5`. A reader that had served `5` was handed the trim and restarted
  it at its base, serving `4` next.

  Reported from durable_streams as a single `ReadWithOffsets` batch that was not
  monotonic — `4 after 5`, `242 after 242`. The most useful part of that report
  was what did *not* break: every record served was genuine and self-consistent
  with the offset it came back at. That ruled out a bad index, a stale cached
  seek and a torn frame, and left only the reader re-visiting a range it had
  already walked.

  The reader now advances to the segment holding its own next offset. A trim
  excludes itself for free — it ends exactly where its source did, so the source's
  next offset is not inside it — and resolution still goes through `current()`,
  so a replacement installed by a compaction rewrite is followed as before.

  The v0.50.0 locking rework made this reachable rather than causing it. Publishing
  the new segment list *before* unlinking the source leaves a window where the
  source is still live and readable and its trim is already published, so an
  ordinary scan can walk from one into the other. Under the old all-under-`l.mu`
  ordering the source was always closed before the trim became reachable, and the
  reader took the `ErrSegmentReplaced` path instead — which re-resolves by offset
  and lands correctly. The bug was latent in the reader the whole time.

## v0.50.0 — 2026-08-03

- **Fixed**: a truncation stopped every read and every append on the log for as
  long as it took.

  `TruncateBefore` held `l.mu` — the lock `Segments()` RLocks and `split()`
  Locks — across all of its file I/O: every segment close, every unlink, and
  then a scan of the boundary segment end to end and the write of a whole new
  one. Retention is meant to be background work, and this made it a hard stop
  for the whole log. Reported downstream as a 10-minute test timeout whose stack
  was one truncator inside a Windows `FlushFileBuffers` with the writer and
  every reader queued on the mutex behind it. Not a deadlock — a convoy.

  It now decides under the lock, does the file work with it released, and
  re-takes it only to publish the surviving list. Same discipline
  `CleanWithSpec` already used. Against a log of 500 small segments: reads
  completed during the truncation went from **0 to ~41,000**, at roughly 60% of
  the rate they run at when nothing is truncating.

  Two things follow from letting go of the lock, and the fix handles both. An
  append can roll a new segment while it is down, so what `split()` appended has
  to be carried into the published list rather than spliced away. And `Close()`
  does not take `cleanMu`, so the log can close underneath the rewrite;
  publishing into a set `closeSegments` has already walked would leave the trim
  with nothing to close it.

  A failure part-way through is also better behaved than it was. The scan and
  the rewrite now happen before anything is deleted, so a boundary segment that
  cannot be read leaves the log exactly as it was found — previously the
  obsolete segments below it had already been unlinked, and the call returned an
  error having left `l.segments` naming files that were gone.

  Together with the `fsync`-before-unlink fix in v0.49.0, this is both halves of
  durable_streams' second report.

- **Fixed**: `Truncate` stopped every read for the length of the call, the same
  way `TruncateBefore` did.

  It held `l.mu` across the scan of the boundary segment, the write of its
  replacement, and the unlink of every segment above the cut. A follower
  reconciling after an unclean election can be told to cut a long way back, so
  that is not a small amount of work. Measured on a log of 500 small segments:
  reads completing during the call went from **0 to ~41,000**, and the call
  itself got faster — 1.94s to 0.86s — because it was no longer contending with
  the readers it had queued up behind itself.

  `appendMu` is still held for the whole call and that has not changed. The
  boundary scan runs outside the segment's own lock, so an append extending the
  segment mid-scan tears the copy; appends are meant to wait here. Holding it
  also makes this simpler than the `TruncateBefore` fix — nothing can roll a
  segment underneath the call, so the list at publish time is the list that was
  snapshotted and there is no rebase to do.

  One deliberate difference from `TruncateBefore`: the unlinks happen *before*
  the publish rather than after. The records above the cut are the ones the call
  exists to make unreachable, so the window in which they can still be served has
  to be the earliest available, not the latest. Deleted-but-still-listed is
  exactly the mid-pass state `current()` is written for.

- **Fixed**: a log recreated at a deleted log's path read the dead log's index,
  and seeks landed on the wrong record.

  `RemoteIndexCache` is process-wide and outlives any one log. It was keyed by
  the segment's log path and base offset — unique across every log in a process,
  but not across *time*. Delete a log's directory, create a new log at the same
  path, and its segments restart at base offset 0 and produce byte-identical
  keys. `acquire` then returned the dead log's index on a hit without consulting
  the store at all. An index says "the record at offset X is at byte position
  P"; applied to a different log's bytes that is not a stale answer, it is a
  wrong one. Reported from durable_streams against v0.48.0: a read asking for
  offset 5 began at offset 7, in order, with no error.

  The cache is now keyed by the index **object key**, which it was already being
  handed. `newStoreKeys` mints a fresh 128-bit random id for every upload attempt
  and `openOffloadedSegment` takes the key from the offload marker verbatim, so
  it is unique across logs and across incarnations by construction. That is
  strictly better than the two shapes the report proposed: no nonce to create and
  persist alongside the log directory, and no `InvalidatePrefix` for a caller to
  remember to call — a rule that only the callers using option-2 tiering would
  ever discover they were supposed to follow. `segment.indexCacheKey()` is gone,
  and `acquire`, `fetch` and `Invalidate` lost their now-redundant parameter.

  `withIndex` also now treats an empty `indexKey` as corruption rather than
  passing it through. It is what the cache is keyed by, so an empty one is not a
  miss to paper over — it would collide with every other segment in the same
  state.

  Reachability: needs `WithRemoteIndexCache` plus a delete-and-recreate of the
  same name within one process lifetime, before eviction clears the entry. A
  name being reused is ordinary, not exotic.

  Verified both ways by hand rather than by `hack/guardcheck.sh`: the fix deletes
  a derived key and reuses a parameter that was already there, so there is no
  single line to remove, and adding an indirection purely to make one
  neutralisable would make the code worse than the bug did. With the key put back
  to a base-offset-derived one,
  `TestARecreatedLogDoesNotSeekWithTheDeletedLogsCachedIndex` fails at the second
  generation's first probe with a CRC error — the first generation's byte
  positions landing mid-record — while the first generation stays green
  throughout.

- **Internal**: two guards that cover `TruncateBefore` were re-anchored, and one
  of them gained a new test.

  `hack/guardcheck.sh` removes each guard and requires the test named for it to
  go red. Both of these named code the rewrite above had moved, so they reported
  `SKIP` — pattern not found, which is the one failure mode that looks like
  nothing being wrong.

  The copy-on-write guard just moved. The redirect guard —
  `boundary.SupersededBy(trimmed)`, which is what stops a reader following a
  deleted boundary into nothing — needed more: the chaos test it named stopped
  covering it. Checked both ways, that test fails in under a second without the
  link on the previous commit and passes eight consecutive runs without it on
  this one. The hazard is unchanged; the window narrowed, because truncation now
  publishes the new segment list *before* it unlinks, so a reader that
  re-resolves finds the trim already published and never consults the boundary.
  Only a snapshot older than the publish still reaches it, and chaos cannot
  manufacture one on demand. `TestAStaleSegmentSnapshotFollowsATrimmedBoundary`
  takes that snapshot deliberately instead. All 20 guards are covered again.

## v0.49.0 — 2026-08-03

- **Fixed**: a log reopened after a crash inside `TruncateBefore` served records
  twice.

  Trimming the boundary segment writes the surviving records into a new file at
  a higher base offset and then deletes the source. Those are two steps, and a
  crash lands between them: the trim has been renamed into place, the source is
  still there, and the directory holds two `.log` files whose offset ranges
  overlap. `open()` had no notion of overlap and took both, so a read walked the
  source to its end and then began the trim again in the middle — offsets came
  back twice, in order, with no error anywhere. Confirmed as `0..7` then
  `6,7,8,9`.

  `open()` now drops any segment whose records the one before it already holds.
  The trim is a strict suffix of its source, so the source alone is a complete
  log; dropping the trim un-does an unfinished truncation the caller can simply
  run again. Dropping the source instead would be wrong despite it being the
  older file — the segments below the boundary are deleted one at a time before
  the trim is written, so a crash can leave some of them standing, and removing
  the source's low records then opens a hole in the middle of the log.

  An overlap where neither segment contains the other cannot be produced by any
  path in this package, since every other rewrite renames over its source and so
  keeps the name. That one is reported rather than repaired: serving an offset
  twice is the failure being fixed here, and guessing at a partial overlap would
  only choose a different way to do it.

  Found while reading durable_streams' report on `TruncateBefore` holding `l.mu`
  across its I/O, and it is the reason the deletes have not simply been moved
  outside the lock: publishing the surviving list before doing the file work
  widens this window rather than closing it.

- **Performance**: deleting a segment stopped `fsync`ing the index it was about
  to unlink.

  `segment.Delete` closes before it removes, and closing an index syncs and
  shrinks it. Both are durability work, and durability is meaningless for bytes
  that are about to stop existing — the flush pushed an index to stable storage
  microseconds before the file was removed. On Windows each is a blocking
  syscall (`FlushFileBuffers`, `SetEndOfFile`), and a retention pass dropping N
  segments paid both N times, inside the log's write lock.

  The delete path now closes without either. It still releases the mapping and
  the handle, which is not optional: a mapped index cannot be unlinked on
  Windows at all.

  Measured over ~225 segments: **1.27s → 0.79s**. The first half of
  durable_streams' second report.

## v0.48.0 — 2026-08-03

- **Fixed**: a read from a published retention floor came back past it.

  `TruncateBefore(f)` keeps `f`, so a floor that has been published and not
  raised since must stay readable. A reader opened at `f` came back at `f+1`,
  `f+2` or `f+3` while `OldestOffset()` still answered `f` — the log asserting a
  record is there while a read from it starts past it, with no error anywhere on
  the path. Silent data loss rather than a retention decision.

  The trim itself was never wrong; what was missing is a link. `current()` tells
  a redirect from a deletion by the LINK, not by the flags: a segment that is
  gone WITH a replacement sends the reader to the replacement, and one that is
  gone WITHOUT one means retention collected those records, so skip to the next
  segment. `Replace` records that link as part of renaming a rewrite over its
  source. A boundary segment trimmed at a new base offset cannot take that path
  — its replacement is a differently named file, with nothing to rename over —
  and `TruncateBefore` deleted the source without recording the link. So a
  reader already resolved into the boundary got the retention answer and skipped
  the records the trim had just preserved: one segment's worth, which is exactly
  the 1-3 record stride reported.

  Reported from durable_streams against v0.47.0 with a pure-commitlog repro,
  where it had been carried as an unreproducible CI sighting for weeks — a
  consumer resuming at its own published floor silently skipping the first
  records after it. `Truncate` was never affected: it installs its rewrite with
  `Replace`, which records the link.

## v0.47.0 — 2026-08-03

- **Fixed**: compaction erased the log's leader epoch, and every follower of a
  compacted stream was fenced out permanently.

  A clean `Replace`d the live epoch cache with one the compactor rebuilt from
  the per-record epoch stamps of the surviving records. That cache could only
  ever be a subset of the real one, and on a leader it was empty: the only
  writer that stamps a record with an epoch is the follower path taking a
  leader's framing verbatim. `Append` writes 0, and `NewLeaderEpoch` writes to
  the checkpoint file and nowhere else.

  So one ordinary maintenance pass took `LastLeaderEpoch()` from 3 to 0.
  `LastOffsetForLeaderEpoch` still answered, but only through the fallback that
  fires when there is no history at all. Downstream that epoch is the
  replication fence, so every follower's fetch was refused, the follower did as
  it was told — re-probe, truncate, refetch — with the same epoch, and was
  refused again, forever. It needs no race and no failure: a stream with
  `Compact: true` and more than one replica is enough, which in a cluster is the
  ordinary configuration. Reported from durable_streams with a pure-commitlog
  repro, now a test here.

  A clean removes records. It does not renumber them and it does not change when
  a leadership began, so every entry the live cache holds is still true
  afterwards, and the only ones needing attention are those anchored below the
  surviving floor. `ClearEarliest` already does exactly that — re-anchoring the
  newest of them at the floor rather than dropping it — and is what the
  retention path has always used. The compaction path now uses it too, and
  nothing else touches the cache.

  This also fixes the two narrower versions of the same hole: an epoch assigned
  to an empty log (start offset -1, below every base offset), and an epoch whose
  records have all been compacted away, which was lost even on a follower whose
  records do carry stamps.

- **Changed**: the epoch offsets a clean leaves behind are where each leadership
  BEGAN, not where each epoch's first surviving record sits. An epoch that began
  at 5 keeps its anchor at 5 even when compaction leaves only offset 9. This is
  also the safe answer for the follower asking: told 5 it rolls back everything
  from 5 on, whereas 9 would tell it to keep local copies of 5..8 that the
  leader no longer has and so could never correct. A test asserted the old
  answers and has been updated.

- **Removed**: `leaderEpochCache.Replace` and `Rebase`, the epoch cache threaded
  through `compact`/`CompactSpec`/`cleanSegment`, and the per-rewrite epoch
  collection. All of it existed to build the cache that is no longer used.
  `Replace` in particular is the primitive that made this bug possible; the
  cache is now only ever added to or trimmed at an end.

- **Changed**: the key digest sidecar is format v2 — the leader-epoch section is
  gone. A v1 sidecar is not recognized, and an unrecognized digest has always
  been rebuilt from a segment scan rather than treated as an error, so this
  needs nothing from callers.

## v0.46.0 — 2026-08-03

- **Fixed**: a truncation that cut below the high watermark left the watermark
  naming records it had just removed, and the log then served nothing at all.

  `Truncate` is allowed to cut below the watermark — a follower reconciling
  against a leader promoted from OUTSIDE the ISR is told to discard records it
  had locally committed, which is the whole point of an unclean election and not
  something the log should refuse. Reported by a consumer as reachable through
  exactly that path, in a rollback that does not touch the watermark afterwards.

  What made it more than untidy is that the state could not be escaped. The
  watermark resolves through `findSegment`, which returns nil past the last
  segment, so every committed reader failed to build: the log served no
  committed reads whatsoever. `SetHighWatermark` is monotonic and silently
  ignores a smaller value, so the obvious repair does nothing;
  `OverrideHighWatermark` was the only way and its documentation said it was for
  unit tests. And nothing was raised at the call that caused it — the log
  recovered on the next reopen, where a checkpoint above the log is clamped, and
  not before.

  `Truncate` now clamps the watermark itself, with the same warning the reopen
  path logs. That is the log's own stated principle applied one call earlier:
  the records are not there, and a log does not get to keep asserting they are.
  A truncation ABOVE the watermark — the ordinary `Truncate(HW+1)` — leaves it
  untouched, since the watermark is still true.

  Callers that truncate below the watermark no longer need to pair the call with
  anything.

- **Changed**: `SetHighWatermark`'s monotonicity and `OverrideHighWatermark`'s
  purpose are now documented. Both were load-bearing and unwritten, and the
  latter's "used for unit testing purposes" was actively wrong.

## v0.45.0 — 2026-08-03

- **Added**: `AtomicWriteFileWithRetry`, which was `atomicWriteWithRetry`. Same
  implementation, now exported, requested by a consumer that hit the identical
  Windows failure writing its own config file beside a log.

  It writes a file atomically and retries briefly, and the retry exists for one
  platform reason — which is why it is in the name. On Windows the underlying
  `ReplaceFile` fails with `Access is denied` when any open handle to the
  destination was not opened with `FILE_SHARE_DELETE`. That handle need not be
  yours: a virus scanner or the search indexer opening the file after your
  previous write is enough, as is a process that has just exited and not yet
  been reaped. The condition clears in milliseconds; a real conflict — a second
  live writer, a read-only file — never does, so the bound of 25 attempts 20ms
  apart keeps that case failing rather than hiding it behind a stall. On Unix
  rename is atomic, the first attempt always succeeds, and nothing is added.

  The doc comment now says explicitly that buffering the payload up front is
  load-bearing rather than incidental, because that is the part a reimplementation
  gets wrong: a retry must write the SAME bytes, and the underlying `WriteFile`
  consumes the reader, so a version that streams instead of buffering replaces
  the file with nothing — silently, and only on the path the helper exists for.
  Exporting it rather than letting the second copy exist is the point.

## v0.44.2 — 2026-08-03

- **Fixed**: retention wrote into a segment slice that readers were already
  holding — a data race on the log's own backing array, reported downstream as
  red under `-race` in a deletion and a truncate chaos test.

  `Segments()` returns the slice header rather than a copy, deliberately: it is
  called on the path of every read, and copying there would allocate per call.
  The price of that choice is an obligation on the writing side — whoever
  changes the segment set publishes a NEW array instead of writing into the one
  readers are indexing. `Truncate` has always done that. `TruncateBefore` did
  not: it replaced its boundary segment with `l.segments[boundaryIdx] = trimmed`
  in place, under `l.mu`, which readers do not take.

  That is an unsynchronised write against concurrent reads of the same memory,
  so which of the two segments a reader observes is unordered with respect to
  the state of the segment it then uses — and one of the two candidates is on
  its way to being deleted. The reads are lock-free by design, so nothing on the
  read side could have made this safe; the fix is not to write there at all.

  `TruncateBefore` now snapshots, edits the snapshot, and publishes it, exactly
  as `Truncate` does. There are no remaining in-place element writes to
  `l.segments` anywhere in the package.

## v0.44.1 — 2026-08-03

- **Fixed**: a committed reader could be built with no high watermark and then
  serve records that were never committed. This is the defect behind the
  intermittent `no segment to consume` in the follower chaos test, which had
  survived several releases as an unexplained flake — and drew two independent
  investigations to the same wrong theory, a stale segment snapshot. The
  snapshot was never stale. The reader had no watermark.

  The way in is a race a consumer cannot avoid. It asks the log where to start,
  gets `OldestOffset() == -1` because the log is empty, and passes that back a
  moment later — by which time records have landed. Both guards in
  `newReaderCommitted` then miss: `offset > hw` is `-1 > -1`, false, and
  `OldestOffset() == -1` is no longer true. Execution reaches the tail of the
  constructor, which computes the watermark position only `if hw != -1`, so it
  returns a reader with a nil `hwSeg` and a NON-nil starting segment.

  That combination is unbounded, because the read is clamped to the watermark
  only when `r.seg == r.hwSeg` and a nil `hwSeg` is equal to nothing. The reader
  took the whole segment regardless of what was committed. Running off the end
  of it is what raised the error — but that was the second symptom; the first
  was a committed reader serving uncommitted records, which is the one promise
  that interface exists to make.

  Fixed at both levels: `hw == -1` now takes the park-and-wait branch on its own
  merits (nothing is committed, whatever offset was asked for), and an unset
  `hwSeg` no longer reads as "the watermark is somewhere else". The error text
  at the end of the segment names the reader's state now, rather than saying
  only that it ran out.

- **Fixed**: a segment rolled while the log was closing was never closed — a
  file handle and an index mmap held until the process exited, and on Windows a
  log directory that could not be removed. Reported downstream, reproducing 10
  runs in 10.

  Closing walks `l.segments` and marks the log closed under `l.mu`. Rolling
  published the new segment in two steps and only the second took that lock, so
  an append still in flight when `Close` ran could install a segment AFTER the
  walk had gone past it. The log's own slice then named a segment nothing would
  ever close. Worse than a leak in isolation: that segment is open, so appends
  into the closed log kept SUCCEEDING, and each further roll leaked another one.

  Both publishing steps now happen under `l.mu`, and a roll into a log whose
  segments are already closed is refused with `ErrCommitLogClosed` rather than
  installed. A roll therefore either completes before the walk and is closed by
  it, or does not happen.

  The same shape was in two more places, found by looking rather than by report.
  `Clean` rewrites outside `l.mu` and installs at the end, so a pass overlapping
  a close installed a freshly built set that nothing would close; it now closes
  what it built and returns `ErrCommitLogClosed`. `Truncate` and `TruncateBefore`
  each build a replacement segment, and on an already-closed log left it open;
  they now refuse. Shutdown is exactly when this happens — a process takes a
  signal with maintenance and appends still in flight.

## v0.44.0 — 2026-08-03

Five ways an unclean shutdown or a damaged byte cost more than the bytes it
touched. Every one of them turns a local fault into a global one: damage in a
segment taking the log, damage in a log taking the process. The pattern is the
same each time — a recovery path that could not tell "I could not read this"
from "there is nothing more here" — and the fix is the same each time: say
which one it was, and never let the second answer be a guess.

- **Fixed**: truncating one byte off the active segment of a **block-compressed**
  log made `New` fail outright with `block scan overran segment`, taking every
  sealed segment under it with it. The log was unopenable, not degraded. A raw
  log survived the identical cut, because only the block path treated a
  half-written trailing block as a malformed segment rather than as the tail of
  an interrupted append — which is what it is, and what every crash mid-append
  leaves behind.

  `scanBlocks` now ENDS its walk at a block it cannot resolve — too few bytes
  left for a header, a header that does not parse, or a payload reaching past
  the file's end — instead of failing the segment. A version mismatch is still a
  hard error: that is a format the reader genuinely cannot handle, not an
  interrupted write.

- **Fixed**: the same crash on a **raw** segment — default configuration, no
  compression — read back as **zero records**. Tail recovery corrected the
  segment's position but left the torn bytes on disk, and the write handle is
  `O_APPEND`: the next record landed after the orphan rather than over it, so
  every subsequent read walked into the gap and stopped. The log accepted
  writes and served nothing.

  The torn frame is now removed from the file, not merely stepped over, and an
  index that reaches past the log it belongs to is rebuilt for raw segments
  rather than trusted — a half-written frame has no offset, so nothing later
  could have trimmed it.

- **Fixed**: after recovery trimmed a torn tail, the high-watermark checkpoint
  still named a record that no longer existed, and every read of the reopened
  log failed with `segment not found` — including reads of records that were
  never in question. The checkpoint is a durable file and outlives the records
  it counted; it is now clamped to the log's newest offset when the log opens,
  with a warning naming both numbers.

- **Fixed**: a `New` that failed left every segment it had already opened open —
  a file handle and an index mmap each, released only by exiting the process. A
  caller retrying an open that keeps failing leaked the whole set again on every
  attempt. The half-built log is now closed on the way out of a failed open.

- **Fixed**: `segmentScanner.Scan` sized its payload allocation from a frame
  header it had not checked, so a handful of damaged bytes in one sealed segment
  **killed the process** — `makeslice: len out of range` is a panic, not an
  error, and it took every unrelated log in the same binary with it. Raised from
  routine maintenance rather than from a read anyone was waiting on. The read
  path had verified that header's CRC since the CRC existed; the scan never did.
  It does now, and also refuses a frame claiming more payload than the segment
  has left.

  **Behaviour change.** The same scans then had to stop calling an unreadable
  segment a finished one. Every rewrite loop — `Truncate`, `TruncateBefore`,
  compaction, and the key digest behind it — ended on any error from `Scan`,
  wrote out what it had collected and deleted the source. That is the step that
  turns damage into loss: the records past the damage were gone, with the file
  that still held them, and the call returned success. They now distinguish
  `io.EOF` from a read failure and return an error wrapping the new exported
  `ErrSegmentUnreadable` on the latter, leaving the segment exactly as they
  found it — worth telling apart, because it says the bytes on this replica are
  damaged, which a caller with a peer to copy from can act on and a retry of the
  same call cannot fix. Callers that ignored the error
  from these three will now see one where they previously saw silent success —
  which is the point. Retention is unaffected: the delete stage runs before
  compaction and its removals are kept when compaction fails.

  `Truncate` also built its replacement segment only after deleting the segments
  above it, so any failure in the rewrite left the log naming files that were
  already gone and an active segment that was already closed — the next append
  died on it. The replacement is now built before anything is deleted, so a
  truncation that fails is a truncation that did not happen.

## v0.43.8 — 2026-08-03

- **Retracted v0.43.6.** The warning in the v0.43.7 note was only a note, which
  does nothing for a resolver; `retract` is what actually keeps the version out
  of a build. Under v0.43.6 a log with a torn tail — any unclean shutdown
  mid-append — reads back empty.

## v0.43.7 — 2026-08-02

- **Fixed**: v0.43.6 regressed torn-write recovery. Its test for "this index
  cannot describe this log" was that the index reaches past the log's end — but
  a torn write produces exactly that too, and there the index is the SOUND half:
  it describes this log, which merely lost its tail, and the surviving entries
  are all correct. Rebuilding in that case ran ahead of the tail truncation that
  makes the segment consistent and left it looking empty, so a reopened log
  served nothing at all where it had served every intact record.

  The two are now told apart before anything is discarded, by checking the
  deepest index entry that still fits inside the log against the frame it points
  at. A torn write leaves that entry exactly right; a crash between the two
  renames leaves it pointing at a record that is no longer there, because the
  rewrite dropped records and shifted everything after them. One frame read,
  only on the abnormal path.

  **v0.43.6 should not be used.** A log with a torn tail — any unclean shutdown
  mid-append — reads back empty under it.

## v0.43.6 — 2026-08-02

- **Fixed**: a log that lost power midway through installing a compacted segment
  came back permanently unable to serve that segment by offset. Installing a
  rewrite is two renames — the log file, then the index file — and stopping
  between them pairs the compacted log with the SOURCE's index, whose every
  position was computed against a strictly larger file (a rewrite only ever drops
  records).

  Nothing detected it. Both files are individually well formed; only their
  relationship is wrong, and nothing on disk marks that. A forward scan still
  worked, because it walks positions rather than looking them up — so the log
  would hand out a record from a scan and then fail to serve that same offset
  directly, landing inside a record and reporting a CRC failure. Permanently:
  no pass rebuilds an index, and the next compaction of that segment starts from
  the same mismatched pair.

  An index whose last entry ends past the end of its log cannot describe that
  log, and that is now checked when a segment opens; such an index is discarded
  and rebuilt by walking the log. The direction is what makes this safe to act
  on — an index BEHIND its log is ordinary, since the append path writes the
  frame before the entry, and `reconcileIndexTail` has always filled that in.

## v0.43.5 — 2026-08-02

- **Added**: `TestChaosAFollowerNeverSeesTheSequenceGoBackwards`, covering the
  one part of the read path that had nothing on it. Every other test that reads
  a maintaining log opens a reader, reads, and closes it — which exercises reader
  CONSTRUCTION, where two of this week's defects were, but never the scan's own
  segment jump, because a short read finishes inside the segment it started in.
  A follower held open across hundreds of passes crosses a boundary in place
  several hundred times per run.

  No new defect: the read path is clean under it, and it never once had to
  surface a maintenance error to the follower. Test-only release.

## v0.43.4 — 2026-08-02

- **Fixed**: the last three ways a maintenance pass could still surface a raw
  `segment has been closed` to a reader. v0.43.3 gave `Delete` a `gone` flag so
  a departed segment redirects instead of erroring; these are the places that
  did not consult it.

  `Delete` set the flag at the END of the function, after `Close` had already
  returned and released the lock. In that window the segment was closed but not
  yet gone, and a reader that resolved into it got exactly the error the flag
  exists to prevent. Closing and marking are now one step under one hold of the
  lock — which also means a segment whose file removal fails is skipped rather
  than errored on, and it is closed either way, so skipping is the better of the
  two answers.

  The read path itself only checked `replaced`, never `gone`: `readAtLocked` and
  `scanReadAt` reported a deleted segment as closed mid-scan, and `findEntry` and
  `findEntryByTimestamp` did not check at all — the index knows only that it is
  shut. All four now answer `ErrSegmentReplaced`, which sends the reader back to
  the segment list; `ErrSegmentClosed` is a claim about a handle, and a reader
  can do nothing with it.

- **Fixed**: `OldestOffset` reported the base offset of a segment retention had
  already deleted, because it answered from `segments[0]` and a pass does not
  publish the survivors until it ends. A caller that started a read there got
  records back from further along, which reads as history disappearing between
  the offset it was told and the first one it received. It now answers with the
  first SURVIVING segment.

## v0.43.3 — 2026-08-02

- **Fixed**: the same defect as v0.43.1, with RETENTION as the mutator instead
  of compaction. A delete pass removes segments as it walks them and publishes
  the survivors only at the end, so a reader resolving an offset mid-pass landed
  on a segment whose files were already gone and failed with `segment has been
  closed` — for offsets retention had lawfully collected, where the correct
  answer is simply to start at the next surviving segment.

  It was missed the first time because the two cleaners close a segment by
  different routes: compaction's `Replace` leaves a link to a successor, and
  retention's `Delete` leaves nothing, so the redirect had nothing to follow and
  no flag saying the segment was gone rather than merely closed. `Delete` now
  marks it, in `Delete` rather than in the cleaner, because every path that
  removes a segment owes this — including the ones outside a pass.

  Found by a chaos test that ran both cleaners at once. Neither of the two
  existing ones could have: one runs retention with compaction off, the other
  runs compaction with no retention limits at all.

## v0.43.2 — 2026-08-02

- **Fixed**: a read that crossed a segment being compacted failed with `failed
  to reinitialize reader: entry not found`. The other half of v0.43.1, and only
  reachable because of it.

  A rewrite drops superseded records, so a replacement segment can END BELOW
  where its source did. Redirecting a lookup to it without re-checking that it
  still reaches the offset resolved the reader into a segment whose records all
  sit below where it was — for an offset that belongs to the NEXT segment.
  `findSegment` now re-applies its own search predicate to the resolved segment
  and moves on when it no longer holds the offset, which is the same thing it
  does for a segment the pass removed.

## v0.43.1 — 2026-08-02

- **Fixed**: a read against a compacting log failed, at random, with `segment
  has been closed` — for an offset that was valid and whose record was on disk
  the whole time.

  A compaction pass mutates segments long before the log publishes the result.
  Installing a rewrite renames the new files over the source's and CLOSES the
  source; a segment whose every record was superseded is deleted outright.
  Neither leaves `l.segments` — that list is swapped once, at the very end of
  the pass — so for the whole duration of a pass the log hands out segments that
  are closed or gone, and resolving an offset through one fails.

  A segment now carries a link to what superseded it, and `findSegment` follows
  it: a replaced segment resolves to its replacement, a removed one is skipped
  the way retention already leaves readers skipping. The window closes for every
  lookup at once — readers, the high-watermark position, the timestamp probes —
  rather than for whichever call path happened to be reported.

  Retrying the resolve is NOT what fixes this, and was the first attempt: the
  stale segment stays published for the rest of the pass, so a retry re-resolves
  to the same closed segment. It is kept, bounded, for the much smaller window
  inside `Replace` itself, where the source is closed a few instructions before
  the link to its replacement is set.

  `getHWPos` returns the segment rather than its index, because all four callers
  used the index to re-read the raw slice — which would have reinstated exactly
  the closed segment the redirect exists to avoid.

  Reachable by any caller that reads while compaction runs. Found by a chaos
  test in `durable_streams` that raced reads against a maintenance loop; it
  reproduced in 0.16s. `TestConcurrentReadersAndProbesOnLiveLog` looks like it
  should have covered this and does not: it sets `Compact` but never RUNS a
  pass, leaving compaction to a background cleaner whose interval is minutes, so
  in a five-second test no segment is ever replaced.

## v0.43.0 — 2026-08-02

- **Added**: `CleanSpec.RetentionFloor` — a bound on what RETENTION may delete.
  A segment is eligible only if the whole of it lies strictly below the floor;
  deletion is per segment, so a segment holding one protected record is
  protected entire. Nil, the zero value, means no floor, which is what every
  caller had before this existed.

  Retention and settlement answer different questions, and only this side of the
  boundary could tell them apart. Age, bytes and message count know nothing
  about a caller still USING the records they collect — and a transactional
  caller's staged records sit in the log for as long as its transaction is open.
  A limit reached in the meantime deleted them out from under it, and the commit
  that followed referred to offsets that no longer existed. There was no way to
  express the protection from above: `TruncateBefore` deletes, and a caller that
  skipped its own maintenance pass to avoid the loss stopped collecting
  everything else too.

  It bounds the DELETE cleaner only; compaction is already bounded by `Ceiling`,
  and a caller's floor is at or above its ceiling by construction — the records
  it protects are precisely the ones not yet decided. The clamp lives in the one
  function all four limits (age, messages, bytes, tier) delete through, rather
  than in each of them, because one limit forgetting the floor would surface far
  from the limit that caused it.

  A POINTER rather than a sentinel, deliberately. Every obvious sentinel is a
  real floor: 0 protects the whole log, which is exactly what a transaction that
  began at offset 0 needs — the first transaction a fresh log ever sees — and an
  `int64` field whose zero value had to mean "unset" would silently give that
  caller no protection at all.

- **Removed**: the package-private `min(int64, int64) int64` in `reader.go`,
  which shadowed the builtin for the whole package. Every call site passed
  int64s, so the builtin is a drop-in; what it cost was that a new call passing
  ints failed to compile with a type error naming int64 out of nowhere.

## v0.42.2 — 2026-07-30

- **Fix**: an as-of timestamp lookup no longer answers a failed read with an
  offset. `LatestOffsetBeforeTimestamp` returned the segment's **newest** offset,
  with a nil error, when the read behind it failed — telling a consumer asking
  "where was I at time T" that it was already caught up, so every record it had
  not yet read was skipped. Silently, with nothing to log.

  `scanForward` is the block-mode (compressed segment) path under both
  `findEntry` and `findEntryByTimestamp`. It has no end bound — it walks frames
  until a read fails — so `io.EOF` is legitimately its stop condition and
  `ErrEntryNotFound` its answer. But EVERY other error arrived as
  `ErrEntryNotFound` too, and both timestamp lookups convert that into a
  plausible offset. `ErrSegmentReplaced` is the one that matters: compaction
  produces it routinely, and `Reader.ReadMessage` **retries** it rather than
  accepting it. `ErrSegmentClosed` and a failed tiered object fetch are the
  others.

  Now only `io.EOF` ends the scan; everything else is wrapped and returned. The
  two lookups match `ErrEntryNotFound` with `errors.Is` and no longer also accept
  `io.EOF`, which had meant a truncated index — `position` claiming more entries
  than are mapped — was answered with an offset instead of an error.

  `EarliestOffsetAfterTimestamp` was less severe and worth stating precisely: it
  retries the next segment and propagates that error, so it *failed* — just
  describing a failed read as "entry not found". It fabricates an offset only
  when the target lands in the last segment.

  Also removed a dead `io.EOF` arm in `EarliestOffsetAfterTimestamp` that returned
  the next assignable offset with a nil error. `findSegmentIndexByTimestamp`
  stopped producing `io.EOF` when the empty-segment case was fixed; the arm's only
  remaining effect would have been to convert a future read failure into a
  fabricated offset. "Beyond the end of the log" is already answered by the
  `idx == len(segments)` path.

  Verified by neutralising the guard: `LatestOffsetBeforeTimestamp` returned
  offset 23 with a nil error. Added as an eighth guard to `hack/guardcheck.sh`.

  The regression test took two attempts, and the first is the more useful record:
  it closed the segments to break the read, but that closes their indexes too, so
  `findSegmentIndexByTimestamp` failed first and the test passed with the fix
  removed. It now swaps the segment's `segmentBacking` — the data, leaving the
  index alone — which is what makes the injection land on the code under test.

## v0.42.1 — 2026-07-30

- **Hardening**: `bufReader` no longer invents `io.EOF`. Three branches returned
  end-of-data for conditions that were not the end of anything: a reader used
  before being positioned on a segment, a short copy out of a buffer the guard
  above had just established was long enough, and a backing that returned no
  bytes and no error. Each is now an error naming the condition.

  All three were unreachable — both constructors refuse a nil segment, the short
  copy is impossible given the guard's arithmetic, and every backing here
  documents `os.File.ReadAt` semantics where the end returns `io.EOF`. That is
  the reason to fix them rather than leave them: nothing can fail while the
  condition never arises, so whoever makes one reachable would get a silent
  truncation instead of a test failure.

  Removing them left `io` unused in the file, which is the finding in one line.
  `bufReader` has no way to know whether a log ends; an empty buffer is a
  statement about the buffer. It now only forwards the segment's `io.EOF`.

  Prompted by durable_streams reporting what v0.42.0 fixed for them: two of their
  scan loops break on `io.EOF` as an ordinary end and ran under a caller's
  context, so a cancellation made them return **partial results as complete
  answers**. Their generalisation — an end-of-data signal doubling as a failure
  signal makes a consumer stop early and report success — is what this sweep
  applies to the rest of the package.

  The two remaining `io.EOF`-on-close returns in `reader.go` are correct but only
  because `Reader.ReadMessage` converts them to `ErrCommitLogClosed`, and
  `IsClosed()` reads the same channel that woke the reader, so the conversion
  cannot lose the race. Both now say so where the `io.EOF` is returned, because
  a second consumer of `contextReader` that skipped the conversion would inherit
  a reader that reports a closed log as a fully drained one. `index.ReadAt` and
  `collectRun` already made this choice and already explain it.

## v0.42.0 — 2026-07-30

- **Fix (breaking for anyone matching on io.EOF)**: a cancelled context is no
  longer reported as end-of-data. Reads now return `context.Canceled` /
  `context.DeadlineExceeded` when the CALLER gave up.

  `io.EOF` is this package's documented end-of-read signal — `NewReader`'s
  contract says a read ends when `errors.Is(err, io.EOF)`. Both readers returned
  exactly that when the caller's context finished, so a timeout was
  indistinguishable from "the log ended". A consumer reading with a per-read
  deadline — an ordinary pattern — would stop tailing and believe it had caught
  up, silently, at the tail.

  Three tests asserted the old behaviour, which pinned the defect rather than a
  property. They now assert the cancellation.

- **Tests**: live tailing is covered. Nothing here previously parked a reader at
  the TRUE end of the log and then appended: `TestReaderFollow` wakes a reader by
  advancing the high watermark over records that already existed on disk, so a log
  that stopped delivering the moment a reader genuinely caught up would have
  passed. Added the post-drain wake-up, repeated wake-ups, and a filtered reader
  that must not be satisfied by a tail record its prefix rejects.

  Prompted by durable_streams tracing a live-tail stall and noting that their
  suite only ever appended before consuming. This one was nearly the same.

## v0.41.1 — 2026-07-30

- **Fix (panic)**: a `headersBuf` smaller than `HeaderBufferLen` panicked instead
  of returning an error.

  Reported by durable_streams within an hour of v0.41.0: 24 call sites across 18
  files still allocated 28 bytes, and every one panicked inside `storedHeaderCrc`,
  indexing past the end of its own argument. Their call sites were stale; crashing
  the host process over it was still this package's fault.

  It could not have been caught downstream either. `Read` fills whatever it is
  given, so a short buffer quietly consumes a partial header and desynchronises
  the stream — the header CRC would then report corruption in a log that is
  perfectly intact.

  Both read paths now check up front and name `HeaderBufferLen` in the error.

### Migration from a pre-v0.41.0 log

Stated plainly, because v0.41.0's note was not loud enough. **v0.41.0 and later
cannot read segments written before it.** The frame header grew by four bytes and
carries no version field of its own — the irony is deliberate to name: the header
was unprotected precisely because nothing in it described itself.

There is no in-place upgrade and no converter today. A deployment with data on
disk needs a dump-and-reload through the old version.

**Do not implement the obvious fallback.** An earlier revision of this section
suggested trying the 32-byte header and falling back to 28 when its CRC fails.
That is wrong, and durable_streams was right to say so: a CRC failure means
EITHER "an old frame" OR "a corrupt new frame", and nothing distinguishes them. A
genuinely corrupted 32-byte header would fail its checksum, be reinterpreted as a
plausible 28-byte one, and served as fact — exactly the defect v0.41.0 closed,
reintroduced on the error path. A checksum failure is the one signal that must
never be recovered from by guessing.

**Decided: there will be no migration path.** This is a clean break, taken
deliberately while the cost is small, not an omission to be fixed later. Do not
open this again expecting a converter.

Recorded for whoever needs it anyway: the sound shape would be a per-SEGMENT
version marker — a segment is homogeneous (one writer, one format, immutable once
sealed), so the layout is decided ONCE from a declaration rather than inferred per
record, the way block framing is already decided by its magic byte. A per-LOG
marker cannot serve, because a rolling upgrade is exactly the moment one directory
holds both formats. The cheapest form would be a sidecar beside `.keys` and
`.offloaded`, absent meaning legacy; a preamble inside the `.log` would shift every
byte position, and the index stores file offsets.

## v0.41.0 — 2026-07-30

**BREAKING FORMAT CHANGE.** Segments written by an earlier version will not read.
There is no migration; the header layout changed.

- **Fix (data integrity)**: the frame header is now checksummed, so a record's
  IDENTITY is protected the way its value already was.

  A record's CRC covers its payload. The 28 bytes in front of it — offset,
  timestamp, leader epoch, size — carried no checksum, so a damaged offset was
  reported as fact. `FuzzCorruptFrameHeaderIsNeverServedAsTruth` produced offset
  7 carrying record 0's value, CRC passing, in a log holding 0..15. v0.39.2's
  segment-bounds check could reject an offset outside its segment's range and
  nothing more, because nothing contradicted one inside it.

  The header is now 32 bytes: a CRC32 over the four fields. Verified on both read
  paths, and BEFORE `size` is used — size is one of the fields being verified,
  and trusting it first is how a corrupt length becomes a bad allocation.

  Four bytes per record. `HeaderBufferLen` (v0.40.1) already carries the size, so
  callers using it need no change; callers who copied the literal `28` do.

- **Changed**: `ReadMessage` and `ReadMessageMetadata` return `ErrCorruptRecord`
  for a header that fails its checksum, the same sentinel a bad payload gets.

## v0.40.1 — 2026-07-29

Two details this package leaked, reported by durable_streams against v0.40.0.

- **New**: `HeaderBufferLen`, the capacity `Reader.ReadMessage` and
  `Reader.ReadMessageMetadata` require of their `headersBuf`.

  It was a bare "28" in a doc comment, so a consumer wrote `make([]byte, 28)`
  against prose. A number copied out of documentation is the same mistake as a
  magic byte copied into another repo: correct until it isn't, and silent when it
  stops being.

- **Docs**: `InspectSegment` now states the two things a caller had to guess —
  that it is NON-MUTATING, and that it takes no `Options`.

  The non-mutation is the point, and the reason `New` cannot serve this purpose:
  opening a log runs recovery, may adopt a descriptor and may rewrite segments,
  so aiming it at evidence alters the evidence. Every hand-written mirror this
  replaced carried a warning to work on a copy of the data directory. This reads
  one file, once, and writes nothing.

  And nothing about `Path`, `Name`, `Compact` or descriptor adoption is
  load-bearing: a segment file describes itself, so there is nothing to configure
  when inspecting a foreign directory.

## v0.40.0 — 2026-07-29

A layering audit, and the one thing it found that was costing other people time.

- **New**: `InspectSegment` reads a segment `.log` file without opening the log.

  ```go
  f, err := commitlog.InspectSegment(path)
  blocks, err := f.Blocks()          // physical block layout
  err = f.Records(func(r commitlog.RecordInfo) error { … })
  ```

  Both consumers of this package had already written this themselves, because
  the package exported no way to read its own format — and both were wrong. One
  walked block headers a byte short; the other hard-codes the `0xC1` magic and a
  format version in product code to avoid importing this package. When a
  corruption report arrived, the two mirrors disagreed with each other and with
  the log, and the hours spent reconciling them bought nothing: the records were
  fine and the decoders were not.

  Read-only, over a directory nothing has open. No index, no recovery, no second
  read path — a forensic tool. It is built on the same internals the log reads
  with, so it cannot drift from the real framing the way a copy in another repo
  does.

  A damaged record is REPORTED (`RecordInfo.CRCValid`), not refused: erroring
  would make it useless for the one job it has. And `Blocks` names the
  pre-v0.15.0 layout in its error — those segments have a 10-byte header with no
  version field, so this build reads their codec byte as a version and refuses a
  zstd block as "format version 3", a version that never existed. That bare
  message sent someone hunting a phantom writer for hours.

- **Changed (internal)**: `commitlog.go` split from 2,199 to 1,671 lines — tier
  ownership and reclamation to `tier_state.go`, filesystem retry helpers to
  `util.go`, clean supervision and `CleanSpec` to a new `clean.go`. The read
  functions moved out of `message_set.go` into `reader.go`, where the interface
  they take is defined. Pure moves; no behaviour or signature changes.

## v0.39.2 — 2026-07-29

Two ways a log torn mid-write could hurt the process reading it. Both found by a
new fuzz target within seconds of it existing, one of them by its first seed
before any fuzzing had started.

- **Fix (panic)**: a frame declaring a payload too short to hold its own checksum
  panicked out of the caller's process.

  The size field is not covered by any checksum — the CRC lives INSIDE the
  payload it describes — so a torn frame can declare a length of zero.
  `readMessage` then took `Crc()` of an empty slice: *"index out of range [3]
  with length 0"*. Reachable from an ordinary crash, and precisely what a log
  embedded in someone else's binary must not do. Such frames are now refused as
  `ErrCorruptRecord`.

- **Fix (out of memory)**: that same unchecksummed size field could declare up to
  4GiB, and both read paths allocated it BEFORE reading a byte.

  A torn segment therefore handed an out-of-memory kill to the host. The fuzzing
  worker did not report an assertion — it died outright. Payloads are now read in
  chunks, and the buffer grows only as bytes actually arrive, so a frame claiming
  4GiB of a file holding a hundred bytes costs one chunk and an error.

  The same sweep went from dying at 128 executions to surviving 6000, and its
  rate rose from ~1/sec to ~59/sec: the workers had been thrashing on
  allocations rather than exploring inputs.

- **Tests**: three fuzz targets now assert the corruption invariants directly —
  damaged record bytes, a damaged key digest, and a torn log — alongside
  `hack/guardcheck.sh`, which removes each guard and requires the test named for
  it to fail. Fuzzing explores inputs and cannot tell whether an assertion bites;
  this repo shipped a defect behind a test that did not.

## v0.39.1 — 2026-07-29

- **Fix (data integrity)**: compaction RE-SIGNED corrupt records, certifying the
  damage.

  `stripFrame` recomputes a record's CRC over whatever bytes it is handed. Handed
  one that was already damaged, it signed the damage: the rewritten record
  carried a fresh, valid checksum over wrong bytes, and every later read —
  including the CRC-verifying one added in v0.39.0 — reported it as sound.

  Worse than serving corruption once. The stored CRC is the only evidence a
  record is what the writer wrote, and the rewrite destroyed that evidence
  permanently, leaving verification downstream structurally blind.

  Reported by durable_streams, whose `Clean` always sets `StripHeaders` +
  `StripBelow`, so every pass took the re-framing path.

  The CRC is now verified before re-encoding, and a record that fails it is
  copied VERBATIM — keeping its failing checksum, staying exactly as damaged as
  it was, still returning `ErrCorruptRecord` to readers.

  The clean is **not** failed. The cleaner runs unattended on a timer, so
  erroring here would wedge compaction and retention behind one bad record until
  someone intervened, turning an unreadable record into a full disk. The record
  is logged, counted, and marks the segment residually strippable so the digest's
  strip stamp cannot claim there is nothing left to strip.

## v0.39.0 — 2026-07-29

A filtered read could hand a caller a record it knew nothing about the integrity
of, because the fast path never asked. And when the other path did ask and did
not like the answer, it killed the caller's process.

- **Changed**: the five reachable panics left in the package now return their
  errors instead — two in `index.writeAt` (a failed `Truncate` or `mmap` while
  expanding the index), one in `index.InitializePosition` (a failed index read
  inside a `sort.Search` predicate, carried out in a variable and checked after
  the search), and two in `newMessageSetFromProto` (a batch given to a log with
  concurrency control enabled, which is caller misuse and still not worth
  killing a process over, and a message that fails to encode).

  All five were already inside functions returning an error, so they were
  discarding a channel they had. No behaviour change for any caller that was not
  already dying.

  One panic remains, in `newUploadID`, and it is documented as the place that is
  right: `crypto/rand.Read` "never returns an error, and always fills b
  entirely" — it crashes the program itself if the OS source fails — so the
  branch is unreachable, and threading an error out of it would change ten call
  sites to handle a condition that cannot arise.

- **Changed (breaking for anyone relying on the panic)**: a record failing its
  CRC now returns `ErrCorruptRecord` from `ReadMessage` instead of panicking.

  The panic said "data on disk is corrupted which means the server is in an
  unrecoverable state". That was true of the server this package was extracted
  from and is wrong for a library embedded in someone else's process: the host
  had good answers available — skip the record, fail the read, resync the stream
  — and the panic took both the choice and the process away. A read is exactly
  where a caller is positioned to choose.

  Reported by durable_streams, who were recovering the panic at their own
  boundary to stop one bad record from killing their host.

  A sentinel, because choosing depends on telling corruption from an ordinary
  read failure. Both routes return the same one: which of them found the record
  is this package's business, not the caller's.

  The trade, stated rather than buried: an error CAN be ignored where a panic
  cannot, so a caller that checks nothing now proceeds past a record it should
  not trust. Every caller of `ReadMessage` already handles an error return, and
  none could handle a panic.

- **Fix (data integrity)**: a `KeyPrefix` read over SEALED segments returned
  records that fail their own CRC, without an error.

  `readOne` serves the digest-planned portion of a filtered read directly from
  the prefix source, so it never reaches `readMessage` — which held the only
  verification of a message payload in the package. The same corrupted record
  read sequentially was refused as unrecoverable; read by key prefix it came back
  as data. One flipped byte in a sealed segment, both routes over one log:

  ```
  KeyPrefix path : SERVED "PAYLOAD-Q05-ZZZZZZZZ"
  sequential path: PANIC (CRC caught it)
  ```

  The digest earns that path its speed by naming which offsets hold matching
  KEYS. It cannot vouch for what is stored at them, and planning a read from it
  does not make a record more trustworthy than one found by walking.

  **Behaviour change for callers**: such a read now fails where it previously
  returned data. It returns an ERROR rather than `readMessage`'s panic, because
  this runs in worker goroutines where a panic cannot be recovered by the caller
  and takes the process with it.

  Verification costs nothing here — the record is already being copied, so it is
  a second pass over bytes in cache, not a request or a decode.

- **Docs (contract)**: `ReadMessageMetadata` said it returns metadata "without
  CRC-validating the payload or retaining the value bytes", while the
  `MessageMetadata.Raw` field beside it advertised "full raw message bytes
  (Key() + Value() available)".

  Both cannot be true, and the one a caller acts on is the second. `Raw` is
  BORROWED: it points into the `payloadBuf` the next call overwrites. A caller
  that reads a batch and decodes afterwards — the shape a reusable buffer invites
  — gets its values rewritten underneath it. A shorter following record
  overwrites only the head, so the retained slice keeps its length and still
  parses with another record's bytes at the front, and nothing errors.

  No behaviour change: the aliasing is the point of the API, and is why an LSO
  rebuild can scan millions of records without allocating. Both docs now say
  borrowed rather than owned, and state that the payload is unverified — this
  path returns a corrupted record as data where `ReadMessage` refuses it.

## v0.38.2 — 2026-07-28

Concurrent readers and timestamp probes on a LIVE log are supported. They were
also, until now, racy and prone to a spurious error — both found by writing the
test that was supposed to merely demonstrate the support.

- **Fix (data race)**: `LatestOffsetBeforeTimestamp` read `seg.lastOffset`
  without the segment's lock, racing every concurrent append.

  Found by `TestConcurrentReadersAndProbesOnLiveLog` under `-race`. This is a
  path callers run **unattended on a timer** — a tiering horizon, an offset
  lookup — so it raced whenever a probe happened to land while a record was
  being written. Every other reader of that field goes through `LastOffset()`,
  which takes the read lock; this one did not.

- **Fix**: `LatestOffsetBeforeTimestamp` failed with a bare `EOF` whenever the
  segment search touched an EMPTY segment — the normal state of the active
  segment in the window just after a roll.

  `findSegmentIndexByTimestamp` reads each segment's first index entry, and an
  empty segment legitimately has none. That `io.EOF` was recorded as a failure;
  `EarliestOffsetAfterTimestamp` special-cased it, `LatestOffsetBeforeTimestamp`
  did not. An empty segment now sorts after every timestamp — where a segment
  holding no records belongs — with no error. Timing-dependent, which is why
  concurrent readers made it observable.

- **New**: `ErrTimestampBeforeLog`, returned when a timestamp lookup asks for a
  point earlier than anything the log still holds.

  It was an ad-hoc `errors.New` that callers could only string-match. The
  distinction matters to an unattended caller: "the log does not go back that
  far" is a normal answer to absorb — clamp to the oldest offset and carry on —
  while an index or I/O failure is not. Without a sentinel the two are one
  opaque error, and the safe handling of each is the wrong handling of the other.

- **Changed**: `delete_cleaner`'s age-retention loop reads `LastWriteTime()`
  through the accessor rather than the bare field, matching the identical read
  in `cleanTier` a few lines above. Safe before only as a property of the loop
  rather than of the field.

## v0.38.1 — 2026-07-28

- **Fix**: shrinking an index while it was EMPTY left it silently unwritable,
  and any later read of it panicked.

  Reported by sqlcdc against v0.36.1: `findEntry` → `index.ReadAt`, *"slice
  bounds out of range capacity 0"*, on an empty active-segment index.

  On Windows `shrink()` unmaps before truncating, because an open view blocks
  `SetEndOfFile`. It remapped only `if remap && idx.position > 0`, and set
  `idx.size` inside that same branch — so an empty index was left with a **nil
  mapping** while `size` still described the pre-allocated file that had just
  been truncated away.

  The panic is the *second* symptom. The first is silent corruption: `writeAt`
  compared against the stale size, decided no expansion was needed, and copied
  into the nil mapping — and slicing a nil slice at `[0:]` is legal Go, so the
  write did **nothing** while `position` still advanced. The index then claimed
  entries it did not hold, without error. Only a later read of one of those
  entries surfaced it, as a crash.

  `shrink()` now keeps `size` consistent with the file on both platforms;
  `writeAt` skips the unmap when there is no mapping; and `ReadAt` reports
  `errIndexCorrupt` rather than indexing past a short mapping — a panic inside a
  library takes the caller's process down, and an error is strictly better
  whatever the cause. Deliberately not `io.EOF`, which would read as "no more
  entries" and silently truncate a scan.

  **No production path to this has been demonstrated** — a stronger statement
  than the one made when this shipped, and it is the accurate one.

  Two later attempts to reach the defect through a real log both passed with the
  fix reverted. The bad state exists only IN MEMORY, and closing the log discards
  it: on reopen `newIndex` finds a zero-length file and maps it fresh. Reaching
  it needs the index to be *used* after an empty shrink within one process, and
  `seal()` — its only caller — marks the segment sealed, after which nothing
  writes to it. The unit test reaches it only by calling `Shrink` then
  `writeEntries` directly, a sequence `seal()` does not produce.

  So this is defence against a latent inconsistency, not a proven live bug, and
  it is **probably not** what the reporter hit. The likelier cause of that report
  is the timestamp-path race and spurious `EOF` fixed in v0.38.2.

## v0.38.0 — 2026-07-28

One reader, configured by options, with a key-prefix filter that reads only the
records it returns. Every read entry point changes, and two of their defaults
invert. `docs/read-interface.md` records the reasoning.

- **Breaking**: `NewReader(offset int64, uncommitted bool)` and `NewScanReader`
  are replaced by `NewReader(opts ...ReadOption)`.

  The two constructors differed in exactly one axis — terminate versus follow —
  and a third (committed versus uncommitted) was an unlabelled bool at the call
  site. Adding a key filter and a supersession setting as further constructors
  would have multiplied out to eight entry points for one read with four
  independent settings.

  Migration is mechanical:

  ```go
  NewReader(off, false)  ->  NewReader(From(off), Follow())
  NewReader(off, true)   ->  NewReader(From(off), Uncommitted(), Follow())
  NewScanReader(off)     ->  NewReader(From(off), Uncommitted())
  ```

  Options rather than a spec struct because the zero value of a read setting is
  meaningful: offset 0 is a real offset, committed-only is a real choice, and a
  bound has no natural "none" value. That is the opposite conclusion to
  `CleanSpec`, which is data a transactional layer computes and may want to log
  — deliberately, and the reasoning is recorded.

- **Breaking**: two defaults invert, and callers that were relying on the old
  ones will notice at once rather than subtly.

  A reader now **terminates** at the end of the data unless `Follow()` says
  otherwise, and reads **committed data only** unless `Uncommitted()` says
  otherwise. The failure modes were never symmetric: a reader that unexpectedly
  ends returns `io.EOF` and its caller notices, while one that unexpectedly
  follows blocks forever. That is how `RecoverTail` could hang before v0.18.0.

  **Migrating: check each call site's intent, do not translate its signature.**
  Dropping an option does not fail to compile — it silently changes behaviour,
  which is the one thing a compiler cannot catch here.

  And that same asymmetry inverts for the migration itself. Because a reader
  that unexpectedly ends is noticed while one that unexpectedly follows hangs,
  the dangerous direction of a bad translation is **accidentally adding
  `Follow()` where it was not wanted** — the old `NewScanReader` sites, which
  terminated. A site that loses a `Follow()` it needed fails loudly; a site that
  gains one it did not need waits forever. (Observation from durable_streams,
  migrating nine call sites.)

- **Breaking**: `ReadKeyPrefix` and `PrefixRecord`, added in v0.37.0, are
  **removed**. They were the wrong shape and lasted one release.

  They promised the latest surviving record per key. But compaction is
  asynchronous and budgeted, so a key can have several live copies at any
  moment — every consumer already tolerates duplicates, and one that did not
  would already be broken. A read has no business promising otherwise.

  Dropping that promise removed the eager whole-range merge, the inability to
  follow, the clash with offset tracking (a key's surviving record can sit
  *below* a consumer's resume offset), the `completeThrough` handoff and the
  snapshot-then-tail protocol around it. What replaces it is an ordinary
  following reader with a filter.

- **New**: `KeyPrefix(prefix)` returns only records whose key begins with
  prefix.

  Over sealed segments this is planned from the key digests, so only matching
  records are read rather than every record being read and tested — one segment
  scanned for one hit across 60 sealed segments, against 33 with the digests
  ignored. The active segment holds no digest and is filtered record by record;
  the acceleration is a property of having a digest, not of the API.

  Unkeyed records cannot match and are dropped. So are control markers, which
  are keyless — `IncludeControl()` keeps them.

- **New**: `SkipSuperseded()` drops copies of a key that a later copy in the
  same segment supersedes, taking duplicate reading to O(segments) per key.

  An optimisation, never a guarantee: duplicates still arrive across segments
  and from the tail. It is decided from the digest alone, with no lookahead,
  which is why it streams and can follow. One asymmetry is documented rather
  than engineered away: what counts as superseded depends on where the read
  began, so a reader resuming mid-segment can return *more* records than one
  that read the whole segment — never fewer, and never a stale value for a key
  it reports.

- **New**: `KeyPrefix` with `Uncommitted` is **refused at construction** unless
  the caller passes `Until` or `IncludeControl`.

  Reading past the commit boundary yields records whose transactions are
  undecided, and the markers that say which committed are keyless — the filter
  drops them. The caller would hold records it cannot classify, silently. The
  log cannot verify that a stated bound really is a commit boundary, having no
  notion of decidedness, exactly as `CleanSpec.Ceiling` is an input it must
  trust; it can insist the caller considered the boundary at all. durable_streams
  shipped precisely this bug and fixed it in `broker/v0.17.0`.

- **Changed**: `PrefixReadTierCoalesceBytes` defaults to 4KB, down from 64KB.

  Now measured rather than argued. At 64KB the budget behaved identically to
  1MB on every shape tested — coalescing everything — so a default justified on
  price sat an order of magnitude above the price breakeven
  (`gap = 1e9 * C_req / C_GB`, ~4.4KB at commonly quoted egress pricing).
  Deployments reading from inside the same region, where bytes are effectively
  free, should raise it.

## v0.37.0 — 2026-07-28

Three parts of the tier surface existed because of how the API grew, not
because a log needed them. All three were breaking, so they ship together as
one migration rather than three. `docs/tier-layering.md` records the reasoning.

- **Breaking**: `CleanWithSpec` no longer returns superseded store keys — it
  returns `(verified int64, err error)` — and the log **reclaims those objects
  itself**.

  Returning them made the caller responsible for commitlog's own garbage, and
  that was not a boundary but an evasion. The reason the keys were exported at
  all is that a rewrite cannot tell when an in-flight reader has finished with
  the object it replaced; handing them upward passed that problem to someone
  with strictly less information.

  The log knows its readers, so it tracks them. A backing over a store object
  carries a reference count, taken when a scan acquires it and released when
  that scan closes. A rewrite queues the superseded key with the backing that
  was serving it, and the queue drains at the *start* of a later clean pass — by
  which point most readers are long gone, and anything still held waits another
  pass.

  Reclamation touches only what this log uploaded and its own rewrite replaced.
  That is a far narrower claim than "unreferenced by me", which remains unsafe
  on a shared store. It never deletes an object the manifest still names: a pass
  republishes the manifest before queueing, and a pass whose publish failed
  holds reclamation off entirely, so a crash in between leaves an orphan rather
  than a dangling reference — storage, not correctness, and
  `UnreferencedObjects` reports it. It is suppressed while the tier is
  read-only, a delete being a store write like any other, and the queue is held
  across that rather than dropped.

  Callers should **delete** their `DeleteStoreObjects` plumbing rather than port
  it. `DeleteStoreObjects` and `UnreferencedObjects` remain as operator tools
  for the shared-store case, where garbage this log did not create can still
  appear; they are no longer part of the normal path.

- **Breaking**: `CleanSpec.SkipTiered` is gone. Use `SetTierReadOnly`.

  The same idea at two scopes — one said "not this pass", the other "not this
  process" — and offering both invited a caller to set them to disagree.
  Whether a log may write to its store is a property of the **log**, so the mode
  that expresses ownership is what remains, and `CleanWithSpec` derives the
  pass's behaviour from it.

- **Breaking**: `ExportTierState` and `ImportTierState` are gone.

  They existed only so a caller could carry commitlog's segment index through
  its own consensus. Since the tier manifest (v0.36.0) the store describes
  itself and a log adopts what it finds on open, so these were a second way to
  do what now happens by itself — and one that could disagree with it.
  `TierObject` stays; `TierManifest()` returns it.

## v0.36.1 — 2026-07-27

- **Fixed**: `UnreferencedObjects` judged garbage by this log's own segments
  only, so on a shared store it named objects another live process had
  offloaded — the exact case v0.36.0's manifest was supposed to settle. The
  documentation and the implementation disagreed, and the documentation was the
  correct one.

  Live is now judged against the store's **manifest** as well as this log's
  segments. The union matters in both directions: the manifest alone would miss
  an object this log is reading but has not yet republished (a rewrite installs
  objects, then writes the manifest), and the segments alone miss everything a
  peer offloaded since this log opened.

  A manifest that exists but cannot be read is now an error rather than an
  empty result, since a garbage list built without it would name objects the
  tier still depends on.

  The remaining caveat is timing rather than locality, and is documented as
  such: an object uploaded by a process that has not yet published its manifest
  is named by nothing and will be listed. Anything already published is safe.

## v0.36.0 — 2026-07-27

- **Added**: a **tier manifest** — the store now describes itself, and
  `TierManifest()` reads it.

  A tier previously held bytes it could not explain. The mapping from offset
  range to object lived only in local marker files beside the log, so the
  objects were readable but uninterpretable on their own: a process holding the
  store and not the directory could not say what it was looking at, and the
  bookkeeping had to be carried out of band by whoever had consensus. That is
  commitlog's own segment index, and it belongs with the segments.

  The manifest is written **after** the objects it names, making it the tier's
  commit point exactly as the local marker is the log directory's. An object no
  manifest names was never committed — a crash between an upload and its
  manifest — so it is a recognisable orphan rather than an ambiguity.

- **Changed**: opening a log over a store it has no local markers for now picks
  up the offloaded segments automatically. Where the index was offloaded too the
  segment is complete in the store; where it stayed local it is **rebuilt from
  the object**, because otherwise the segment would open with an empty index and
  read back as though it held no records — present, described, and silently
  empty. That rebuild costs one pass, which is one request now that a sweep
  streams.

  `ExportTierState`/`ImportTierState` remain, but are no longer the mechanism —
  they are an optimisation for a caller that would rather not re-read the
  manifest.

- **Fixed**: `UnreferencedObjects` would have reported the manifest itself as
  garbage, so feeding it to `DeleteStoreObjects` would have deleted the tier's
  own index.

- **Fixed**: a log adopting only offloaded segments had no writable active
  segment, since every offloaded segment is sealed. It now gets one starting
  where the tier ends.

- **Fixed**: `CleanSpec.SkipTiered` promises zero store writes, and a manifest
  Put is a store write. A skipped pass leaves the manifest alone.

## v0.35.0 — 2026-07-27

**Breaking.** `SegmentStore` and the internal `segmentBacking` gain streaming
reads. An out-of-tree store implementation needs two new methods.

- **Added**: `SegmentStore.Stream(key, off) (io.ReadCloser, error)`. A sweep —
  compaction, recovery, digest build, replay — reads a segment front to back,
  but `ReadAt` alone cannot express "I want the rest of this object". Against
  object storage that distinction is the bill: cost is per **request**, not per
  byte, so scanning a 1 GiB object through a 1 MiB window was ~1024 GETs for
  bytes one GET would have delivered.

  Pull-shaped rather than a `WriteTo`, so a caller can stop early, and because
  every backend already has a reader to hand back (gocloud's `NewRangeReader`,
  `os.Open`). `Put` already takes an `io.Reader`, so the two directions stay
  symmetric.

- **Added**: `StreamPays()`, which says whether streaming beats ranged reads for
  a given backing — true for a store, false for a local file.

  This is where measurement changed the design. The intent was one streaming
  path for local and remote alike; local turned out to be **measurably worse**.
  A store charges per request, but a local read costs a syscall against an OS
  already reading ahead, and opening a second handle to stream from costs a
  syscall of its own. Routing local scans through a stream made this repo's
  test suite take five times as long, all of it in file opens. So local scans
  read exactly as they did before, and the choice is an explicit method rather
  than a type switch.

- A scan releases its stream when the **scan ends**, not when the caller
  returns. A caller typically rewrites the segment straight after draining it,
  and a deferred close has not run by then — on Windows an open read handle
  blocks the rename that installs the rewrite, so this was 49 failing tests
  rather than a slow leak.

- Non-sequential reads fall back to a ranged read without disturbing the
  stream's position, so a sweep that steps aside once carries on streaming.
  Block-compressed segments use the same path, since a scan visits block
  payloads in ascending physical order.

## v0.34.0 — 2026-07-27

**Breaking.** Tier ownership moves out of the log. `v0.32.0`'s writer identity
was the wrong layer, and this replaces it.

- **Removed**: `TierWriter`, `SetTierWriter`, `AdoptTierWriters`, the writer
  stamp in object keys, and the delete fence. Who owns a store is a question
  about cluster membership, and nothing the log can observe answers it — so it
  no longer pretends to.

- **Added**: `Options.TierReadOnly` and `SetTierReadOnly`. A log whose tier is
  read-only does not offload, does not rewrite a tiered segment, does not apply
  tier retention, and refuses `DeleteStoreObjects`. Reads stay entirely
  transparent, so a process that owns nothing still serves its log. Ownership
  moves by the previous owner going read-only **before** the next comes out of
  it.

- **Added**: a **single-writer contract**, stated on the `CommitLog` interface
  rather than left implicit, since it is now load-bearing. At most one process
  may have a store writable at a time. The part that is easy to get wrong: an
  ordered handover has to wait out both how long a former owner can still
  *believe* it owns the tier **and** how long a write it already issued can
  still be in flight, retries included. No amount of signalling shortens the
  second — once a request is with the storage client it will land regardless of
  what the sender has since learned.

- **Changed**: every upload now gets its **own object key** rather than a
  generation-numbered one. A deterministic key is wrong even for a single
  writer: an upload that failed ambiguously may still be in flight, so retrying
  to the same key races the attempt being retried and nothing can tell who won.
  A fresh key makes that a spare object instead. The id is random rather than
  counted, because a counter must be read from somewhere and every such place
  is state a crash, a restart or a second process can leave stale.

  This follows Kafka's tiered storage, which requires a unique id per copy
  attempt *"even when it retries ... for the same log segment data."*

  Generations are gone with it. Nothing ever recomputed a key from one — the
  offload marker records keys verbatim and is the only thing that resolves them
  — so existing objects stay readable and existing markers stay valid.

- **Changed**: `tla/MultiWriter.tla` is reframed. It no longer describes the
  log's defences, because there are none; it is the **evidence for the
  contract**. The config renamed `MultiWriter_ContractBroken.cfg` shows what
  the shipped code does when single ownership is violated — not a defect to fix
  here, but the caller's obligation backed by a counterexample instead of a
  warning.

## v0.33.0 — 2026-07-27

- **Added**: `ExportTierState` / `ImportTierState` and the `TierObject` type —
  moving a log's tier bookkeeping between processes.

  That bookkeeping (which store object currently holds each offloaded segment)
  lives in local marker files. Fine while one process owns a store; fatal when
  ownership moves. The next owner holds no markers for anything its predecessor
  uploaded, so it cannot read those objects through the log, cannot avoid
  uploading a **second copy of the same bytes**, and can never reclaim them.

  **Recovering it by scanning the store does not work.** Generations are
  per-writer — each writer derives the next from its own local marker — so one
  base offset may carry objects from two writers with nothing in the store to
  order them: not the keys, not the sizes, not the timestamps. Adoption by
  discovery would be guessing at which object is current.

  So the total order comes from whatever gives the cluster one. The caller's
  state is authoritative; the log honours a decision it cannot itself make,
  exactly as it does for `CleanSpec.Ceiling`.

  Importing a segment whose bytes are local attaches it to the existing object
  instead of uploading a duplicate, and brings those objects into the log's
  lineage so retention can reclaim them later. Guards worth knowing: the
  offsets must match what the segment holds (adoption **drops** the local
  bytes, so an object covering other records would swap a reader's data
  underneath it, silently); each object must be the size the state claims,
  since that size bounds every read of it and the local copy is gone by then;
  the whole batch is validated — objects' existence included — before any of it
  is applied; and the active segment cannot be
  named.

- **Changed**: `AdoptTierWriters`' documented condition was too weak. It said
  the identity must be one that "can no longer write". The condition that
  actually matters is that it is **no longer serving** those objects, which is
  stronger: markers are local, so a process that lost ownership still holds
  markers naming its objects and still reads through them, and nothing tells it
  to stop. Adopting a demoted-but-live peer's identity would let this log
  delete objects that peer is actively reading.

- **Fixed**: `offloadTo` left `storeGen` and `storeWriter` unset, right only by
  accident (a first offload is always generation 0), while reopening a log
  filled both from the marker — so a segment offloaded in this process
  disagreed with the same segment after a restart. The offload and adopt paths
  now share one attach step, so they cannot drift.

## v0.32.0 — 2026-07-27

- **Added**: `TierWriter` / `SetTierWriter` — an identity stamped into the
  object keys a log writes to its `SegmentStore`, for logs whose tier may be
  written by more than one process.

  It exists because **consensus cannot fence a write to an external store**. A
  node is leader at the moment it *decides*, not at the moment its PUT lands;
  its view of leadership lags, its in-flight requests can be neither observed
  nor cancelled, and the claim it would need — "I will still own writes when
  this lands" — is about the future, so no amount of waiting for finality
  establishes it. The generation does not close the window either: it is read
  from each writer's own local marker, so two writers that both believe they
  own the tier compute the same next generation and address the identical key.
  `SegmentStore.Put` has no compare-and-swap form, so that overwrite is silent
  and the loser may be the one holding current data.

  With the identity in the key, two owners cannot address the same object. A
  stale writer produces garbage to reclaim instead of corruption nobody can
  detect.

  Ids are restricted to letters, digits, `-` and `_` (max 64) and refused
  where they are supplied. This is a real constraint — a dotted hostname is
  **not** a valid id — but a `.` does not survive the key format, and a stamp
  that parses back short would fence a writer out of its *own* objects.

  Reads are unaffected. Keys are resolved from the offload marker verbatim and
  never recomputed, so objects written under a previous identity stay readable.
  That is what allows the identity to change at all.

- **Added**: `DeleteStoreObjects` and `UnreferencedObjects` — fenced reclamation
  of tier garbage.

  Fencing trades a corruption bug for a storage leak, which is better but still
  a failure, so the leak is made visible and bounded. `UnreferencedObjects`
  reports objects no live segment reads: rewrites whose superseded key was
  never deleted, uploads orphaned by a crash before the marker, and objects
  from a previous identity. It **reports rather than deletes** — whether an
  unreferenced object is safe to remove depends on whether the tier is shared,
  which only the caller knows.

- **Added**: `AdoptTierWriters` — declares identities whose objects this log may
  also reclaim, for objects the fence would otherwise strand for good. It is
  deliberately an assertion the caller makes: the claim is "that identity can no
  longer write", and nothing observable at this layer establishes it.

- **Fixed**: the writer fence is no longer applied to segments the log **holds**.
  Applying it there looked like the same defence but was a different one, and it
  was wrong in a way that only appears after a failover — when every object
  already in the tier carries the *previous* identity's stamp. It refused the
  superseded keys `CleanWithSpec` had just handed the caller, on every rewrite
  from then on, and it refused retention's removal of any segment offloaded
  before the change, so the oldest tier segment could never be dropped again and
  the tier grew without bound. Both were reproduced before being changed.

  What entitles a process to remove an object is not the stamp but its own
  marker naming it, which is the stronger claim. It costs nothing even where
  processes share a store: two of them holding markers for the *same* object
  could not safely delete it whatever stamp it carried, so the fence would not
  have saved that topology either. The fence now applies where it is meaningful
  — keys a caller learned from a store listing, where nothing establishes the
  object is theirs.

- **Added**: `tla/MultiWriter.tla` — TLA+ model of the whole protocol under
  contested ownership, including the rotating identity that produced the bug
  above. Three deliberately broken configs, one per defence: an unstamped key
  violates `NoClobber`, an unfenced delete violates `MarkerIntegrity`, and a
  fence with no lineage rule violates `EveryOrphanReclaimable`.

  The unfenced-delete counterexample is worth reading: the object is removed by
  the *legitimate new owner*, not by a stale writer. Markers are local, so
  "garbage by my own view" is not the same claim as "garbage".

## v0.31.0 — 2026-07-27

- **Added**: `CleanSpec.SkipTiered` — a pass that leaves segments in a
  `SegmentStore` entirely alone: no rewrite, and no tier retention. Local
  segments compact and retain normally.

  **No budget can express this**, which is why it is a flag and not a number.
  Both rewrite budgets guarantee at least one rewrite so debt always drains, so
  `TierRewriteBudget: 0` means "the shared budget", never "none". For a caller
  that wants to spend less on the tier that guarantee is right; for one that
  must not WRITE to the tier at all it is a hole — and a single rewrite into
  storage shared with other replicas is corruption, not duplicated work.

  Tier retention is suppressed too, because deleting a tier's copy is a tier
  write as much as uploading one is. The test asserts **zero** store puts and
  deletes rather than a reduced count, since "fewer" is not the guarantee.

  Consequence worth knowing: a pass that skips the tier cannot remove a record
  that GOVERNS one still in it — an expired tombstone, or a control marker
  below `StripBelow` — because the records it governs were not rewritten. Those
  removals wait for a pass that does own the tier.

## v0.30.0 — 2026-07-27

- **Added**: `CleanSpec.TierRewriteBudget` — a separate wall-clock budget for
  rewriting segments whose bytes live in a `SegmentStore`. Zero falls back to
  `RewriteBudget`, so a caller that sets nothing sees no change.

  The two rewrites cost wildly different things: a local one reads and writes
  local disk, a tiered one downloads the object, rewrites it and uploads the
  result — orders of magnitude slower against remote storage, and metered.
  Under one shared budget a single slow tiered rewrite could consume the pass
  and starve local compaction while local debt grew.

  **The subtlety this exposed**, recorded because getting it wrong reintroduces
  a bug this log has already had: skipping a segment for want of budget is only
  safe in the order-INSENSITIVE phase. A late segment removes a record that
  governs older ones and may only do so if everything it governs was rewritten
  in the same pass. Two independent budgets make it possible to skip a governed
  segment for want of TIER budget and then rewrite its governor anyway because
  the LOCAL budget still had room — which is exactly the orphaning the ordering
  rule exists to prevent. Once any segment has been skipped, no governor is
  rewritten at all.

## v0.29.0 — 2026-07-27

- **Added**: `CommitLog.ReadMessageSet(offset, maxBytes)` — the read counterpart
  to `AppendMessageSet`, returning the log's own framing verbatim so a follower
  replicates bytes instead of reconstructing it. `AppendMessageSet` was
  documented for replication, but nothing exported returned raw message-set
  bytes: `messageSet` and `msgSetHeaderLen` are internal, so a follower had to
  hand-roll the framing and would break silently when it changed.

  Contract details, each of which is a way this could have been useless:

  - **Whole frames only.** A `maxBytes` smaller than the first frame returns
    that frame rather than a truncation — a partial message set is not something
    a follower can append, so starving it is worse than overshooting the budget
    once.
  - **Records above the high watermark are included.** Replication is how the
    watermark advances, so withholding them would deadlock it. A follower that
    cares about the commit boundary applies its own.
  - **An offset below the oldest surviving record clamps up to it**, as the
    readers do, so a follower resuming from a position retention has passed
    carries on rather than failing. `ErrSegmentNotFound` only when the log holds
    no segment at or after the offset.
  - A single call does not cross a segment boundary; the caller continues from
    the last offset it appended.

  Tested by round-tripping a log into a second one through `AppendMessageSet`,
  in bounded chunks, and comparing the result.

## v0.28.0 — 2026-07-27

- **Breaking / Fixed**: offloaded segments are no longer exempt from compaction
  and retention (C6). This is the change the previous four releases were
  building toward, and the reason it matters is correctness, not disk use:
  whatever garbage a segment held when it offloaded was frozen there
  **permanently** — a tombstone that offloaded before it could be collected
  never took effect, and every value it shadowed was kept with it.

  `clean` no longer holds the offloaded prefix aside. A rewrite of an offloaded
  segment becomes the next generation of its store objects
  (`ReplaceOffloaded`), and per-tier retention means those bytes count toward
  the tier's budget rather than escaping every limit.

- **Breaking**: `CleanWithSpec` now returns the **superseded store objects**
  alongside the verified floor: `(verified int64, superseded []string, err
  error)`. They are deliberately not deleted — a reader that opened the segment
  before the rewrite holds a backing over the old key and is entitled to finish,
  and where replicas share a tier those writes belong to whoever holds
  tier-write ownership. **A caller that ignores them leaks one object per
  rewrite.** Empty for a log with no `SegmentStore`.

  Two bugs found by fuzzing this, both worth recording:

  - An **option-1** offloaded segment keeps its index on local disk, and the
    rewrite replaced the index only for option 2. The stale index pointed seeks
    at positions in the superseded object, so the log read back short and lost a
    key outright. The rewrite's index is now installed over it — last, because
    reopening it re-derives the segment's boundaries and needs the new block
    mode, blocks and position.
  - The fuzz harness itself discarded the superseded keys, which is exactly the
    leak the contract warns about; its own no-orphans assertion caught it.

## v0.27.0 — 2026-07-27

- **Added**: `MaxTierBytes`, `MaxTierMessages` and `MaxTierAge` — retention
  becomes **per tier** (C5). A segment over the local budget that also exists in
  a `SegmentStore` has left the tier those limits govern rather than being
  deleted; the record is gone only when the last tier's limit is reached.

- **Changed**: `MaxLogBytes`, `MaxLogMessages` and `MaxLogAge` now bound LOCAL
  disk alone and no longer count offloaded segments. Counting them deleted
  records to reclaim space that offloading had already reclaimed — the budget an
  offloaded segment belongs to is its tier's.

  In practice this changes nothing yet, because the cleaners still never see
  offloaded segments; C6 removes that exclusion. It is the behaviour that will
  apply once they do.

  A log with no `SegmentStore` has no offloaded segments, so the split is a
  no-op and retention behaves exactly as before.

  One asymmetry worth knowing: the local pass always retains the last segment,
  because it is the one being appended to. A tier has no such segment, so tier
  retention may reclaim every object in it — otherwise the oldest could never be
  collected.

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
