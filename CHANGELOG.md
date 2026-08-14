# Changelog

A file-backed write-ahead log with block compression and transaction-aware
compaction. Extracted from [liftbridge-io/liftbridge](https://github.com/liftbridge-io/liftbridge)'s
internal commitlog package in June 2024; this changelog covers the standalone
library from that fork onward.

## Unreleased

### Added

- **`InspectIdentity(path)`** reads the identity stored beside a log without
  opening it — the descriptor twin of `InspectSegment`. No directory lock, no
  recovery, no segment opened, nothing written.

  ```go
  type LogIdentity struct {
      Identity []byte // the stamp, nil when the log stores none
      Stored   bool   // whether the log carries one AT ALL
  }
  func InspectIdentity(path string) (LogIdentity, error)
  ```

  Four answers, because the caller acts differently on each: an identity;
  a log with **no** identity (`Stored` false, nil error); **`ErrNoLog`**; and
  anything else, which means *cannot judge*.

  Requested by durable_streams (#625) for their reclaimer's periodic orphan pass,
  which judges logs it must **not** open — `New` would take the directory lock,
  run recovery and possibly adopt a descriptor on a copy the pass is about to
  delete, mutating the evidence it exists to judge. Their alternative was a
  descriptor parser on their side, which is exactly the stale mirror `inspect.go`
  was written to eliminate.

  `ErrNoLog` covers a missing directory **and** a directory holding no
  descriptor, deliberately. Every log that has been through `New` has one, so a
  directory without one is not a log — and collapsing those two keeps the other
  answer sharp: `Stored` false then means *a real log, created before its owner
  used identity*, which is the one a reclaimer must never delete, since unstamped
  and stale are indistinguishable by identity and only one of them should be
  destroyed.

  **Local only, by definition.** A store-backed log keeps its descriptor in the
  store, and a path cannot reach one, so this answers for *this directory's*
  copy — which is the question a reclaimer judging this broker's disk is asking.
  A tiered log with no local descriptor is `ErrNoLog` here, not an
  identity-less log.

### Fixed

- **A block table's size is checked before it is allocated.**
  `(*segment).fetchBlockTable` read `store.Size(blocksKey)` and passed it
  straight to `make([]byte, size)`. Every length check in `decodeBlockTable`
  happens *after* that allocation, so none of them could protect it.

  A **negative** size was not a small read — it was `makeslice: len out of
  range`, a panic, in the caller's process, out of a library. A large one was
  simply taken: a remote store deciding how much of this process's memory it
  gets.

  Both ends are refused now, and the upper bound is *derived* rather than picked,
  in the style of `maxDescriptorBytes`: the table is fixed-width and every block
  occupies at least `blockHeaderLen` physical bytes, so a segment of
  `physPosition` bytes needs at most
  `blockTableHeaderLen + (physPosition/blockHeaderLen)*blockTableEntryLen + 4`.
  Anything past that could not decode whatever else were true of it.

  This was the **fourth** reader of a caller-supplied store and the last one
  without the check — `readStoreDescriptor` refuses a non-positive size and
  bounds the rest, `readTierManifest` refuses a non-positive size, and the remote
  index cache's `fetch` now does too. The first of them has documented this exact
  hazard since it was written.

- **An index object a store reports as zero bytes is refused rather than
  fabricated.** `RemoteIndexCache.fetch` drove its download from
  `store.Size(key)` and never checked the value. A size of zero is the one
  length between the two checks already standing there — the `(0, nil)` contract
  breach inside the loop, and the ended-early check after it — because that
  second check compares the download against *the same number that is wrong*, and
  `0 == 0` passes.

  What got through was not a missing index but a **fabricated** one: `newIndex`
  pre-allocates when it finds an empty file, which is the arm a genuinely *fresh*
  index takes, so the empty download was indistinguishable from a new index and
  got 10 MB of zeroes mapped and read as that segment's table. Seeks then
  *answered* instead of failing. The entry was also recorded as `bytes: 0`, so it
  never counted toward the cache's total, could never be evicted for size, and
  left the budget reading as empty while the disk filled.

  `readStoreDescriptor` already refuses `size <= 0` one reader over; this now
  matches it.

- **Every byte budget that bounds a RESOURCE now counts the bytes that are
  there.** `Options.MaxLogBytes`, `Tier.MaxBytes` and `LocalBytes()` all summed
  `(*segment).Position` — the segment's *logical* extent, which on a
  block-compressed log is the size the bytes decompress to and not the size of
  anything that exists. They ask `(*segment).PhysicalSize` instead, which is the
  file (or the store object, for an offloaded segment).

  The error ran **both ways**, so neither "it errs on the safe side" nor "it errs
  on the generous side" describes it:

  | fixture | `Position` vs disk | consequence |
  | --- | --- | --- |
  | bytes that compress | too **large** | retention hit the limit early and deleted records the budget had room for — a 20:1 log threw away most of what the caller asked to keep |
  | bytes that do not compress | too **small** | stored raw, but still 11 bytes of block header each, so a small-append workload overran the limit it was given |

  `MaxSegmentBytes` is deliberately **not** changed: it is the one byte setting
  that is not about a resource. What a roll bounds is everything sized by the
  segment's extent — its offset span, its index, a compaction pass's working set
  — and none of that shrinks when the bytes compress. `Options` now documents the
  pair, and `CheckSplit` says at the comparison which measure it wants and why.

  Nothing caught this because the entire byte-retention suite runs on
  **uncompressed** fixtures, where the two measures are equal by construction:
  the tests stood exactly where the defect cannot occur. The third instance of
  that shape this sweep. Four new tests set `Compression` and place the budget
  *between* the two numbers, so the choice of measure alone decides the outcome
  rather than the margin, and three new guards hold the three call sites — each
  is separately removable and each fails differently.

### Changed

- A `KeyPrefix` read over a **block-compressed** segment now plans its runs in
  physical bytes. `planRuns` ended a run wherever the *logical* byte gap to the
  next wanted record exceeded the coalesce budget. Index positions are logical,
  and on a raw segment the logical space *is* the file, so that gap is exactly
  the bytes a split avoids.

  A block segment is billed differently. Nothing smaller than a whole block is
  ever transferred or decompressed (`blockCopyIntoCache`), and `fetchRuns` hands
  every run its own single-entry `blockCache` — so a split *inside* one block
  avoids nothing and repeats a fetch, pulling and decompressing the same block
  once per run that touches it. Measured on a tiered log with ~50KB blocks and a
  hit every ~6KB, at the 4KB tier default:

  | budget | requests | bytes |
  | --- | --- | --- |
  | 4096 | 120 | 314799 |
  | 16384 | 3 | 7827 |

  Forty times the requests **and** forty times the bytes for the same 120
  records: the setting inverted, not merely mistuned. A budget below a block's
  compressed size cannot buy anything, and that is where the default sits —
  `cleanBlockTarget` is 256KB uncompressed against a 4KB tier default.

  The gap is now the physical distance between the block holding the previous
  record and the block holding this one. Two records in one block have a
  negative gap and are never split; blocks lying entirely between two hits are
  the only thing a split can skip. The raw path is untouched, and
  `TestPrefixReadCostProfile` reports the same numbers as before.

  Why it went unmeasured: that cost profile's fixture sets no `Compression`, so
  every number it established — including the ~4.4KB tier breakeven — was
  measured on segments with no blocks at all. A cost test can name the path it
  means and stand on another one.

- **A guard lost its coverage to the change below it, and CI caught it.**
  `TestKeyPrefixRefusesRecordsThatFailCRC` and its tiered sibling never cleaned,
  so once the digest-less scan path existed they ran on *that* route while their
  names, their doc (*"collectRun is reached for both"*) and their guard all said
  `collectRun`. Neutering `collectRun`'s CRC check stopped failing them.

  Both fixtures now run a clean, so they exercise the planned route they are
  named for, and a third test covers the scan route with a guard of its own. The
  old guard's `^TestKeyPrefixRefuses` selector was also tightened to name its two
  tests: a selector matching tests that cannot see the guarded line is one bad
  fixture away from silently reporting nothing.

- A `KeyPrefix` read over a sealed segment with no key digest now scans the
  segment once and filters, instead of building a whole digest and discarding
  it. This is not an edge case: the compact cleaner is the only thing that
  persists a `.keys` sidecar, so on a log that never compacts *no* sealed
  segment has one and *none ever will* — every prefix read paid this, forever.

  The old fallback was strictly more expensive than the scan it was avoiding.
  `buildKeyDigest` reads every record in the segment **and** holds a map over
  every distinct key in it; the digest was then thrown away and the offsets it
  named were read a second time. The map is the part that matters:
  `loadOrBuildDigests` caps itself at two concurrent builds because ten of them
  over ~40MB segments measured >1GB, and nothing capped this path at all —
  `PrefixReadConcurrency` bounds record *reads*, not digest builds, so the
  number in flight was however many readers happened to be doing prefix reads.

  Records returned are unchanged, which `TestReaderKeyPrefixMatchesScan` has
  always pinned by running its whole comparison with no sidecars present. The
  new path keeps the same CRC refusal `collectRun` performs (verified by
  mutation: `FuzzCorruptedRecordIsNeverServedSilently` catches its removal on
  the keyprefix route) and the same within-segment `SkipSuperseded` rule
  `digestHits` applies.

  A second, older inefficiency fell out of it: the search loop visited every
  segment **twice**, because `pop` walks the resume offset back to the last
  record it served, leaving a fully drained segment still looking unfinished.
  With a digest that second visit was nearly free, which is why it survived
  unnoticed; it is now skipped outright via `servedThrough`, so the digest path
  saves a redundant plan per segment as well.

  Two cost tests were measuring the wrong path and are corrected: `costLog` and
  `offloadedPrefixLog` both set `Compact: true` with `DisableAutoClean: true`
  and never cleaned, so their segments had no digests and every measured run
  silently paid a full rebuild scan underneath the numbers it was comparing.
  Both now clean before offloading.

- Dropped the `github.com/dustin/go-humanize` dependency. It was in `go.mod`
  for exactly one call — `english.Plural` in a leader-epoch parse error, so
  that a count could say "entries" instead of "entrys". The count there is
  always plural-or-zero, so the message reads identically without it.

  Found by a dependency-necessity pass over `go.mod`. The same pass looked hard
  at the two snappy implementations this module links (`golang/snappy` behind
  the Snappy codec, and the drop-in inside `klauspost/compress`, which is
  already here for S2 and Zstd) and **kept both**, which is worth recording
  because the tidy-up looks obviously right: measured over `sampleMessageSet`
  batches, the drop-in compresses 1.4%–16.2% larger and encodes ~2x slower.
  Its stricter sibling refuses an S2 block where the drop-in accepts one, but
  that is *not* the reason — a block header has no checksum, yet `decodeBlock`
  already refuses a length that disagrees with the header and the frame CRCs
  sit under that, so the refusal is redundant. The measurement now lives on the
  `Snappy` constant, where the tidy-up would start.

### Fixed

- Four error messages named no subject. `"stat file failed"` appeared three
  times — twice inside `newIndex`, on the stat before pre-allocation and the
  stat after it, so a failure told an operator nothing about which one broke.
  `"open file failed"` was the index file in one place and the segment log in
  the other; `"path is empty"` was `Options.Path` in one and the index path in
  the other; and `reopenLocked` wrapped all three of its steps — open backing,
  positions, index — in one sentence. Each names its subject now. The index
  pre-allocation `Truncate` also returned a bare OS error with no context at
  all, which is the same defect with the message missing rather than
  duplicated.

  Found by scanning every error text in the non-test tree: 207 distinct, seven
  duplicated. The other three duplicates are correct — two platform-split pairs,
  which are one error with two implementations, and `"commitlog: negative read
  offset %d"`, which is one rule refused identically by both readers.

### Testing

- **A method could leave the public interface without anything going red.**
  `New` returns `CommitLog` and `commitLog` is unexported, so that interface is
  the entire API reachable from outside the package — and it is a
  hand-maintained transcription of the concrete type's exported method set with
  nothing keeping the two in step.

  The suite could not have noticed, for a structural reason: `setup` and
  `setupWithOptions`, the helpers behind essentially every test here, return
  `*commitLog`. Tests hold the concrete type. So deleting a method from
  `interface.go` still compiles — the struct keeps it, the helpers keep calling
  it — and `go build ./...` is clean. Verified rather than argued: removing
  `UnreferencedObjects` builds the whole module and passes every test that
  exercises it, while no caller of `New` could reach it any more. The reverse
  direction, a method added to the struct and forgotten in the interface, is the
  same hole seen from the other side.

  The two sets agreed exactly, 40 and 40 — the discipline having held, not
  anything enforcing it. Now checked by reflection, so it stays true without
  being maintained, and by guard 176, whose mutation is the real refactor.

  The obvious follow-up — that forty methods is too many — is answered
  downstream and the answer is no: durable_streams defines its own seven-method
  `StreamLog` and asks for the rest with a type assertion. commitlog's interface
  is a return type, not a bill for implementors, so keeping it honest is the
  right response rather than shrinking it.

- **Stripping's floor was only ever tested from below.**
  `TestCleanDigestMergeEquivalence` asserted that records below `StripBelow`
  lose their headers and never that records at or above it KEEP them, so a
  strip that ignored the floor and took the headers off every record it rewrote
  passed. The evidence had been sitting in the fixture the whole time: a
  `hasPid` field, recorded per record since the test was written, that nothing
  read. Falsified before landing — dropping `offset < spec.StripBelow` from both
  data arms of `classify` turns it red — and guarded. The count is floored
  across all five seeds rather than per seed, because a seed whose
  header-carrying records all land under the floor makes the assertion vacuous
  and failing *that* seed would say nothing about stripping.

- **`TestChaosAReadFromThePublishedFloorStartsAtIt` assumed two of its
  dangers.** It already retires on danger rather than duration, but "a
  truncation ran" is not "a truncation REWROTE the boundary segment" — a cut
  landing on a segment's own base drops whole segments and never builds the
  replacement a reader can be holding the original of, which is the entire
  mechanism of the bug it exists for. And "a read was taken" is not "a read
  overlapped a trim in flight". Both are counted and floored now. Establishing
  the overlap needs a trim SEQUENCE number rather than a boolean: the truncator
  hammers without a sleep, so `trimming` is true at both ends of almost any
  read, including one that straddled two different trims.

  Floors chosen from measurement (rewrites 19–32, overlaps 11706–11789 of
  ~12030 checked over six runs) and set well under the low end, at 10 and 2000,
  so a loaded runner cannot fail on its own precondition. The floor is on
  rewrites rather than on truncation calls deliberately: the call count swung
  from 33 to 61,497 across those runs, since a repeat cut at an unchanged floor
  is a no-op that returns nil. Both falsified in
  isolation: cutting at the boundary segment's own base drives rewrites to 0,
  and not announcing the trim window drives overlaps to 0.

- `keyDigest.keyedLen` was parsed, stored and read by nothing. `newDigestIter`
  seeks to `keyedOff` and reads exactly `nKeys` entries; where the section ends
  is not needed and is recorded nowhere else either. The struct doc listed it as
  part of the location record, so the doc described a mechanism that did not
  exist. Removed.

## v0.87.0 — 2026-08-14

### Performance

- **Opening a block-compressed tier without a `RemoteIndexCache` made a remote
  round trip per segment.** With the index kept local (option 1, which
  durable_streams reports is their default tiered mode) `setupIndex` derives the
  segment's last record from its index — and a block index anchors *blocks*, not
  records, so the last anchor is a block's first message and finding the segment's
  end means reading that final block back. For an offloaded segment that block is
  in the store. Measured on an ordinary reopen of a 90k-record snappy tier:
  40,947 bytes across 8 requests, one per segment, before serving anything.

  Every manifest entry already carries `LastOffset` and `LastWriteTime`.
  `setupIndexKnownEnd` takes them from the caller, so an open of a cache-less
  tier now reads **zero** bytes of segment objects — the same promise the cached
  configuration has kept since v0.71.0. Only the block branch is overridden: a
  raw index has one entry per record, its last entry *is* the answer, and reading
  it costs nothing, so overriding there would only hide a disagreement between
  index and manifest.

  The reason nothing caught it: the test asserting that an open reads no log
  objects only ever ran with a cache configured, which is the configuration that
  cannot reach this path. It has a cache-less sibling now, covering both boot
  paths — including an assertion that adopting into a *fresh* directory still
  downloads, because that rebuild is real and necessary and a test that only
  checks for zeros would certify a version that skipped it everywhere.

### Fixed

- **The manifest adopt sorted the live segment array in place.**
  `segmentsSnapshot`'s doc states the obligation on everyone who changes the
  segment set — readers index a snapshot *without* holding `l.mu`, so writing an
  element in place is a data race against all of them, which is what v0.44.2 was
  spent on. `adoptTierManifestLocked` appended to `l.segments` and then called
  `sort.Slice` on it, and `sort.Slice` swaps elements in place.

  Not reachable today: it runs inside `open()`, before there is a log for anyone
  to hold a reader on. That makes it safe by the schedule rather than by the
  function, and the first maintenance path to adopt a manifest another process
  published would have taken the schedule away without touching this code. It
  builds a new array now, published on every exit including the error paths —
  which is what the direct appends it replaces did, and it matters because a
  segment opened before a failure holds a file handle that only the log closing
  its segments releases.

  No test could have caught it — there is no reader to lose the race — so the
  rule is a script now rather than a sentence: `hack/cowsegments.sh`, in CI
  beside `layercheck` and `atomicwrite`. It refuses an element assignment, any
  sort, and `copy` into the slice, across tests as well as production, and it
  fails loudly if `segmentsSnapshot` ever stops handing out the live header —
  otherwise it would go on passing while enforcing a rule that had ceased to
  exist. Both halves were falsified before landing.

### Changed

- The last place a reader moved part of its cursor by poking a field is now a
  named operation. `committedReader.readLoop` falls through to a direct
  `seg.ReadAt` for a read that crosses the high watermark or outruns the 64 KB
  buffer, and then had to hand the cursor back with `r.br.pos = r.pos`. That is
  `bufReader.advanceTo` now, whose doc carries the sentence the assignment could
  not: keeping `bufStart` and `data` is the *point* — a written byte never
  changes, so a buffer filled before the direct read is still true of the
  segment after it — and `reset` would also be correct while throwing that
  buffer away. Guarded, because the failure is a stream off by the size of one
  large message and it surfaces as a CRC error on the healthy record after it.
  No behaviour change.

- Three sites open-coded `segmentsSnapshot()` under their own `RLock` and then
  copied the result — `ReadMessageSet`, `forEachSegment` and `movePlaced`. With
  the set copy-on-write the copy defends against nothing, and it reads as though
  it *were* the safety, which is how the rule above stops looking obligatory to
  whoever writes the fourth one. All three call the function now.

### Testing

- `TestDisableAutoClean` waited a fixed 400 ms before asserting that nothing had
  been cleaned. Its claim is a negative — *the loop did not clean* — so that
  assertion holds equally when the flag works and when the cleaner never got a
  tick's work done inside the sleep, and on a runner slow enough for the second
  reading the test passed having proved nothing. `CleanerInterval` is 50 ms, so
  400 ms reads like eight intervals of margin; a tick is a roll check, a
  retention scan and a segment delete, and a margin is a constant racing real
  work. The wait is priced now: an identical log with the flag OFF is waited on
  via `require.Eventually` until it actually cleans, and that measurement sets
  the disabled log's wait. The `Eventually` fails loudly, naming the fixture, in
  precisely the state that used to make the real assertion vacuous.

- `TestRetentionNeverWritesIntoASliceAReaderIsHolding` reproduces the v0.44.2
  in-place segment write, and by design asserts nothing itself — the race
  detector is the assertion. A detector that finds nothing is therefore
  indistinguishable from a test that never performed the operation, and this one
  had two ways to perform none: `TruncateBefore`'s error was discarded outright,
  and its rewrite branch runs only when the cut *straddles* a sealed segment,
  which `newest - 20` merely hoped for. The cut is now one past a sealed
  segment's base — a straddle by construction, which a concurrent roll cannot
  spoil because rolls only append — and the rewrites and non-empty reader
  snapshot walks are counted and floored. The rewrite count checks segment
  identity, not just the resulting base offset: an untouched segment already
  starts at its own base, so the offset alone counts a whole-segment delete as a
  rewrite.

- Eight tests were renamed so their names state which method they drive.
  `Truncate` drops the log's suffix, `TruncateBefore` its prefix — different
  paths with different lock discipline — and the tests told them apart by word
  form alone: `TestTruncating…` meant `Truncate`, `TestATruncation…` meant
  `TruncateBefore`, and nothing recorded that. `TestATruncateUnlinks…` and
  `TestATruncationUnlinks…` sit 37 lines apart in one file, drive different
  methods and differ by two letters. All eight were correct; the hazard is the
  next edit. Found by a whole-function near-duplicate scan, which also reported
  the production tree clean — one pair over the floor, and it is
  `digestDecoder.uvarint`/`.varint`.

## v0.86.0 — 2026-08-14

### Changed

- **Breaking.** `Options.AdoptOptions` no longer adopts `Options.Identity`. It
  adopts the log's gating settings and nothing else; the deliberate re-stamp is
  now `Options.AdoptIdentity`, which is the only thing that writes an identity
  onto an existing log and the only way to resolve an `IdentityConflict`.

  One flag was making two statements, and the cost fell on exactly the caller
  identity is for. A caller whose settings come from a catalog rather than a
  config file has to pass `AdoptOptions` on *every* open — there is no other way
  to say "the catalog is authoritative" — and adoption returned a nil conflict
  for that whole branch, so it could never be told an identity disagreed. The
  signal was suppressed by the one thing every one of its opens did.

  Adopting settings now reports a conflict like any other open, and publishes
  nothing while one stands: a caller holding the wrong log's identity is the
  caller whose settings this log has least reason to write. This also removes
  the identity carry-over added in v0.85.0 rather than moving it — the adopting
  branch builds from the STORED record like every other now, so there is nothing
  to carry.

  Migration: a caller that passed `AdoptOptions` to re-stamp an identity passes
  `AdoptIdentity` instead. A caller that passed it to retune settings changes
  nothing, and starts seeing conflicts it was previously never shown.

- Sentinel matching in the read and roll paths moved from
  `pkgErrors.Cause(err) == X` (and one bare `==`) to `errors.Is`. `Cause` walks
  a `causer` chain and stops at a `%w` one, and this package writes
  `fmt.Errorf("%w: ...")` in a dozen places — so `readOne` and
  `ReadMessageMetadata` retried a compaction swap on a comparison that worked
  only because nothing between the segment and the reader wraps that way *yet*.
  `segmentSwapped` also stated its predicate twice, `Cause(err) == X` OR
  `errors.Is(err, X)`, where the second subsumes the first. No behaviour change.

- The three operations that only mean anything for a segment backed by a store
  — replacing its objects, swapping the replacement in, and repointing it at a
  different store — refused a local segment with three separate `errors.New`
  carrying the same sentence. One unexported `errSegmentNotOffloaded` now, so
  one condition has one wording. No behaviour change.

- The readers no longer move `seg`, `pos` and `br` by hand. `segmentCursor` grew
  two operations and every site calls one of them: `seekTo(seg, pos)` for a move
  — which is the rule "all three fields change together", now enforced by there
  being one place that changes them — and `refill()` for re-anchoring the buffer
  where the cursor already is, which is a different thing that had been spelled
  the same way. The two boundary advances, one per reader, were the pair the
  extraction was for; they had been written in different statement orders.

- `uncommittedReader.Read` advanced to the next segment in two places, chosen by
  a `waiting` flag: one for a roll it walked into, one for a roll that woke it
  out of `waitForData`. They are one rule — drained, so take the next segment if
  there is one and otherwise park — and it is now written once, without the flag
  or the loop label. The segment snapshot is also re-taken at each boundary
  rather than once at entry: the arm that searched the entry-time snapshot could
  hold it for as long as the writer stayed idle, which is precisely how a parked
  reader would fail to see the roll it is waiting for. Two new tests drive a roll
  with the reader parked and with it not, and each was falsified against the old
  arms separately before the merge.

- `uncommittedReader` and `committedReader` now embed a `segmentCursor` — the
  segment they are positioned in, the byte offset into it, the buffered reader
  over it, and the mutex guarding all three — so `contextReader.segmentBounds`
  is written once instead of twice, byte for byte. The base stops short of the
  readers' other shared fields on purpose: `noWait` names a different condition
  in each of them. No behaviour change.

- `SegmentStore.ReadAt`'s contract said "the two places that read a caller's
  store" compared `io.EOF` with `==`. There were three. The sentence — written
  by the fix to two of them — now says so, and says that a call-site count in a
  comment is a claim nothing verifies.

## v0.85.0 — 2026-08-14

### Fixed

- **A store that wraps its errors could not have a cold index downloaded at
  all.** `SegmentStore.ReadAt` documents that its sentinels may be wrapped and
  that commitlog compares with `errors.Is` — a sentence added when the two sites
  in `storeBacking` were fixed. `RemoteIndexCache.fetch` is a third site that
  reads a caller's store and kept the `==`, so an ordinary end-of-object arrived
  as an outage: every cold seek into an offloaded segment failed with "read
  remote index", after writing the entire index to disk and then deleting it.

  Two more holes in the same loop. An object that ends before the size the store
  *itself* reported one call earlier now fails instead of leaving a short cache
  file that `newIndex` maps and reads as a whole index — that produced "not
  found" for offsets the segment holds, silently, from a fetch that reported
  success. And a `(0, nil)` return, which `io.ReaderAt` forbids, is named as the
  contract breach it is rather than retried at the same offset forever.

- **Two concurrent seeks into the same cold segment could truncate each other's
  index cache file.** `RemoteIndexCache.acquire` downloads outside its lock, on
  purpose, and dealt with a concurrent download of the same key by keeping one
  and discarding the other. That is the right shape only when the two attempts
  are independent, and these were not: the cache file is named from the object
  key alone, so both wrote the *same* path. The second `os.Create` truncated a
  file the first had already opened and mmapped — a SIGBUS in the first
  reader's next seek on Linux, and on Windows a discard that deleted the
  winner's file out from under it.

  Reachable by ordinary use rather than by a pathological caller: `withIndex`
  runs under the segment *read* lock, so two readers seeking into one offloaded
  segment race here by design. One download now runs per key, and a second
  acquire waits for it.

- **The descriptor and the leader epoch checkpoint were written atomically but
  never made durable.** Both called the atomic-file library directly instead of
  `AtomicWriteFileWithRetry`, so they got torn-write safety and nothing else. An
  atomic write ends in a rename, and a rename that has returned is visible to
  every later reader in this boot while still being undoable by a power cut —
  the directory holding the new name needs its own fsync.

  It matters most for the descriptor, which is what says a log exists and what
  it is. Losing it leaves a directory of segments that `readDescriptor` refuses
  on every subsequent open: the same bricked state the `Delete` ordering fix was
  made to prevent, reached by a different route. Both writes now go through the
  wrapper, which also rides out the Windows `ReplaceFile` failure that any open
  handle on the destination causes — including the one a supervisor's restart
  meets when the killed process's handles have not been reclaimed yet.
  `readDescriptor` waits that window out too, where it used to fail the whole of
  `New()`.

  `hack/atomicwrite.sh` now keeps that rule: only the wrapper's own file may
  reach the library. The rule had been written down before — as a list of
  callers in a doc comment — and a hand-written list cannot notice a caller that
  never joins it.

- **A key digest could not be published while anything held the previous one
  open.** The rename that installs it was bare, so on Windows a scanner's handle
  — or a reader of the previous digest that had not been reaped — failed the
  publish outright, and the pass then rebuilt the digest by walking the whole
  segment on its next tick, having just walked it to produce the one it could
  not install. It retries now, on `tickWriteRetryBudget` rather than the five
  seconds a waiting caller gets, because a lost digest is free: every caller
  rebuilds it from the segment when it is absent.

- **Adopting a log with no identity erased its stored stamp.** `AdoptOptions`
  skips the descriptor comparison and republishes the caller's record whole,
  identity included — so a caller that does not use identity at all, adopting
  only to retune a gating setting (which is what the option exists for),
  published an empty one. `renderDescriptor` omits an empty identity entirely,
  so the stamp did not become wrong, it ceased to exist.

  That is the state `Options.Identity` exists to make unreachable: an unstamped
  copy and a stale one look identical, so the owner can reclaim neither. The
  option's own doc already promised the opposite — "empty means the caller does
  not use this, and never conflicts with anything" — which held on the normal
  republish path, where the record is rebuilt from what is stored, and not on
  this one. An absent identity is carried over now. A caller that means to
  re-stamp still does by supplying the bytes, and adopting remains the
  documented way to resolve an identity conflict.

### Changed

- `publishTierManifests` merged its pending overrides by walking the pending
  list twice: once to replace matching tier-state entries, then again to ask a
  map whether each entry had survived the first walk. Whatever the first walk
  leaves in the map *is* the set of additions, so it is appended from the map
  now. The second walk needed a second `delete` to stop two pending entries
  sharing a base offset from being appended twice — deduplication written as a
  side effect of a lookup, with nothing saying so. No behaviour change.

- Two comments still described the frame header as unchecksummed, one of them
  contradicting a paragraph four lines above it in the same block. The header
  has carried its own CRC since it grew one, verified before any field is read,
  so `readOne`'s segment-range cross-check is no longer the damage check its
  comment claimed — it survives because a header that passes its own checksum
  still says nothing about *where* it was found (a stale index position, a
  half-installed `Replace`, a seek into the neighbouring file). Documentation
  only.

- `newMessageSetFromProto` maintained a running byte counter, advanced by six
  hand-written `+=` lines that transcribed the frame header's field widths a
  second time, in order to compute one thing: where in the buffer each record
  starts. The buffer already knows, and the header checksum three lines below
  already asked it. The counter is gone; `buf.Len()` answers directly. A width
  changed in the encoder and not in the counter would have moved every index
  entry's `Position` without changing a byte on disk. No behaviour change.

- `setupIndex` ended its block-mode and raw branches with the same five lines —
  read index entry 0, assign `firstOffset` and `firstWriteTime` — when only the
  *last* offset is layout-dependent. Entry 0 is the segment's first message
  under both layouts, so that read now happens once, above the split. No
  behaviour change.

- `digestHits` spelled its `[from, bound]` window test at three sites, the third
  of them about a differently named variable, with nothing comparing the copies.
  One `inWindow` closure now. No behaviour change.

- `readMessage` and `readMessageMetadata` shared a verbatim copy of the frame
  header read, CRC verification and field extraction, which had already drifted
  by one error message. Both now call one `readFrameHeader`. No behaviour
  change.

- `openWithRetry`, `renameWithRetry` and `ReadFileWithRetry` each carried their
  own copy of the same retry loop, and each stated the same rule in its own
  prose — absent returns at once, locked is waited out. One implementation now,
  which also means the guard on that rule covers all three instead of one: the
  two ends of a single Windows window were among the copies it did not reach.
  No behaviour change.

- The key digest's varint writer was declared twice, character for character, in
  `buildKeyDigest` and `encodeKeyDigest`. Both now use one small writer type. No
  format or behaviour change.

- The list of store objects a rewrite, a join or a tier move supersedes was
  built by hand at three sites, each with its own wording of the same two facts.
  It is one `supersededObjectsLocked` now, beside the `objectKeysLocked` the
  reclaim sweep spares. The consumer of that list *deletes*, so an entry with
  the wrong pin removes an object a reader is still on and a missing entry leaks
  one forever; three transcriptions were three chances for the next object kind
  to reach only two of them. No behaviour change.

- `findEntry` and `findEntryByTimestamp` were verbatim copies of one another but
  for three predicates — the sparse-index anchor, the frame test and the index
  test. One `findEntryBy` now takes those three and owns the body. What they
  also copied was the five-line comment on what a segment answers once it has
  left the log, and a rule written down twice is a rule that can be corrected in
  one place. No behaviour change.

- Three more transcriptions found by counting repeated runs of code rather than
  by reading: `keyOffsets`/`valueOffsets` were one length-prefixed field read
  twice (including the "-1 means absent, and nothing follows it" rule), both
  `Stream` implementations ended in the same seek-or-close-the-handle tail, and
  `readAtLocked`/`scanReadAt` each spelled out when a shut segment answers
  `ErrSegmentReplaced` rather than `ErrSegmentClosed`. One `fieldOffsets`,
  `streamFrom` and `shutErrorLocked` now. No behaviour change.

- Two more from the same count: both segment constructors opened with the same
  seven-field literal, `firstOffset`/`lastOffset` sentinels included, and both
  background loops wrote out the same ticker-and-select. `emptySegment` and
  `tickUntilClosed` now. The sentinels are the reason for the first — offset 0
  is a real record, so a constructor that forgot the `-1`s would report the
  log's first append as already present — and the select's ordering is the
  reason for the second: the closed arm returns before the body runs, so a
  shutting-down log gets no extra pass. No behaviour change.

- `TruncateBefore` no longer holds the kept region of a boundary segment in
  memory. It collected every record at or above the cut into a slice, then
  created the trim and wrote them, because `Trimmed` needs the new base offset
  before the destination exists — an `int64` the *first* kept record already
  had. The trim is now created on that record and the rest stream into it, the
  way `Truncate` has always done the mirror-image job. On a boundary that
  straddles the cut the buffer was bounded only by `MaxSegmentBytes`, so a
  retention pass over large segments allocated a segment's worth of records to
  learn one offset. No behaviour change.

- A log now settles on one spelling of its directory. `New` resolved the
  absolute path and used it for the directory lock, the leader epoch cache and
  the descriptor, while `l.Path` and both cleaners kept whatever string the
  caller passed — two names for one directory inside one log's reach. They
  agree exactly as long as the process never changes working directory, and if
  it does, the half on the relative path opens files somewhere else while the
  half holding the lock does not notice. The resolve also discarded its error,
  so a failed `Getwd` would have built the log at the filesystem root instead of
  reporting that the working directory was gone; it is returned now.

  `commitLog.init()` went with it: a `MkdirAll` on a directory `New` had
  created forty lines earlier, with no other caller.

- `newSegment` no longer takes a bool and an empty string at every call site.
  Its signature ended `..., isNew bool, suffix string, codec` and all 25 callers
  spelled out `true, ""` or `false, ""` — putting the one decision that matters,
  refuse an existing file or adopt it, behind a bare `true` that reads as noise,
  beside a string that never varied except through `newWorkingSegment`. It is
  `newSegment` (create, refuse an existing file) and `openSegment` (adopt what
  the directory listing found) now, over a shared `newSegmentWith` that keeps
  both parameters for `newWorkingSegment`. No behaviour change.

- A tier move mints a store key for each object that exists, rather than minting
  all three and blanking two back. `moveSegment` assigned `LogKey`, `IndexKey`
  and `BlocksKey` unconditionally and then cleared the latter two when the
  source had none — stating the rule as its own exception, twice, with only the
  first retraction carrying the reason. No behaviour change.

## v0.84.0 — 2026-08-14

### Breaking

- **`SegmentStore.LiveRead() bool` is removed.** Implementers delete the method;
  nothing else changes. It reported whether a store served transparent
  read-through or required an explicit restore first, and nothing anywhere
  called it. Inside commitlog that was deliberate and documented: opening a log
  stopped touching the store when boot began reading sizes from the tier
  manifest, which removed the one place a capability check would have gone. A
  restore-required tier reports *itself*, to the caller that reads the segment,
  by returning `ErrRestoreRequired` — which is why a log holding a cold segment
  nobody reads opens and runs normally.

  It was kept for a caller holding only a `SegmentStore` that wanted to schedule
  a restore before reading. That caller does not exist: the only implementation
  outside this repo returns a constant `true` and never consults it either. So
  the method was a requirement placed on every implementer to serve nobody, and
  it is the kind of dead weight a linter cannot see — an interface-satisfying
  method counts as used.

### Fixed

- **A `SegmentStore` that wraps its errors had its short reads treated as
  failures.** `SegmentStore` is implemented by callers — that is the point of
  the interface, so the cloud dependency never enters commitlog — and wrapping
  an error with `%w` is simply what Go code does. `ErrObjectNotFound`'s doc says
  so explicitly ("possibly wrapped, so callers use `errors.Is`") and commitlog
  honours it. `io.EOF`, returned by the *same* `ReadAt`, had no such statement
  and was compared with `==` in the two places that read a caller's store.

  The cost is on the short-read path, where the object is smaller than the size
  commitlog recorded: the arm that exists to accept that (`nread > 0`) was
  skipped for a wrapping store, so a buffer holding valid bytes was discarded
  and a hard error returned in its place. `refill` and `storeBacking.ReadAt` now
  use `errors.Is`, and `ReadAt`'s contract states that any of its sentinels may
  arrive wrapped — which is the half that keeps this from recurring, since an
  implementer had no way to know.

- **A digest-planned prefix read reported a damaged segment as `io.EOF`.**
  `KeyPrefix` reads plan from the key digest and then collect the named offsets
  in one forward pass. When the segment ended before an offset its own digest
  names, `collectRun` returned a wrapped `io.EOF` — so the one route that can
  tell a replica *these bytes are damaged, copy from a peer* instead told it the
  segment ended. The sequential route returns `ErrSegmentUnreadable` for exactly
  those bytes.

  `io.EOF` is the ordinary value in every other scan loop in the package, which
  is what made this easy to miss: those run to the end of a segment, so EOF is
  how they stop and only a non-EOF error is damage. This loop is the opposite —
  it stops once it has collected what the digest promised — so EOF *is* the
  damage. All three failure arms (end of segment, unparseable frame, and a
  record the segment steps over) now carry `ErrSegmentUnreadable`.

  The same argument was already written in this file, twenty lines below, for
  `ErrCorruptRecord`: which route found the damage is the package's business,
  not the caller's, and a caller matching on it should not have to know whether
  a digest happened to plan the read. It had been applied to one sentinel and
  not the other.

  `ErrSegmentUnreadable`'s doc said "a scan could not reach the end of a
  segment", which described only the loops that had it. It now states the fact
  those loops are evidence of — the segment does not hold what the log says it
  holds — and names the prefix read as the case that reaches it from the other
  side.

### Changed

- `commitLog.Segments()` is now unexported (`segmentsSnapshot`). It returned
  `[]*segment`, an unexported type, and was deliberately absent from the
  `CommitLog` interface that `New` returns — so no caller outside the package
  could reach it or do anything with the result. `hack/layercheck.sh` carried a
  hand-written exception for it whose own justification argued for unexporting;
  the exception list is now empty.

- Local retention's message and byte limits were two byte-identical eighteen-line
  functions differing in the measure and the limit, sitting directly below
  `applyTierLimit` — the same backwards walk already parameterised by exactly
  those two things. What kept them apart was one rule each spelled out in
  control flow: retention never deletes the active segment, which a tier does
  not have. `deletablePrefix` makes that load-bearing rather than belt and
  braces, since with no floor it returns the whole log.

  Only the messages copy had a test. Deleting the bytes copy's protection
  outright — guard and loop start both — left all twenty-five retention tests
  green, which is what a hand copy costs: the rule was enforced twice and
  falsifiable once. All three now share `applyTotalLimit`, where what varied is
  a named argument (`keepActiveSegment` / `keepNoSegment`), and
  `TestDeleteCleanerBytesKeepActiveSegment` fills the coverage hole.

- Internal, no API change: two more places where one rule was written out
  several times. `offloadMeta` and `TierObject` carry the same ten facts about
  an offloaded segment and were transcribed at four sites; a field added to
  either is now caught by a reflection round-trip test rather than by
  remembering. And the index binary search existed four times — once per lookup
  key (offset, timestamp) and once per layout (dense entries, sparse block
  anchors) — where the real differences were a comparison and an error policy,
  buried in loops that otherwise looked identical. Both are now stated once, in
  `index.searchEntries` and at the two call sites that choose the comparison.

## v0.83.0 — 2026-08-13

### Breaking

- **Client sidecar names must now carry `ClientSidecarPrefix` (`"client-"`).**
  `PutSidecar`, `GetSidecar` and `RemoveSidecar` refuse anything else with
  `ErrInvalidSidecarName`. Rename existing sidecars; there is no compatibility
  path and none is wanted pre-v1.

  What it replaces: two hand-maintained lists — the exact file names the log
  writes, and the suffixes it matches on — which the sidecar check consulted to
  decide whether a client name collided with one of commitlog's. Enumerating one
  side of a collision is a losing position. The lists described the log as it was
  *that day*, so commitlog gaining a file was commitlog silently taking a name a
  client might already be using: at runtime, on data already written, with no way
  for the client to have checked. The client's namespace was defined by
  subtraction from a set only commitlog could change.

  The prefix closes the set from the other side. commitlog promises never to
  write a file carrying it, so it may now add any file it likes — new name, new
  suffix — and a client may use any name it likes after the prefix. Neither party
  needs to know what the other calls things, and both lists are deleted.

  **The refusal is only half of it, and the smaller half.** `checkSidecarName`
  governs names arriving through the API and says nothing about files already on
  disk — and the log's own directory scans dispatch on *suffix* over whatever the
  directory holds. So a sidecar named `client-notes.log` failed the open outright
  on its non-integer stem, and `client-notes.index` was *deleted* as an orphaned
  index, neither of them reachable by a refusal. `openLog` and `logIsNew` now
  skip the prefix, which is what makes the promise true rather than merely
  stated.

  `logIsNew` matters for a second reason: it decides whether the descriptor
  *records* the caller's options or is *checked against* them. A client that
  wrote its config sidecar before the first append — the natural order, since the
  config is what says how to create the log — was creating a log that believed it
  already existed.

- **A negative `CompactMinAge` or `CompactTombstoneRetention` is now refused.**
  v0.82.0's notes list both as "not affected, checked with the same lens", and
  that reading was correct as far as it went: both consumers gate on `> 0`, so a
  negative disables the feature exactly as zero does and nothing misbehaves.

  What it missed is that both fields are in `descriptor.enforced()`. The negative
  is written into the descriptor and becomes part of what the log IS, so a log
  created with `-1h` refuses a reopen with `0` — two values that do the identical
  thing, one permanently rejecting the other, surfacing later as a mismatch
  naming a knob whose two spellings mean the same. Refused rather than normalised
  to zero on the way in: normalising is a converter, and it would make the
  descriptor disagree with the `Options` the caller can see in their own config.

### Added

- **`ClientSidecarPrefix`** is exported, because the clients that hold sidecar
  names need to spell it — and a prefix nobody can name is a rule with no
  affordance for obeying it.

### Fixed

- **`guardcheck` read an empty test selection as a passing one.** `guard_finish`
  ran `go test -run "$re" ... .` — the root package only, and without asking
  whether anything matched. `go test -run` with a pattern that selects nothing
  exits 0, which the script read as "the test passed without the guard". A guard
  whose test lives in a subpackage therefore reported `NO COVERAGE` for a test
  that was never selected, and a guard whose test name went stale after a rename
  produced the same output as a working one. Now `-v ./...`, with the run
  required to have printed a `=== RUN` line; absent that it is a harness error
  rather than a verdict. Every guard name was checked against the real test
  functions, so the fix surfaced nothing pre-existing.

  That check then failed a guard *because* its test matched. It was written as
  `printf '%s' "$out" | grep -qE '^=== RUN'`, under the `set -o pipefail` this
  script has always carried: `grep -q` exits at the first match, `printf` is
  still writing a verbose multi-package log into a pipe with no reader, takes
  EPIPE, and pipefail hands the pipeline printf's failure — non-zero precisely
  because the match succeeded. It only fires once the output outgrows the pipe
  buffer, so the windows job stayed green and the linux one went red on the one
  guard that printed enough, with `printf: write error: Broken pipe` sitting
  directly above the false `HARNESS ERROR`. Now matched with bash's own pattern
  operator, which has no second process and so no EPIPE to take.

### Testing

Three test-only additions, all from one observation: a hand-maintained list that
must stay in sync with something it does not own always rots, and a test built
from a second copy of that list cannot see it happen. Each of these now derives
its cases from the thing itself.

- **Every `descriptor` field survives a render and a parse.**
  `renderDescriptor` and `set()` enumerate the struct's fields twice, by hand,
  and every other descriptor test goes through `New` and `Options` — so each
  covers only the fields it happens to set, and a field that persists *nowhere*
  was invisible to all of them. Three failures were reachable with nothing going
  red: a field missing from the writer is silently not persisted (and, for a
  field `reconcileDescriptor` keeps current, means the descriptor is rewritten on
  every open); a field missing from the reader makes the file unreadable by the
  build that wrote it; a formatting mismatch changes the value.

- **Every signed numeric `Options` field has an opinion about negatives**, found
  by reflection over the struct rather than from a list kept beside it. This is
  the third time in one day that `New`'s refusal table turned out to be missing
  an entry. It keeps an *allowlist* of the three fields where a negative has a
  meaning, deliberately inverted: the refused list is the one that has to grow
  with the struct and forgetting to grow it is silent, while an allowlist only
  grows when someone is already deciding what a negative should mean.

- **Every codec `Valid()` admits round-trips its data and its name.** The codec
  set is written out five times in `compress/codec.go` and both existing tests
  enumerate it again from literals, so a fifth codec was covered by none of them.
  Cases now come from `Valid()`, which is what *defines* the set — `New` refuses
  what `Valid` rejects, and that is the only reason `Compress` may have a silent
  default arm at all. The worst failure it reaches: a codec present in `Valid`
  and `DecompressInto` but not `Compress` stores the block **raw** under a header
  that names the codec, so the read decompresses raw bytes as compressed ones.

## v0.82.0 — 2026-08-13

### Removed

- **Descriptor format v0 is no longer read.** V0 — the same file without the
  optional identity line — was read for one release so v0.79.x directories kept
  opening across the v0.80.0 upgrade. Both downstream repos confirmed they hold
  no v0.79.x data worth keeping ("every dir is created by a test or soak run and
  thrown away with it"), so it goes now rather than becoming permanent. Pre-v1, a
  format this package reads but never writes is a branch with no live input.

  A v0 descriptor is now refused by name — "unsupported descriptor version 0" —
  which matters because every *other* line in a v0 file parses fine. The version
  line is the only thing that catches one, so a test asserting merely "some
  error" would not have distinguished a working check from a deleted one.

  The version line itself stays. `set()` refuses an unknown key on purpose, so a
  build handed a file from a *newer* writer would otherwise report "unknown
  descriptor field" — loud, but it names a field instead of a version and reads
  like corruption. That value is about future formats and never depended on a
  past one still being readable.

### Fixed

- **A negative `MaxSegmentAge` rolled a new segment on every append.**
  `CheckSplit` disables rolling on `logRollTime == 0`, so a negative got past
  that and reached `timestamp()-firstWriteTime >= int64(logRollTime)`, true for
  anything a clock can produce. This is the identical failure already documented
  in `New`'s refusal table for a negative `MaxSegmentBytes` — the two fields sit
  one line apart in `Options`, and only one was checked.

- **A negative `MaxLogBytes`/`MaxLogMessages`/`MaxLogAge` disabled retention
  while reporting it as configured.** `noRetentionLimits()` asked `== 0` while
  the three apply-gates in `cleanLocal` ask `> 0`, so a negative was "retention
  is configured" to the first and "do not apply it" to the others. `Clean` took
  the do-work path, split the tiers, walked the segments, logged the policy it
  was enforcing — and enforced none of it. The log grew without bound while the
  caller believed a limit was in force.

  Fixed at both ends, which are different fixes rather than redundancy: `New`
  refuses a negative, which is the caller-facing answer and the only one that
  produces a message; and `noRetentionLimits()` now asks `<= 0` so it agrees with
  its own apply-gates. A `deleteCleaner` is constructed directly in tests and
  takes `Retention` as a plain struct, so "the boundary already checked" is a
  promise that file cannot read.

  Not affected, checked with the same lens: `CompactMinAge` and
  `CompactTombstoneRetention` gate on `> 0` consistently, so a negative reads as
  disabled everywhere.

## v0.81.0 — 2026-08-13

### Changed

- **A negative `PrefixReadConcurrency` or `PrefixReadTierConcurrency` is now
  refused by `New` instead of silently becoming the default.** `concurrencyBudget`
  defaults on `v <= 0`, so a negative reached the arm that exists to catch a
  *missing* value and the caller got 8 or 64 instead of what it asked for. Same
  defect as the three options already refused there, in a fourth and fifth place.

  What makes this one more than a table entry: it is reachable by following the
  documentation. The sibling `PrefixReadCoalesceBytes` knobs are described in the
  same `Options` paragraph, and that paragraph teaches that a negative is
  meaningful and powerful here — "NEGATIVE means never coalesce... the
  maximum-concurrency and maximum-request-count setting". A caller who read that
  and asked for the analogous extreme on the concurrency knob was quietly given
  the default.

  Refused rather than given a meaning, because there is no defensible one: the
  analogous extreme is unbounded on one reading and serial on the other, and
  picking either would be this package deciding something the caller was trying
  to say. A negative `CoalesceBytes` still means "never coalesce" and is
  unaffected; that asymmetry is now pinned by a test, since it reads as an
  oversight and is not.

  Minor rather than patch: a caller passing a negative concurrency today gets a
  working log and will now get an error from `New`. No caller in this repo or
  either downstream does.

### Added

- `docs/sweep-2026-08-13-complexity.md` — the standing complexity sweep, both
  axes, recording what was checked and found clean as well as what changed.

## v0.80.2 — 2026-08-13

### Fixed

- **An open by a caller with no identity erased the stamp.** The republish that
  keeps `Compression` and `MaxSegmentBytes` current built its record from the
  caller's options, so it carried the caller's `Identity`. v0.80.0 guarded the
  case where those bytes are *different* — a conflicted open must not re-stamp
  the log — and missed the case where they are *absent*.

  A caller that does not use identity conflicts with nothing, deliberately: it
  has no opinion to disagree with. So the guard let the republish through, and
  it published an empty identity, which `renderDescriptor` omits entirely. The
  stamp did not become wrong; it stopped existing. Any tool that opens a stamped
  log without using identity and retunes a codec or a segment size did this — a
  repair utility, a compaction job, another service on the same directory.

  That is the worse half of the two. `Options.Identity` exists so an unstamped
  copy is not a state that occurs, because downstream cannot reclaim one: an
  unstamped copy and a stale one look identical and only one of them should be
  destroyed. Erasing a stamp manufactures precisely that, from a log that was
  correctly stamped, on an open that did nothing wrong.

  The republish now refreshes those two fields **on top of the stored record**
  instead of publishing the caller's. The stored identity is then carried by
  construction rather than by a condition someone has to remember to extend, so
  neither direction is reachable. The conflict term stays for an independent
  reason: while the caller and the log disagree about what the log is, its
  opinion about how to encode it is not one to act on.

  Found by re-reading v0.80.0 rather than from a failure. The nearest existing
  test opened a stamped log with no identity and asserted no conflict, but never
  checked that the stamp survived — and without an option change no write path
  runs, so it passed either way.

## v0.80.1 — 2026-08-13

### Fixed

- **A store's claimed descriptor size drove an unbounded allocation.**
  `readStoreDescriptor` sized its buffer from `store.Size(descriptorKey)` with
  only a `<= 0` check, so a store reporting a large object at that key allocated
  it entirely into memory during `New`, in the caller's process, before a single
  byte was parsed. The length steering the read was the one thing nothing had
  verified.

  The bound is derived rather than picked: `parseDescriptor` reads with a
  `bufio.Scanner` at its default 64 KiB maximum token, so a descriptor holding
  any line longer than that cannot parse whatever else is true of it — and a
  descriptor is one short line per field. Reading past that point can only ever
  end in a parse error, so refusing early costs nothing that could have
  succeeded. A real descriptor measures a few hundred bytes, which the test
  asserts directly so the bound stays honest.

  The local path never had this: it hands the open file to the same scanner and
  streams, so an enormous file on disk fails on its first oversized token
  without being read into memory. That asymmetry is what made the store path
  easy to miss.

  **Not fixed, deliberately:** `manifest.go`, `segment.go`'s block table, and
  `index_cache.go` size their reads from the store the same way. The descriptor
  is the only one whose bound is *derivable* — the others legitimately scale
  with how much data the log holds, so any limit would be a policy invented
  here rather than a fact about the format. Named so the shape is on record
  rather than rediscovered.

## v0.80.0 — 2026-08-13

### Added

- **`Options.Identity` and `CommitLog.IdentityConflict()`** — opaque caller
  bytes saying which of its own entities a log's data belongs to, recorded
  atomically with the log's creation.

  It closes a gap that could only be closed here. A caller that stamps identity
  *after* `New` returns has a window — log on disk, not yet stamped — and a crash
  inside it leaves bytes nothing identifies. That state is unrecoverable rather
  than untidy: an unstamped copy and a stale one look identical, so neither can
  be reclaimed without risking the wrong one, and the copy leaks permanently.
  commitlog creates the directory, so it is the only layer that can make the
  stamp and the log appear together. `New` already settled the descriptor before
  opening anything; the identity now rides that same atomic write.

  A mismatch on reopen is **reported, not refused and not adopted**. Refusing
  would take a partition offline over bookkeeping. Adopting would consume the
  signal — and consuming it at open time means a crash immediately after loses
  it, which moves the window instead of closing it. The stored bytes are left
  alone, so the disagreement is still there on the next open and the one after
  that. `AdoptOptions` re-stamps and is the deliberate resolution.

  `IdentityConflict.Stored` is nil when the log carried no identity at all,
  which is kept distinguishable from "stamped for someone else" because the two
  warrant opposite actions: unidentified data may still be the caller's own.

  The descriptor file is now version 1. Version 0 is still read — an older log
  simply has no identity — so this is additive and no log needs migrating. The
  identity is hex-encoded because the descriptor is line-based and the bytes are
  the caller's: a raw newline would otherwise write a descriptor that does not
  parse back, turning a legal choice of identity into an unopenable log.

- **Five exported methods joined the `CommitLog` interface**: `RecoverTail`,
  `ActiveSegmentBase`, `SegmentBlockCounts`, `IsClosed` and `IsDeleted`.

  All five were exported on the concrete type and absent from the interface
  `New` returns — which means they were not public in any useful sense. The only
  way to reach one was a structural type assertion, and that degrades *silently*
  when it misses: the caller gets the zero value or skips the call, with nothing
  to log. durable_streams was reaching `RecoverTail` and `ActiveSegmentBase`
  exactly that way, and `RecoverTail` at open is what makes their producer-id
  records survive a restart.

  `Segments` stays off, and the reason is about its signature rather than
  convenience: it returns `[]*segment`, an unexported type, so nothing outside
  the package could use the result.

  `hack/layercheck.sh` now enforces this — an exported `*commitLog` method must
  be on the interface or be listed with a reason. Five had drifted off before
  anyone noticed, which is what makes it worth a check rather than a habit.

### Changed

- **`hack/layercheck.sh` replaces a layering metric that could not fail.**
  `docs/layering.md` defended its stack with "`*commitLog` is named by six
  files". That count is now ten with nothing violated in between: almost every
  hit is a `func (l *commitLog)` method *declaration*, and a file full of
  commitLog methods is by definition the top layer — so the number measured how
  the log's methods are spread across files, and would have read identically on
  a tree where `index.go` had started calling the log.

  The check now measures direction: nothing in the lower half of the stack may
  name `*commitLog`, as receiver, field, or parameter. A non-test `.go` file in
  neither half is also an error, so a new file cannot escape the rule by not
  being mentioned. Runs in CI beside `docdrift`.

  `docs/audit-2026-08-13-separation.md` records the full separation-of-concerns
  pass, including the two boundary findings that are still open questions rather
  than code.

## v0.79.2 — 2026-08-13

### Fixed

- **A half-done `Finalize` orphaned the log it had already renamed.**
  `TruncateBefore` installs its trimmed segment with two renames — log, then
  index — and had no rollback between them. `Replace` performs the same two
  renames and does roll back; this was the odd one out.

  What is at stake differs, which is why it read as a different situation.
  `Replace`'s rollback protects the source it is renaming over. `Finalize` has no
  source: it protects the **discriminator**. Publishing a working copy means
  clearing its suffix, and `segment.dropIfUnpublished` reads that suffix to
  decide whether the copy still needs dropping. Returning with the log already at
  its final name and the suffix still set makes that answer wrong in the one
  direction nothing checks — the caller's `defer` concludes "not published",
  deletes by the *suffixed* paths, removes the index it can still see and leaves
  the log it cannot. The result is an orphan `.log` at the trim's base offset
  with no index beside it.

  The next `open()` does survive it, which is how this could sit unnoticed: the
  orphan overlaps the boundary segment that the failed call left in place, and
  `resolveSegmentOverlaps` drops the contained one. Depending on that is the
  thing the suffix rule exists to avoid, and it would go on passing every test
  that only checks the log still reads. Now either both files are at their final
  names or neither is.

## v0.79.1 — 2026-08-13

### Fixed

- **A failed `Truncate` could strand the replacement it had already built.** A
  rewrite builds a suffixed working copy and publishes it by renaming the suffix
  off, so a copy still carrying its suffix has not been published and has to be
  dropped. Five paths build one — `cleanSegment`, `consolidateOne`, `joinOne`,
  `Truncate` and `TruncateBefore` — and each carried its own transcription of
  that disposal.

  `Truncate`'s was a closure wired into three of its four error returns. The
  fourth is a failed `Replace`, which strands the copy in exactly the same state:
  open, mapped, named by nothing, its handle and index mapping held for the life
  of the process. On Windows that is a directory that cannot be removed after a
  `Close` that reported success.

  All five now share `segment.dropIfUnpublished`, which reads the suffix and also
  honours `left`, so a segment that has already departed is not deleted twice.
  `TruncateBefore` was the last one found and the only one with no test asserting
  it disposed of anything; it now has one. Guards re-anchored per call site
  rather than on the shared method — one anchor would prove the rule exists and
  stop proving each path invokes it, and the invocation is the half that gets
  forgotten.

### Internal

- `GUARDCHECK_ANCHORS=1 hack/guardcheck.sh` resolves every guard's anchor and
  runs nothing else: seconds instead of forty minutes, for the one question worth
  asking after any refactor — did I move text a guard is standing on? It ignores
  the platform deferral, so it is also the only thing on a Linux box that can
  speak for the Windows-only anchors.

## v0.79.0 — 2026-08-13

### Fixed

- **Nothing repaired a short index on a sealed segment, and a short index made
  its records unreadable.** `setupIndex` takes `lastOffset` straight from the
  index's last entry, so a segment whose index stops short of its log answers as
  if the records past that point are not there. They are in the file; the log
  will not serve them. Measured: **one lost index entry in the first segment of a
  60-record log cost 56 of them, permanently.**

  The repair already existed. `reconcileIndexTail` appends entries for exactly
  the frames past the last indexed one — but it ran on the ACTIVE segment and on
  adopted tiers, and on nothing else. `setupIndex`'s own rebuild fires on
  `indexOvershootsLog`, the OPPOSITE direction: an index reaching PAST its log.
  `indexOvershootsLog`'s comment calls an index BEHIND its log "ordinary ... and
  `reconcileIndexTail` fills it in", which was true of one segment out of however
  many the directory held.

  `open()` now reconciles every segment. It is O(1) per healthy segment and reads
  no log bytes: the walk starts at the last indexed frame's end and runs while
  that is below the file size, so an index that covers its log executes the loop
  body zero times. Only a segment that is actually short reads anything, and only
  over the part that is missing.

  Two details that are not incidental. The floor is the NEXT segment's base
  offset, not the high watermark — what a sealed segment drops below that is not
  an uncommitted tail but a HOLE between two segments that both still exist; the
  watermark is the right floor only for the segment nothing follows, which is why
  the active segment keeps its own call. And it runs BEFORE
  `resolveSegmentOverlaps`, which decides containment from `NextOffset`, derived
  from the very `lastOffset` a short index understates.

  `TestAShortIndexOnASealedSegmentHidesRecords` pins it, with a guard.

### Performance

- **A reopened log now fsyncs no index it did not write.** v0.78.0 skipped the
  per-segment index fsync for segments the process sealed itself. The shape that
  costs most is the restart: a broker opens 336 segments, serves reads, shuts
  down, and paid one device-cache flush per segment for bytes it never touched.
  That is now none — counted directly off the flush counter, not inferred.

  `dirtyIndex` started true for everything opened from disk on the ground that it
  "was written by a process whose flush state we cannot know". That was the right
  worry and the wrong remedy: an fsync at CLOSE does not recover a predecessor's
  lost tail, it only writes back whatever survived. What recovers it is the walk
  above — so until that ran on sealed segments, the worry was answered by a flush
  that could not fix the thing it was worried about.

  A reconcile that wrote NOTHING is the proof that the index on disk describes
  the log beside it, and that is what now clears the mark. Where it DID write the
  mark stands and close still flushes: those entries exist nowhere else.

  This is not a claim that the bytes are on stable storage in every case. A
  predecessor that crashed between writing index bytes and flushing them leaves
  them in the page cache, complete, and a power failure can still take them. What
  changed is that losing them is recoverable rather than silent — the next open
  walks the gap and rebuilds it.

  Closing a 44-segment log, ms per segment: **reopened 7.3 → 3.2**, against 2.6
  for the same log rolled in-process. At 9 segments the two are a dead heat and
  at 3 the reopened log is the faster of the two. `BenchmarkCloseCleanLog`
  carries the numbers.

## v0.78.0 — 2026-08-13

### Fixed

- **`seal()` cleared a segment's `dirtyIndex` mark even when the flush it
  performed had failed.** `dirtyIndex` means "these index bytes are on stable
  storage". `seal()` called `Index.Sync()`, discarded the error deliberately —
  the shrink beside it is best-effort for good reasons, and seal runs on paths
  that cannot return an error — and then cleared the mark unconditionally. A
  `Sync` that returned an error did not put the bytes anywhere, so the segment
  was left asserting a durability it did not have.

  Nothing could observe that while the close path fsynced every index
  unconditionally on the way out, which it did until this release: the second,
  unconditional flush covered the lie. It stops being covered the moment close
  starts trusting the mark, which is exactly what the change below does — so
  this had to be fixed first, and the speedup rides on top of it rather than
  the other way round.

  The discarded error stays discarded; only the mark is now conditional.
  `TestSealKeepsTheDirtyMarkWhenTheFlushFails` pins it, with a guard.

### Performance

- **Closing a log no longer fsyncs index files that are already on stable
  storage.** `closeSegment` passed `durable=true` straight through to
  `index.Close()` for every segment, so a clean shutdown issued one
  `FlushFileBuffers` (or `fsync`) per segment whether this process had written
  to that segment or not. For a log that has been up long enough to roll, most
  of those segments were sealed by this process, flushed by `seal()`, and never
  touched again — the flush pushed nothing and cost a syscall that, on Windows,
  flushes the whole DEVICE cache.

  That last detail is the reason this is worth doing, and it is not the mean.
  Measured on an idle box the fsync is about +2ms against an unmap of ~3.2ms —
  the smaller of the two. Measured with a neighbour working the same disk, every
  syncing case went to 36-50ms while every non-syncing case stayed at ~3ms,
  because `FlushFileBuffers` pays for whatever else is dirty on the volume. A
  shutdown that fsyncs once per segment therefore has a cost with no upper bound
  in the thing it is nominally measuring. durable_streams runs 336-segment logs.

  End to end, closing a 44-segment log this process rolled itself: **7.3 →
  3.4 ms per segment**. The saving is proportional to how many CLEAN segments
  the log holds, so it is worth most to exactly the long-lived logs that have
  the most of them. `BenchmarkCloseCleanLog` and `BenchmarkIndexTeardownParts`
  carry the numbers and the method.

  A REOPENED log still pays the fsync for every segment. `dirtyIndex` is
  initialized to true for anything opened from disk, on the stated ground that
  a segment opened from disk was written by a process whose flush state this one
  cannot know, and nothing repairs a SHORT index on a SEALED segment. Changing
  that needs either a clean-shutdown marker or an additive index reconcile at
  open; neither is in this release.

### Changed

- **`index.Close()` split into three named closers.** `closeIndex` took one
  `durable` flag that decided two independent things — whether to flush, and
  whether to trim the file to its content. Those only agreed by coincidence while
  there were two callers. There are now `Close()` (flush and trim),
  `CloseFlushed()` (trim only, for an index already on stable storage) and
  `CloseDiscarding()` (neither, for a caller about to unlink the files), over a
  `closeIndex(flush, trim bool)`.

  The trim also now skips when `position == size`, which is the common case for
  a reopened sealed segment: its index was already shrunk, so `SetEndOfFile` was
  being asked to change nothing.

- **`index` counts its flushes.** A flush that is skipped and a flush that is
  performed leave byte-identical files behind, so the skip above was
  unobservable from outside and a guard would have had nothing to go red
  against. The counter is on the real type rather than a test double, because a
  double replacing `*index` would switch off the very code path under test.

## v0.77.0 — 2026-08-13

### Fixed

- **Reverted: `CleanWithSpec` no longer refuses a `StripBelow` above a supplied
  `Ceiling`.** v0.76.0 added that refusal. It was wrong twice over, and it broke
  durable_streams in production — every clean pass on a stream that had a decided
  transaction and a lagging consumer group at the same time was rejected. They
  reported it and worked around it by clamping `StripBelow` to the ceiling; the
  clamp can come off on this release.

  The first error was about what `Ceiling` means. The refusal read it as a claim
  about DECIDEDNESS — "at or above me, records may be undecided" — so a
  `StripBelow` above it looked like the caller asserting decided and undecided
  about the same range. `Ceiling` makes no such claim. It bounds COMPACTION.
  Passing the LSO is one reason to hold it down, not the only one:
  durable_streams builds both fields equal at the LSO and then lowers `Ceiling`
  ALONE to pin records a slow group has not read yet. Nothing about that is
  inconsistent, and only `StripBelow` speaks about decidedness at all.

  The second error was about what the pass does. The refusal claimed the pass
  would strip records above the ceiling, citing `mergeDigests` marking on
  `r.offset < spec.StripBelow` before the ceiling is consulted. That mark only
  says a segment MAY have strip work in it. The decision that matters is
  `classify`, which returns `dispRetain` for `offset >= spec.ceiling` before it
  considers stripping at all — so the ceiling already wins, and a `StripBelow`
  reaching above it stops applying rather than doing damage. The hazard the
  refusal existed to prevent could not occur.

  What replaces it is a comment at the same spot saying not to add it back, and
  why. `TestAStripBelowAboveTheCeilingIsHonoured` pins the behaviour: the pinned
  copies survive compaction and keep their headers, and a decided record below
  the ceiling is still stripped, so the spec is honoured rather than merely
  tolerated. The three guards that held the refusal are gone with it.

  `docs/layering.md` keeps the episode rather than the invariant. It used to
  invite a cheap check on `Ceiling` "at the top of `CleanWithSpec`"; it now
  records that an invariant which reads as arithmetic between two fields is still
  a claim about their MEANING, and the meaning lives in the caller.

## v0.76.0 — 2026-08-13

### Changed

- **`CleanWithSpec` now refuses a `CleanSpec.StripBelow` above a supplied
  `CleanSpec.Ceiling`.** `docs/layering.md` has carried a standing note that
  `Ceiling` is the LSO by convention only, that this package cannot verify it,
  and that "if a cheap invariant on `Ceiling` ever becomes available, it belongs
  at the top of `CleanWithSpec`". This is one.

  The two fields are one caller's account of one boundary from opposite sides.
  `Ceiling` says records at or above it MAY BE UNDECIDED and are retained
  verbatim; `StripBelow` says records below it ARE DECIDED and no longer need
  their transactional bookkeeping. `StripBelow > Ceiling` asserts both about the
  range in between, and the pass acts on the second: `mergeDigests` marks a
  record for stripping on `r.offset < spec.StripBelow` BEFORE it consults the
  ceiling, so records the ceiling was set to protect keep their offsets and lose
  their `pid`/`epoch`/`seq` headers — the bookkeeping the caller needs to decide
  the transaction they belong to. An undecided record that loses them cannot be
  decided by anyone afterwards.

  It also makes `Ceiling`'s own doc true. "Retained verbatim" was not what a
  record in that range got, and verbatim is what a spec refused here leaves them.

  Deliberately scoped to a SUPPLIED ceiling. With the field unset the bound is
  the log's own high watermark, resolved inside the pass and free to move between
  a check and a use of it, so a refusal there would be a race dressed as an
  invariant — `TestAStripBelowWithNoCeilingIsNotJudged` holds that boundary, and
  `TestAStripBelowAtTheCeilingStillRuns` holds the other one, because every
  transactional caller passes the two EQUAL and a `>=` would reject the normal
  spec.

  It is scoped again to specs where stripping is ACTIVE, gated on the same pair
  `mergeDigests` uses (`StripBelow > 0 && len(StripHeaders) > 0`). The first
  version was not, and it was wrong in the way this refusal's own neighbour
  exists to prevent: `StripBelow`'s zero value means "no stripping", not "strip
  below offset 0", so an unset field was read as a value the caller wrote down.
  `HighWatermark` returns -1 for "nothing committed yet" and
  `Ceiling: At(l.HighWatermark())` is what callers write, so an unset `StripBelow`
  of 0 sat above a legitimate ceiling of -1 and the pass was refused.
  `TestACeilingBelowEveryOffsetIsLegitimate` — which exists because an earlier
  change made the same class of mistake about that ceiling's sign — went red on
  every CI platform. `TestAStripBelowWithoutStripHeadersIsNotJudged` now holds
  the arm that was missing.

  Not breaking for any caller that exists: every spec in this repo and in
  durable_streams passes `Ceiling: At(hw), StripBelow: hw`, or the same pair at
  `hw+1`. What the refusal rejects is a combination nothing constructs on
  purpose.

- **An open that has to walk a segment's block chain now keeps the table it
  built.** A block-compressed segment's physical layout is a chain: each block's
  header carries the length that locates the next, so rebuilding the table is one
  read per block over the whole file, before the segment serves anything. The
  sidecar written at seal removes that — and `closeSegment` ends in `seal()`, so
  a log shut down cleanly persists a table for every segment including the ACTIVE
  one, and its next open walks nothing at all.

  What no close covers is the open that FOLLOWS a crash. The process that would
  have sealed the active segment died, and a segment whose best-effort write at
  seal failed has nothing that tries again either. Both then walked on that open
  and threw the result away, so the open after that walked the same chain, and so
  on for as long as crashes kept arriving before a clean close did. `newSegment`
  now writes the table on the far side of the walk, which makes a crash cost the
  chain once rather than once per restart.

  It sits in `newSegment` rather than in `initPositions`, which has three other
  callers. Two of them — `Replace` and `trimTo` — reopen a segment whose files
  were just renamed, and they delete the old sidecar first deliberately: a
  rewrite that dropped nothing lands on the same size, and a table believed on
  that evidence maps logical offsets onto the wrong records. Writing a fresh
  table under them would heal the different-size case and leave that removal
  looking unnecessary, which is how a guard quietly stops being able to fail.
  `TestInstallingARewriteDropsTheReplacedBlockTable` went red on exactly that
  while this was being placed.

  The write is keyed on `blocksWalked`, so it is skipped for the segment that
  answered from its sidecar and for the empty segment a split just created. It is
  best-effort, one degree more so than at seal: this is the open path, and
  refusing to open a log because a file it can regenerate could not be written
  would turn a slow open into no log at all. Writing a table for a file still
  being appended to is safe because of the check that reads it back — the next
  append leaves the table describing fewer bytes than the file holds, and
  `loadLocalBlockTable` refuses a table that does not account for exactly the file
  beside it, so a stale sidecar costs the walk it would have cost anyway and
  never a wrong answer.

  Not a full removal of the walk, and the remainder is not bookkeeping: after an
  unclean shutdown the active segment's chain is walked once, and that same pass
  is what finds the torn tail and discards it. It is bounded by one segment.

## v0.75.0 — 2026-08-13

### Changed

- **A `CleanSpec.TierBudgets` entry of 0 is now refused.** It used to fall back
  to `RewriteBudget`, silently — `budgetFor` read `!ok || d == 0`, so the arm a
  caller reaches by saying NOTHING was also the arm reached by saying 0, and a
  value the caller wrote down became unreachable.

  Refused rather than given a meaning, because both available meanings are wrong
  here. Read as "unbounded", which is what 0 means on `RewriteBudget` one field
  up, it removes the only bound on the remote rewrite that this field exists to
  keep from consuming a pass. Read as "unset", it duplicates what absence from
  the map already says. The entry is less bad input than input with no meaning
  available, so the error names both readings and how to spell each: a duration
  longer than a pass for unbounded, no entry at all for unset.

  It lands in `CleanWithSpec` beside the `Ceiling`/`DisableAutoClean` refusal and
  by the same rule — a spec that cannot be honoured fails loudly rather than
  being reinterpreted into one that can. `Clean()` routes through
  `CleanWithSpec`, so there is no path around it, and `budgetFor`'s arm narrows
  to `!ok`, where it can no longer swallow a supplied value.

  Breaking only for a caller that passes 0 today and expects the fallback. No
  such caller is known: durable_streams fills every tier with a positive
  duration on purpose, which is what its own `tierBudgets` doc says it is for.

### Fixed

- **`CleanSpec.RewriteBudget` documented a guarantee it does not provide.** It
  said a pass "always finishes inside a short-lived process's kill window". It
  bounds a STAGE. One pass builds a fresh budget for local rewrites, one per
  tier, and one for the join stage, so a compacted log with two tiers can spend
  4×`RewriteBudget` before the pass returns.

  No behaviour changed and none should: a single shared counter would starve
  whatever ran last, which is exactly what the per-tier split and the join's own
  budget exist to prevent. The worst case is bounded and knowable, and dividing
  it is the caller's job — but only if the doc admits there is something to
  divide.

## v0.74.0 — 2026-08-12

> **Correction.** The annotated tag for this release claims the tiered *rewrite*
> tier-name fix as part of it. It is not: that fix is `5c22c1f`, which **is** the
> `v0.73.0` tag, and it is recorded under v0.73.0 below where it belongs. The tag
> message is wrong and this file is right. Left standing rather than rewritten,
> because moving a published tag to fix prose is the worse trade.

### Added

- **Segment join now runs against a tier.** `CleanSpec.TierJoinBelow` was
  accepted in v0.73.0 and the runs it described were planned and then skipped;
  they are now executed. A run is refused if this log does not OWN the tier —
  configuration is not permission, and leaving a read-only tier out of
  `TierJoinBelow` only refuses the callers who did not ask.

  A store has no rename, so the local commit point does not transfer: the join is
  committed by a manifest write instead. That write has to be ONE write, because
  a join changes the SET — it retires N base offsets and adds one — and a
  manifest naming some of the inputs plus the result would claim records twice
  while one naming neither would lose them. It is one write because
  `publishTierManifests` rebuilds a whole manifest body and `Put`s it per tier,
  and a run never spans tiers.

  The two halves of that set reach the manifest by different routes, which is the
  part that took the longest to get right. The result goes in as an ordinary
  PENDING entry — its objects are uploaded but the segment has not switched to
  them — and because the override is keyed by base offset and the result keeps
  the run's LOWEST one, it REPLACES the first input rather than adding to it. The
  other inputs cannot be expressed that way at all, since a pending entry names an
  object and "stop naming this one" is not an object, so `publishTierManifests`
  grew a retiring set to say it directly.

  Retiring at publish time rather than mutating the segments first, which is the
  route a tier MOVE takes. `swapTier` may repoint before its commit because it
  repoints at objects that are real and complete; a join has nowhere to repoint an
  input to, because the input is about to stop existing. Clearing their tier
  fields ahead of the write would leave a failed publish holding segments the log
  still serves but no longer believes are offloaded, and something would have to
  roll that back — the obligation "the publish is the commit" exists to abolish.
  Everything is now pre-commit or post-commit and nothing is in between.

  The inputs a run absorbs are retired with a new `retireIntoJoin`, which is what
  `Delete` does minus the deletion: `Delete` on an offloaded segment goes straight
  to `store.Delete`, and a join holds every input's backing open until after the
  install, so the objects it absorbs are exactly the ones something may still be
  reading. They go on the pass's reclaim queue with the log object's backing
  pinned, and are removed by a later `drainReclaim` once nothing holds it.

  `retireIntoJoin` also clears the retired input's tier fields — post-commit,
  which is the only point at which that is free. A pass joins several runs and a
  segment stays in `l.segments` until the splice at the very end, so a later
  run's commit, which rebuilds the manifest from `tierState()`, republished an
  earlier run's retired inputs and named objects already queued for deletion.
  Taking the segment out of the tier's view is what makes a retirement stick for
  the rest of the pass.

  A run that does NOT get through hands back nothing to reclaim, which is worth
  saying because the opposite reads as the careful choice: the upload produced
  those entries, so passing them on looks like not losing track of them. They
  name the first input's *current* objects, and they only stop being current when
  the repoint happens. `drainReclaim` deletes an entry whose backing has no
  readers, and it is allowed to treat that as terminal only because every entry
  was put there BY a swap — so entries from a run that never swapped are deleted
  out from under a segment still serving them.

  The test for the commit asserts on EVERY manifest the pass publishes rather
  than the one it settles on, and in offsets rather than in keys: base offsets
  alone cannot tell the states apart, since the result keeps the first input's, so
  a manifest that had already retired the rest while that entry still described
  the pre-join object would look perfectly consistent and would have lost every
  record above it.

  See `docs/segment-join.md`.

## v0.73.0 — 2026-08-12

### Added

- **Segment join: `CleanSpec.JoinBelow` and `CleanSpec.TierJoinBelow`.** A run of
  adjacent sealed segments is replaced by one segment holding every record,
  verbatim. Off by default; a tier absent from `TierJoinBelow` is never joined,
  which is also how a read-only tier stays untouched.

  It exists because compaction only ever SHRINKS a segment — a rewrite keeps its
  predecessor's base offset and replaces it in place — so nothing ever merged two
  back into one and a long-lived log converges on many small ones. The bytes are
  fine; the count is the cost: a file set, an open handle, an index mapping and a
  slot in every walk over the segment list, per segment, forever. durable_streams
  reports 336-segment logs.

  It is its own stage of the pass, after both arms of `if l.Compact`, because
  compaction and consolidation are those two arms and a join in either would only
  ever reach half the logs — while both accumulate segments. It is budgeted as a
  rewrite, after their debt: reclaiming bytes beats reclaiming file handles.

  What made this more than a rewrite is that a segment IS its base offset — file
  stem, tier manifest key, sidecar derivation, and the reader's "which segment
  holds offset N" search — and a join means one base offset ceases to exist. The
  result is therefore built at the run's LOWEST base offset, which makes the
  install the ordinary rename over the first input, and makes every other input a
  segment strictly CONTAINED in the result. That is the state
  `resolveSegmentOverlaps` already resolves on open, keeping the superset; it was
  written for an interrupted truncation, and a join produces the identical shape
  deliberately. So a crash resolves to the old set or the new single segment, and
  never to an offset served twice — with no new mechanism.

  The pass returns the spliced list rather than mutating the live one, so a run's
  inputs all leave and its result arrives in one swap. A window naming both would
  be observable: the replacement link a join sets is many-to-one, and
  `LocalBytes` resolves every entry through it and sums `Position()`.

  Tiered runs are planned but not yet executed — their commit point is a single
  manifest write that adds the result and removes every input together, which is
  a separate piece of work. `joinOne` refuses an offloaded input outright rather
  than assuming no caller will offer one.

  See `docs/segment-join.md`.

### Fixed

- **A rewrite of an offloaded segment published it under the wrong tier.** The
  commit point of a tiered rewrite passes a PENDING manifest entry, and it built
  that entry naming `defaultTierName` — a constant — rather than the tier the
  segment is actually in. Correct only for a log whose tier happens to be called
  `"default"`.

  It is worse than a misfiling. `publishTierManifests` applies pending entries as
  an override keyed by base offset, so the wrong-tier entry CONSUMED the correct
  one that `tierState` reported, and was then filed under a tier name the log has
  no store for. The manifest that went out therefore named neither the segment's
  old objects nor its new ones — it did not name the segment at all. A process
  dying anywhere in the rest of the pass would reopen to a tier that has lost
  those records, with their objects indistinguishable from garbage.

  What kept it alive is that `clean()` ends with an unconditional republish
  rebuilt from `tierState` alone, which repairs the entry — so the damage is only
  reachable through the crash window, and the state the pass settles on is always
  correct. And what kept it invisible is the test helper: `oneTier` names every
  single-store test chain `defaultTierName`, so the whole suite used the one name
  that made the constant right. The new test builds a tier named anything else,
  records EVERY manifest the pass publishes, and requires each to name the
  segments the log is still serving — the invariant a per-segment commit exists
  to hold, rather than the one the pass converges on.

- **The replication fetch read a tiered segment with no claim on its object.**
  `ReadMessageSet` was the one site in the package that assembled a
  `segmentScanner` as a struct literal instead of calling
  `newSegmentScannerCache`. The constructor exists to do three things under one
  lock: read the segment's backing, register the read's claim on it, and open a
  `scanStream` where the backing pays for one. The literal did none of them.

  So a replication fetch of an offloaded segment held no claim, and
  `drainReclaim` — which deletes a superseded object once nothing references it —
  judged the object unreferenced and could delete it out from under the read.
  `prefix_read.go` carries this warning in full ("a scanner assembled by hand
  holds no claim, and a tiered object it is reading can be reclaimed underneath
  it"); this call site predates it and never followed it. It also read the
  `backing` field without the lock, and forwent the stream, turning a fetch of a
  cold segment into one store request per frame.

  No guard for this one: its failure is a reclamation race, and a guard whose
  test cannot be made to go red is worse than none. The existing `reclamation
  pin` guard covers the mechanism.

- **A damaged segment stalled a follower silently.** `ReadMessageSet` broke out
  of its scan on any error and returned what it had with a nil error — so damage
  at the start of the requested range produced an EMPTY set and no error. The
  caller is a follower that continues from the last offset it appended, so it
  went back to the same offset forever: no progress, no diagnostic, and nothing
  to tell "caught up with this segment" from "this segment is damaged".

  It now wraps `ErrSegmentUnreadable`, whose doc describes exactly this caller —
  one with a peer to copy from, which retrying the same call cannot help. Only
  when the damage leaves it with nothing to return: a short set is real progress,
  and the follower's next call starts AT the damaged frame and gets the error
  then.

## v0.72.6 — 2026-08-12

### Fixed

- **A damaged segment silently destroyed every record past the damage on
  non-compacted logs.** `consolidateOne` walks a sealed segment, copies every
  record into a working copy, and `Replace` then renames that copy over the
  source's files and closes the source. It drove the walk with

  ```go
  for ms, _, err := ss.Scan(); err == nil; ms, _, err = ss.Scan() {
  ```

  which cannot tell `io.EOF` from a read failure. Both simply end the loop, and
  what comes after the loop is the install — so a segment that could not be read
  to its end was replaced by a PREFIX, the file holding the rest was deleted, and
  the pass returned `nil`. The red test lost 1273 records (offsets 6365–7637) to
  a `Clean()` that reported success.

  This is the default configuration, not a corner. `consolidateSegments` is the
  `else` branch of `if l.Compact` in `clean.go`, so it is the pass every
  non-compacted log runs on every automatic clean tick. The compaction path has
  had the `io.EOF` check since `ErrSegmentUnreadable` was introduced, and
  `TestDamageInOneSegmentDoesNotKillTheProcess` covers all three paths that had
  it — with `Compact: true`, which is exactly why it never reached this one.

  That test is also, by construction, blind to this failure: it measures survival
  by reading sequentially from the oldest offset, and a sequential read stops AT
  the damaged frame either way. Deleting the damaged bytes is what lets the
  reader walk on into the next segment, so the broken behaviour scores BETTER on
  that measure than the fixed one. The new test asserts on the victim segment's
  extent and an indexed seek to its last record instead.

- **A failed consolidation left its working copy open, mapped and unreachable.**
  Every error return from `consolidateOne` abandoned an open `.cleaned` segment
  that nothing names and nothing can close — a file handle and an index mapping
  held until the process exits, plus stray artifacts on disk. `cleanSegment` has
  carried the suffix-checked disposal for this since v0.71.2; its sibling never
  got it.

- **A failed consolidation pass discarded the rewrites it had already
  installed.** `clean.go` answered a consolidation failure with `return cleaned,
  -1, err` — the delete stage's list — throwing away the partial list
  `consolidateSegments` hands back. Every rewrite the pass installed was then
  named by nothing: absent from `l.segments`, so never walked by
  `closeSegments`, while `current()`'s redirect kept reads working and hid it.
  The only symptom is an index mapped until exit and a data directory that will
  not remove.

  v0.71.2 fixed this shape on the compaction path and its changelog entry states
  that "`consolidateSegments` had the same shape and takes the same fix". Half of
  that fix shipped: `consolidateSegments` returns the partial list, and the call
  site kept discarding it. The half that shipped was the half nothing could
  observe.

## v0.72.5 — 2026-08-12

### Fixed

- **A corrupt record crashed the caller's process through
  `ReadMessageMetadata`.** Its own doc promised the opposite — "a record
  corrupted on disk is returned here as data, where `ReadMessage` refuses it" —
  but the bytes went to `parseHeadersAfterValue`, which indexed the payload with
  no bounds check anywhere: key length at `buf[6:10]`, then
  `buf[keyEnd:keyEnd+4]`, then a loop of `buf[n:n+size]`. A key length of `1<<20`
  in a 51-byte record indexed straight off the end. `buf[5]`, for the attributes
  byte, was unchecked too.

  Nothing upstream stands in the way, and that is the point: this path skips the
  payload CRC deliberately (it is a metadata scan — LSO rebuild reads every
  record in the log through it), and the frame header's CRC covers the record's
  identity, not its contents. So the length fields that decide how far parsing
  reaches are precisely the fields nothing verifies.

  The parse is now bounds-checked end to end and returns `ErrCorruptRecord`,
  matching what the sequential path has done since v0.42.0. Every `Raw` handed
  out has therefore survived a checked parse, which is what makes a later
  `Raw.Headers()` — the same walk, unchecked — safe on it.

## v0.72.4 — 2026-08-12

### Fixed

- **A `headersBuf` longer than a header made the log report itself corrupt.**
  `ReadMessage` and `ReadMessageMetadata` document the buffer as needing "at
  least `HeaderBufferLen`", and both then read the WHOLE slice — the readers
  under them fill however many bytes they are handed. So a 64-byte buffer
  consumed a 32-byte header plus the first 32 bytes of the record's payload, the
  next frame began mid-record, and its header failed the CRC. `ErrCorruptRecord`,
  on intact data, reached by a caller doing exactly what the doc invited.

  Both paths now read `headersBuf[:HeaderBufferLen]`, so "at least" is true as
  written and a longer buffer behaves identically.

  This is the mirror of the too-SMALL buffer durable_streams reported against
  v0.41.0 — 24 sites passing 28 bytes, every one panicking. That direction was
  loud enough to be found in a day. This one is silent, and the doc recommended
  it.

### Changed

- `HeaderBufferLen`'s doc said *capacity* where all three call sites check
  *length*, so `make([]byte, 0, HeaderBufferLen)` satisfied the documentation and
  was refused by the code. All three now say length, and say that longer is fine.

## v0.72.3 — 2026-08-12

### Fixed

- **A committed reader that could not locate the high watermark parked forever
  instead of reporting it.** `readLoop` re-locates the watermark whenever it
  reaches the one it holds, and wrote that lookup as
  `hwSeg, hwPos, err := getHWPos(...)`. All three names are new in that scope, so
  the `err` it tested was not the function's named return — the `break` under it
  left the loop with the outer `err` still nil, and `Read` returned `(n, nil)`.

  A caller cannot see that, because `n` is not what it reads: `readMessage`
  ignores the byte count and parses `headersBuf`, which still holds the PREVIOUS
  record's header — valid, CRC-checked, describing a payload already served. So
  the reader asked for that payload, agreed with the watermark on the way back,
  and waited. The symptom was a follower hanging on a healthy log; the reason,
  most often `ErrSegmentReplaced` from a compaction swap racing the lookup, was
  discarded one frame earlier. It is the one error this reader knows how to
  retry.

  The three inline copies of that block are now one `syncHW`. They had drifted
  in a second way too: only two honoured `noWait`, so a bounded read that reached
  the watermark through the third waited for an advance it was built not to wait
  for.

### Changed

- `bytesCompare` and `hasPrefix`, hand-rolled in the key-prefix filter, are now
  `bytes.Compare` and `bytes.HasPrefix` — the same answers from assembly, on the
  path every filtered record is tested against.
- Two conditions in the read path that could not be false are gone: an `until`
  default re-applied over the literal that already set it, and a `hw != -1` guard
  below a branch that returns for every `hw == -1`. The second read as though an
  unset watermark still reached the code under it, which is the exact state its
  own comment describes as impossible.

## v0.72.2 — 2026-08-12

### Fixed

- **A compaction pass that failed part-way abandoned every rewrite it had
  already installed, and each one held a file handle and an index mapping for
  the life of the process.** Reported by durable_streams as a `Log.Close()` that
  returned nil while a segment index stayed mapped: on Windows the data
  directory then could not be removed, blocked by `00000000000000000160.index:
  The process cannot access the file because it is being used by another
  process`. Every close in their test was asserted and every one was nil.

  Installing a rewrite means `Replace`: the rewrite's files are renamed OVER the
  source's and the source is closed, so from that moment the rewrite is what
  that base offset IS on disk. The pass answered any later failure with
  `return nil, 0, -1, err`, and the caller swapped in the delete stage's list —
  the closed sources. Every rewrite already installed was then named by nothing:
  absent from `l.segments`, so `closeSegments` never walked it.

  Nothing noticed because reads kept working. `current()` redirects through the
  source's replacement link, so the log went on serving the right records out of
  a segment its own list did not name — which is exactly how a leak this loud
  stayed invisible to everything but a directory removal.

  The pass now stops and returns the partial list — rewrites where they landed,
  sources everywhere else — as the delete stage already does for the segments
  that survive a partial retention failure. Its verified floor is forced to -1:
  a floor must cover a gap-free prefix, and the rewrite phase spends its budget
  in drop-density order, so "what was rewritten before the failure" says nothing
  about which prefix is contiguous. `consolidateSegments` had the same shape and
  takes the same fix.

- **A truncation that failed on the way to installing its replacement stranded
  it.** `Truncate` builds the replacement first, deliberately — until the first
  `Delete` every failure can be returned with the log exactly as it was found —
  and then deletes the segments above the cut. A failure in THAT loop returned
  with the replacement built, open, and reachable by nothing: `Replace` had not
  run, so it was neither in `l.segments` nor linked to from its source.

  The same defect as the abandoned rewrites above, reached from the other side.
  There the segment was already installed and had to be published; here it is
  not installed yet and has to be dropped. What decides which applies is whether
  the rename has happened, and nothing else — which is why one fix is guarded on
  the working suffix and the other on the install.

- **A rewrite that failed left its working copy open, mapped and unreachable.**
  Only the scan-failure path disposed of one; every other error return out of
  `cleanSegment` — a block write, an fsync, a tiered upload or swap — left an
  open `.cleaned` segment that nothing named, plus its artifacts on disk. It is
  now dropped by a deferred disposal guarded on the working suffix, because past
  `Replace`'s renames that same object owns the SOURCE's files under the
  source's names and deleting it there would unlink the installed rewrite.

## v0.72.1 — 2026-08-12

### Fixed

- **staticcheck could not read Go 1.27's export data, so v0.72.0 shipped with a
  red CI job.** The analyzer reads what the compiler emits, Go 1.27 emits export
  format version 4, and the pinned v0.7.0 understands at most 2 — so the
  toolchain move turned that step into `internal error in importing
  "internal/byteorder" ... please report an issue` for every stdlib package.
  It reads like a staticcheck bug and is not one.

  The pin therefore tracks the toolchain, not just a taste in versions.
  v0.8.0-rc.1 is the release that understands 1.27: an rc toolchain gets an rc
  analyzer, and both drop their suffix together at GA. Recorded at the pin so
  whoever moves the toolchain next knows this line moves with it.

  v0.72.0's tag is 13 of 14 for this reason alone — build, all three platform
  test jobs, both race jobs, both guard-coverage jobs and all six fuzz targets
  were green under the rc. Nothing about the library was wrong; prefer this tag
  anyway, since it is the one with a green CI behind it.

- **A dead store in `leaderEpochCache.ClearEarliest`.** With the analyzer able
  to run again it reported `this value of removed is never used (SA4006)`, and
  it was right. `removed` is last read one line above the branch, where it
  slices the dropped epochs off the front; the `removed--` inside the branch
  meant "one of them went back on", but nothing reads the count afterwards.
  A leftover rather than a masked bug — the count is function-local and never
  returned, so no caller could observe either value.

- `govulncheck` now reports **no vulnerabilities at all** on this tree, where
  go1.26.0 reported GO-2026-4602 as reachable plus 24 uncalled findings.

## v0.72.0 — 2026-08-12

### Changed

- **Builds on Go 1.27, pinned to `go1.27rc2`.** `go.mod` now reads `go 1.27`
  with `toolchain go1.27rc2`, matching durable_streams, sqlcdc and gocdc, and
  CI moves off `go-version: stable` onto the same prerelease.

  This raises the minimum Go for anyone importing commitlog — the `go`
  directive is a floor consumers inherit, unlike `toolchain`, which only binds
  the main module. The one consumer is durable_streams, already on `go 1.27`.

  The motivating defect: on go1.26.0 `govulncheck` reported **GO-2026-4602**
  (*FileInfo can escape from a Root in `os`*) as reachable from our own code,
  traced `FileSegmentStore.List → os.ReadDir`. It is fixed in go1.26.1, so 1.27
  clears it. Only locally built binaries were ever affected; CI built with
  `stable` and consumers build with their own toolchain.

  The two version strings cannot be derived from each other while a prerelease
  is pinned: go.mod spells it `go1.27rc2` and setup-go spells it
  `1.27.0-rc.2`, and setup-go cannot resolve go.mod's form — so
  `go-version-file: go.mod`, the obvious way to keep them in step, does not
  work. Both are written out, with the reason recorded at each. They collapse
  back to one plain release at 1.27.0 GA.

## v0.71.1 — 2026-08-12

### Fixed

- **A guard disarmed itself when the line it anchored on was restructured.**
  "delete does not checkpoint a directory it is removing" anchored on
  `if err := l.closeSegmentsOnly(); err != nil {`, and the next commit turned
  that into `closeErr := l.closeSegmentsOnly()` so the release could also run
  on the failure path. guardcheck reported SKIP rather than a failure — the
  guard stopped guarding and said so quietly, which is exactly why it has to be
  run unfiltered. A filtered run had passed both new guards before the
  restructure, and nothing rechecked them after it. Re-anchored; 105 guards,
  all covered.

- **`Delete` kept the directory lock when it failed, which nothing could ever
  give back.** The first design held the claim on a failed delete on the
  grounds that the segments were still open and no one else should walk into a
  half-deleted directory. That argument assumes the caller retries, and the
  retry assumes the caller still has the log.

  durable_streams does the opposite: a failed delete drops the log, so the name
  stays openable. Once the last reference is gone nothing can call `Delete`
  again — so the lock is held until the process exits, and the directory is
  then neither deletable (no handle to delete it with) nor openable (a fresh
  `New` is refused with `ErrLogLocked`). One transient sharing violation on one
  segment would brick that name for the life of the process.

  This is not hypothetical. sqlcdc's failed deletes fail on a held `.index`
  with "being used by another process" — the segment-close branch exactly —
  and they retry. Under the held-lock design that retry hits `ErrLogLocked`.

  `Delete` now releases on every path out. Nothing is given up by that: the
  lock exists to keep a second WRITER out, and after `Delete` this log is not
  one — `l.deleted` is set, the background loops are joined, appends are
  refused, and whatever handles leaked from the failed close have no writer
  behind them.

  Still no `removeLogDir` after a failed close, deliberately. The files are
  open, so the removal would half-succeed, and `removeLogDir` sequences the
  descriptor last precisely so a partial failure leaves a log that can still be
  opened. Deleting around held segments would strip that protection and leave
  an openable log with pieces missing.

- **`Delete` wrote a high-watermark checkpoint into the directory it was
  removing.** A checkpoint records where to resume a log that still exists.
  `Delete` already said as much about the background loop — it sets `l.deleted`
  before signalling close specifically "so the checkpoint loop skips writing to
  a directory about to be removed" — and then called `closeSegments`, which
  wrote that same checkpoint synchronously on the way out.

  That contradiction was harmless only while a checkpoint failure aborted
  `closeSegments` before it did anything. With the fix below, the error reaches
  `Delete`, whose early return skips both the lock release and the removal — so
  a best-effort write nobody wanted could leave the log closed, the directory
  locked, and every file still on disk. The segment walk is now its own method,
  `closeSegmentsOnly`, and `Delete` calls that. Reported by sqlcdc.

- **A failed high-watermark checkpoint aborted the rest of `Close`.**
  `closeSegments` returned at the first checkpoint error, before closing a
  single segment — so every segment stayed open with its index mapped, for the
  life of the process, because nothing retries a `Close` it was already told
  failed. On Windows a mapped index cannot be unlinked, so the directory could
  not be removed afterwards either.

  The checkpoint had no business being fatal to that. It is an optimization:
  `checkpointHWLoop` logs a failed tick and carries on, on the grounds that
  `RecoverTail` rides out a stale checkpoint. Closing the segments is the part
  nothing else will do. The function's own comment already made this argument
  one paragraph lower, about a segment that genuinely fails to close; the
  checkpoint above it was the same mistake, unfixed. It is now attempted, its
  error collected, and the segment walk happens regardless.

  This also silently broke the invariant `Close` documents at its `release()`:
  that the directory claim is given back only after the segments are shut, so
  no window exists where this process has let go of the directory but still
  holds files open in it. A checkpoint failure opened exactly that window —
  lock released, every segment still live — and a second process taking the
  lock inside it is the two-writer state v0.71.0's lock exists to prevent.

  `Close` still releases the lock on failure, deliberately: durable_streams
  closes and reopens one directory synchronously (a provisionally-opened stream
  reopened with real config, and a promote), and a `Close` that kept the claim
  after a transient checkpoint error would brick that path until the process
  restarted. The fix is to make the failure stop being a half-close, not to
  start holding the directory hostage to it.

## v0.71.0 — 2026-08-12

### Fixed

- **Two processes could open the same log directory, and both would write into
  it.** Nothing in the package locked anything on disk — no `flock`, no
  `LockFileEx`, no `O_EXCL` — so the only exclusion the log had was
  `appendMu`, which is a claim over one process's memory. Every offset the log
  hands out is computed from state that lives there: the active segment's
  `Position` and `NextOffset`. A second process opens the same directory, builds
  its own copy of that state from the same files, and is then correct about a
  log that is no longer the one on disk.

  Both writers append at their own believed position into the same file, each
  overwriting frames the other just wrote. Both see every append succeed. The
  damage only surfaces on the way back out, as `failed to read message headers`
  and zero readable records — which is how durable_streams found it.

  Nothing in the format can detect that after the fact: the frames are
  individually well-formed, and what is wrong is that two of them were written
  to one span of the file. So the exclusion has to happen before the second
  writer exists. `New` now takes an exclusive claim on the directory and holds
  it for the life of the log; a second open returns the new `ErrLogLocked`.
  `Close` and `Delete` give it back.

  `ErrLogLocked` is its own sentinel because the two things a caller might do
  about it are opposite. A directory held by a peer that is still running is a
  configuration mistake — two brokers pointed at one data dir — and retrying
  makes it worse. A directory held by a process that has just died releases on
  its own, and there retrying is exactly right. Only the caller can tell those
  apart, so the log names the condition instead of guessing.

  Implemented with `flock(LOCK_EX|LOCK_NB)` on unix and a `CreateFile` share
  mode of zero on Windows — stdlib only, no new dependency, and on both
  platforms the OS drops the claim when the holder dies by any means, including
  a kill. That is the property this has over a lock file whose mere existence
  means "locked": a crashed process leaves the directory openable rather than
  needing an operator to clear litter.

  This is the LOCAL counterpart to the single-writer contract the `CommitLog`
  interface already stated for tiers, and the failure here is far worse. Two
  writers on a shared store produce a duplicate object and cost storage. Two
  writers on a shared directory destroy the log.

  Checked before building: an unconditional lock does not break the five sqlcdc
  tools that call `New` on a data directory (`buscheck`, `dslayout`,
  `offsetkeys`, `streamkeys`, `walstat`). Every one of them opens a
  `dslayout.Snapshot` copy with a `release()`, never the live directory — so no
  read-only open mode is needed.

## v0.70.0 — 2026-08-12

### Removed

- **Optimistic concurrency control, which nothing enabled and which could not
  have worked.** Breaking, and the whole feature goes: `Options.
  ConcurrencyControl`, `IsConcurrencyControlEnabled()` off the `CommitLog`
  interface, the exported `ErrIncorrectOffset`, both checks it added to the
  append encode path, and `Message.Offset`.

  It offered a compare-and-swap append: set `Message.Offset` to the offset you
  expect this record to land at, and the log refuses with `ErrIncorrectOffset`
  if it would land elsewhere. Inherited from the liftbridge fork.

  Nothing turned it on — not one caller in this repo, in durable_streams, in
  gocdc or in sqlcdc, and not one test. Every call site passed the flag as a
  literal `false`. That alone would not settle it: the v0.68.0 sweep learned
  that a capability whose only consumers are tests is still a capability. What
  settles it is that turning it on would have broken ordinary appends. The
  "don't check me" sentinel was `Offset == -1` while the zero value of the
  field is `0`, so a caller that filled in a `Message` the normal way had every
  record past the first segment position compared against `0` and refused. The
  feature had no working configuration to protect.

  `Message.Offset` goes with it. It was the input to that check, it was the only
  reader, and nothing in the log ever wrote it — while it sat in a block of
  fields documented as ones "filled in by the log on the way out". A caller that
  believed the doc and read it back after `Append` got `0`. Offsets come from
  `Append`'s return value and always did.

### Documentation

- **`Message`'s framing fields say which way each one travels.** The block they
  sit in claimed all three were "filled in by the log on the way out". None of
  that was true of `Offset`, which is now gone, and it was never true of
  `LeaderEpoch`, which the log reads and never writes. The one field that really
  is written back is `Timestamp`: a zero one is stamped with the append's clock
  reading **on the caller's own `Message`**, so a caller that leaves it zero can
  read the stamp back off the struct it passed in.

  That write-back had no test — `TestAppendStampsMissingTimestamps` covers what
  lands on disk, which a version stamping a copy would satisfy just as well. It
  has one now, plus a guard whose neutralization stamps a copy: every on-disk
  assertion stays green and only the write-back goes, so the test is pinned to
  the claim rather than to the stamping.

- **`LatestOffsetBeforeTimestamp` says why it has no caller.** The same sweep
  found it with zero callers in every checkout, which is what
  `ConcurrencyControl` looked like — so the difference is now written on the
  method instead of left for the next sweep to re-derive. It is half of a pair
  whose other half durable_streams uses, it works, its bugs have been worth
  fixing six times in this changelog, and it answers a question the After
  direction cannot: *the last record that existed as of T*, which is what an
  as-of read wants on a log where compaction and retention leave no record
  sitting exactly at T.

  The rule the two cases give: zero callers puts a thing on the shortlist and
  never on the chopping block. What decided `ConcurrencyControl` was not the
  count but that it had no working configuration.

## v0.69.0 — 2026-08-12

### Changed

- **`LastOffsetForLeaderEpoch` now takes an `Epoch` and returns an error.**
  Breaking. It was `LastOffsetForLeaderEpoch(epoch uint64) int64`, and a
  `uint64` has no value meaning "I don't know". A follower with no epoch history
  — a fresh replica, or one whose checkpoint died with its process — had nothing
  to pass but `0`, and `0` is a real epoch: it is what ordinary `Append` stamps
  and the first epoch of every log that has never failed over.

  So the log answered the question it was asked — *where does the epoch after 0
  begin* — which on a log whose first recorded epoch is 1 is offset 0. A
  follower truncates to that answer. durable_streams lost a node this way, with
  450 committed records, and every step of it read as correct.

  The log cannot tell those two callers apart from a `uint64` and must not
  guess, so the caller says which it is: `AtEpoch(e)` names an epoch, the zero
  `Epoch` names none, and the zero one is refused with the new
  `ErrUnknownLeaderEpoch` rather than answered. Every answer for a named epoch
  is unchanged, epoch 0 included.

  The same shape `CleanSpec.Ceiling` was given when its zero value had to mean
  both "no ceiling" and "ceiling at 0". The difference is what the unset case
  does: a missing ceiling has a safe default and falls back, a missing epoch has
  none and is refused. Requested by durable_streams, who own the caller side.

### Fixed

- **An atomic write made the bytes durable and stopped there.** Every file the
  log finishes with a rename — the high watermark checkpoint, a client sidecar,
  an object published into a `FileSegmentStore` — went through a temp file that
  was fsynced and a rename that was not followed by an fsync of the directory
  holding it. That makes the write atomic, which it was documented as being, and
  leaves it undurable, which nothing said. A rename that has returned is visible
  to every later reader in the same boot and can still be undone by a power cut;
  POSIX makes the directory a separate fsync precisely because the bytes and the
  name that reaches them are two different questions.

  It matters wherever the rename IS the commit point, which is all three of
  those: `SyncAll` and `Close` return to a caller that has been told the high
  watermark is recorded, and the tier manifest — the object that says an offload
  happened at all — is `Put` through the same path as any other object.

  Now `syncDir` after the rename. It is a no-op on Windows, where the rename
  underneath is `ReplaceFile` and NTFS journals the metadata change before
  returning, and where a directory cannot be opened for `FlushFileBuffers`
  anyway. On unix an `EINVAL`/`ENOTSUP` from the directory fsync is treated as
  success: those say the filesystem never had the guarantee to give, which is
  not a reason to fail a write that succeeded by every other measure.

  Surfaced by durable_streams losing a recovery-floor sidecar in a soak. That
  particular loss was a `SIGKILL`, which a completed rename survives — this is
  the failure one step past it, and the one their sidecar was relying on not
  happening.

- **A sidecar name was an action, and the log took it on faith.** `PutSidecar`,
  `GetSidecar` and `RemoveSidecar` passed the client's `name` straight into
  `filepath.Join(l.Path, name)`. The contract was written down — *"the name must
  not collide with the log's own files"* — on the `CommitLog` interface, where it
  is advice to the caller rather than something the log does.

  `filepath.Join` CLEANS a traversal instead of refusing it, so `"../../state"`
  named a real file outside the log directory and `RemoveSidecar` deleted it. A
  name did not have to leave the directory to do damage either:
  `"00000000000000000000.index"` named a live index, `"replication-offset-checkpoint"`
  overwrote the high watermark, and `"notes.log"` made the log refuse to OPEN —
  `openLog` scans by suffix and fails outright on a `.log` whose stem is not an
  integer, which is the failure hardest to read backwards to its cause.

  All three calls now refuse such a name with the new `ErrInvalidSidecarName`
  rather than acting on it. The plain-name half of the rule is `validBareName`,
  shared with `validStoreKey` — the manifest's keys had already been given this
  exact treatment for the same reason, and one rule with two callers beats two
  rules that have to agree.

  `GetSidecar` and `RemoveSidecar` had no caller anywhere in the repo before
  this, tests included, so nothing said what they did.

- **The leader epoch checkpoint accepted a negative format version.**
  `readLeaderEpochOffsets` gated the version line with
  `if version > leaderEpochFileV0`, which reads as "reject anything newer".
  `strconv.Atoi` is signed and v0 is the first version there has ever been, so
  the other half of that comparison was not the empty set: every negative
  version passed, and the file was then parsed as v0. A checkpoint whose version
  line read `-1` opened clean.

  The argument against this was already written in the same function, five lines
  below, for the epoch field — *"this is the one place a value from OUTSIDE the
  process becomes one... the file carries no checksum, so refusing it here is
  the only chance to notice"*. It had been applied to the epoch and not to the
  version above it. The parse is the entire integrity check this file gets, and
  one of its two gates was open across half the integer range.

  Now exact equality, which is the right shape rather than merely a tighter one:
  `flush` only ever writes `leaderEpochFileV0`, and tolerance for "older"
  versions is meaningless when nothing is older than the first.

  `numEntries` on the next line is also a signed `Atoi` and is deliberately left
  alone — it is only ever compared as `numEntries != len(epochOffsets)`, a
  negative can never equal a length, and nothing is sized from it.

  Found by sweeping for version gates that accept more than one version. Three
  of the package's four (descriptor, manifest, inspect) were already exact
  equality; this was the one that was not.

### Removed

- `crcField.Check`, unexported and unreachable — no call site anywhere in the
  package, tests and method expressions included. `staticcheck`'s `U1000` never
  flagged it because `&crcField{}` is converted to the `pushEncoder` interface,
  after which every exported method on the type counts as reachable; the same
  shelter that once kept 175 dead lines of `packetEncoder` green. It also read
  as the CRC verification path while the real one is
  `SerializedMessage.crcMatches` and the explicit checks on the read paths.
  Nothing outside the package could name it, so nothing outside the package
  changes.

## v0.68.1 — 2026-08-12

### Security

- **`github.com/klauspost/compress` v1.19.1 → v1.19.2.** Not a routine bump: the
  release fixes an *out-of-bounds write in zstd's unsafe `decodeSync`*, and
  `decodeSync` is the path behind `Decoder.DecodeAll`, which `compress/codec.go`
  calls on every zstd-compressed block this log reads. Two commits ride with it
  ("re-enable unsafe decodeSync memory copies", "fold the unsafe-copy margin into
  the space check"), which is the shape of a fix first mitigated by disabling the
  fast path and then properly closed.

  Also in the release and relevant here: a zstd ARM64 assembly fix where locals
  overwrote the saved link register, and new huff0 ARM64 assembly for
  `Decompress4X`/`1X`. CI is amd64, so neither is exercised there, but any arm64
  consumer of this log is on that code.

  The four dictionary fixes in the same release are *not* reachable — the codec
  constructs its encoder and decoder with no dictionary options at all. Recorded
  so it does not have to be re-derived. `s2` and `flate` are untouched by this
  release.

### Fixed

- **Three maintenance-race tests paced on machine speed rather than on progress.**
  `TestTimestampLookupsWhileCompactionReplacesSegments`,
  `TestOpeningAReaderWhileCompactionReplacesSegments` and
  `TestOpeningAReaderWhileRetentionDeletesSegments` (the last two share one
  helper) each held themselves open until `writes >= 1000` against a hard
  60-second deadline, then asserted `writes > 500` afterwards. Both numbers were
  proxies for one condition — that a pass had actually moved a segment, so the
  window under test existed — and neither measured it.

  The timestamp one failed CI on the v0.68.0 tag: `too slow: writes=799
  probes=2693442`. The runner was about 2.2x slower than usual (1058s for this
  package, against a 447-499s band and 429s locally), so the writer reached 80% of
  a fixed target on a machine at 45% speed. Its probes meanwhile ran to 898x
  *their* threshold, which is how lopsided the two guesses were. A re-run of the
  same commit took 480s and passed.

  All three now wait on `segmentDepartures`, a new counter following the existing
  `segmentScans` precedent for a package-level counter tests assert on. It counts
  a segment LEAVING the log — superseded by a replacement (`Replace`,
  `SupersededBy`) or removed outright (`Delete`) — which is exactly the condition
  `readAtLocked` and `current()` test (see the flag merge below). Counted once per
  segment: the delete path skips one that has already left, so
  `cleanupEmptySegment`, which marks and then deletes, reports one departure
  rather than two. The post-hoc `writes > 500` assertion is replaced by the
  departure count itself.

  Counting only *replacements* was the first attempt and it was wrong. Retention
  deletes a segment and links it to nothing, so that counter stayed at zero for
  the whole of `TestOpeningAReaderWhileRetentionDeletesSegments` and it could only
  ever time out. Targeted runs of the two tests the change was aimed at passed;
  the full suite is what reached the third. The union is the fix, for the same
  reason `current()` draws its distinction by the link rather than the flag.

  Three hundred departures, calibrated against both directions of getting it
  wrong. Replacements alone are front-loaded and then asymptotic — three arrive in
  the first half-second, but only 19-26 after 140 seconds, as each pass slows over
  a longer segment list — so a budget of 100 on that counter was unreachable.
  Counting deletions too changes the supply completely, and a budget of 10 then
  finished in under a second, which is far *less* hammering than the 60-second
  gate it replaces. 300 puts each run at 3-8 seconds with 940-2030 appends, above
  the old write gate, with progress rather than the clock ending the run. Both
  measurements are recorded in the tests so the next reader does not re-derive
  them.

  The deadline moves from 60 seconds to 5 minutes and is re-labelled as what it
  should always have been: a liveness backstop for a log where maintenance has
  stopped moving segments at all, not a performance assertion. Conflating those
  two is what made the old gate fail.

  Neither reader test had ever failed. They carried the identical gate and were
  one slow runner away.

### Changed

- **`segment.replaced` and `segment.gone` merged into one `segment.left` flag.**
  Internal; no public surface moves. The two fields recorded how a segment left
  the log — rewritten over versus files removed — and nothing ever asked. Every
  read site tested them together as `replaced || gone`; no site anywhere tested
  either alone.

  `current()` already said why: the cases are told apart by the LINK, not by the
  flags. A departed segment with a `replacement` is a redirect and a reader
  follows it; one without is a skip. Which flag was set answers neither question.
  `cleanupEmptySegment` had already collapsed the distinction in practice — it
  set `replaced` on a segment with no replacement, which is precisely what `gone`
  meant — so the two names had stopped describing the two states anyway.

  The counter added above is what made this concrete: it had to count the union
  to be correct, and a counter of one flag alone was a bug (see the retention
  case above). One flag also removes a latent trap in `Delete`, which guards its
  count with "has this segment already left by the *other* route" — correct only
  for as long as there are exactly two routes. It now reads the same flag it is
  about to set.

  What is lost is a human-readable distinction in a debugger. The merged field's
  doc records both origins, and `replacement` still distinguishes them where it
  matters. `Replace`'s failed-reopen path is unchanged: it deliberately sets
  *neither* flag, so a rewrite that could not be reopened stays loudly closed
  rather than silently skipped over records that are sitting on disk.

## v0.68.0 — 2026-08-12

### Removed

Deleted in a complexity sweep. Breaking and free: nothing in any repo on this
machine set the option, and pre-v1 there is nothing to migrate.

- **`Options.CompactMaxGoroutines`.** An unbounded public `int` whose entire
  meaningful range was `{1, everything else}`. `loadOrBuildDigests` clamped it to
  2 — each digest build holds a transient per-segment key map, and ten at once
  measured over 1GB on a 12h soak — so no value could raise the cap and the only
  setting that changed anything was `1`, which made the builds serial. The
  field's own doc spent ten lines apologising for the gap between its name and
  its behaviour, which is the tell: a knob needing a paragraph about which of its
  values do nothing is not a knob.

  It read as a lie as well as being one. Two of its three cross-references in
  this repo claimed it bounded segment *rewrites*. It never did — rewrites are
  bounded by time (`CleanRewriteBudget`, `TierBudgets`), and a worker count is
  the wrong instrument for them. Nobody caught the contradiction because nothing
  ever set the field.

  The build cap is now a `const 2` carrying the soak measurement that fixes it
  there. A caller wanting shorter passes sets `CleanRewriteBudget`, which was
  doing the work all along.

- **Five write-only struct fields**, all internal, so no caller is affected.
  A sweep of every struct in the package for fields that are written and never
  read now comes back empty.

  | field | was |
  |---|---|
  | `Reader.uncommitted`, `Reader.noWait` | copied out of the `readSpec` at construction |
  | `commitLog.name` | set to `filepath.Base(path)` |
  | `commitLog.syncJoined` | incremented per joiner, reset per flush |
  | `prefixRun.segIdx` | set at construction, always `0` |

  `prefixRun.segIdx` took two more things with it: `planRuns`' `segIdx`
  parameter, whose only use was setting the field, and the literal `0` both call
  sites passed. The concept was left from a design where runs were planned
  *across* segments; today `planRuns` is called once per segment.

  Write-only state is the hard kind to find. staticcheck cannot flag it, because
  a write counts as a use; review cannot, because a field named after a real
  concept reads as though something consults it. `Reader`'s pair had extra cover:
  `uncommittedReader` and `committedReader` carry their own fields of the same
  names, so the reads look present when you grep — they are just not reads of
  these.

  Deliberately kept: `commitLog.syncLeaders` and `syncFollowers`, which are also
  written only by `Sync` but *are* read, by `sync_batch_probe_test.go`. Only a
  test reading a field is not a reason to remove it — see the
  `OverrideHighWatermark` note below for what that mistake costs.

### Added

- **`TestTheHighWatermarkNeverGoesBackwards`.** `SetHighWatermark` is monotonic,
  and that rule had no test at all, from the day it was written — while
  `OverrideHighWatermark`, the *exception* to it, did. The escape hatch was
  covered and the contract was not. Registered with guardcheck (now 100 guards).

- **`TestOverrideHighWatermarkBuildsRecordsAboveTheBoundary`**, replacing
  `TestOverrideHighWatermark`, which asserted only that a lower value was
  applied — the mechanism, not the reason.

  `OverrideHighWatermark` was deleted during this sweep and restored before
  release. Recorded because the failure is instructive rather than incidental.
  Its doc justified it with "a caller that has some other reason", and a doc that
  will not name its caller reads as one having none, so the sweep looked for
  production call sites, found none, and read the remaining test uses as
  incidental. One was not: it was the only construction of the above-watermark
  state, performing a real lowering. The substitution looked equivalent — the
  watermark there was assumed to be `-1`, making the call a raise — and was not,
  because the caller commits as it appends. `SetHighWatermark` silently no-opped
  and turned a downstream fetch test red. Caught by `durable_streams`, who
  measured it.

  Two things changed as a result. The method's doc now names the state it exists
  to build, and this test asserts that state rather than the mechanism, so the
  next sweep meets a reason instead of a shrug. **A capability whose only
  consumers are tests is still a capability;** what made it invisible was the doc
  declining to say what it was for.

### Fixed

- **A consolidation pass held every segment's backing to the end of the pass.**
  `consolidateSegments` opened a scanner per segment with `defer ss.Close()`
  *inside* its loop — the only defer-in-loop in the package.

  What that deferred was not what it looked like. Not a file handle: `Scan`
  closes the stream itself the moment the scan ends, precisely because a caller
  typically rewrites the segment straight after draining it and, on Windows, an
  open read handle blocks the rename that installs the rewrite. That hazard was
  already closed a layer down.

  What `Close` still holds is the *pin*, which outlives the stream deliberately —
  the scan may read through the backing after the stream is gone.
  `acquireBacking` only takes one for a store backing, a local file being
  reference-free because nothing supersedes it under a reader. So this reached
  **tier-backed segments only**, and it meant every already-consolidated
  segment's superseded object stayed referenced until the whole pass returned
  rather than until its own iteration ended, with nothing able to reclaim them in
  between. Bounded per pass by `maxRewrites`/`CleanRewriteBudget`, so a delay and
  a bounded accumulation rather than a wrong answer.

  The loop body moved into `consolidateOne` so the defer scopes to one segment.
  The release stays *after* `Replace`, where the inline defer had it: dropping
  the pin before the swap would release the claim on a backing about to be
  superseded. Scoping the release to the iteration is the fix; moving it earlier
  within the iteration would be a commit-point change and is a separate question.

## v0.67.4 — 2026-08-11

### Fixed

- **Leader epoch 0 could never be recorded.** `assign` gated on
  `epoch > latestEpoch()`, and `latestEpoch()` answers `0` for an empty cache —
  so `0 > 0` refused the first assignment on every log, permanently, for the
  whole of its life before a first failover. Ordinary `Append` stamps epoch 0,
  so this was the common case rather than an edge one: a sentinel made of a
  valid value, where `0` meant both "nothing recorded yet" and "the latest epoch
  is 0" and the gate could not tell which it had.

  It was silent as well. The refusal's `warn` fires on `epoch < latestEpoch`
  (`0 < 0`) or `offset < latestOffset` (against `-1`), and neither holds for the
  epoch-0 case, so nothing was logged at the call or afterwards.

  The gate now asks the cache's length, which is the fact actually in question,
  and leaves the comparison meaning only what it says. Monotonicity is unchanged
  for every assignment after the first.

  Scope, since the discovery came from an incident this does **not** explain:
  `LastOffsetForLeaderEpoch` is unaffected either way, because it answers from
  `findEpoch(epoch+1)` — the *next* epoch's anchor, which was always recorded
  normally. What was wrong is the cache's account of where a log's epoch history
  begins, and with it the "a trim at the earliest end re-anchors rather than
  drops" invariant, which could not apply to an epoch that was never added.

- **A refused epoch assignment could log nothing at all.** The refusal warning's
  two cases are strict comparisons — `epoch < latestEpoch` and
  `offset < latestOffset` — so an assignment of the epoch already latest, at an
  offset at or after its own, fell between them: no entry written, nothing
  logged, `nil` returned. Indistinguishable from a successful assign, at the
  call and forever after.

  There is now a default arm, so the switch is total by construction rather than
  by having anticipated the cases. Requested by durable_streams, whose side
  believed it held epoch history it had never been able to record; the silence,
  not the refusal, is what cost them the investigation.

  The two existing messages are unchanged apart from a trailing `%s` that was
  never a format verb — `slog.Warn` takes attributes, not a format string — and
  so was printed literally.

## v0.67.3 — 2026-08-11

### Fixed

- **A lookup by timestamp failed at random while compaction ran.**
  A pass rewrites and removes segments as it walks them and swaps the published
  segment list only at the very end, so for the whole of it the log hands out
  segments that are already replaced or gone. The offset path resolves every
  candidate through `current()` for exactly that reason; the timestamp path
  walked the list directly, so it searched a replaced segment and got
  `ErrSegmentReplaced` — which it correctly refuses to read as "not in this
  segment" and returns as an error. Both public lookups failed that way, since
  `LatestOffsetBeforeTimestamp` is defined in terms of
  `EarliestOffsetAfterTimestamp`, and the records were sitting in the
  replacement the whole time. A consumer resuming as-of a timestamp against a
  compacting log saw it as an intermittent, unexplained failure.

  Resolving is necessary but not sufficient: `current()` and the search are two
  steps, and a pass can replace the resolved segment between them. So the lookup
  also retries a swap, the same answer `newSourceReader` already gives for the
  same two-step problem on the offset side, under the same bound and the same
  predicate.

  Found as a once-in-a-suite flake in an unrelated test, and reproduced by
  driving compaction rather than waiting for the cleaner's interval — the
  cleaner's interval is minutes, so a test that merely enables compaction and
  waits never opens the window at all.

  Stated plainly because the two halves are not equally covered: removing the
  resolve fails the new test in 0.13s, and it is guarded. Removing the retry
  passed 8 runs out of 8, having failed once at 3s in an earlier sample, so its
  window is real but far too narrow to schedule — it is not registered as a
  guard, and rests on that observation and the precedent rather than on a test
  that bites.

## v0.67.2 — 2026-08-11

### Fixed

- **A corrupt block header mid-segment still discarded every record after it.**
  v0.67.1 stopped `scanBlocks` discarding the whole file when the FIRST header
  did not parse, but left the same walk discarding from any later bad header to
  the end of the file — which is every record beyond the flipped byte,
  acknowledged ones included. A single corrupted codec field halfway through a
  segment cost twenty committed records, and the open still returned nil with
  the watermark clamped down to match.

  The rule is now the same at every offset, and simpler for it: tearing versus
  corruption. A partial write leaves a *prefix*, so a torn tail always runs out
  of file — a header the file is too short to hold, or a payload shorter than
  its header promises — and both of those still discard, because that is what a
  crash mid-append leaves. A header that is entirely present and does not parse
  is corruption, and the open refuses. That is the same bound the raw walk gets
  from the high watermark, applied where no offsets exist yet to compare
  against.

## v0.67.1 — 2026-08-11

### Fixed

- **Recovery discarded a torn tail straight through committed records.**
  `reconcileIndexTailRaw` walks the frames the index does not cover, and a frame
  it cannot resolve ends the walk — everything from there to the end of the file
  is handed to `discardTornTail`. Above the high watermark that is the point:
  those bytes are uncommitted. But the walk had no floor, so the damage scaled
  with how early the bad frame sat, and a FIRST frame whose size field overruns
  the file resolves nothing at all — the "tail" is then the entire segment.

  The open still succeeded, and the watermark, now higher than the empty log,
  was clamped down to match it. Fifty acknowledged records to nothing, durably,
  with a `WARN` as the only trace. A replica doing that on restart comes back
  claiming it holds no records, and its leader truncates its own tail to agree.

  A torn tail is now dropped only above what the log has acknowledged. The floor
  is gated on the segment's own base offset rather than the watermark alone, so
  a crash during a fresh segment's first append — which holds nothing committed,
  and is the ordinary unclean shutdown this path exists for — still discards and
  still opens. Reported as a whole-partition loss by sqlcdc, whose `hw=895
  newest=-1 truncated_at=0` is this bug seen from outside. This needs no
  compression configured and is therefore the default shape; the block-format
  case below is the same defect on the other walk.

- **A corrupt first block header discarded the whole segment, and the open
  still succeeded.** `scanBlocks` walks a chain of block headers, and a header
  it cannot resolve ends the walk rather than failing it — right for a torn
  tail, since everything before the cut is intact and refusing would take every
  sealed segment down with the active one. But the walk then hands the distance
  between where it stopped and the end of the file to `discardTornTail`, and
  when the FIRST header is the one that did not parse it had got nowhere: the
  "torn tail" was the entire segment.

  So one flipped byte in that header truncated the whole file, and `New`
  returned no error. The log came up empty and the high-watermark checkpoint was
  clamped down to match it — fifty records to none, durably, with a log warning
  as the only trace. A replica doing that on restart comes back claiming it has
  recorded nothing, and a leader then truncates its own tail to agree.

  A header that is entirely present is the header that was written, because a
  partial write leaves a prefix. So an unparseable one at position 0 is now
  refused, on the same reasoning the block-version check beside it already used:
  dropping bytes we merely failed to understand is not recovery. The genuine
  cut-inside-the-first-block cases — too few bytes for a header, or a payload
  shorter than the header promises — are unaffected and still discard.

## v0.67.0 — 2026-08-11

### Fixed

- **A reconcile that could not read the tail reported success.**
  `reconcileIndexTailRaw` walks the log frames the index does not yet cover.
  When the read of a frame header failed it broke out of the loop, and that
  path leaves `torn` false — so the function fell through to `return nil` and
  the segment opened having reconciled nothing, with `lastOffset` at the stale
  index tail while `position` was the file size. The next append then took its
  offset from `NextOffset` and wrote a record at an offset the file already
  held. That is the duplicate a replica was reported holding, and the open that
  produced it had said it was fine.

  Keeping the bytes on a failed read was always right; reporting success was
  not. An open that cannot read the tail it is reconciling now fails, and the
  caller retries it. Reported by durable_streams, who also established that
  neither offset guard below catches this one — the under-reported tail means
  the follower asks for a frame strictly *above* what the log claims.

- **`AppendMessageSet` took the caller's offsets on trust.** `Append` derives
  every offset from the segment's own tail and cannot produce a bad one;
  `AppendMessageSet` takes the framing verbatim, and nothing on that path
  compared those offsets to anything. A set starting at or below the tail was
  written as-is, and the log then held two records claiming one offset.

  A set must now hold at least one whole frame, start strictly above the log's
  newest offset, and ascend; anything else returns `ErrMessageSetRefused` and
  writes nothing. Strictly above rather than exactly next, because compaction
  leaves holes and `ReadMessageSet` serves the survivors — a follower resuming
  from a compacted source appends across a gap legitimately.

- **A segment's tail could move backwards, and an empty set panicked it.**
  `segment.write` took `lastOffset` by assignment, whichever direction the last
  entry pointed; it is the field `NextOffset` derives from, so a segment that
  lowers it hands out offsets that already name records. Now a max. Separately,
  it indexes `entries[len(entries)-1]` and the block path takes `entries[:1]`,
  both *after* the payload is on the backing, while `entriesForMessageSet`
  yields nothing for any input shorter than one header — so a short or garbled
  frame panicked a segment it had already appended bytes to. Refused before the
  write now.

### Changed

- CI checks that a doc comment names the function it sits above
  (`hack/docdrift.sh`). Go's convention is that a doc comment opens with its
  function's name, so an opener naming something else means a rename or a move
  left the doc behind — invisible to the compiler, to vet and to staticcheck.
  Not the `ST*` comment-style family this repo declines: that asks whether a
  comment is well-formed, this asks whether it is attached to the right
  function. Six were.

## v0.66.2 — 2026-08-11

### Fixed

- **A failed `Delete` could leave a log nothing can ever open again.**
  `os.RemoveAll` does not stop at its first failure — it records one error and
  keeps deleting the remaining entries. So a single held file (a locked
  `.index`, a scanner's handle) never prevented it removing everything around
  that file, the descriptor included. The result was not a delete that failed:
  it was a directory full of segments with no descriptor, which
  `readDescriptor` refuses on every subsequent open, forever. No retry fixes
  that. Reported by sqlcdc, which lost a view's name to it in a soak.

  `Delete` now removes the descriptor LAST, because the descriptor is the
  commit point: everything else in the directory is data it accounts for, so
  while it survives the log survives, and a failed delete is a log that still
  opens and a `Delete` that can simply be retried. Removing it is what makes
  the log stop existing, so it goes after everything that can fail.

## v0.66.1 — 2026-08-10

### Changed

- **The two lines that make `NotifyLEO` safe are now tested.** `NotifyLEO`
  reads shared state and mutates based on it across two independent loads of
  `vActiveSegment` — one to pick the segment to park a waiter on, one inside
  `NewestOffset` to decide whether parking is still right — with no lock
  spanning them. The LEO comparison does not catch a roll landing in between: a
  roll writes no records, so the new active segment's `NextOffset` equals the
  old LEO and the check agrees. The waiter parks on a segment nothing will ever
  append to again.

  It is safe, for a reason neither call site shows. A roll seals the segment it
  rolled off, and `seal` does two things under that segment's own lock: sets
  `sealed`, and closes every channel already registered. The lock reduces the
  interleavings to exactly two and one of those lines covers each. Removing
  either parks a waiter forever, silently, and `-race` finds nothing — every
  access is correctly locked; the bug would be the gap between two correctly
  locked operations. Both halves now have a test and a guard, and the argument
  is recorded on `NotifyLEO`. No behaviour change.

## v0.66.0 — 2026-08-10

### Fixed

- **One segment refusing to close left every later segment open.** `Close`
  returned at the first error, and that walk of `l.segments` is the last one
  there will ever be: `segmentsClosed` is set right after it, and no caller
  retries a `Close` it was already told failed. So a single failing segment
  took the whole tail of the set with it, holding file handles and index mmaps
  for the life of the process — and on Windows a mapped index cannot be
  unlinked, so the log's own directory could not be removed either. It now
  closes every segment and joins the errors, which is the rule `closeSegment`
  already held over its own two halves, one level up and for the same reason.
  Found while looking into a durable_streams report of a partition `.index`
  still open after every `Close` had returned; that report is an unreproduced
  observation and this is not confirmed to be its cause.

- **One retry budget served two callers with opposite failure economics.**
  `atomicWriteRetryBudget` was 500ms, and its comment gave the reason: a
  checkpoint write runs on a tick, a genuinely conflicted destination never
  clears, and stalling every tick for seconds to discover that is worse than
  letting the next tick try — *"a lost checkpoint write is retried by
  definition"*. True for the tick. The same write is also reached by `SyncAll`
  and by `Close`, which are durability BARRIERS: a caller invoked them to make
  the log durable now and is waiting on the answer, and nothing retries them.
  There the 500ms turned a transient Windows handle — precisely what
  `AtomicWriteFileWithRetry` exists to ride out — into a hard failure out of a
  user-facing operation. Reported from durable_streams as a failed stream
  creation: `cannot replace ...replication-offset-checkpoint...: Access is
  denied`.

  The budgets now split on whether anything will retry the operation, which is
  what actually differed, rather than on read versus write, which did not.
  `waitedOnRetryBudget` (5s) covers every retry a caller waits on — the boot
  read, `SyncAll`, `Close`, `PutSidecar`, and any caller outside this package
  reaching the exported helpers, which cannot have a tick of ours behind it.
  `tickWriteRetryBudget` (500ms) is the checkpoint tick alone, an order of
  magnitude under the default `HWCheckpointInterval` so a stalling tick cannot
  back the loop up. `readRetryBudget` and `atomicWriteRetryBudget` are gone;
  the read side's bound is unchanged in value.

  The suite could not see this: every Windows retry test released its handle at
  120–300ms, comfortably inside even the short budget. The new one holds past
  `tickWriteRetryBudget` and well inside `waitedOnRetryBudget`, so it fails on
  exactly the bound that was wrong.

- **A refused `DeleteStoreObjects` batch deleted a prefix of itself.** The
  documented contract was that the call is refused outright while a tier is
  read-only; the code checked each object as it reached it, so a batch ran
  until it hit one it may not touch and returned what it had already removed.
  What survived therefore depended on the ORDER of the caller's slice — the
  same batch, sorted differently, left a different set of objects standing.
  For an unfenced operator tool that is the worst property available: the
  caller sees an error either way, and a retry after fixing the ownership
  removes a different remainder than the first attempt implied. Every object's
  tier is now resolved and checked before anything is deleted. Only a failure
  from the store itself can still leave a batch half applied, and that one
  returns what got through, because those objects really are gone.

### Added

- **BREAKING: `UnreferencedObjects` is on the `CommitLog` interface.** Breaking
  only for an external implementor of the interface; every caller gains a
  method. It was a method on
  the concrete log only, while the interface doc directed callers to it three
  times — the single-writer contract's warning about shared stores, the
  `DeleteStoreObjects` pairing, and `CleanWithSpec`'s note on crash orphans all
  named a method no interface consumer could call. It was introduced as a pair
  with `DeleteStoreObjects` and only one of the two was exported.

### Changed

- **Interface documentation that still described the pre-chain single store.**
  `Options.SegmentStore` became `Options.Tiers` in v0.63.0 and the contract
  docs did not follow: the single-writer section spoke of "a log with a
  SegmentStore" and "a log with no SegmentStore", `TierManifest` described
  reading one store rather than reading every tier and merging, and
  `CleanWithSpec` had reclamation stopping wholesale on a read-only tier when
  read-only is per tier and so is the reclaim. `TierManifest`'s merge rule —
  a double claim resolved from `TierObject.MovedFrom`, and refused when the
  claims do not explain it — was not documented anywhere a caller reads.

## v0.65.1 — 2026-08-10

### Fixed

- **A move's release could fail silently and make a double claim permanent.**
  `writeOneTierManifest` took a tier name and published nothing at all when
  nothing matched it, returning nil. That call is the RELEASE half of a move:
  the destination's manifest is already published, both tiers claim the
  segment, and the whole reason that is survivable is that the source is about
  to stop naming it. A name matching no tier turned the recoverable window into
  a permanent one — `TierObject.MovedFrom` exists to make that state readable,
  not to make it the resting state. Nothing upstream caught it either, because
  `tierWritable` answers "yes" for a tier it has never heard of.

  The name is now resolved, and an unconfigured one is an error like everywhere
  else an object names a tier.

### Changed

- **One function publishes tier manifests, not three.** `writeTierManifest` and
  `writeOneTierManifest` were one-line delegations to `publishTierManifests`,
  which took an `only string` where `""` meant "every tier". `""` is the one
  tier name `validateTiers` refuses — it is what an absent field decodes to,
  which is the whole reason it is refused — so the publish path had given a
  meaning to the value that exists to have none. `publishTierManifests` now
  takes the tiers to publish. Internal; no API change.

  `movePlaced` and `storeForTier` also stop carrying their own copies of "find
  the tier by name"; both go through `tierByName`.

## v0.65.0 — 2026-08-10

### Changed

- **BREAKING: `Message.AckInbox` and `Message.CorrelationID` are gone.** NATS ack
  routing from liftbridge, dead since the extract: `Encode` never wrote them,
  nothing in this repo or any consumer read them, and a value round-tripped
  through the log came back with them empty. A caller that set one was writing
  to a field the log threw away.

- **The `Options.Tiers` godoc no longer refuses a chain.** It said "This build
  accepts at most ONE tier and refuses more", which v0.64.0 made false —
  `validateTiers` refuses a duplicate NAME and nothing else. Reported by
  durable_streams, who verified a two-tier `New` succeeds. The doc a caller
  reads before writing its config is the worst place for a stale refusal: it
  talks them out of the feature and nothing goes red.

### Removed

- **Dead code, found by the recurring sweep.** No behaviour change:
  - `findSegmentByBaseOffset` — "the next segment whose base is above mine",
    the query that served a record twice across a `TruncateBefore` trim. Its
    last production caller went in v0.50.1; since then its own unit test kept
    it green and reachable. A helper whose semantics are known-wrong for the
    log's invariant should not be sitting in the package with a passing test
    next to it.
  - Seven of `packetEncoder`'s thirteen methods, and both implementations of
    each: `PutBool`, `PutInt32`, `PutInt64`, `PutArrayLength`, `PutRawBytes`,
    `PutNullableString`, `PutStringArray`, `PutInt32Array`, `PutInt64Array`.
    Kafka's wire protocol, of which the record format writes none. staticcheck
    counts a method as used when it satisfies an interface, so declaring them
    kept fourteen unreachable bodies green indefinitely.
  - `newStoreBacking`, which asked the store for an object's size. Every real
    open knows the size already, so it was a round-trip waiting to be put back
    on the boot path. `newStoreBackingSize` also stops returning an error it
    could never produce, removing four dead branches — two of them directly
    above a comment explaining that the repoint below is unconditional.

### Fixed

- **`TestStoreBacking_RestoreRequired` asserted a path that no longer ships.**
  It checked that a restore-required tier reports itself when a backing is
  OPENED; opening stopped touching the store when boot started reading sizes
  from the tier manifest. It now asserts the contract as it is: opening costs
  no store call, and the tier reports itself to the caller that READS the
  segment — so a log holding a cold segment nobody reads opens.

## v0.64.1 — 2026-08-10

### Fixed

- **`primaryStore` had no callers left**, and staticcheck's `U1000` refused
  the v0.64.0 tag over it. Step 3 gave every write site a tier to name, so the
  "just give me somewhere to write" helper became dead. No library change:
  v0.64.0 and v0.64.1 behave identically, and every test, race, guard-coverage
  and fuzz job was green at v0.64.0. Use this tag rather than that one.

## v0.64.0 — 2026-08-10

### Added

- **A log can have a chain of tiers, and a segment can be placed in one.**
  `CleanSpec.TierPlacement map[int64]string` names, per segment base offset,
  the tier that segment should live in after the pass; commitlog copies the
  objects, repoints the segment and republishes both manifests.
  `Options.Tiers` no longer refuses a chain longer than one — what it refuses
  now is a chain it cannot honour, which as of this release means a duplicate
  tier name.

  The second hop is EXPRESSED, not scheduled. commitlog gains no clock for
  descent between stores: that is a policy question about cost and durability
  that only the caller has the context for. The first hop — local disk into the
  nearest tier — stays scheduled here by `LocalRetentionAge`, because it is
  about local disk pressure, which the log can see and the caller cannot.

  A placement naming a tier that is not in `Options.Tiers` is an error and
  nothing moves. A placement naming a base offset with no offloaded segment
  behind it is skipped — a caller's map is a snapshot, and retention deletes
  segments between the snapshot and the pass.

- **`TierObject.MovedFrom`, and an interrupted move that reopens.** A move
  commits by publishing the destination's manifest and releases by publishing
  the source's, in that order, because the reverse leaves a segment named by
  nothing. Between the two, both tiers claim the segment — the state
  `mergeTierManifests` refuses — so the destination's entry records which tier
  it came out of and the merge drops the source's stale claim. Every other
  double claim is still refused, and the choice still comes from what the
  stores say rather than from how the caller listed them.

### Changed

- **BREAKING: `CleanSpec.TierRewriteBudget` becomes
  `CleanSpec.TierBudgets map[string]time.Duration`.** One rewrite budget per
  tier, falling back to `RewriteBudget` for a tier with no entry. A rewrite in
  a fast nearby store and one in a cold archive differ by as much as local and
  remote do, so a caller that gives its archive a small budget must not thereby
  shrink its hot tier's.

- **BREAKING: tier retention and tier ownership move onto `Tier`.**
  `Options.MaxTierBytes`, `MaxTierMessages`, `MaxTierAge` and
  `Options.TierReadOnly` are gone; they are now `Tier.MaxBytes`,
  `Tier.MaxMessages`, `Tier.MaxAge` and `Tier.ReadOnly`. `SetTierReadOnly`
  takes the tier's name and returns an error, and a name that is not in
  `Options.Tiers` is refused rather than ignored — this is the one call whose
  purpose is to STOP writing, so a caller that misnames the tier it is handing
  over must not be told it succeeded.

  Both were per-LOG settings for something that is per tier. A node can own the
  store it writes and not the archive under it, and one flag for the whole
  chain would have made it choose between offloading nothing and claiming a
  store it does not own. Likewise a budget: descent is per tier, so a segment
  over the hot tier's limit has left that tier rather than been deleted, and
  the record is gone only when the last tier in the chain runs out of room.

  A single-tier log moves each value one level down and is otherwise unchanged.

- **Retention deletes a tier at a time, oldest first, and a newer tier waits
  for the older to drain.** Each tier's budget applies to that tier's own
  contiguous run of segments. A run that keeps anything — for its budget, or
  because this log does not own its tier — stops every newer run from deleting,
  because deleting around a surviving older segment does not shorten the log,
  it punches a hole out of its middle. The retention floor stays ONE allowance
  for the whole tiered half rather than one per tier.

  A segment naming a tier that is not configured is an error, not a segment
  cleaned under a default budget: an unlimited default keeps objects a caller
  believes it bounded, and a zero default deletes a store because the caller
  mistyped its name.

- **The tier manifest and the log descriptor publish per tier.** A pass on a
  chain with one owned tier and one it does not own republishes the half it
  may, rather than skipping both.

### Added

- **Each tier carries its own manifest, and open merges them.** A manifest now
  names only the objects of the tier it sits in. With one tier configured — all
  this build accepts — that is the same single manifest it always was, so
  nothing changes for a single-tier log.

  This is the fork `docs/multi-store-tiering.md` marked as its only genuine
  one, decided the way `docs/tier-layering.md` already argued: the store
  describes itself and a log adopts what it finds. One manifest in the nearest
  tier would leave the archive holding bytes it cannot describe.

  The price is that two manifests can disagree about who owns an object.
  `mergeTierManifests` refuses that and names both tiers rather than picking a
  winner — picking would serve one tier's bytes and silently orphan the
  other's, and picking by configuration order would make the answer depend on
  how the caller listed its stores.

## v0.63.0 — 2026-08-10

### Changed

- **BREAKING: `Options.SegmentStore` becomes `Options.Tiers []Tier`.** A log's
  store is now a named entry in a chain — `Tier{Name, Store}` — and every
  offloaded object records the name of the tier it went into. This build accepts
  at most ONE tier and refuses more, because everything below the first would
  silently never be written to; step 3 of `docs/multi-store-tiering.md` lifts
  that. Nothing else about a single-tier log changes.

  A tier with no `Name` or no `Store` is refused at `New`, where the option
  arrives, rather than at the first offload.

- **BREAKING: `DeleteStoreObjects` and `UnreferencedObjects` take and return
  `StoreObject{Tier, Key}`.** A bare key stopped being enough the moment a log
  could have two stores, and the sweep's whole subject is objects no manifest
  names — so nothing else could resolve one afterwards. The pair round-trips.

- **An object naming a tier that is not configured is an error.** It is not
  resolved against the nearest store: keys are allocated per upload, so
  answering with the primary tier reads one store's bytes under another store's
  keys and reports a missing object rather than a misconfigured chain.

- **The log descriptor is written to every tier and read from the nearest.**
  A store that cannot say which log it belongs to is not self-describing, and
  `logIsNew` now asks every tier — a node adopting ONE tier of a chain has that
  tier's descriptor and not the others', and treating it as a new log because
  the first store it looked in was empty is the silent adoption the descriptor
  exists to prevent.

## v0.62.1 — 2026-08-10

### Fixed

- **A Windows store-retry guard was covered by a race the runner could lose.**
  `TestAStoreReadRetriesThroughAHeldObject` released its exclusive handle 300ms
  after TAKING it, and the read it was protecting came several assertions
  later. On a slow runner that setup consumed the whole window, so the read
  began after the handle was already gone and passed without retrying — which
  is what `guard coverage (windows)` reported at the v0.62.0 tag while the same
  run was green locally.

  The release is now timed from the moment the read starts. Both Windows retry
  tests also assert up front that the exclusive handle actually denies an open
  on the machine running them, so a runner where it does not fails instead of
  passing every assertion for the wrong reason.

  No library code changed. This is the first thing the v0.62.0 guard-ordering
  fix caught: the guard had been running below the summary that reports it, so
  a failure went nowhere.

## v0.62.0 — 2026-08-10

### Added

- **A tier manifest names the tier holding each object.** `TierObject` gains
  `Tier string`, and the manifest version goes to 3. With one store configured
  the answer is always `"default"`, so nothing behaves differently — the field
  exists so that a store already carrying a manifest can describe itself once a
  second tier exists, rather than needing a second version bump at the moment
  that starts to matter.

  This is step 1 of `docs/multi-store-tiering.md`, which durable_streams asked
  for and which was approved for build against that doc.

### Changed

- **BREAKING: a version 2 tier manifest is refused, not converted.** Same call
  version 1 got when `BlocksKey` landed: nothing is deployed against version 2,
  so a store written by an older build is re-offloaded. There is no converter.

- **BREAKING: an entry naming no tier is refused, and the whole manifest with
  it.** `""` is what an absent JSON field decodes to, so letting it mean "the
  only tier" would make a manifest written by something that never set the
  field indistinguishable from one that meant the default — the sentinel
  collision `CleanSpec.Ceiling` was an `int64` bug for. Refusing the file
  rather than the entry follows the key check beside it: a manifest with one
  bad entry is not a manifest with one segment missing.

### Fixed

- **The orphan sweep reported every live block table as garbage.**
  `UnreferencedObjects` built its live set from the manifest's `LogKey` and
  `IndexKey` and from each segment's `storeKey` and `indexKey`. A
  block-compressed offloaded segment has a THIRD object — its block table,
  under `BlocksKey` — and nothing named it, so the sweep listed it, and the
  documented use of this call is to hand its result straight to
  `DeleteStoreObjects`.

  Deleting one corrupts nothing; it removes the only copy of the map from
  logical offsets to compressed blocks. The local table goes with the local
  file at offload — deliberately, since it describes bytes that no longer
  exist — so the segment cannot rebuild it and every read of it fails at "size
  block table". The bytes stay intact and become unreachable.

  Both halves of the live set had the hole, so neither could cover for the
  other. Both are fixed and guarded separately. It went unseen because the
  tiered test fixture writes raw segments, so no test had ever put a block
  table in a store and asked the sweep about it.

- **`hack/guardcheck.sh`: two guards ran after the summary that reports them.**
  The store read/publish retry guards were appended below the block that prints
  the totals and exits, so on Windows they ran with nothing left to report to —
  `failures` had already been compared and `checked` already printed. The job's
  "all N guards covered" line was printed before two of its guards had run, and
  N excluded them. Both were green in fact; nothing would have said so had they
  not been.

## v0.61.2 — 2026-08-07

### Fixed

- **`hack/guardcheck.sh`: two guard anchors stopped identifying one place.**
  v0.61.1's `openWithRetry` and `renameWithRetry` copy `ReadFileWithRetry`'s
  loop shape, so the missing-file early return and the `readRetryBudget`
  deadline each appeared in more than one function. `apply_edit` refuses an
  ambiguous match rather than neutralizing whichever it finds first, so both
  guards reported SKIP and the ubuntu guard-coverage job failed. Both are now
  narrowed to `ReadFileWithRetry` with multi-line anchors.

  No library code changed between v0.61.1 and this release. Tagged so that a
  released version exists whose CI is green at its own commit.

## v0.61.1 — 2026-08-07

### Fixed

- **A store object could not be read while it was being republished, on
  Windows.** `FileSegmentStore.Put` commits by renaming a temp file over the
  object path, and the read side opened that path with a bare `os.Open`. On
  Windows an open during that rename fails outright — "The process cannot
  access the file because it is being used by another process" — rather than
  succeeding or reporting the file missing. `readTierManifest` runs inside
  `open()`, so losing the race did not degrade a read: it failed the whole log
  open. Not a crash-recovery window and not a corrupted store, an ordinary
  manifest publish on a healthy machine. `ReadFileWithRetry` already existed for
  the killed-process form of this and is the wrong shape here — it buffers whole
  files, and these objects are segments — so `ReadAt` and `Stream` now go
  through an `openWithRetry` twin that retries the open instead.

- **The same window failed the publisher.** A reader holding the destination
  open makes the rename itself fail with "Access is denied", so retrying only
  the readers moved the error to the publisher rather than removing it. The
  commit point now retries. Only the commit needs it: the temp file is already
  complete by the time the rename runs.

- **`TestALogCleansAtOpenWithoutWaitingForATick` could fail for the opposite of
  the reason it reported.** Its size probe stat'd files the cleaner was
  unlinking, using `require` inside a `require.Eventually` condition — so a
  vanished file called `FailNow` on Eventually's goroutine, the condition could
  never return true, and thirty seconds later the test blamed the cleaner for
  not cleaning. Both messages in the CI log were one cause. Test-only.

### Notes

- No `statWithRetry` was added alongside the two above, though symmetry argued
  for one: `os.Stat` goes through `GetFileAttributesEx`, which does not open a
  handle and is not refused by one. Neither a racing publisher nor a deliberate
  deny-all handle could make `Size` fail, and guardcheck reported the retry as
  uncovered because nothing could falsify it. It was withdrawn rather than kept
  for tidiness.

## v0.61.0 — 2026-08-07

### Changed

- **`CleanWithSpec` now refuses a `CleanSpec.Ceiling` on a log whose automatic
  cleaner is still running.** The two settings do not merely disagree — the
  second silently undoes the first. A ceiling protects the records at or above
  it because they may be undecided; the automatic pass is spec-less, bounds
  itself at the high watermark, and compacts exactly those records on its own
  timer. The ceiling was honoured on every pass the caller drove and ignored on
  every pass it did not, which is indistinguishable from working until an
  undecided record goes missing. Callers supplying a `Ceiling` must set
  `Options.DisableAutoClean`; the error says so by name.

  This was previously only documented, on `DisableAutoClean` — the field you
  read if you already know to look, rather than `Ceiling`, the field such a
  caller is actually setting. `Ceiling` now cross-references it too.

  The hazard is not hypothetical, and this repo was one of the callers. Two
  such fixtures surfaced when v0.60.1 made the log clean at open; turning the
  rule into a refusal immediately found four more — `TestCleanVerifiedFloor`,
  `TestIncrementalCleanBudget`, `TestCleanScansLeaveSegmentCachesCold`, and the
  original open of `TestCleanSpecStripSurvivesReopen`, whose reopen had been
  fixed the day before and whose open had not. All six drove a ceiling over a
  live spec-less pass and asserted on exactly which records survived; all six
  were green, because the automatic pass had never once fired before their
  assertions. Nothing was red about any of it. Every caller outside the repo
  had the same latent misconfiguration available.

  A `Ceiling` of `At(0)` is refused along with the rest. It is the narrowest
  bound there is and precisely what a caller whose oldest open transaction
  begins at offset 0 must pass, so the check tests whether a bound was
  *supplied*, not whether its offset is nonzero — guarding on the value would
  wave through the one caller that needs the ceiling most.

- **`DisableAutoClean`'s doc no longer claims segment splitting is unaffected
  without qualification.** Splitting does still happen on every cleaner tick,
  but the pass at open added in v0.60.1 deliberately cleans without rolling, so
  that opening a log cannot move the active segment under a log that asked for
  no automatic cleaning.

## v0.60.1 — 2026-08-07

### Fixed

- **A log whose process restarts more often than `CleanerInterval` never cleaned
  at all.** `time.NewTicker` does not fire until `t+interval`, and nothing on
  disk records when the last pass ran — so `cleanerLoop` waited a full interval
  before its first pass and the clock started over on every process start. A
  process that lives less than the interval never reached a tick. Not rarely, not
  late: never, for the life of the deployment, however much there was to reclaim.

  Reported by sqlcdc from a soak — 149 restarts averaging 95.9s against a 5m
  interval, zero passes in four hours, and the one pass that did once fire
  reclaimed 69%.

  This is the rolling-tick bug of v0.51.0 one level out: there the pass worked
  and the loop skipped it, here the pass works and the loop never reaches it.
  Both survived for the reason `cleanerTick`'s comment already gave — every
  compaction test called `Clean()` directly, which is the one path production
  does not take. The new test drives neither `Clean` nor a tick: it reopens a log
  with an hour-long interval and requires the bytes on disk to drop.

  Fixed by cleaning once at open rather than by persisting a last-clean
  timestamp. A timestamp is a new durable file, a new parse and a new way to be
  wrong about time; the price of the simpler answer is that a restart storm runs
  a pass per start, which `CleanRewriteBudget` already bounds — it exists so a
  pass fits inside a short-lived process's kill window. Startup latency is
  unchanged: the pass runs on the background goroutine `New` has already returned
  from. Registered as guard 59.

## v0.60.0 — 2026-08-07

### Added

- **`ClassifySegment` — read a segment's framing from its header, not its body.**
  Returns a `SegmentFormat` (`Blocked`, `Version`, and a `Readable` method) after
  reading two bytes, for a process deciding at startup whether it understands a
  data directory.

  `InspectSegment` already knew the answer, but only as a side effect of
  `os.ReadFile` on the whole segment — so a boot probe had the choice between
  reading gigabytes it did not want and hard-coding `0xC1` into its own code.
  Both consumers chose the copy, which is the failure the note at the top of
  `inspect.go` was written about. The note diagnosed it and the package still
  offered no cheap correct answer; this is that answer. Both paths now decide
  "blocked" through one internal helper, so they cannot drift.

  An unrecognised version is **reported, not refused** — a caller probing a
  foreign directory needs to learn which format it is looking at, and `Readable`
  is what judges it. A file that starts with the magic but is too short to hold a
  version byte *is* refused, deliberately rather than reported as version 0: that
  zero is indistinguishable from a real version byte, and would answer a question
  the bytes never settled. Registered as guard 58.

### Fixed

- **`Blocks` called a truncated segment healthy while `Records` refused it.** A
  block header claiming more payload than the file holds was added to the walk's
  cursor unchecked; the cursor stepped past the end, the loop condition ended the
  walk, and the overrunning block came back listed as fine with a `nil` error.
  `Records` had the bound check all along and errored on the same bytes.

  So the package shipped two readers of one format that gave opposite verdicts —
  the exact failure `inspect.go`'s own header comment describes between repos,
  reproduced between two functions in it. Truncation is the likeliest damage an
  inspector gets pointed at (a short write, a partial upload, a download cut off
  mid-object), and "this file is sound" is the one answer that sends the
  investigation somewhere else. Same bound and same wording as `recordsBlocked`
  now, and the test asserts the two *agree* rather than asserting either one in
  isolation. Registered as guard 57.

## v0.59.0 — 2026-08-06

### Added

- **`Options.LocalRetentionAge` — the log schedules its own offload.** A clean now
  offloads every sealed segment lying entirely before `now - LocalRetentionAge`.
  Zero never offloads, which is what every log that has not opted in is carrying.

  This is a *schedule*, not a retention limit: nothing is deleted, the segment
  keeps serving, and `MaxTier*` still decide when the records finally go. It
  moves here because every input to the decision already was here — the horizon
  is this duration and a clock, the offset lookup is
  `EarliestOffsetAfterTimestamp`, and whether a process may write to the store
  at all is `tierWritable`, which `OffloadBefore` consults for itself. A caller
  scheduling this from outside had to keep a second copy of that ownership rule,
  and the copy that does not sit beside `SetTierReadOnly` is the one that
  drifts. `OffloadBefore` and `EarliestOffsetAfterTimestamp` both stay public.

  The offload runs *after* the pass and outside its lock. That is not stylistic:
  the pass holds `cleanMu` for its whole body and `OffloadBefore` takes `cleanMu`
  itself, so scheduling it inside deadlocks the log rather than answering wrong —
  and the pass is what decides which segments still exist, so offloading first
  would copy records to the store that the pass was about to drop. Registered as
  guard 56.

### Fixed

- **Four options accepted a negative value and failed somewhere else entirely.**
  Each is defaulted by a test for zero, because zero is the unset value — and a
  test for zero reads as "the caller supplied a number" for every value that is
  not exactly the zero value. So a negative passed the arm that exists to catch
  a missing one:

  | option | what it did |
  | --- | --- |
  | `CompactMaxGoroutines` | `panic: makechan: size out of range` |
  | `HWCheckpointInterval` | `panic: non-positive interval for NewTicker` |
  | `CleanerInterval` | `panic: non-positive interval for NewTicker` |
  | `MaxSegmentBytes` | no panic — every append rolls, and the probe never returned |

  Not one of them fails at the call that set the option. Two are panics on
  background tickers, where there is no caller left to hand an error to, and one
  is a hang. `New` now refuses them, rather than clamping, for the reason the
  compression-codec check beside it refuses: a clamp keeps the caller's mistake
  and hides it. Registered as guard 55.

  `CleanRewriteBudget` is defaulted the same way and is deliberately *not*
  refused — a negative budget there means "no budget at all", which is what
  every spec-less pass had before budgets existed.

  `LocalRetentionAge` is refused too, for the plainer reason: zero already means
  "never offload", so a negative is not an unset value reaching a default — it
  is a horizon in the *future*, which makes every sealed segment older than it
  and offloads the whole log on the first pass.

### Changed

- **`CompactMaxGoroutines` no longer means what its name says, and now says so.**
  Key-digest builds are capped at 2 regardless of this option, because each
  build holds a transient per-segment key map and ten at once measured over 1GB
  on a soak. The only value of the field that changes anything is 1, which makes
  the builds serial; anything above 2 is the same as 2. Documented on the field
  rather than left in `loadOrBuildDigests` for the next reader to find.

## v0.58.1 — 2026-08-06

### Changed

- **Opening a log no longer stat-s the disk once per index file.** The sweep that
  removes orphaned indexes asked `os.Stat` whether each `.index` had a `.log`
  beside it, having read the whole directory two lines earlier. It now looks the
  stem up in a set built from that listing — 336 syscalls saved on the
  336-segment logs durable_streams reports, and one consistent view of the
  directory rather than a listing plus a later stat that can disagree with it.

  Two tests, because this path had none. The one that matters is the offloaded
  case: an index with no log beside it is the *normal* resting state of a tiered
  segment, and the only thing standing between it and deletion on every boot is
  the manifest lookup. Deleted, adoption rebuilds each index by streaming its
  segment back from the store — measured at 31059 bytes against 2184 on the same
  fixture. Registered as guard 54.

- **CI runs staticcheck.** `go vet` is deliberately narrow, and nothing else
  looked at the code, so a first run turned up four findings that had
  accumulated unnoticed — one of them worth having: `blockHeaderLen` sat in a
  const group under `blockMagic byte` with its own value, which does not inherit
  that type but reads exactly as though it does. All four are cleared and the
  step is pinned, with `U1000,S1*,SA*` rather than the default set: `ST*` is a
  naming and comment-style opinion this repo does not share, and enabling it
  would buy a large reformat or an exclusion list to maintain.

## v0.58.0 — 2026-08-06

### Changed

- **Opening a block-compressed log no longer walks every block header in it.** A
  segment's block table was rebuilt on every open by following the chain — each
  block's header carries the length that locates the next, so it was one read per
  block, across every segment, before a single record was served. The cost scales
  with the block *count*, and the append path writes one block per message set,
  so it is set by how small the commits were rather than by how much data there
  is; `cleanBlockTarget`'s comment records 18.6M ~140-byte blocks in one real run.
  A sealed segment now persists its table beside its files, in exactly the bytes
  the tier already writes as a store object, and the open reads it instead. With
  the walk isolated from per-segment work, an open of 40000 blocks goes from
  209ms to 87ms; at 18.6M blocks the walk alone is the better part of a minute.

  Nothing to migrate and nothing to configure. Opening a log seals its non-active
  segments, and sealing is where the table is written, so a log written by an
  earlier build gains its tables the first time this one opens it. A table that
  is absent, unreadable, or that does not account for exactly the bytes in the
  file beside it is recomputed by the walk — the table is derived data, and the
  bytes it describes are on the same disk, so an unusable one costs a slow open
  and never the log. (That is the deliberate opposite of the rule for the store
  object, where walking means downloading it again.)

### Fixed

- **`hack/guardcheck.sh` reported a filter that matched no guard as a pass.** The
  optional argument is a plain substring match, so a caller who reached for a
  regex selected nothing — and an empty selection printed the header, no summary,
  and exit 0, which is what a run that checked everything asked of it also looks
  like. It now names the filter and exits 1. Guards deferred to another platform
  count as selected: a filter naming only a windows guard did pick something out
  on linux, and the deferral line already says it was not checked here.

## v0.57.5 — 2026-08-06

Both from the same audit as v0.57.2 through v0.57.4: a step that tears something
down before rebuilding it, with an early error return in between that leaves the
torn-down state published.

### Fixed

- **An offload whose local cleanup failed stranded the segment halfway.** The
  attach drops the now-redundant local files and swaps the backing over to the
  store object — and it returned early from each of those steps, leaving the
  segment published with a closed local backing and no store, against a manifest
  entry naming its objects that had *already* been written. Every read of it then
  failed until the process restarted, and `OffloadBefore`, which aborts its pass
  on that error, reported one fewer offload than the manifest recorded. Nothing
  after the store backing is open can make staying local correct — the commit
  happened in the store — so the swap now happens regardless and a cleanup
  failure is reported alongside it.
- **A segment whose close failed during retention was left neither closed nor
  gone.** `Delete` marks the segment gone so readers skip the offsets retention
  lawfully collected, but returned from a failed close before setting the flag —
  and retention keeps a failed segment in the surviving list, so it stayed
  published and answered a raw `segment has been closed` for those offsets. The
  records are being collected either way; a close that failed does not make them
  readable again.

## v0.57.4 — 2026-08-06

### Fixed

- **Installing a compaction rewrite now waits out the Windows handle-release
  window.** Past both renames `Replace` has nothing left to undo, and the step
  that can fail there is opening a log this process closed microseconds earlier
  — the same asynchronous handle reclamation `ReadFileWithRetry` exists for. The
  redirect link from the source to the replacement also goes up only once the
  replacement is fully open; it used to be set before the positions and index
  were built, so a failure there handed readers a half-open segment. If the open
  still fails after the budget the segment stays closed and reads of it report
  `segment has been closed` until a restart — deliberately, since the
  alternative (marking it gone so readers skip it) would silently lose records
  that are intact on disk.

## v0.57.3 — 2026-08-06

### Fixed

- **A compaction whose install failed left the segment it was replacing closed
  and published**, so every read of it reported `segment has been closed` until
  the process restarted. `Replace` closes both segments, renames the rewrite
  over its source and links the two — and it wrote that link only on success,
  while the pass that calls it publishes nothing on the way out. The source
  therefore stayed in the live segment list, closed, with nothing to redirect a
  reader to. Each failure now puts back what it got through: up to and including
  the log rename that restores the exact prior state. Past that point the files
  are installed under the source's names and there is nothing left to undo, so
  the failure is reported rather than papered over.

## v0.57.2 — 2026-08-06

Both fixes below came out of an audit prompted by the v0.57.1 report: looking
for the same shape — a step that tears something down before rebuilding it,
with a failure in between — rather than waiting for the next symptom.

### Fixed

- **A refused mmap during index expansion panicked the next append** — all
  platforms, not just Windows. `writeAt` grows the file, unmaps, and maps again,
  recording the new size at the truncate; both the unmap and the map can fail
  after it. `size` then described a file while the write copies into a mapping —
  a shorter one if the unmap failed, none at all if the map did — and since the
  only test for expansion was `offset+pSize >= idx.size`, the next write
  concluded the room was already there, skipped the expansion, and sliced past
  the end of the mapping. A panic raised inside a library takes the caller's
  process down with it. An expansion now requires room in both the file and the
  mapping: neither is a proxy for the other, since a failed expansion leaves the
  mapping shorter than the recorded size, while the unix shrink truncates
  without unmapping and so leaves it longer. Found while auditing for the same
  shape as the v0.57.1 shrink wedge; not reported in the wild.
- **A failed shrink while closing an index leaked the file handle**, leaving the
  index marked open and the segment permanently undeletable on Windows —
  `closeIndex` states that rule in its own comment and applied it only to the
  flush, while the steps after it still returned early. Every step now runs the
  teardown to the end and reports at the bottom.

## v0.57.1 — 2026-08-06

### Fixed

- **A failed shrink left the index unmapped, and every later read of that
  segment called it corrupt** — for the life of the process, because nothing
  re-opens a segment's index once the log is running. Windows only. `shrink`
  must unmap before it can truncate there, since an open `MapViewOfFile` makes
  `SetEndOfFile` fail, so the truncate runs inside a window where the mapping is
  already gone; failing there returned straight out and left no mapping behind a
  non-zero position. `seal` discards this error by design, on the premise that a
  failed shrink costs a rebuilt index tail rather than data — true only while the
  mapping survived the failure. Both failure paths now restore the mapping before
  returning. Reported by sqlcdc from production: `position=275700`,
  `closed=false`, 28 views wedged.

## v0.57.0 — 2026-08-05

### Fixed

- **A cleaner tick that rolled a segment skipped its clean**, so a compacted log
  under continuous write never compacted. The loop returned early on a roll, on
  the stated premise that the cleaner "already ran" — `checkAndPerformSplit`
  rolls and seals, and `Clean` has exactly one caller. Load-dependent in the
  worst way: a quiet log rarely has a segment ready to roll and cleaned every
  tick, and a log with `MaxSegmentAge` at or below `CleanerInterval` has one
  pending at *every* tick and never cleaned at all. Reported by durable_streams
  from a 5.5h soak — 4.5GB, 336 segments, 239 live keys, ~66 ticks, zero
  rewrites, every key digest stamped in the final minute.

### Added

- **`Options.CleanRewriteBudget`** bounds how long one automatic pass may spend
  rewriting. `CleanSpec.RewriteBudget` already existed, but the spec-less
  `Clean()` the cleaner goroutine uses built an empty spec, so the one pass
  nobody drives by hand was the one pass that could run unbounded — 6m42s
  against a 5m interval on the log above. Defaults to `CleanerInterval`;
  negative means no budget. Segments a pass does not reach are carried through
  untouched and what it did do is installed, so a log too large to compact in
  one pass converges over several.

## v0.56.0 — 2026-08-05

### Changed

- **Breaking**: `CleanSpec.Ceiling` and `CleanSpec.RetentionFloor` are `Bound`,
  not `*int64`. The zero `Bound` is "no bound supplied"; `At(0)` is a bound at
  offset 0. The pointer solved the sentinel bug v0.55.0 describes, but a caller
  should not have to take an address to name an offset, handle a nil at every
  use, or be trusted not to mutate what it handed over. A two-word comparable
  value has none of that. Migration is `Ceiling: &x` → `Ceiling: At(x)`, and an
  omitted field still means unset.

### Fixed

- **A timestamp lookup panicked on a tiered log.** `EarliestOffsetAfterTimestamp`
  and `LatestOffsetBeforeTimestamp` read each segment's base timestamp out of its
  index. An offloaded segment whose index lives in the store has a nil `Index` —
  every other consumer goes through `withIndex`, this one reached past it — so
  either call nil-dereferenced on any log opened with a `RemoteIndexCache`. The
  segment already holds that timestamp (`firstWriteTime`), so the lookup now asks
  it: no index, no I/O, and nothing left that can fail.

### Changed

- **Opening an offloaded tier no longer downloads every segment it names.** Each
  entry re-derived `blockMode`, `position` and `physPosition` from its object —
  a stat, a 1MiB prefetch to read one magic byte, and for a block-compressed
  segment a walk of the entire header chain — all of it already recorded in the
  manifest. Measured on the boot path before serving a single read: 22,136,648
  bytes for a 22-segment snappy tier, 5,242,880 for a 5-segment raw one. Now
  zero, on both the reopen and adopt paths.
- **A segment's block table is written to the store at offload**, as its own
  object beside the log and index, and named by `TierObject.BlocksKey`. It is
  the one thing a manifest entry did not carry, and the alternative to storing
  it was rebuilding it by walking the object's header chain — a read of the whole
  object. Deferring that walk to the first read would only have moved the same
  download behind the first record anyone asked for; persisting removes it. A
  cold segment now fetches a few KB. It is not inlined in the manifest because
  the manifest is read whole on every open, and block tables are bounded by the
  tier's total block count rather than its segment count.
- **Breaking**: the tier manifest is version 2, and version 1 is refused rather
  than adapted. A version 1 entry names no block table, so its block-compressed
  segments could only be served by the walk this replaces. Nothing is deployed
  against version 1; a store written by an older build is re-offloaded, not
  converted. `CopyTier` copies block tables, and a manifest entry whose
  `BlockMode` and `BlocksKey` disagree is refused where it arrives.

## v0.55.0 — 2026-08-05

- **Fixed**: the Windows sharing-violation retries are bounded by time, not by
  attempt count. `ReadFileWithRetry` now waits up to 5s; `AtomicWriteFileWithRetry`
  keeps its ~500ms.

  The bound was `i >= atomicWriteRetries` — 25 attempts, 20ms apart. That bounds
  how many times you ask, and what the retry waits for is the OS reclaiming a
  killed process's handles, which takes an amount of TIME that depends on what
  the machine is doing. So the real bound was 500ms: a number that appeared
  nowhere, and that changed meaning silently whenever the delay constant did.

  sqlcdc measured it. On a 3h50m kill -9 soak against a 36 GB data dir with 30
  live views, it still killed 2 of 86 daemon restarts outright — a node that does
  not come back from a crash — after the retry had already cut the same failure
  from 1 in 30. Both deaths were `open()`'s own high-watermark read.

  The two budgets differ deliberately, the same split as
  `RewriteBudget`/`TierRewriteBudget`. The read happens once, on the boot path,
  with no tick to stall, and giving up too early costs a node; the write runs on
  a checkpoint tick, a genuinely conflicted one never clears, and stalling every
  tick for seconds to discover that is worse than letting the next tick try. One
  number for both operations meant the cheaper one set the price.

  `TestRecoveryReadsRetryThroughTransientHandle` now holds its handle for 1.2s —
  past the old ceiling — so it exercises the budget rather than fitting inside
  it, and a guard pins the bound as a duration.

- **Breaking**: `CleanSpec.Ceiling` is a `*int64`. Nil means no bound was
  supplied and the pass uses the high watermark, as before; every other value is
  taken literally.

  It was an `int64` resolved with `if spec.Ceiling <= 0 { spec.Ceiling =
  l.HighWatermark() }`. But zero is a REAL ceiling — "compact nothing" — and it
  is exactly what a transactional caller whose oldest open transaction begins at
  offset 0 must pass. Every record in that log is undecided, and the answer the
  caller got was that every record was compactable: the one spec asking for the
  narrowest bound received the widest one there is. `TestCleanSpecCeilingAbove
  UndecidedLosesKey` is what that costs.

  `RetentionFloor` is a `*int64` for this reason and documents the argument in
  its own doc comment, twenty lines below in the same struct. A bound whose zero
  value is a real offset cannot spare its zero to mean unset. Reported by
  durable_streams, who hit it.

  A negative `Ceiling` stays legitimate and is not validated. `HighWatermark`
  returns -1 for "nothing committed yet" and callers pass it straight through, so
  -1 arrives whenever a log is cleaned before its first commit — meaning, as any
  ceiling does, retain everything at or above it. Even the sign of this field is
  a fact the caller has and this layer does not, which is what
  `docs/layering.md` already said about it. A guard holds the zero case.

## v0.54.0 — 2026-08-05

- **Added**: `CopyTier(src, dst SegmentStore)` copies a log's whole tier between
  stores — every object the manifest names, then the descriptor, then the
  manifest.

  Moving a tier between stores is a real operation (durable_streams does it when
  a stream is promoted), and doing it by hand means knowing that the manifest is
  the commit point and must be written last. The keys are not exported, so the
  only way to get that ordering by hand was to rely on `List()` returning
  `"manifest"` after the digit-prefixed segment keys — true of
  `FileSegmentStore`'s lexicographic listing, not true of a store that lists by
  insertion or upload time. The ordering rule belongs in this package, so the
  operation does too; a guard holds it in place.

  It refuses three things rather than papering over them: a destination that
  already holds a manifest or a descriptor (copying one tier over another
  strands every object already there), a source with no descriptor (a store that
  never belonged to a log), and a manifest entry whose object the source does not
  hold. An interrupted copy needs no cleanup — the destination has no manifest,
  so it reads as an empty tier and everything in it is collectable.

- **Fixed**: an unknown `Compression` codec is refused by `New` instead of being
  written into every block header and refused on the way back.

  `Codec.Compress` has no error to return, so its default arm stored the batch
  raw under whatever byte it was handed. `Valid()` was called in exactly one
  place — `parseBlockHeader` — which runs on READ. So the write path accepted
  precisely what the read path rejects.

  It did not stop at unreadable blocks. The descriptor records the codec by NAME,
  an unknown one renders as `codec(9)`, and `compress.Parse` refuses that: a log
  configured with a bad codec took appends, closed cleanly, and could never be
  opened again. The records were lost to a value that was wrong before the first
  one was written. A codec is a value the caller hands in, so it is now checked
  where it arrives, with a guard.

- **Fixed**: `Codec.DecompressInto` no longer returns a slice aliasing its input.

  `None` returned `src` itself, so the "decompressed" bytes were the caller's
  compressed-payload buffer — which on the block path is a recycled read buffer
  that the next block refills. Nothing was wrong with those bytes when they were
  returned; they stopped being those bytes later. `decodeBlock` knew, and carried
  an unexplained pointer-identity check against exactly it.

  That check is the tell. A contract that holds for three codecs of four is not a
  contract, it is something every caller has to rediscover, and the one that
  forgets gets a bug with no bad value anywhere in it. The copy moves into the
  codec, where a caller cannot forget it — and it is the same copy `decodeBlock`
  was already making the moment it noticed, so the block path does no more work
  than before.

- **Internal**: `writeTierManifest` and `writeTierManifestWith` were two names
  for one function, separated only by passing no arguments to a variadic.

## v0.53.0 — 2026-08-05

- **Breaking**: the tier manifest is the only record that a segment is
  offloaded. The per-segment local `.offloaded` marker is no longer written or
  read, and publishing the manifest is the commit point for an offload.

  An offloaded segment was described twice — a marker beside the log and an
  entry in the manifest — and adoption projected one into the other, writing
  markers back out for every entry it imported. Two records of one fact, kept in
  step by hand, when only one of them is reachable by a process that has the
  store and not the directory.

  The commit point moves with it, which is the part that had to be decided
  rather than derived. An offload now uploads, publishes a manifest naming the
  new objects, and only then drops the local bytes — per segment, not per pass,
  because a batch would leave every segment in the pass able to lose its local
  copy against an entry that is not yet published. Rewriting an already-offloaded
  segment follows the same three steps, which is why `ReplaceOffloaded` is now
  two calls with the publish between them: `tierState` reads every segment under
  its read lock, so committing with the segment's write lock held would deadlock.

  `open()` reads the manifest before the directory now, because the manifest is
  what tells an offloaded segment's local index apart from an orphaned one.

  The marker machinery is gone with it: the suffix, the path, the reader, its
  key check, and the removal in `segment.Delete`. Two guards named that code and
  come out with it — a key that leaves the store and a marker that is not JSON
  were both refused there, and the manifest reader refuses the same two things
  at the one remaining way in. A log reopened without the store it was offloaded
  to still refuses to open, and now does so on the descriptor rather than on a
  marker, which is the right way round: a store-backed log is unopenable without
  its store whether or not anything has been offloaded yet.

- **Breaking**: a `SegmentStore` must report an absent key as `ErrObjectNotFound`
  from `Size`, `ReadAt` and `Stream`. Absence is no longer inferred from a read
  failing.

  It was, for want of an `exists()` on the interface — the comment on
  `readStoreDescriptor` said so. Both callers act on absence, and both act on it
  in a way that is only correct when the absence is real. No descriptor means the
  log is NEW, and a new log records the settings it was handed without comparing
  them to anything: v0.52.0's bug, arriving through a timeout instead of an empty
  directory. No manifest means the tier is EMPTY, so the log adopts nothing and
  the next publish rebuilds the manifest without those entries — after which
  every object they named is unreferenced, and the collect-then-`DeleteStoreObjects`
  cycle this API documents deletes live data.

  The protection for the second one was already written and already correct.
  `UnreferencedObjects` refuses to build a garbage list it cannot check against
  the manifest, and explains why beside the code. It was unreachable: the reader
  had turned the failure into an ordinary empty manifest before the caller could
  see it. A guard now holds each half open.

- **Internal**: documentation only. Round 2 of the complexity sweep found the
  same thing round 1 did, in prose rather than in code: comments that describe a
  format, an API or a tolerance this package no longer has.

  `NewReader` carried a migration table from two functions that no longer exist.
  Three comments called a raw segment "legacy" — raw is what a `None` codec
  writes today, so the word named the wrong thing. Two more explained, at
  length, what the code used to do before the previous release changed it; that
  belongs here, not beside the code. `TestCommitLogCompressionBackwardCompat` is
  renamed `TestTurningCompressionOnLeavesExistingSegmentsRaw`, because turning
  compression on for an existing log is a retune someone will actually do, not
  compatibility with a past nobody has.

## v0.52.0 — 2026-08-05

- **Breaking**: a log with a `SegmentStore` keeps its descriptor IN the store,
  beside the tier manifest, not in its local directory. An existing local
  `log-descriptor` beside a store-backed log is ignored; there is no migration.

  A store-backed log's data outlives any particular directory — that is what a
  tier is for — so a process that has the store and not the directory has the
  log, and must be able to ask what the log IS from the same place it asks what
  the log HOLDS. A log with no store is unchanged: there is no catalog to be
  authoritative, so the directory is the catalog.

  This fixes a hole in the descriptor check. Whether a log was new was decided by
  reading the local directory for `.log`/`.offloaded` files. A node adopting a
  tier has a store full of segments and an *empty* directory, so it called itself
  new — and a new log skips the descriptor check entirely and records whatever it
  was passed. The one moment a process picks up a log it did not create was the
  one moment its retention settings were never compared. `Compact` and
  `CompactTombstoneRetention` decide what gets deleted, and their zero values
  mean no protection rather than "disabled", so adopting a compacted tier with an
  empty config began applying a retention policy the log was never created with,
  to data this process did not write.

  Reported by durable_streams, who also named the fix: the check belongs against
  the catalog, always.

- **Breaking**: an unknown key in a descriptor is an error.

  It used to be ignored so a newer writer stayed readable by an older reader.
  Pre-v1 there is no older reader, and the tolerance covered more than it was
  aimed at — a typo, a renamed key and a line mangled by a partial write all look
  like a field from the future, and all left a descriptor that read as valid
  while describing a log nobody configured. The version line is what makes a real
  format change detectable, and it stays.

- **Changed**: `UnreferencedObjects` never reports the descriptor.

  It judges by what references an object, and nothing references the descriptor —
  it is what the store says *about* itself rather than what it holds. So the rule
  as written collected it, and the collect-then-`DeleteStoreObjects` cycle the API
  documents would have left a log that refuses its own next open, since a log that
  exists with no descriptor is a refusal by design. The manifest was already
  exempt for the same reason; the descriptor is the first object of that kind
  added since the rule was written, and now there is a guard on it.

- **Changed**: `ErrDescriptorMismatch` no longer describes a missing descriptor
  as a log that "predates the descriptor". `AdoptOptions` still resolves it, and
  now means one thing rather than two: *I know what this log is, record it.* Its
  message names the tier rather than the local directory for a store-backed log,
  since that directory is often a scratch path this process picked and the
  disagreement is with the tier everyone shares.

The first round of a standing sweep for complexity with no beneficiary
(`.failover/maintenance/needless-complexity.md`). Pre-v1, nothing is deployed,
so a compatibility path is not a cost we are carrying for someone — it is a code
path nobody executes. Each one removed here turned out to be doing something
worse than nothing: it was the branch that accepted corrupt input.

- **Breaking**: an `.offloaded` marker must be JSON. The older layout — the bare
  store key, no JSON around it — is gone.

  The two were told apart by whether the first byte was `{`. Every file that is
  not JSON satisfies that, so the fallback was reached by truncated markers,
  half-flushed markers, markers full of NULs, and files that were not markers at
  all. Each became a segment whose store key was the garbage, and a store key is
  an *action*: it reaches `store.Delete`. One layout, and a marker that is not
  it is now an error.

- **Breaking**: a tier manifest must carry `version` equal to the version this
  build writes.

  The check was `>` — refuse anything newer. Only one version has ever existed,
  so that reads as complete, but an absent `version` field decodes to `0`, and
  `0` is not newer than `1`. Any JSON object that parsed was accepted as a
  manifest, `{}` included — and `{}` is a manifest with no segments, which is
  indistinguishable from an empty tier and would unpublish every offloaded
  segment the store holds.

- **Changed**: `InspectSegment(...).Blocks()` no longer claims a block header it
  cannot read came from a pre-v0.15.0 segment.

  The claim was inferred from the symptom rather than from the file: that layout
  has no version byte, so its codec byte reads as a version — but so does a
  segment written by a *newer* build, which was told the same wrong story. That
  is the shape of error the sentence was added to prevent. It now names both
  versions, the one found and the one this build writes, which is true either
  way.

- **Internal**: two guards added (`a marker must be JSON`, `a manifest must be
  this version`), and a duplicated doc comment removed from three functions that
  had two stacked descriptions each contradicting the other in part.

## v0.51.1 — 2026-08-04

- **Fixed**: four ways `EarliestOffsetAfterTimestamp` and
  `LatestOffsetBeforeTimestamp` answered with the wrong record.

  All four need a clock *coarser* than the rate records are appended at, which is
  every real clock: a run of records then shares one instant. Every test this
  package had gave each record a timestamp of its own, and with distinct
  timestamps none of the four can happen — which is why they survived.

  These two functions are how a consumer turns a wall-clock resume point into an
  offset, so a wrong answer is not a wrong number; it is records skipped or
  re-read, silently, by something that has already moved on.

  - **A resume point skipped its own instant's records in the previous segment.**
    The segment search is by *base* timestamp and strictly greater, so when a
    roll lands inside a run sharing an instant, the new segment's base is exactly
    the timestamp the previous segment's tail still carries. Asking for it
    answered with the later segment's first record. Measured on a log with runs
    of four: asking for the first run's instant answered offset 3 instead of 0 —
    three records handed to a reader as already consumed.

  - **A time in the gap before the last segment answered past the end of the
    log.** The fallback searched on only while `idx < len(segments)-1`, so the
    final segment was never reached. A consumer resuming from that answer reads
    nothing and waits, having been told the whole final segment is in its past.
    Reachable whenever a roll coincides with a pause — an ordinary stream, idle
    a moment and then written to again.

  - **One step forward was not always enough.** The segment after the answer's
    may itself hold nothing at or after the target (the active segment, in the
    window just after a roll, is empty), and landing on it reported its
    not-found as a real error. It is a bounded loop now, normally stopping on
    the first or second iteration.

  - **`LatestOffsetBeforeTimestamp` answered with the *first* record of a run
    where its contract asks for the last**, because `findEntryByTimestamp`
    answers with the first entry carrying a timestamp.

  The last one is now *defined* as "one below the earliest record strictly after
  T" rather than searched for separately. Carrying a second copy of the segment
  search is how it came to have its own set of bugs; there is one copy now.
  `math.MaxInt64` saturates to the newest offset rather than wrapping.

  Found and diagnosed by durable_streams, which hit them while chaos-testing
  `Stream.OffsetAtTime` against a moving retention floor.

- **Fixed**: an append read the clock *before* it took the append lock, so a
  later offset could carry an earlier timestamp.

  Offsets are handed out under `appendMu`; the clock read that stamps them was
  not. Two appenders could interleave as "A reads T2, B reads T1 and wins the
  lock, A writes T2 after it", and the log then held a record whose timestamp
  went backwards as its offset went forwards.

  That inverts the one precondition every timestamp lookup runs on.
  `findEntryByTimestamp` and `findSegmentIndexByTimestamp` both binary-search,
  and a binary search over a sequence that is not sorted does not degrade
  gracefully — it answers, with whatever the halving lands on. One inversion is
  enough, and the records are stamped wrong on disk for good.

  Reported by durable_streams while it was chaos-testing `Stream.OffsetAtTime`
  against a moving retention floor.

- **Internal**: three guards in `hack/guardcheck.sh` are no longer proved by
  timing, and the four new guards added here are not either.

  A lock discipline is the hard case: the outcome it protects — monotonic
  stamps, an unstarved reader — is only observable when two goroutines race for
  it, and a test that has to win a race asserts a *rate*. That is what these
  had. It goes quiet on a loaded runner, which is the moment it matters.

  Each is stated as the discipline instead, asked of the code from inside an
  operation it must perform: mutexes are not reentrant, so `TryLock` on the
  operation's own goroutine answers "is this lock held right now" with no
  timing in it at all.

  The tests anchoring the two "unlink outside the lock" guards and the "carry a
  segment rolled under a truncation" guard asserted a read *rate* while a
  truncation ran, so on a loaded runner the neutralised code still passed them
  and CI reported the guards as uncovered while nothing was wrong. A guard whose
  coverage depends on how fast the machine is goes quiet exactly when nobody is
  watching.

  They act from inside the truncation now instead of racing it. An offloaded
  segment reads and deletes through its `SegmentStore`, so a wrapping store is
  called synchronously on the truncation's own goroutine: the unlink guards ask
  there whether `l.mu` is free (Go's mutexes are not reentrant, so a lock held
  across the delete loop fails `TryRLock`), and the carry guard appends from
  inside the boundary rewrite. Nothing sleeps, races or measures a rate. All 30
  guards are covered.

## v0.51.0 — 2026-08-04

- **Added**: `Log.LocalBytes()` reports how many bytes of log data a log occupies
  on *local* disk.

  It exists for the caller that has to decide what MOVING a log would cost — a
  broker weighing whether to reassign a partition — and its two exclusions follow
  from that question rather than from convenience. Offloaded segments do not
  count: their bytes are in a `SegmentStore` that whoever takes the log over
  reads the same way this process does, so the move does not copy them. A tiered
  log with a terabyte in object storage and one live segment costs one live
  segment to move, and reporting the terabyte would refuse every move of exactly
  the logs that are cheapest to make. Indexes do not count either, being derived:
  a copy rebuilds its own, so their bytes are never transferred.

  Computed from the positions the segments already hold, so it is arithmetic
  under a lock rather than a walk of the filesystem — cheap enough to ask on a
  timer, which is the only way anything watching a whole broker can ask it.
  Segments mid-replacement are followed to their replacement and dropped if gone,
  the same rule `OldestOffset` uses, so a compaction or retention pass in flight
  cannot report space that has already been given back.

## v0.50.5 — 2026-08-04

- **Fixed**: recovery could fail to open a log at all when the previous process
  had just been killed, on Windows.

  `open()` recovered the high watermark with a bare `os.ReadFile`. On Windows a
  handle held by a process that has just been killed is not closed when
  `TerminateProcess` returns — the OS reclaims it asynchronously — so an open in
  that window fails with `ERROR_SHARING_VIOLATION` rather than succeeding or
  reporting the file missing. A log recovering right after a hard kill is exactly
  that window, so losing the race failed the whole `open()`.

  Reported from sqlcdc: a kill/restart soak lost the race about one restart in
  thirty, and the replacement daemon wrote one line and exited — *"read high
  watermark file failed: ... The process cannot access the file because it is
  being used by another process."*

  This package already carried the knowledge on the write side
  (`AtomicWriteFileWithRetry`, which exists for the same condition on
  `ReplaceFile`); the read side never got it. New `ReadFileWithRetry` is its twin
  — same bound, ~500ms — and is now used for every recovery-time read of a log's
  own metadata: the high watermark, offload markers, the leader epoch checkpoint,
  and sidecars (whose writes already retried).

  **A missing file returns immediately**, which is the point rather than an
  optimisation: absent is a legitimate state — a log with no checkpoint yet, an
  unwritten sidecar, a first open — and must stay instantly distinguishable from
  locked, which is a race worth waiting out.

  `ReadFileWithRetry` is exported alongside `AtomicWriteFileWithRetry` for callers
  that read the same kinds of small files next to a log.

## v0.50.4 — 2026-08-04

- **Fixed**: the store-key rule added in v0.50.3 covered the tier manifest but
  not the offload marker, which is the other route to the same value.

  `readOffloadMarker` produces the `offloadMeta` that becomes `s.storeKey` and
  `s.indexKey`, and `openOffloadedSegment` reads a marker whether it was written
  by `offloadTo` or synthesised from a manifest entry by
  `adoptTierManifestLocked`. Validating only the manifest left the rule true of
  one path in and not the other. The legacy marker format — the whole file taken
  as the key — was unchecked too.

  A marker sits in the log's own directory, so this is a weaker case than the
  manifest: anyone who can write there can already delete the log's segments. It
  is checked so the rule holds wherever a store key comes from, rather than in
  the one place it was first noticed. A bad key now refuses to open the log,
  which is the right trade — a log that will not open is recoverable, a delete
  that has already happened is not. Keys this package mints are unaffected in
  both marker formats.

## v0.50.3 — 2026-08-04

- **Fixed**: a tier manifest could name a store object outside the store, and
  deleting that segment deleted the named path.

  The keys in a manifest are the one part of it that becomes an *action* rather
  than a description. `readTierManifest` decodes `LogKey` and `IndexKey` out of
  an object in the store, `adoptTierManifestLocked` writes them into a local
  offload marker, and they end up as `s.storeKey` / `s.indexKey` — which
  `segment.Delete` hands straight to `store.Delete`. For `FileSegmentStore` that
  is an `os.Remove` of the store directory joined with the key, so a manifest
  naming `../../x` removed a file the store never held. `filepath.Join` *cleans*
  a traversal rather than refusing it, which is what made the escape silent.

  `FileSegmentStore` documented the assumption it relied on — *"Keys are
  log-relative segment identifiers (no separators), so the join stays within
  dir"* — and that is true of every key this package mints. It was never checked
  for keys arriving from outside.

  A key must now be a bare filename. The manifest reader refuses the **whole**
  manifest rather than the offending entry: a manifest holding a key this package
  could not have minted is not one whose other entries have been established as
  trustworthy. `FileSegmentStore` enforces the same rule where a key becomes a
  path, since `SegmentStore` is an interface and an object-storage implementation
  has no reason to care about separators. An empty `IndexKey` stays legal — it
  means the index was kept on local disk.

  Hardening rather than a live exploit: a writer that can put a manifest in the
  store can already put objects in it. But corrupting a log through its own
  objects is inherent to owning the bytes, and deleting a path outside the store
  directory is not.

## v0.50.2 — 2026-08-04

- **Fixed**: a leader epoch checkpoint holding a negative epoch was accepted as
  the highest epoch representable instead of being refused.

  An epoch is `uint64` everywhere in this package, so nothing here can write a
  negative one — the hazard was entirely on the way back in.
  `readLeaderEpochOffsets` parsed with `ParseInt` and then converted, so `-1` in
  the file became `2^64-1`: a well-formed epoch above any a leader will ever
  assign. The cache opened cleanly, `latestEpoch()` sat at the ceiling
  permanently, and nothing downstream can tell that value from a genuine one.
  The file carries no checksum, so the parse is the only place the corruption is
  still visible. It now parses unsigned and refuses the line.

  Found by applying a report from durable_streams to this repo: they had just
  shipped the same shape one layer up, where a partition index skipped its
  lower-bound check and `-1` became `4294967295`, keying an offset log no reader
  would ever consult.

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
