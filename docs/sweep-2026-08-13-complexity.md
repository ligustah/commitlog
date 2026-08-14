# Standing complexity sweep — 2026-08-13

The standing task: *"scan your respective repos for things that are obviously too
complicated. We continuously have situations where you describe how something
works and I can immediately think of a MUCH simpler solution. Same for migrations
or backwards compatibility, we don't need either until we are v1."*

Two axes. Recording the negatives as well as the positives, because a sweep that
only writes down what it changed gets re-run against the same clean code.

## Axis 1 — backwards compatibility and migrations

### Descriptor v0 read support — a real one, decided, sequenced

`parseDescriptor` accepts `descriptorFileV0` or `descriptorFileV1`. That is
pre-v1 back-compat and goes.

The decision it forced first, though, was the more interesting half.
`renderDescriptor` stamps V1 **unconditionally**, while omitting the identity
line when unset — so a log that never uses `Options.Identity` gets a V1
descriptor whose bytes a v0.79.x reader could parse perfectly. It announces a
capability the file does not use, and that alone is what makes downgrade
one-way. It is why both downstream repos were holding a workaround: *"keep
Compression and MaxSegmentBytes fixed if you want a rollback path."*

Two coherent answers, in opposite directions:

- **A.** Emit V1 only when an identity is actually present, so the version states
  the minimum reader required. This is what a format that must maximise interop
  does — zip's version-needed-to-extract, PNG chunk criticality. It removes the
  downgrade caveat for every caller not using identity, and keeps two versions
  and a parse branch standing forever.
- **B.** Keep V1 unconditional and delete V0 reading once nobody needs a
  rollback. Ends with one version and no branch.

**Chose B.** A buys interop with old readers, which is exactly what the standing
instruction says we do not want before v1, and it is the more sophisticated of
the two — the sweep is supposed to remove that kind of cleverness, not add it.
B's end state is strictly simpler.

**Done in v0.82.0.** Both peers cleared it — sqlcdc: *"every dir we own is
recreated per soak/test run, no v0.79.x data we cannot discard"*;
durable_streams: *"Go ahead, delete it... No deadline to work around."*

Two things the deletion turned up that the deletion itself would not have:

- `TestDescriptorRefusesUnknownKeysAndBadValues` built its fixtures with a `"0"`
  version line. Dropping V0 leaves that test **green for the wrong reason**:
  every body now fails on the version check without ever reaching the key
  parsing the test exists to exercise, and `require.Error` cannot tell those
  apart. The fixtures moved to version 1.
- `TestReopeningAnUnchangedLogDoesNotRewriteItsDescriptor` was written to answer
  a downstream compatibility question, so it looked like it should go with the
  compatibility. It should not. Its stated *reason* expired; its *property* —
  a reopen that changes nothing must not touch the file — did not, and is
  worth more without V0 than with it, because every write is a window a crash
  can land in. Rewritten to assert it directly, and tightened to compare mtime
  as well as bytes: equal content would be satisfied by a rewrite that happened
  to reproduce it, and the write is the thing under test.

The general rule, since it caught two tests in one small deletion: **when
removing a feature, the tests to re-read are the ones whose SETUP mentions it,
not the ones whose name does.** A fixture that encodes the removed thing keeps
passing and stops testing.

### Checked and clean

- **`block_table_local.go`** — already correct, and instructively so. The
  "every segment sealed before this existed" justification had been struck from
  the comment while the branch stayed, because the failed-write case keeps it
  alive independently. That is the right treatment: a branch defended only by a
  claim about old data invites deletion the day someone checks whether old data
  exists.
- **`block.go` / `parseBlockHeader`** — clean cutover, pre-version segments
  refused outright, "there is deliberately no compatibility path". Nothing to do.
- **`manifest.go`** — "nothing to migrate; a store written by an older build is
  re-offloaded, not converted."
- **`Options.Compression`'s** "byte-for-byte compatible with logs written before
  compression existed" is a statement of fact about what `None` means, not a
  compatibility branch.

## Axis 2 — obvious over-complication

Length ranking was already sampled in an earlier pass (top 5 functions, clean).
This pass swept the surface ranking cannot reach, using the repo's own recorded
lenses: *a knob whose doc apologises*, and *default arms launder bad input*.

### `Options.PrefixReadConcurrency` — a real one, fixed

`concurrencyBudget` defaults on `v <= 0`, so a negative reached the arm that
exists to catch a **missing** value and the caller silently got 8 or 64.

What lifts this above a routine laundered default is that it is reachable **by
following the documentation**. The sibling `CoalesceBytes` knobs are described in
the same `Options` paragraph, and that paragraph teaches that a negative is
meaningful and powerful here — *"NEGATIVE means never coalesce... the
maximum-concurrency and maximum-request-count setting"*. A caller who reads that
and asks for the analogous extreme on the concurrency knob was quietly given the
default. The doc created the expectation the code then violated.

Refused rather than given a meaning: the analogous extreme is unbounded on one
reading and serial on the other, and picking either would be this package
deciding something the caller was trying to say.

The remaining asymmetry — four knobs in one paragraph, two refusing a negative
and two not — is now pinned by a test, because it reads as an oversight and is
not. Without that test, "finishing the job" by adding the two `CoalesceBytes`
fields to the refusal list would go red only in a cost test that never mentions
`New`.

### The retention and rolling knobs — four more, and the worse ones

The pass above stopped at the prefix-read paragraph, which was a sample and not a
sweep. Carrying the same lens across the rest of `Options` found four more, two
of which fail worse than the one already fixed.

**`MaxSegmentAge`.** `CheckSplit` disables rolling on `logRollTime == 0`, so a
negative slips past and reaches
`timestamp()-firstWriteTime >= int64(logRollTime)` — true for anything a clock
can produce. Every append rolls a new segment, forever.

That is the *identical* failure the refusal table already documents for a
negative `MaxSegmentBytes`, whose comment reads "no panic — every append rolls,
forever. Measured: the probe never returned." The two fields sit one line apart
in `Options`, produce the same hang by the same mechanism, and only one of them
was checked. Worth stating plainly: the table was written by someone who had
just measured this exact failure and still did not generalise it to the field
next door.

**`MaxLogBytes` / `MaxLogMessages` / `MaxLogAge`.** These make two checks
disagree about one value. `noRetentionLimits()` asked `== 0`; the three apply
gates in `cleanLocal` ask `> 0`. A negative is therefore "retention is
configured" to the first and "do not apply it" to the others, so `Clean` takes
the do-work path, splits the tiers, walks the segments, emits a debug line naming
the policy it is enforcing — and enforces none of it. The log grows without bound
while the caller believes a limit is in force, and the only thing that would say
otherwise is the log line reporting the policy it is about to ignore.

Fixed at both ends, which are genuinely different fixes rather than belt and
braces: `New` refuses a negative, which is the caller-facing answer and the only
one that produces a message; and `noRetentionLimits()` now asks `<= 0` so it
agrees with its own apply gates. The second is for the type itself — a
`deleteCleaner` is constructed directly in tests and takes `Retention` as a plain
struct, so "the boundary already checked" is a promise that file cannot read.

### Not findings, checked with the same lens — SUPERSEDED, see below

- **`CompactMinAge`** — `compact_cleaner.go:186` gates the horizon on `> 0`, so a
  negative leaves it at zero and means "no protection", the same as unset.
- **`CompactTombstoneRetention`** — `> 0` at both `clean.go:412` and
  `compact_cleaner.go:527`. A negative reads as disabled everywhere. Worth having
  checked rather than assumed: a negative retention that had been *subtracted*
  would have meant "every tombstone is old enough", i.e. silent key destruction.

Both readings above are correct and both are now **refused anyway**. The lens
asked "does a negative misbehave?" and the answer is no. It did not ask what a
negative does to the log's IDENTITY, and both fields are in
`descriptor.enforced()` — so the negative is written into the descriptor and a
log created with `-1h` refuses a reopen with `0`. Two values that do the
identical thing, one permanently rejecting the other, surfacing months later as
a mismatch naming a knob whose two spellings mean the same.

Worth recording as a miss, not an update: a lens finds what it asks about. "Is
this value handled correctly?" and "is this value a second spelling of one we
already have?" are different questions, and only the second one finds this.

### Not a finding: `RewriteBudget`'s two zero-semantics

`CleanSpec.RewriteBudget` documents `0 = unbounded`. `Options.CleanRewriteBudget`
documents `0` as "default to the cleaner tick" and **negative** as unbounded. The
same concept with opposite zero-semantics in two structs used together is exactly
the shape the sweep is looking for.

It survives inspection. `clean.go:409` reconciles them at the seam
(`if l.Options.CleanRewriteBudget > 0`), so a negative in `Options` is simply not
copied, the spec keeps its `0`, and both mean unbounded. The two structs
legitimately differ in what "unset" means — `Options` is configuration, where
unset should pick a sane default; `CleanSpec` is an explicit per-call spec, where
a field the caller did not fill in means no bound was asked for. The conversion
is deliberate, documented at both ends, and `clean_tier_budget_zero_test.go`
already reasons about it directly.

Recorded so the next sweep does not re-open it.

---

## Second half: one lens, four findings

Everything above came from two lenses ("a default arm launders bad input", "a
knob whose doc apologises"). The rest of the day came from one, and it was by
far the most productive:

> **A list, maintained by hand, that has to stay in sync with something the list
> does not own.**

It describes the world as it is today, so the world changing IS the defect, and
nothing goes red — the list is still valid, just incomplete.

### 1. Two reserved-name lists guarding one directory

`logOwnedFileNames` and `logOwnedFileSuffixes`. The client's namespace was
defined by subtraction from a set only commitlog could change. Replaced by
`ClientSidecarPrefix`; both lists deleted. Full write-up in
`audit-2026-08-13-separation.md` §3b.

The part worth repeating here: **a refusal was not enough.** It governs names
arriving through `PutSidecar` and says nothing about files already on disk, and
`openLog`/`logIsNew` dispatch on suffix over whatever the directory holds. Until
the scans skipped the prefix, the prefix bought the client nothing — it was
still commitlog's suffix list defining the client's namespace, one layer in.

### 2. `renderDescriptor` and `set()` enumerate the `descriptor` struct

Twice, by hand, with no test tying either to the struct. Every other descriptor
test goes through `New` and `Options`, so each covers only the fields it happens
to set; a field that persists NOWHERE is invisible to all of them. Three
reachable failures, none of which anything would have caught:

- in the struct, not in `renderDescriptor` — silently not persisted, and for a
  field `reconcileDescriptor` keeps current that means the descriptor is
  rewritten on every single open;
- in the struct, not in `set()` — the file is unreadable by the build that
  wrote it;
- formatted differently either side — the value comes back changed.

### 3. `New`'s negative-refusal table enumerates `Options`

Missing an entry twice in one day (§ above). A test built from the same hand
list cannot see that. Now built from the struct by reflection, with an
**allowlist** of the three fields where a negative has a meaning — inverted
deliberately, because the refused list is the one that grows with the struct and
forgetting is silent, while the allowlist only grows when someone is already
thinking about what a negative should mean.

### 4. The codec set, written out five times

`Compress`, `DecompressInto`, `Valid`, `String`, `Parse` — and both existing
tests enumerate it a sixth and seventh time from literals, so a fifth codec was
covered by none of them. Cases now derived from `Valid()`, which is what DEFINES
the set: `New` refuses what `Valid` rejects, and that is the only reason
`Compress` is allowed a silent default arm at all.

Worst reachable failure: a codec in `Valid` and `DecompressInto` but not
`Compress` stores the block RAW under a header that names the codec, so the read
decompresses raw bytes as compressed ones. Silent corruption of data the write
path accepted.

## Two techniques worth reusing

**Reflection for completeness, hand-written values for validity.** A generic
"set every field to something non-zero" renders a `compress.Codec` that `Parse`
rejects and reports the wrong defect. So the fixture is hand-written and
reflection asserts that no field is left at its zero value. A newly added field
fails at the fixture, with the two ways to resolve it named in the message.

**Every derived assertion needs a floor.** A derivation that reaches zero cases
passes. `checked >= 15`, `found >= 4`, `len(suffixes) >= 3` — each one exists
because the loop above it would otherwise be green over an empty set.

## Found by fixing the above: guardcheck read an empty selection as a pass

`guard_finish` ran `go test -run "$re" ... .` — the root package only, with no
check that anything matched. The two new codec guards reported NO COVERAGE for a
test that was never selected. The existing `compress` guard had worked only by
accident: its test happens to live in the root package.

That is the fourth layer of one bug in one tool (build tags, the filter
argument, the package selection, "did anything match"), each found only after
the previous was fixed. Hardening one selector says nothing about the others.

---

# 2026-08-14 — the same lens, one file at a time

The sweep is standing, so this continues the file above rather than starting a
new one. The productive lens was again duplication, but sharpened: instead of
reading files and noticing repetition, an awk pass over the normalised
non-comment source reports every run of N identical consecutive lines that
appears twice. That finds copies a reader's eye slides over — including ones
whose *comments* differ, which is exactly the state a copy reaches just before
the code diverges too.

## A contorted guard anchor is evidence of duplication in the code

The single most useful signal of the day, and it was already written down.

`hack/guardcheck.sh` neutralizes a guard by replacing an exact string, and
refuses an ambiguous match. Two of its anchors carried comments explaining why
they were shaped oddly:

- the frame-header CRC guard was anchored on the RETURN beneath the condition,
  "because readMessage and readMessageMetadata now decode out of an identically
  named local";
- and v0.61.2's changelog entry says the missing-file and budget guards were
  "narrowed to `ReadFileWithRetry` with multi-line anchors" after
  `openWithRetry` and `renameWithRetry` copied its loop.

Both notes describe a duplication, accept it, and work around it. Both
duplications were still there today. **The workaround outlived the reason to
tolerate it**, and the anchor stayed contorted long after anyone remembered
why — so an odd anchor is worth reading as a report about the code rather than
about the tool.

Both are now single-sourced (`readFrameHeader`, `retryWhileHeld`) and both
anchors are back to the bare condition. The retry one is the better outcome of
the two: that guard had been *narrowed to one of three copies*, so it read as
covering the rule and covered a third of it — and the two ends of a single
Windows window (a reader's open, a publisher's rename) were among the copies it
did not reach.

## A hand-written list of callers cannot notice one that never joins

`syncDir`'s doc named the writes that need it: "the high watermark checkpoint, a
client's sidecar, an object published into a file-backed tier." Two more
belonged on that list and were absent from it *because they were absent from the
path*: `writeDescriptor` and `leaderEpochCache.flush` called the atomic-file
library directly, so they got torn-write safety and no durability.

The descriptor is the worst possible file for that. It is what says the log
exists and what it is; `removeLogDir` orders the whole of `Delete` around
exactly that fact, and `readDescriptor` refuses a directory of segments with no
descriptor, permanently. So an unsynced rename could produce the same bricked
log the `Delete` ordering was fixed to prevent, by a different route.

The fix is one call each. The interesting part is what replaced the sentence:
`hack/atomicwrite.sh` requires that only the wrapper's own file imports the
library. A rule stated in prose is a rule that new code does not have to read.

## A "discard the loser" race needs the two attempts to be independent

`RemoteIndexCache.acquire` downloads outside its lock — correct, it is I/O — and
handled a concurrent download of the same key by keeping one and discarding the
other. That shape is right only when the two attempts do not interfere, and
these did: the cache file is named from the object key alone, so both wrote the
*same path*. The second `os.Create` truncated a file the first had already
mmapped.

Ordinary use reaches it. `withIndex` runs under the segment **read** lock, so
two readers seeking into one cold segment race here by design.

Worth generalising: *"run both and throw one away"* is a claim about
independence, and a shared destination — a fixed filename, a fixed key, a
well-known temp path — falsifies it. The comment said "the insert below dedups",
which is true of the map and says nothing about the file.

## Negatives, recorded so the next sweep does not re-open them

- `read_options.go` mixes `untilSet` with a `-1` sentinel for "unbounded".
  Checked: `prefixSource.bound()` genuinely uses the `-1`, and `resolve` refuses
  a negative `Until` before it can be confused with the sentinel. Not a
  laundered value.
- `messageSet`'s field accessors and `readFrameHeader`'s extraction both read
  the frame header, but both index the same named constants, so the constant is
  the single source. Not a transcription.
- `keydigest.go`'s `writeKeyDigest` commits with a bare `os.Rename`, which is
  the publisher's half of the Windows window. Left alone deliberately: all three
  callers treat it as best-effort with a rebuild behind it, and by `util.go`'s
  own rule a write with a retry behind it belongs on `tickWriteRetryBudget`,
  which `renameWithRetry` cannot express. Recorded as work, not done badly now.
- `uncommittedReader.segmentBounds` and `committedReader.segmentBounds` are
  identical. The honest fix is a shared embedded base for the two reader
  structs, which is a real refactor and not a tack-on; nine trivial lines can
  wait for it.

# 2026-08-14 — counting runs instead of reading them

The lens that produced everything below is mechanical: normalise every non-test
`.go` file (strip blanks and comments), then report every run of N identical
consecutive lines that appears more than once. At W=8 it found two things; at
W=6, seven. Reading had walked past all of them, which is the point — a
transcription is invisible exactly where the two copies read naturally in their
own context.

## What it found

- `keyOffsets` / `valueOffsets` — one length-prefixed field read twice. The rule
  both spelled out is that `-1` means ABSENT rather than a length to skip, so a
  copy that added it anyway moves `end` back four bytes and returns a slice into
  the previous field.
- Both `Stream` implementations ended in the same seek-or-close tail. The close
  is the whole of it: on Windows a leaked handle is not merely a leak, it is
  what fails the next publish's rename over that path.
- `readAtLocked` / `scanReadAt` — the same five-line comment and the same
  closed-then-left discrimination. Collapsing it forced the ordering to be
  stated: `left` is consulted only once `closed` is true, because a segment
  marked as left but still open still has its bytes. `findEntryBy` asks a
  neighbouring question and gets a different answer, which is now written down
  rather than inferred from the two not matching.
- Both segment constructors opened with the same seven-field literal. The
  interesting fields are `firstOffset`/`lastOffset`, whose default is `-1`:
  a default that is the OPPOSITE of the empty value is exactly the one that
  survives nine copies and not the tenth, and offset 0 is a real record.
- `cleanerLoop` / `checkpointHWLoop` — the same ticker-and-select. The ordering
  inside it is the rule: the closed arm returns before the body runs.
- `findEntry` / `findEntryByTimestamp` — identical but for three predicates, and
  the five-line `ErrSegmentReplaced` comment appeared in both.

## A guard can be covered here and uncovered on the runner

`descriptor read retries a held file` was green in the local unfiltered run and
NO COVERAGE on `guard coverage (windows)`, twice.

The fixture held an exclusive handle on the descriptor for a fixed 120ms and
required `New()` to succeed. That is a test only while the handle is still held
when the descriptor is READ, and `New()` reaches that read after claiming the
directory, building the epoch cache and running `init()`. Here that prologue is
a few milliseconds; on the runner the window closed first. Nothing went red: the
mutated build passed, and the ordinary Windows job was green throughout, because
an unobstructed open succeeds too.

Worth generalising: **a fixture whose window is a `time.Sleep` proves nothing
unless the test asserts the wait HAPPENED.** Time the call and require
`elapsed >= hold`. Without it, "the fixture missed its own window" and "the code
is correct" are the same green — and the only thing that noticed here was a tool
whose whole job is noticing, on the one platform it runs.

The second half of that story is that `guardcheck` captured the run that would
have explained it and printed only the verdict. It prints the result lines now;
the durations are the answer.

## Negatives, recorded so the next sweep does not re-open them

- The eight `ss.Scan()` sites each re-state "io.EOF ends the scan, anything else
  is damage" in their own words, and a helper looked like the obvious collapse.
  Declined, on what the recorded failures actually were: both times this rule
  was got wrong, the loop had **no discrimination at all** — `for ms, _, err :=
  ss.Scan(); err == nil;` simply ended on either — and a helper is reachable
  only from code that already knows to call it. It would not have prevented
  either bug. It also saves no lines (four at each site becomes four), the
  message at each site is different and worth being different, one site adds
  `len(out) == 0`, another has to `ss.Close()` first, and `prefix_read` needs
  the rule INVERTED. Centralising here buys the appearance of a single source
  over a rule whose every use is a judgement about what that caller loses.
- `uncommittedReader.segmentBounds` / `committedReader.segmentBounds` — still
  identical, still waiting on the shared embedded base that is the honest fix.

# 2026-08-14 — measuring the mechanism, not the source

Once the repeated-run count stopped finding anything, the next lens was size:
every non-test function of 80 lines or more, with its branch count. It reported
15, and the two biggest after `New`/`open` were `Truncate` (171) and
`TruncateBefore` (180) — the mirror image of each other, and exactly the shape
the transcription lens keeps finding.

They are **not** a transcription, and that is worth writing down so the next
sweep does not try to merge them. Their commit-point orders are deliberately
opposite: `Truncate` deletes the records above the cut *before* publishing,
because making them unreachable as early as possible is the whole point of the
call (a follower that has been told it diverged); `TruncateBefore` publishes
*before* it unlinks, because a retention pass that fails halfway should leave
files behind rather than a list naming files that are gone. Both say so in
place. One function taking a direction flag would have to carry both orders and
choose between them, which is the same code with the reason deleted.

What the size lens *did* find is one real defect, and it was invisible to every
lens before it because nothing was duplicated: `TruncateBefore` collected the
whole kept region of the boundary segment into `kept []messageSet` before
creating the trim, while `Truncate` streamed. The buffer existed to learn one
`int64` — the new base offset `Trimmed()` needs — which the FIRST kept record
already had. Creating the destination lazily on that record removes an
allocation bounded only by `MaxSegmentBytes`, and removes the `newBaseOffset`
sentinel with it.

The generalisation: **a duplication lens cannot see a divergence.** Two
functions doing the mirror image of one job are the place to look for one of
them having quietly grown a worse implementation, and the tell is not that the
code matches — it is that the *job* does.

## Lenses run and closed

- Exported functions with zero production call sites. Eight hits, all false:
  most are library API for external callers, and the two that looked genuinely
  dead — `segment.SyncData` (0 prod, 0 test) and `segment.MessageCount` — are
  called as **method expressions**, `l.forEachSegment((*segment).SyncData)`. A
  call-site scan looking for `.Name(` cannot see those. Any future dead-code
  pass has to count `(*T).Name` too, or it will propose deleting live code.

## The same lens, two more finds

Reading the top of the size list in pairs also reached `New`/`open`, and `New`
turned up a variant of the divergence above that is not about performance:

- **Two spellings of one directory.** `New` resolved the absolute path and used
  it for the dir lock, the epoch cache and the descriptor, while `l.Path` and
  both cleaners kept the caller's string. They agree until the process chdirs,
  and then the half on the relative path opens files somewhere else while the
  half holding the lock does not notice. One resolve at the top now; the
  `descOpts` copy that existed only to carry the absolute form to two calls is
  gone with it. `commitLog.init()` went too — a `MkdirAll` on a directory `New`
  had created forty lines earlier, no other caller.

- **A flag argument at 25 call sites.** `newSegment(..., isNew bool, suffix
  string, codec)`, called as `true, ""` twenty-two times and `false, ""` three.
  The bool is the whole decision — refuse an existing log file, or adopt it —
  and it was the least legible thing at every call site. `newSegment` and
  `openSegment` now, over a shared `newSegmentWith` that keeps both parameters
  for the working copies. Worth noting what the split bought beyond reading:
  *which* production site creates and which adopts became checkable, and it is
  three creates (roll, empty log, local head after a tier adoption — all of
  which mean "nothing may be here") against one adopt (`open`, which found the
  file in a listing).

The pattern across all three: none of them were duplication. Each was a
decision that had been spelled out in a place where nothing could check it —
a buffer that stood in for one integer, a second name for one directory, a
boolean at a call site. Repetition is the easy defect to scan for; a fact
recorded in a place that cannot verify it is the one that needs reading.

## 2026-08-14, second pass — down the rest of the size list

The size lens was worked to the bottom of the top-32. Most of what it reached
was fine: `compact`, `cleanSegment`, `mergeDigests`, `cleanPass`, `clean`,
`Sync`, `collectRun`, `scanBlocks`, `reconcileIndexTailRaw`,
`earliestOffsetAfterTimestampLocked` and `UnreferencedObjects` are long because
the decisions in them are, and every one of them is carrying its reasons.

That is worth recording as a result. A size ranking is a way to find the
functions where something can hide, not a list of defects, and a pass that
reports nothing on ten of them and something on four is behaving correctly.

What it did find, in one shape:

- **A counter beside the thing that knew.** `newMessageSetFromProto` advanced a
  running `int32` with six `+=` lines transcribing the frame header's field
  widths, to compute one thing: where each record starts in the buffer.
  `buf.Len()` answers that, and the header checksum three lines below already
  asked it that way. Change a width in the encoder and not in the counter and
  every index entry's `Position` moves while every byte on disk stays right.

- **One read in both branches.** `setupIndex` ended its block-mode and raw arms
  with the same five lines. Only the *last* offset is layout-dependent — entry
  0 is the first message under both, since a sparse index anchors each block AT
  its first message.

- **One predicate, three spellings.** `digestHits` wrote its `[from, bound]`
  test at three sites, the third about a differently named variable.

- **Deduplication written as a lookup.** `publishTierManifests` walked its
  pending list twice, the second walk asking a map whether each entry had
  survived the first. Whatever the first walk leaves in the map *is* the set of
  additions. The second walk's `delete` was the only thing stopping two pending
  entries with one base offset from being appended twice, and nothing said so.

## The find that was not a refactor

Reading `uncommittedReader.Read` for structure turned up `err != io.EOF`, and
grepping that shape across the package found `index_cache.go:210` — a third
place that reads a **caller's** `SegmentStore` with `==`.

The `SegmentStore.ReadAt` doc already forbade it. It says the sentinels may be
wrapped and that commitlog compares with `errors.Is`, and it says so *because*
two sites in `storeBacking` had got it wrong. The doc names two places. There
were three.

This is the fifth-copy shape again, and the tell is specific enough to reuse: a
doc comment that says "the N places that do X" is a claim about the call graph
written in prose. Nothing checks it, and the copy that is not next to the fix is
the one that keeps the bug. Grep the shape, not the doc.

Two further holes fell out of reading that loop once the first was clear: an
object ending before the size the store itself had just reported was accepted
(leaving a short cache file that `newIndex` maps and reads as a whole index, so
seeks answer "not found" for offsets the segment holds), and a `(0, nil)`
return — which `io.ReaderAt` forbids — was retried at the same offset forever.

## What a red run was actually saying: two constants racing a goroutine

Not a complexity finding, but this pass produced it and the lens generalises, so
it is recorded here.

Verifying the reader work, `-race` came back red on
`TestChaosAFollowerNeverSeesTheSequenceGoBackwards/codec=0` at 122.31s with *the
run never became dangerous enough to assert on: retention never got past the
follower*. The invariant held — `violation` was nil. `overtaken` is the last case
in `unmet`, so every other condition was met and the run spent its entire
deadline waiting for that one. Nothing to do with the reader.

Two constants, found one after the other:

- **The stall between reader lifetimes, a fixed `800 * time.Millisecond`.**
  `overtaken` only increments if retention collected past the follower during it.
  The interval was chosen by measurement — 250ms had come back ten offsets short,
  run after run — but what it must outpace is the *writer*, which slows under
  `-race` and under load exactly as everything else does. So the number describes
  one machine at one moment, and on a box also running two peers' suites it
  stopped buying enough records. Replaced with `l.OldestOffset() > lastOffset`,
  which is not an approximation of being overtaken but the exact test the resume
  four lines later makes. `codec=0`: 122.31s failing → 3.29s passing.

- **The stall within a scan, a fixed `100 * time.Millisecond`.** Found by asking
  the rest of the test the question the first one taught: *what does this number
  have to outpace, and does that thing slow down with the machine?* This one must
  outlast retention reaching a position an open reader is sitting on — and its
  failure was silent rather than red. `rebuilds` was zero on every run, and the
  test's own doc read that zero as a fact about the log. It could equally have
  meant the stall never provoked a departure. Same zero, two readings, nothing to
  tell them apart. Waiting on the condition tells them apart: the wait now returns
  only once the record the follower last read has been collected *while it still
  holds the reader that read it*, so the next read is issued from a position that
  no longer exists. It is still zero — which is now the fact the doc claimed.
  Whole test under `-race`: 92.21s → 7.20s, `overtaken=2` on each codec,
  `crossed` 142-145 against 132-142, the same 528 reads.

The generalisable part is not "don't sleep in tests". It is that **removing the
binding constant does not make a run condition-paced — it promotes the next
constant to binding.** The rewrite recorded above `unmet` deleted a fixed write
count of 3000 and wrote that the run was now "self-pacing on any machine at any
speed"; that sentence is what kept the two survivors invisible. After deleting
one, grep for every other literal duration and count on the same path.

It is also the answer to why issues here seem to need high-intensity soak runs to
surface: a run whose danger arrives on a timer is soak-shaped by construction.
Neither of these needed a longer run. Both needed the dangerous thing to happen
on purpose.

### A third, found by the same lens turning up in CI hours later

`guard coverage (windows)` went red with **"a barrier waits longer than a tick"
NO COVERAGE** — under the guard's mutation, which hands `SyncAll` the tick's
500 ms budget instead of the caller-waited 5 s, the test passed anyway. Nobody
had touched it, and every other job was green.

The fixture held the checkpoint file for `tickWriteRetryBudget + 250ms`. The
sleep starts when the goroutine is scheduled; the retry's **deadline** starts
when `checkpointHW` is reached, and `SyncAll` fsyncs every segment first.
Everything in between is spent out of that 250 ms. The runner spent all of it,
so by the shortened deadline the handle had already been released and the
mutated call succeeded.

Same shape as both stalls above, in a place none of the greps for durations in
the chaos tests would have reached: **a margin is a constant racing real work.**
Fixed by deriving the hold as the midpoint of the two budgets — the largest
margin they allow, and it tracks if either moves.

The addition worth copying is the assertion, not the number. `SyncAll` returning
`nil` is *equally* what a `SyncAll` that never met the held handle returns; only
`returned.After(closedAt)` separates waiting it out from missing it. Without it
the failure mode is silence — the test passes, and the only thing that notices
is guardcheck calling its own guard uncovered, on one platform. That is the
silent-zero shape again: one observation, two readings, and no way to tell them
apart until the distinguishing condition is asserted.

## A mechanical duplicate scan, and the one thing it caught

After the hand-picked duplications ran out, a mechanical pass: every non-comment
line of every non-test `.go` file, hashed in windows, looking for repeats. At six
lines the only hits in the package were two import blocks — worth recording as a
negative, because it says the transcription findings above are genuinely done and
the next sweep should not go looking for more of them.

At four lines there were thirteen, nearly all of them legitimate: the same struct
literal built from the same fields, the same `wg.Wait()` + error-drain tail.
Reading them was still worth it, because one was not duplication at all.

**Three sites open-coded `segmentsSnapshot()` and then copied the result** —
`ReadMessageSet`, `forEachSegment`, `movePlaced`, each with its own `RLock` and a
`make`+`copy`. The function they were reproducing hands out the live slice
header on purpose, and its doc explains at length why that is safe: the set is
copy-on-write, so the array no one may write to needs no defending. The copy was
buying an allocation and nothing else.

That is the harmless half. Following the doc's *obligation* — "whoever changes
the set publishes a NEW array" — to its callers turned up one that broke it:

**`adoptTierManifestLocked` appended to `l.segments` and then called
`sort.Slice` on it.** A sort swaps elements in place, which is the exact thing
the doc forbids and the exact thing v0.44.2 was spent on, in `TruncateBefore`.

Two lessons, and they are the reusable part:

- **A write that does not look like a write.** Both breaks of this rule were
  that. An element assignment at least reads as a write; a sort does not read as
  one at all. Write invariants as the OPERATIONS that violate them — `x[i] =`,
  any sort, `copy` into it — not as the concept, because the next violation
  will be spelled a way the concept did not suggest.
- **Safe by the schedule is not safe, and it is where a comment is weakest.**
  The adopt was harmless only because it runs inside `open()`, before there is a
  log for anyone to hold a reader on. So no test could go red — there is no
  reader to lose the race — and a guardcheck guard was impossible in principle
  rather than merely expensive. Nothing would ever have contradicted the
  comment. The first maintenance path to adopt a manifest another process
  published would have removed the schedule without touching this function.

Hence `hack/cowsegments.sh` rather than a guard, in CI beside `layercheck` and
`atomicwrite`. **The decision rule: if a violation is invisible to every test you
could write, it is a lint, not a guard.** It strips comments before matching —
the prohibited forms are quoted verbatim in the docs that explain the rule, here
included — and it carries the harness self-check this repo now always writes: if
`segmentsSnapshot` ever stops returning the live header, it errors loudly instead
of passing forever while enforcing a rule that has ceased to exist. Both halves
falsified before landing, the offender half against a throwaway `.go` file made
visible to `git ls-files` with `git add -N`.

## A fourth: the fixed sleep in front of a negative assertion

`TestDisableAutoClean` is the same silent-zero shape as the two chaos-test finds,
in the plainest possible form — three lines:

```go
l.SetHighWatermark(last)
time.Sleep(400 * time.Millisecond) // several cleaner intervals
require.Equal(t, int64(0), l.OldestOffset(), "auto-clean ran despite DisableAutoClean")
```

The claim is a NEGATIVE — *the loop did not clean* — and the only way to give a
loop the chance to misbehave is to let wall clock pass. So "nothing was cleaned"
has two readings: the flag worked, or the loop never got a tick's worth of work
done at all. `CleanerInterval` is 50 ms and the sleep is 400 ms, which reads like
eight intervals of margin; on a runner where a tick's actual work — roll check,
retention scan, segment delete — does not finish inside 400 ms, the second
reading is the true one and the test passes having proved nothing. A margin
racing real work, for the fourth time this pass, and this one predates all three.

The fix is the same as the chaos tests': **price the provocation instead of
guessing at it.** An identical log with the flag OFF is built first and waited on
via `require.Eventually` until it actually cleans; that measurement, `loopTakes`,
sets the disabled log's wait at `2*loopTakes + 200ms`. The `Eventually` is the
provocation made into a condition — it fails loudly, naming the fixture, in
exactly the state that would otherwise have made the real assertion vacuous.

Both halves falsified before landing, by mutating `cleanOnce`:
`if false && l.DisableAutoClean` (the flag ignored) fails the real assertion;
`if true || l.DisableAutoClean` (nothing ever cleans) fails the priced
`Eventually` with its own message rather than passing the test. The old body
survives the second mutation.

Worth noting what this costs: the priced wait is *self-scaling*, so on this box
the test now runs in 0.46 s where the fixed sleep took 0.4 s of pure sleeping,
and on a loaded runner it grows instead of lying. A priced wait is usually
faster than the conservative constant it replaces, because the constant has to be
large enough for the worst machine anyone will ever run it on.

## A fifth, and the sharpest form of it: a test whose only assertion is a detector

`TestRetentionNeverWritesIntoASliceAReaderIsHolding` reproduces the v0.44.2 bug —
`TruncateBefore` replacing its boundary segment *in place* while lock-free
readers index the same backing array. Its doc says so plainly: *"It asserts
nothing by itself — the race detector is the assertion."*

Which makes it the purest instance of the lens on this page, because a detector
that finds nothing and a test that never performed the operation produce the
identical result. This one had two independent ways to perform none:

- The truncation's error was discarded — `_ = l.TruncateBefore(newest - 20)`.
  Every call could have been failing.
- The rewrite branch runs **only when the cut straddles a sealed segment**. Cut
  on a segment base and `TruncateBefore` unlinks whole segments and returns nil,
  never touching a shared array. `newest - 20` was a *hope* that the cut would
  land mid-segment.

And the only thing checked afterwards was `l.NewestOffset() >= 0`.

Fixed by making the straddle a **construction** rather than a hope — the cut is
one past a sealed segment's base, which that segment therefore spans by
definition, and a concurrent roll cannot spoil it because rolls only append — and
then counting the rewrites and flooring them, alongside a count of non-empty
reader snapshot walks. 66 rewrites and ~400k walks in three seconds here; the
floors are 10 and 100, sized to catch zero rather than to measure the machine.

### The counter needed falsifying too, and failed the first time

The first version counted `TruncateBefore` returning nil, relying on my reading
of the selection rule to make that mean "rewrote" — the transcription trap. The
second counted the outcome: the log's first segment now starting at the cut.

Mutating the cut down onto `s.BaseOffset` — which by the production condition
skips the rewrite entirely — **still counted 418 rewrites**, because an
*untouched* segment already starts at its own base. The offset alone cannot tell
a published trim from a survivor. What distinguishes them is object identity:
`after[0] != s`. That is what "rewritten" means, and a whole-segment delete never
produces it.

Worth stating as a rule, because it is the same mistake in miniature: **an
outcome check is only a check if the outcome is unreachable without the
operation.** Falsify the counter, not just the assertion — a floor over a count
that a vacuous run also produces is exactly the thing this whole section is about.

## The same lens, pointed at a cost assertion instead of a correctness one

`TestOpeningAnOffloadedTierReadsNoLogObjects` is a good test. It asserts a hard
zero — an open must download none of its offloaded segments' bytes — it covers
both boot paths, and it ends with "the work moved, it did not vanish" so the zero
cannot be satisfied by a tier that never opened.

It configures a `RemoteIndexCache` in every case, and the code path it needed to
guard is reachable **only without one**.

Tiering has two options. Option 2 offloads the index into the store; option 1
keeps it on local disk, and durable_streams confirms cache-less is their
**default** tiered mode. Under option 1 `setupIndex` derives the segment's last
record from the local index — and a block index anchors *blocks*, so the last
anchor is a block's first message and finding the end means reading that final
block. For an offloaded segment that block is in the store. Measured on an
ordinary reopen of a 90k-record snappy tier: **40,947 bytes across 8 requests,
one per segment**, for two numbers every manifest entry already carries. On a
tier of hundreds of segments against real object storage that is hundreds of
round trips serialised into startup.

Fixed by handing the boundaries in (`setupIndexKnownEnd`), and only for the
branch that would otherwise pay for them: a raw index has one entry per record,
so its last entry *is* the answer, and overriding it would only hide a
disagreement between index and manifest.

### The half of this finding that was not real, and what caught it

The first version of the fix also made adoption's rebuild-the-index-from-the-object
branch conditional, on the reasoning that it ran unconditionally for every
option-1 entry and therefore downloaded the whole tier on every ordinary reopen.
That reasoning was wrong, and the CHANGELOG entry claiming it was written before
it was checked.

What caught it: **measuring the pre-fix code against the new test** — reverting
the branch to its original form and running the test that was supposed to prove
it broken. It passed. `reconcileIndexTail` starts at the last indexed frame's end
and runs while that is below the segment's size, so an index that already
describes its object executes the loop body zero times and reads nothing. Its own
CHANGELOG entry says so, in the release that introduced it.

So the conditional bought no I/O at all, and it was reverted along with the
predicate written to support it, the doc explaining the predicate, and the
guardcheck guard pinning it. Two lessons, and the first is the uncomfortable one:

- **A plausible cost is not a measured one.** Every step of the argument was
  sound except the premise, and the fix, the test, the guard and the changelog
  entry were all built on top of it before anything measured it. The check is
  cheap and mechanical: put the old code back, run the new test, and require it
  to go red. Same discipline as falsifying a guard, applied to a performance
  claim instead of a correctness one.
- **A comment can be true of one caller and read as true of all of them** — the
  branch says *"this directory has never held that index"*, which is true of a
  fresh directory and false of the one the log offloaded from. That reading is
  what produced the wrong premise. It is the disease in
  `project_a_retry_budget_belongs_to_the_caller`, and the tell is the same: ask
  how many situations reach the code and whether the justification covers them
  all. Here the answer happened not to matter, because the code was cheap in both
  — but a justification that only covers one caller is worth rewriting even when
  the behaviour it defends turns out to be right.

What survived is the genuinely missing thing: the cache-less configuration had
**no cost coverage whatsoever**, and the block-tail round trip was sitting in it.
The new test covers both boot paths, and asserts the fresh-directory adopt still
downloads — deleting a cost is not the same as deleting the work, and a test that
only checks for zeros would certify a version that skipped the rebuild
everywhere and opened every segment with an empty index.

## Negatives from the configuration-coverage lens, recorded

Having found one cost assertion scoped to a configuration its branch cannot
reach, the obvious next question is how many more there are. Four checks, all
negative — worth writing down so the next sweep starts somewhere else:

- **Every `Options` field is set by some test.** A mechanical pass over the 25
  fields found two apparent gaps, `PrefixReadConcurrency` and
  `PrefixReadTierConcurrency`; both are set as `l.Options.X = …` rather than in a
  struct literal, which is what the scan matched. No field is untouched.
- **The cache-less tier is not otherwise under-covered.** 36 test files configure
  a tier and only nine involve a `RemoteIndexCache`, so option 1 is the *default*
  in the suite as well as in deployment. The open-cost assertion was the outlier,
  not the pattern.
- **`open_block_table_test.go` is the model for this kind of test.** Its zero
  ("reopening walks no block headers") is paired with a segment-count floor, and
  three sibling tests assert the walk is `Positive` when the sidecar is absent,
  truncated, corrupt, or describes a different file. It even asserts *while the
  log is still open*, because closing seals every segment and writes the sidecar
  anyway — so a check afterwards would pass against code that persists nothing.
  Both directions, every time.
- **No pre-v1 compatibility code is left.** A scan for migration/legacy/backcompat
  language across non-test Go returns nine hits, all of them the word "backwards"
  used about offsets or loop direction, plus two comments stating outright that
  there is nothing to migrate. Nothing to clean up here.

The complexity ranking was also re-run (branch count rather than line count, so a
different ordering): `compact`, `mergeDigests`, `open`, `cleanSegment`,
`TruncateBefore` head the list, and all five were read in the 2026-08-14 size
pass and found to be carrying their reasons. Two independent rankings reaching
the same functions and the same verdict is the signal that this lens is spent.

Two more, from lenses that looked promising and were not:

- **Nothing exported is unreferenced.** Every exported function, type, var and
  const in the module has at least one use outside its own definition.
- **The `compress` package needs nothing.** Its four switches over the codec set
  are already closed by `TestEveryValidCodecRoundTripsItsDataAndItsName`, which
  derives its cases from `Valid()` — the function that *defines* the set — rather
  than from a literal list, floors the count so a broken bound cannot empty the
  loop, and names the three failures it reaches worst-first. It is the model for
  a test over a hand-written set.

### The `Locked` suffix: a real inconsistency, deliberately left alone

Nineteen functions document a lock the caller must already hold. Six carry the
`Locked` suffix (`adoptTierManifestLocked`, `offloadMetaLocked`,
`attachOffloadedLocked`, `shutErrorLocked`, `objectKeysLocked`, `evictLocked`);
thirteen do not (`isOffloaded`, `withIndex`, `fetchBlockTable`, `scanForward`,
`closeSegments`, `anchorPositionFor*`, …). The suffix's whole value is that the
requirement is visible at the CALL site, which is where the mistake happens —
and this repo has paid for a lock mistake before, when a lock-taking call added
under `setupIndex` hung the suite for its full 30-minute timeout.

Not taken, and the reason is worth recording so the next pass does not re-open it:
the information is not missing, only placed differently — all nineteen say it in
the doc — and renaming thirteen functions touches every call site to restate a
rule the enclosing function's own body already establishes. More to the point,
the suffix would not have caught the incident it is supposed to prevent:
`setupIndex` requires no lock at all. What bit was a callee that TAKES one, and
Go has no convention that marks those.

One thing did come out of it. `notifyHWChange` says *"This must be called within
the log mutex"* where the other eighteen say *"the caller holds …"*. A rule
written a second way is the shape a mechanical check misses — the same finding as
the dissimilar copy in `project_a_transcribed_rule_grows_a_dissimilar_copy`, in a
place where nothing mechanical is looking yet.

## Follow-ups this pass opened

- ~~**`r.br.pos = r.pos` in `committedReader.readLoop`.** The one place left that
  moves part of the cursor by poking a field. It is deliberate and it is not a
  `refill()`: keeping `bufStart` and `data` means a subsequent small read can
  still be served from the buffer the direct `ReadAt` bypassed, and that is safe
  only because a written byte never changes. `reset` would be correct and would
  throw the buffer away. Worth a named `bufReader` method carrying that
  sentence, so the next reader does not "tidy" it into a reset and quietly lose
  the buffer, or into nothing and read from a stale position. Not taken in the
  same change as the cursor operations — one verified thing at a time.~~
  **Taken, 2026-08-14, as `bufReader.advanceTo`.**

  Guarded, which was not obvious in advance: the tidy-into-a-`reset` failure is
  a performance regression no test can see, but the tidy-into-*nothing* failure
  is loud. Mutating the line to `_ = r.br` leaves the buffered reader at its
  pre-`ReadAt` position, so the next small read re-serves bytes the direct read
  already consumed — and what surfaces is not "wrong data" but a **CRC failure
  on the healthy record after the large one**, with an expected value of
  `0x78787878`. That reads as ASCII `xxxx`, which is the tell for misalignment
  rather than damage.

  Both large-message tests are named on the one guard, because `ReadMessage`
  and `ReadMessageMetadata` reach this line through different callers and either
  alone would leave the other asserting nothing about it.

## Deferred, with reasons

- ~~**`uncommittedReader.Read`.** Two arms advance to the next segment with
  near-identical code, a `waiting` flag threads through both, and one arm sets
  `r.pos = 0` where the other relies on the next iteration to resync it. It is
  genuinely harder to read than anything else in the package. Not taken: it is
  live-tailing code with recent fixes under it, the duplication is four lines,
  and a restructure buys legibility at the cost of a class of bug this package
  has already paid for once. Worth doing behind a test that drives a roll
  during a parked read, not before one.~~ **Taken, 2026-08-14, in that order.**

  The test came first because the deferral said it had to. Two of them, in
  `reader_roll_test.go`: a reader parked at the tail when an age-driven
  `cleanerTick` seals its segment, and a reader that finds the roll already
  done. Two rather than one because *which arm a run takes was decided by a
  race* — a single test covers whichever state it happened to reach and reports
  the other as covered. The parked case waits until a waiter is actually
  registered on the segment before rolling, so the pair cannot silently collapse
  into the same test run twice.

  Both were falsified against the pre-restructure code, one arm at a time:
  carrying `r.pos` into the new segment in the woken arm turned the parked test
  red and left the other green, and the same edit in the walked-into arm did the
  reverse. That is the evidence that the two arms were separately reachable, and
  it is what made the merge safe to attempt.

  The restructure then deleted the `waiting` flag, the `LOOP:` label and one of
  the two advances. What remains is the rule both arms were spelling: *drained —
  so take the next segment if there is one, otherwise park; on waking, refill and
  read again rather than deciding here why you woke.* The old second arm existed
  only because the flag had committed it to answering that question at the point
  of waking; a sealed segment simply reads EOF a second time and takes the one
  advance on the next pass.

  One thing changed on purpose rather than incidentally: the segment snapshot is
  re-taken at each boundary instead of once at function entry. The old first arm
  searched the entry-time snapshot, which a reader parked at the tail can hold
  for as long as the writer is idle — and a roll is exactly the event it must
  not miss. The old second arm already re-snapshotted; now there is one rule
  instead of two, and it is the safe one.

  The two guards collapsed into one with it, and the remaining guard names
  *both* tests — because the code is one site now, and a guard naming one test
  would leave the other asserting nothing about that line.

  **One loose end, recorded rather than buried.** The first `-race` run after the
  merge came back `FAIL`, and it was piped through `tail -6` — which drops the
  `--- FAIL` line and keeps the noise, exactly the failure mode already written
  down in the local-suite note. The failing test's name is simply gone. Ten
  subsequent runs of the same selection were clean: four standalone, four in one
  `-count=4`, and two deliberately run concurrently with each other to test the
  CPU-starvation theory (which did not reproduce it). `tempDir` is
  `os.MkdirTemp`, so concurrent runs cannot collide on a fixture path either. It
  is committed on that evidence, with CI's fourteen jobs across three platforms
  as the next check — but if this area goes red once more, this paragraph is the
  prior, and the run that produces it must not be piped through anything.
- ~~**A shared embedded base for `uncommittedReader`/`committedReader`,** so
  `segmentBounds` exists once.~~ **Taken, 2026-08-14.** The base is
  `segmentCursor` — `mu`, `seg`, `pos`, `br` — and it stops there on purpose.
  `cl` and `noWait` are in both readers as well, and pulling them in was the
  tempting move that would have been wrong: `noWait` names a *different* thing
  in each (do not park for appends / do not park for the watermark), so a shared
  field means one doc comment stretched over two behaviours, which is how a
  reader ends up believing the wrong one. The mutex does come along, because it
  is what `segmentBounds` locks — and it stays a *named* field rather than an
  embedded `sync.Mutex`, or `Lock`/`Unlock` would be promoted onto both readers
  and advertise a lock no caller should take.

  Worth naming what "shared base" is not licence to do. The readers still have
  their own `Read`, their own parking, and their own end conditions; what moved
  is the state a *third* party (`Reader.readOne`) asks them about. A base that
  had absorbed the union of their fields would have made the two types look
  interchangeable at exactly the boundary where they are not.

## The duplicate scan again, this time whole functions and including the tests

The earlier scan hashed *line windows* over non-test files. Two things it could
not see: a duplicate whose two copies drifted by a token per line, and anything
in the tests — which is where transcription actually lives, because a new test is
usually a copy of its neighbour.

So: every function body in the tree, normalized (string literals to `S`, numbers
to `N`, comments dropped), reduced to a set of 3-line shingles, every pair scored
by Jaccard overlap. 901 functions, 229 files.

**The production half came back empty**, and that is the finding worth recording.
One pair over the 0.45 floor — `digestDecoder.uvarint` and `.varint`, twelve
lines each, differing in the one `binary.` call that is their entire reason to
exist. Merging those behind a bool parameter would cost a branch on every varint
in a digest parse to save six lines. Left alone. After the twenty-odd transcription
findings taken this sweep, the non-test tree no longer has a mechanical one in
it.

**The test half returned 42 pairs, and 40 of them are correct.** A positive and a
negative case of one rule *should* be near-identical — that similarity is what
makes the one differing line legible. `TestDeleteCleanerBytes` /
`TestDeleteCleanerBytesMessages`, the two leader-epoch checkpoint refusals, the
committed/uncommitted reader pairs: all of them read better as twins than they
would factored into a table.

### What it did catch: three word-forms of one verb, two different operations

`Truncate` drops the log's SUFFIX. `TruncateBefore` drops its PREFIX. They are
different code paths with different lock disciplines — `Truncate` holds
`appendMu` throughout on purpose, `TruncateBefore` deliberately does not. Their
tests were named:

| name form | operation |
|---|---|
| `TestATruncate…`, `TestTruncateBefore…` | as written |
| `TestTruncating…` (3 tests) | `Truncate` |
| `TestATruncation…` (5 tests) | `TruncateBefore` |

Nothing states that mapping, and it is not guessable — "a truncation" reads more
naturally as `Truncate` than as `TruncateBefore`, which is the wrong way round.
The sharpest instance: `TestATruncateUnlinksWithTheSegmentLockAvailable` and
`TestATruncationUnlinksWithTheSegmentLockAvailable` sit 37 lines apart in
`truncate_lock_determinism_test.go`, drive different methods, and differ by two
letters. Both are correct today. The hazard is the next edit — this repo has
already shipped three tests named for a fix that asserted the wrong half of it,
and two adjacent near-homographs are how a copy-paste gets there.

Fixed by one rule, applied to all eight: **the test's name states the method it
calls.** `TestATruncationUnlinksWithTheSegmentLockAvailable` becomes
`TestATruncateBeforeUnlinks…`, `TestTruncatingBelowTheWatermarkClampsIt` becomes
`TestTruncateBelow…`.

Two guardcheck anchors move with them, and this is the part that had to be
checked rather than assumed: the anchors are `^Test…$` regexes, so a rename that
missed one leaves the guard selecting *nothing*. That direction is safe here —
an empty selection means the mutated build "passes", which `run_guard` reports as
NOT COVERED and goes red — but only because `run_guard` requires a failure. The
same mistake in a check that requires a *pass* is the silent one already recorded
under "an empty test selection is a pass". Verified by `go test -list` against
each renamed anchor, not by reading the diff.

No script was added to hold the rule. "A test name must name the method it
drives" is not mechanically checkable in general, and a check that special-cased
the word "truncation" would be a guard against one instance of a class — the
thing this sweep keeps deleting.

## Two more mechanical scans: write-only fields, and one message for many causes

**Fields assigned and never read.** staticcheck cannot see these — a field counts
as used the moment anything mentions it, so a field faithfully maintained and
never consulted stays green forever. 240 unexported fields, two real hits (the
other three were parser artifacts: a local named like a field, and
`x := s.superseded` read as a literal key).

`keyDigest.keyedLen` was parsed, stored, and read by nothing. `newDigestIter`
seeks to `keyedOff` and reads exactly `nKeys` entries; where the section ends is
not needed. The struct doc listed it as part of the location record, so the doc
was asserting a mechanism that did not exist. Deleted — the parse still uses a
length to bounds-check and skip, and that is a local.

`recInfo.hasPid` was the better one, because a test field that is written and
never read is usually a *missing assertion* rather than dead weight, and it was:
`TestCleanDigestMergeEquivalence` checked that records below `StripBelow` lose
their headers and never that records at or above it keep them. A strip that
ignored the floor entirely passed. Falsified before landing (drop `offset <
spec.StripBelow` from both data arms of `classify` → red on seed 2, offset 52
against a floor of 51) and now guarded. The count is floored across all five
seeds, not per seed: a seed whose header-carrying records all land below the
floor makes the assertion vacuous, and failing *that* seed would say nothing
about stripping.

**One error message, several causes.** 207 distinct error texts in the non-test
tree, seven duplicated. Three are correct: two platform-split pairs
(`dirlock_unix`/`dirlock_windows`, the two `index_mmap` files) which are one
error with two implementations, and `"commitlog: negative read offset %d"`,
which is one rule refused identically by both readers.

The other four named nothing. `"stat file failed"` appeared three times —
twice inside `newIndex`, on the stat before pre-allocation and the stat after
it, so an operator reading a log could not tell which. `"open file failed"` was
the index file in one place and the segment log in the other; `"path is empty"`
was `Options.Path` in one and the index path in the other. And `reopenLocked`
wrapped all three of its steps — open backing, positions, index — in one
sentence. Each now names its subject. One bare `return nil, err` on the index
pre-allocation `Truncate` picked up a wrap while there, which is the same defect
with the message missing entirely rather than duplicated.

Neither scan is worth keeping as a script. The write-only check has a real false
positive rate (3 of 5 here) that a human resolves in a minute and a script would
have to encode a Go parser to avoid, and the error-text check has three
legitimate duplicates it cannot distinguish from the bad ones.

## Answering the standing question: why does finding things need a soak run?

The standing complaint is that issues surface under high-intensity soak tests
rather than under something that pressures the flow directly. Most of this
sweep is the answer by example — the strip floor, the COW rewrite counter and
the block-tail open cost were all found by construction, not by running
anything for a long time. But it is worth stating what makes a chaos test
carry its weight, because this repo has two and they were not equal.

A chaos test earns its place when it **retires on danger, not on duration**: it
runs until the dangerous thing has demonstrably happened enough times, and
fails if it never did. `TestChaosAFollowerSurvivesMaintenance` was rebuilt that
way in #264/#265 and counts five — passes run, retention collected, reads
taken, boundaries crossed mid-scan, retention overtaking the follower — plus a
writer check that stays in the list for its failure message alone.
`TestChaosAReadFromThePublishedFloorStartsAtIt` had four, and two of its
dangers were *assumed*:

- **"a truncation ran" is not "a truncation rewrote the boundary segment."** A
  cut landing on a segment's own base drops whole segments and never builds the
  replacement a reader can be holding the original of — which is the entire
  mechanism of the bug this test exists for. Counted on segment IDENTITY, for
  the reason #272 established: an untouched segment already starts at its own
  base, so checking the resulting offset scores a whole-segment delete as a
  rewrite.
- **"a read was taken" is not "a read overlapped a trim in flight."** Two
  booleans cannot establish this — the truncator hammers without a sleep, so
  `trimming` is true at both ends of almost any read, including reads that
  straddled two different trims. A trim SEQUENCE number can: same seq at both
  ends, with `trimming` true at both, means the read ran entirely inside one
  call.

Measured over six runs before choosing floors: rewrites 19–32, overlapping
reads 11706–11789 of ~12030 checked. The rewrite spread is load-dependent —
the low end came from a run sharing the box, and the count of truncations
swung from 33 to 61,497 across the same runs, which is why the floor is on
rewrites rather than on calls. Floors set at 10 and 2000 — under the low end
so a loaded runner does not fail on its own precondition, far enough above
zero that the danger cannot quietly evaporate.

### The falsification that failed, and why it was right to

The first attempt to falsify the overlap floor gave the truncator a 50 ms
sleep, expecting overlap to collapse. **The test still passed**, and that is the
correct outcome rather than a hole. The floor claims *at least 2000 reads ran
inside a trim*; with the sleep that claim is still true, and 2000 overlapping
reads are still 2000 chances at the race. An absolute count is the honest
measure here — a ratio would have failed that run while the test was still
just as dangerous.

The first attempt at the rewrite floor was worse: it made the truncator cut at
`OldestOffset()`, an idempotent no-op. That does stop rewrites, but it also
stops deletion, so `"nothing was ever deleted"` fires first and the run says
nothing about the floor under test. **A falsification has to isolate the thing
it falsifies** — earlier conditions in an ordered check will mask it otherwise.
The isolations that worked: cut at the boundary segment's own base (deletion
proceeds, rewrites go to 0 → fails on the rewrite floor at 120s), and stop
announcing the trim window (everything proceeds, overlaps go to 0).

## A lens not yet run: is every module in go.mod carrying its weight?

The near-duplicate scans had gone dry on the production tree, so this pass
turned the same question on the dependency list instead of the source: not "is
this code duplicated" but "is this *module* here for enough".

### One module for one call

`github.com/dustin/go-humanize` was in `go.mod` for exactly one call site —
`english.Plural(numEntries, "entry", "")` in a leader-epoch parse error. A whole
module, its `go.sum` block, and a supply-chain surface, so that one error string
could say "entries" instead of "entrys". Replaced with

```go
return nil, fmt.Errorf("expected %d entries, got %d", numEntries, len(epochOffsets))
```

which is not a workaround for losing pluralisation: the count is *always*
plural-or-zero here, and the message reads the same for every value it can
take. Dropped from `go.mod` and `go.sum`.

### Two implementations of one format — measured, and kept

The louder finding was that this module links **two snappy implementations**:
`github.com/golang/snappy` behind the `Snappy` codec, and
`klauspost/compress` (already present for S2 and Zstd), which ships a drop-in
`snappy` package. One format, two implementations, one of them removable
without touching the on-disk layout. That is the shape this sweep exists to
delete.

It survives, because the argument for removing it is the one this sweep keeps
being wrong about — a plausible cost rather than a measured one. Three
questions, in the order they had to be asked:

**Does the swap cost a corruption cross-check?** `golang/snappy` *refuses* an
S2 block; `s2.Decode` and `klauspost/compress/snappy` both accept it. So the
drop-in is strictly the more permissive decoder, and the strictness looks
load-bearing — a block header carries **no checksum**, so a flipped codec byte
reaches the decoder entirely unverified.

It is not load-bearing. `decodeBlock` compares the decompressed length against
the header's `uncompressedLen` and refuses a mismatch, and the per-record frame
CRCs sit under that. A lenient decoder that reads an S2 block labelled Snappy
produces the *correct* bytes at the correct length — a silent repair, not a
silent corruption — and one that produces different bytes is caught twice over.
**The cross-check is redundant, and is not a reason to keep the dependency.**

**Then what does the swap actually cost?** Ratio and encode time, which is the
reverse of the usual klauspost result and the reason this needed measuring at
all. Over `sampleMessageSet` batches:

| batch | raw | golang/snappy | klauspost | size delta | encode g → k |
|---|---|---|---|---|---|
| 10 | 5430 | 633 | 642 | +1.4% | ~0 → 5.1µs |
| 100 | 54480 | 3732 | 4130 | +10.7% | 8.4µs → 22.5µs |
| 1000 | 546880 | 37421 | 39114 | +4.5% | 85µs → 175µs |
| 5000 | 2746880 | 186414 | 216543 | **+16.2%** | 401µs → 776µs |

Decode is a wash (slightly faster at the large sizes). So the swap pays up to
16% more bytes on disk and roughly 2x the encode time, on the codec whose
entire purpose is a cheap ratio, to remove one frozen zero-dependency module.

**Verdict: keep it, and say so where the tidy-up would start.** The measurement
now lives in a comment on the `Snappy` codec constant, not only here — a future
reader meets "two snappy implementations" at the import and needs the answer
there. Recorded as a negative so the next sweep does not re-open it.

`natefinch/atomic` was checked in the same pass and is legitimately used.
