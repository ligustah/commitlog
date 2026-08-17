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

## The interface and abstraction surface

The third lens: eight interfaces in the production tree, asked one question each
— how many implementations, and does anything need the indirection?

### The one that mattered: the interface IS the reachable API

`New` returns `CommitLog`, and `commitLog` is unexported. So that interface is
not a convenience over the concrete type — it is the *entire* API reachable from
outside the package, and it is a hand-maintained transcription of the concrete
type's exported method set with nothing keeping the two in step.

The suite cannot notice the drift, and the reason is structural rather than an
oversight: `setup` and `setupWithOptions`, the helpers behind essentially every
test in the repo, return `*commitLog`. Tests hold the concrete type. So

- an exported method **added** to the struct and forgotten in the interface is
  callable from every test here and reachable from nowhere outside, and
- a method **deleted** from the interface still compiles — the struct keeps it,
  the helpers keep calling it through the concrete type, and `go build ./...` is
  clean.

The second one is a way to break the published API of the library without
breaking anything visible, and it was verified rather than argued: removing
`UnreferencedObjects` from `interface.go` builds the whole module and passes
every test that exercises it.

The two sets agreed exactly, 40 and 40, when this was written. That is the
discipline having held, not anything enforcing it — the same shape this sweep
has deleted repeatedly elsewhere. Now checked by reflection (`NumMethod` on a
non-interface type reports exactly the exported set), plus guard 176, whose
mutation is the real refactor rather than a contrived one.

### Was the 40-method interface itself the problem?

The obvious follow-up — forty methods is too many — is answered downstream, and
the answer is no. durable_streams defines its own `StreamLog` of seven methods
and asks for the rest at the call site with a type assertion, reasoning in its
own doc that `commitlog.CommitLog` "is the right size for what it is … the wrong
size for a CONTRACT, because a contract is a bill for whoever implements it."

That is the boundary in the right place: commitlog's interface is a **return
type**, and the consumer defines the contract it wants to implement against. The
completeness check above is therefore the correct response — keep the return
type honest — rather than shrinking it.

### Negatives from this lens, recorded

- **No pure-delegation wrapper types.** A scan for types whose every method is a
  one-line forward to an inner field found none, and none with even three such
  methods.
- **`contextReader.segmentBounds` is not a stub tax.** One method of the two has
  a single implementation, which looks like the forty-stubs problem in
  miniature; it is not, because `segmentCursor` provides it once and both
  readers embed it.
- **The encoder trio is live.** Per-method call-site counts (not staticcheck,
  which calls an interface-satisfying method "used") show every `packetEncoder`
  and `pushEncoder` method has a production caller. `encoder` itself has one
  implementation and two call sites both passing `*Message` — a real, if small,
  abstraction over one type.
- **`digestByteReader` is the textbook case, not a finding.** `io.Reader` plus
  `io.ByteReader`, named by the consumer, with two genuinely different
  implementations behind it (a `bytes.Reader` over an in-memory keyed section, a
  `bufio.Reader` streaming a sidecar off disk). This is what the other seven are
  being measured against.
- **Exported symbols with no downstream consumer are mostly not dead.** Scanning
  1549 `.go` files across durable_streams and sqlcdc for every exported
  commitlog symbol leaves nine unreferenced, and eight are legitimate: five
  error sentinels (a library must export what a caller may need to test for),
  and `CopyTier`, `IncludeControl`, `SkipSuperseded` — capabilities, and a
  capability whose consumers have not arrived is still a capability.

  The ninth, `ReadFileWithRetry`, looked like the real find: an internal
  Windows-race file helper whose two siblings from the same unification
  (`openWithRetry`, `renameWithRetry`) are unexported, with no downstream
  caller. Unexporting it would have been wrong. It was exported deliberately as
  the twin of `AtomicWriteFileWithRetry`, which durable_streams **does** use —
  and removing the read half would recreate exactly the asymmetry commitlog
  added it to fix ("this package already carried the knowledge on the write
  side; the read side never got it").

  Which turned the finding around and pointed it downstream: durable_streams'
  `PutStreamSidecar` writes through the retrying writer, citing one-run-in-twelve
  sharing violations, and `GetStreamSidecar` reads that same file with plain
  `os.ReadFile`. commitlog's own `GetSidecar` uses the retrying read. Raised with
  that repo's owner rather than fixed here.

### Searching for coverage by NAME finds nothing when the test asserts a consequence

The one thread in this lens that looked like a hole and was not, kept because
the reason it looked like one generalises.

`segmentBacking.StreamPays` is a two-arm cost predicate — `localBacking` returns
false, `storeBacking` returns true — and it decides whether a segment scan builds
a `scanStream`. It stood out because it is the only internal interface method
with a production caller and **zero test calls**, and grepping the test tree for
`StreamPays`, `scanStream` and `newScanStream` found only two tests that build a
stream directly, bypassing the decision entirely. The one test that does drive
the decision, `TestStreamedScanReturnsTheSameRecords`, asserts *equivalence* — a
streamed scan returns what a local one did — which is satisfied whether or not
streaming happened. On that evidence the decision looked uncovered.

It is thoroughly covered. Mutating `storeBacking.StreamPays` to `false` turns
**two** tests red: `TestScanningAnOffloadedSegmentCostsOneRequest`, which
requires exactly one stream and zero ranged reads, and `TestPrefixReadCostProfile`.
Neither names `StreamPays` or `scanStream` anywhere, because both assert on the
**consequence** — the request count a store is billed for — rather than on the
mechanism that produces it.

That is the lesson, and it cuts against how the rest of this sweep has been
searching: **coverage found by grepping for a symbol's name is a lower bound,
and a bad one for anything whose whole purpose is a cost.** A test that asserts
"this costs one request" is exactly the right test for a performance decision
and is invisible to a name search. The falsification is the only instrument that
answers the question — the same rule already recorded for chaos-test floors, now
pointed at coverage claims.

Recorded as a negative: the streaming decision needs nothing.

## Two lenses that came back empty, and are worth the sentence

### Migrations and back-compat before v1

The standing task names this explicitly, so it was re-run rather than assumed
closed. Grepping the production tree for `backward|legacy|deprecat|migrat|for
older|pre-v|compatibilit` returns twelve hits and **not one** is a compatibility
branch: most are the word "backwards" describing a direction, and the rest are
records that the branch already went —

- `block.go`: "Clean cutover: pre-version segments are not supported."
- `block_table_local.go`: "the library is pre-v1 with nothing to migrate."
- `manifest.go`: "a store written by an older build is re-offloaded, not converted."
- `descriptor.go`: V0 "went in v0.82.0 rather than becoming permanent."

The sharper form agrees: there is exactly one `BlockFormatVersion`, exactly one
`descriptorFileV1`, and both parses refuse anything else rather than branching on
it. The version fields that remain are about *future* formats — a file from a
newer writer should say "this file is newer than me" instead of naming a field
it does not know — which does not depend on any past format staying readable.

Nothing to do. Earlier passes took it all.

### Sibling asymmetry: does the read side know what the write side knows?

The lens that found the durable_streams sidecar defect this session, turned back
on this repo. Pair functions by an antonym in their names (read/write, get/put,
load/save, parse/render) and diff which safety properties each body actually
has: retry, fsync, atomic write, validation, CRC.

Every difference it reports is the correct one — `writeDescriptor` and
`PutSidecar` write atomically and their readers do not, which is what atomicity
*is*. On the property that matters, retry, the pairs match.

The precise form confirms it. All three files written through
`AtomicWriteFileWithRetry` are read through a retrying reader:

| file | write | read |
|---|---|---|
| descriptor | `AtomicWriteFileWithRetry` | `openWithRetry` |
| leader epoch checkpoint | `AtomicWriteFileWithRetry` | `ReadFileWithRetry` |
| sidecar | `AtomicWriteFileWithRetry` | `ReadFileWithRetry` |

The reads that stay plain are the DERIVED sidecars — the local block table and
the key digest — and both callers return "not ok" on any error at all and
rebuild. That is the right answer for a regenerable file: a sharing violation
costs a rebuild, not a failure, and paying a five-second retry budget for a file
you can reconstruct is the trade #236 already rejected.

So the asymmetry this lens hunts exists downstream and not here. Recorded as a
negative — and the lens itself is worth keeping, because it is mechanical and it
found a real defect the first time it was pointed anywhere new.

### A published number that the obvious check would "correct" to a wrong one

The lens behind #220 (a CHANGELOG citing a guard count the release grew past)
and #262 (a doc naming a signature that no longer exists), re-run over numbers
this repo publishes about itself.

Comments in the Go tree are clean: every comment that names a duration or size
constant — `waitedOnRetryBudget`, `tickWriteRetryBudget`, `maxSyncWindow`,
`maxDescriptorBytes`, the two prefix-read coalesce sizes — states a value
consistent with the constant it describes.

The guard count is where it got interesting, in the opposite direction from
#220. `grep -c '^run_guard ' hack/guardcheck.sh` answers **164**, and the
CHANGELOG says 176. The CHANGELOG is right: there are three registration
spellings — `run_guard`, `run_guard_windows` (11 calls) and `run_guard_pair`
(1) — and the obvious grep misses two of them.

So the hazard here is not a stale number, it is a **checking method that makes a
correct number look stale** and invites replacing it with an undercount. Noted
where the counting happens, in `guardcheck.sh` beside the helpers, rather than in
this document: the script prints its own count, and that is the answer.

Same shape as [[a transcribed rule grows a dissimilar copy]] seen from the
measuring end — a second and third spelling of one registration, and a count
that only knows about the first.

### The lock-order lens, and why its first two versions were worthless

This package documents three lock orders — `cleanMu` before `mu`, `appendMu`
before `mu`, `mu` before `mapMu` — and has already paid for getting one wrong
(a lock-taking call added below `setupIndex`, which has callers on both sides of
the segment write lock, hung the whole suite). Nothing checks them, so this
looked like a lens worth building.

The orders hold. What is worth writing down is how badly the first two attempts
lied about that.

**Version one, name-based: 62 "violations", all false.** Two reasons, both
embarrassing and both general:

- `append`, `close`, `len`, `make` are **Go builtins**, and a call-graph built
  by matching `name(` resolves `append(...)` to whatever method in the package
  happens to be called `append`. Half the report was "holds mu, calls append()".
- **Five different types have a field named `mu`** — `commitLog`, `index`,
  `indexCache`, `leaderEpochCache`, `segment`. A rule about *`commitLog`'s* `mu`
  applied to every one of them at once.

**Version two, scoped to the receiver type that owns both locks and with
builtins removed: 2 candidates.** Both still false, for a third reason: the scan
treats "acquires L somewhere in the body" as "holds L at the call site".
`cleanPass` takes `cleanMu` first and `mu` after it, which is the documented
order; `closeIndex` takes `mu` first and `mapMu` inside, releasing it before the
call that was flagged.

So: **the documented orders are respected, verified structurally**, and the
lens is not worth keeping in this form. A real one needs types and a
lock-held-at-this-point analysis, which is a type checker's job and not a
regex's. Recorded so the next pass builds it properly or not at all, rather than
re-deriving 62 false positives and either chasing them or — worse — concluding
from a scoped run of 2 that the codebase is nearly clean, which would have been
the right answer reached by an instrument that could not have told the
difference.

## "All checks against the catalog, never the disk" — applied inside commitlog

One of the standing questions is why anything is checked against directories on
disk when a catalog exists. Applied here, the lens is: **does any code path
answer a question by consulting the filesystem when an authoritative structure
already knows?**

Grepping `os.Stat`/`os.ReadDir`/`filepath.Glob`/`Walk` gives ~80 hits, of which
**five are non-test**, and all five turn out to be legitimate — which is the
useful half of the result, because it is not what I expected going in.

- `commitlog.go:796`, `index_cache.go:78`, `segment_store.go:350,361` — a
  filesystem-backed store and a cache directory enumerating themselves. The
  disk *is* the catalog for these; there is nothing above them to ask.
- `util.go:239` (`removeAllExcept`) — a deletion enumerating what to delete.
- `descriptor.go:386` (`logIsNew`) — this one *looks* like the defect and is
  not, and its doc already says why: it asks the store when there is one and
  the directory only when there is not.

`exists()` has four production callers. Three are a creation refusal and two
deletion checks, where the disk is the only authority that can answer. The
fourth, `clean_join.go:253`, is the interesting one:

```go
wantDigest := exists(digestPath(first))
```

That reads as the defect — a *configuration* question ("does this log keep key
digests?") answered by a file's existence. **The hypothesis was wrong, and the
refutation is worth keeping.** I assumed segment join was reachable only from
the compaction arm, which would have made `l.Compact` the catalog answer.
`clean.go:760-764` says otherwise, deliberately:

> Its own stage, after both branches, because it belongs to both: compaction
> and consolidation are the two arms of `if l.Compact`, and a join placed in
> either would only ever reach half the logs.

So a retention-only log reaches this line, has genuinely never had a digest, and
`exists()` is the *more* accurate predicate — `l.Compact` would answer "should
this log have digests in principle", where the code needs "does this segment
have one". **A negative for the lens; the whole finding is downstream of it.**

### The finding the refutation produced

Chasing "so who *does* write a digest" gives the fact the lens was actually
worth running for: **`loadOrBuildDigests` (the compact cleaner) is the only
thing that ever persists a `.keys` sidecar.** Therefore on a log with `Compact`
disabled, no sealed segment has a digest and none ever will. Not a warm-up — a
steady state.

And `prefix_source.go` handled a missing digest by *building one*:

```go
if d == nil {
    if d, err = buildKeyDigest(seg, newBlockCache()); err != nil { ... }
}
```

`buildKeyDigest` reads every record in the segment **and** holds a
`map[string]*keyRecs` over every distinct key in it. The digest was then thrown
away, and the offsets it named were read a **second** time. So the "optimised"
prefix path cost strictly more than the scan-and-filter it was avoiding, on
every read, forever.

The sharpest version of it is a **sibling asymmetry** (the lens from earlier in
this sweep, used again): the compact cleaner bounds this exact call at two
concurrent builds, and says why —

> 10 concurrent ~40MB maps measured >1GB on a 12h soak … peak memory is what
> matters, and peak memory is a function of this number alone.

— while the read path called the same function with **no bound at all**.
`PrefixReadConcurrency` sounds like the bound and is not: it governs record
*reads*, not digest builds. The number in flight was however many readers
happened to be doing prefix reads.

**Why nothing caught it.** Correctness was covered well — two tests run their
whole comparison with the sidecars deleted, and the fuzz target catches a
missing CRC check on this route. *Cost* was covered by tests that were
measuring the wrong path: `costLog` and `offloadedPrefixLog` both set
`Compact: true` **with `DisableAutoClean: true`** and never called `Clean`, so
their segments had no digests either, and every number they compared was
measured on top of an unmeasured full rebuild scan. A test can be pointed at
the path it names and still be standing on a different one.

### A second bug that only became visible once the first was fixed

With the scan in place the new cost test read **exactly 2× the sealed segment
count**. The traversal visited every segment twice: `pop` walks `p.next` back to
the last record it served — correct, since that is where a *resumed read* must
continue — so a fully drained segment still looks unfinished to the search loop,
which re-plans it to discover it has nothing left.

With a digest that second visit is nearly free, which is exactly how it survived
in a path that had been profiled. **A cheap redundant operation is invisible
until something makes it expensive.** The fix separates the two offsets
(`servedThrough` vs `next`), and the digest path saves a redundant plan per
segment as a side effect.

The new tests assert **equality**, not a bound: "at most one pass per segment"
would still be satisfied by a build-then-fetch that happened to coalesce into a
single run, so it would not notice the old shape coming back.

### The same lens, swept: are there other bounded-here/unbounded-there pairs?

Worth asking once the first one was found, and the answer bounds the question
rather than opening it. The package has exactly **two** production concurrency
bounds — `compact_cleaner.go:671` (`buildConc`, the digest builds) and
`prefix_read.go:150` (`fetchRuns`, the record reads). Both are the ones already
under discussion, and there is no third expensive operation with a semaphore in
one caller and none in another. The asymmetry found above was the only instance,
not the first of a family.

### Back-compat sweep, re-run over the format layers

The standing brief says migrations and backwards compatibility should not exist
before v1. Re-run over the format-carrying code, the answer is that they mostly
already do not:

- `block.go` and `block_table.go` both refuse an unrecognised version outright
  — *"Clean cutover: pre-version segments are not supported"* — and
  `blockformat_test.go` pins the refusal in both directions (a newer version and
  a pre-version byte). Nothing to remove.
- `block_table_local.go:80` explicitly retired its own compatibility argument
  already, saying so in place.
- The descriptor's v0 read support went earlier in this sweep.

The one that reads like a survivor and is not: `Options.Compression`'s
*"byte-for-byte compatible with logs written before compression existed;
existing segments keep whatever format they were written in."* That is a live
capability, not a migration path. `compress.None` is the zero value and a
perfectly ordinary current setting, and a mixed-format log arises from a caller
**changing** the option on a running log, not from history —
`TestTurningCompressionOnLeavesExistingSegmentsRaw` names the distinction
exactly: *a retune, not a migration*. Deleting the "detected as raw and stay
raw" behaviour would break a supported operation today.

Recorded because the grep for `compat|legacy|migrat` puts these four in one
list, and three of them are already dead while the fourth only looks it.

### The measure and the thing measured: a budget in the wrong currency

The digest-less prefix read (above) made one path expensive and, by doing so,
made a long-standing redundancy legible. Re-reading the same file for *other*
places where a decision is priced in units the code does not actually pay in
turned up a second, larger one — in the same function's neighbour.

`planRuns` decides where one contiguous read ends and the next begins:

```go
if cursor < 0 || e.Position-cursor > coalesce {   // start a new run
```

`e.Position` is an index position, and index positions live in the **logical**
(uncompressed) byte space (`block.go:11-14`). On a raw segment that space *is*
the file, so the difference is exactly the bytes a split would avoid, and the
whole economic argument at the top of `prefix_read.go` — *read a contiguous span
and discard the frames between, or address each record and pay a request each* —
holds as written.

A block-compressed segment offers neither option:

- **A record cannot be addressed.** `blockCopyIntoCache` decodes a whole block
  on a cache miss; there is no smaller unit of transfer or of work.
- **The bytes between two records in one block are not separately transferred.**
  Reading through them costs nothing beyond the block already being fetched.

So a split inside a block cannot save anything — and it *adds*, because
`fetchRuns` gives every run its own single-entry `blockCache`, so the same block
is pulled and decompressed once per run that touches it, concurrently.

That is not a rounding error. `cleanBlockTarget` is 256KB uncompressed and the
tier coalesce default is 4KB, so on a tiered block-compressed object almost every
gap split, and each split was an extra round trip for bytes already in flight.
Measured, ~50KB blocks with a hit every ~6KB, 120 hits:

| budget | requests | bytes |
| --- | --- | --- |
| none | 120 | 314799 |
| 4096 (the default) | 120 | 314799 |
| 16384 | 3 | 7827 |

Both axes forty times worse at the default. The setting was not mistuned, it was
**inverted**: its own documentation promises fewer bytes for more requests, and
here the small budget bought more of both.

The fix is one line of arithmetic, and it collapses the special cases rather than
adding them. Measure the gap between the *block* holding the previous record's
last byte and the *block* holding this one's first:

```go
split = blk.physStart-(prevBlk.physStart+prevBlk.physLen) > coalesce
```

Same block gives a negative gap, so it never splits. Adjacent blocks give zero,
so they never split — correctly, since nothing lies between them to skip. Blocks
entirely between two hits give the sum of their physical lengths, which is
exactly what a split avoids. No `if sameBlock` branch is needed; the right
measure produces the right answer at every shape.

**Why nothing caught it.** `TestPrefixReadCostProfile` sets no `Compression`. Its
fixture is raw, so `Position` is physical and the model is coherent — and the
"MEASURED" ~4.4KB tier default it established was measured somewhere the defect
cannot occur. This is the same trap as `costLog`'s missing `Clean` earlier in
this sweep, one layer out: there the fixture had no digests and so measured the
wrong *route*; here it has no blocks and so measures the wrong *currency*.

**The lens, stated generally.** When a knob is denominated in a unit, ask what
the code below it actually transacts in. A byte budget above a block-structured
layer, a record count above a batch-structured one, a time budget above something
that only yields at checkpoints — in each case the knob can be set to a value
finer than the layer's granularity, and there the knob does not merely stop
helping, it starts costing. The tell is a fixture: if the cost test that
justified the default cannot produce the granularity, the default was never
measured against it.

### A guard can lose its coverage to the fix in the next commit

Worth recording separately because it is the failure guardcheck exists for and it
fired for real. The digest-less scan path landed; CI's `guard coverage` job came
back with:

```
prefix-read CRC   NO COVERAGE — ^TestKeyPrefixRefuses passed without it
```

`TestKeyPrefixRefusesRecordsThatFailCRC` and its tiered sibling never ran a
clean, so their segments had no digests — and the moment a digest-less prefix
read stopped going through `collectRun`, both tests moved to the scan path.
Their names, their guard and their own doc comment (*"It is — collectRun is
reached for both"*) all still said `collectRun`. The check they were named for
could be deleted without either of them noticing.

Three things came out of it:

1. Both fixtures now clean, so they exercise the route they are named for.
2. The scan route got its own test and its own guard — separate code with no
   shared helper, so the old guard never spoke for it.
3. The old guard's selector, `^TestKeyPrefixRefuses`, was tightened to name its
   two tests. A prefix selector that also matches tests which *cannot* see the
   guarded line still reports `ok` as long as one match fails — so it survives
   exactly until the last correctly-placed fixture drifts, and then reports
   nothing. Related: [[an-empty-test-selection-is-a-pass]], where the same
   selector-shaped hole made a guard vacuous rather than merely loose.

#### The same lens, swept: every knob against its layer's granularity

Having found one budget denominated in a unit the layer below does not transact
in, the obvious question is whether it is the only one. Walking the whole
`Options`/`CleanSpec`/`Tier` surface, most knobs **name their own granularity in
the doc**, which is the thing that keeps them honest:

- `CleanRewriteBudget` is a duration over a segment-at-a-time rewrite, and says
  so: *"At least one rewrite always proceeds, so even a pathologically small
  budget drains debt."* The granularity is acknowledged and handled.
- `LocalRetentionAge` is a duration over whole-segment descent: *"Only whole
  sealed segments move, so a segment lives locally until its NEWEST record is
  past the horizon."*
- `PrefixReadConcurrency` names its unit outright: *"The unit is a RUN — a span
  of wanted records read contiguously — not a segment."*
- `MaxLogMessages` is a count over a segment-granular walk, which is the same
  unit `applyTotalLimit` drops in.

The byte budgets are the ones that name nothing. `MaxLogBytes` is documented as
*"Retention by bytes"*, `Tier.MaxBytes` as bounding *"the segments whose bytes
are in THIS tier"*, and `LocalBytes()` as *"how many bytes of log data this log
holds on LOCAL disk — what copying it elsewhere would cost."* All three sum
`(*segment).Position`, which is the **logical, uncompressed** extent — see #285.
`physPosition` is the real one, is tracked, and is carried through
`offloadMeta.PhysPosition` from a size the upload measured.

Two directions of wrongness, not one:

- **Large batches compress**, so the file is smaller than the logical extent and
  `Position` OVERSTATES the disk. Retention's backwards walk therefore reaches
  the limit early and drops segments the budget had room for — silent data loss
  dressed as policy — while `LocalBytes` reports the log as bigger than a copy of
  it would be. Measured in this session's block fixture: 52KB logical per block
  against ~2.5KB on disk, a factor of twenty.
- **Small appends do not compress** — `compressMinBlock` stores anything under
  4KB raw — but each still carries an 11-byte block header, so the file is
  *larger* than the logical extent, `Position` UNDERSTATES the disk, and the log
  overruns the limit it was given.

Worth writing down because I got it backwards on the first pass: the intuition
"compression means we keep more than we meant to" is the wrong way round. The
budget is a ceiling on a running total, so overstating each term makes the total
hit the ceiling SOONER. An overstated measure deletes more, not less.

`TestALogsLocalBytesAreTheBytesOnDisk` already asserts exactly the right thing
(`LocalBytes() == diskLogBytes(dir)`, measured independently by walking the
files) and cannot see either case, because its fixture sets no `Compression`.
The assertion was never wrong; the fixture could not reach the states that
falsify it. That is the third time in this sweep — after `costLog`'s missing
`Clean` and the CRC tests' missing digests — that the defect was not a missing
test but a test standing somewhere the defect cannot occur.

Also worth recording against the standing question *"why do we always need
high-intensity soak tests to find issues?"*: neither of today's two findings came
from a soak. Both came from reading one function against the layer beneath it and
then writing a direct, seconds-long cost assertion. The soak was never going to
find them — a soak measures throughput, and both defects are invisible in
throughput terms while being forty times off in requests and bytes.

### #285, resolved: `PhysicalSize`, and the one budget that keeps `Position`

The fix is a second accessor rather than a change to `Position`, because both
numbers are wanted and the interesting part was deciding which callers want
which. `(*segment).PhysicalSize` returns `physPosition` in block mode and
`position` otherwise — it has to branch, because the RAW append path never
advances `physPosition` (it has no second number to track, every byte written
being a byte of logical extent), so an unconditional read would return whatever
the file measured at open.

The dividing line, stated once and cited from all four sites:

> **A budget over a RESOURCE is denominated in the bytes that are there. A budget
> over the RECORD STREAM is denominated in the extent.**

- `MaxLogBytes`, `Tier.MaxBytes`, `LocalBytes()` — resource. Disk, a store's
  bill, a transfer. All three now sum `PhysicalSize`.
- `MaxSegmentBytes` — **not** a resource, and deliberately left on `position`.
  What a roll protects is everything sized by the segment's extent: the offset
  span it covers, its index, the working set a compaction pass has to hold. None
  of that shrinks when the bytes compress. `clean_join.go`'s `JoinBelow` has to
  agree with it and so is logical too, which was checked and is correct.

This is the same lens as the previous section — a budget denominated in a unit
the layer below does not transact in — applied one layer down. There it was a
byte budget above a block-structured *transfer*; here it is a byte budget above
block-structured *storage*.

**What made it invisible** is the recurring shape, now at four instances:
`TestALogsLocalBytesAreTheBytesOnDisk` asserts precisely the right thing, by an
independent measure (it walks and stats the `.log` files rather than asking the
segments again), and could not fail, because no fixture in the byte-retention
suite sets `Compression`. The four new tests are built the other way round: the
budget is placed *between* the two measures, so the choice of measure is the
whole difference between "delete nothing" and "delete most of it" rather than a
question of margin. A budget above both numbers passes either way; one below both
deletes either way; only a fixture standing between them has an opinion.

Three guards, one per call site, because each is separately removable and each
fails differently — and a fourth test (`...StillDeletes`) so that "never delete
anything" cannot satisfy the first three.

### #287: the length between two length checks

Found by following the same thread one more step — after `Position` vs
`physPosition`, the question "what else is a number here trusted as a measure of
something it does not measure?" leads straight to `RemoteIndexCache.fetch`, where
`store.Size(key)` steers a download and is never checked.

Two checks already stand in that loop, both added deliberately and both with
guards:

- `(0, nil)` mid-download is named as the `io.ReaderAt` contract breach it is,
  rather than retried forever.
- an object that ends before the size the store just reported is refused, because
  "a partial download is not a smaller index, it is bytes newIndex would map and
  read as a whole one."

Zero is the length neither can see. The loop never runs, so there is no read to
breach a contract; and the second check asks `off != size`, which compares the
download against **the same number that is wrong**. Two checks, adjacent, with
one state uncovered between them — the shape recorded earlier this sweep for the
four recovery mechanisms, arriving again in a much smaller function.

The consequence is the default-arm lens, exactly:

> `newIndex` pre-allocates when it finds an EMPTY file — the arm a genuinely
> FRESH index takes. An empty download is not distinguishable from a new index,
> so it gets the same treatment.

Ten megabytes of zeroes, mapped, and read as that segment's table. A seek into it
does not fail; it *answers*. And `cachedIndex.bytes` is set from the same zero, so
the entry is invisible to the cache's byte budget: `total` never grows for it, it
can never be evicted for size, and the disk fills under a budget that reads as
empty.

The fix is four lines, and the argument for where it goes is the whole finding:
it belongs **before** `os.Create`, not in the loop, because the second half of the
damage is a file existing at all. `readStoreDescriptor` has had `if size <= 0`
since it was written, for the identical reason, one reader over — so this is also
another instance of a rule stated in one place and not transcribed to its sibling.

### #288: the fifth reader, and the one that panics

The obvious follow-up to #287 is not "is there another one like it?" but the
mechanical version of that question: `store.Size` has five call sites, so read all
five. They are `descriptor.go:177`, `manifest.go:377`, `index_cache.go:190`,
`copy_tier.go:129`, and `segment.go:723`. Three refuse a bad answer. `copy_tier`
does not allocate from it — it hands the number to a copy that streams. The fifth,
`(*segment).fetchBlockTable`, allocated it:

```go
size, err := s.store.Size(s.blocksKey)
...
buf := make([]byte, size)
```

`decodeBlockTable` is thorough — magic, version, an EXACT length, a CRC, a
per-block minimum — and none of it can help, because every one of those checks
runs on `buf` after `buf` exists. That is the general form worth keeping:

> the checks that matter for an allocation are the ones that happen before it.

Two bounds, because the two ends fail differently and only one of them is even an
error. A **negative** size is `makeslice: len out of range` — not a wrong answer, a
panic, in the caller's process, thrown by a library. Any store that can return a
negative int64 can crash a process that merely opened a tier. A **large** size is
taken quietly: the object is allocated in full before a single byte is parsed, so
a remote store decides how much of this process's memory it gets, and the refusal
that eventually comes from `decodeBlockTable` arrives after the damage.

The upper bound is derived rather than picked, the argument `maxDescriptorBytes`
already makes: the table is fixed-width, and every block occupies at least
`blockHeaderLen` physical bytes in the object, so a segment of `physPosition`
bytes describes at most `physPosition/blockHeaderLen` blocks. Nothing that could
have decoded is refused by it — a size past the ceiling fails the exact-length
check anyway. Only *when* changes, which is the entire point, and is why the test
for the upper bound asserts on `runtime.MemStats.TotalAlloc` and not on the error:
the error is there either way.

**What made it hard to test** is worth recording, because it is a property of the
code and not of the test. The state where a store's size reaches this allocation
is `blocksPending`, and it is set in exactly one place: `openOffloadedSegment`. A
segment that offloads inside this process keeps the table it already built, and a
reopen with the index kept locally resolves the table during `setupIndex`. Only a
reopen with a `RemoteIndexCache` — option 2 — leaves the table for the first read
to fetch. So the fixture has to build a log, offload it, close it, and reopen it
through a cache; there is no shorter path to the only arrangement in which the bug
is reachable, and the first draft of the test failed on its own precondition
(`no offloaded segment with a table still to fetch`) rather than on the code.

### #289: an allocation sized by the ceiling instead of the contents

The `store.Size` thread ends at #288, but the question underneath it does not:
*what else is allocated from a number that is not a measure of what is there?*
Grepping `make([]byte, 0, ` answers it in one screen. Every site in the package
sizes by `len(x)` — the thing itself — except one:

```go
out = make([]byte, 0, maxBytes)   // ReadMessageSet
```

`maxBytes` is a **ceiling on what the caller is willing to receive**. It carries
no information about how much there is to send, and on the path this function
exists for the two are as far apart as they get: a follower that has caught up to
the head asks for its whole fetch size on every poll and is answered with the one
frame that just landed. Megabytes reserved, hundreds of bytes returned, once per
poll per stream.

Not a correctness defect — nothing observable changes — which is exactly why it
survived a package that has otherwise been swept twice. It is a *steady-state*
cost on the replication path, and the fix is the bound that was already sitting
there: `seg.Position() - start` is the most the loop could possibly append.

Two details worth keeping.

**Which measure.** `Position`, not `PhysicalSize` — the distinction #285 put in
place. This bounds the RECORD STREAM the loop accumulates (logical framing bytes,
which is what the scanner yields), not a resource; on a block-compressed segment
`PhysicalSize` is the compressed object and would under-size the buffer by the
compression ratio. Getting a *hint* wrong low is only a growth, so this would
never have failed — which is precisely the kind of wrong that stays wrong.

**How the tests see it.** The assertion is `cap()` of the returned slice, because
`out` IS the allocation — `ReadMessageSet` returns it — so the number under test
is observable exactly, with no sampling and no flake. That matters more for the
second half than the first. The fix is a `min()` of two bounds and each side is
separately droppable, but the *budget* side is invisible to `len()`: the
truncation is performed by the loop's own `maxBytes` break, so a version that
dropped the clamp entirely would still return the right bytes — after allocating
the whole segment to do it. `len()` cannot tell those apart; `cap()` can.

The counter-example, in this same repo, is `loadKeyDigest`: it verifies a CRC over
the whole file first, then bounds *every* count it reads — `nHdrs > 64`,
`keyedLen > len(body)`, `nUnkeyed > len(body)`, `nControl > len(body)` — before
allocating from any of them. That is the discipline #288 was missing and #289 is
the last place in the package that departs from.

### #288b: the rule that had been written four times and checked none

The thing worth fixing after #288 is not the missing bound. It is that the bound
was missing *and nothing could have said so*.

Three readers had it. `readStoreDescriptor` refused a non-positive size and
bounded the rest by `maxDescriptorBytes`; `readTierManifest` refused a
non-positive one; `RemoteIndexCache.fetch` grew its own in #287. Each was written
separately, for the same reason, by someone who had just been bitten by it —
which is the definition of a rule that lives in people rather than in the repo.
`fetchBlockTable` arrived later and simply did not get a copy. There was no
decision to skip it and no place where the omission was visible.

This is a shape the repo has already met twice, and both times the answer was the
same. `atomicwrite.sh` exists because the rule "finish through the retrying
wrapper, not the library" lived in a doc comment *listing the callers it covered*
— and, as that script's header puts it, "a list written out by hand cannot see a
caller that never arrives." `cowsegments.sh` exists for the same reason one layer
over. So `hack/storesize.sh`:

> per function, in non-test files: if a variable is assigned from a `.Size(` call
> and later reaches `make(`, at least one comparison of that variable must stand
> between the two.

Three deliberate limits, each of which is the interesting part.

**It checks the allocation, not the call.** `copyObjectAs` is the fifth reader of
a store's `Size` and is entirely correct without a bound, because it never
allocates from it — the size is handed to a streaming `Put`. A checker that
policed the *call* would have to carry an exception list, and an exception list is
the hand-written list this exists to replace.

**It requires that a bound exists, not which one.** The right ceiling differs per
reader: `maxDescriptorBytes` for one, a value derived from the segment's physical
extent for another. A linter that tried to pick the ceiling would be wrong more
often than the code it checks. Requiring the *shape* is the part that generalizes;
guardcheck is what holds each specific bound in place.

**It refuses to pass on an empty selection.** Finding zero size-to-allocation
flows exits 1 as a HARNESS ERROR rather than 0 as a clean run — the lesson from
the `go test -run` guard that selected nothing and read as a pass, and from
`layercheck.sh`'s `| while` that set its flag in a subshell and reported every
violation with exit 0. This script reads its findings from a file for exactly
that second reason.

It currently sees three flows and passes. The value is entirely in the fourth,
which does not exist yet.

### #294: the budget whose doc calls its own error acceptable

`(*segment).MessageCount` returns `Index.CountEntries()` for a raw segment — the
index is dense there, so that is exact — and `lastOffset - firstOffset + 1` for a
block-compressed one, because the block index is sparse. Its doc says:

> After compaction a compressed segment stores one message per block and may have
> offset gaps, in which case this is an upper bound — acceptable for the
> retention heuristic that consumes it.

The consumer is `applyTotalLimit`, and it is not a heuristic. It is the same
budget walk `MaxLogBytes` uses, so #285's direction rule applies unchanged:
**overstating each term makes the running total reach the ceiling sooner, so the
walk stops earlier and deletes MORE.** "Upper bound" and "over-deletes" are the
same sentence, and only one of them sounds acceptable.

Measured, on a log with `Compression: Snappy` and `Compact: true`, 400 records
where a quarter use a key that appears once (survivors) and the rest churn eight
hot keys:

```
AFTER  seg base=0   first=0   last=48  MessageCount=49   Index.CountEntries()=1  blocks=1
AFTER  seg base=50  first=52  last=96  MessageCount=45   Index.CountEntries()=1  blocks=1
...
REPORTED total=381   ACTUAL surviving records=138
```

**2.76× over.** A caller setting `MaxLogMessages: 138` — asking to keep exactly
what is there — would have the walk believe it holds 381 and delete about
two-thirds of the log. That is #285's failure mode again: silent data loss
dressed as policy.

The fixture is worth noting because the first two attempts could not see it, both
for reasons already recorded in this sweep. A single segment large enough not to
roll is never compacted at all (compaction touches sealed segments). And a
fixture where *every* key is superseded gets its segments **deleted rather than
rewritten** — the "a rewrite budget needs a survivor" shape — so no offset gap is
ever created. The bug needs a segment that keeps some records and loses others,
which is the only thing that produces a gap.

**Why it cannot be fixed cheaply.** The probe answers that too: a rewritten
segment came back as **one block with one index entry holding thirteen records**.
The index cannot count them, the block table records lengths and not counts, and
the block header — magic, version, codec, uncompressed length, compressed length,
11 bytes — has nowhere to put one either. Nothing on disk knows how many records
a compacted block segment holds. The offset span is not a lazy approximation of a
number that is available; it is the only number there is.

So the correct fix is a stored count, and the natural home is the block header
itself (11 → 15 bytes, with the existing `BlockFormatVersion` bumped, which is
already a documented clean cutover). That makes the count recoverable by
`scanBlocks` after a crash has destroyed the sidecar, rather than only by the
sidecar — the distinction that decides whether the fix is exact or merely usually
exact. `blockTableEntryLen` and `offloadMeta` would carry it too, so neither a
reopen nor a tiered segment pays a walk for it.

That is an on-disk format change, and it is being raised rather than taken:
durable_streams is repinning onto v0.88.0 as this is written, and a block-format
bump makes existing block-compressed segments unreadable to the new build.

**Taken, 2026-08-14.** Both approvals came in on the same day the section above
was written. durable_streams answered the compatibility question — they do run
`Compression` and `Compact` on one `commitlog.Options`, their compaction and
maintenance chaos suites cover both codecs, and they keep no on-disk data across
versions and do no migrations: *"Bump the block format."* The user's card
answered `bump-format`.

What shipped is the design the section describes, with one addition it did not
foresee. The count went into all three places a segment can be described from,
because each of them is the ONLY description in some state the segment can be in:

- the **block header** (11 → 15 bytes, `BlockFormatVersion` 1 → 2), which is what
  `scanBlocks` recovers from when a crash has destroyed the sidecar;
- the **block table** (`blockTableEntryLen` 9 → 13, `blockTableVersion` 1 → 2),
  so a reopen and a tiered read do not walk the object for it;
- **`offloadMeta`/`TierObject`**, which is the addition. An offloaded segment
  fetches its block table at the first READ, not at open, and option 2 keeps its
  index in the store — so at the moment tier retention asks a cold segment for
  its count there is nothing resident to ask. Without the manifest field the fix
  would have reached every local segment and none of the tiered ones, which is
  where compacted data is most likely to be sitting.

`MessageCount` is now three reads of a stored fact and no arithmetic, and the
comment defending the span is gone rather than corrected.

Two things fell out of writing it that are worth keeping:

**A zero is refused, in both formats.** No block holds no records — `write`
refuses an empty message set before a byte is appended — so a zero in that field
is a field nobody wrote. Accepting it as "empty" would make a count read LOW,
which is the opposite failure and just as bad: a segment reporting 0 is one a
retention walk can never trim. There is deliberately no value meaning "ask
someone else".

**The cross-check was free.** `fetchBlockTable` already refused a table whose
extents disagreed with the manifest entry beside it; the record count is the same
kind of claim written by the same offload, so it is compared there too. That
matters more than it reads: the fetch is the moment a segment stops answering
from the manifest and starts answering from the table, and two sources that could
disagree would make a segment's count change the first time anybody read it.

**And one test was asserting the wrong half.** `TestBlockHeaderErrors`'s
"unknown codec" case poked `hdr[1]` — the VERSION byte, which the case above it
already covers — so `compress.Codec.Valid()`, whose only production caller is
`parseBlockHeader`, had no test reaching it at all. It pokes `hdr[2]` now, and
the version case it was duplicating is spelled out separately.

### The bump's own aftershock: a fixture that ages into a different test

The format change made the whole suite go red in exactly one place, and the
place is worth recording because two more sites had the same defect and stayed
green.

`TestBlocksAndRecordsAgreeOnATruncatedPayload` laid its block header out as a
byte literal — magic, version, codec, then two hand-written `uint32`s — to
express "a header that is well formed in every way except the payload length it
claims". At 15 bytes the literal is a 14-byte file, so the parse stopped at the
length check, one step before the overrun check the test exists to cover. It
only went red at all because the assertion names the claim
(`"claims 1024 payload bytes"`) instead of merely requiring an error.

`TestClassifySegmentDoesNotReadTheBody` has the identical fixture and did NOT go
red: its second half only requires that `Blocks()` fail, and a short header fails
too. Its stated proof — "the fixture's body must be unparseable" — had quietly
become a proof about the header. Both now build the header with
`encodeBlockHeader` and corrupt only the field under test, so the next layout
change carries them along instead of degrading them.

The two `version 9` fixtures (`TestAnUnknownBlockVersionIsReportedNotRefused`,
`TestInspectSegmentNamesBothBlockFormatVersions`) are fine and were left alone:
they pad with 64 bytes, so the header is long enough for the version check to be
the one that fires. That is luck rather than design, but they assert on the
version byte specifically, which is the field they hand-write.

**The lens:** a fixture asserting "valid except for X" is coupled to the whole
layout, not to X. Build it with the writer. And assert on the error's CONTENT —
that is the difference between the one that went red and the one that did not.

### Rule 4 of layercheck could be switched off by renaming a receiver

Same family, one layer up. `hack/layercheck.sh` selected the methods to check
with `grep -ohE '^func \(l \*commitLog\) [A-Z]...'`, hard-coding the receiver
name. All 95 methods spell it `l` today; renaming it would have emptied the
selection, skipped the loop body, and printed
`every exported commitLog method is on the interface, with no exceptions` — the
same green as a fully checked run. The other four checkers all treat an empty
selection as a HARNESS ERROR and print what they actually examined; this rule
printed the length of the hand-written `LOWER` list instead, which is a different
rule's input.

Now: the receiver is matched as an identifier, an empty selection exits 1, and
the green line quotes the method count that was checked. Falsified by pointing
the pattern at a type that does not exist — it exits 1 with the harness error.

**Recorded as a negative:** `docdrift.sh` has the weaker form (it counts files,
not doc comments examined) and was left alone. Its inner selection is `/^func /`
over gofmt'd Go, which cannot silently become empty while the package has
functions; the file count it prints is the selection that can.

### The manifest count, asserted where it is actually read

`TestATieredSegmentCountsWithoutFetchingItsBlockTable` sets `seg.records` by
hand, because a gappy segment cannot be built through a store fixture. It can be
built through an offload, and
`TestATieredCompactedLogReportsItsRecordCountFromTheManifest` now does: compact a
snappy log, offload it index-and-all, and reopen it over the same store from a
directory that never held it. Nothing is poked — the count leaves as
`offloadMeta.Records` and comes back through `openOffloadedSegment`.

Two things had to be got right for it to mean anything, and the first one failed
first:

- **`RemoteIndexCache` is what selects the cold path.** Without it the index
  stays local (option 1), and `openOffloadedSegment` runs `setupIndexKnownEnd`,
  which fetches the block table on the spot — so the count would have come from
  resident blocks and the manifest field would never have been read. The
  assertion that every table stayed pending is what caught that.
- **Exactly one table is fetched**, the newest segment's, where the log
  establishes its tail. The test asserts that exact set rather than "not all of
  them", because a regression that fetched a second table would satisfy any
  weaker form while putting the per-segment round trip back.

Mutation-verified: reinstating the span for the manifest arm alone makes it
report **95 records for 59 present**. The existing guard could not have caught
that arm — it neutralizes the resident-table branch, which this path never
reaches — so the third arm got a guard of its own.

### The third place the count lives, and the version that did not move

The block header got a hard version bump for `records`. The block table got one.
The manifest — the third place a segment describes its count from, and the only
one a cold tiered segment can answer from — got the field and kept version 3.

`manifest.go` documents the rule it broke, in the comment above the constant it
broke it on. Version 2 added `BlocksKey` and version 1 is refused; version 3
added `Tier` and version 2 is refused; both say *refused rather than adapted*.
The same comment then explains why the check is `!=` and not `>`:

> A `>` comparison would also accept version 0, which is what an absent field
> decodes to, so any JSON object that happened to parse would be read as a
> manifest.

That is the exact defect, one field down. An absent `Records` decodes to 0, and
nothing rejects it, so a v0.88.0 manifest reads clean and reports every segment
it names as holding no records.

It is worth being precise about which direction that fails in, because it is not
the direction the release is about. `applyTotalLimit` walks segments oldest
first, summing counts until the running total is under `MaxLogMessages`.
Overstating each term makes the walk reach the ceiling sooner — that is the
2.76x over-delete this release fixes. **Understating never reaches it at all.**
The limit stops being enforced over the tier, and nothing goes wrong out loud:
no error, no missing records, just a log that keeps growing past a cap somebody
set. The loud failure is the one that got found in a week; this one is the sort
that gets found in a bill.

**What was NOT done, and why.** The first instinct was to also refuse a
non-positive `Records` per entry, mirroring the existing `Tier == ""` check and
the block table's own `records == 0` refusal. That would have been wrong.
`uploadTo` refuses an *unsealed* segment and nothing else, so an empty sealed
segment can be offloaded, and for that entry zero is the **true count**. The
block table can refuse zero because no block holds no records; a manifest entry
describes a whole segment, and a segment holding nothing is a thing that exists.

Which sharpens what the defect actually was. It was never "zero is an illegal
value" — it was "a manifest that never stated a count is read as having stated
zero", which is the same sentinel collision the constant's own comment describes
about version 0, and the one `defaultTierName` exists to avoid. The fix is the
version, and only the version: state it or be refused.

The guard anchors on `const manifestVersion = 4` and neutralizes it to `3` —
what shipping the field without the bump looked like. Everything compiles, every
round trip through a manifest this build wrote is fine, and only a manifest from
the release before goes wrong. Deliberately not anchored on the reader's
comparison: that check was already correct. The number it defends is what failed
to move.

#### The same question asked of every other format

A defect found in one format is a question owed to the rest. The repo has four
things it writes and reads back, and each was checked for the shape #300 had —
a reader that accepts a file written before a field existed and reads the
absence as a value:

- **Block header / block table.** Both refuse anything that is not the version
  this build writes. Both bumped for `records`.
- **Descriptor.** One version, equality check, and `set()` refuses an unknown
  key by default — so a file from a newer writer is refused rather than
  half-read. The other direction cannot bite either: every field absent from an
  older descriptor lands as a zero that `describeDifference` reports as a
  mismatch, which is `ErrDescriptorMismatch` and loud. `Identity` is the one
  optional field, and its absence is reported as `Stored: false`, which is the
  honest answer rather than a laundered one.
- **Leader epoch checkpoint.** Versioned with an equality check, and the comment
  at the check records that the "reject anything newer, silently accept anything
  older" trap was already found and closed here.

So the manifest was the only one, and it is closed. Worth stating as a negative
rather than leaving unsaid: the next person to add a field to a format has three
worked examples of the bump being mandatory and none of it being optional.

### A refusal that was a side effect of a file not being written

`TestOffload_ReopenWithoutStoreErrors` went red on the first full suite that
actually completed after #295. Its own doc comment names the mechanism it
depended on, and reads as a design statement rather than an accident:

> What refuses is the DESCRIPTOR, which is in the store too: a directory that
> plainly holds a log, with no descriptor to say what log it is, is not
> something to guess at.

Which is true, and was never the rule. The rule was an ABSENCE. A tiered log
wrote its descriptor only to its tiers, so opening its directory alone found no
descriptor and fell into "the log exists and its identity does not". #295 writes
the local copy — which is the whole point, durable_streams' reclaimer needs it —
so the directory now has one, the branch is not reached, and `New` returns nil
for a log whose bytes are somewhere else.

What that costs is not abstract. The local segments are the **tail**:
`OldestOffset` reports far past what the caller wrote, reads skip the offloaded
prefix, and retention runs against a log it can only see the end of. Every one
of those is silent, which is why nothing but that one test noticed.

**The lens.** A rule enforced by a side effect has no anchor. There is no line
to guard, no name to grep, and no comment that a change would contradict — so
the change that removes it looks unrelated to it. The test comment above is
what a stated rule looks like *after* it has been inferred from behaviour, and
it was accurate right up until it wasn't. Worth asking of any refusal: is there
a line of code that would have to be deleted for this to stop happening? If the
answer is "no, you would just have to write a file somewhere", it is not
enforced.

**Two decisions in the fix.**

- **Not an `enforced()` field.** The obvious shape is to add `Tiered` beside
  `Compact` and let `describeDifference` name it. That would have been wrong in
  the direction that matters: `AdoptOptions` bypasses `enforced()` entirely, and
  durable_streams adopts on *every* open because its settings come from a
  catalog rather than a config file. The check would have been no check at all
  for the caller most exposed to it. So it is a separate refusal, above the
  adoption branch. Adopting is a statement about POLICY; where the bytes are is
  not policy, and adopting does not relocate them. This is strictly stronger
  than what was lost — the old refusal lived in a branch `AdoptOptions` was
  allowed straight through.
- **One-directional.** The reverse — a plain log the caller is attaching a store
  to — is a legitimate adoption and must keep working. It is also unreachable
  from this check: `loadDescriptor` reads the nearest TIER when `Tiers` are set
  and never consults the local file. Asserted anyway, because "unreachable" is a
  claim about code that changes.

And the version bump, one day after #300 taught the same lesson on the manifest:
a v1 descriptor is refused rather than read as `tiered=false`, because that
default is wrong on exactly the logs the field exists to protect.

**Two guards, not one.** Deleting the whole refusal turns both subtests red, so
it cannot say whether the AdoptOptions arm is covered on its own. The second
guard neutralizes to the plausible wrong version — `!opts.AdoptOptions && ...` —
and only `with_adoption` goes red under it, which is what proves the two
subtests cover different arms rather than one arm twice.

## The bump's aftershock, second pass: two more fixtures, and the one that predicted itself

`descriptorFileV1 → V2` broke two descriptor fixtures the same day the block
header bump broke one (#297). Three fixtures, three different failure modes, one
cause — a fixture built from a format's CURRENT value.

- **The no-op.** `TestAVersion0DescriptorIsRefusedByVersion` downgraded with
  `strings.Replace(body, "1\n", "0\n", 1)`. At version 2 there was no `"1\n"`
  left to match — no other line in a descriptor ends in a bare 1 — so the
  substitution changed nothing and the file written back was the file read. The
  only thing that caught it was the fixture's own self-check,
  `require.NotEqual(body, old, "the fixture did not actually downgrade the
  version")`. Without that line a v2 descriptor would simply have opened and the
  assertion below would have reported that the version check had gone missing.
- **The wrong-reason pass.** `TestDescriptorRefusesUnknownKeysAndBadValues` hand-
  wrote `"1\n"` atop three bodies. At version 2 all three fail on the VERSION
  check without reaching the key parsing the test exists to exercise, and
  `require.Error` cannot tell those apart. All three subtests were green for the
  wrong reason.

The second one is the interesting one, because **its doc comment already said
so**: the fixtures read `"0"` until v0.82.0 dropped V0, and the comment written
then explained in full that leaving a stale version would keep the test green
without reaching the key parsing. It predicted the bug exactly. It was still
there, unchanged and correct, while the bug it described was live.

**A comment warning about rot is not a defence against rot.** The fix has to be
something that runs. Two things now do: the version is interpolated from
`descriptorFileV2`, so the fixture follows the format rather than recording one
moment of it; and each subtest asserts the error is NOT `"unsupported descriptor
version"`, which converts a wrong-reason pass into a failure. Pinning the fixture
to `descriptorFileV2-1` turns all three red on exactly that line — which is the
only version of the warning that has any force.

Method note, because it nearly cost the verification: the first attempt at that
mutation silently failed to apply and `go test` answered `ok (cached)`. A
mutation that did not apply plus a cached pass is indistinguishable from a
mutation that applied and was survived. **Use `-count=1` on every mutation run.**

## Closing the version-guard set from the other side

Prompted by #300 — `TierObject.Records` shipped without moving
`manifestVersion` — the obvious follow-up is not "check the other formats once"
but "what makes the NEXT format get checked". Inventory first:

| format | refusal tested | guarded |
|---|---|---|
| `manifestVersion` | yes | yes (the refusal, and the constant) |
| `descriptorFileV2` | yes | anchor stale, see below |
| `leaderEpochFileV0` | yes | yes |
| `BlockFormatVersion` | yes (`blockformat_test.go`) | **no** |
| `blockTableVersion` | yes (`block_table_test.go`, `b[1] = 9`) | **no** |
| `digestVersion` | **no** | **no** |

The digest one soft-fails to "no digest", which is the right behaviour for a
cache — a version it cannot read is a digest it rebuilds. But nothing asserts
it, so a stale-version sidecar could start being MISPARSED rather than ignored
and no test would notice.

Three guards fixes today. The structural fix is a `hack/` check that enumerates
version constants compared in a refusal and requires each to be named by a
`run_guard` — same shape as `hack/storesize.sh` (#293), and the same reasoning as
every other place this repo replaced "remember to" with a script.

## An anchor audit is cheap and finds what a guard cannot report

`hack/guardcheck.sh` reports a missing anchor as a failure, but only when it
runs — and it takes the better part of an hour. Parsing every `run_guard` line
and checking its anchor against a `git archive` of HEAD takes two seconds, needs
no build, and does not touch the tree, so it can run while a suite or a
guardcheck is in flight.

104 single-line anchors, one genuine miss: `hack/guardcheck.sh:1862` still names
`descriptorFileV1`, renamed by #301. (Multi-line and `$'...'` guards need a real
shell to parse and were reported as unparseable rather than guessed at — an audit
that quietly skips what it cannot read is the failure mode this repo already
knows from `GUARDCHECK_SET=platform`.)

## Negatives, recorded so the next sweep does not re-open them

- **Back-compat and migrations.** A grep for backward/legacy/deprecated/migrate
  across every non-test file returns nothing that keeps an old path alive. Every
  hit is a comment explaining why a clean cutover was taken. Axis 1 is closed.
- **`blockHeaderLen` cannot drift from its writer.** `compression_test.go:395`
  asserts `require.Len(t, hdr, blockHeaderLen)` against `encodeBlockHeader`'s
  output, and the round-trip test reads all four fields back.
- **The format bump did not disarm any fuzz seed.** This is the trap from the
  keydigest change — a byte-offset seed becomes a no-op when a layout grows — and
  v0.89.0 moved two byte layouts. All three byte-poking targets are immune by
  construction: `corruption_fuzz_test.go` finds its byte with
  `bytes.Index(raw, marker)`, `frame_header_fuzz_test.go` walks the frames to
  build `starts`, and `digest_corruption_fuzz_test.go` uses `raw[at%len(raw)]`.
  None uses a fixed offset. The seeds are `{recordIdx, byteWithinRecord, mask}`,
  which is a coordinate the layout cannot invalidate.
- **`manifest_key_traversal_test.go`.** Flagged by the bare-`require.Error` scan,
  and both are fine. The four version cases all target the SAME check, so there
  is no earlier condition to mask them, and they interpolate `manifestVersion`.
  The traversal case follows its `require.Error` with a positive consequence
  assertion — the file outside the store must still exist — which is stronger
  than an error-identity check, because it proves the traversal did not happen
  rather than that something failed.

## Closing the version-guard set, and what closing it exposed

The six-format table above said three of six version constants had neither
guard. Fixed in `cab05fa`, but the interesting part is not the three.

**The per-format fixtures had to be written twice.** The claim under test is
*the constant moved when the layout did*, so the falsifying mutation is the
constant put back to its previous value — and two obvious fixture shapes are
invariant under exactly that mutation:

- `theVersion - 1` moves **with** the constant. Neutralize `digestVersion` to 1
  and the fixture writes 0, which is still refused, and the test stays green.
- an arbitrary wrong value (9, `0xFF`) is refused whatever the constant says,
  so it tests the *other* claim — that the version line is checked — which was
  already held.

Both were written before this was noticed. All three tests now name the old
version as a literal, with a comment saying why, because a later tidy-up that
"removes the magic number" would disarm them in silence.

`TestAV1BlockTableIsRefusedByItsVersion` needed one more thing: an assertion on
the error **message**. A v1 entry is 9 bytes against v2's 13, so a v1 table
fails `decodeBlockTable`'s exact-length check as well, and both refusals are
`ErrBlockTableFormat` — `require.ErrorIs` cannot tell them apart. Neutralized,
it dies on the length instead, and only the message assertion sees it. This is
the #307 shape one format over: an ordered check where the sentinel is shared.

The digest had no test at all, and the reason generalizes. Its version mismatch
is a **soft** failure by design — `loadKeyDigest` returns nil and the caller
rebuilds — and nothing goes red when a soft path stops working. It gets slower,
or worse, it starts succeeding on bytes it should not have understood.

### The structural half found the same three cold, then failed at its own job

`hack/formatversion.sh` enumerates version constants that gate a refusal and
requires each to be named by a guard. Run before the guards were added, it
independently reported the same three — a real cross-check of the hand
inventory, not a restatement of it.

Then it reported **all five held**, and there are six.

`manifestVersion` is declared `const manifestVersion = 4` on its own line rather
than inside a `const (...)` block. The declaration pattern was written against
the spellings that happened to be in front of me, none of which had the keyword,
so the scan never saw it — and the script silently omitted the one format whose
missed bump (#300) is the entire reason the script exists. **That is #300 one
layer up: the check ran, against the wrong set.**

The fix is not a wider pattern, because the next unpredicted shape defeats a
wider pattern the same way. It is the lens from #294: *close the set from the
other side.* The script now also extracts every name **compared** as a version
and requires each to appear in the declaration scan. A name compared like a
version whose declaration was not found is a **harness error**, not a missing
guard — it cannot be satisfied by adding a guard, and until the scan is taught
the shape the script has no standing to report anything. Reverting the `const`
widening now produces that error naming `manifestVersion`, where before it
produced a green.

### Two more, found by mutating the checker rather than reading it

Neither was visible on a read-through. Both came from trying to falsify the
script one constant at a time.

- **The guard-presence test was a substring match.** `grep -q "$name" "$guards"`
  reads as "is this constant named by a guard" and is not: renaming the guard's
  anchor to `manifestVersionXX` still satisfied it. It is now word-bounded. Same
  slip as guardcheck's own `--filter` argument, which was a substring taken for
  a regex — the third time this exact confusion has cost something here.
- **A `| grep -q` in the declared-name lookup.** The comment beside CI's
  shellcheck step says neither script pipes into `grep -q` any more, and that is
  not a style rule: under `pipefail` grep exits early, the upstream command takes
  EPIPE, and the pipeline's status becomes the write failure rather than the
  match. It is a `case` on a newline-delimited string now.

The general point: **a checker gets read for what it says and mutated for what it
does, and only the second one found these.** Six mutations, one per constant,
took under a minute; the read-through that preceded them found nothing.

## An ordered refusal needs an identity assertion, not an error assertion

`parseBlockHeader` refuses in five steps: length, magic, version, codec, zero
records. `TestBlockHeaderErrors` asserted `require.Error` five times, and that
is not a weak test so much as a test of something else — it asks whether the
function said no, when the question is *which step* said it.

The gap already cost something, and the comment recording it was sitting right
there in the test: a case labelled "unknown codec" poked byte 1, the **version**.
The version check answered it, `require.Error` was satisfied, and
`compress.Codec.Valid()` — whose only production call site is the next line —
had no test reaching it at all. The label was the only thing claiming otherwise,
and labels do not run. Aiming that case back at byte 1 is now red.

The general shape: **in an ordered refusal, a fixture aimed at the wrong byte
still produces AN error**, so the assertion has to name the step. What
distinguishes them is the message, so the message is what is asserted, and the
failure text states the consequence — a case that stops testing its own step
reports success in exactly the same way as one that works.

### The second question: should all five wrap ErrBlockFormat?

**No — fewer, not more.** Two did; the right number is one.

`ErrBlockFormat` is documented as a caller-facing probe run at startup before
anything is touched: *this data was written by another build, stop*. The answer
to it is to run the right build, not to repair anything. The zero-record check
wrapped it, and that check is reachable **only once the version byte has been
read and found to be this build's own** — the message said so out loud:
`unsupported block format version: block header claims no records`.

`ErrBlockTableFormat` goes the other way and every one of its six sites wraps
it. Both are right, and the difference is in the sentinels' own words: *"not a
block table"* is true of a bad magic and a bad CRC alike, where *"unsupported
block format version"* is a claim about one byte. **A sentinel whose text is
specific cannot be applied generally without lying** — and the direction that
matters is widening, because narrowing makes a caller miss a case it handles
while widening makes it act on a build mismatch that never happened.

Both tests now assert the sentinel in **both** directions.

### The arm that protected nothing

`scanBlocks` ran `if errors.Is(err, ErrBlockFormat) { return err }` before the
wrap that names the byte offset. It bought nothing: `errors.Is` sees through
`errors.Wrapf`, so a caller matched the sentinel either way. Its only effect was
to **delete the offset from exactly the refusals that had been given a
sentinel** — so the next refusal filed under one silently lost its position too,
which is precisely what happened to the zero-record check.

Worth stating as a rule: **a fast path conditioned on an error's identity, whose
only difference is the message, is a trap for the next error that gains that
identity.** The removal is falsifiable — `versionMidSegment` in
`TestACorruptBlockHeaderIsNotATornTail` is the only case that reaches the arm.

### The same lens, one file over

`decodeBlockTable` refuses in six ordered steps, all `ErrBlockTableFormat`, and
`TestADamagedBlockTableIsRefused` had nine cases asserting only `ErrorIs`. Nine
fixtures could have collapsed onto three checks with nothing to show for it. Two
pairs do share a check on purpose (nil and header-only are both too short; a
bogus count and a trailing byte are both size-vs-count) — deliberate sharing is
fine, and accidental drift onto a shared check is what the message assertion
catches. Aiming the wrong-magic case at the version byte is now red.

## The same question, answered twice in one package

#302 (sqlcdc's report) is not really about which error is right. Both spellings
existed and both were defensible in isolation. The defect is that **two paths of
one package had to decide what a dead log looks like to a caller, and only one
of them had been asked.**

`ReadMessage` translated `IsClosed`/`IsDeleted` into `ErrCommitLogClosed` /
`ErrCommitLogDeleted`. `newSourceReader` consulted the same two predicates — but
only to decide whether to stop retrying — and then returned whatever error it
happened to be holding, which is `ErrSegmentClosed`: the value `segmentSwapped`
in that same file defines as *the storage layer announcing a compaction swap*,
and which the comment above `newSourceReader` describes as the exact condition
the retry loop exists to absorb.

So at construction, a dead handle and a segment swap were the same value. The
symptom is what a caller does with that: four retries in 521µs, 0s, 0s, 0s —
backoff with nothing to back off from, because the sentinel said "try again"
about a log that was never coming back.

**The lens is not "is this error right".** It is *does any other path in this
package answer this same question, and does it answer it the same way?* An error
that is locally reasonable and globally inconsistent has no test that can see
it: each path passes its own tests, and the caller who suffers is holding the
difference between them.

Related but distinct from the #307 lens above, which is about a *single* path
whose refusals cannot be told apart. This one is about *two* paths that agree
they are answering the same question and disagree on the answer.
