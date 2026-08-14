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
