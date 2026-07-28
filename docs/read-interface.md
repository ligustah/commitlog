# The read interface — a proposal

**Status: PROPOSAL, nothing agreed and nothing built.** `ReadKeyPrefix` exists
today (`40b4d0d`); this proposal **deletes it**. See "What this replaces".

## The premise that decides the design

**Compaction is an optimisation, so every consumer must already tolerate
duplicates.** A key can have many live copies at any moment — compaction is
asynchronous, budgeted, and lags behind writes by design. A consumer that broke
on a second copy of a key would already be broken today, with no read API
involved.

So a read has no business promising *exactly one record per key*. Once that
promise is dropped, most of the complexity in a prefix read goes with it.

## What exists now

```go
NewReader(offset int64, uncommitted bool) (*Reader, error)  // tails: parks for appends
NewScanReader(offset int64) (*Reader, error)                // terminates: io.EOF at the end
ReadMessageSet(offset int64, maxBytes int) ([]byte, error)  // raw framing, for replication
ReadKeyPrefix(prefix []byte, upTo int64) ([]PrefixRecord, int64, error)
```

Two of those constructors differ in exactly one axis — terminate versus follow —
and a third axis (committed versus uncommitted) is a bare bool. Adding a key
filter and a dedup setting as further constructors multiplies out to eight entry
points for one read with four independent settings.

Backwards compatibility is explicitly **not** a constraint, so the fix is to
replace them rather than accumulate a third.

## The shape: one constructor, functional options

Both existing constructors are **replaced**, not wrapped. There is one way to
open a reader:

```go
func (l *commitLog) NewReader(opts ...ReadOption) (*Reader, error)

From(offset int64)      // start here. Default: OldestOffset
Until(offset int64)      // stop after this offset. Default: unbounded
Follow()                 // park for appends. Default: io.EOF at the end
Uncommitted()            // read past the high watermark. Default: committed only
KeyPrefix(p []byte)      // only keys beginning with p. Default: every record
SkipSuperseded()         // drop copies a later copy in the SAME segment supersedes
IncludeControl()         // keep control markers in a filtered read (see below)
```

One combination is refused at construction: `KeyPrefix()` with `Uncommitted()`
and neither `Until()` nor `IncludeControl()`. See "Control markers" — it returns
records the caller cannot classify, and does so silently.

```go
r, err := l.NewReader(
    commitlog.From(resumeOffset),
    commitlog.KeyPrefix([]byte("user:")),
    commitlog.SkipSuperseded(),
    commitlog.Follow(),
)
```

Every combination streams, is offset-resumable, and can follow. There are no
modes and no ordering variants — just settings, with the single refusal above.

`Reader` is already an iterator (`ReadMessage`), so a filtered read streams in
bounded memory rather than materialising every matching record.

### Why options rather than a spec struct

`CleanWithSpec(CleanSpec)` is the codebase's existing answer to this problem, and
the first draft of this sketch copied it. Options are better **here**, for a
reason that does not apply to `CleanSpec`: **the zero value of a read setting is
meaningful.**

Offset 0 is a real offset. `Uncommitted: false` is a real choice. `Until` has no
natural "no bound" value, so a struct has to invent a sentinel — the first draft
used "negative means unbounded", which is exactly the kind of thing a caller
misreads. With options, *unset* is genuinely absent and the default is stated
once, in one place.

`CleanSpec` has the opposite property: it is data a transactional layer
**computes**, passes down, and may want to log or compare. Options cannot be
inspected or serialised — the same objection raised against taking a `func`
predicate for `StripHeaders` instead of a `[]string`.

That objection does not bite here, because the options only **construct** an
internal `readSpec`. The struct still exists, is still what the reader holds, and
is still what gets logged; it just stops being the caller-facing surface.

### Two defaults worth flipping while compatibility is off the table

Replacing the constructors is a chance to fix two defaults that are currently
the wrong way round:

**Terminating becomes the default; following is opt-in.** Today `NewReader`
follows and `NewScanReader` terminates. The failure modes are not symmetric: a
reader that unexpectedly ends returns `io.EOF` and the caller notices, while one
that unexpectedly follows **blocks forever**. `NewScanReader`'s own doc says
hand-rolling a bounded scan "is how a scan ends up hanging on a watermark that
never advances" — so the hanging case should be the one you have to ask for.

**Committed-only becomes the default; uncommitted is opt-in.** It is already the
safer of the two, and it currently arrives as an unlabelled bool at the call
site (`NewReader(off, false)` tells a reader nothing).

## Why there is no latest-per-key mode

It was in the first draft of this sketch and is now gone. Dropping it removes,
in one stroke:

- the eager merge across every digest in range before the first record
- the inability to follow ("latest" is undecidable until you stop reading)
- the incompatibility with offset tracking — a key's surviving record can sit
  *below* a consumer's resume offset, so it would never be seen
- the `completeThrough` handoff, and the snapshot-then-tail protocol around it
- the key-versus-offset ordering question
- the question of whether to include the active segment in a snapshot

**The last two deserve emphasis.** With a following, filtered read there is no
snapshot phase and no seam: a restoring consumer reads from its offset with a
prefix filter and carries on into the tail as an ordinary reader. The two-phase
protocol existed only to serve a guarantee nobody needs.

**And little is actually lost.** On a log that has been compacted, superseded
copies are already physically gone, so per-segment skipping lands very close to
latest-per-key in practice. The residual duplicates are exactly the ones
compaction has not caught up with yet — which is to say, exactly the ones a
consumer must already tolerate.

## Where the acceleration applies, and where it does not

The digest holds every keyed record's offset, sorted by key, per **sealed**
segment. A filtered read over sealed segments therefore reads only matching
records. The active segment has no digest, so its records are scanned and
filtered one at a time.

A following reader accelerates its historical portion and runs plain at the tail,
transparently and with no flag. The speedup is a property of *having a digest*,
not of the API.

`SkipSuperseded` is decided from the digest alone: whether a copy is the last one
for its key **within its segment** needs no lookahead, which is why it streams
and can follow.

## Settled, and still open

**1. Unkeyed records and control markers — SETTLED.** Excluded, mechanically;
see "Control markers" below for why that costs nothing and for the one
combination it makes unsafe.

**2. Ordering is offset order**, everywhere, with no option. It is consistent
with the rest of the API, causally ordered for application, and the only ordering
that streams — output order matching physical order is what removes the buffer.

**3. `SkipSuperseded` and offset tracking.** Resuming works, but a consumer that
resumes mid-segment sees a different (larger) set of records than one that read
the segment from its start, since "superseded within this segment" depends on
where the read began. Harmless for a state-carrying consumer, worth documenting
precisely, and a reason not to make it the default.

## Control markers, in detail

**What they are here.** `AttrControl` marks a transactional commit/abort marker
written by a layer above — durable_streams. commitlog attaches **no meaning** to
them. It never interprets one; the only thing it knows is that markers below
`StripBelow` are removable once the records they governed have been stripped to
plain records, and that the two are only safe together.

**They are keyless, so their exclusion is mechanical.** `buildKeyDigest` routes
control records into `d.control` — a bare list of offsets — separately from
keyed and unkeyed data. A marker has no key at all, so a key-prefix filter
cannot match one. Dropping them from a filtered read is not a policy decision;
there is nothing for the predicate to test.

**Today every reader sees them.** `reader.go` does no attribute filtering
whatever, so every existing consumer already receives markers inline and either
interprets or skips them. A prefix filter is therefore the first read in this log
that would ever *withhold* one.

**When a consumer actually needs them.** A marker answers one question: did this
record's transaction commit? That question has already been answered, in the
data, for the decided and compacted part of the log:

- aborted records below the ceiling are **removed** by compaction (`Aborted`),
- survivors below `StripBelow` are **stripped to plain records**,
- and the markers themselves are removed there.

So for compacted, decided history a consumer needs no markers, because nothing
ambiguous remains. Markers matter in the **undecided region** — at or above the
ceiling (the caller's LSO), where nothing has been removed and a reader that
looks there must decide for itself what committed.

**Which makes `Uncommitted()` the axis that matters**, not the prefix:

- a **committed-only** filtered read stays below the high watermark, where the
  transactional layer has already decided and compaction has already acted, and
  loses nothing by dropping markers;
- an **uncommitted** read walks past that boundary and must filter aborted
  transactions itself, which it cannot do without them.

**commitlog cannot resolve this by being clever, and should not try.** It has no
notion of "decided" — `tla/README.md` states this as the class of bug the engine
cannot defend against, since `Ceiling` is an input it must trust. A log that
started inferring which markers a reader needs would be guessing at exactly the
thing it has no information about.

**Answered by durable_streams: no consumer of theirs needs markers while reading
a keyed prefix**, below the LSO or above. Their reasoning is sharper than the
above and subsumes it: *a state-transfer read must be bounded by the LSO, so
every record it can return is already decided; markers exist to decide undecided
records, and a read that cannot return one has nothing left to decide.*

So markers are excluded from a filtered read, mechanically, and that is correct.

### The trap this leaves, and the guard for it

Asking the question turned up a live bug in their layer — their `ReadKeyPrefix`
was bounded by the log **end** rather than the LSO, making an open transaction's
records eligible for transfer. A destination could be handed state that never
comes to exist, with nothing to reconcile it afterwards because the transfer has
already finished. Fixed in `broker/v0.17.0`.

Their warning generalises to this design, and it is worth taking seriously: the
ordinary reader gets away with treating the LSO loosely because it reads
committed-only and the layer above filters for it. **A scan path has no such
help.**

The dangerous combination here is therefore precise:

> `KeyPrefix()` + `Uncommitted()`, with markers excluded.

That yields records the caller **cannot classify**: it is reading past the
decided boundary, and the only thing that could tell it what committed has been
filtered out. It is not merely risky — it is unusable, and it fails silently.

**So it should be refused at construction**, not documented. `Uncommitted()`
together with `KeyPrefix()` requires the caller to either

- take the markers (`IncludeControl()`) and do its own transactional filtering,
  or
- state a bound (`Until`), asserting where decidedness ends.

commitlog cannot verify that an `Until` really is the LSO — it has no notion of
decidedness, exactly as `CleanSpec.Ceiling` is an input it must trust. But it can
insist the caller has *thought about the boundary at all*, which is the step that
was skipped above. Trusting a stated bound is the established relationship here;
silently allowing an unstated one is not.

## What this replaces

`ReadKeyPrefix` as shipped in `40b4d0d` is the wrong shape under this premise:
eager, buffered, key-ordered, unable to follow, and built around a
`completeThrough` seam that stops being necessary. It should be **deleted**, not
wrapped — its machinery (the digest merge, run planning, per-tier coalescing and
fan-out) is what survives, moving behind the reader.

`PrefixRecord` goes with it: a filtered read returns records through
`Reader.ReadMessage` like any other, so there is no second record type.

The per-tier fetch tuning added in `40b4d0d` and `a3eaf79` is unaffected and
carries over unchanged.

## What does not change

Tombstones are always returned, carrying `AttrTombstone`. A destination has to
delete those keys; filtering them out resurrects deleted data with nothing to
report it.

The digests remain an optimisation and never the definition: a missing, corrupt
or stale sidecar is rebuilt by scanning, and every read returns the same records
with or without them.
