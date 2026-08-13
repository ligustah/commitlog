# Separation-of-concerns audit — 2026-08-13

Asked for: is each layer responsible for the right things, and is any layer
working around a deficiency in another instead of fixing it where it belongs.
Two axes — commitlog's internal boundaries, and the commitlog/durable_streams
split.

Method: re-measure the claims in `docs/layering.md` rather than trust them, then
read the public surface for concepts this package does not own, then read the
call sites in `durable_streams` for what it does to compensate.

## 1. The internal layering holds. Its self-check did not. — FIXED

Every claim in `docs/layering.md` re-measured true:

- no file below the log calls up into it (`segment.go`'s six `l.` hits are all
  comments; `segment_store.go`'s seven are a `localBacking` receiver named `l`)
- `index.go` → `*segment`: 0
- `segment.go` → `*Reader`, → cleaners: 0
- `compress/` → commitlog: 0

`segment.go`'s 43 tier/store references looked like the segment layer reaching
up into the tier layer. They are the opposite: `SegmentStore` is an interface
the segment is handed, so the arrow points down. That is the one place the repo
does dependency inversion deliberately, and the grep that found it cannot tell
the two directions apart.

**The defect was the doc's headline metric, not the code.** It read "`*commitLog`
is named by six files". It is now ten, and nothing was violated in between:
almost every hit is a `func (l *commitLog)` method DECLARATION, and a file full
of commitLog methods is *by definition* the top layer. The number was measuring
how the log's methods are spread across files. It would have read identically on
a tree where `index.go` had started calling the log — which is the only event it
existed to catch.

A count that a violation would not change is not a measurement. Putting it in a
table makes it look like one, and it drifted 6→10 without anybody noticing that
the drift meant nothing.

Fixed: `hack/layercheck.sh`, wired into CI beside `docdrift`. One rule — nothing
in the lower half may name `*commitLog`, as receiver, field, or parameter — plus
a second rule that a non-test `.go` in neither half is an error, so a new file
cannot escape by not being mentioned. Both rules verified falsifiable against a
throwaway file before landing. `docs/layering.md` rewritten to say what is
enforced and what is only asserted.

## 2. Not a finding: file size

`segment.go` is 3,233 lines and `commitlog.go` is 2,662. Both are single-type:
77 methods on `*segment`, 52 on `*commitLog`. Splitting them across more files
would move text without moving a boundary — the concerns are already separated,
they are just all reachable, which is a property of the package and not of the
file. Recording this so the next sweep does not re-open it as if it were free.

## 3. The sidecar API is the one seam that exports a mechanism instead of an operation

Every other cross-layer seam in this repo hands the caller a specific operation
and keeps the rule inside. `CopyTier` is the model: the ordering rule (manifest
last, because it is the commit point) belongs to commitlog, so commitlog exports
the operation and leaves `descriptorKey` unexported — the doc comment says
outright that a caller doing it by hand "has to know the manifest's key to write
it last, which is why the key is not exported and this is."

`PutSidecar`/`GetSidecar`/`RemoveSidecar` does the reverse. It exports the
filesystem — arbitrary name, arbitrary bytes — and leaves the rules to the
client. `checkSidecarName` is a good refusal as far as it goes, but what it
defends is commitlog's own files, not the client's invariants.

This is not gratuitous: the log owns the directory, `openLog` scans it by
suffix, and a stray `notes.log` makes the log unopenable rather than merely
messy. The client genuinely cannot write there unaided. The question is whether
the answer should have been a generic namespace.

### 3a. The measured cost: identity cannot be stamped atomically with creation

In `durable_streams`, `stampIncarnation` records which lifetime of a stream name
the local bytes belong to. Its own comment states the constraint:

> Called AFTER the stream is open, because the stamp is a file in the stream's
> own directory and there is no directory until then.

So there is a window — log created, not yet stamped — and a crash inside it
leaves an unstamped copy. Unstamped copies are then deliberately never
reclaimed, because "an unstamped copy and a stale one look the same, and only
one of them should be destroyed." The leak is permanent by design, to avoid a
worse outcome. `notReadyToJudge` was added to close a related window on the same
question.

commitlog is the only layer that can remove the window, because commitlog
creates the directory. `New` already settles the log's identity atomically
before anything opens (`reconcileDescriptor`, commitlog.go:576), for exactly the
reason given in the comment there — a policy the log was not created with must
never get applied at all. Client identity has the same shape and the same
requirement, and the two-step is forced only by the API.

**Shipped in v0.80.0.** `Options.Identity` carries opaque client bytes written
with the descriptor at creation, so "unstamped" stops being a state that exists
rather than one tolerated forever. durable_streams asked for one thing on top of
the proposal, and it was the right thing: a mismatch on reopen must be
*reported*, not silently adopted and not refused — "swallowing it just moves the
window."

That requirement shaped the design more than the storage did:

- **Not refused**, because a caller whose identity disagrees still needs the log
  open to do anything about it. Refusing takes a partition offline over
  bookkeeping.
- **Not adopted**, because a signal consumed at open time is lost to a crash
  immediately after. Leaving the stored bytes alone is what makes the
  disagreement survive restarts. `AdoptOptions` already meant "I know what this
  log is, record it" and is the deliberate resolution, so no new knob.
- The republish that keeps non-gating descriptor fields current carries the
  *caller's* identity, so it had to be suppressed while a conflict stands —
  otherwise a plain codec change re-stamps the log and destroys the
  disagreement. That is adopt-on-open by the back door, firing only on the
  subset of opens that also retune something else. Guarded and mutation-tested.

**Then the same republish turned out to be wrong in the other direction**, found
by re-reading v0.80.0 rather than by a failure. The guard above tests
`conflict == nil`, and a caller with NO identity conflicts with nothing — by
design, since it has no opinion to disagree with. So the republish ran, carrying
that caller's empty identity, which `renderDescriptor` omits entirely. The stamp
did not become wrong; it stopped existing.

The two halves look nothing alike from inside the function and are the same
mistake: the record being published was built from the caller's options, so
every field in it is the caller's answer, including the one this function had
just decided to *report* rather than adopt. The fix is to build it from the
stored record and refresh only the two fields the republish exists to refresh —
then the stored identity is carried by construction, and the next field added
cannot reopen this by not being mentioned in a condition.

Worth recording that the erase is the worse half. The whole argument for 3a is
that an unstamped copy must not be a state that occurs, because durable_streams
cannot reclaim one — unstamped and stale look identical and only one should be
destroyed. An erase manufactures exactly that, from a correctly stamped log, on
an open that did nothing wrong. The feature shipped with a path that produced
the condition it was built to eliminate.

### 3b. Two reserved-name lists guard one directory, from two repos — FIXED in v0.83.0

commitlog refuses names it owns (`logOwnedFileNames`, `logOwnedFileSuffixes`);
durable_streams separately refuses names *it* owns (`reservedSidecars`). Each
layer defending its own names is correct and not the problem.

The problem is that commitlog's set is **open-ended and unversioned**. Adding a
sidecar file or a working-copy suffix in any future release retroactively makes
a client name that was legal illegal — at runtime, on data already written, with
no way for the client to have checked. The client's namespace is defined by
subtraction from a set that only commitlog can change.

The fix is to close the set from the other side: give client sidecars a reserved
prefix commitlog promises never to take. Then commitlog can add files freely,
the client can name freely, and neither list needs to know the other. Breaking
change, no migration burden pre-v1 — but it changes durable_streams' names, so
it is an ask and not a unilateral edit.

**Shipped as `ClientSidecarPrefix` after durable_streams agreed** ("both are
cheap to rename now and impossible later"). Both lists are gone. Implementing it
turned up a half the finding had missed: the refusal is not enough on its own.
`checkSidecarName` governs names arriving through `PutSidecar`, but the log's
directory scans — `openLog` and `logIsNew` — dispatch on SUFFIX over whatever is
already on disk, so a file named `client-notes.log` fails the open on its
non-integer stem and `client-notes.index` is deleted as an orphaned index, with
no call for a refusal to intercept. Both scans now skip the prefix.

That is the load-bearing part. Without it the prefix buys the client nothing it
did not already have: it would still be commitlog's suffix list defining the
client's namespace by subtraction, just enforced one layer further in.

## 3c. The public surface was not the interface — FIXED in v0.80.0

Found by durable_streams' half of this audit rather than by mine, which is
worth recording: they reported it as a workaround on their side, and it was a
defect on ours. They were reaching `RecoverTail` and `ActiveSegmentBase`
through anonymous type assertions.

`New` returns the INTERFACE. So a method exported only on the concrete type is
not public in any useful sense — the sole route to it is a structural
assertion, and that degrades **silently** on a miss: the caller takes the zero
value or skips the call, with nothing to log. `RecoverTail` at open is what
makes their producer-id records survive a restart, so the failure mode there is
data quietly ceasing to be recovered.

Checking turned up five, not the two reported: `RecoverTail`,
`ActiveSegmentBase`, `SegmentBlockCounts`, `IsClosed`, `IsDeleted`. All are now
on the interface and documented there rather than only on the implementation.

Named optional interfaces were offered as the alternative and declined. These
are not optional — they were missing — and an optional interface preserves the
silent-miss path instead of removing it.

`Segments` stays off, on an argument about its SIGNATURE rather than about
convenience: it returns `[]*segment`, an unexported type, so nothing outside
the package could use the result. It is the one entry in layercheck's
`EXPORTED_EXCEPT`, with that reason written beside it.

**Why this needed a check and not a habit.** Five methods drifted off an
interface that is the package's entire public contract, and nothing anywhere
noticed — not the compiler, not staticcheck, not review. The concrete type
satisfies the interface either way, so there is no error to produce. Same
shape as finding 1: the failure is invisible because nothing was ever asking
the question.

## 4. Checked and clean

- **Leader epochs, high watermark.** Replication vocabulary, but the log is the
  only layer that knows the offset an epoch's data ends at and the only one that
  can keep it consistent with the data across a restart. Correctly placed.
- **`CleanSpec`'s `Aborted`/`StripBelow`/`StripHeaders`.** Mechanism here, policy
  in the caller. Already argued in `docs/layering.md`, and v0.76.0 is the
  recorded cost of this package holding an opinion about them. No change.
- **`descriptor` vs `stream-incarnation`.** Looked like the same fact twice.
  It is not: the descriptor is the log's physical shape, the incarnation is which
  lifetime of a cluster-assigned *name* the bytes belong to. commitlog has no
  concept of a name. Correctly split.
