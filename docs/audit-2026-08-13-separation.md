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

**Proposal (needs durable_streams' agreement, not filed as a commitlog task):**
let `Options` carry opaque client identity bytes, written with the descriptor at
creation. "Unstamped" then stops being a state that exists rather than a state
that is tolerated forever.

### 3b. Two reserved-name lists guard one directory, from two repos

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
