# Joining undersized segments

A design document, not an implementation. It exists because the TODO it replaces
(`compact_cleaner.go`, "join segments that are below the bytes limit") read as a
small piece of work and is not one, and because the reason it is not is worth
writing down once rather than rediscovering.

## The problem

Compaction only ever shrinks a segment. It rewrites a sealed segment keeping the
latest record per key, dropping aborted records, expired tombstones and control
markers — and the result replaces the original at the same base offset. Nothing
ever grows a segment back or merges two.

So a long-lived compacted log converges on many small segments. Each one costs a
file set (`.log`, `.index`, and where present `.keys` and `.blocks`), an open
handle and a mapping, and a slot in every linear walk over `l.segments` —
`clean()`, retention, `tierState()`, the reader's segment search. durable_streams
reports 336-segment logs. The bytes are fine; the per-segment overhead is the
cost, and it is paid on every pass forever.

`consolidateSegments` is **not** this. It rewrites the blocks *within* one
segment when the block layout has fragmented, and leaves segment boundaries
exactly where they were.

## Why it is not just another rewrite

**A segment is identified by its base offset.** Not "has" one — *is* identified
by it. The base offset is:

- the file name stem (`00000000000000000042.log` and its siblings);
- the key under which the tier manifest records an offloaded segment
  (`TierObject.BaseOffset`), and the key `CleanSpec.TierPlacement` uses to name a
  segment a caller wants moved;
- the name the index and key-digest sidecars are derived from;
- the search key for "which segment holds offset N", which is a walk over
  `l.segments` comparing base offsets.

Joining segments A and B means **A's base offset survives and B's ceases to
exist**. Every one of the above has to agree about that, and agree at one moment.

The rewrite path never faces this, and that is precisely why it is safe: a
replacement **keeps its predecessor's base offset**. That is what lets
`Replace()` swap a segment under live readers, what lets `current()` redirect a
reader by following the replacement link, and what lets a manifest publish name
the same `BaseOffset` before and after. A join breaks the invariant every one of
those mechanisms is built on.

## The specific hazards

**1. Readers hold the disappearing segment.** `current()` resolves a stale
segment by following `replacement`, and reports `ok=false` for one that is gone
with no successor. A joined-away segment B is neither: its records still exist,
in A. So B's `replacement` must point at A' — a link that, for the first time,
points at a segment with a *different* base offset.

That link turns out to be safe, and it is worth saying why, because the reason is
narrower than "nothing assumes the base offset". Every consumer of `current()`
bounds the resolved segment only from ABOVE. `findSegment` searches on
`NextOffset() > offset` and then re-applies exactly that predicate to the
resolved segment; the re-check exists because a *rewrite* can end BELOW where its
source did, leaving an offset in the gap belonging to the next segment. A join is
the opposite case: A' spans `[A.base, B.next)`, a superset of both inputs, so an
offset that resolved into either input still satisfies the upper bound and lands
in a segment that genuinely holds it. Nothing anywhere re-checks a lower bound,
which is precisely the check a differing base offset would fail.

**1b. But the link is many-to-one, and one walk accumulates.** This is the real
finding, and it is not the one the hazard was written to expect. During the
window where `l.segments` still holds both A and B, both resolve to A'. Callers
that resolve and *select* are unaffected — `OldestOffset` returns the first
survivor, `findEntryByTimestampResolving` resolves a single segment. `LocalBytes`
resolves every entry and **sums** `seg.Position()`, so it would count A' once per
input and report a log using more local bytes than it does. It is a public method
(`interface.go:308`) with no in-repo caller, so the wrong answer would leave this
repo silently.

The conclusion is a design constraint, not a bug to fix later: **the swap in
`l.segments` must replace both entries with one, atomically, under the segment
write lock.** The replacement link may not be used to carry readers through a
window in which both entries are still present. The link exists only for readers
holding a direct `*segment` pointer taken before the swap — which is what it is
for in the rewrite path too. `replacementDepth` bounds chain length, so chains
are safe; the many-to-one shape was the risk, not the loop and not the offset.

**2. The commit point.** A join publishes one new segment and retires two. The
existing rule (see `docs/multi-store-tiering.md`) is that the manifest publish is
the commit point for an offloaded segment, and that it happens per segment and
never under a segment lock. A join has *two* inputs, so "per segment" is
ill-defined: there is a window where A' is written but the manifest still names A
and B, and a crash there must resolve to either the old pair or the new single —
never to a state where an offset is claimed twice or by nothing. This is the same
question `project_the_commit_point_decides_the_fix` keeps answering, and it is
the hard part of this feature.

**3. Tiering.** Two segments in different tiers have no single home. A join must
either refuse across a tier boundary, or define which tier the result lands in
and move bytes to get it there — and moving bytes is the expensive operation
tier budgets exist to bound. Refusing is almost certainly right: a join is an
optimisation, and an optimisation that triggers a cross-store copy is not one.
The same argument refuses a join where either input is in a read-only tier.

**4. Retention.** The delete cleaner removes whole segments from the oldest end,
and `RetentionFloor` protects a lowest offset. Joining A (deletable) with B (not)
produces a segment that is neither, and coarsens retention's granularity: the log
can now only free memory in bigger steps. A floor on how recent a segment may be
before it is eligible to join — the `MinAge` horizon already does this for
compaction — is the natural guard.

**5. Leader epochs.** The epoch cache anchors epochs at start offsets and
`ClearEarliest` re-anchors rather than drops. A join does not remove records, so
no anchor should move; this is the one subsystem that is probably untouched, and
it should be *asserted* rather than assumed.

## Sketch

Order matters more than mechanism. Every step below is chosen so a crash between
any two resolves to the old state.

1. **Select.** In the classification pass in `compact()`, after `disp[]` is
   built, find maximal runs of *adjacent* segments where: all are
   `keepConverged` or freshly rewritten (never `keepProtected`, never deferred),
   the combined size is below `MaxSegmentBytes`, all share one tier, none is in a
   read-only tier, and the newest write in the run is older than the `MinAge`
   horizon. Adjacency is required: a gap means an unjoinable segment between them.
2. **Build.** Write a new segment at the run's **lowest** base offset, into a
   temporary name, appending every record of the run in offset order. Records are
   copied verbatim — a join drops nothing, which is what keeps it independent of
   compaction's correctness.
3. **Sync**, then build the index and key digest for the result. A join must not
   leave a segment whose sidecars have to be rebuilt on the next open.
4. **Publish.** For a local run, rename into place and splice `l.segments` under
   the segment write lock — **all inputs out and A' in, in one critical section**
   (see hazard 1b: a window with both A and B still listed double-counts in
   `LocalBytes`). For an offloaded run, upload first and let the manifest publish
   be the commit — one manifest write that adds A' and removes A and B together,
   so the "claimed twice or by nothing" window never exists.
5. **Retire.** Mark each input `SupersededBy(A')`. Queue the replaced files and
   store objects through `pendingReclaim` exactly as a rewrite does, so a reader
   that opened B before the join finishes on the bytes it holds.

Budget: a join is a rewrite for costing purposes and should draw on the same
`RewriteBudget` / `TierBudgets`, after compaction's own debt — reclaiming bytes
beats reclaiming file handles.

## What to settle before writing code

- ~~Does anything assume `replacement` preserves the base offset?~~ **Answered —
  see hazard 1.** No: every consumer bounds the resolved segment from above only,
  and a join's result is a superset of its inputs. The constraint it produced is
  a different one (1b): the slice splice must be atomic, because the link is
  many-to-one and one public method sums over it.
- Is `MaxSegmentBytes` the right ceiling for the *result*, or should a join stop
  well below it so the joined segment is not immediately eligible to roll?
- Should a join happen in `compact()` at all, or in a separate pass like
  `consolidateSegments`, which already exists for logs without compaction and
  would otherwise never join anything?

## Status

Not scheduled. Filed so the constraint is recorded rather than rediscovered; the
TODO in `compact_cleaner.go` now points here in spirit, and the per-segment
overhead this would reclaim is real but not currently hurting anything measured.
