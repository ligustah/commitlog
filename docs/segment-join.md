# Joining undersized segments

The design behind `clean_join.go`. It exists because the TODO it replaces
(`compact_cleaner.go`, "join segments that are below the bytes limit") read as a
small piece of work and is not one, and because the reason it is not is worth
writing down once rather than rediscovering.

Every question this document once left open is now settled; they are recorded
under "Settled" below, with the reasoning, because in each case the losing option
was the more obvious one.

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
question `project_the_commit_point_decides_the_fix` keeps answering, and it was
expected to be the hard part of this feature.

**Locally it is not, and the reason is worth keeping.** Building the result at the
run's LOWEST base offset makes installing it the ordinary `Replace` rename over
the first input — so the commit is one rename, exactly as for a rewrite — and
makes every *other* input a segment whose range is strictly CONTAINED in the
result. `resolveSegmentOverlaps` already resolves that shape on open by keeping
the superset and deleting the duplicate; it was written for an interrupted
truncation, and a join produces the identical shape on purpose. A crash between
the rename and the unlinks therefore resolves to the old set or the new single
segment, with no marker file, no new format and no new recovery rule. That makes
it load-bearing rather than merely tidy, which the warning it logs now says.

The hard part survives intact for a TIERED run: a store has no rename, so there
the manifest write has to be the commit, adding the result and removing every
input together.

**3. Tiering.** Two segments in different tiers have no single home. A join must
either refuse across a tier boundary, or define which tier the result lands in
and move bytes to get it there — and moving bytes is the expensive operation
tier budgets exist to bound. **Settled: refuse.** A join is an optimisation, and
an optimisation that triggers a cross-store copy is not one. A read-only tier is
refused by the same rule from the other direction — it is simply left out of
`TierJoinBelow`, and an unconfigured tier is never joined.

Worth knowing before assuming a local-only first cut would be a useful subset: in
durable_streams, offload is bimodal. A log with no tier or no local retention
keeps everything local; a log with both ends up with nearly everything tiered. So
a join that handled only local segments would no-op on exactly the deployments
that have the most segments to join.

**4. Retention.** The delete cleaner removes whole segments from the oldest end,
and `RetentionFloor` protects a lowest offset. Joining A (deletable) with B (not)
produces a segment that is neither, and coarsens retention's granularity: the log
can now only free memory in bigger steps. **Settled: accepted, bounded by the
size cap** rather than by a new horizon. A `MinAge`-style floor was the obvious
guard and is the wrong one — it answers "how recent may an input be", when the
thing that hurts is "how much can one undeletable segment pin", which is bytes.
`JoinBelow` already names that number, and a joined segment is otherwise an
ordinary segment with no join-specific retention rule.

**5. Leader epochs.** The epoch cache anchors epochs at start offsets and
`ClearEarliest` re-anchors rather than drops. A join does not remove records, so
no anchor should move; this is the one subsystem that is probably untouched, and
it should be *asserted* rather than assumed. **Asserted** —
`TestAJoinLeavesLeaderEpochsWhereTheyWere` checks both halves: every epoch's last
offset as the cache answers it, and the epoch on every record as it reads back.
The reason to check at all is that a join DOES change which segment an offset
lives in, so an anchor derived from a segment boundary rather than from a record
would move with it — silently, and only for the epochs whose first record
happened to open a retired segment.

## Sketch

Order matters more than mechanism. Every step below is chosen so a crash between
any two resolves to the old state.

1. **Select** — `planJoins`, its own stage of the pass. Find maximal runs of
   *adjacent* sealed segments that share one tier and whose combined size stays
   within that tier's configured cap. Adjacency is required: a gap means an
   unjoinable segment between them, and skipping it would produce a segment whose
   offset range contains records it does not hold. A tier with no configured cap
   is not joined at all, which is also how a read-only tier stays untouched.
2. **Build.** Write a new segment at the run's **lowest** base offset, into a
   temporary name, appending every record of the run in offset order. Records are
   copied verbatim — a join drops nothing, which is what keeps it independent of
   compaction's correctness.
3. **Sync**, then the sidecars. The index needs nothing extra — it is written as
   the copy goes. The key digest must be rebuilt only AFTER the rename, and that
   is a trap worth naming: a digest's path carries no working suffix, so the
   result and the run's first input share it, and writing the digest before the
   install would clobber the input's while the input was still the live segment.
   It is rebuilt only for a log that already keeps digests, and it carries no
   strip stamp — each input's stamp was verified over its own records, so
   adopting either would extend a claim over records it never covered.
4. **Publish.** For a local run, rename into place and splice `l.segments` under
   the segment write lock — **all inputs out and A' in, in one critical section**
   (see hazard 1b: a window with both A and B still listed double-counts in
   `LocalBytes`). The stage does this by RETURNING the new list rather than
   mutating the live one; the pass swaps it in once, at the end, which is the
   single critical section. For an offloaded run, upload first and let the
   manifest publish be the commit — one manifest write that adds A' and removes A
   and B together, so the "claimed twice or by nothing" window never exists.
5. **Retire.** Mark each input `SupersededBy(A')` and only then delete it: the
   link must be in place before the close, or a reader that resolved into the
   segment in between gets the raw `ErrSegmentClosed` the link exists to turn
   into a redirect. Local files go through `Delete`, as every other segment
   retirement in the package does; store objects will go through `pendingReclaim`
   exactly as a rewrite's do, so a reader that opened B before the join finishes
   on the bytes it holds.

Budget: a join is a rewrite for costing purposes and should draw on the same
`RewriteBudget` / `TierBudgets`, after compaction's own debt — reclaiming bytes
beats reclaiming file handles.

## The tiered commit point (next)

The local case commits with a rename. A store has no rename — `Put` overwrites
unconditionally and cannot be made conditional — so the tiered join has to borrow
the substitute a tiered REWRITE already uses: write the new bytes to a key
nothing is reading, and let the MANIFEST decide which object the segment reads.
`uploadReplacement` / `swapReplacement` are that pair, split around the publish.

What a join changes about it, and the whole difficulty: a rewrite's publish names
ONE base offset before and after, so "publish per segment, never under a segment
lock" is well defined. A join's publish retires N base offsets and adds one. So
the manifest write must be a SET operation — add A', remove A…N — applied as one
write, or the window between two writes is exactly the "claimed twice or by
nothing" state hazard 2 forbids.

### The set operation is already atomic, and removal is not an override

`publishTierManifests` does not patch a manifest. It calls `tierState()`, which
walks `l.segments` and emits one `TierObject` per segment whose `store != nil`,
and `Put`s the whole body — one object write per tier. A run the planner confines
to one tier therefore changes exactly one manifest, in exactly one `Put`. The set
operation a join needs is atomic for free, for the same reason the local one was:
the mechanism that already exists has the shape the join wants.

But the two halves of that set reach the manifest by completely different routes,
and this is the part the sketch below had wrong.

**Adding A' is an override.** `pending ...TierObject` exists precisely for "an
object that is uploaded and complete but that its segment has not switched to
yet", which is what the joined result is at the moment of the commit. Because
`override` is keyed by base offset and A' keeps the run's LOWEST base offset, the
pending entry REPLACES the first input's entry rather than adding a second — the
identity choice that made the local rename work pays off a second time here.

**Removing A…N is not.** There is no removal override, because a pending entry
names an object and "stop naming this one" is not an object. The mover shows one
route — its release passes NO pending entry, and the source manifest stops naming
the segment purely because `swapTier` changed `s.tier` first — but that route is
not open to a join. `swapTier` could mutate before the commit because it repointed
the segment at objects that are real and complete. A join has nowhere to repoint
inputs 2…N to: they are about to stop existing. Clearing their tier fields before
the commit would mean a failed publish leaves segments the log still serves but no
longer believes are offloaded, and the pass would have to roll that back — which
is exactly the obligation "the publish is the commit" exists to abolish.
`swapReplacement` states the rule the other way round: *everything here is
post-commit; nothing it does can be undone by failing.*

So the removal is expressed at publish time instead, as an explicit set of base
offsets the write retires — the symmetric partner to `pending`. The two describe
the same instant, the one `writeTierManifest`'s own doc already names: the moment
of the commit, when the log's view and the tier's necessarily disagree. `pending`
is "this object is real but its segment has not switched to it yet"; retiring is
"these segments are real but their objects stop being the tier's at this write".
It is not a patch — the manifest is still rebuilt whole from `tierState`.

Ordering, with what each crash leaves:

1. Upload the joined log object (and its index object, for an offloaded index)
   under fresh keys. A crash here leaves orphans nothing points at — reclaimable
   by comparing the store's keys against the manifest, which is what a crashed
   rewrite already leaves.
2. ONE manifest write: A' as the pending entry, A₂…A\_N as the retiring set. This
   is the commit, and nothing before it has changed anything. Before it the
   manifest names the inputs and the log reads them; after it, A' alone. There is
   no state in between, which is the entire requirement.
3. Repoint the first input at A' with the existing `swapReplacement`, and retire
   the rest onto it with `SupersededBy`. All post-commit.
4. Splice the list and return it for the pass to publish in one critical section,
   exactly as the local path does.
5. Queue the superseded objects through `pendingReclaim`, AFTER the splice, so an
   object is only ever considered for deletion once a published manifest has
   stopped naming it AND nothing can still reach the segment to acquire it.

Note what this ordering does NOT need: a `MovedFrom`-style marker. A move can
leave two tiers claiming one object because it writes two manifests; a join
writes one, and every crash point above leaves the single manifest naming either
all the inputs or A' — never both, never neither. The orphaned objects a crash
leaves are unreferenced garbage, not a contested claim.

Open questions this raises:

- ~~Is the manifest write atomic over a set of entries?~~ **Yes** — one whole-body
  `Put` per tier, rebuilt rather than patched, and a run never spans two tiers.
- ~~Which base offset does the tier manifest keep?~~ **The run's lowest**, same as
  the local path, which is also what makes the pending entry a replace.
- **What does `publishTierManifests` need to take?** A retiring set of base
  offsets, applied to the rebuilt list after the overrides and before the grouping
  by tier. Base offsets are unique across a log, so the set needs no tier of its
  own. A base offset that is both pending and retiring should be REFUSED rather
  than resolved either way: A' keeps the first input's base offset and the first
  input is not retired, so an overlap is a caller who has confused which input
  survives, and both readings of it publish a manifest that is wrong.

  Small, and symmetric with `pending` — but it is a new way to be wrong about a
  manifest, so it wants the treatment the pending path just got: a test that
  watches every manifest a pass publishes, not only the one it settles on.

  Between the publish (2) and the splice (4) an input is still reachable through
  `findSegment` while the manifest has stopped naming its object. That is fine —
  the object still EXISTS, and a crash there reopens from a manifest naming A',
  which covers those records anyway. It stops being fine the moment the object
  can be DELETED: `drainReclaim`'s safety rests on "for a superseded backing,
  refs can only fall", which holds only once nothing can reach the segment to
  acquire its backing again. Hence queueing after the splice, not merely after
  the publish. An input is unreachable only when it is out of `l.segments`.

  Note that the FIRST input needs no new mechanism at all: it becomes A', and the
  existing `uploadReplacement`/`swapReplacement` pair already describes exactly
  that. Only inputs 2…N are new, which is a smaller surface than it looked.

- **Does the retiring set need to reach `writeOneTierManifest` too?** A run never
  spans tiers, so the join only ever needs one tier's manifest — but the mover is
  the only caller of the one-tier form today, and adding a second changes what
  that helper is for. Worth deciding deliberately rather than by whichever is
  fewer lines.

- **The tiered path must check `tierWritable`, not just `TierJoinBelow`.** Leaving
  a read-only tier out of `TierJoinBelow` is how it stays untouched by default,
  but nothing stops a caller naming one — and absence-as-refusal only refuses the
  callers who did not ask. Uploading into a store this log does not own is the
  thing `ReadOnly` exists to prevent, so the run has to be refused on ownership
  as well as on configuration. The local path never had to care: it writes files.

## Settled

- ~~Does anything assume `replacement` preserves the base offset?~~ No: every
  consumer bounds the resolved segment from above only, and a join's result is a
  superset of its inputs (hazard 1). The constraint it produced is a different
  one (1b): the slice splice must be atomic, because the link is many-to-one and
  one public method sums over it.

- ~~Is `MaxSegmentBytes` the right ceiling for the result?~~ **No — the caller
  sets its own, per tier**, as `CleanSpec.JoinBelow` / `TierJoinBelow`.
  `MaxSegmentBytes` is the ROLLING threshold, and rolling is a property of the
  active segment: a sealed segment is never appended to, so a joined result
  sitting above it would not be "immediately eligible to roll" — nothing would
  ever look. The cap is really about retention granularity (hazard 4) and the
  cost of a later rewrite, and only the caller knows what it will tolerate there.

  The same field settles hazard 4 without a new horizon. A joined segment is an
  **ordinary** segment: no join-specific retention rule, no `MinAge` equivalent.
  Joining a deletable segment with a non-deletable one does coarsen retention —
  that is accepted, and the cap is what bounds how coarse it can get.

- ~~In `compact()`, or a separate pass?~~ **Its own stage.** Neither existing
  pass reaches every log: `compact()` and `consolidateSegments` are the two
  branches of `if l.Compact`, so a join placed in either would only ever join
  half the logs — and both accumulate segments. It runs after compaction's debt,
  drawing on the same `RewriteBudget` / `TierBudgets`, because reclaiming bytes
  beats reclaiming file handles.

- **Triggering is caller-driven**, like everything else on `CleanSpec`: a join is
  worth doing when load is low, and commitlog cannot see load.

- **Per-tier configuration does not fall back.** `TierBudgets` falls back to
  `RewriteBudget` for a tier it does not name; `TierJoinBelow` deliberately does
  not, because absence is how a read-only tier stays untouched without having to
  be named, and a fallback would join into a store the log may not write to.

## Status

Built for LOCAL runs; tiered runs are planned and skipped.

`clean_join.go` holds all of it: `planJoins` (step 1), `joinOne` (steps 2–3 and
5) and `joinSegments`, which is the stage `clean()` runs after both arms of `if
l.Compact` and whose return value is the splice (step 4).

The local commit point turned out to need no new mechanism, which is the one
genuinely pleasant surprise in this feature. Building the result at the run's
LOWEST base offset makes installing it the ordinary rename over the first input,
and makes every other input a segment strictly CONTAINED in the result — the
exact state `resolveSegmentOverlaps` already resolves on open, keeping the
superset. It was written for hazard-shaped-like-a-truncation; a join produces the
identical shape on purpose, and `TestAJoinInterruptedBeforeItsInputsAreGoneResolvesOnOpen`
reconstructs the window to prove it.

What remains is hazard 2 in its tiered form: a store has no rename, so a tiered
run needs one manifest write that adds the result and removes every input
together. `joinOne` refuses an offloaded input outright until that exists —
`joinSegments` never offers it one, and the refusal is there so a later caller
cannot wander in.
