# Tiered storage compaction — the commitlog side

A design document, not an implementation. It covers only what cannot live above
this library; the tier chain, the replication and eviction clocks, and the
per-tier budgets are policy and belong in the layer that already owns them.

## The problem

An offloaded segment is permanently exempt from maintenance. `clean()` holds the
offloaded prefix aside as an immutable oldest prefix and prepends it back
afterwards, because the rewriters build a local working segment and rewrite in
place — there is nothing for them to rewrite when the bytes live in a store.

Two consequences follow, and the second is the one that matters:

- Those bytes are invisible to retention as well. The delete cleaner never sees
  the prefix, so offloaded segments count toward none of `MaxLogBytes`,
  `MaxLogMessages` or `MaxLogAge`, and only `TruncateBefore` can remove them.
- **Whatever garbage is in a segment when it offloads is frozen there forever.**
  A tombstone that offloads before it can be collected never takes effect, and
  every value it shadows is preserved with it.

For a consumer whose deletes are heavy and whose tombstones carry a minimum
retention window, that is a correctness problem rather than bloat: the reason to
offload is to reach durable storage as soon as a segment seals, which is exactly
when the tombstone is youngest and least collectable.

The goal is that **garbage never settles below the tier it was created in**.

## What commitlog must gain

Only two things. Everything else the design needs already exists here or belongs
above.

1. **A rewrite that stays correct while a copy of the segment exists in a
   store.** Today a rewrite is safe partly because offloaded segments are never
   rewritten — that immutability is load-bearing, and
   `offload_compaction_fuzz_test.go` bases its own correctness argument on it.
2. **Retention that drops one tier's copy** rather than deleting the record.
   Data is gone only when the last tier's limit is reached; a log with a single
   tier keeps exactly today's delete-on-limit behaviour.

Deliberately NOT here: the ordered chain of tiers, the two clocks (replicate on
one schedule, evict on another, so a segment lives in several tiers at once),
and the per-tier budgets. Those generalise what the policy layer already does
with its store factory and its local-versus-remote retention split.

A separate tiered-commitlog package was considered and rejected: per-tier
compaction needs `compactCleaner`, `mergeDigests`, `segment.Replace` and
`storeBacking`, all unexported. Exporting them would be a larger and worse API
surface than the feature itself.

## Why digests, not an LSM merge

An LSM merges levels because they overlap in key space, and the merge is what
resolves which version of a key wins.

That is not the situation here. There is **one global offset order, identical in
every tier**. A tier holds *copies* of offset ranges, never a divergent version
of one, and a rewrite is 1:1 per base offset — the assembly phase in
`compact_cleaner.go` replaces `segments[i]` with `rewritten[i]` and merges
nothing. Recency is therefore settled by offset alone, and the persisted `.keys`
digests can decide what is superseded without reading a byte of slow storage.

A merge would buy no correctness and would pay for it in slow-tier rewrites.

Note this argument does **not** rest on tiers partitioning the offset range —
they do not, since the replication model deliberately keeps one segment in
several tiers at once. It rests on every copy of a given base offset being the
same offset range.

## Descent is gated on convergence

The first durable tier accepts every sealed segment immediately, because
durability is the point: get the bytes off a single disk's fate as soon as the
segment seals. Garbage may therefore *transit* that tier.

**Every tier below it receives only already-compacted segments.** That is what
stops short-lived data from settling deep in the chain, and it generalises a
rule that already exists rather than inventing one: `OffloadBefore` already
refuses a segment still owing block consolidation, on the same reasoning — a
pure byte copy would freeze a bloated layout into the tier.

A rewrite obsoletes the copies in other tiers. They must be deleted and the
compacted result re-replicated promptly, so the durability gap stays as small as
it is at seal.

**A rewrite can empty a segment entirely** — `cleanupEmptySegment` removes both
the working copy and the source — so descent has to delete stale objects, not
merely overwrite them. Overwriting alone leaves an orphan whenever the rewrite
produces nothing.

## Tombstones

A tombstone retires in the slowest tier that still holds anything it shadows. It
may only be dropped once no older value of its key physically exists anywhere,
or a later scan resurrects the row.

This is the ordering rule compaction already enforces locally, lifted to the
chain. `mergeDigests` flags the tombstone-GC segments via `gcSegs`, and
`CompactSpec` enforces the order — the `lateRewrite` closure marks any segment
that removes a governing record, and a stable sort puts those last, in ascending
offset order, so a governor is never removed ahead of what it governs.

Tombstones ride down the chain like any other record and always sit above what
they shadow, so they never accumulate on fast disk.

**Archival is terminal for automatic reclaim.** Bytes that cannot be read live
cannot be rewritten, so a tombstone shadowing archival data is retained
indefinitely. Convergence-on-descent does not prevent this and does not need to:
a row inserted years ago and deleted today produces a fresh tombstone over
long-settled data. Deep tiers are cheap and a stranded tombstone is a key plus a
header. Deliberate erasure of archival values is a separate, explicitly
triggered concern and out of scope.

## The invariant, and the three things that currently violate it

**A reader must never splice pre- and post-compaction bytes.**

Local rewrites get their atomicity from `Replace`'s rename over the same path. A
store `Put` is not equivalent, and three specific mechanisms in the current code
assume tiered segments are immutable. Each was checked against the source rather
than assumed:

| # | Mechanism | Why it breaks under rewrite |
|---|---|---|
| 1 | `segmentStoreKey` is `%020d` of the base offset plus the log suffix — **no generation** | A rewrite re-`Put`s over the live key. There is no version a reader can pin, so the object can change underneath an in-flight read. |
| 2 | `storeBacking` holds a 1 MiB read-ahead buffer (`buf`, `bufOff`) for the backing's lifetime, refilled only on a miss | There is **no invalidation path at all** — the type has `ReadAt`, `readOne`, `refill`, `Write`, `Size`, `Sync`, `Close`, `Name`, and nothing else. A backing opened before a rewrite keeps serving pre-rewrite bytes from its buffer. |
| 3 | `RemoteIndexCache` evicts by LRU only | `acquire`, `fetch`, `release`, `evictLocked`, `Close` — there is no invalidate-by-key. A cached index outlives the object it describes. |

Whatever mechanism is chosen, it must satisfy both halves:

- the swap is **atomic from a concurrent reader's point of view**, and
- it leaves **no stale cached extent servable** — neither the backing's
  read-ahead window nor a cached remote index.

Generation-stamped keys address (1) and make (2) and (3) tractable, since a
reader pinning a generation can be allowed to finish against the old object
while new readers open the new one. That is a direction, not a decision; the
constraint above is the requirement.

## Two things this mechanism must be given, not decide

Both come from the policy layer's companion document, and both are right.

### No internal clock for tiered descent

commitlog must not run a background loop moving segments down the chain. This is
the same rule `DisableAutoClean` already encodes for compaction: the internal
cleaner has no transaction awareness, so a pass it started on its own could
compact across an open transaction's range. The layer above is the only one that
knows whether a pass is safe *yet*.

This is already the status quo for tiering — `OffloadBefore` is caller-driven,
and there is no internal offload loop — so the requirement is to keep it that
way rather than to change anything. Worth stating explicitly, because "descend
on a timer" is the obvious thing to add and it would quietly move the *when*
decision to the layer that cannot make it safely.

### Exactly one writer per store key

The invariant above is written from a single process's point of view: it says a
*reader* must never splice pre- and post-compaction bytes. That is not
sufficient once replicas share a tier. Two nodes rewriting the same base offset
into one store is a different and harder problem, and not one this mechanism
should be asked to solve — ownership has to be granted above.

**What makes it urgent is that the violation is silent.** `SegmentStore.Put`
overwrites any existing object and there is no conditional or compare-and-swap
form, so two compactors racing on one key produce a lost update with no error
reported to either. Nothing in the read path can detect it afterwards, for the
same reason a rewrite is invisible to offsets: both nodes still agree on every
offset and disagree only about which records remain readable at them.

**Generation-stamped keys can make it loud instead.** They are already the
direction for the reader invariant, and if a rewrite writes a *new* key rather
than overwriting the live one, two competing compactors produce two distinct
objects instead of one corrupted object. The conflict becomes detectable, and
the loser's object is discardable. That does not move ownership into commitlog —
it stays policy — but it turns a silent corruption into something a caller can
notice, which is worth having even when ownership is working correctly.

One refinement to the tombstone-divergence concern, since it affects how much
the retention window must be widened: the GC horizon is a comparison of a
record's own timestamp against `now - CompactTombstoneRetention` at pass time.
Two nodes therefore apply the *same* rule and differ only in how far each has
got — a node that ran later has collected a superset of what an earlier one did,
never a different set. The divergence is one-directional and bounded by the lag
between passes, which is what makes widening the window by worst-case
replication lag a sound fix rather than an approximation.

## What changes in `clean()`

The offloaded-prefix exclusion goes away: tiered segments stop being exempt from
maintenance, and their bytes start counting toward the tier's budget.

That exclusion is currently doing real work, so removing it is the step that
depends on everything above. Until the invariant holds, the exclusion is what
keeps the rewriters correct.
