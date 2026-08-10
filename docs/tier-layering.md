# Tier layering

Three parts of the tier surface existed because of how the API grew, not because
a log needed them. All three were breaking, all three shipped in ONE release,
and this records what they were and why the result is shaped as it is.

## 1. `ExportTierState` / `ImportTierState` — removed

They existed only so a caller could carry commitlog's segment index through its
own consensus. Since the tier manifest (v0.36.0) the store describes itself and
a log adopts what it finds on open, so these were a second way to do what now
happens by itself.

`TierObject` stays — `TierManifest()` returns it.

## 2. Superseded objects — reclaimed internally

`CleanWithSpec` used to return superseded keys, and `DeleteStoreObjects` removed
them. That made the caller responsible for commitlog's own garbage, and it was
not a boundary but an evasion: the reason those keys were exported at all is
that a rewrite cannot tell when an in-flight reader has finished with the object
it replaced, and handing them upward passed that problem to someone with
strictly less information.

The log knows its readers, so it tracks them.

**How it works.** A `storeBacking` is handed to whoever reads a segment, and a
rewrite swaps the segment's backing for one over the new object. The backing
carries a reference count: taken when a scan acquires it, released when that
scan closes. A rewrite queues the superseded key together with the backing that
was serving it, and the queue is drained at the START of a later clean pass — by
which point most readers are long gone, and anything still held waits another
pass. `CleanWithSpec` no longer returns keys.

Two things this has to get right, both learned the hard way elsewhere in this
package:

- The count is taken under the same lock that swaps the backing. Acquiring
  outside it would let a reader take a backing a rewrite had already judged
  unreferenced. Deferring the drain makes that window nearly unreachable in
  practice — a reader would have to be descheduled across a whole subsequent
  pass — so no test pins it, and the ordering is argued rather than demonstrated.
  It costs nothing, so the code takes it anyway.
- Draining never deletes an object the manifest still names. Each pass
  republishes the manifest before queueing, and a pass whose publish FAILED sets
  `tierManifestStale`, which holds reclamation off entirely. A crash between the
  two leaves an orphan rather than a dangling reference — storage, not
  correctness — and `UnreferencedObjects` reports it.

**What reclamation will not touch.** Only objects this log uploaded and its own
rewrite replaced. Not "unreferenced by me", which is the judgement that is
unsafe on a shared store, since it counts everything another live process wrote.
Reclamation is suppressed entirely while the tier is read-only, a delete being a
store write like any other; the queue is held across that, not dropped, because
going read-only is a handover rather than a licence to forget.

Removal is a separate matter from rewriting, deliberately. Retention dropping a
segment, or compaction finding every record in one superseded, takes that
segment's objects with it immediately, and a reader gets `ErrSegmentClosed` from
the segment. Those records are gone by decision. A rewrite decides nothing of
the sort — the records survive, only the object holding them changed — which is
why a rewrite may not break a reader and a removal may.

`DeleteStoreObjects` and `UnreferencedObjects` remain as OPERATOR TOOLS for the
shared-store case, where garbage this log did not create can still appear. They
are no longer part of the normal path.

## 3. `CleanSpec.SkipTiered` — merged into tier ownership

The same idea at two scopes: one said "not this pass", the other "not this
process". Offering both invited a caller to set them to disagree. Whether a log
may write to a store is a property of the LOG, so the ownership flag — since
moved onto each tier as `Tier.ReadOnly`, with `SetTierReadOnly` to change it —
is what remains, and `CleanWithSpec` derives the per-pass behaviour from the
log's current modes.

## Sequencing with consumers

A consumer needed `SegmentStore.Stream` and `StreamPays` (v0.35.0) before any of
this, since that was also breaking and unrelated. Landing it first meant one
migration rather than two overlapping ones.

Nothing here blocks — or is blocked by — the compaction ceiling issue, which is
not a commitlog change: an over-advanced `Ceiling` is invisible to the log by
construction (see `tla/README.md` and `TestCleanSpecCeilingAboveUndecidedLosesKey`).
