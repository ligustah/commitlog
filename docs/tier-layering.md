# Tier layering: agreed changes, not yet made

Three parts of the tier surface exist because of how the API grew, not because
a log needs them. All three are agreed with the consumer that drove them, and
all three are breaking.

**Ship them as ONE release.** Each is breaking on its own, and consumers have
already absorbed two same-day reversals; three more migrations would be worse
than one.

## 1. Remove `ExportTierState` / `ImportTierState`

They exist only so a caller could carry commitlog's segment index through its
own consensus. Since the tier manifest (v0.36.0) the store describes itself and
a log adopts what it finds on open, so these are a second way to do what now
happens by itself.

`TierObject` stays — `TierManifest()` returns it.

Mechanical. The only care needed is in tests that use `ImportTierState` to set
up a log with tier state: they should delete the manifest and let adoption not
happen, or be rewritten around adoption.

## 2. Reclaim superseded objects internally

`CleanWithSpec` returns superseded keys and `DeleteStoreObjects` removes them,
which makes the caller responsible for commitlog's own garbage. That is not a
boundary, it is an evasion: the reason it is exported is that a rewrite cannot
tell when an in-flight reader has finished with the object it replaced, and
handing the keys upward passes that problem to someone with even less
information.

The log knows its readers and should track them.

**Design.** A `storeBacking` is handed out to whoever is reading a segment, and
a rewrite swaps the segment's backing for one over the new object. The
superseded object is safe to delete once no reader still holds the old backing.
So the backing needs a reference count: taken when a read path acquires it,
released when that read finishes, and a rewrite queues the old key for deletion
once the count reaches zero.

Deletion then happens on a later pass rather than inline — the queue is drained
at the start of the next clean, by which point most readers are long gone, and
anything still pinned waits another pass.

Two things to get right, both learned the hard way elsewhere in this package:

- The count must be taken under the same lock that swaps the backing, or a
  reader can acquire a backing the rewrite has already decided is unreferenced.
- Draining must not delete an object the manifest still names. Publish the
  manifest first, then delete, so a crash between them leaves an orphan rather
  than a dangling reference. `UnreferencedObjects` already reports orphans.

`DeleteStoreObjects` and `UnreferencedObjects` can stay as operator tools for
the shared-store case, where garbage this log did not create can still appear.
They stop being part of the normal path.

## 3. Merge `CleanSpec.SkipTiered` into `TierReadOnly`

The same idea at two scopes: one says "not this pass", the other "not this
process". A caller that wants to skip the tier for one pass can set the mode
around it, and the per-pass flag then has no reason to exist.

Keep `TierReadOnly`, which is the one that expresses ownership.

## Sequencing with consumers

A consumer needs `SegmentStore.Stream` and `StreamPays` (v0.35.0) before any of
this, since that is also breaking and unrelated. Landing that first means one
migration rather than two overlapping ones.

Nothing here blocks — or is blocked by — the compaction ceiling issue, which is
not a commitlog change: an over-advanced `Ceiling` is invisible to the log by
construction (see `tla/README.md` and `TestCleanSpecCeilingAboveUndecidedLosesKey`).
