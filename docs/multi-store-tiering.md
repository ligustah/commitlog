# Multi-store tiering — design

A log has exactly one store. `Options.SegmentStore` is a single field, so bytes
descend from local disk into that store and are deleted from it, and there is no
second hop.

durable_streams asked for the second hop on 2026-08-10. Approved for build the
same day, against this doc rather than against a size estimate.

Written against v0.61.2.

## What the caller actually needs

durable_streams already ships `TierSpec` as stream config — `Name`,
`ReplicateAfter`, `EvictAfter`, `Budget` — validated, persisted and
conflict-checked. Nothing reads it, because there is nothing below it to read it
with.

Their hard requirement is narrow, and it is worth quoting because it constrains
the design more than the feature does: per-segment descent must become
**expressible**, and must NOT become **scheduled** down here. commitlog gains no
internal clock for descent, for the same reason it runs no automatic cleaner on
these logs — it would be deciding *when* without the context that makes the
decision safe.

So: the caller names the destination, commitlog moves the bytes and records who
owns them.

## The API

```go
type Tier struct {
    Name  string        // stable identity; matches durable_streams' TierSpec.Name
    Store SegmentStore

    // Retention, per tier. Today's MaxTierBytes/Messages/Age are these for the
    // one tier that exists.
    MaxBytes    int64
    MaxMessages int64
    MaxAge      time.Duration

    // ReadOnly is per tier because ownership is. A node can own the tier it
    // writes and not the archive below it.
    ReadOnly bool
}

// Options
Tiers []Tier   // ordered, nearest first; replaces SegmentStore
```

`CleanSpec` gains placement and per-tier budgets:

```go
// TierPlacement names, per segment base offset, the tier that segment should
// live in after this pass. A segment absent from the map does not move.
TierPlacement map[int64]string
// TierBudgets bounds rewrite time per tier, replacing the single
// TierRewriteBudget.
TierBudgets map[string]time.Duration
```

Keyed by base offset because that is already how `TierObject` identifies a
segment, and by tier NAME rather than index because indices renumber when a
caller edits its tier chain and a renumber must not silently redirect bytes.

A placement naming an unknown tier is an error, not a no-op — the same rule
`CleanSpec.Ceiling` now follows. A spec whose intent cannot be honoured must
fail loudly rather than be partially applied.

## The manifest is the hard part

Everything above is a morning's work. The cost is here.

`tierManifest` today is `{Version, Segments []TierObject}`, and a `TierObject`
says which object holds a segment's log, index and blocks. It does not say
which STORE, because there has only ever been one.

`TierObject` gains `Tier string`. The manifest is rebuilt from the log's own
segments rather than patched (see `writeTierManifest`), so adding a dimension
costs nothing structurally — the rebuild cannot drift the way a patch could.

### Where the manifest lives

Two options, and this is the one genuine fork in the design.

**(a) One manifest, in tier 0, naming every object with its tier.** Open does
one read. Offset routing sees the whole picture at once. "Which tier owns this
object" is a fact that cannot be split, so two stores can never disagree about
it, and no object can be named by two manifests or by none.

**(b) One manifest per tier, naming that tier's objects.** Open reads N and
merges. A segment is in exactly one tier, so the merge is a union with a
uniqueness check — and that check is the point: it can fail.

**Decided: (b), per-tier manifests.** The first draft of this doc recommended
(a) on the strength of the uniqueness argument. That was wrong, and the reason
is written down in this repo already — `docs/tier-layering.md`: *"the store
describes itself and a log adopts what it finds on open."* That is the principle
`ExportTierState`/`ImportTierState` were REMOVED to establish. Option (a) breaks
it the moment there is a second store: tier 1 would hold bytes it cannot
describe, and a log adopting tier 1 alone would find nothing to adopt.

The concrete costs of (a) follow from that. Losing tier 0 loses the map to
tier 1's objects even though they are intact — a bad property for the cold
archive that is the whole point of a second hop. And `CopyTier` exists for
handover, which presumes a tier is individually portable; under (a) copying
tier 1 hands over objects with no manifest.

What (a) genuinely buys is that two stores can never disagree about who owns an
object. Under (b) that disagreement is representable, so the merge at open needs
a uniqueness check that REFUSES — two manifests naming the same base offset is
not a state to resolve by picking one, and it needs its own guard. That is the
price, and it is a check we can write rather than a property we would lose.

### The descriptor

`readStoreDescriptor` answers "does this log exist, and what is it" — and
`logIsNew` asks the STORE rather than the directory, deliberately, because a
node adopting a tier has a full store and an empty directory. That is the one
moment a process picks up someone else's log, and it must be checked.

Every tier gets a descriptor, for the same reason every tier gets a manifest: a
store that cannot say which log it belongs to is not self-describing. `logIsNew`
then asks the tiers in order and takes the first answer, so a node adopting any
single tier is checked against that tier's recorded settings rather than
skipping the check.

Descriptors disagreeing across tiers is the same class of fault as manifests
disagreeing, and gets the same treatment: refuse, do not reconcile. It means one
store was attached to the wrong log, and guessing which is worse than stopping.

## Every consumer that follows

Each of these is `l.SegmentStore` today and becomes "which tier":

| site | today | becomes |
|---|---|---|
| `commitlog.go:630` open | `readTierManifest(l.SegmentStore)` | read every tier and merge, refusing a duplicate base offset |
| `commitlog.go:1682` offload | `s.uploadTo(l.SegmentStore, …)` | upload to the placed tier |
| `manifest.go:237` attach | opens the object | pick the store from `TierObject.Tier` |
| `segment.go` rewrite | `uploadReplacement`/`swapReplacement` | fresh key in the segment's CURRENT tier |
| `tier_state.go:224,245` reclaim | `l.SegmentStore.Delete(key)` | delete from the owning tier |
| `tier_state.go:259` orphan sweep | `l.SegmentStore.List()` | list per store, compare against that tier's slice of the manifest |
| `descriptor.go:278-374` | descriptor read/write | one per tier; `logIsNew` asks in order |
| `clean.go:376` retention | `LocalRetentionAge` gate | unchanged: this is the local→tier-0 hop |
| retention budgets | `MaxTierBytes/Messages/Age` | per tier |
| `TierReadOnly` | one flag | per tier |

`CopyTier` is the good news: it is store-to-store and tier-agnostic already, so
it does not change. A log-level handover becomes "copy each tier", which is a
caller loop, not new mechanism.

## An asymmetry worth deciding on purpose

`LocalRetentionAge` schedules the first hop — local disk to tier 0 — inside
commitlog, on commitlog's clock. Every hop below it would be caller-driven.

That is defensible: the first hop is about local disk pressure, which commitlog
can see and the caller cannot, whereas descent between stores is a policy
question about cost and durability that only the caller has context for. But it
means the answer to "when do bytes move" is `LocalRetentionAge` for one hop and
`CleanSpec` for the rest, and nobody reading the API cold will guess that.

Either keep it and document the split at both ends, or make hop 0 spec-driven
too and let `LocalRetentionAge` become a caller-side clock. The second is more
consistent and is a bigger break.

## Staging

This can land incrementally, which materially lowers the risk:

1. `TierObject.Tier`, written and read, with exactly one tier configured. No
   behaviour change; the field is always the same value. Manifest version bump.
2. `Options.Tiers` replacing `SegmentStore`, still length 1 enforced. Every
   consumer above moves to "ask which tier" while the answer stays constant.
3. Lift the length-1 restriction; add `CleanSpec.TierPlacement` and
   `TierBudgets`. This is the first step where a segment can actually move
   between stores, and the first that needs new guards.

Steps 1 and 2 are mechanical and independently releasable. Step 3 is where the
real work and the real risk are.

## What stays out

- **No clock for descent.** Explicitly requested, and consistent with there
  being no automatic cleaner on these logs.
- **No scheduling policy.** commitlog does not know what a tier costs.
- **No migration converters.** Breaking is cheap here; the manifest version bump
  is a refusal to read an old one, not a translation of it.
