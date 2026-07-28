# Key-prefix-filtered read — what the digest can and cannot do

Notes for durable_streams #102 (sqlcdc dynamic rebalancing: migrate a segment by
state-transferring its operators' keys instead of replaying input history).

Status: **analysis only, nothing agreed, nothing built.** Written so the read is
shaped against what the digest actually is rather than against a description of it.

## What holds

The per-segment key digest (`<base>.keys`, `keydigest.go`) really does carry, for
every keyed data record in a sealed segment: offset, tombstone flag, header
presence and timestamp — **sorted by key**. A prefix is therefore a contiguous
range in that order, and the tombstone flag means live-vs-deleted is answerable
without reading records. Fetching only the matching records, rather than scanning
the compacted log, is sound.

## Two things that do not hold

### 1. The keyed section streams; it is not addressable

`digestIter` (`keydigest.go:449`) seeks **once** to `keyedOff` and then decodes
varints sequentially through a `bufio.Reader`. Keys are length-prefixed with no
offset table over them, so there is no way to jump to a key.

Sorted order lets a prefix read **stop early. It does not let it start late.**

Per segment the decode cost is therefore proportional to the number of keys up to
the *end* of the prefix range — not to the number of keys *in* it. For a prefix
late in key order that is the entire keyed section.

Consequence for the estimate: sqlcdc's measured ~40× reduction is in **bytes
fetched from the log**, and that survives. Digest decode is a separate cost that
does not, and a rebalance walks every segment.

### 2. The active segment has no digest at all

`compact_cleaner.go:615` sets `sealed := i < len(segments)-1`, and `:644` passes
that same flag as `persist`. Only sealed segments load or write a sidecar. The
active tail is rebuilt by `buildKeyDigest` — a **full segment scan** — on every
clean, and never persisted.

So the tail is not "an in-memory digest, served differently". It is the expensive
case, paid per read unless the read is bounded to sealed segments or the result is
cached.

## What would make it cheap

A **sparse key index** in the digest: every Nth key → byte offset into the keyed
section, so the prefix locate becomes a real seek instead of a scan-from-start.

This is additive and the format is already versioned (`digestVersion byte = 1`,
`keydigest.go:36`), so old sidecars stay loadable and are simply rebuilt. Whether
it is *required* or merely nice depends on the read shape — specifically on
whether prefixes are typically early or late in key order, which is a question for
sqlcdc's key layout, not for me.

## The constraint I will hold to

The digest is documented as an optimisation that is **"never required for
correctness, only to avoid the scan"** (`keydigest.go:28`). Validity is bound to
the segment's `.log` byte size, and a missing, corrupt or stale sidecar simply
returns `nil` from `loadKeyDigest` so the caller rebuilds.

A state-transfer read would make it load-bearing: its output becomes the migrated
operator's state, not a hint. So the read must be **defined to return the same
answer with no digest present**, falling back to a scan.

Without that, a stale or corrupt sidecar silently ships incomplete state to a
migrating segment. Tombstone-vs-live is where this bites hardest — a dropped
tombstone resurrects a deleted row in the destination, and nothing errors.

## Agreed shape (durable_streams, 2026-07-28)

Both corrections above were accepted. The resolved answers:

**Bounded to sealed segments.** The read never touches the active tail; it returns
the offset it is complete through, and the caller tails normally from there —
snapshot-then-tail. This removes the expensive case in §2 entirely and gives the
migrating operator a defined consistency boundary rather than a fuzzy one.

**Signature** (durable_streams' surface, not commitlog's):

```
ReadKeyPrefix(ctx, stream string, partition int32, prefix []byte, upTo int64)
    (records, completeThrough int64, err error)
```

Tombstoned keys are **returned as tombstones, not omitted** — the caller must
delete, not skip. This is the flag the digest already carries.

**Correctness constraint agreed non-negotiable:** same answer with no digest
present, falling back to a scan.

## Still open

Prefix position in key order — early or late — which decides whether the sparse
key index is required or merely an optimisation. That is sqlcdc's key layout to
answer, not something to guess here.

Because the read is now sealed-only, the commitlog-side work is a prefix-bounded
digest merge; it does not change the clean path or the digest format unless the
sparse index turns out to be required.
