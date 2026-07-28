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

## What was built

`ReadKeyPrefix(prefix, upTo) ([]PrefixRecord, completeThrough, error)` in
`prefix_read.go`. Sealed segments only, latest-per-key within the prefix,
tombstones returned as tombstones, and the same answer with or without sidecars
present.

### Fetching the records is a cost decision, not a correctness one

Locating the winners is the digest merge. Reading them is a separate problem:
they are scattered, and there are two ways to get them.

- Read a contiguous span and discard the frames between the ones wanted.
- Address each one, paying a request per record.

Which wins is **not a property of the storage device in the abstract** — it is
what that tier charges for. So both the coalescing budget and the fan-out are
configured **per tier**, and chosen **per segment**, since a log mid-offload
holds both kinds at once.

**Local.** No per-request price, so the trade is set by the **device**, and
"local" is not one device. On a spinning disk a seek costs milliseconds: read
through megabytes to avoid one, and keep concurrency low, because concurrent
random reads serialize on a single head and buy seeks rather than bandwidth. On
an NVMe both of those invert — random access is nearly free and a deep queue is
how the device is saturated, so the window should be small and the fan-out large,
matching or exceeding the tiered settings.

The defaults assume the unfavourable case (4MB window, concurrency 8). That is a
conservative choice, **not a claim about local storage**, which is exactly why
the values are configurable rather than inferred from the backing.

**Tiered.** A store charges per request and answers many at once. Splitting is
what gives the fan-out something to parallelise, so the budget is small and the
concurrency is high. Where bytes are actually priced, the breakeven is
computable rather than guessable — reading through a gap transfers bytes that
are discarded, splitting costs one more request, so coalescing pays while

```
C_req > (gap / 1e9) * C_GB        i.e.        gap < 1e9 * C_req / C_GB
```

At, say, $0.0004/1k GETs and $0.09/GB that is a few KB. Read from inside the
same region, where bytes are effectively free, the right-hand side runs away and
coalescing always wins on price.

**Latency deliberately does not enter the trade.** An earlier version of this
argued a megabyte-scale default from round-trip time. That only holds if requests
are issued serially; with enough in flight the round trip is hidden and price is
all that remains. Concurrency is what makes requests cheap in time, so the tier
default is set on price and the fan-out pays for it.

The four knobs are `PrefixReadCoalesceBytes` / `PrefixReadTierCoalesceBytes` and
`PrefixReadConcurrency` / `PrefixReadTierConcurrency`. Zero takes the default, as
everywhere else in `Options`; a **negative** coalesce budget means *never
coalesce* — one request per isolated record, the fastest and most expensive
setting. That escape hatch is negative rather than zero precisely so it cannot be
confused with "unset".

### The unit of parallelism is a run, not a segment

A **run** is a span of wanted records close enough to read in one pass; the
coalesce budget decides where one ends. Every run, across every segment, is
fetched concurrently, and the two tiers' limits are enforced **independently** so
a log holding both does not have its store reads throttled behind its disk reads.

Parallelising per *segment* is the obvious shape, since each segment is its own
file or object. It is also wrong: it caps the fan-out at the number of segments
holding hits, so a prefix whose keys are concentrated in a few segments barely
fans out however many records it wants.
`TestPlanRunsFanOutIsNotCappedBySegmentCount` pins this — collapsing the planner
back to one run per segment fails it.

### What is tested

Beyond the semantics: a differential test against an independent scan across ten
prefixes and every bound in range; equivalence with sidecars deleted and with
sidecars corrupted; agreement across every combination of coalesce and
concurrency setting; a read over segments actually offloaded to a
`FileSegmentStore`, including a tombstone making the round trip; and
`TestPrefixReadTierBudgetGovernsTieredSegments`, which uses the scanner counter
to prove the tier budget reshapes tiered reads while the local budget leaves them
untouched.

`TestReadKeyPrefixDoesNotScanSegmentsWithoutHits` pins the acceleration itself —
one segment scanned for one hit across 60+ sealed segments, against 33 when the
digests are ignored.

Each load-bearing guard was checked by mutation rather than by passing: inverting
latest-per-key, dropping the prefix filter, ignoring the sidecars, and collapsing
the run planner to one run per segment each fail a test that names the defect.

## Measured, finally

`TestPrefixReadCostProfile` counts what a store actually bills for — **requests
issued and bytes transferred** — through a `SegmentStore` wrapper. It does not
measure wall-clock: these tests run against a filesystem store, so timings would
describe a local disk and say nothing about the object store the setting exists
for. Request and byte counts are hardware-independent.

4000 records of ~230 bytes, 64KB segments, prefix `want:`:

**Dense — 1 hit in 4 (960 hits, 24 segments), gaps ≈ 0.9KB**

| tier budget | requests | bytes | vs. no coalescing |
|---|---|---|---|
| none | 972 | 844 KB | — |
| ≥ 1KB | 24 | 1569 KB | **40× fewer requests, 1.86× the bytes** |

**Sparse — 1 hit in 40 (99 hits, 30 segments), gaps ≈ 9KB**

| tier budget | requests | bytes | vs. no coalescing |
|---|---|---|---|
| ≤ 4KB | 114 | 990 KB | — |
| ≥ 16KB | 30 | 1826 KB | 3.8× fewer requests, 1.84× the bytes |

**The formula predicts both.** At $0.0004/1k GETs and $0.09/GB the breakeven gap
is ~4.4KB. Dense gaps (0.9KB) sit below it, so coalescing wins — saving 948
requests costs 725KB, which is about **6× cheaper**. Sparse gaps (9KB) sit above
it, so coalescing loses — saving 84 requests costs 835KB, about **2× more
expensive**. The measurement agrees with the model, on both sides of the line.

**And it contradicts the default I had chosen.** At 64KB the tier budget behaves
identically to 1MB on both shapes: it coalesces everything. A default justified
*on price* was sitting an order of magnitude above the price breakeven, which
means it was really a "bytes are free" default wearing a cost argument.

The tier default is now **4KB**, matching the computed breakeven. Deployments
reading from inside the same region, where bytes genuinely are free, should raise
it — and that is now an explicit instruction rather than an accident.

Two structural notes the numbers make visible:

- **Requests never drop below the segment count** (24 and 30). One stream per
  segment is the floor; coalescing cannot do better than that.
- **The budget is a threshold on the actual gap distribution, not a dial.**
  Nothing changes between 1KB and 4KB in the sparse case, then everything changes
  between 4KB and 16KB, because the real gaps are ~9KB. Tuning it without knowing
  the workload's gap distribution is guesswork.

## Still open

Prefix position in key order — early or late — which decides whether a sparse key
index (every Nth key → byte offset in the digest) is required or merely an
optimisation. That is sqlcdc's key layout to answer. Nothing built here changes
the clean path or the digest format; the sparse index would be the first thing
that does.

The defaults are **argued, not measured**. There is no benchmark behind either
number yet, and a deployment paying per-GB reads should compute its own coalesce
budget from the formula above rather than trust the default.
