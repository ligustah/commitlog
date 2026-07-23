# TLA+ verification of commitlog

Abstract [TLA+](https://lamport.azurewebsites.net/tla/tla.html) models of
commitlog's core guarantees, model-checked with **TLC**. This is a **one-off
confidence pass** — bounded model checking of abstract specs, not TLAPS proofs
and not wired into CI. The goal is to establish that the design's soundness
invariants are mutually consistent and are maintained across every interleaving
of the operations that touch committed data.

The specs are deliberately abstract: they model the *contract* each subsystem
promises (offsets, the commit/durability boundary, latest-per-key, read-through
transparency), not the byte-level file format. They are grounded in the code —
`interface.go`, `commitlog.go`, `compact_cleaner.go`, `segment_store.go`,
`index_cache.go` — and in the invariants the Go tests already assert
(`clean_spec_test.go`, `clean_convergence_test.go`, `recover_tail_test.go`).

## Layout

| File | Models | Code |
|------|--------|------|
| `CommitLog.tla` | append / commit / checkpoint / crash + tail recovery / truncation | `commitlog.go` (`SetHighWatermark`, `checkpointHW`, `RecoverTail`, `Truncate`, `TruncateBefore`) |
| `Compaction.tla` | transaction-aware compaction: latest-per-key, aborted-shadowing, tombstone GC, stripping, convergence | `compact_cleaner.go` |
| `Offload.tla` | tiered storage: read-through + LRU index cache with pinning | `segment_store.go`, `index_cache.go` |

Each spec has a `.cfg` (the verified configuration) plus a `*_Buggy.cfg` that
swaps in a deliberately broken variant to show the invariants have teeth (TLC
produces a counterexample).

## Running

TLC ships in `tla2tools.jar` (needs a JRE; Java 8+ is fine). The jar is not
vendored here — download it once:

```sh
curl -L -o tla2tools.jar \
  https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar
```

Then, from this directory:

```sh
# Core spec — expect: "Model checking completed. No error has been found."
java -cp tla2tools.jar tlc2.TLC -workers auto -config CommitLog.cfg   CommitLog.tla
java -cp tla2tools.jar tlc2.TLC -workers auto -config Compaction.cfg  Compaction.tla
java -cp tla2tools.jar tlc2.TLC -workers auto -config Offload.cfg     Offload.tla

# The "teeth" runs — each is EXPECTED to report an invariant violation
java -cp tla2tools.jar tlc2.TLC -workers auto -config CommitLog_Buggy.cfg  CommitLog.tla
java -cp tla2tools.jar tlc2.TLC -workers auto -config Compaction_Buggy.cfg Compaction.tla
java -cp tla2tools.jar tlc2.TLC -workers auto -config Offload_Buggy.cfg     Offload.tla
```

## Results

| Spec | Distinct states | Result |
|------|-----------------|--------|
| `CommitLog` | 9,984 | all invariants hold |
| `Compaction` | 813,616 | all invariants hold |
| `Offload` | 5,441 | all invariants hold |

Each `*_Buggy.cfg` produces the expected counterexample (see below).

## What each spec proves

### CommitLog.tla — core WAL soundness

Invariants:

- **CommittedPrefixStability** — a record that was ever *durably committed* is
  never lost, altered, reordered, or torn by any operation (append, commit,
  checkpoint, crash, recovery, or prefix/suffix truncation).
- **OffsetMonotonicity** — offsets stay dense and contiguous; the durably
  committed frontier only ever grows (`EverDCMono`, a temporal property).
- **RecoverySoundness** — after recovery the high watermark never sits below
  the checkpoint, and no torn record is exposed as committed. `RecoverTail`
  extends the tail past a *stale* checkpoint via a CRC-good forward scan and
  drops a torn suffix.

The model is faithful to commitlog's real durability boundary: `SetHighWatermark`
raises the commit frontier **without** an fsync, and `checkpointHW` persists the
HW value only periodically — so the on-disk checkpoint lags the durable tail.
That is the exact stale-checkpoint window `RecoverTail` was written to survive
(see `recover_tail_test.go`). A record is guaranteed to survive a crash only
once it is durable (fsynced); a committed-but-unsynced tail can be lost on power
loss — cluster-level durability comes from replication, which is out of scope.
`everDC` ("ever durably committed") is the monotone ground truth of what *must*
survive.

**Teeth** (`CommitLog_Buggy.cfg`, `BuggyRecovery = TRUE`): swaps in the old
"amputate everything above the checkpoint" recovery. TLC reports a
**CommittedPrefixStability** violation — the stale-checkpoint data loss the real
`RecoverTail` prevents.

> While building this spec TLC also caught a genuine gap in a first draft of the
> recovery action: the forward scan must clamp its start up to the oldest
> surviving offset, because retention can delete the prefix below a stale
> checkpoint. That matches the code's "start = hw+1, clamped up to
> `OldestOffset()`" behavior.

### Compaction.tla — transaction-aware compaction

Invariants (checked over every bounded log):

- **ViewPreserved** — a committed reader's latest-per-key value is unchanged by
  compaction; the only permitted change is a GC'd tombstone turning a deletion
  ("deleted") into an absence ("absent").
- **aborted-shadowing exclusion** — an aborted record is never counted
  latest-per-key, never survives below the ceiling (`NoAbortedBelowCeiling`),
  and never deletes a committed value (the H1 regression from
  `clean_spec_test.go`).
- **TombstoneGCCorrect** — a latest-per-key tombstone is removed only when it is
  old enough *and* below `GCBelow`; young and unstamped tombstones always
  survive.
- **NoControlBelowStrip / AboveCeilingRetained** — control markers are removed
  below `StripBelow`; undecided records at or above the ceiling are retained
  verbatim.
- **Idempotent** — a second pass changes nothing (the convergence fixed point
  from `clean_convergence_test.go`).

Records keep their original offsets across compaction, so the log is modeled as
a set of records each carrying its offset, and compaction is a pure set-to-set
operator. The `Ceiling` boundary is modeled as strict (records with
`offset < Ceiling` are decided/compacted; `offset >= Ceiling` are retained
verbatim), a faithful abstraction of the inclusive-ceiling code where the
boundary record is retained either way.

**Teeth** (`Compaction_Buggy.cfg`, `BuggyAborted = TRUE`): models the
transaction-blind scan that counts an aborted record as latest-per-key. TLC
reports the aborted record surviving / shadowing a committed value.

### Offload.tla — tiered storage

Invariants:

- **ReadTransparency** — every read returns the ground-truth bytes, whatever the
  segment's location (local vs offloaded) or the index cache's state.
- **PutByteIdentity** — an offloaded object is a byte-exact copy of the original.
- **PinnedStaysCached** — an index pinned by a live seek is never evicted out
  from under it (holds at all times).
- **EvictionEffective** — right after an acquire the cache is within its byte
  budget, unless everything left is pinned. `evictLocked` runs only on acquire,
  so an unpinned entry may legitimately linger over budget between acquires —
  the model reproduces that timing exactly.
- **CacheStructOK** — the LRU list stays a faithful permutation of the cached
  set.

The prefetch read-ahead buffer inside `storeBacking` sits *below* this level and
is abstracted away: it is a transparent cache over immutable, read-only bytes,
so it cannot change a read's result. The index is modeled as exact derived data
(a verbatim copy), so cache coherence reduces to read transparency.

**Teeth** (`Offload_Buggy.cfg`, `BuggyEvict = TRUE`): eviction that ignores pin
counts. TLC reports a **PinnedStaysCached** violation — an index evicted out
from under an in-flight seek, the corruption the pin count prevents.

## Scope and abstraction notes

- Bounded model checking, small constants (a handful of offsets/keys/segments).
  These bounds exercise every structural interleaving; they do not prove the
  properties for unbounded logs.
- Durability is anchored at the fsync/checkpoint boundary (see CommitLog above);
  replication-level durability is out of scope.
- Leader-epoch tracking, timestamps-as-index, message framing/CRC bytes, and the
  concurrency/locking of the Go implementation are not modeled — the specs are
  about the logical committed state, not the byte layout or the mutex protocol.
