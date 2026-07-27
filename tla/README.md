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
| `MultiWriter.tla` | tier writes when two processes may each believe they own them: writer-stamped keys, fenced deletes, reclaimable garbage | `segment.go` (`segmentStoreKey`, `deleteFenced`), `commitlog.go` (`SetTierWriter`, `DeleteStoreObjects`, `UnreferencedObjects`) |

Each spec has a `.cfg` (the verified configuration) plus at least one
`*_Buggy*.cfg` that swaps in a deliberately broken variant to show the
invariants have teeth (TLC produces a counterexample). `MultiWriter` has two,
one per defence it models — an unstamped key and an unfenced delete fail in
different ways, and a single config could only demonstrate one of them.

## Running

TLC ships in `tla2tools.jar` (needs a JRE; Java 8+ is fine). The jar is not
vendored here — download it once:

```sh
curl -L -o tla2tools.jar \
  https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar
```

There is also a `workflow_dispatch`-only GitHub Actions workflow
(`.github/workflows/tla.yml`) that runs everything below on demand. It is
deliberately **not** on the push/PR gate — see above — but it means a spec edit
can be re-verified without a local JDK. It asserts the verified configs print
TLC's success line and that each `*_Buggy.cfg` violates its *own* named
invariant, so a spec that stopped model-checking cannot masquerade as a pass.

Then, from this directory:

```sh
# Core spec — expect: "Model checking completed. No error has been found."
java -cp tla2tools.jar tlc2.TLC -workers auto -config CommitLog.cfg   CommitLog.tla
java -cp tla2tools.jar tlc2.TLC -workers auto -config Compaction.cfg  Compaction.tla
java -cp tla2tools.jar tlc2.TLC -workers auto -config Offload.cfg     Offload.tla
java -cp tla2tools.jar tlc2.TLC -workers auto -config MultiWriter.cfg MultiWriter.tla

# The "teeth" runs — each is EXPECTED to report an invariant violation
java -cp tla2tools.jar tlc2.TLC -workers auto -config CommitLog_Buggy.cfg  CommitLog.tla
java -cp tla2tools.jar tlc2.TLC -workers auto -config Compaction_Buggy.cfg Compaction.tla
java -cp tla2tools.jar tlc2.TLC -workers auto -config Offload_Buggy.cfg     Offload.tla
java -cp tla2tools.jar tlc2.TLC -workers auto -config MultiWriter_Buggy.cfg       MultiWriter.tla
java -cp tla2tools.jar tlc2.TLC -workers auto -config MultiWriter_BuggyDelete.cfg MultiWriter.tla
```

## Results

| Spec | Distinct states | Result |
|------|-----------------|--------|
| `CommitLog` | 9,984 | all invariants hold |
| `Compaction` | 813,616 | all invariants hold |
| `Offload` | 5,441 | all invariants hold |
| `MultiWriter` | 1,344 | all invariants hold |

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
operator. The `Ceiling` boundary matches `compact_cleaner.go` exactly:
candidacy for latest-per-key is `offset <= Ceiling` (the record **at** the
ceiling *is* counted latest and supersedes lower copies), a record is dropped
only strictly below the ceiling (`classify` retains everything at/above it
verbatim), and `offset > Ceiling` is retained and uncounted. Callers pass
`Ceiling` = the transactional LSO, so undecided records above it are never
compacted.

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

### MultiWriter.tla — tier writes under contested ownership

Invariants:

- **NoClobber** — no PUT ever overwrites an existing object with different
  content. `SegmentStore.Put` has no compare-and-swap form, so a lost update
  here produces no error for either party and leaves nothing to recover.
- **MarkerIntegrity** — the object a writer's marker names still holds what
  that writer put there. It is required of a *demoted* writer too: losing an
  election does not stop it serving reads, and nothing tells it to stop.
- **ReadCorrect** — the observable form: a read returns what the reader's own
  marker promised.
- **ReclaimAttributable** — every object is either live (some marker names it)
  or carries a stamp saying who may reclaim it. Fencing converts corruption
  into garbage, so the garbage has to stay accountable or the leak is unbounded.

The model exists because the hazard is easy to argue about incorrectly. It is
*not* that consensus is unreliable. It is that consensus establishes who leads
at the moment it **decides**, while the PUT lands at a later moment nobody
controls — so each writer here acts on a belief that is allowed to be stale, and
`Elect` deliberately has no synchronous revocation. The generation does not
close the window either, and the spec shows why mechanically: a generation is
read from each writer's **own local marker**, so two writers that both believe
they own the tier compute the same next generation and address the same key.

**Teeth** (`MultiWriter_Buggy.cfg`, `Stamped = FALSE`): the pre-stamp key,
generation only. TLC reports **NoClobber** violated in four states — elect the
new owner, the old one (not yet knowing) offloads, the new one offloads, and the
second PUT silently replaces the first at the identical key.

**Teeth** (`MultiWriter_BuggyDelete.cfg`, `FencedDelete = FALSE`): stamped keys,
unfenced deletes. TLC reports **MarkerIntegrity** violated. The counterexample is
sharper than the one that motivated the fence: the object is removed by the
*legitimate new owner*, not by a stale writer. Markers are local, so a writer
cannot see that another process's marker still names an object, and "garbage by
my own view" is not the same claim as "garbage". That is exactly why
`UnreferencedObjects` reports rather than deletes — whether an unreferenced
object is safe to remove depends on whether the tier is shared, which only the
caller knows.

## Scope and abstraction notes

- Bounded model checking, small constants (a handful of offsets/keys/segments).
  These bounds exercise every structural interleaving; they do not prove the
  properties for unbounded logs.
- Durability is anchored at the fsync boundary, taken conservatively. Verified
  code facts: `SetHighWatermark` never fsyncs; a segment roll seals without
  fsyncing; `checkpointHW` fsyncs only the active segment and writes the
  checkpoint; `SyncAll` fsyncs every segment then checkpoints. The core spec
  collapses this per-segment granularity into one `durable` frontier and proves
  the safety floor (fsync'd-committed data never lost, recovery never below the
  checkpoint). The code additionally recovers written-but-unsynced data after a
  kill -9 (`RecoverTail`'s forward scan over the OS-cached tail) — a stronger
  recovery the model does not assert. Replication-level durability is out of
  scope.
- **`Ceiling` is assumed to be the transactional LSO — it is an input, not a
  derived quantity, and nothing here checks it.** `Compaction.tla` reasons about
  what the engine does *given* a ceiling; a caller that advances one past a
  record no one has decided yet is outside the state space, so no amount of
  model checking here will find that. It is a real class: supersession and an
  abort compose across two passes to remove a key entirely, but only when an
  earlier pass ran with a ceiling above an undecided record (pinned in Go by
  `TestCleanSpecCeilingAboveUndecidedLosesKey`, which shows the same records
  surviving or vanishing purely on where the first pass's ceiling sat). The
  engine has no notion of "undecided" and cannot defend against it, so this
  invariant belongs to whoever computes the LSO. These specs prove the engine
  honours its contract, **not** that the contract is supplied correctly.
- `OverrideHighWatermark` (a test-only backdoor that can lower the HW) is not
  modeled; the spec's HW is monotone, matching production `SetHighWatermark`.
- Leader-epoch tracking, timestamps-as-index, message framing/CRC bytes, and the
  concurrency/locking of the Go implementation are not modeled — the specs are
  about the logical committed state, not the byte layout or the mutex protocol.

## Fidelity audit

The specs were re-checked operation-by-operation against the implementation.
One real divergence was found and fixed: `Compaction.tla` had modeled candidacy
with a strict `offset < Ceiling`, but the code counts the record **at** the
ceiling as latest-per-key (`compact_cleaner.go` excludes only `offset > Ceiling`
from candidacy, while `classify` retains everything `>= Ceiling`). The model now
matches — candidacy `<= Ceiling`, dropping only `< Ceiling` — and re-verifies
clean, now exercising the boundary offset the strict model never reached. The
durability abstraction and the offload prefetch-buffer abstraction are
documented above; everything else was confirmed faithful.
