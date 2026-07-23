-------------------------------- MODULE Offload --------------------------------
(***************************************************************************)
(* Abstract model of commitlog's tiered storage: a sealed segment's log     *)
(* bytes are offloaded to a SegmentStore (segment_store.go) and its index    *)
(* is served from a process-wide, byte-budgeted LRU RemoteIndexCache with    *)
(* pinning (index_cache.go).                                                 *)
(*                                                                          *)
(* Two headline guarantees:                                                 *)
(*   - Read-through transparency: an offloaded segment reads back           *)
(*     byte-identically to its local form (Put copies exactly; reads go     *)
(*     through the store). The prefetch read-ahead buffer sits BELOW this   *)
(*     level and is abstracted away -- it is a transparent cache over        *)
(*     immutable, read-only bytes, so it cannot change a read's result.     *)
(*   - Index-cache safety: a cached index is exact derived data; an index    *)
(*     pinned by a live seek is NEVER evicted out from under it; and         *)
(*     eviction (which runs only on acquire, as in the code) drives the      *)
(*     cache back within budget unless everything left is pinned.            *)
(*                                                                          *)
(* Invariants:                                                              *)
(*   ReadTransparency   - every read returns the ground-truth bytes,        *)
(*                        whatever the segment's location or cache state.    *)
(*   PutByteIdentity    - an offloaded object equals the original bytes.     *)
(*   PinnedStaysCached  - refs[s] > 0 => s is still cached (pins protect an  *)
(*                        in-flight seek) -- holds at ALL times.             *)
(*   EvictionEffective  - right after an acquire the cache is within budget, *)
(*                        unless every remaining entry is pinned. (Between   *)
(*                        acquires an unpinned entry may linger over budget, *)
(*                        exactly as evictLocked-on-acquire permits.)        *)
(*   CacheStructOK      - the LRU list is exactly a permutation of cached.   *)
(*                                                                          *)
(* BuggyEvict = TRUE models an eviction that ignores the pin count; TLC then *)
(* reports a PinnedStaysCached violation (evicting an index a live seek      *)
(* still holds).                                                            *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
    NumSegs,     \* number of sealed segments
    RecPerSeg,   \* records (readable positions) per segment
    Budget,      \* index-cache capacity, in index units (1 per segment)
    MaxRefs,     \* cap on concurrent pins per index (bounds the model)
    BuggyEvict   \* TRUE => eviction ignores pins (models the broken cache)

VARIABLES
    loc,       \* segment -> "local" | "offloaded"
    cached,    \* set of segments whose index is currently in the cache
    refs,      \* segment -> pin count of its cached index
    lru,       \* sequence of cached segments, head = most recently used
    storeObj,  \* segment -> offloaded object bytes (only meaningful when offloaded)
    lastVal,   \* value returned by the most recent read
    lastExp,   \* ground-truth value the most recent read should have returned
    lastOp     \* the most recent action (so eviction is checked post-acquire)

vars == <<loc, cached, refs, lru, storeObj, lastVal, lastExp, lastOp>>

Segs == 1..NumSegs
Positions == 1..RecPerSeg
Content(s, i) == s * 10 + i          \* distinct ground-truth byte per record
Range(seq) == { seq[i] : i \in 1..Len(seq) }
Min(a, b) == IF a <= b THEN a ELSE b

Pos(seq, x)       == CHOOSE i \in 1..Len(seq) : seq[i] = x
RemoveAt(seq, p)  == SubSeq(seq, 1, p - 1) \o SubSeq(seq, p + 1, Len(seq))
RemoveElt(seq, x) == RemoveAt(seq, Pos(seq, x))
MoveToFront(seq, x) == <<x>> \o RemoveElt(seq, x)
Prefix(seq, n)    == SubSeq(seq, 1, Min(n, Len(seq)))

RECURSIVE FilterSeq(_, _)
FilterSeq(seq, S) ==
    IF seq = <<>> THEN <<>>
    ELSE IF Head(seq) \in S
         THEN <<Head(seq)>> \o FilterSeq(Tail(seq), S)
         ELSE FilterSeq(Tail(seq), S)

Init ==
    /\ loc = [s \in Segs |-> "local"]
    /\ cached = {}
    /\ refs = [s \in Segs |-> 0]
    /\ lru = <<>>
    /\ storeObj = [s \in Segs |-> [i \in Positions |-> 0]]
    /\ lastVal = 0
    /\ lastExp = 0
    /\ lastOp = "init"

(* Offload a sealed segment: Put copies its bytes into the store exactly. *)
Offload(s) ==
    /\ loc[s] = "local"
    /\ loc' = [loc EXCEPT ![s] = "offloaded"]
    /\ storeObj' = [storeObj EXCEPT ![s] = [i \in Positions |-> Content(s, i)]]
    /\ lastOp' = "offload"
    /\ UNCHANGED <<cached, refs, lru, lastVal, lastExp>>

(* Acquire (pin) an offloaded segment's index, fetching on a miss and         *)
(* running LRU eviction after the insert (evictLocked). Pinned entries are    *)
(* always kept; among the unpinned, the most-recently-used are kept until the *)
(* budget is met, and the rest are evicted LRU-first.                         *)
AcquireIdx(s) ==
    /\ loc[s] = "offloaded"
    /\ refs[s] < MaxRefs
    /\ LET wasCached == s \in cached
           refs1   == [refs EXCEPT ![s] = refs[s] + 1]
           cached1 == cached \cup {s}
           lru1    == IF wasCached THEN MoveToFront(lru, s) ELSE <<s>> \o lru
           pinned  == { x \in cached1 : refs1[x] > 0 }
           unpinnedSeq == FilterSeq(lru1, { x \in cached1 : refs1[x] = 0 })
           keepN   == IF Cardinality(pinned) >= Budget
                      THEN 0 ELSE Budget - Cardinality(pinned)
           resident == IF BuggyEvict
                       THEN Range(Prefix(lru1, Budget))        \* ignores pins
                       ELSE pinned \cup Range(Prefix(unpinnedSeq, keepN))
       IN /\ refs' = refs1
          /\ cached' = resident
          /\ lru' = FilterSeq(lru1, resident)
    /\ lastOp' = "acquire"
    /\ UNCHANGED <<loc, storeObj, lastVal, lastExp>>

(* Release one pin. The entry stays cached (evictable on a later acquire). *)
ReleaseIdx(s) ==
    /\ refs[s] > 0
    /\ refs' = [refs EXCEPT ![s] = refs[s] - 1]
    /\ lastOp' = "release"
    /\ UNCHANGED <<loc, cached, lru, storeObj, lastVal, lastExp>>

(* Read a record. An offloaded read goes through the store (read-through);    *)
(* a local read hits the file directly. Both must return the same bytes.      *)
ReadRec(s, i) ==
    /\ LET val == IF loc[s] = "offloaded" THEN storeObj[s][i] ELSE Content(s, i)
       IN /\ lastVal' = val
          /\ lastExp' = Content(s, i)
    /\ lastOp' = "read"
    /\ UNCHANGED <<loc, cached, refs, lru, storeObj>>

Next ==
    \/ \E s \in Segs : Offload(s)
    \/ \E s \in Segs : AcquireIdx(s)
    \/ \E s \in Segs : ReleaseIdx(s)
    \/ \E s \in Segs, i \in Positions : ReadRec(s, i)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Invariants *)

TypeOK ==
    /\ loc \in [Segs -> {"local", "offloaded"}]
    /\ refs \in [Segs -> 0..MaxRefs]
    /\ cached \subseteq Segs
    /\ lastVal \in Int
    /\ lastExp \in Int

\* The LRU list is exactly a permutation of the cached set.
CacheStructOK ==
    /\ Range(lru) = cached
    /\ Len(lru) = Cardinality(cached)

\* Every read returns the ground-truth bytes, whatever the location / cache.
ReadTransparency == lastVal = lastExp

\* An offloaded object is a byte-exact copy of the original.
PutByteIdentity ==
    \A s \in Segs :
        loc[s] = "offloaded" => storeObj[s] = [i \in Positions |-> Content(s, i)]

\* Only offloaded segments can have a cached index.
OnlyOffloadedCached == \A s \in cached : loc[s] = "offloaded"

\* An index pinned by a live seek is never evicted out from under it (always).
PinnedStaysCached == \A s \in Segs : refs[s] > 0 => s \in cached

\* Right after an acquire the cache is within budget, unless everything left
\* is pinned (evictLocked runs on acquire; residue between acquires is fine).
EvictionEffective ==
    (lastOp = "acquire") =>
        \/ Cardinality(cached) <= Budget
        \/ \A s \in cached : refs[s] > 0

=============================================================================
