------------------------------ MODULE MultiWriter ------------------------------
(***************************************************************************)
(* Abstract model of what happens to a shared SegmentStore when more than   *)
(* one process writes to it.                                               *)
(*                                                                          *)
(* commitlog does NOT defend against this. A log assumes it is the only     *)
(* writer to its store: no identity in the keys, no fenced deletes, no way  *)
(* to detect a second writer. This spec is therefore not a description of a *)
(* defence -- it is the EVIDENCE for the contract that says the caller must *)
(* guarantee single ownership, and it exists because the failure it models  *)
(* is silent, so nobody hits it and learns.                                 *)
(*                                                                          *)
(* The hazard is not that consensus is unreliable. It is that consensus     *)
(* settles who leads at the moment it DECIDES, while the write lands at a   *)
(* later moment nobody controls. A node acts on its own applied view, which *)
(* lags; a request already handed to the storage client cannot be observed  *)
(* or cancelled; and the claim it would need ("I will still own the tier    *)
(* when this lands") is about the FUTURE, so no wait for finality           *)
(* establishes it. Each writer here therefore holds a belief that is        *)
(* allowed to be stale, and acts on it.                                     *)
(*                                                                          *)
(* What the model shows, in order of how much it matters:                   *)
(*                                                                          *)
(*   Stamped = FALSE -- deterministic keys, which is what commitlog writes. *)
(*     Two writers derive the same next generation from their own local     *)
(*     markers, address the SAME key, and one silently overwrites the       *)
(*     other. This is the configuration the shipped code is in, and         *)
(*     NoClobber is violated. That is the point: it is the cost of the      *)
(*     contract, stated exactly.                                            *)
(*                                                                          *)
(*   Stamped = TRUE -- keys made disjoint per writer, the way Kafka gives   *)
(*     every remote copy a fresh id. NoClobber then holds, which shows the  *)
(*     overwrite is a property of DETERMINISTIC keys rather than of         *)
(*     concurrency itself.                                                  *)
(*                                                                          *)
(*   FencedDelete = FALSE -- deletes that ignore ownership. MarkerIntegrity *)
(*     is violated, and the counterexample is worth reading: the object is  *)
(*     removed by the LEGITIMATE owner, not by a stale writer. Markers are  *)
(*     local, so "garbage by my own view" is not the same claim as          *)
(*     "garbage" -- which is why UnreferencedObjects reports rather than    *)
(*     deletes.                                                            *)
(*                                                                          *)
(*   LineageReclaim = FALSE -- what happens if reclamation is restricted to *)
(*     objects a process wrote under its CURRENT identity. Every object     *)
(*     already in the tier is stranded after a handover, permanently, and   *)
(*     EveryOrphanReclaimable is violated. Recorded because it is the trap  *)
(*     an over-eager fence falls into.                                     *)
(*                                                                          *)
(* One segment (one base offset) is modelled: collisions are between        *)
(* writers at the same base offset, so further offsets add no structure.    *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
    Writers,       \* the processes that may believe they own tier writes
    MaxGen,        \* generation bound (bounds the model)
    MaxEpoch,      \* how many identities one process may hold in turn
    Stamped,       \* TRUE => keys carry the writer stamp (C8)
    FencedDelete,  \* TRUE => deletes check the stamp (C9)
    LineageReclaim \* TRUE => a writer may reclaim keys its OWN marker named

VARIABLES
    store,      \* key -> content, or NoObj
    owner,      \* the writer consensus currently designates (ground truth)
    believes,   \* writer -> does it think it owns tier writes (may be STALE)
    marker,     \* writer -> the key its local marker names, or NoKey
    gen,        \* writer -> generation of its local marker
    stamp,      \* writer -> the identity it currently writes under
    mine,       \* writer -> every key its marker has ever named
    promised,   \* writer -> content it believes is at its marker
    clobbered,  \* history: a PUT overwrote different content
    lastRead,   \* content the most recent read returned
    lastWant    \* content that read should have returned

vars == <<store, owner, believes, marker, gen, stamp, mine, promised,
          clobbered, lastRead, lastWant>>

\* The sentinels have the same SHAPE as the values they stand in for. TLC
\* refuses to compare values of different types, and the invariants must ask
\* whether a key is stamped, and whether a marker's object is still the one put
\* there -- comparisons that necessarily span present and absent.
NoStamp == [w |-> "none", e |-> 0]
NoKey   == [g |-> -1, s |-> NoStamp]

NoObj   == [w |-> "none", g |-> -1]

Gens     == 0..MaxGen
Epochs   == 1..MaxEpoch

\* An identity is a process AND the epoch it holds, because a process writes
\* under a succession of identities: SetTierWriter moves it on at every
\* leadership change. Objects it wrote under an earlier one keep that stamp.
Idents   == [w : Writers, e : Epochs]
Stamps   == Idents \cup {NoStamp}
Keys     == [g : Gens, s : Stamps]
Contents == [w : Writers, g : Gens]

\* The key a writer addresses for a generation. Without the stamp every writer
\* computes the SAME key -- which is exactly the pre-C8 collision.
KeyOf(w, g) == [g |-> g, s |-> IF Stamped THEN stamp[w] ELSE NoStamp]
StampOf(k)  == k.s

\* Distinct content per (writer, generation): two writers compacting the same
\* segment do not produce the same bytes, so any overwrite loses data.
Content(w, g) == [w |-> w, g |-> g]

\* A writer may delete an object it does not consider live. Fenced, it may touch
\* its own stamp, an unstamped object predating any identity, or -- the rule
\* that keeps reclamation possible at all -- a key its OWN marker once named.
\*
\* Without that last clause the fence is unsatisfiable after an identity change:
\* every object already in the tier carries the previous stamp, so nothing the
\* process wrote before the change can ever be removed again.
MayDelete(w, k) ==
    \/ ~FencedDelete
    \/ StampOf(k) = NoStamp
    \/ StampOf(k) = stamp[w]
    \/ (LineageReclaim /\ k \in mine[w])

Init ==
    /\ store = [k \in Keys |-> NoObj]
    /\ owner \in Writers
    /\ believes = [w \in Writers |-> w = owner]
    /\ marker = [w \in Writers |-> NoKey]
    /\ gen = [w \in Writers |-> 0]
    /\ stamp = [w \in Writers |-> [w |-> w, e |-> 1]]
    /\ mine = [w \in Writers |-> {}]
    /\ promised = [w \in Writers |-> NoObj]
    /\ clobbered = FALSE
    /\ lastRead = NoObj
    /\ lastWant = NoObj

(* Consensus moves ownership. The new owner learns immediately; every other  *)
(* writer keeps whatever it believed -- there is no synchronous revocation,  *)
(* which is the entire problem.                                             *)
Elect(w) ==
    /\ owner # w
    /\ owner' = w
    /\ believes' = [believes EXCEPT ![w] = TRUE]
    /\ UNCHANGED <<store, marker, gen, stamp, mine, promised, clobbered,
                   lastRead, lastWant>>

(* A writer's view catches up with the truth, whenever that happens to be. *)
Learn(w) ==
    /\ believes[w] # (owner = w)
    /\ believes' = [believes EXCEPT ![w] = (owner = w)]
    /\ UNCHANGED <<store, owner, marker, gen, stamp, mine, promised, clobbered,
                   lastRead, lastWant>>

(* Put is unconditional: SegmentStore has no compare-and-swap, so an          *)
(* overwrite is invisible to both parties. Record it so the invariant can see *)
(* what the store cannot report.                                            *)
PutAt(k, c) ==
    /\ store' = [store EXCEPT ![k] = c]
    /\ clobbered' = (clobbered \/ (store[k] # NoObj /\ store[k] # c))

(* First offload: generation 0. *)
Offload(w) ==
    /\ believes[w]
    /\ marker[w] = NoKey
    /\ LET k == KeyOf(w, 0) IN
       /\ PutAt(k, Content(w, 0))
       /\ marker' = [marker EXCEPT ![w] = k]
       /\ mine' = [mine EXCEPT ![w] = mine[w] \cup {k}]
       /\ promised' = [promised EXCEPT ![w] = Content(w, 0)]
    /\ UNCHANGED <<owner, believes, gen, stamp, lastRead, lastWant>>

(* A compaction rewrite: the NEXT generation goes to a new key and the marker *)
(* is the commit point. The superseded object deliberately stays -- a reader  *)
(* that opened the segment first is still on it.                             *)
Rewrite(w) ==
    /\ believes[w]
    /\ marker[w] # NoKey
    /\ gen[w] < MaxGen
    /\ LET g == gen[w] + 1
           k == KeyOf(w, g)
       IN /\ PutAt(k, Content(w, g))
          /\ marker' = [marker EXCEPT ![w] = k]
          /\ gen' = [gen EXCEPT ![w] = g]
          /\ mine' = [mine EXCEPT ![w] = mine[w] \cup {k}]
          /\ promised' = [promised EXCEPT ![w] = Content(w, g)]
    /\ UNCHANGED <<owner, believes, stamp, lastRead, lastWant>>

(* Ownership moved, so the process writes under a NEW identity from here on --  *)
(* SetTierWriter. Everything it wrote before keeps the previous stamp, which is *)
(* what makes the fence unsatisfiable without the lineage rule.                 *)
Rotate(w) ==
    /\ stamp[w].e < MaxEpoch
    /\ stamp' = [stamp EXCEPT ![w] = [w |-> w, e |-> stamp[w].e + 1]]
    /\ UNCHANGED <<store, owner, believes, marker, gen, mine, promised,
                   clobbered, lastRead, lastWant>>

(* Reclaim an object this writer's own view says is dead: a superseded key    *)
(* from a rewrite, or a segment retention dropped. It cannot see anyone       *)
(* else's marker -- markers are local -- so only the fence stands between it  *)
(* and another writer's live object.                                         *)
Delete(w, k) ==
    /\ believes[w]
    /\ store[k] # NoObj
    /\ marker[w] # k
    /\ MayDelete(w, k)
    /\ store' = [store EXCEPT ![k] = NoObj]
    /\ UNCHANGED <<owner, believes, marker, gen, stamp, mine, promised,
                   clobbered, lastRead, lastWant>>

(* A read follows the marker VERBATIM -- keys are never recomputed, which is  *)
(* what lets an identity change without stranding earlier objects.            *)
Read(w) ==
    /\ marker[w] # NoKey
    /\ lastRead' = store[marker[w]]
    /\ lastWant' = promised[w]
    /\ UNCHANGED <<store, owner, believes, marker, gen, stamp, mine, promised,
                   clobbered>>

Next ==
    \/ \E w \in Writers : Elect(w)
    \/ \E w \in Writers : Learn(w)
    \/ \E w \in Writers : Offload(w)
    \/ \E w \in Writers : Rewrite(w)
    \/ \E w \in Writers : Rotate(w)
    \/ \E w \in Writers, k \in Keys : Delete(w, k)
    \/ \E w \in Writers : Read(w)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Invariants *)

TypeOK ==
    /\ store \in [Keys -> Contents \cup {NoObj}]
    /\ owner \in Writers
    /\ believes \in [Writers -> BOOLEAN]
    /\ marker \in [Writers -> Keys \cup {NoKey}]
    /\ gen \in [Writers -> Gens]
    /\ stamp \in [Writers -> Idents]
    /\ clobbered \in BOOLEAN

\* No PUT ever overwrote an existing object with different content. The store
\* cannot report this and neither writer can detect it, so it is checked here.
NoClobber == ~clobbered

\* The object a writer's marker names still holds what that writer put there.
\* Required of a DEMOTED writer too: it is still serving reads and was never
\* told to stop.
MarkerIntegrity ==
    \A w \in Writers :
        marker[w] # NoKey => store[marker[w]] = promised[w]

\* The observable form of the above.
ReadCorrect == lastRead = lastWant

\* Every object is either live or attributable to a writer that can reclaim it.
\* Fencing turns corruption into garbage; unaccountable garbage is a leak with
\* nobody able to prove it is safe to remove.
ReclaimAttributable ==
    \A k \in Keys :
        store[k] # NoObj =>
            \/ \E w \in Writers : marker[w] = k
            \/ StampOf(k) \in Idents

\* Every object that is no longer live can still be removed by the process whose
\* lineage produced it. This is what the fence costs if it is applied too
\* widely: after an identity change every object in the tier carries the
\* PREVIOUS stamp, so a fence with no lineage rule strands all of them forever --
\* and unlike the corruption it prevents, that failure is permanent and grows.
EveryOrphanReclaimable ==
    \A k \in Keys :
        (store[k] # NoObj /\ (\A w \in Writers : marker[w] # k)) =>
            \E w \in Writers : (k \in mine[w] /\ MayDelete(w, k))

=============================================================================
