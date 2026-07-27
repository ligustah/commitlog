------------------------------ MODULE MultiWriter ------------------------------
(***************************************************************************)
(* Abstract model of commitlog's tier-write protocol when more than one     *)
(* process may believe it owns writes to a shared SegmentStore.            *)
(*                                                                          *)
(* The hazard this exists to check is NOT that consensus is unreliable. It  *)
(* is that consensus decides who leads at the moment it DECIDES, while the  *)
(* PUT lands at some later moment nobody controls. A node acts on its own   *)
(* applied view of leadership, which lags; its in-flight requests can       *)
(* neither be observed nor cancelled; and the claim it would need ("I will  *)
(* still own writes when this lands") is about the FUTURE, so no amount of  *)
(* waiting for finality establishes it. The model therefore gives each      *)
(* writer a belief that is allowed to be stale, and lets it act on it.      *)
(*                                                                          *)
(* The generation alone does not close the window, which is the point worth *)
(* checking mechanically: a generation is read from each writer's OWN local *)
(* marker, so two writers that both believe they own the tier read the same *)
(* generation N and both write N+1 -- the identical key, a straight         *)
(* overwrite, and no error to either. Stamping the writer identity into the *)
(* key makes their key spaces disjoint; fencing deletes by the same stamp   *)
(* stops a stale writer removing a live object.                            *)
(*                                                                          *)
(* One segment (one base offset) is modelled: collisions are between        *)
(* writers at the same base offset, so further offsets add no structure.    *)
(*                                                                          *)
(* Invariants:                                                              *)
(*   NoClobber       - no PUT ever overwrites an existing object with       *)
(*                     different content. This is the C8 property: the      *)
(*                     silent lost update that has no error to report and   *)
(*                     leaves nothing to recover.                           *)
(*   MarkerIntegrity - the object a writer's marker names still holds the   *)
(*                     content that writer put there. Catches both the      *)
(*                     overwrite and a stale writer deleting a live object. *)
(*                     It must hold for a DEMOTED writer too: losing an     *)
(*                     election does not stop it serving reads, and it is   *)
(*                     never told to stop.                                  *)
(*   ReadCorrect     - the observable form: a read returns what the reader's *)
(*                     own marker promised.                                 *)
(*   ReclaimAttributable - every object in the store is either live (some   *)
(*                     marker names it) or carries a stamp identifying who  *)
(*                     may reclaim it. Fencing converts corruption into     *)
(*                     garbage, so the garbage has to stay accountable.     *)
(*                                                                          *)
(* Stamped = FALSE models the pre-stamp key (generation only); TLC reports  *)
(* NoClobber violated. FencedDelete = FALSE models an unfenced delete; TLC  *)
(* reports MarkerIntegrity violated.                                        *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
    Writers,      \* the processes that may believe they own tier writes
    MaxGen,       \* generation bound (bounds the model)
    Stamped,      \* TRUE => keys carry the writer stamp (C8)
    FencedDelete  \* TRUE => deletes check the stamp (C9)

VARIABLES
    store,      \* key -> content, or NoObj
    owner,      \* the writer consensus currently designates (ground truth)
    believes,   \* writer -> does it think it owns tier writes (may be STALE)
    marker,     \* writer -> the key its local marker names, or NoKey
    gen,        \* writer -> generation of its local marker
    promised,   \* writer -> content it believes is at its marker
    clobbered,  \* history: a PUT overwrote different content
    lastRead,   \* content the most recent read returned
    lastWant    \* content that read should have returned

vars == <<store, owner, believes, marker, gen, promised, clobbered,
          lastRead, lastWant>>

NoStamp == "unstamped"
NoKey   == [g |-> -1, s |-> NoStamp]

\* The absent object has the same SHAPE as a present one. TLC refuses to compare
\* values of different types, and the invariants have to ask whether the object a
\* marker names is still the one put there — a comparison that necessarily spans
\* present and absent.
NoObj   == [w |-> NoStamp, g |-> -1]

Gens     == 0..MaxGen
Stamps   == Writers \cup {NoStamp}
Keys     == [g : Gens, s : Stamps]
Contents == [w : Writers, g : Gens]

\* The key a writer addresses for a generation. Without the stamp every writer
\* computes the SAME key -- which is exactly the pre-C8 collision.
KeyOf(w, g) == [g |-> g, s |-> IF Stamped THEN w ELSE NoStamp]
StampOf(k)  == k.s

\* Distinct content per (writer, generation): two writers compacting the same
\* segment do not produce the same bytes, so any overwrite loses data.
Content(w, g) == [w |-> w, g |-> g]

\* A writer may delete an object it does not consider live. Fenced, it may only
\* touch its own stamp or an unstamped object predating any identity.
MayDelete(w, k) ==
    \/ ~FencedDelete
    \/ StampOf(k) = NoStamp
    \/ StampOf(k) = w

Init ==
    /\ store = [k \in Keys |-> NoObj]
    /\ owner \in Writers
    /\ believes = [w \in Writers |-> w = owner]
    /\ marker = [w \in Writers |-> NoKey]
    /\ gen = [w \in Writers |-> 0]
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
    /\ UNCHANGED <<store, marker, gen, promised, clobbered, lastRead, lastWant>>

(* A writer's view catches up with the truth, whenever that happens to be. *)
Learn(w) ==
    /\ believes[w] # (owner = w)
    /\ believes' = [believes EXCEPT ![w] = (owner = w)]
    /\ UNCHANGED <<store, owner, marker, gen, promised, clobbered,
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
       /\ promised' = [promised EXCEPT ![w] = Content(w, 0)]
    /\ UNCHANGED <<owner, believes, gen, lastRead, lastWant>>

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
          /\ promised' = [promised EXCEPT ![w] = Content(w, g)]
    /\ UNCHANGED <<owner, believes, lastRead, lastWant>>

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
    /\ UNCHANGED <<owner, believes, marker, gen, promised, clobbered,
                   lastRead, lastWant>>

(* A read follows the marker VERBATIM -- keys are never recomputed, which is  *)
(* what lets an identity change without stranding earlier objects.            *)
Read(w) ==
    /\ marker[w] # NoKey
    /\ lastRead' = store[marker[w]]
    /\ lastWant' = promised[w]
    /\ UNCHANGED <<store, owner, believes, marker, gen, promised, clobbered>>

Next ==
    \/ \E w \in Writers : Elect(w)
    \/ \E w \in Writers : Learn(w)
    \/ \E w \in Writers : Offload(w)
    \/ \E w \in Writers : Rewrite(w)
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
            \/ StampOf(k) \in Writers

=============================================================================
