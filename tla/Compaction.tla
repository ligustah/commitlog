------------------------------- MODULE Compaction -------------------------------
(***************************************************************************)
(* Abstract model of commitlog's transaction-aware compaction pass         *)
(* (compact_cleaner.go). It captures the latest-per-key rule, the          *)
(* aborted-shadowing exclusion (the H1 regression), tombstone GC, control- *)
(* marker + header stripping below StripBelow, and convergence.            *)
(*                                                                         *)
(* Records keep their original offsets across compaction ("offsets survive *)
(* the rewrite"), so the log is modelled as a SET of records each carrying *)
(* its own offset; compaction is a pure set-to-set operator. A small state *)
(* machine builds every bounded log (data / tombstone / control appends),  *)
(* and the invariants are checked against each one.                        *)
(*                                                                         *)
(*   Ceiling    - records at offset >= Ceiling are retained verbatim and   *)
(*                never counted latest-per-key (undecided / above the LSO). *)
(*   StripBelow - below it, control markers are removed and survivors are   *)
(*                header-stripped (StripBelow <= Ceiling).                  *)
(*   GCBelow    - a latest-per-key tombstone strictly below it, old enough, *)
(*                is removed entirely (the key vanishes).                   *)
(*                                                                         *)
(* Invariants:                                                             *)
(*   ViewPreserved    - a committed reader's latest-per-key value is        *)
(*                      unchanged by compaction; the only allowed change is *)
(*                      a GC'd tombstone turning "deleted" into "absent".   *)
(*   NoAbortedBelowCeiling / NoControlBelowStrip - decided junk removed.    *)
(*   AboveCeilingRetained - undecided tail is never touched.               *)
(*   TombstoneGCCorrect - a latest tombstone survives iff not GC-eligible;  *)
(*                        young / unstamped tombstones always survive.      *)
(*   Idempotent       - a second pass changes nothing (convergence).       *)
(*                                                                         *)
(* BuggyAborted = TRUE models the transaction-blind scan that counted an   *)
(* aborted record as latest-per-key; TLC then reports a ViewPreserved      *)
(* violation (an aborted record shadowing / deleting a committed value).   *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
    Keys,          \* the set of real message keys
    Vals,          \* the set of data values
    NoKey,         \* sentinel key for keyless (control) records
    MaxLen,        \* bound on the number of records in the log
    Ceiling,       \* compaction bound (records >= it are retained verbatim)
    StripBelow,    \* below this offset, control markers + headers are stripped
    GCBelow,       \* below this offset, an old latest tombstone is GC'd
    GcActive,      \* whether tombstone GC is enabled (TombstoneRetention > 0)
    BuggyAborted   \* TRUE => count aborted records latest-per-key (the H1 bug)

VARIABLES log   \* sequence of records, in append order

FixedVal == CHOOSE v \in Vals : TRUE
TS == {"none", "old", "young"}   \* tombstone age: unstamped / GC-eligible / young

Record == [ off     : 0..(MaxLen - 1),
            key     : Keys \cup {NoKey},
            val     : Vals,
            kind    : {"data", "tomb", "control"},
            aborted : BOOLEAN,
            ts      : TS,
            stripped: BOOLEAN ]

Recs == { log[i] : i \in 1..Len(log) }   \* the log as a set of records

-----------------------------------------------------------------------------
(* Compaction as a pure operator over a set of records S. *)

\* Candidacy matches compact_cleaner.go: a record participates in latest-per-key
\* when off <= Ceiling (the record AT the ceiling IS counted latest; only
\* off > Ceiling is retained verbatim/uncounted). An aborted record is a
\* candidate only under the bug.
IsCandidate(r) == /\ r.key \in Keys
                  /\ r.off <= Ceiling
                  /\ (BuggyAborted \/ ~r.aborted)
                  /\ r.kind \in {"data", "tomb"}

CandFor(S, k) == { r \in S : /\ r.key = k
                             /\ r.off <= Ceiling
                             /\ (BuggyAborted \/ ~r.aborted)
                             /\ r.kind \in {"data", "tomb"} }

\* Highest offset among a key's candidates (only used when CandFor is nonempty).
LatestOff(S, k) ==
    LET offs == { r.off : r \in CandFor(S, k) }
    IN CHOOSE o \in offs : \A p \in offs : o >= p

GCElig(r) == /\ GcActive
             /\ r.kind = "tomb"
             /\ r.ts = "old"
             /\ r.off < GCBelow

\* Whether compaction drops record r, given the surrounding set S. classify
\* retains every record at or above the ceiling verbatim; only strictly-below-
\* ceiling records are decided (dropped/stripped), so all drops are gated on
\* off < Ceiling.
Drop(S, r) ==
    /\ r.off < Ceiling
    /\ \/ (~BuggyAborted /\ r.aborted)                               \* aborted removed
       \/ (r.kind = "control" /\ r.off < StripBelow)                 \* control below strip
       \/ (IsCandidate(r) /\ r.off # LatestOff(S, r.key))            \* superseded copy
       \/ (IsCandidate(r) /\ r.off = LatestOff(S, r.key) /\ GCElig(r)) \* GC'd latest tombstone

\* Below StripBelow a surviving record is header-stripped (offset/value/etc kept).
MarkStrip(r) == IF r.off < StripBelow THEN [r EXCEPT !.stripped = TRUE] ELSE r

Compact(S) == { MarkStrip(r) : r \in { x \in S : ~Drop(S, x) } }

-----------------------------------------------------------------------------
(* The committed latest-per-key view a reader observes (aborted never seen). *)

ViewOf(S) ==
    [ k \in Keys |->
        LET c == { r \in S : r.key = k /\ ~r.aborted /\ r.off <= Ceiling }
        IN IF c = {} THEN "absent"
           ELSE LET lt == CHOOSE r \in c : \A p \in c : r.off >= p.off
                IN IF lt.kind = "data" THEN lt.val
                   ELSE IF GCElig(lt) THEN "absent" ELSE "deleted" ]

-----------------------------------------------------------------------------
(* State machine: build every bounded log. *)

TypeOK == /\ Len(log) <= MaxLen
          /\ \A i \in 1..Len(log) : log[i] \in Record

Init == log = <<>>

MkRec(k, v, kd, ab, t) ==
    [ off |-> Len(log), key |-> k, val |-> v, kind |-> kd,
      aborted |-> ab, ts |-> t, stripped |-> FALSE ]

AppendData(k, v, ab) ==
    /\ Len(log) < MaxLen
    /\ log' = Append(log, MkRec(k, v, "data", ab, "none"))

AppendTomb(k, t) ==
    /\ Len(log) < MaxLen
    /\ log' = Append(log, MkRec(k, FixedVal, "tomb", FALSE, t))

AppendControl ==
    /\ Len(log) < MaxLen
    /\ log' = Append(log, MkRec(NoKey, FixedVal, "control", FALSE, "none"))

Done == Len(log) = MaxLen /\ UNCHANGED log   \* self-loop to avoid deadlock

Next ==
    \/ \E k \in Keys, v \in Vals, ab \in BOOLEAN : AppendData(k, v, ab)
    \/ \E k \in Keys, t \in TS : AppendTomb(k, t)
    \/ AppendControl
    \/ Done

Spec == Init /\ [][Next]_log

-----------------------------------------------------------------------------
(* Invariants, checked on every reachable log. *)

Cmp == Compact(Recs)

\* A committed reader's latest-per-key value is preserved; the only permitted
\* change is a GC'd tombstone turning a deletion into an absence.
ViewPreserved ==
    \A k \in Keys :
        LET vb == ViewOf(Recs)[k]
            va == ViewOf(Cmp)[k]
        IN \/ va = vb
           \/ (vb = "deleted" /\ va = "absent")

NoAbortedBelowCeiling == \A r \in Cmp : ~(r.off < Ceiling /\ r.aborted)

NoControlBelowStrip == \A r \in Cmp : ~(r.kind = "control" /\ r.off < StripBelow)

AboveCeilingRetained == \A r \in Recs : (r.off >= Ceiling) => ~Drop(Recs, r)

\* A latest-per-key tombstone survives exactly when it is not GC-eligible.
TombstoneGCCorrect ==
    \A r \in Recs :
        ( /\ r.kind = "tomb"
          /\ ~r.aborted
          /\ r.off < Ceiling
          /\ r.off = LatestOff(Recs, r.key) )
        => (~Drop(Recs, r) <=> ~GCElig(r))

\* Convergence: a second pass changes nothing.
Idempotent == Compact(Cmp) = Cmp

=============================================================================
