-------------------------------- MODULE CommitLog --------------------------------
(***************************************************************************)
(* Abstract model of commitlog's core write-ahead-log guarantees:          *)
(* append/commit, the durability/checkpoint boundary, crash + tail         *)
(* recovery, and prefix (retention) + suffix (torn) truncation.            *)
(*                                                                         *)
(* The model is deliberately abstract: offsets are dense positions, a      *)
(* record carries a value and a validity bit (a torn record has a bad      *)
(* CRC), and durability is a frontier. It is NOT a byte-level model of     *)
(* the segment format -- it captures the offset/commit/recovery contract   *)
(* that commitlog.go's SetHighWatermark / checkpointHW / RecoverTail /     *)
(* Truncate / TruncateBefore promise.                                      *)
(*                                                                         *)
(* Durability note (faithful to the code): SetHighWatermark raises the     *)
(* high watermark WITHOUT an fsync, and checkpointHW writes the HW value   *)
(* only periodically. So the on-disk checkpoint (ckpt) can lag the         *)
(* in-memory HW, and the fsync frontier (durable) is separate from both.   *)
(* A record is guaranteed to survive a crash only once it is durable       *)
(* (fsynced); a committed-but-unsynced tail can be lost on power loss --   *)
(* cluster-level durability comes from replication, which is out of scope. *)
(* everDC ("ever durably committed") is the monotone ground truth of what  *)
(* MUST survive: offsets that were both committed (<= HW) and durable.     *)
(*                                                                         *)
(* Invariants checked:                                                     *)
(*   CommittedPrefixStability - a durably-committed record is never lost,  *)
(*                              altered, or torn by any operation.         *)
(*   OffsetMonotonicity       - offsets stay dense/contiguous; the durably *)
(*                              committed frontier only grows (EverDCMono).*)
(*   RecoverySoundness        - after recovery HW never sits below the     *)
(*                              checkpoint and no torn record is committed. *)
(*                                                                         *)
(* Setting BuggyRecovery = TRUE swaps in the old "amputate to the          *)
(* checkpoint" RecoverTail to demonstrate the invariants have teeth: TLC   *)
(* then reports a CommittedPrefixStability violation (the stale-checkpoint  *)
(* data loss the real RecoverTail was written to prevent).                 *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
    MaxOffset,      \* highest offset the model will assign (bounds the log)
    Vals,           \* the set of distinct payload values a producer may append
    BuggyRecovery   \* TRUE => model the pre-fix amputating RecoverTail

VARIABLES
    log,      \* sequence of records currently on disk, oldest first
    base,     \* offset of log[1] (the retention floor); = leo when empty
    hw,       \* in-memory high watermark (commit frontier); -1 = nothing committed
    ckpt,     \* last checkpointed HW value on disk; -1 = none
    durable,  \* highest offset whose bytes are fsynced (survive a crash); -1 = none
    phase,    \* "running" | "recovering"
    written,  \* offset -> value first appended there (ground truth for stability)
    everDC    \* monotone: highest offset ever both committed AND durable

vars == <<log, base, hw, ckpt, durable, phase, written, everDC>>

MinNum(a, b) == IF a <= b THEN a ELSE b
MaxNum(a, b) == IF a >= b THEN a ELSE b
AnyVal == CHOOSE v \in Vals : TRUE

Record == [val : Vals, valid : BOOLEAN]

leo       == base + Len(log)      \* log end offset + 1 (next offset to assign)
newest    == leo - 1              \* offset of the last record, base-1 when empty
Idx(o)    == o - base + 1         \* sequence index of offset o
ValAt(o)   == log[Idx(o)].val
ValidAt(o) == log[Idx(o)].valid
Present(o) == (base <= o) /\ (o <= newest)

TypeOK ==
    /\ base \in 0..(MaxOffset + 1)
    /\ Len(log) \in 0..(MaxOffset + 1)
    /\ \A i \in 1..Len(log) : log[i] \in Record
    /\ hw \in (-1)..MaxOffset
    /\ ckpt \in (-1)..MaxOffset
    /\ durable \in (-1)..MaxOffset
    /\ everDC \in (-1)..MaxOffset
    /\ phase \in {"running", "recovering"}
    /\ written \in [0..MaxOffset -> Vals]

Init ==
    /\ log = <<>>
    /\ base = 0
    /\ hw = -1
    /\ ckpt = -1
    /\ durable = -1
    /\ everDC = -1
    /\ phase = "running"
    /\ written = [o \in 0..MaxOffset |-> AnyVal]

(* Append a record. Dense, strictly increasing offsets; does NOT move HW    *)
(* (the producer/commit separation). The new record is not yet durable.     *)
DoAppend(v) ==
    /\ phase = "running"
    /\ leo <= MaxOffset
    /\ log' = Append(log, [val |-> v, valid |-> TRUE])
    /\ written' = [written EXCEPT ![leo] = v]
    /\ UNCHANGED <<base, hw, ckpt, durable, phase, everDC>>

(* SetHighWatermark: monotone raise of the commit frontier, no fsync.       *)
Commit ==
    /\ phase = "running"
    /\ newest >= 0
    /\ \E h \in (hw + 1)..newest :
         /\ hw' = h
         /\ everDC' = MaxNum(everDC, MinNum(h, durable))
    /\ UNCHANGED <<log, base, ckpt, durable, phase, written>>

(* fsync log bytes durable WITHOUT rewriting the checkpoint file: this is    *)
(* how the on-disk checkpoint becomes stale below the real durable tail      *)
(* (the scenario RecoverTail must extend past).                             *)
Sync ==
    /\ phase = "running"
    /\ newest >= 0
    /\ durable' = newest
    /\ everDC' = MaxNum(everDC, MinNum(hw, newest))
    /\ UNCHANGED <<log, base, hw, ckpt, phase, written>>

(* checkpointHW / SyncAll: fsync everything and persist the HW value.        *)
Checkpoint ==
    /\ phase = "running"
    /\ durable' = MaxNum(durable, newest)
    /\ ckpt' = hw
    /\ everDC' = MaxNum(everDC, MinNum(hw, MaxNum(durable, newest)))
    /\ UNCHANGED <<log, base, hw, phase, written>>

(* TruncateBefore: retention deletes a committed prefix below m. This is an  *)
(* intentional GC; committed-prefix stability is asserted only over the      *)
(* live window [base, ...], so removed offsets are not stability violations. *)
TruncateBefore ==
    /\ phase = "running"
    /\ \E m \in (base + 1)..(hw + 1) :
         /\ m <= leo
         /\ base' = m
         /\ log' = SubSeq(log, m - base + 1, Len(log))
    /\ UNCHANGED <<hw, ckpt, durable, phase, written, everDC>>

(* Crash: the volatile suffix above `durable` may tear. Records [base..d]    *)
(* (d >= durable) survive intact; if d < newest the record at d+1 is torn    *)
(* (partial write) and everything above it is gone. Reopen loads HW from     *)
(* the checkpoint file.                                                      *)
Crash ==
    /\ phase = "running"
    /\ \E d \in MaxNum(durable, base - 1)..newest :
         LET torn == d < newest
             kept == SubSeq(log, 1, d - base + 1)
         IN /\ log' = IF torn
                      THEN Append(kept, [val |-> AnyVal, valid |-> FALSE])
                      ELSE kept
            /\ hw' = ckpt
    /\ phase' = "recovering"
    /\ UNCHANGED <<base, ckpt, durable, written, everDC>>

(* RecoverTail: forward-scan the suffix above the checkpoint; extend HW to    *)
(* the last structurally-valid (CRC-good) record, dropping any torn suffix.   *)
(* Never drops below the checkpoint. BuggyRecovery models the old amputation. *)
RecoverTail ==
    /\ phase = "recovering"
    \* Scan starts at the checkpoint+1, clamped up to the oldest surviving
    \* offset: retention may have deleted the prefix below a stale checkpoint.
    /\ LET scanStart == MaxNum(ckpt + 1, base)
           cv == { o \in scanStart..newest :
                     \A k \in scanStart..o : Present(k) /\ ValidAt(k) }
           lastGood == IF cv = {} THEN MaxNum(ckpt, base - 1)
                       ELSE CHOOSE x \in cv : \A y \in cv : x >= y
       IN IF BuggyRecovery
          THEN /\ log' = SubSeq(log, 1, MaxNum(ckpt - base + 1, 0))
               /\ hw' = ckpt
               /\ durable' = MaxNum(ckpt, -1)
          ELSE /\ log' = SubSeq(log, 1, MaxNum(lastGood - base + 1, 0))
               /\ hw' = lastGood
               /\ durable' = lastGood
    /\ phase' = "running"
    /\ everDC' = MaxNum(everDC, MinNum(hw', durable'))
    /\ UNCHANGED <<base, ckpt, written>>

Next ==
    \/ \E v \in Vals : DoAppend(v)
    \/ Commit
    \/ Sync
    \/ Checkpoint
    \/ TruncateBefore
    \/ Crash
    \/ RecoverTail

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Invariants *)

(* Offsets stay dense: present offsets are exactly the contiguous range      *)
(* [base, newest], with no gaps or reuse of a live offset.                   *)
Contiguity ==
    /\ base >= 0
    /\ leo = base + Len(log)
    /\ leo <= MaxOffset + 1

(* A record that was ever durably committed (and is still in the live        *)
(* window) is present, structurally valid, and carries its original value:   *)
(* never lost, torn, or altered by any operation.                           *)
CommittedPrefixStability ==
    \A o \in base..everDC :
        /\ Present(o)
        /\ ValidAt(o)
        /\ ValAt(o) = written[o]

(* After recovery the HW never sits below the checkpoint, and no torn record *)
(* is ever exposed as committed (in fact none remain in the running phase).  *)
RecoverySoundness ==
    (phase = "running") =>
        /\ hw >= ckpt
        /\ \A o \in base..newest : ValidAt(o)

(* The checkpoint is a past (hence <=) value of the monotone HW.             *)
Durability ==
    (phase = "running") => ckpt <= hw

(* The durably-committed frontier only ever grows -- offset monotonicity of  *)
(* the committed log (a temporal safety property).                          *)
EverDCMono == [][ everDC' >= everDC ]_everDC

=============================================================================
