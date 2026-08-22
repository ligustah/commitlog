package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The probe must answer the last offset ACTUALLY stamped with the named epoch.
//
// This is the whole contract: interface.go promises an INCLUSIVE last offset and
// tells the caller to keep up to and including it, truncating from answer+1. An
// answer one too high is therefore an instruction to KEEP the first record of
// the next epoch -- precisely the divergent record the probe exists to discard.
//
// It fails on the append path because there are two Assign call sites storing
// opposite values. NewLeaderEpoch stores NewestOffset(), the predecessor's last
// offset, which is the invariant epochOffset's doc says this probe relies on.
// append() stored entry.Offset, the new epoch's FIRST offset, one higher.
//
// A leader takes the first path: it calls NewLeaderEpoch at election, so by the
// time records arrive `entry.LeaderEpoch > lastLeaderEpoch` is already false. A
// follower ingesting replicated records takes the second. So the same history
// produced two different checkpoints depending on which node wrote it, and only
// the leader's satisfied the published contract.
func TestTheProbeAnswersTheLastRecordActuallyStampedWithThatEpoch(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path: tempDir(t), MaxSegmentBytes: 1 << 20, DisableAutoClean: true,
	})
	defer cleanup()

	// epoch 1 -> offsets 0..4, epoch 2 -> 5..9, epoch 3 -> 10..14. Appended
	// through the public API with the epoch stamped on the message, which is
	// exactly how a follower ingests a replicated batch.
	for _, epoch := range []uint64{1, 2, 3} {
		for i := 0; i < 5; i++ {
			_, err := l.Append([]*Message{{
				Value:       []byte("x"),
				Timestamp:   int64(i + 1),
				LeaderEpoch: epoch,
			}})
			require.NoError(t, err)
		}
	}
	require.EqualValues(t, 14, l.NewestOffset(), "fixture: 15 records, 0..14")

	require.EqualValues(t, 4, lastOffsetForEpoch(t, l, 1),
		"epoch 1's records are offsets 0..4; answering 5 hands the caller epoch 2's "+
			"first record to KEEP, which is the record it asked how to discard")
	require.EqualValues(t, 9, lastOffsetForEpoch(t, l, 2),
		"epoch 2's records are offsets 5..9")
	require.EqualValues(t, 14, lastOffsetForEpoch(t, l, 3),
		"epoch 3 is the newest, so the answer is the log end")
}

// The two write paths must produce the SAME checkpoint for the same history.
//
// Pinned separately from the test above because they fail for different reasons
// and a single fixture would let one mask the other: the test above can be
// satisfied by changing what the probe READS, this one only by making the two
// WRITERS agree. Leader and follower disagreeing about their own epoch history
// is the defect that outlives any particular arithmetic fix.
func TestBothEpochWritePathsRecordTheSameOffset(t *testing.T) {
	// The leader's path: NewLeaderEpoch at election, then records.
	leader, cleanupLeader := setupWithOptions(t, Options{
		Path: tempDir(t), MaxSegmentBytes: 1 << 20, DisableAutoClean: true,
	})
	defer cleanupLeader()

	for i := 0; i < 3; i++ {
		_, err := leader.Append([]*Message{{
			Value: []byte("x"), Timestamp: int64(i + 1), LeaderEpoch: 1,
		}})
		require.NoError(t, err)
	}
	// Elected into epoch 2 with the log standing at offset 2, then it writes.
	require.NoError(t, leader.NewLeaderEpoch(2))
	for i := 0; i < 3; i++ {
		_, err := leader.Append([]*Message{{
			Value: []byte("x"), Timestamp: int64(i + 4), LeaderEpoch: 2,
		}})
		require.NoError(t, err)
	}

	// The follower's path: the same six records arrive already stamped, and the
	// epoch transition is recorded by append() alone.
	follower, cleanupFollower := setupWithOptions(t, Options{
		Path: tempDir(t), MaxSegmentBytes: 1 << 20, DisableAutoClean: true,
	})
	defer cleanupFollower()

	for i := 0; i < 3; i++ {
		_, err := follower.Append([]*Message{{
			Value: []byte("x"), Timestamp: int64(i + 1), LeaderEpoch: 1,
		}})
		require.NoError(t, err)
	}
	for i := 0; i < 3; i++ {
		_, err := follower.Append([]*Message{{
			Value: []byte("x"), Timestamp: int64(i + 4), LeaderEpoch: 2,
		}})
		require.NoError(t, err)
	}

	require.EqualValues(t, leader.NewestOffset(), follower.NewestOffset(),
		"fixture: both logs must hold the same records for the comparison to mean anything")

	require.EqualValues(t,
		lastOffsetForEpoch(t, leader, 1), lastOffsetForEpoch(t, follower, 1),
		"leader and follower recorded different offsets for the SAME epoch boundary; "+
			"a follower probing either one gets a different truncation point depending "+
			"only on which node happened to answer")
}

// An epoch opening across a HOLE anchors at the last record this log holds, not
// at one below the new epoch's first offset.
//
// checkAppendedSet allows a set to start above the tail on purpose: a compacted
// source has holes and a follower resuming from one appends across them. Where
// an epoch change lands on such a boundary, `first offset - 1` names an offset
// no record ever occupied, and the probe would hand a follower a hole as a
// truncation point -- telling it to KEEP records the leader compacted away,
// which this log can then never correct because it does not have them.
func TestAnEpochOpeningAcrossAGapAnchorsAtTheLastRecordHeld(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path: tempDir(t), MaxSegmentBytes: 1 << 20, DisableAutoClean: true,
	})
	defer cleanup()

	for i := 0; i < 3; i++ {
		_, err := l.Append([]*Message{{
			Value: []byte("x"), Timestamp: int64(i + 1), LeaderEpoch: 1,
		}})
		require.NoError(t, err)
	}
	require.EqualValues(t, 2, l.NewestOffset(), "fixture: epoch 1 holds 0..2")

	// Epoch 2 arrives at offset 10, leaving 3..9 as a hole -- the shape a
	// compacted source produces.
	msgs := []*Message{{Value: []byte("y"), Timestamp: 10, LeaderEpoch: 2}}
	ms, _, err := newMessageSetFromProto(10, 0, msgs)
	require.NoError(t, err)
	_, err = l.AppendMessageSet(ms)
	require.NoError(t, err, "a set starting above the tail is legitimate across a compacted hole")

	require.EqualValues(t, 2, lastOffsetForEpoch(t, l, 1),
		"epoch 1's last record here is offset 2; answering 9 names a hole and tells "+
			"the follower to keep 3..9, which this log does not have and can never correct")
}
