package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// An epoch that writes NO records does not survive to be probed for, and this
// test pins that rather than endorsing it.
//
// v0.95.8 documented the shape at leaderEpochCache.LastOffsetForLeaderEpoch
// after establishing it by probe. The probe was throwaway; this is not. The
// behaviour is a consequence of two correct-in-isolation rules meeting —
// NewLeaderEpoch anchors at NewestOffset(), which is -1 on an empty log, and
// ClearEarliest re-anchors sub-floor entries as the log trims — so nothing in
// either rule's own tests would notice if the combination changed.
//
// If a future change makes the earlier epoch survive, or makes the probe answer
// -1 ("this epoch wrote nothing") instead of the successor's re-anchored
// offset, this test fails and the CHANGELOG's "Known limitation" section is
// stale. That is the point of it: the limitation is written down in prose, and
// prose does not fail.
func TestAnEpochThatWroteNothingIsNotPreserved(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t), MaxSegmentBytes: 1024})
	defer cleanup()

	require.EqualValues(t, -1, l.NewestOffset(), "premise: the log is empty")

	// Two elections land before anything is ever written, so both record -1.
	require.NoError(t, l.NewLeaderEpoch(1))
	require.NoError(t, l.NewLeaderEpoch(2))

	// Epoch 2 then writes every record in the log.
	appendToLog(t, l, []keyValue{
		{[]byte("a"), []byte("1")},
		{[]byte("b"), []byte("2")},
		{[]byte("c"), []byte("3")},
	}, true)

	require.Len(t, l.leaderEpochCache.epochOffsets, 1,
		"epoch 1 was expected to be collapsed away; if it survived, the probe "+
			"below can now answer accurately and the documented limitation is gone")
	require.EqualValues(t, 2, l.leaderEpochCache.epochOffsets[0].leaderEpoch)
	require.EqualValues(t, 0, l.leaderEpochCache.epochOffsets[0].assignedAtOffset,
		"the surviving entry re-anchored to the floor rather than staying at -1")

	// The probe for epoch 1 is therefore answered from epoch 2's re-anchored
	// offset. The accurate answer would be -1: epoch 1 wrote nothing, so a
	// follower that was at epoch 1 holds nothing this leader can vouch for.
	got, err := l.LastOffsetForLeaderEpoch(AtEpoch(1))
	require.NoError(t, err)
	require.EqualValues(t, 0, got,
		"a probe for a write-nothing epoch is answered from its successor; "+
			"see the Known limitation entry for v0.95.8")
}
