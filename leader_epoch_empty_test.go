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
	// Driven on the cache directly, and deliberately.
	//
	// The first version of this built a log, called NewLeaderEpoch twice on it
	// while empty, and appended -- letting the log's own retention do the
	// re-anchoring. That made the test a race against a background clean pass:
	// on this author's machine no pass ran between the two assignments and both
	// were recorded, while on CI one did, epoch 1 moved from -1 to 0, and the
	// second assignment was refused as out-of-order. It passed locally and on one
	// CI run before failing on the next. The window, not the assertion, was
	// wrong.
	//
	// ClearEarliest IS the re-anchoring, so calling it is both deterministic and
	// a more honest statement of what the limitation depends on.
	c, err := newLeaderEpochCache("empty-epoch", tempDir(t))
	require.NoError(t, err)

	// Two elections before anything is ever written. NewLeaderEpoch passes
	// NewestOffset(), which is -1 on an empty log.
	require.NoError(t, c.Assign(1, -1))
	require.NoError(t, c.Assign(2, -1))
	require.Len(t, c.epochOffsets, 2, "premise: both epochs are recorded while the log is empty")

	// Retention then trims to a floor of 0, and every entry below it re-anchors.
	require.NoError(t, c.ClearEarliest(0))

	require.Len(t, c.epochOffsets, 1,
		"epoch 1 was expected to be collapsed away; if it survived, the probe "+
			"below can now answer accurately and the documented limitation is gone")
	require.EqualValues(t, 2, c.epochOffsets[0].leaderEpoch)
	require.EqualValues(t, 0, c.epochOffsets[0].assignedAtOffset,
		"the surviving entry re-anchored to the floor rather than staying at -1")

	// So the probe for epoch 1 is answered from epoch 2's re-anchored offset.
	// The accurate answer would be -1: epoch 1 wrote nothing, so a follower that
	// was at epoch 1 holds nothing this leader can vouch for.
	off, found := c.LastOffsetForLeaderEpoch(1)
	require.True(t, found, "epoch 2's surviving entry is what answers the probe")
	require.EqualValues(t, 0, off,
		"a probe for a write-nothing epoch is answered from its successor; "+
			"see the Known limitation entry for v0.95.8")
}
