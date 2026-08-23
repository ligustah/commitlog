package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// An epoch that writes NO records survives a clean that trims nothing, and the
// probe naming it then answers accurately.
//
// This test used to pin the opposite, as a limitation rather than an
// endorsement: v0.95.8 documented that two epochs opened back to back on an
// empty log collapse into one, so a probe for the earlier was answered from the
// successor's re-anchored offset instead of with the -1 that says "this epoch
// wrote nothing".
//
// Half of that is fixed in v0.102.0. The collapse needed `ClearEarliest` to
// treat the -1 anchor as an offset below a floor of 0, and -1 is a SENTINEL --
// "nothing preceded this epoch" -- not an offset. A floor of 0 also means
// nothing was trimmed, so the pass had no work to do and mutated the cache
// anyway. It now returns early, the entries stay at -1, and the probe is exact.
//
// The other half is not fixed, and has its own test below.
func TestAnEpochThatWroteNothingSurvivesACleanThatTrimsNothing(t *testing.T) {
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
	// a more honest statement of what the behaviour depends on.
	c, err := newLeaderEpochCache("empty-epoch", tempDir(t))
	require.NoError(t, err)

	// Two elections before anything is ever written. NewLeaderEpoch passes
	// NewestOffset(), which is -1 on an empty log.
	require.NoError(t, c.Assign(1, -1))
	require.NoError(t, c.Assign(2, -1))
	require.Len(t, c.epochOffsets, 2, "premise: both epochs are recorded while the log is empty")

	// A clean on a log where nothing has been removed passes a floor of 0.
	require.NoError(t, c.ClearEarliest(0))

	require.Len(t, c.epochOffsets, 2,
		"a floor of 0 trims no record, so it must trim no entry")
	require.EqualValues(t, -1, c.epochOffsets[0].assignedAtOffset,
		"epoch 1 still opened on an empty log; re-anchoring to 0 invents a record before it")

	off, found := c.LastOffsetForLeaderEpoch(1)
	require.True(t, found)
	require.EqualValues(t, -1, off,
		"epoch 1 wrote nothing, so a follower at epoch 1 holds nothing this log "+
			"can vouch for -- the exact answer, where this used to say 0")
}

// The half that remains: an epoch that wrote nothing IS lost once retention has
// genuinely moved, and the probe for it is then answered from the successor's
// re-anchored offset.
//
// Pinned rather than endorsed, exactly as its predecessor was. The re-anchor is
// correct here -- records really were removed and the floor really did move, so
// where the earlier epoch began is no longer vouchable -- but the consequence
// for the probe is the same inaccuracy v0.95.8 documented. If a later change
// makes this answer -1 too, this test fails and the CHANGELOG's remaining
// limitation note is stale. That is the point of it: the limitation is written
// down in prose, and prose does not fail.
func TestAnEpochThatWroteNothingIsStillLostToARealTrim(t *testing.T) {
	c, err := newLeaderEpochCache("empty-epoch-trimmed", tempDir(t))
	require.NoError(t, err)

	require.NoError(t, c.Assign(1, -1))
	require.NoError(t, c.Assign(2, -1))
	require.Len(t, c.epochOffsets, 2)

	// Retention has actually removed records: the oldest survivor is offset 5.
	require.NoError(t, c.ClearEarliest(5))

	require.Len(t, c.epochOffsets, 1, "both entries were below a floor that really moved")
	require.EqualValues(t, 2, c.epochOffsets[0].leaderEpoch,
		"the HIGHEST epoch removed is the one re-anchored")
	require.EqualValues(t, 5, c.epochOffsets[0].assignedAtOffset)

	off, found := c.LastOffsetForLeaderEpoch(1)
	require.True(t, found, "epoch 2's surviving entry is what answers the probe")
	require.EqualValues(t, 5, off,
		"still answered from the successor's re-anchored offset rather than -1; "+
			"see the remaining limitation note for v0.95.8")
}
