package commitlog

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

// captureWarnings redirects the default slog logger into a buffer for the rest
// of the test, and restores it afterwards. Safe because nothing in this package
// calls t.Parallel: the default logger is process-wide state.
func captureWarnings(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

// Leader epoch 0 must be recordable, because it is the epoch nearly every
// record in this package carries.
//
// assign() gated on `epoch > l.latestEpoch()`, and latestEpoch() returns 0 for
// an EMPTY cache. So 0 > 0 is false and epoch 0 was refused — on every log, for
// the whole of its life before a first failover, which is the state a partition
// that has never lost its leader is permanently in. Ordinary Append stamps
// epoch 0 (see leader_epoch_compaction_test.go), so this is the common case,
// not an edge one.
//
// A sentinel made of a valid value: 0 meant both "nothing recorded yet" and
// "the latest epoch is 0", and only one of those can be true of a given cache.
// The fix asks the length, which is the fact actually being tested, and lets
// the monotonicity check mean only what it says.
//
// The refusal was also SILENT. assign's else branch calls warn(), whose two
// arms are `epoch < latestEpoch` (0 < 0) and `offset < latestOffset` (offset
// against -1) — both false for the epoch-0 case, so nothing was logged at all.
// Nothing distinguished "recorded" from "dropped" at the call or afterwards.
//
// Scope, stated because the discovery came from an incident this does NOT
// explain: the public probe is unaffected either way. LastOffsetForLeaderEpoch
// asks findEpoch(epoch+1), so it answers from the NEXT epoch's anchor, which is
// recorded normally. What was wrong is the cache's account of where a log's
// epoch history begins.
//
// That incident has since been explained, and by the probe after all — not by
// its arithmetic, which is right, but by its ARGUMENT. It took a bare uint64,
// so a follower with no epoch history had nothing to pass but 0, and the log
// answered the question it was asked. See TestAnUnknownLeaderEpochProbeIsRefused
// and the Epoch type; the sentence above is still true of this fix and is left
// standing because a scope note that quietly grows to cover a later fix stops
// being one.
func TestLeaderEpochZeroIsRecorded(t *testing.T) {
	dir := tempDir(t)
	l, err := newLeaderEpochCache("epoch-zero", dir)
	require.NoError(t, err)

	require.NoError(t, l.Assign(0, 0))
	require.Len(t, l.epochOffsets, 1,
		"leader epoch 0 was dropped: the cache cannot represent the epoch that "+
			"nearly every record in this package is stamped with")
	require.EqualValues(t, 0, l.earliestOffset(),
		"the log's epoch history starts at offset 0 and the cache says otherwise")

	// Monotonicity is still enforced, which is what the gate was FOR. Without
	// this the fix would read as "accept anything when the cache is short".
	//
	// And the refusal has to SAY so. This exact shape — the epoch already latest,
	// at an offset at or after its own — fell between warn's two strict
	// comparisons and logged nothing at all, which is indistinguishable from a
	// successful assign both at the call and afterwards.
	warnings := captureWarnings(t)
	require.NoError(t, l.Assign(0, 5))
	require.Len(t, l.epochOffsets, 1, "epoch 0 was assigned twice")
	require.Contains(t, warnings.String(), "reassign",
		"a refused reassignment logged nothing: a caller cannot tell it from a "+
			"successful assign, which is how a log comes to be trusted for epoch "+
			"history it never recorded")
	require.Contains(t, warnings.String(), "epoch:0",
		"the warning did not name the assignment it refused")

	// And a real failover still appends rather than replacing.
	require.NoError(t, l.Assign(1, 50))
	require.Len(t, l.epochOffsets, 2)
	require.EqualValues(t, 1, l.LastLeaderEpoch())
	require.EqualValues(t, 0, l.earliestOffset(),
		"the epoch-0 anchor was lost when the first failover arrived")

	// The anchor a follower probes with is unchanged by the fix — measured, not
	// assumed. Asserted so a future change to findEpoch cannot quietly move it.
	off, found := l.LastOffsetForLeaderEpoch(0)
	require.True(t, found)
	require.EqualValues(t, 50, off)

	// It has to survive a reopen, or the entry exists only in memory and the
	// next process starts from the same empty cache the bug produced.
	reopened, err := newLeaderEpochCache("epoch-zero", dir)
	require.NoError(t, err)
	require.Len(t, reopened.epochOffsets, 2, "the checkpoint did not round-trip")
	require.EqualValues(t, 0, reopened.epochOffsets[0].leaderEpoch)
	require.EqualValues(t, 0, reopened.epochOffsets[0].assignedAtOffset)

	// Retention re-anchors epoch 0 rather than dropping it. This is the
	// invariant the file's own comment defends — "a trim at the earliest end
	// re-anchors rather than drops" — and it could not apply to epoch 0 while
	// epoch 0 was never in the cache to trim.
	require.NoError(t, reopened.ClearEarliest(20))
	require.EqualValues(t, 20, reopened.earliestOffset())
	require.EqualValues(t, 0, reopened.epochOffsets[0].leaderEpoch,
		"the re-anchored entry lost its epoch, so records from offset 20 no "+
			"longer report the epoch they were written under")
}

// "The epoch is already recorded" is two different events, and one message for
// both told readers the wrong thing about the common one.
//
// An IDENTICAL repeat -- same epoch, same offset -- changes nothing. It is the
// ordinary result of a double notification during a takeover, and an idempotent
// assign is exactly what should absorb it. A repeat at a DIFFERENT offset is a
// real disagreement about where that epoch began, the first answer wins, and
// something asked for an answer it did not get.
//
// Both used to say "this assignment was dropped", which is false for the first:
// nothing was dropped, because the cache already held what the call asked for. A
// cluster soak logged 21 of the benign kind across three nodes in one run and a
// peer had to ask what the line meant before they could rule it out.
//
// Neither is silent, which is the property this must not regress -- see
// TestLeaderEpochZeroIsRecorded, where the absence of a line for a refused
// assignment cost a downstream team an investigation.
func TestARepeatedEpochAssignmentIsNotReportedAsADroppedOne(t *testing.T) {
	t.Run("identical repeat says nothing changed", func(t *testing.T) {
		l := &leaderEpochCache{name: "idempotent"}
		require.NoError(t, l.Assign(6, 77))

		warnings := captureWarnings(t)
		require.NoError(t, l.Assign(6, 77))
		require.Len(t, l.epochOffsets, 1, "fixture: the repeat must not append")

		out := warnings.String()
		require.Contains(t, out, "idempotent repeat",
			"an identical assignment must be named as one, or every reader has to "+
				"compare New and Previous by eye to find out whether it mattered")
		// The precise phrase, not the substring: this arm's own message says
		// "nothing was dropped", which a looser assertion trips over.
		require.NotContains(t, out, "this assignment was dropped",
			"nothing was dropped: the cache already held exactly what was asked for")
		require.Contains(t, out, "epoch:6",
			"the line must name the assignment, or it cannot be ruled out")
	})

	t.Run("a different offset is still reported as dropped", func(t *testing.T) {
		l := &leaderEpochCache{name: "conflict"}
		require.NoError(t, l.Assign(6, 77))

		warnings := captureWarnings(t)
		require.NoError(t, l.Assign(6, 90))
		require.Len(t, l.epochOffsets, 1)
		require.EqualValues(t, 77, l.epochOffsets[0].assignedAtOffset,
			"the first anchor wins; an epoch's start offset is fixed once assigned")

		out := warnings.String()
		require.Contains(t, out, "this assignment was dropped",
			"this assignment really was dropped and the caller's answer differs "+
				"from the recorded one")
		require.NotContains(t, out, "idempotent repeat",
			"a disagreement about where epoch 6 began must not read as a no-op")
	})
}
