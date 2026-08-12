package commitlog

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Compaction must not erase the log's leader epoch.
//
// The compacted cache is rebuilt from nothing but the per-record epoch stamps of
// the surviving records, and on a LEADER no record carries one: the only writer
// that stamps a record is AppendMessageSet, which is the follower path taking
// the leader's framing verbatim. Ordinary Append writes epoch 0, and
// NewLeaderEpoch puts the epoch in the checkpoint file and nowhere else. So a
// leader lost its epoch on the first compaction pass — the general case, not a
// corner.
//
// Downstream that epoch is the replication fence. Once it read 0, every
// follower's fetch was refused, the follower did as it was told (re-probe,
// truncate, refetch) with the same epoch, and was refused again, forever. Every
// replica of a compacted stream fell out of the in-sync set and could not
// rejoin. Reported against a stream with Compact: true and more than one
// replica, which in a cluster is the ordinary configuration.
func TestCompactionKeepsTheAssignedLeaderEpoch(t *testing.T) {
	opts := Options{Path: tempDir(t), MaxSegmentBytes: 100, Compact: true}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()

	require.NoError(t, l.NewLeaderEpoch(3))

	appendToLog(t, l, []keyValue{
		{[]byte("foo"), []byte("first")},
		{[]byte("bar"), []byte("first")},
		{[]byte("foo"), []byte("second")},
		{[]byte("foo"), []byte("third")},
		{[]byte("bar"), []byte("second")},
		{[]byte("baz"), []byte("first")},
		{[]byte("baz"), []byte("second")},
		{[]byte("qux"), []byte("first")},
		{[]byte("foo"), []byte("fourth")},
		{[]byte("baz"), []byte("third")},
	}, true)
	require.EqualValues(t, 3, l.LastLeaderEpoch(), "premise")

	require.NoError(t, l.Clean())

	require.EqualValues(t, 3, l.LastLeaderEpoch(),
		"compaction erased the leader epoch, which fences every follower out for good")
}

// An epoch assigned to an EMPTY log gets startOffset -1, since NewLeaderEpoch
// passes NewestOffset(). That is below every base offset, so anything that
// re-anchors entries at a surviving floor has to keep it rather than treat it as
// belonging to a range that no longer exists.
func TestAnEpochAssignedToAnEmptyLogSurvivesCompaction(t *testing.T) {
	opts := Options{Path: tempDir(t), MaxSegmentBytes: 100, Compact: true}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()

	require.EqualValues(t, -1, l.NewestOffset(), "test needs an empty log")
	require.NoError(t, l.NewLeaderEpoch(7))

	appendToLog(t, l, []keyValue{
		{[]byte("foo"), []byte("first")},
		{[]byte("foo"), []byte("second")},
		{[]byte("bar"), []byte("first")},
		{[]byte("foo"), []byte("third")},
		{[]byte("bar"), []byte("second")},
	}, true)
	require.EqualValues(t, 7, l.LastLeaderEpoch(), "premise")

	require.NoError(t, l.Clean())

	require.EqualValues(t, 7, l.LastLeaderEpoch(),
		"an epoch assigned before the first record did not survive compaction")
}

// An epoch every one of whose records is superseded. Its whole tenure is
// written to one key that a later epoch overwrites, so after a clean not a
// single surviving record carries it — the case that was lost even on a
// follower, where records DO carry epoch stamps. The cache is not a summary of
// what the records say; it is the log's own record of when each leadership
// began, and compaction has nothing to say about that.
func TestAnEpochWithNoSurvivingRecordsIsStillInTheCache(t *testing.T) {
	opts := Options{Path: tempDir(t), MaxSegmentBytes: 6, Compact: true}
	l, cleanup := setupWithOptions(t, opts)
	defer l.Close()
	defer cleanup()

	write := func(epoch uint64, key, value string) {
		_, err := l.Append([]*Message{{
			Key:         []byte(key),
			Value:       []byte(value),
			Timestamp:   time.Now().UnixNano(),
			LeaderEpoch: epoch,
		}})
		require.NoError(t, err)
	}
	write(1, "a", "0") // offset 0, the only copy of a — survives
	write(2, "b", "1") // offset 1, superseded
	write(2, "b", "2") // offset 2, superseded — epoch 2 keeps NOTHING
	write(3, "b", "3") // offset 3, latest b — survives
	write(3, "c", "4") // offset 4, active segment
	l.SetHighWatermark(l.NewestOffset())

	require.EqualValues(t, 1, lastOffsetForEpoch(t, l, 1), "epoch 2 began at 1")
	require.EqualValues(t, 3, lastOffsetForEpoch(t, l, 2), "epoch 3 began at 3")

	require.NoError(t, l.Clean())

	require.EqualValues(t, 0, l.OldestOffset(), "the floor did not move")
	require.EqualValues(t, 3, l.LastLeaderEpoch())
	require.Len(t, l.leaderEpochCache.epochOffsets, 3,
		"the epoch whose every record was compacted away was dropped from the cache")
	// Unchanged: epoch 2 still owns [1, 3), which is where it led, however
	// little of what it wrote is still on disk.
	require.EqualValues(t, 1, lastOffsetForEpoch(t, l, 1))
	require.EqualValues(t, 3, lastOffsetForEpoch(t, l, 2))
}
