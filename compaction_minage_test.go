package commitlog

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// countByKey reads every committed message and tallies how many survive per key.
func countByKey(t *testing.T, l *commitLog) map[string]int {
	t.Helper()
	newest := l.NewestOffset()
	counts := map[string]int{}
	if newest < 0 {
		return counts
	}
	r, err := l.NewReader(0, true)
	require.NoError(t, err)
	hdr := make([]byte, 28)
	for {
		msg, off, _, _, err := r.ReadMessage(context.Background(), hdr)
		if err != nil {
			break
		}
		counts[string(msg.Key())]++
		if off >= newest {
			break
		}
	}
	return counts
}

// TestCompactionMinAge verifies the protected compaction horizon: with a
// non-zero CompactMinAge, sealed segments whose writes are within the horizon
// are kept intact (their superseded versions survive), while older segments
// compact to the latest value per key.
func TestCompactionMinAge(t *testing.T) {
	now := timestamp()
	old := now - int64(48*time.Hour)

	// appendTS appends one keyed message stamped with ts (the segment's
	// lastWriteTime is taken from the message timestamp), committing it.
	appendTS := func(l *commitLog, key, val string, ts int64) {
		offs, err := l.Append([]*Message{{Key: []byte(key), Value: []byte(val), Timestamp: ts}})
		require.NoError(t, err)
		l.SetHighWatermark(offs[len(offs)-1])
	}

	run := func(minAge time.Duration) map[string]int {
		l, cleanup := setupWithOptions(t, Options{
			Path:            tempDir(t),
			MaxSegmentBytes: 40, // small: each append rolls a new segment
			Compact:         true,
			CompactMinAge:   minAge,
		})
		defer cleanup()

		// Old key "foo": three versions stamped 48h ago.
		appendTS(l, "foo", "f1", old)
		appendTS(l, "foo", "f2", old)
		appendTS(l, "foo", "f3", old)
		// Recent key "bar": three versions stamped now.
		appendTS(l, "bar", "b1", now)
		appendTS(l, "bar", "b2", now)
		appendTS(l, "bar", "b3", now)

		require.NoError(t, l.Clean())
		return countByKey(t, l)
	}

	noLag := run(0)
	withLag := run(24 * time.Hour) // horizon = now-24h: protects the recent "bar" segments

	// Old "foo" is compacted to its latest version in both cases.
	require.Equal(t, 1, noLag["foo"], "old foo should compact to latest")
	require.Equal(t, 1, withLag["foo"], "old foo should compact to latest even with lag")
	// Recent "bar": without lag its superseded versions compact away; with lag
	// the protected recent segments keep them.
	require.Greater(t, withLag["bar"], noLag["bar"],
		"CompactMinAge must protect recent bar versions (noLag=%d withLag=%d)", noLag["bar"], withLag["bar"])
}
