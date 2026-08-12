package commitlog

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Hazard 5 of docs/segment-join.md, asserted rather than assumed — which is what
// the document asks for, because "probably untouched" is not a property.
//
// The epoch cache anchors each leader epoch at the offset it starts at, and
// ClearEarliest re-anchors rather than drops. A join removes no records and
// changes no offset, so no anchor should move. The reason to check anyway is
// that the join DOES change which segment an offset lives in, and an anchor
// derived from a segment boundary rather than from the record would move with
// it — silently, and only for the epochs whose first record happened to open a
// segment the join retired.
func TestAJoinLeavesLeaderEpochsWhereTheyWere(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 2 << 10,
	})
	defer cleanup()

	// A new epoch every 25 records, so several epochs open inside the segments a
	// run will collapse — including, with this segment size, at least one whose
	// first record is a segment's first record.
	const records = 400
	want := make(map[int64]uint64, records)
	for i := range records {
		epoch := uint64(i / 25)
		off, err := l.Append([]*Message{{
			Key:         []byte(fmt.Sprintf("key-%04d", i%16)),
			Value:       []byte(fmt.Sprintf("value-%04d-padding-padding-padding", i)),
			LeaderEpoch: epoch,
		}})
		require.NoError(t, err)
		want[off[0]] = epoch
	}
	l.SetHighWatermark(l.NewestOffset())

	pre := liveSegments(l)
	require.NotEmpty(t, planJoins(pre, joinSpec),
		"the fixture must give the planner something to join")

	// Every epoch's last offset, as the cache answers it before the join.
	lastBefore := map[uint64]int64{}
	for e := uint64(0); e <= uint64((records-1)/25); e++ {
		lastBefore[e] = lastOffsetForEpoch(t, l, e)
	}

	_, err := l.CleanWithSpec(joinSpec)
	require.NoError(t, err)
	require.Less(t, len(liveSegments(l)), len(pre), "the pass joined nothing")

	for e, off := range lastBefore {
		require.Equal(t, off, lastOffsetForEpoch(t, l, e),
			"the join moved epoch %d's boundary", e)
	}

	// And on the records themselves, which is where the epoch is actually
	// stored: the join copies frames verbatim, so every one must come back
	// carrying the epoch it was written with.
	r, err := l.NewReader(From(l.OldestOffset()), Uncommitted())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	headers := make([]byte, HeaderBufferLen)
	newest := l.NewestOffset()
	for {
		_, off, _, epoch, err := r.ReadMessage(ctx, headers)
		require.NoError(t, err)
		require.Equal(t, want[off], epoch, "record %d came back under a different leader epoch", off)
		if off >= newest {
			break
		}
	}
}
