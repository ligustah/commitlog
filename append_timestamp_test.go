package commitlog

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Messages appended without a timestamp get append time (LogAppendTime
// fallback), so segment write times — which drive age retention, MaxSegmentAge
// rolling, and the CompactMinAge horizon — are real. Explicit timestamps
// (CreateTime) are preserved.
func TestAppendStampsMissingTimestamps(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t), MaxSegmentBytes: 256})
	defer l.Close()
	defer cleanup()

	before := time.Now().UnixNano()
	for i := 0; i < 20; i++ {
		_, err := l.Append([]*Message{{Value: []byte("untimestamped-" + strconv.Itoa(i))}})
		require.NoError(t, err)
	}
	after := time.Now().UnixNano()
	segs := l.Segments()
	require.GreaterOrEqual(t, len(segs), 3, "need several sealed segments")
	for i, seg := range segs {
		require.GreaterOrEqual(t, seg.FirstWriteTime(), before, "segment %d first write time", i)
		require.LessOrEqual(t, seg.FirstWriteTime(), after, "segment %d first write time", i)
		require.GreaterOrEqual(t, seg.LastWriteTime(), seg.FirstWriteTime(), "segment %d last write time", i)
	}

	// An explicit timestamp is preserved, not overwritten.
	explicit := int64(12345)
	offsets, err := l.Append([]*Message{{Value: []byte("timestamped"), Timestamp: explicit}})
	require.NoError(t, err)
	l.SetHighWatermark(offsets[0])
	r, err := l.NewReader(offsets[0], true)
	require.NoError(t, err)
	headers := make([]byte, 28)
	_, _, ts, _, err := r.ReadMessage(context.Background(), headers)
	require.NoError(t, err)
	require.Equal(t, explicit, ts)

	// Age retention now judges real ages: sealed-but-fresh segments survive a
	// generous TTL instead of being treated as infinitely old (lastWriteTime 0
	// would have deleted every sealed segment here).
	l.deleteCleaner.Retention.Age = time.Hour
	require.NoError(t, l.Clean())
	require.Equal(t, int64(0), l.OldestOffset(), "fresh segments must survive age retention")
	require.Len(t, l.Segments(), len(segs)+1, "no segment may be deleted (+1: explicit-ts append may have rolled)")
}
