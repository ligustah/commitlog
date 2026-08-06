package commitlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	headers = map[string][]byte{
		"foo": []byte("bar"),
	}
	msgs = []*Message{
		{Value: []byte("one"), Timestamp: 1, LeaderEpoch: 42, Headers: headers},
		{Value: []byte("two"), Timestamp: 2, LeaderEpoch: 42, Headers: headers},
		{Value: []byte("three"), Timestamp: 3, LeaderEpoch: 42, Headers: headers},
		{Value: []byte("four"), Timestamp: 4, LeaderEpoch: 42, Headers: headers},
		{Value: nil, Timestamp: 5, LeaderEpoch: 42, Headers: headers},
	}
)

func TestNewCommitLog(t *testing.T) {
	var err error
	l, cleanup := setup(t)
	defer l.Close()
	defer cleanup()

	_, err = l.Append(msgs)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := l.NewReader(From(0), Uncommitted(), Follow())
	require.NoError(t, err)

	headers := make([]byte, HeaderBufferLen)
	for i, exp := range msgs {
		msg, offset, timestamp, leaderEpoch, err := r.ReadMessage(ctx, headers)
		require.NoError(t, err)
		require.Equal(t, int64(i), offset)
		require.Equal(t, msgs[i].Timestamp, timestamp)
		require.Equal(t, msgs[i].LeaderEpoch, leaderEpoch)
		require.Equal(t, []byte("bar"), msg.Headers()["foo"])
		compareMessages(t, exp, msg)
	}
}

func TestNewCommitLogEmptyPath(t *testing.T) {
	_, err := New(Options{})
	require.Error(t, err)
}

func TestAppendMessageSet(t *testing.T) {
	var err error
	l, cleanup := setup(t)
	defer l.Close()
	defer cleanup()

	set, _, err := newMessageSetFromProto(0, 0, msgs, false)
	require.NoError(t, err)

	offsets, err := l.AppendMessageSet(set)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 1, 2, 3, 4}, offsets)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := l.NewReader(From(0), Uncommitted(), Follow())
	require.NoError(t, err)

	headers := make([]byte, HeaderBufferLen)
	for i, exp := range msgs {
		msg, offset, timestamp, leaderEpoch, err := r.ReadMessage(ctx, headers)
		require.NoError(t, err)
		require.Equal(t, int64(i), offset)
		require.Equal(t, msgs[i].Timestamp, timestamp)
		require.Equal(t, msgs[i].LeaderEpoch, leaderEpoch)
		compareMessages(t, exp, msg)
	}
}

func TestCommitLogRecover(t *testing.T) {
	for _, test := range segmentSizeTests {
		t.Run(test.name, func(t *testing.T) {
			var err error
			opts := Options{
				Path:            tempDir(t),
				MaxSegmentBytes: test.segmentSize,
			}
			l, cleanup := setupWithOptions(t, opts)
			defer cleanup()

			// Append some messages.
			numMsgs := 10
			msgs := make([]*Message, numMsgs)
			for i := 0; i < numMsgs; i++ {
				msgs[i] = &Message{Value: []byte(strconv.Itoa(i))}
			}
			for _, msg := range msgs {
				_, err := l.Append([]*Message{msg})
				require.NoError(t, err)
			}

			// Read them back as a sanity check.
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r, err := l.NewReader(From(0), Uncommitted(), Follow())
			require.NoError(t, err)

			headers := make([]byte, HeaderBufferLen)
			for i, exp := range msgs {
				msg, offset, timestamp, leaderEpoch, err := r.ReadMessage(ctx, headers)
				require.NoError(t, err)
				compareMessages(t, exp, msg)
				require.Equal(t, int64(i), offset)
				require.Equal(t, msgs[i].Timestamp, timestamp)
				require.Equal(t, msgs[i].LeaderEpoch, leaderEpoch)
			}

			// Close the log and reopen, then ensure we read back the same
			// messages.
			require.NoError(t, l.Close())
			l, cleanup = setupWithOptions(t, opts)
			defer cleanup()
			defer l.Close()

			ctx, cancel = context.WithCancel(context.Background())
			defer cancel()
			r, err = l.NewReader(From(0), Uncommitted(), Follow())
			require.NoError(t, err)
			for i, exp := range msgs {
				msg, offset, timestamp, leaderEpoch, err := r.ReadMessage(ctx, headers)
				require.NoError(t, err)
				compareMessages(t, exp, msg)
				require.Equal(t, int64(i), offset)
				require.Equal(t, msgs[i].Timestamp, timestamp)
				require.Equal(t, msgs[i].LeaderEpoch, leaderEpoch)
			}
		})
	}
}

func TestCommitLogRecoverHW(t *testing.T) {
	opts := Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
		MaxLogBytes:     100,
	}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()
	// Records the watermark can actually stand on. It used to be set over an
	// EMPTY log, which reads as a checkpoint claiming 101 committed records that
	// were never written — the state a reopen now refuses to inherit, so the
	// round-trip has to be shown over a log that really holds them.
	for i := range 101 {
		offs, err := l.Append([]*Message{{Value: []byte(fmt.Sprintf("v:%d", i))}})
		require.NoError(t, err)
		require.EqualValues(t, i, offs[0])
	}
	l.SetHighWatermark(100)
	require.Equal(t, int64(100), l.HighWatermark())
	require.NoError(t, l.Close())
	l, cleanup = setupWithOptions(t, opts)
	defer cleanup()
	defer l.Close()
	require.Equal(t, int64(100), l.HighWatermark())
}

// A high watermark is a claim that records up to it are committed, and a log
// that reopens holding fewer than it claims must stop making the claim.
//
// The checkpoint is written on its own schedule and a crash can take back log
// bytes it had already counted, so the two can disagree by the tail. Left alone
// the log is unreadable — resolving the watermark to a segment finds none, and
// every committed read fails with "segment not found" for offsets far below it —
// and it is unsafe once readable, because the next append lands on the very
// offset the stale watermark already calls committed, publishing a record the
// moment it is written and before anyone has committed it.
func TestAHighWatermarkAboveTheLogIsNotInherited(t *testing.T) {
	opts := Options{Path: tempDir(t), MaxSegmentBytes: 1024}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()
	for i := range 10 {
		_, err := l.Append([]*Message{{Value: []byte(fmt.Sprintf("v:%d", i))}})
		require.NoError(t, err)
	}
	l.SetHighWatermark(9)
	require.NoError(t, l.Close())

	// Stand in for the crash: the checkpoint counted records whose bytes never
	// reached the disk. Writing the file directly is the same end state without
	// depending on which recovery path removed them.
	require.NoError(t, os.WriteFile(
		filepath.Join(opts.Path, hwFileName), []byte("40"), 0o644))

	reopened, cleanup2 := setupWithOptions(t, opts)
	defer cleanup2()
	defer reopened.Close()
	require.EqualValues(t, 9, reopened.NewestOffset())
	require.EqualValues(t, 9, reopened.HighWatermark(),
		"the log kept a watermark 31 records above anything it holds")

	// Readable, which is the failure this was found through.
	r, err := reopened.NewReader(From(0))
	require.NoError(t, err, "a log with a stale watermark could not be read at all")
	msg, offset, _, _, err := r.ReadMessage(context.Background(), make([]byte, HeaderBufferLen))
	require.NoError(t, err)
	require.EqualValues(t, 0, offset)
	require.Equal(t, "v:0", string(msg.Value()))

	// And the next record is uncommitted until someone commits it, rather than
	// arriving already inside the range the old checkpoint claimed.
	offs, err := reopened.Append([]*Message{{Value: []byte("v:after")}})
	require.NoError(t, err)
	require.Greater(t, offs[0], reopened.HighWatermark(),
		"an append landed at or below the high watermark, so it was published "+
			"as committed by the act of being written")
}

func TestOverrideHighWatermark(t *testing.T) {
	l, cleanup := setup(t)
	defer l.Close()
	defer cleanup()

	l.SetHighWatermark(100)
	require.Equal(t, int64(100), l.HighWatermark())
	l.OverrideHighWatermark(90)
	require.Equal(t, int64(90), l.HighWatermark())
}

func BenchmarkCommitLog(b *testing.B) {
	var err error
	l, cleanup := setup(b)
	defer l.Close()
	defer cleanup()

	for i := 0; i < b.N; i++ {
		_, err = l.Append(msgs)
		require.NoError(b, err)
	}
}

func TestOffsets(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 20,
	})
	defer l.Close()
	defer cleanup()
	require.Equal(t, int64(-1), l.OldestOffset())
	require.Equal(t, int64(-1), l.NewestOffset())

	numMsgs := 5
	msgs := make([]*Message, numMsgs)
	for i := 0; i < numMsgs; i++ {
		msgs[i] = &Message{Value: []byte(strconv.Itoa(i))}
	}
	_, err := l.Append(msgs)
	require.NoError(t, err)

	require.Equal(t, int64(0), l.OldestOffset())
	require.Equal(t, int64(4), l.NewestOffset())
}

func TestDelete(t *testing.T) {
	l, cleanup := setup(t)
	defer cleanup()
	_, err := os.Stat(l.Path)
	require.False(t, os.IsNotExist(err))
	require.NoError(t, l.Delete())
	_, err = os.Stat(l.Path)
	require.True(t, os.IsNotExist(err))
}

func TestCleaner(t *testing.T) {
	l, cleanup := setup(t)
	defer l.Close()
	defer cleanup()

	_, err := l.Append(msgs)
	require.NoError(t, err)
	segments := l.Segments()
	require.Equal(t, 1, len(l.Segments()))

	_, err = l.Append(msgs)
	require.NoError(t, err)

	require.NoError(t, l.Clean())

	require.Equal(t, 1, len(l.Segments()))
	for i, s := range l.Segments() {
		require.NotEqual(t, s, segments[i])
	}
}

// Ensure Clean deletes leader epoch offsets from the cache when segments are
// deleted but compaction is not run.
func TestCleanerDeleteLeaderEpochOffsets(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 6,
		MaxLogMessages:  5,
	})
	defer l.Close()
	defer cleanup()

	require.Equal(t, uint64(0), l.LastLeaderEpoch())
	require.Equal(t, int64(-1), l.LastOffsetForLeaderEpoch(0))

	// Add some messages.
	for i := 0; i < 5; i++ {
		_, err := l.Append([]*Message{{
			Value:       []byte(strconv.Itoa(i)),
			Timestamp:   time.Now().UnixNano(),
			LeaderEpoch: 1,
		}})
		require.NoError(t, err)
	}

	for i := 0; i < 5; i++ {
		_, err := l.Append([]*Message{{
			Value:       []byte(strconv.Itoa(i + 5)),
			Timestamp:   time.Now().UnixNano(),
			LeaderEpoch: 2,
		}})
		require.NoError(t, err)
	}

	for i := 0; i < 5; i++ {
		_, err := l.Append([]*Message{{
			Value:       []byte(strconv.Itoa(i + 10)),
			Timestamp:   time.Now().UnixNano(),
			LeaderEpoch: 3,
		}})
		require.NoError(t, err)
	}

	require.Equal(t, 15, len(l.Segments()))

	require.Equal(t, 3, len(l.leaderEpochCache.epochOffsets))
	require.Equal(t, uint64(3), l.LastLeaderEpoch())
	require.Equal(t, int64(0), l.LastOffsetForLeaderEpoch(0))
	require.Equal(t, int64(5), l.LastOffsetForLeaderEpoch(1))
	require.Equal(t, int64(10), l.LastOffsetForLeaderEpoch(2))
	require.Equal(t, int64(14), l.LastOffsetForLeaderEpoch(3))

	// Force a clean.
	require.NoError(t, l.Clean())

	require.Equal(t, 5, len(l.Segments()))
	require.Equal(t, int64(10), l.OldestOffset())
	require.Equal(t, int64(14), l.NewestOffset())
	require.Equal(t, 1, len(l.leaderEpochCache.epochOffsets))
	require.Equal(t, uint64(3), l.LastLeaderEpoch())
	require.Equal(t, int64(10), l.LastOffsetForLeaderEpoch(0))
	require.Equal(t, int64(10), l.LastOffsetForLeaderEpoch(1))
	require.Equal(t, int64(10), l.LastOffsetForLeaderEpoch(2))
	require.Equal(t, int64(14), l.LastOffsetForLeaderEpoch(3))
}

// Ensure a compacting Clean keeps every leader epoch and only raises the
// cache's floor to what survived.
//
// Note what the epoch offsets are NOT: where the first surviving record of an
// epoch sits. Epoch 2's records here are offsets 5..9 and compaction keeps only
// the last of them, but the cache still answers 5, because that is where epoch
// 2's leadership began and compaction did not change it. The answer is also the
// safe one for the caller that asks: a follower told 5 rolls back everything it
// holds from 5 on, whereas 9 would tell it to keep local copies of 5..8 — which
// this leader no longer has, so nothing would ever correct them.
func TestCleanerKeepsLeaderEpochOffsetsThroughCompaction(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 6,
		Compact:         true,
	})
	defer l.Close()
	defer cleanup()

	require.Equal(t, uint64(0), l.LastLeaderEpoch())
	require.Equal(t, int64(-1), l.LastOffsetForLeaderEpoch(0))

	// Add some messages.
	for i := 0; i < 5; i++ {
		_, err := l.Append([]*Message{{
			Key:         []byte("foo"),
			Value:       []byte(strconv.Itoa(i)),
			Timestamp:   time.Now().UnixNano(),
			LeaderEpoch: 1,
		}})
		require.NoError(t, err)
	}

	for i := 0; i < 5; i++ {
		_, err := l.Append([]*Message{{
			Key:         []byte("bar"),
			Value:       []byte(strconv.Itoa(i + 5)),
			Timestamp:   time.Now().UnixNano(),
			LeaderEpoch: 2,
		}})
		require.NoError(t, err)
	}

	for i := 0; i < 5; i++ {
		_, err := l.Append([]*Message{{
			Key:         []byte("baz"),
			Value:       []byte(strconv.Itoa(i + 10)),
			Timestamp:   time.Now().UnixNano(),
			LeaderEpoch: 3,
		}})
		require.NoError(t, err)
	}

	// Set the HW so compaction will run.
	l.SetHighWatermark(l.NewestOffset())

	require.Equal(t, 15, len(l.Segments()))

	require.Equal(t, 3, len(l.leaderEpochCache.epochOffsets))
	require.Equal(t, uint64(3), l.LastLeaderEpoch())
	require.Equal(t, int64(0), l.LastOffsetForLeaderEpoch(0))
	require.Equal(t, int64(5), l.LastOffsetForLeaderEpoch(1))
	require.Equal(t, int64(10), l.LastOffsetForLeaderEpoch(2))
	require.Equal(t, int64(14), l.LastOffsetForLeaderEpoch(3))

	// Force a clean.
	require.NoError(t, l.Clean())

	require.Equal(t, 3, len(l.Segments()))
	require.Equal(t, int64(4), l.OldestOffset())
	require.Equal(t, int64(14), l.NewestOffset())
	require.Equal(t, 3, len(l.leaderEpochCache.epochOffsets))
	require.Equal(t, uint64(3), l.LastLeaderEpoch())
	// Epoch 1 started at 0, which is now below the floor, so it is re-anchored
	// there rather than dropped. Epochs 2 and 3 are untouched: a clean does not
	// move where a leadership began.
	require.Equal(t, int64(4), l.LastOffsetForLeaderEpoch(0))
	require.Equal(t, int64(5), l.LastOffsetForLeaderEpoch(1))
	require.Equal(t, int64(10), l.LastOffsetForLeaderEpoch(2))
	require.Equal(t, int64(14), l.LastOffsetForLeaderEpoch(3))
}

// Ensure EarliestOffsetAfterTimestamp returns the earliest offset whose
// timestamp is greater than or equal to the given timestamp.
func TestEarliestOffsetAfterTimestamp(t *testing.T) {
	opts := Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
	}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()

	// Append some messages.
	numMsgs := 10
	msgs := make([]*Message, numMsgs)
	for i := 0; i < numMsgs; i++ {
		msgs[i] = &Message{Value: []byte(strconv.Itoa(i)), Timestamp: int64(max(i*10, 1))}
	}
	for _, msg := range msgs {
		_, err := l.Append([]*Message{msg})
		require.NoError(t, err)
	}

	// Underflowed timestamp should return the first offset.
	offset, err := l.EarliestOffsetAfterTimestamp(-1)
	require.NoError(t, err)
	require.Equal(t, int64(0), offset)

	// Find offset in the first segment.
	offset, err = l.EarliestOffsetAfterTimestamp(20)
	require.NoError(t, err)
	require.Equal(t, int64(2), offset)

	// Find offset in an inner segment.
	offset, err = l.EarliestOffsetAfterTimestamp(30)
	require.NoError(t, err)
	require.Equal(t, int64(3), offset)

	// Find offset in the last segment.
	offset, err = l.EarliestOffsetAfterTimestamp(90)
	require.NoError(t, err)
	require.Equal(t, int64(9), offset)

	// Overflowed timestamp should return the next offset.
	offset, err = l.EarliestOffsetAfterTimestamp(500)
	require.NoError(t, err)
	require.Equal(t, int64(10), offset)

	// Find offset for timestamp not present in log. Should return offset with
	// next highest timestamp.
	offset, err = l.EarliestOffsetAfterTimestamp(25)
	require.NoError(t, err)
	require.Equal(t, int64(3), offset)
}

// A resume point must not skip the records of its own instant that happen to
// sit in the previous segment.
//
// Every case above gives each message a timestamp of its own, and with distinct
// timestamps the search cannot get this wrong. Real logs are not like that: the
// clock is coarser than the rate records are appended at, so a run of them
// shares one instant, and a segment roll lands wherever the byte budget says —
// including in the middle of such a run. The new segment's base timestamp is
// then exactly the one the previous segment's tail is still carrying.
//
// The search is by base timestamp and strictly greater, so asking for that
// instant used to land on the later segment and answer with its first record.
// Every record of the same instant in the earlier segment — the ones the caller
// asked for first — was skipped, and a resume point goes straight to a reader.
// Measured here before the fix: asking for the first run's instant answered 3
// instead of 0, so a consumer resuming from it lost three records.
func TestEarliestOffsetAfterTimestampWhenAnInstantSpansSegments(t *testing.T) {
	opts := Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
	}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()

	// Runs of four sharing a timestamp, against a segment that holds fewer than
	// four: every run is guaranteed to straddle a roll.
	const runLength = 4
	for i := range 12 {
		_, err := l.Append([]*Message{{
			Value:     []byte(strconv.Itoa(i)),
			Timestamp: int64((i/runLength + 1) * 10),
		}})
		require.NoError(t, err)
	}

	for run := range 3 {
		ts := int64((run + 1) * 10)
		want := int64(run * runLength)
		offset, err := l.EarliestOffsetAfterTimestamp(ts)
		require.NoError(t, err)
		require.Equal(t, want, offset,
			"timestamp %d is carried by offsets %d..%d; resuming from it must start at the first",
			ts, want, want+runLength-1)
	}

	// And the gaps between runs, which resolve to the start of the next run for
	// the same reason.
	offset, err := l.EarliestOffsetAfterTimestamp(15)
	require.NoError(t, err)
	require.Equal(t, int64(runLength), offset)
}

// A time that falls in the gap before the LAST segment must resolve into that
// segment, not past the end of the log.
//
// The fallback for "no entry in the chosen segment is at or after the target"
// searched the next segment only while idx < len(segments)-1, so when the
// segment holding the answer was the last one it was never looked at and the
// answer was the next assignable offset. A consumer resuming from that reads
// nothing and waits, having been told the whole final segment is in its past.
//
// Reachable whenever a roll and a pause coincide: the previous segment's tail is
// stamped before the pause, the new segment's first record after it, and any
// resume point aimed into the pause lands in the gap between them. Which is
// ordinary — a stream idle for a moment and then written to again.
func TestEarliestOffsetAfterTimestampInTheGapBeforeTheLastSegment(t *testing.T) {
	opts := Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
	}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()

	// Spaced out, so between any two consecutive records there is a time no
	// record carries — which is where a resume point aimed at a quiet moment
	// lands.
	for i := range 12 {
		_, err := l.Append([]*Message{{
			Value:     []byte(strconv.Itoa(i)),
			Timestamp: int64(i+1) * 10,
		}})
		require.NoError(t, err)
	}

	last := l.segments[len(l.segments)-1]
	prev := l.segments[len(l.segments)-2]
	require.Positive(t, last.FirstWriteTime(),
		"the last segment is empty, so the case this test is for was not built")

	// Inside the gap the roll falls in: after everything the previous segment
	// holds, before anything the last one does.
	target := prev.LastWriteTime() + 1
	require.Less(t, target, last.FirstWriteTime(), "the gap is not a gap")

	offset, err := l.EarliestOffsetAfterTimestamp(target)
	require.NoError(t, err)
	require.Equal(t, last.BaseOffset, offset,
		"a resume point in the gap before the last segment must start at that "+
			"segment's first record, not past the end of the log")
}

// Ensure EarliestOffsetAfterTimestamp returns the next assignable offset
// when the log is empty.
func TestEarliestOffsetAfterTimestampEmptyLog(t *testing.T) {
	opts := Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
	}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()

	offset, err := l.EarliestOffsetAfterTimestamp(100)
	require.NoError(t, err)
	require.Equal(t, int64(0), offset)
}

// Ensure LatestOffsetBeforeTimestamp returns the latest offset whose
// timestamp is less than or equal to the given timestamp.
func TestLatestOffsetBeforeTimestamp(t *testing.T) {
	opts := Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
	}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()

	// Append some messages.
	numMsgs := 10
	msgs := make([]*Message, numMsgs)
	for i := 0; i < numMsgs; i++ {
		msgs[i] = &Message{Value: []byte(strconv.Itoa(i)), Timestamp: int64(max(i*10, 1))}
	}
	for _, msg := range msgs {
		_, err := l.Append([]*Message{msg})
		require.NoError(t, err)
	}

	// Underflowed timestamp should return an error.
	_, err := l.LatestOffsetBeforeTimestamp(-1)
	require.Error(t, err)

	// Find offset for timestamp right after first entry.
	offset, err := l.LatestOffsetBeforeTimestamp(5)
	require.NoError(t, err)
	require.Equal(t, int64(0), offset)

	// Find offset in the first segment.
	offset, err = l.LatestOffsetBeforeTimestamp(20)
	require.NoError(t, err)
	require.Equal(t, int64(2), offset)

	// Find offset in an inner segment.
	offset, err = l.LatestOffsetBeforeTimestamp(30)
	require.NoError(t, err)
	require.Equal(t, int64(3), offset)

	// Find offset in the last segment.
	offset, err = l.LatestOffsetBeforeTimestamp(90)
	require.NoError(t, err)
	require.Equal(t, int64(9), offset)

	// Overflowed timestamp should return high water mark.
	offset, err = l.LatestOffsetBeforeTimestamp(500)
	require.NoError(t, err)
	require.Equal(t, int64(9), offset)

	// Find offset for timestamp not present in log. Should return offset with
	// next lowest timestamp.
	offset, err = l.LatestOffsetBeforeTimestamp(25)
	require.NoError(t, err)
	require.Equal(t, int64(2), offset)
}

// The mirror of the tie question, for the as-of function.
//
// Every case above gives each record a timestamp of its own, so "the latest
// offset whose timestamp is at or before T" and "the first offset whose
// timestamp is T" are the same record and the difference never shows. Under a
// clock coarser than the append rate they are not the same record: a run shares
// an instant, and this function's contract is the LAST of that run.
func TestLatestOffsetBeforeTimestampLandsOnTheLastRecordOfATie(t *testing.T) {
	opts := Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
	}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()

	const runLength = 4
	for i := range 12 {
		_, err := l.Append([]*Message{{
			Value:     []byte(strconv.Itoa(i)),
			Timestamp: int64((i/runLength + 1) * 10),
		}})
		require.NoError(t, err)
	}

	for run := range 3 {
		ts := int64((run + 1) * 10)
		want := int64(run*runLength + runLength - 1)
		offset, err := l.LatestOffsetBeforeTimestamp(ts)
		require.NoError(t, err)
		require.Equal(t, want, offset,
			"timestamp %d is carried by offsets %d..%d; the latest at or before "+
				"it is the last of them", ts, want-runLength+1, want)
	}

	// And a time between two runs, which is the same question asked of a
	// timestamp no record carries: still the last record of the earlier run.
	offset, err := l.LatestOffsetBeforeTimestamp(15)
	require.NoError(t, err)
	require.Equal(t, int64(runLength-1), offset)
}

// Ensure Truncate removes log entries up to the given offset and that the
// leader epoch cache is also truncated.
func TestTruncate(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 6,
	})
	defer l.Close()
	defer cleanup()

	// Add some messages.
	for i := 0; i < 5; i++ {
		_, err := l.Append([]*Message{{
			Value:       []byte(strconv.Itoa(i)),
			Timestamp:   time.Now().UnixNano(),
			LeaderEpoch: 1,
		}})
		require.NoError(t, err)
	}

	for i := 0; i < 5; i++ {
		_, err := l.Append([]*Message{{
			Value:       []byte(strconv.Itoa(i + 5)),
			Timestamp:   time.Now().UnixNano(),
			LeaderEpoch: 2,
		}})
		require.NoError(t, err)
	}

	for i := 0; i < 5; i++ {
		_, err := l.Append([]*Message{{
			Value:       []byte(strconv.Itoa(i + 10)),
			Timestamp:   time.Now().UnixNano(),
			LeaderEpoch: 3,
		}})
		require.NoError(t, err)
	}

	require.Equal(t, int64(14), l.NewestOffset())
	require.Equal(t, 3, len(l.leaderEpochCache.epochOffsets))
	require.Equal(t, uint64(3), l.LastLeaderEpoch())
	require.Equal(t, int64(0), l.LastOffsetForLeaderEpoch(0))
	require.Equal(t, int64(5), l.LastOffsetForLeaderEpoch(1))
	require.Equal(t, int64(10), l.LastOffsetForLeaderEpoch(2))
	require.Equal(t, int64(14), l.LastOffsetForLeaderEpoch(3))

	require.NoError(t, l.Truncate(7))

	require.Equal(t, int64(6), l.NewestOffset())
	require.Equal(t, 2, len(l.leaderEpochCache.epochOffsets))
	require.Equal(t, uint64(2), l.LastLeaderEpoch())
	require.Equal(t, int64(0), l.LastOffsetForLeaderEpoch(0))
	require.Equal(t, int64(5), l.LastOffsetForLeaderEpoch(1))
}

// Ensure NotifyLEO returns a closed channel when the given offset is not the
// current log end offset.
func TestNotifyLEOMismatch(t *testing.T) {
	l, cleanup := setup(t)
	defer l.Close()
	defer cleanup()

	// Add some messages.
	for i := 0; i < 5; i++ {
		_, err := l.Append([]*Message{{
			Value:       []byte(strconv.Itoa(i)),
			Timestamp:   time.Now().UnixNano(),
			LeaderEpoch: 1,
		}})
		require.NoError(t, err)
	}

	// Get current log end offset and then add another message.
	leo := l.NewestOffset()
	_, err := l.Append([]*Message{{
		Value:       []byte(strconv.Itoa(5)),
		Timestamp:   time.Now().UnixNano(),
		LeaderEpoch: 1,
	}})
	require.NoError(t, err)

	// Notify LEO should return a closed channel because the LEO is different
	// than the expected LEO.
	waiter := struct{}{}
	ch := l.NotifyLEO(waiter, leo)
	select {
	case <-ch:
	default:
		t.Fatalf("Expected closed channel")
	}
}

// Ensure NotifyLEO returns a channel that is closed once more data is written
// to the log past the log end offset.
func TestNotifyLEONewData(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 256,
	})
	defer l.Close()
	defer cleanup()

	// Add some messages.
	for i := 0; i < 5; i++ {
		_, err := l.Append([]*Message{{
			Value:       []byte(strconv.Itoa(i)),
			Timestamp:   time.Now().UnixNano(),
			LeaderEpoch: 1,
		}})
		require.NoError(t, err)
	}

	// Get current log end offset.
	leo := l.NewestOffset()

	// Register a waiter.
	waiter := struct{}{}
	ch := l.NotifyLEO(waiter, leo)

	select {
	case <-ch:
		t.Fatalf("Unexpected channel close")
	default:
	}

	// Add another message.
	_, err := l.Append([]*Message{{
		Value:       []byte(strconv.Itoa(5)),
		Timestamp:   time.Now().UnixNano(),
		LeaderEpoch: 1,
	}})
	require.NoError(t, err)

	select {
	case <-ch:
	default:
		t.Fatalf("Expected channel to close")
	}
}

// Ensure NotifyLEO returns the same channel if the waiter is already
// registered in the log.
func TestNotifyLEOIdempotent(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 256,
	})
	defer l.Close()
	defer cleanup()

	// Get current log end offset.
	leo := l.NewestOffset()

	// Register a waiter.
	waiter := struct{}{}
	ch1 := l.NotifyLEO(waiter, leo)

	// Register the same waiter again. This should return the same channel.
	ch2 := l.NotifyLEO(waiter, leo)

	require.Equal(t, ch1, ch2)
}

// Ensure when SetReadonly is called with true on a log, Append returns
// ErrCommitLogReadonly. When SetReadonly is called with false, Appends
// succeed.
func TestSetReadonlyAppend(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 256,
	})
	defer l.Close()
	defer cleanup()

	require.False(t, l.IsReadonly())

	l.SetReadonly(true)
	require.True(t, l.IsReadonly())

	_, err := l.Append(msgs)
	require.Equal(t, ErrCommitLogReadonly, err)

	l.SetReadonly(false)
	require.False(t, l.IsReadonly())

	_, err = l.Append(msgs)
	require.NoError(t, err)
}

// Ensure when SetReadonly is called with true on a log, committed readers
// receive ErrCommitLogReadonly once they reach the LEO.
func TestSetReadonlyReadToLEO(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 256,
	})
	defer l.Close()
	defer cleanup()

	_, err := l.Append(msgs)
	require.NoError(t, err)
	r, err := l.NewReader(From(0), Follow())
	require.NoError(t, err)
	l.SetHighWatermark(4)

	l.SetReadonly(true)

	headers := make([]byte, HeaderBufferLen)
	for range msgs {
		_, _, _, _, err := r.ReadMessage(context.Background(), headers)
		require.NoError(t, err)
	}

	_, _, _, _, err = r.ReadMessage(context.Background(), headers)
	require.Equal(t, ErrCommitLogReadonly, err)
}

// Ensure when SetReadonly is called with true on a log, committed readers
// waiting for the HW to advance do not receive ErrCommitLogReadonly when the
// HW is less than the LEO.
func TestSetReadonlyDoNotWakeHWLessThanLEO(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 256,
	})
	defer l.Close()
	defer cleanup()

	_, err := l.Append(msgs)
	require.NoError(t, err)
	r, err := l.NewReader(From(0), Follow())
	require.NoError(t, err)

	go func() {
		time.Sleep(5 * time.Millisecond)
		l.SetReadonly(true)
		l.SetHighWatermark(4)
	}()

	headers := make([]byte, HeaderBufferLen)
	for range msgs {
		_, _, _, _, err := r.ReadMessage(context.Background(), headers)
		require.NoError(t, err)
	}
}

// Ensure when SetReadonly is called with true on a log, committed readers
// waiting for the HW to advance receive ErrCommitLogReadonly when the HW is
// caught up to the LEO.
func TestSetReadonlyWakeHWEqualsLEO(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 256,
	})
	defer l.Close()
	defer cleanup()

	r, err := l.NewReader(From(0), Follow())
	require.NoError(t, err)

	go func() {
		time.Sleep(5 * time.Millisecond)
		l.SetReadonly(true)
	}()

	headers := make([]byte, HeaderBufferLen)
	_, _, _, _, err = r.ReadMessage(context.Background(), headers)
	require.Equal(t, ErrCommitLogReadonly, err)
}

func setup(t testing.TB) (*commitLog, func()) {
	opts := Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 6,
		MaxLogBytes:     30,
	}
	return setupWithOptions(t, opts)
}

func setupWithOptions(t testing.TB, opts Options) (*commitLog, func()) {
	l, err := New(opts)
	require.NoError(t, err)
	return l.(*commitLog), func() {
		l.Close()
		remove(t, opts.Path)
	}
}

// tempDir creates a temporary directory and registers its removal as a
// t.Cleanup callback. Using t.Cleanup (rather than defer) ensures the
// directory is removed after all other cleanup callbacks (e.g., segment
// Close calls registered by createSegment) have run, preventing
// "file in use" errors on Windows when the mmap is still active.
func tempDir(t testing.TB) string {
	p, err := os.MkdirTemp("", "lift_")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(p) })
	return p
}

func remove(t require.TestingT, path string) {
	require.NoError(t, os.RemoveAll(path))
}
