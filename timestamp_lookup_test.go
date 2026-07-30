package commitlog

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/ligustah/commitlog/compress"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

var errInjectedBackingRead = errors.New("injected: segment data read failed")

// readFailingBacking is a segmentBacking whose data reads always fail. Embedding the
// interface leaves everything else — including the index, which lives in its own
// file — working.
//
// That separation is the whole point of the injection. Closing a segment breaks
// its index too, so findSegmentIndexByTimestamp fails before the code under test
// runs; my first attempt at this test did exactly that and passed with the fix
// REMOVED. Only the log data may fail here.
type readFailingBacking struct {
	segmentBacking
}

func (*readFailingBacking) ReadAt([]byte, int64) (int, error) { return 0, errInjectedBackingRead }

// buildBlockModeLog returns a compressed multi-segment log with real clock
// timestamps, and a target timestamp landing mid-log.
//
// Real timestamps because LatestOffsetBeforeTimestamp refuses anything earlier
// than the first segment's write time, so synthetic small values never reach the
// scan. Incompressible payloads because Snappy folded 40 copies of one string
// into a single segment, and the target must land in a segment that is not the
// first.
func buildBlockModeLog(t *testing.T) (*commitLog, int64) {
	t.Helper()
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		Compression:     compress.Snappy,
		MaxSegmentBytes: 4096,
	})
	t.Cleanup(cleanup)

	base := time.Now().UnixNano()
	const n = 40
	for i := 0; i < n; i++ {
		v := make([]byte, 512)
		_, err := rand.Read(v)
		require.NoError(t, err)
		_, err = l.Append([]*Message{
			{Key: []byte("k"), Value: v, Timestamp: base + int64(i)*int64(time.Millisecond)},
		})
		require.NoError(t, err)
	}
	require.Greater(t, len(l.Segments()), 1, "need more than one segment")
	return l, base + int64(n/2)*int64(time.Millisecond)
}

var timestampLookups = []struct {
	name string
	call func(*commitLog, int64) (int64, error)
}{
	{"LatestOffsetBeforeTimestamp", (*commitLog).LatestOffsetBeforeTimestamp},
	{"EarliestOffsetAfterTimestamp", (*commitLog).EarliestOffsetAfterTimestamp},
}

// A failed read during an as-of lookup must be an ERROR, not an offset.
//
// The block-mode timestamp path is anchorPositionForTimestamp then scanForward.
// scanForward has no end bound — it walks frames until a read fails — so it used
// to treat EVERY read failure as "nothing in this segment matched". Both
// timestamp lookups then turn that into a plausible offset with a NIL error:
//
//   - LatestOffsetBeforeTimestamp answers with the segment's NEWEST offset.
//   - EarliestOffsetAfterTimestamp answers with one PAST THE END of the log.
//
// So a consumer resuming as-of a timestamp was told it was already caught up and
// skipped every record it had not yet read. Silently, with nothing to log.
//
// Reachable, not theoretical: ErrSegmentReplaced is what compaction produces and
// what Reader.ReadMessage explicitly RETRIES rather than accepts, and a tiered
// segment adds a failed object fetch.
//
// Verified by neutralising the guard, which is how the two are known to differ in
// severity. LatestOffsetBeforeTimestamp — the as-of function — returned offset 23
// with a NIL error, the silent case in full. EarliestOffsetAfterTimestamp errored,
// but described a failed read as "entry not found", because it retries the next
// segment and propagates that segment's error; it only fabricates an offset when
// the target lands in the LAST segment. Both are asserted the same way, on the
// error naming the read failure, since a wrong reason is what makes the first one
// possible.
//
// Found by auditing the consumer half of this package's io.EOF use, after
// durable_streams reported that v0.42.0's fix had been returning them partial
// results as complete answers.
func TestTimestampLookupsRefuseAFailedReadInsteadOfGuessing(t *testing.T) {
	for _, tc := range timestampLookups {
		t.Run(tc.name, func(t *testing.T) {
			l, target := buildBlockModeLog(t)

			// A fresh log with no healthy call first: a successful lookup would warm
			// the segment's block cache, and a cached block is served without
			// touching the backing at all — so the injection would do nothing and
			// this would pass for no reason. TestTimestampLookupsWorkOnAHealthyLog
			// is what establishes the lookup works at all.
			for _, s := range l.Segments() {
				s.Lock()
				s.backing = &readFailingBacking{s.backing}
				s.Unlock()
			}

			got, err := tc.call(l, target)
			require.ErrorIs(t, err, errInjectedBackingRead,
				"the read failure did not reach the caller (got offset %d, err %v)", got, err)
		})
	}
}

// The other half: the lookups must still answer on a healthy log.
//
// Without this, the test above is satisfied by a lookup that fails always, which
// would be a worse bug than the one being fixed.
func TestTimestampLookupsWorkOnAHealthyLog(t *testing.T) {
	for _, tc := range timestampLookups {
		t.Run(tc.name, func(t *testing.T) {
			l, target := buildBlockModeLog(t)
			off, err := tc.call(l, target)
			require.NoError(t, err)
			require.GreaterOrEqual(t, off, l.OldestOffset())
			require.LessOrEqual(t, off, l.NewestOffset()+1)
		})
	}
}
