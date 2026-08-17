package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A timestamp lookup on a dead log names the LOG, not the segment.
//
// The fifth path #309's defect reached, after New's Options, a corrupt block
// header, building a reader, and the replication fetch. Both public timestamp
// lookups bottom out in findEntryByTimestampResolving, which retries on
// segmentSwapped and then hands its caller the sentinel raw — and the caller
// wrapped it and returned. So a CLOSED log answered with ErrSegmentClosed, whose
// documented remedy is re-resolve and retry, and the caller re-asked a log that
// was never coming back; a DELETED log said "segment has been closed", which is
// the wrong cause as well as retryable.
//
// EarliestOffsetAfterTimestamp is the direction durable_streams calls, for
// seek-by-time on a subscription, so this is the arm with a live consumer.
// LatestOffsetBeforeTimestamp is covered too because it is defined in terms of
// the same locked helper: one fix, but two exported entry points, and a test
// asserting only the one with a caller would leave the other free to drift.
//
// Split per arm for the reason the reader and fetch cases give: the deleted and
// closed states are read separately — l.deleted under the lock, l.closed as a
// channel — and one test would be satisfied by whichever ran first.
func TestATimestampLookupOnADeadLogNamesTheLogNotTheSegment(t *testing.T) {
	lookups := []struct {
		name string
		call func(CommitLog) (int64, error)
	}{
		{"EarliestOffsetAfterTimestamp", func(l CommitLog) (int64, error) {
			return l.EarliestOffsetAfterTimestamp(0)
		}},
		{"LatestOffsetBeforeTimestamp", func(l CommitLog) (int64, error) {
			return l.LatestOffsetBeforeTimestamp(1 << 62)
		}},
	}

	for _, lookup := range lookups {
		t.Run(lookup.name, func(t *testing.T) {
			t.Run("closed", func(t *testing.T) {
				l, _ := setupWithOptions(t, Options{
					Name: "ts-closed", Path: tempDir(t), MaxSegmentBytes: 512,
				})
				appendMsg(t, l, "a record")
				require.NoError(t, l.Close())

				_, err := lookup.call(l)
				require.Error(t, err, "a timestamp lookup succeeded against a closed log")
				require.ErrorIs(t, err, ErrCommitLogClosed,
					"a closed log reported %v to a caller", err)
				require.NotErrorIs(t, err, ErrSegmentClosed,
					"the segment-level spelling is the one segmentSwapped retries; a "+
						"caller handed it re-asks a log that is never coming back")
			})

			t.Run("deleted", func(t *testing.T) {
				l, _ := setupWithOptions(t, Options{
					Name: "ts-deleted", Path: tempDir(t), MaxSegmentBytes: 512,
				})
				appendMsg(t, l, "a record")
				require.NoError(t, l.Delete())

				_, err := lookup.call(l)
				require.Error(t, err, "a timestamp lookup succeeded against a deleted log")
				require.ErrorIs(t, err, ErrCommitLogDeleted,
					"a deleted log reported %v to a caller", err)
			})
		})
	}
}

// The translation must not cost the healthy answer.
//
// The concurrent half is honest about what it does NOT prove. l.deleted is read
// directly rather than through IsDeleted() because the path already holds l.mu
// and recursive read locking is documented-unsafe — but swapping the accessor
// back in leaves this test GREEN, since appends take appendMu rather than l.mu
// and the arm only runs on a log that is closed or deleted, where nothing else
// writes. So this asserts the lookup still answers under load; the lock rule
// itself is stated as an invariant at the call site, because no test that can
// reach it can fail on it.
func TestATimestampLookupOnALiveLogStillAnswers(t *testing.T) {
	l, _ := setupWithOptions(t, Options{
		Name: "ts-live", Path: tempDir(t), MaxSegmentBytes: 512,
	})
	appendMsg(t, l, "a record")

	off, err := l.EarliestOffsetAfterTimestamp(0)
	require.NoError(t, err, "an open log must still answer a timestamp lookup")
	require.Zero(t, off, "the only record sits at offset 0")

	// Under concurrent writers, so the direct field read is exercised with the
	// write lock genuinely contended rather than only on a quiet log.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			appendMsg(t, l, "more")
		}
	}()
	for i := 0; i < 200; i++ {
		_, err := l.EarliestOffsetAfterTimestamp(0)
		require.NoError(t, err, "a lookup failed while appends were in flight")
	}
	<-done
}
