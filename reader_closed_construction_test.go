package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Building a reader on a closed or deleted log says so in the LOG's spelling.
//
// Reported by sqlcdc against v0.88.0, with the symptom that makes the defect
// legible: an operator died on "new reader: segment has been closed" and its
// four retries took 521µs, 0s, 0s, 0s. A permanent state was wearing a
// transient error, so the caller's backoff had nothing to back off from.
//
// newSourceReader consulted IsClosed/IsDeleted already — but only to decide
// whether to stop retrying, and then handed back the raw error it was holding.
// That error is ErrSegmentClosed, which segmentSwapped in this same file
// defines as the storage layer announcing a compaction swap, and which the
// comment above newSourceReader describes as the exact condition retrying
// exists to absorb. So at construction a dead handle and a segment swap were
// the same value, and no caller could tell them apart.
//
// ReadMessage has always translated. Two paths of one package answering the
// same question differently is worse than either answer applied consistently,
// which is why this mirrors it rather than inventing a third spelling.
func TestBuildingAReaderOnADeadLogNamesTheLogNotTheSegment(t *testing.T) {
	// One case per arm, deliberately. The two are separate branches reading
	// separate state — IsDeleted is a mutex-guarded bool and IsClosed is a
	// channel — and a single test would be satisfied by whichever ran first
	// while the other could return anything at all.
	t.Run("closed", func(t *testing.T) {
		l, _ := setupWithOptions(t, Options{Name: "closed", Path: tempDir(t), MaxSegmentBytes: 512})
		appendMsg(t, l, "a record")
		require.NoError(t, l.Close())

		_, err := l.NewReader(From(0))
		require.Error(t, err, "a reader was built on a closed log")
		require.ErrorIs(t, err, ErrCommitLogClosed,
			"a closed log reported %v, which is the sentinel a caller retries on", err)
		require.NotErrorIs(t, err, ErrSegmentClosed,
			"the segment-level spelling is the retryable one; returning it tells the "+
				"caller to try again against a log that is never coming back")
	})

	t.Run("deleted", func(t *testing.T) {
		l, _ := setupWithOptions(t, Options{Name: "deleted", Path: tempDir(t), MaxSegmentBytes: 512})
		appendMsg(t, l, "a record")
		require.NoError(t, l.Delete())

		_, err := l.NewReader(From(0))
		require.Error(t, err, "a reader was built on a deleted log")
		require.ErrorIs(t, err, ErrCommitLogDeleted,
			"a deleted log reported %v", err)
	})
}

// A live log still builds readers, which is what keeps the check above from
// being satisfied by a constructor that refuses everything.
func TestBuildingAReaderOnALiveLogStillWorks(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Name: "live", Path: tempDir(t), MaxSegmentBytes: 512})
	defer cleanup()
	appendMsg(t, l, "a record")

	r, err := l.NewReader(From(0))
	require.NoError(t, err, "an open log must still hand out readers")
	require.NotNil(t, r)
}

// The replication read answers the same question the same way.
//
// ReadMessageSet grew the identical pair of arms when its resolve loop was
// added, on the reasoning newSourceReader states: one path of a package
// translating and another not is worse than either rule applied consistently.
// Reasoning is not coverage. The loop's end-to-end test asserts only that no
// error appeared, so both arms sat behind a recovery that never let them be
// observed — the exact shape the sweep's "no error appeared is one assertion"
// entry describes, arriving in the change that entry was written about.
//
// Split per arm for the reason the reader cases give: IsDeleted and IsClosed
// read separate state, and one test would be satisfied by whichever ran first.
func TestAReplicationFetchOnADeadLogNamesTheLogNotTheSegment(t *testing.T) {
	t.Run("closed", func(t *testing.T) {
		l, _ := setupWithOptions(t, Options{Name: "fetch-closed", Path: tempDir(t), MaxSegmentBytes: 512})
		appendMsg(t, l, "a record")
		require.NoError(t, l.Close())

		_, err := l.ReadMessageSet(0, 4096)
		require.Error(t, err, "a fetch succeeded against a closed log")
		require.ErrorIs(t, err, ErrCommitLogClosed,
			"a closed log reported %v to a follower", err)
		require.NotErrorIs(t, err, ErrSegmentClosed,
			"the segment-level spelling is the one the resolve loop retries; a "+
				"follower handed it re-fetches a log that is never coming back")
	})

	t.Run("deleted", func(t *testing.T) {
		l, _ := setupWithOptions(t, Options{Name: "fetch-deleted", Path: tempDir(t), MaxSegmentBytes: 512})
		appendMsg(t, l, "a record")
		require.NoError(t, l.Delete())

		_, err := l.ReadMessageSet(0, 4096)
		require.Error(t, err, "a fetch succeeded against a deleted log")
		require.ErrorIs(t, err, ErrCommitLogDeleted,
			"a deleted log reported %v to a follower", err)
	})
}
