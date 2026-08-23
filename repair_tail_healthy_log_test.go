package commitlog

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// RepairTail must not remove records from a HEALTHY log, at any watermark and
// any segment layout.
//
// This is the contract durable_streams builds on, and it is worth stating
// exactly because the doc phrase they quote back -- "truncates a torn or phantom
// suffix" -- describes the intent rather than the code. The code has two arms.
// The scanning arm walks the tail and keeps what parses. The other arm fires
// when newRecoveryReader FAILS TO OPEN, and calls Truncate(hw+1), which discards
// the entire above-watermark tail and logs nothing at all.
//
// On a replicated partition that tail is not spare data. A follower fetches
// before records commit, so everything between the commit boundary and the log
// end is ordinary replicated content on every single open. If the error arm can
// be reached without corruption, every replicated partition silently drops that
// content on every restart -- reached by ordinary chaos restarts rather than by
// damage.
//
// So the question this answers is narrow: can a healthy log take that arm? The
// cases below are the ones where hw+1 is an awkward offset rather than a
// comfortable one -- a segment boundary, a freshly rolled empty active segment,
// an unset watermark, a watermark at the very end of the sealed data.
func TestRepairTailKeepsEveryRecordOnAHealthyLog(t *testing.T) {
	for _, tc := range []struct {
		name string
		// maxSeg 0 means one segment for everything.
		maxSeg int64
		// records written before the watermark is placed.
		records int
		// hw is where the commit boundary sits; -1 leaves it unset, which is
		// what a replica that has never been told a boundary has.
		hw int64
		// rollBefore closes the active segment before reopening, so the log
		// comes back with an EMPTY active segment above the sealed data.
		rollBefore bool
	}{
		{name: "watermark mid segment", records: 6, hw: 2},
		{name: "watermark unset", records: 6, hw: -1},
		{name: "watermark at the last record", records: 6, hw: 5},
		{name: "watermark at zero", records: 6, hw: 0},
		{name: "multi segment, watermark in the first", maxSeg: 220, records: 20, hw: 2},
		{name: "multi segment, watermark unset", maxSeg: 220, records: 20, hw: -1},
		{name: "multi segment, watermark at a segment boundary", maxSeg: 220, records: 20, hw: 4},
		{name: "empty active segment above the tail", maxSeg: 220, records: 20, hw: 2, rollBefore: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := Options{Path: dir}
			if tc.maxSeg > 0 {
				opts.MaxSegmentBytes = tc.maxSeg
			}
			l, err := New(opts)
			require.NoError(t, err)

			for i := 0; i < tc.records; i++ {
				_, err := l.Append([]*Message{{
					Key:   []byte(fmt.Sprintf("k%02d", i)),
					Value: []byte("payload-payload-payload"),
				}})
				require.NoError(t, err)
			}
			newestBefore := l.NewestOffset()
			oldestBefore := l.OldestOffset()
			require.EqualValues(t, tc.records-1, newestBefore, "fixture: every record landed")

			if tc.hw >= 0 {
				l.SetHighWatermark(tc.hw)
				require.NoError(t, l.(*commitLog).checkpointHW(waitedOnRetryBudget))
			}
			if tc.rollBefore {
				// Force a fresh, empty active segment so the reopened log has one
				// above all its data -- the layout a partition has after a roll
				// that no append has followed yet.
				cl := l.(*commitLog)
			require.NoError(t, cl.split(cl.activeSegment()))
			}
			require.NoError(t, l.Close())

			// Reopen: no tearing, no truncation of the files, nothing damaged.
			// This is a plain restart.
			l2, err := New(opts)
			require.NoError(t, err)
			defer l2.Close()
			require.EqualValues(t, newestBefore, l2.NewestOffset(),
				"fixture: a clean reopen sees every record")

			tail, err := l2.(*commitLog).RepairTail()
			require.NoError(t, err)

			require.EqualValues(t, newestBefore, tail,
				"RepairTail reported a tail short of the log it was given")
			require.EqualValues(t, newestBefore, l2.NewestOffset(),
				"RepairTail REMOVED records from an undamaged log; on a replicated "+
					"partition everything above the commit boundary is ordinary "+
					"replicated content, so this is silent data loss on every restart")
			require.EqualValues(t, oldestBefore, l2.OldestOffset(),
				"RepairTail moved the oldest offset on an undamaged log")

			// Every record still reads back, in order, as uncommitted -- which is
			// the state a replica needs them in.
			requireReadsBack(t, l2, oldestBefore, newestBefore)
		})
	}
}

// A failure to LOOK at the tail must not amputate it.
//
// The phantom arm's job is to drop offsets the index claims and the log does not
// hold, and ErrSegmentNotFound is the only error that says so. A log closing or
// deleted underneath recovery, or a compaction swap that outlasts
// newSourceReader's eight retries, produces a different error entirely and says
// nothing about what is on disk -- but reached the same Truncate(hw+1) until
// v0.104.0, discarding the whole above-watermark tail.
//
// A closed log is the deterministic way to produce one of those: newSourceReader
// tests IsClosed before it tests anything else, so the reader fails with
// ErrCommitLogClosed at an offset whose records are sitting on disk untouched.
//
// THE ASSERTION IS THE WARNING, not the returned error and not the surviving
// records, and that is worth explaining because the obvious version of this test
// is vacuous. Truncate on a closed log fails too, so the old arm ALSO returned
// ErrCommitLogClosed and ALSO destroyed nothing -- the first draft here passed
// with the fix disabled. The two arms are separated only by whether the
// amputation is ATTEMPTED, and this log line is the one place that is
// observable. A fixture where Truncate would succeed would show it in the data
// instead, but reaching one needs a segment held permanently mid-swap.
func TestRepairTailDoesNotAmputateWhenItCannotRead(t *testing.T) {
	warnings := captureWarnings(t)
	dir := t.TempDir()
	opts := Options{Path: dir}
	l, err := New(opts)
	require.NoError(t, err)

	const records = 8
	for i := 0; i < records; i++ {
		_, err := l.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("k%02d", i)),
			Value: []byte("payload-payload-payload"),
		}})
		require.NoError(t, err)
	}
	// A commit boundary well below the tail, which is the ordinary state of a
	// replicated partition: everything above it is replicated-but-uncommitted.
	l.SetHighWatermark(2)
	require.NoError(t, l.(*commitLog).checkpointHW(waitedOnRetryBudget))
	require.NoError(t, l.Close())

	// RepairTail against the closed handle: newest is still 7 and the watermark
	// still 2, so it gets past the early return and tries to build a reader.
	cl := l.(*commitLog)
	require.EqualValues(t, records-1, cl.NewestOffset(), "fixture: reaches the reader")
	require.EqualValues(t, 2, cl.HighWatermark())

	_, err = cl.RepairTail()
	require.Error(t, err,
		"a tail this could not READ must be reported, not silently amputated")
	require.ErrorIs(t, err, ErrCommitLogClosed,
		"the reader's own error must reach the caller, so it can tell a log it "+
			"could not read from a log with nothing in it")
	require.NotErrorIs(t, err, ErrSegmentNotFound,
		"fixture: this must exercise the could-not-look arm, not the phantom one")

	// The discriminator. Reaching the amputation at all is the defect, whether or
	// not this particular fixture lets it complete.
	require.NotContains(t, warnings.String(), "amputating the tail",
		"RepairTail tried to amputate a tail it never managed to read; the error "+
			"was about the reader, not about what is on disk")

	// And nothing was destroyed. Weaker than the line above -- the old arm passed
	// this too -- but it is the property callers actually depend on.
	l2, err := New(opts)
	require.NoError(t, err)
	defer l2.Close()
	require.EqualValues(t, records-1, l2.NewestOffset(),
		"RepairTail amputated a tail it never managed to read; on a replicated "+
			"partition that is the replicated-but-uncommitted content, lost to an "+
			"error that was never about it")
	requireReadsBack(t, l2, 0, records-1)
}

func requireReadsBack(t *testing.T, l CommitLog, from, to int64) {
	t.Helper()
	r, err := l.NewReader(From(from), Uncommitted())
	require.NoError(t, err)
	headers := make([]byte, HeaderBufferLen)
	for want := from; want <= to; want++ {
		_, off, _, _, err := r.ReadMessage(t.Context(), headers)
		require.NoError(t, err, "reading back offset %d of %d..%d", want, from, to)
		require.EqualValues(t, want, off)
	}
}
