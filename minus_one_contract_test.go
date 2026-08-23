package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The public -1s, asserted rather than described.
//
// -1 is this package's sentinel for "no such position", and it is uniquely
// treacherous because it arrives as HONEST ARITHMETIC rather than as a marker
// anyone chose: NewestOffset is NextOffset()-1, a truncation clamps the
// watermark to the new end of the log, a follower's committed boundary is one
// below the slowest fetch. Every one of those produces -1 without any code
// meaning to, so no type and no validation catches a caller reading it as a
// position.
//
// Three separate defects in one afternoon turned on exactly that collision, two
// of them here (v0.101.0's floor rule reading a -1 anchor as an offset,
// v0.102.0's ClearEarliest comparing one against a floor) and one in a consumer.
// An audit of every producer and consumer followed; this pins what the audit
// found the interface promising, because a doc nobody executes is a doc that
// drifts.
//
// The audit's finding was not a bug. It was that HighWatermark and RepairTail
// return -1 and did not SAY so, while every other -1 in interface.go did.
func TestTheDocumentedMinusOnesAreReal(t *testing.T) {
	t.Run("a fresh log commits nothing", func(t *testing.T) {
		l, err := New(Options{Path: t.TempDir()})
		require.NoError(t, err)
		defer l.Close()

		require.EqualValues(t, -1, l.HighWatermark(),
			"a log never told a watermark has committed nothing, and 0 cannot "+
				"mean that because offset 0 is a real record")
		require.EqualValues(t, -1, l.OldestOffset(), "the emptiness test")
		require.EqualValues(t, -1, l.NewestOffset(),
			"untrimmed and empty, so this agrees with OldestOffset here -- which "+
				"is exactly why it reads as an emptiness test and is not one")

		// The arithmetic the doc promises stays sound at the sentinel.
		require.EqualValues(t, 0, l.HighWatermark()+1,
			"hw+1 is the first uncommitted offset in every case, including this one")
	})

	t.Run("NewestOffset is not an emptiness test on a trimmed log", func(t *testing.T) {
		// The case the interface doc calls out by name: trim to a non-zero base,
		// then empty it. NewestOffset answers base-1 -- a non-negative offset
		// naming a record that is gone -- while OldestOffset correctly says -1.
		l, err := New(Options{Path: t.TempDir(), MaxSegmentBytes: 220})
		require.NoError(t, err)
		defer l.Close()

		for i := 0; i < 20; i++ {
			_, err := l.Append([]*Message{{Key: []byte{byte(i)}, Value: []byte("payload-payload")}})
			require.NoError(t, err)
		}
		require.NoError(t, l.TruncateBefore(10))
		oldest := l.OldestOffset()
		require.Greater(t, oldest, int64(0), "fixture: the log is trimmed to a non-zero base")

		require.NoError(t, l.Truncate(oldest))
		require.EqualValues(t, -1, l.OldestOffset(),
			"the log is empty and the emptiness test says so")
		require.EqualValues(t, oldest-1, l.NewestOffset(),
			"NewestOffset answers base-1 on an empty trimmed log: a caller testing "+
				"NewestOffset < 0 concludes 'not empty' and reads at a tail holding nothing")
		require.GreaterOrEqual(t, l.NewestOffset(), int64(0),
			"the trap, stated as an assertion: this is non-negative on an EMPTY log")
	})

	t.Run("Truncate lowers the watermark to -1 when it empties the log", func(t *testing.T) {
		l, err := New(Options{Path: t.TempDir()})
		require.NoError(t, err)
		defer l.Close()

		for i := 0; i < 5; i++ {
			_, err := l.Append([]*Message{{Key: []byte{byte(i)}, Value: []byte("v")}})
			require.NoError(t, err)
		}
		l.SetHighWatermark(4)
		require.EqualValues(t, 4, l.HighWatermark())

		require.NoError(t, l.Truncate(0))
		require.EqualValues(t, -1, l.HighWatermark(),
			"a truncation that removes everything leaves nothing committed; the "+
				"watermark must not keep naming records that are gone")
	})

	t.Run("RepairTail reports -1 for a log with nothing good in it", func(t *testing.T) {
		l, err := New(Options{Path: t.TempDir()})
		require.NoError(t, err)
		defer l.Close()

		lastGood, err := l.(*commitLog).RepairTail()
		require.NoError(t, err, "an empty log is not a damaged one")
		require.EqualValues(t, -1, lastGood,
			"a log holding nothing above a watermark of -1 has no good record to name")
		require.EqualValues(t, 0, lastGood+1,
			"the resumable position is lastGood+1, which is 0 and is correct")
	})
}
