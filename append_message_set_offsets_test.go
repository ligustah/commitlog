package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// AppendMessageSet takes the caller's offsets, so it has to check them.
//
// Append derives every offset from the segment's own tail and cannot produce a
// bad one. This path takes the framing verbatim, and nothing on it compared
// those offsets to anything — so a set starting at or below the tail was
// written as-is, and the log then held two records claiming one offset.
//
// The gap case is the reason the rule is "strictly above" rather than "exactly
// next": compaction leaves holes, ReadMessageSet serves the survivors, and a
// follower resuming from a compacted source has to be able to append across one.
// It is the LAST assertion here deliberately — a refusal test whose every case
// is a refusal is satisfied by a function that refuses everything.
func TestAppendMessageSetRefusesOffsetsThatDoNotFitTheTail(t *testing.T) {
	l, cleanup := setup(t)
	defer cleanup()
	defer l.Close() // nolint: errcheck

	seed, _, err := newMessageSetFromProto(0, 0, msgs, false)
	require.NoError(t, err)
	offs, err := l.AppendMessageSet(seed)
	require.NoError(t, err)
	tail := offs[len(offs)-1]
	require.EqualValues(t, tail, l.NewestOffset())

	// Starting exactly AT the tail: the first frame names a record the log
	// already holds. This is the shape a replica was observed writing twice.
	atTail, _, err := newMessageSetFromProto(tail, 0, msgs, false)
	require.NoError(t, err)
	_, err = l.AppendMessageSet(atTail)
	require.ErrorIs(t, err, ErrMessageSetRefused)
	require.EqualValues(t, tail, l.NewestOffset(), "a refused set was written anyway")

	// And below it.
	below, _, err := newMessageSetFromProto(0, 0, msgs, false)
	require.NoError(t, err)
	_, err = l.AppendMessageSet(below)
	require.ErrorIs(t, err, ErrMessageSetRefused)

	// Nothing at all. entriesForMessageSet yields no entries for anything
	// shorter than one header, and segment.write indexes the last entry after
	// the payload is already on disk — so this used to panic a segment it had
	// just appended to.
	_, err = l.AppendMessageSet(nil)
	require.ErrorIs(t, err, ErrMessageSetRefused)
	_, err = l.AppendMessageSet(make([]byte, msgSetHeaderLen-1))
	require.ErrorIs(t, err, ErrMessageSetRefused)

	// Ascending is checked across the whole set, not just at the seam: two sets
	// concatenated out of order start above the tail and then walk backwards.
	// The index is binary-searched, so a set that does not ascend leaves a
	// segment whose seek and scan disagree about which record an offset names.
	high, _, err := newMessageSetFromProto(tail+20, 0, msgs, false)
	require.NoError(t, err)
	low, _, err := newMessageSetFromProto(tail+10, 0, msgs, false)
	require.NoError(t, err)
	_, err = l.AppendMessageSet(append(append([]byte{}, high...), low...))
	require.ErrorIs(t, err, ErrMessageSetRefused)
	require.EqualValues(t, tail, l.NewestOffset(), "a refused set was written anyway")

	// The control: a gap ABOVE the tail is legitimate and must still append.
	gap, _, err := newMessageSetFromProto(tail+10, 0, msgs, false)
	require.NoError(t, err)
	got, err := l.AppendMessageSet(gap)
	require.NoError(t, err,
		"a compacted source has holes, so a follower must be able to append across one")
	require.EqualValues(t, tail+10, got[0])
	require.EqualValues(t, tail+10+int64(len(msgs))-1, l.NewestOffset())
}

// The segment holds the same invariant one level down, where the refusal above
// is only one caller's.
//
// lastOffset is what NextOffset is derived from, so a segment that lowers it
// starts handing out offsets that already name records on disk. It was a plain
// assignment: whatever the last entry said, whichever direction that was.
func TestASegmentsTailNeverMovesBackwards(t *testing.T) {
	l, cleanup := setup(t)
	defer cleanup()
	defer l.Close() // nolint: errcheck

	seed, _, err := newMessageSetFromProto(0, 0, msgs, false)
	require.NoError(t, err)
	_, err = l.AppendMessageSet(seed)
	require.NoError(t, err)

	seg := l.activeSegment()
	before := seg.NextOffset()

	// Straight at the segment, bypassing the check above — which is the point:
	// this is the guard for a caller that does not go through AppendMessageSet.
	regressing, entries, err := newMessageSetFromProto(0, seg.Position(), msgs, false)
	require.NoError(t, err)
	require.NoError(t, seg.WriteMessageSet(regressing, entries))

	require.EqualValues(t, before, seg.NextOffset(),
		"a set ending below the tail moved the segment's tail backwards, so the "+
			"next append would take an offset the segment already holds")
}

// An empty entry list is refused before anything is written, not after.
//
// The bookkeeping indexes entries[len(entries)-1] and the block path takes
// entries[:1], both AFTER the payload has gone to the backing — so this
// panicked, and panicked on a segment it had already appended bytes to.
func TestASegmentRefusesAWriteWithNoEntries(t *testing.T) {
	l, cleanup := setup(t)
	defer cleanup()
	defer l.Close() // nolint: errcheck

	seg := l.activeSegment()
	before := seg.Position()

	require.ErrorIs(t, seg.WriteMessageSet([]byte("some bytes"), nil), ErrMessageSetRefused)
	require.EqualValues(t, before, seg.Position(),
		"the payload was written before the entries were found to be empty")
}
