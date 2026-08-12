package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A probe that names no epoch is REFUSED, because every answer to it is a
// deletion instruction.
//
// LastOffsetForLeaderEpoch took a bare uint64. A follower with no epoch history
// — a fresh replica, or one whose checkpoint died with the process — had nothing
// to pass but 0, and 0 is a real epoch: it is what ordinary Append stamps and
// the first epoch of every log that has never failed over. So the log answered
// the question it was asked, "where does the epoch after 0 begin", which on a
// log whose first recorded epoch is 1 is offset 0. The follower truncated to 0
// and deleted 450 committed records, and then died permanently on a transaction
// it had decided and no longer had. Every step of that reads as correct.
//
// The log cannot tell those two callers apart from a uint64 and must not guess,
// so Epoch lets the caller say which it is and the unset one gets an error
// instead of an offset. This is the shape CleanSpec.Ceiling was given for the
// same reason; the difference is that a missing ceiling has a safe default and
// a missing epoch has none.
func TestAnUnknownLeaderEpochProbeIsRefused(t *testing.T) {
	l, err := New(Options{Path: tempDir(t), Name: "probe", MaxSegmentBytes: 1 << 20})
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })

	for i := range 5 {
		_, err := l.Append([]*Message{{Value: []byte{byte(i)}}})
		require.NoError(t, err)
	}
	require.NoError(t, l.NewLeaderEpoch(1))

	_, err = l.LastOffsetForLeaderEpoch(Epoch{})
	require.ErrorIs(t, err, ErrUnknownLeaderEpoch,
		"the log answered a probe that named no epoch; the answer is an offset "+
			"the caller truncates to")

	// The point is that the two are DIFFERENT calls now, not that epoch 0 became
	// unaskable. It is a real epoch and it still gets a real answer.
	off, err := l.LastOffsetForLeaderEpoch(AtEpoch(0))
	require.NoError(t, err, "epoch 0 is a real epoch and must still be answerable")
	require.EqualValues(t, 4, off,
		"epoch 1's anchor: NewLeaderEpoch records it at the log's newest offset")

	// And the answer for a named epoch is unchanged by the refusal above — the
	// old signature's behaviour, still here, now reachable only on purpose.
	off, err = l.LastOffsetForLeaderEpoch(AtEpoch(1))
	require.NoError(t, err)
	require.EqualValues(t, l.NewestOffset(), off,
		"a follower level with this log has nothing to discard and must be told so")
}

// Epoch's zero value is the unknown one, and AtEpoch(0) is not it.
//
// Asserted directly because the whole fix rests on it: if the zero value ever
// became a known epoch 0, every caller that forgot to name an epoch would go
// back to being answered, and the test above would keep passing on the explicit
// Epoch{} it constructs.
func TestTheZeroEpochValueNamesNothing(t *testing.T) {
	_, known := Epoch{}.Get()
	require.False(t, known, "the zero Epoch claims to name an epoch")

	e, known := AtEpoch(0).Get()
	require.True(t, known, "AtEpoch(0) does not name epoch 0")
	require.EqualValues(t, 0, e)

	e, known = AtEpoch(7).Get()
	require.True(t, known)
	require.EqualValues(t, 7, e)
}
