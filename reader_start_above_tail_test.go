package commitlog

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// A start offset above the tail is refused at construction, and ONLY for an
// uncommitted reader. Both halves are contract, and both were undocumented
// until v0.96.1 -- NewReader's termination contract said ErrSegmentNotFound came
// back "only when the log holds no segments at all", which made a resumable
// position read as an empty log.
//
// The asymmetry is the half a caller cannot guess. Segment lookup resolves an
// offset forward to the first segment that could hold it and there is none at
// newest+1, so the uncommitted reader -- which is handed the caller's offset
// verbatim -- is refused. A committed reader is bounded by the watermark, which
// never exceeds the tail, so it waits at the boundary instead of failing.
func TestAStartOffsetAboveTheTailIsRefusedOnlyForAnUncommittedReader(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path: tempDir(t), MaxSegmentBytes: 1 << 20,
	})
	defer cleanup()

	_, err := l.Append([]*Message{{Value: []byte("a")}, {Value: []byte("b")}})
	require.NoError(t, err)
	l.SetHighWatermark(l.NewestOffset())
	newest := l.NewestOffset()
	require.Equal(t, int64(1), newest, "fixture: two records, tail at 1")

	// One past the tail, and far past it: the same answer, because the lookup
	// asks whether any segment reaches the offset, not how far away it is.
	for _, offset := range []int64{newest + 1, newest + 100} {
		_, err := l.NewReader(From(offset), Uncommitted())
		require.ErrorIs(t, errors.Cause(err), ErrSegmentNotFound,
			"an uncommitted reader starting above the tail must be refused, not served an empty range")
	}

	// Not the empty-log case: this log holds records, which is what makes the
	// old wording wrong rather than merely narrow.
	require.Equal(t, int64(0), l.OldestOffset())

	r, err := l.NewReader(From(newest + 1))
	require.NoError(t, err,
		"a committed reader is bounded by the watermark, so the same offset is a wait and not a failure")
	require.NotNil(t, r)
}
