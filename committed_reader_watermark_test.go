package commitlog

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A committed reader must never read past a high watermark it does not have.
//
// This is the defect behind the intermittent "no segment to consume" in the
// follower chaos test, which had survived several releases as an unexplained
// flake and drew two independent people to the same wrong theory (a stale
// segment snapshot). The snapshot was never stale. The reader had no watermark.
//
// The way in is a race a consumer cannot avoid. It asks the log where to start,
// gets OldestOffset() == -1 because the log is empty, and passes that back a
// moment later — by which time records have landed. In newReaderCommitted both
// guards then miss: `offset > hw` is `-1 > -1`, false, and `OldestOffset() ==
// -1` is no longer true. Execution reaches the tail of the constructor, which
// computes the watermark position only `if hw != -1` and so leaves hwSeg nil
// while still returning a non-nil starting segment.
//
// That combination is unbounded. readLoop clamps a read to the watermark only
// when `r.seg == r.hwSeg`, and a nil hwSeg is equal to nothing, so the reader
// takes the whole segment regardless of what is committed. Running off the end
// of it is what raised the error — but the error was the SECOND symptom. The
// first is a committed reader serving records that were never committed, which
// is the promise the interface exists to make.
func TestACommittedReaderNeverServesWhatIsNotCommitted(t *testing.T) {
	dir := tempDir(t)
	l, cleanup := setupWithOptions(t, Options{
		Path:            dir,
		MaxSegmentBytes: 4096,
	})
	defer cleanup()

	// Records on disk, and deliberately NO SetHighWatermark: written but not
	// yet committed is an ordinary state for a follower being replicated to.
	for i := range 50 {
		_, err := l.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("k:%d", i)),
			Value: []byte(strings.Repeat("x", 48)),
		}})
		require.NoError(t, err)
	}
	require.EqualValues(t, -1, l.HighWatermark(), "test needs an uncommitted log")
	require.EqualValues(t, 0, l.OldestOffset(), "test needs records on disk")

	// -1 is exactly what OldestOffset() hands a caller a moment before the
	// first append lands, so it is a real argument and not a contrived one.
	for _, from := range []int64{-1, 0} {
		t.Run(fmt.Sprintf("from=%d", from), func(t *testing.T) {
			// noWait so "there is nothing committed" ends the read instead of
			// parking for a watermark this test never sets.
			r, err := l.newReaderCommitted(from, true)
			require.NoError(t, err)

			buf := make([]byte, 4096)
			n, err := r.Read(context.Background(), buf)
			require.ErrorIs(t, err, io.EOF,
				"a reader with no high watermark did not report end of committed data")
			require.Zero(t, n,
				"served %d bytes from a log with nothing committed", n)
		})
	}
}
