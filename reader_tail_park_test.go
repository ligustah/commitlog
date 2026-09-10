package commitlog

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// A Follow reader asked for the log's NEXT offset parks at the tail and serves
// the next append, where every other reader above the tail is refused.
//
// The offset is not ahead of anything the reader cannot wait for: it is where
// the next record lands, by construction. Refusing it left a caller that wants
// only new records polling NewReader until the first one arrived, which is the
// shape Follow exists to remove.
func TestAFollowReaderAtTheNextOffsetParksInsteadOfFailing(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path: tempDir(t), MaxSegmentBytes: 1 << 20,
	})
	defer cleanup()

	_, err := l.Append([]*Message{{Value: []byte("a")}, {Value: []byte("b")}, {Value: []byte("c")}})
	require.NoError(t, err)
	next := l.NewestOffset() + 1
	require.Equal(t, int64(3), next, "fixture: three records, next offset 3")

	r, err := l.NewReader(From(next), Follow(), Uncommitted())
	require.NoError(t, err, "the next offset is where the next append lands, so a Follow reader can wait for it")

	// Parked, not ended: nothing is there yet, and the reader must say so by
	// waiting rather than by io.EOF or an error.
	headers := make([]byte, HeaderBufferLen)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	_, _, _, _, err = r.ReadMessage(ctx, headers)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"a reader parked at the tail waits for the append; it does not end")

	_, err = l.Append([]*Message{{Value: []byte("d")}})
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	msg, off, _, _, err := r.ReadMessage(ctx, headers)
	require.NoError(t, err)
	require.Equal(t, next, off, "the first record served is the one at the offset asked for")
	require.Equal(t, "d", string(SerializedMessage(msg).Value()))

	// Only the NEXT offset. A higher one would also be served from the tail,
	// and would start the read below the offset the caller named.
	_, err = l.NewReader(From(l.NewestOffset()+2), Follow(), Uncommitted())
	require.ErrorIs(t, errors.Cause(err), ErrSegmentNotFound,
		"a start past the next offset is still ahead of the writer")

	// And only for Follow: a bounded read of a range that holds nothing is
	// still refused rather than served as an instant empty range.
	_, err = l.NewReader(From(l.NewestOffset()+1), Uncommitted())
	require.ErrorIs(t, errors.Cause(err), ErrSegmentNotFound)
}

// The same on a log with nothing in it: offset 0 is the next offset, and an
// empty active segment reaches it the same way a written one does.
func TestAFollowReaderOnAnEmptyLogParksAtOffsetZero(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path: tempDir(t), MaxSegmentBytes: 1 << 20,
	})
	defer cleanup()

	r, err := l.NewReader(From(0), Follow(), Uncommitted())
	require.NoError(t, err)

	_, err = l.Append([]*Message{{Value: []byte("first")}})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	msg, off, _, _, err := r.ReadMessage(ctx, make([]byte, HeaderBufferLen))
	require.NoError(t, err)
	require.Equal(t, int64(0), off)
	require.Equal(t, "first", string(SerializedMessage(msg).Value()))
}
