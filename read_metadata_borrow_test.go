package commitlog

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ReadMessageMetadata lends its bytes; it does not give them away.
//
// Raw points into the caller's payloadBuf, so the next call overwrites it. This
// pins that contract deliberately rather than leaving it to be discovered: the
// failure is quiet, and quiet is what makes it expensive.
//
// A SHORTER following record overwrites only the head of a retained value and
// leaves the tail intact, so the retained slice keeps its original length and
// still parses — with another record's bytes at the front. A decoder reading a
// count or tag byte near the start gets the wrong answer and no error. That is
// indistinguishable, from the caller's side, from the log having stored the
// wrong thing.
//
// If this test ever fails because Raw stopped aliasing, that is a fine change to
// make — but the doc on ReadMessageMetadata has to change with it.
func TestReadMessageMetadataLendsItsBuffer(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t)})
	defer l.Close() // nolint: errcheck
	defer cleanup()

	var (
		long  = bytes.Repeat([]byte("A"), 48)
		short = []byte("BBBB")
	)
	offsets, err := l.Append([]*Message{{Value: long}, {Value: short}})
	require.NoError(t, err)
	l.SetHighWatermark(offsets[len(offsets)-1])

	r, err := l.NewReader(From(0), Follow())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	headers := make([]byte, 28)
	var payload []byte

	meta, payload, err := r.ReadMessageMetadata(ctx, headers, payload)
	require.NoError(t, err)
	borrowed := meta.Raw.Value()
	require.Equal(t, long, borrowed, "the first read must return the first value")

	// A caller that copies keeps what it read...
	owned := append([]byte(nil), meta.Raw.Value()...)

	// ...and one that does not, does not.
	_, _, err = r.ReadMessageMetadata(ctx, headers, payload)
	require.NoError(t, err)

	require.Equal(t, long, owned, "a COPIED value must survive the next read")
	require.NotEqual(t, long, borrowed,
		"Raw is documented as borrowed — if it now survives the next call, update the doc")
	require.True(t, bytes.HasPrefix(borrowed, short),
		"the retained slice should begin with the FOLLOWING record's bytes: %q", borrowed)
	require.Len(t, borrowed, len(long),
		"and keep its original length, which is what makes the corruption quiet")
}
