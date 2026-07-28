package commitlog

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestReaderCommittedLargeMessage: a committed read of a message larger than
// bufReadSize takes the direct ReadAt path; the buffered reader must stay in
// sync so the NEXT message's header is read from the right position.
func TestReaderCommittedLargeMessage(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t)})
	defer l.Close()
	defer cleanup()

	big := bytes.Repeat([]byte("x"), bufReadSize+1024)
	msgs := []*Message{
		{Value: []byte("small-0")},
		{Value: big},
		{Value: []byte("small-2")},
		{Value: big},
		{Value: []byte("small-4")},
	}
	offsets, err := l.Append(msgs)
	require.NoError(t, err)
	l.SetHighWatermark(offsets[len(offsets)-1])

	r, err := l.NewReader(From(0), Follow())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	headers := make([]byte, 28)
	for i, want := range msgs {
		msg, offset, _, _, err := r.ReadMessage(ctx, headers)
		require.NoError(t, err, "message %d", i)
		require.Equal(t, int64(i), offset, "message %d", i)
		require.Equal(t, want.Value, msg.Value(), "message %d", i)
	}
}

// TestReaderCommittedLargeMessageMetadata: same as above through the
// metadata-scan path used by transactional stream readers.
func TestReaderCommittedLargeMessageMetadata(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t)})
	defer l.Close()
	defer cleanup()

	big := bytes.Repeat([]byte("y"), bufReadSize+1024)
	msgs := []*Message{
		{Value: []byte("small-0")},
		{Value: big},
		{Value: []byte("small-2")},
		{Value: big},
		{Value: []byte("small-4")},
	}
	offsets, err := l.Append(msgs)
	require.NoError(t, err)
	l.SetHighWatermark(offsets[len(offsets)-1])

	r, err := l.NewReader(From(0), Follow())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	headers := make([]byte, 28)
	var payload []byte
	for i, want := range msgs {
		meta, newBuf, err := r.ReadMessageMetadata(ctx, headers, payload)
		payload = newBuf
		require.NoError(t, err, "message %d", i)
		require.Equal(t, int64(i), meta.Offset, "message %d", i)
		require.Equal(t, want.Value, meta.Raw.Value(), "message %d", i)
	}
}
