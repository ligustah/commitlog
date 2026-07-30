package commitlog

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A header buffer too small must be an ERROR, not a panic.
//
// Reported by durable_streams against v0.41.0, which grew the frame header from
// 28 to 32 bytes. 24 of their call sites across 18 files still allocated 28, and
// every one panicked inside storedHeaderCrc — indexing past the end of the
// caller's own slice.
//
// Two reasons that was worse than a mistake on their side. It crashed the host
// process, which is the thing this package spent a day removing; and it could not
// have been caught by the Read that follows, because Read fills whatever it is
// given, so a short buffer quietly consumes a partial header, desynchronises the
// stream, and fails somewhere unrelated.
//
// The size is checked against HeaderBufferLen and the error says so by name, so
// the remedy is copy-pasteable and survives the header changing again.
func TestShortHeaderBufferIsAnErrorNotAPanic(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t)})
	defer l.Close() // nolint: errcheck
	defer cleanup()

	offsets, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("v")}})
	require.NoError(t, err)
	l.SetHighWatermark(offsets[0])

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Exactly the shape that panicked: the previous header length.
	for _, size := range []int{0, 1, 28, msgSetHeaderLen - 1} {
		r, rerr := l.NewReader(From(0), Uncommitted())
		require.NoError(t, rerr)

		var panicked any
		var readErr error
		func() {
			defer func() { panicked = recover() }()
			_, _, _, _, readErr = r.ReadMessage(ctx, make([]byte, size))
		}()

		require.Nil(t, panicked, "a %d-byte header buffer panicked", size)
		require.Error(t, readErr, "a %d-byte header buffer must be refused", size)
		require.Contains(t, readErr.Error(), "HeaderBufferLen",
			"the error should name the constant, not just a number")
	}

	// And the documented size works.
	r, rerr := l.NewReader(From(0), Uncommitted())
	require.NoError(t, rerr)
	msg, _, _, _, err := r.ReadMessage(ctx, make([]byte, HeaderBufferLen))
	require.NoError(t, err)
	require.Equal(t, "v", string(msg.Value()))
}

// The same guard on the metadata path, which has its own entry point.
func TestShortHeaderBufferIsAnErrorOnTheMetadataPath(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t)})
	defer l.Close() // nolint: errcheck
	defer cleanup()

	offsets, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("v")}})
	require.NoError(t, err)
	l.SetHighWatermark(offsets[0])

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := l.NewReader(From(0), Uncommitted())
	require.NoError(t, err)

	var panicked any
	var readErr error
	func() {
		defer func() { panicked = recover() }()
		_, _, readErr = r.ReadMessageMetadata(ctx, make([]byte, 28), nil)
	}()
	require.Nil(t, panicked, "a 28-byte header buffer panicked on the metadata path")
	require.Error(t, readErr)
	require.Contains(t, readErr.Error(), "HeaderBufferLen")
}
