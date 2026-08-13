package commitlog

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// wrappedErrStore is a FileSegmentStore that returns its errors WRAPPED, the way
// a store built on a cloud SDK does — `fmt.Errorf("get %q: %w", key, err)`.
//
// It EMBEDS the real store rather than reimplementing it, so everything except
// the one thing under test behaves exactly as it does in production. No code
// asserts on *FileSegmentStore today, and embedding keeps that a safe assumption
// instead of a thing this double quietly depends on.
type wrappedErrStore struct{ *FileSegmentStore }

func (s wrappedErrStore) ReadAt(key string, p []byte, off int64) (int, error) {
	n, err := s.FileSegmentStore.ReadAt(key, p, off)
	if err != nil {
		return n, fmt.Errorf("objectstore: get %q at %d: %w", key, off, err)
	}
	return n, nil
}

// A store may wrap the io.EOF it returns, because SegmentStore is implemented by
// CALLERS and wrapping is what Go code does with errors.
//
// The interface says so about the sibling sentinel on the very same method:
// ErrObjectNotFound "must be returned (possibly wrapped, so callers use
// errors.Is)", and commitlog honours that at copy_tier.go, descriptor.go and
// manifest.go. io.EOF had no such statement and was consumed with `==` in the
// two places that read a caller's store — so one sentinel off one method
// survived wrapping and the other did not, with nothing saying which was which.
//
// What that costs is on the SHORT-READ path, and only there: refill clamps its
// request to the recorded object size, so a store asked for exactly what exists
// returns no io.EOF at all. It matters when the object is smaller than the size
// commitlog recorded — a stale size, an object still being written — which is
// precisely the case the `nread > 0` arm exists to handle. Wrapped, that arm was
// skipped: a buffer holding VALID bytes was thrown away and a hard error
// returned in place of the data.
//
// The fixture is that state directly: an object of len(body) bytes behind a
// backing told it is one byte longer.
func TestAStoreMayWrapTheIoEOFItReturns(t *testing.T) {
	fs, err := NewFileSegmentStore(tempDir(t))
	require.NoError(t, err)

	const key = "object"
	body := []byte("0123456789abcdef")
	require.NoError(t, fs.Put(key, bytes.NewReader(body), int64(len(body))))

	// One byte longer than the object, so the prefetch asks for more than is
	// there and the store answers with a short read and io.EOF.
	b := newStoreBackingSize(wrappedErrStore{fs}, key, int64(len(body))+1)

	p := make([]byte, len(body))
	n, err := b.ReadAt(p, 0)
	require.NoError(t, err,
		"a short read the store reported as a WRAPPED io.EOF was treated as a failure; "+
			"the bytes it did return were discarded")
	require.Equal(t, len(body), n)
	require.Equal(t, body, p, "the bytes served must be the object's own")
}

// The same fixture with a BARE io.EOF, which is what FileSegmentStore returns.
//
// Pinned separately so the fix above cannot be "implemented" by accepting any
// error on the short-read path: this is the case that already worked, and it has
// to keep working for the same reason and not by falling through a wider arm.
func TestAShortReadWithABareIoEOFStillServesItsBytes(t *testing.T) {
	fs, err := NewFileSegmentStore(tempDir(t))
	require.NoError(t, err)

	const key = "object"
	body := []byte("0123456789abcdef")
	require.NoError(t, fs.Put(key, bytes.NewReader(body), int64(len(body))))

	b := newStoreBackingSize(fs, key, int64(len(body))+1)

	p := make([]byte, len(body))
	n, err := b.ReadAt(p, 0)
	require.NoError(t, err)
	require.Equal(t, len(body), n)
	require.Equal(t, body, p)
}

// And a wrapped error that is NOT io.EOF is still a failure. Without this, the
// two tests above are satisfied by a refill that ignores every error its store
// reports, which would turn a genuine outage into silently truncated data — the
// exact trade this package refuses everywhere else.
func TestAWrappedNonEOFStoreErrorIsStillAFailure(t *testing.T) {
	fs, err := NewFileSegmentStore(tempDir(t))
	require.NoError(t, err)

	const key = "object"
	body := []byte("0123456789abcdef")
	require.NoError(t, fs.Put(key, bytes.NewReader(body), int64(len(body))))

	b := newStoreBackingSize(failingStore{FileSegmentStore: fs}, key, int64(len(body)))

	p := make([]byte, len(body))
	_, err = b.ReadAt(p, 0)
	require.Error(t, err, "a store that could not serve the read reported success")
	require.False(t, errors.Is(err, io.EOF),
		"the failure must not be laundered into an end-of-object")
}

// failingStore answers every read with a wrapped outage.
type failingStore struct{ *FileSegmentStore }

var errStoreOutage = errors.New("objectstore: upstream unavailable")

func (s failingStore) ReadAt(key string, p []byte, off int64) (int, error) {
	return 0, fmt.Errorf("objectstore: get %q at %d: %w", key, off, errStoreOutage)
}
