package commitlog

import (
	"io"
	"path/filepath"
	"strings"
	"sync"

	"github.com/ligustah/commitlog/compress"
	"testing"

	"github.com/stretchr/testify/require"
)

// countingSegmentStore counts the requests a store is asked to serve, split by
// kind. The whole point of streaming is the REQUEST count, so that is what the
// tests assert on — bytes were never the problem.
type countingSegmentStore struct {
	SegmentStore
	mu      sync.Mutex
	readAts int
	streams int
}

func (s *countingSegmentStore) ReadAt(key string, p []byte, off int64) (int, error) {
	s.mu.Lock()
	s.readAts++
	s.mu.Unlock()
	return s.SegmentStore.ReadAt(key, p, off)
}

func (s *countingSegmentStore) Stream(key string, off int64) (io.ReadCloser, error) {
	s.mu.Lock()
	s.streams++
	s.mu.Unlock()
	return s.SegmentStore.Stream(key, off)
}

func (s *countingSegmentStore) counts() (readAts, streams int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readAts, s.streams
}

// A sweep of an offloaded segment must cost ONE request, not one per record or
// one per read-ahead window. Asserted as a ratio against the record count
// rather than "fewer than before", because a windowed read also gets "fewer" —
// the claim here is that the number of requests stops scaling with the data at
// all.
func TestScanningAnOffloadedSegmentCostsOneRequest(t *testing.T) {
	dir := tempDir(t)
	inner, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)
	store := &countingSegmentStore{SegmentStore: inner}

	l, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  4096, // several segments, so sealed ones can offload
		Tiers:            oneTier(store),
		DisableAutoClean: true,
	})
	defer cleanup()

	const records = 400
	var last int64
	for i := 0; i < records; i++ {
		offs, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("a padded value")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)

	n, err := l.OffloadBefore(last + 1)
	require.NoError(t, err)
	require.Positive(t, n)

	l.mu.RLock()
	var seg *segment
	for _, s := range l.segments {
		if s.isOffloaded() {
			seg = s
			break
		}
	}
	l.mu.RUnlock()
	require.NotNil(t, seg)

	beforeReads, beforeStreams := store.counts()

	ss := newSegmentScannerCache(seg, newBlockCache())
	scanned := 0
	for _, _, err := ss.Scan(); err == nil; _, _, err = ss.Scan() {
		scanned++
	}
	require.NoError(t, ss.Close())
	require.Positive(t, scanned, "the fixture must have records to scan")

	afterReads, afterStreams := store.counts()
	require.Equal(t, 1, afterStreams-beforeStreams,
		"a sweep must open exactly one stream")
	require.Zero(t, afterReads-beforeReads,
		"and must not fall back to ranged reads while it is sequential")
}

// The fallback has to be real, not theoretical: a non-sequential read is served
// correctly, and does not corrupt the stream for the sequential reads around it.
func TestScanStreamFallsBackForNonSequentialReads(t *testing.T) {
	dir := tempDir(t)
	inner, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)
	store := &countingSegmentStore{SegmentStore: inner}

	const body = "0123456789abcdefghijklmnopqrstuvwxyz"
	require.NoError(t, store.Put("obj", strings.NewReader(body), int64(len(body))))

	backing := newStoreBackingSize(store, "obj", int64(len(body)))
	ss := newScanStream(backing)
	defer ss.Close()

	read := func(off int64, n int) string {
		p := make([]byte, n)
		got, err := ss.ReadAt(p, off)
		require.NoError(t, err)
		return string(p[:got])
	}

	require.Equal(t, "01234", read(0, 5))
	require.Equal(t, "56789", read(5, 5), "sequential reads continue the stream")

	_, streams := store.counts()
	require.Equal(t, 1, streams)

	// A jump backwards: served correctly, from a ranged read.
	require.Equal(t, "012", read(0, 3))
	readAts, streams := store.counts()
	require.Positive(t, readAts, "a jump must fall back to a ranged read")
	require.Equal(t, 1, streams, "and must not re-open the stream")

	// The stream is undisturbed: the sweep carries on where it left off.
	require.Equal(t, "abcde", read(10, 5),
		"a stepped-aside read must not move the stream")
}

// A store that cannot stream must not break a scan, and must not be asked once
// per record either — a fallback that re-fails on every read is its own
// problem.
func TestScanStreamSurvivesAStoreThatCannotStream(t *testing.T) {
	dir := tempDir(t)
	inner, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)
	store := &noStreamStore{SegmentStore: inner}

	const body = "0123456789abcdefghij"
	require.NoError(t, store.Put("obj", strings.NewReader(body), int64(len(body))))

	backing := newStoreBackingSize(store, "obj", int64(len(body)))
	ss := newScanStream(backing)
	defer ss.Close()

	for off := int64(0); off < 20; off += 5 {
		p := make([]byte, 5)
		n, err := ss.ReadAt(p, off)
		require.NoError(t, err)
		require.Equal(t, body[off:off+5], string(p[:n]))
	}
	require.Equal(t, 1, store.attempts,
		"a store that cannot stream must be asked once, not once per read")
}

type noStreamStore struct {
	SegmentStore
	attempts int
}

func (s *noStreamStore) Stream(string, int64) (io.ReadCloser, error) {
	s.attempts++
	return nil, io.ErrUnexpectedEOF
}

// Streaming must not change what a scan returns — the same records, in the same
// order, byte for byte. This is the assertion that matters most: everything
// else is about cost.
func TestStreamedScanReturnsTheSameRecords(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts func(*Options)
	}{
		{"raw", func(o *Options) {}},
		{"block compressed", func(o *Options) { o.Compression = compress.Zstd }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := tempDir(t)
			store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
			require.NoError(t, err)

			opts := Options{
				Path:             dir,
				MaxSegmentBytes:  4096,
				Tiers:            oneTier(store),
				DisableAutoClean: true,
			}
			tc.opts(&opts)
			l, cleanup := setupWithOptions(t, opts)
			defer cleanup()

			var last int64
			for i := 0; i < 200; i++ {
				offs, err := l.Append([]*Message{{
					Key:   []byte("k"),
					Value: []byte(strings.Repeat("v", 1+i%40)),
				}})
				require.NoError(t, err)
				last = offs[0]
			}
			l.SetHighWatermark(last)

			// Ground truth: scan while the bytes are still local.
			l.mu.RLock()
			seg := l.segments[0]
			l.mu.RUnlock()
			local := scanAll(t, seg)
			require.NotEmpty(t, local)

			n, err := l.OffloadBefore(last + 1)
			require.NoError(t, err)
			require.Positive(t, n)

			require.Equal(t, local, scanAll(t, seg),
				"an offloaded, streamed scan must return exactly what the local one did")
		})
	}
}

func scanAll(t *testing.T, seg *segment) []string {
	t.Helper()
	ss := newSegmentScannerCache(seg, newBlockCache())
	defer ss.Close()
	var out []string
	for ms, _, err := ss.Scan(); err == nil; ms, _, err = ss.Scan() {
		out = append(out, string(ms))
	}
	return out
}
