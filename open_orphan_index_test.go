package commitlog

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// An index with no log bytes anywhere is removed at open.
//
// It is the residue of a segment whose .log was collected while its .index was
// not — a retention pass that died between the two removals. Left in place it is
// not inert: open() names segments by the base offset in the file name, so the
// next segment to reach that base offset finds a populated index sitting under
// the name it is about to write, describing records that no longer exist.
func TestAnIndexWithNoLogAndNoManifestEntryIsRemovedAtOpen(t *testing.T) {
	dir := tempDir(t)

	l, err := New(Options{Path: dir, MaxSegmentBytes: 1 << 16})
	require.NoError(t, err)
	offs, err := l.Append([]*Message{{Value: []byte("v")}})
	require.NoError(t, err)
	l.SetHighWatermark(offs[0])
	require.NoError(t, l.Close())

	orphan := filepath.Join(dir, fmt.Sprintf(fileFormat, 9999, indexFileSuffix))
	require.NoError(t, os.WriteFile(orphan, make([]byte, 64), 0o644))

	reopened, err := New(Options{Path: dir, MaxSegmentBytes: 1 << 16})
	require.NoError(t, err)
	t.Cleanup(func() { reopened.Close() })

	require.NoFileExists(t, orphan,
		"an index with neither a .log beside it nor a manifest entry survived "+
			"open, under the name a future segment at that base offset will use")
}

// An offloaded segment's index has no .log beside it and must NOT be removed.
//
// This is the whole reason the orphan check consults the manifest rather than
// the directory alone. Offloading drops the local log and, when the index is not
// offloaded with it, deliberately keeps the local index — so "index with no log"
// is the NORMAL resting state of a tiered segment, not damage.
//
// The assertion is on BYTES PULLED FROM THE STORE, not on the file existing.
// The file exists either way: adoptTierManifestLocked opens every manifested
// segment after the sweep and calls reconcileIndexTail on the ones whose index
// stayed local, which recreates the file. What it cannot recreate for free is
// the content — with the index gone, "rebuild the missing tail" is a rebuild of
// the WHOLE index, which is a front-to-back pass over the segment, downloaded
// from the store, for every tiered segment, on every boot.
//
// That is the cost the manifest lookup prevents, and asserting on the file's
// existence would not have seen a byte of it. The first version of this test did
// exactly that and passed with the guard removed.
func TestAnOffloadedSegmentsIndexSurvivesOpen(t *testing.T) {
	dir := tempDir(t)
	fs, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)
	store := &byteCountingStore{FileSegmentStore: fs}

	opts := Options{
		Path:             dir,
		MaxSegmentBytes:  1 << 12,
		SegmentStore:     store,
		DisableAutoClean: true,
	}
	l, err := New(opts)
	require.NoError(t, err)

	var last int64
	for i := 0; i < 400; i++ {
		offs, err := l.Append([]*Message{{Value: []byte("padding value for a segment")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)

	n, err := l.(*commitLog).OffloadBefore(last)
	require.NoError(t, err)
	require.Positive(t, n, "the fixture needs an offloaded segment")

	// The offloaded segments are exactly the ones whose .log is gone. Take the
	// state from the directory rather than from the count, so the assertion below
	// names files that really are in this position.
	stranded := strandedIndexes(t, dir)
	require.NotEmpty(t, stranded,
		"offloading should have left at least one index with no log beside it")

	require.NoError(t, l.Close())

	store.reset()
	reopened, err := New(opts)
	require.NoError(t, err)
	t.Cleanup(func() { reopened.Close() })

	// Measured on this fixture: 2184 bytes with the manifest lookup in place —
	// the manifest object plus the headers adoption reads to open each segment —
	// against 31059 without it, which is every offloaded segment streamed back
	// end to end to rebuild an index that was already on disk. The bound sits
	// between them with room on both sides, so it is neither met by accident nor
	// broken by a fixture that grows a segment.
	read := store.bytesRead()
	require.Less(t, read, int64(8<<10),
		"open pulled %d bytes from the store for a log whose segment indexes "+
			"were already on disk — the orphan sweep collected them and adoption "+
			"rebuilt each index by streaming its segment back", read)

	// And the log still works, which is the other half: not reading is only
	// correct if the indexes that were kept are the right ones.
	require.Equal(t, last, reopened.NewestOffset())
	for _, p := range stranded {
		require.FileExists(t, p, "an offloaded segment's index went missing")
	}
}

// byteCountingStore counts bytes served out of the store, so a test can assert
// that an operation did not fall back to streaming segments it should not need.
type byteCountingStore struct {
	*FileSegmentStore
	mu    sync.Mutex
	bytes int64
}

func (s *byteCountingStore) add(n int64) {
	s.mu.Lock()
	s.bytes += n
	s.mu.Unlock()
}

func (s *byteCountingStore) reset() {
	s.mu.Lock()
	s.bytes = 0
	s.mu.Unlock()
}

func (s *byteCountingStore) bytesRead() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bytes
}

func (s *byteCountingStore) ReadAt(key string, p []byte, off int64) (int, error) {
	n, err := s.FileSegmentStore.ReadAt(key, p, off)
	s.add(int64(n))
	return n, err
}

func (s *byteCountingStore) Stream(key string, off int64) (io.ReadCloser, error) {
	rc, err := s.FileSegmentStore.Stream(key, off)
	if err != nil {
		return nil, err
	}
	return &countingReadCloser{ReadCloser: rc, store: s}, nil
}

type countingReadCloser struct {
	io.ReadCloser
	store *byteCountingStore
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	c.store.add(int64(n))
	return n, err
}

// strandedIndexes returns the .index files in dir with no .log beside them.
func strandedIndexes(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	logs := make(map[string]bool, len(entries))
	for _, e := range entries {
		if n := e.Name(); filepath.Ext(n) == logFileSuffix {
			logs[n[:len(n)-len(logFileSuffix)]] = true
		}
	}
	var out []string
	for _, e := range entries {
		n := e.Name()
		if filepath.Ext(n) != indexFileSuffix {
			continue
		}
		if stem := n[:len(n)-len(indexFileSuffix)]; !logs[stem] {
			out = append(out, filepath.Join(dir, n))
		}
	}
	return out
}
