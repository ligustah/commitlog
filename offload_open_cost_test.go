package commitlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// countingLogStore counts reads of LOG objects — the segment bytes themselves,
// which is what opening a log must not touch.
type countingLogStore struct {
	*FileSegmentStore
	logReads atomic.Int64
	logBytes atomic.Int64
}

func (s *countingLogStore) ReadAt(key string, p []byte, off int64) (int, error) {
	if strings.HasSuffix(key, logSuffix) {
		s.logReads.Add(1)
		s.logBytes.Add(int64(len(p)))
	}
	return s.FileSegmentStore.ReadAt(key, p, off)
}

// Opening a log reads none of its offloaded segments' bytes.
//
// It used to read all of them. openOffloadedSegment called initPositions, which
// derives blockMode, position and physPosition from the object: a stat, a
// one-byte read of the format magic — a 1MiB prefetch through the store backing,
// for one byte — and, for a block-compressed segment, a walk of the whole header
// chain, which is the entire object. Measured before this changed: 22,136,648
// bytes for a 22-segment snappy tier and 5,242,880 for a 5-segment raw one,
// downloaded on an ordinary reopen before serving a single read.
//
// None of it was needed. The manifest already records all three fields for every
// entry, and attachOffloadedLocked — the path a segment takes when it offloads
// inside this process — has always taken them from there and kept the block
// table it already had. Only the segment that came back from a manifest went and
// re-derived what the manifest had just told it.
//
// The block table is the one thing the manifest does not carry, so it is built
// on the first read that needs it instead of on open. Both boot paths are
// covered here: reopening the directory the log offloaded from, and adopting the
// tier into a fresh directory.
func TestOpeningAnOffloadedTierReadsNoLogObjects(t *testing.T) {
	for _, tc := range []struct {
		name    string
		codec   compress.Codec
		records int
	}{
		{"raw", compress.None, 20000},
		{"block", compress.Snappy, 90000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := tempDir(t)
			fs, err := NewFileSegmentStore(filepath.Join(root, "store"))
			require.NoError(t, err)
			store := &countingLogStore{FileSegmentStore: fs}
			cache, err := NewRemoteIndexCache(filepath.Join(root, "idxcache"), 1<<30)
			require.NoError(t, err)
			defer cache.Close()

			opts := func(path string, adopt bool) Options {
				return Options{
					Name: "cost", Path: path, MaxSegmentBytes: 1 << 20,
					Compression: tc.codec, SegmentStore: store,
					RemoteIndexCache: cache, AdoptOptions: adopt,
				}
			}

			dir := filepath.Join(root, "log")
			l, err := New(opts(dir, false))
			require.NoError(t, err)
			// Batched so block-mode segments consolidate into real blocks rather
			// than one per append.
			for i := 0; i < tc.records; i += 200 {
				batch := make([]*Message, 0, 200)
				for j := i; j < i+200 && j < tc.records; j++ {
					batch = append(batch, &Message{
						Key:   []byte(fmt.Sprintf("k:%05d", j)),
						Value: []byte(fmt.Sprintf("v:%08d:%s", j, strings.Repeat("x", 40))),
					})
				}
				_, aerr := l.Append(batch)
				require.NoError(t, aerr)
			}
			offloaded, err := l.OffloadBefore(l.NewestOffset())
			require.NoError(t, err)
			require.Greater(t, offloaded, 0, "nothing was offloaded, so this proves nothing")
			require.NoError(t, l.Close())

			// The ordinary case: reopen the directory the log offloaded from.
			store.logReads.Store(0)
			store.logBytes.Store(0)
			same, err := New(opts(dir, false))
			require.NoError(t, err)
			require.Zerof(t, store.logBytes.Load(),
				"reopening read %d bytes of segment objects across %d reads, before "+
					"serving anything", store.logBytes.Load(), store.logReads.Load())
			require.NoError(t, same.Close())

			// The adoption case: a fresh directory, the tier taken from the store.
			fresh := filepath.Join(root, "adopt")
			require.NoError(t, os.MkdirAll(fresh, 0o755))
			store.logReads.Store(0)
			store.logBytes.Store(0)
			adopted, err := New(opts(fresh, true))
			require.NoError(t, err)
			defer adopted.Close()
			require.Zerof(t, store.logBytes.Load(),
				"adopting read %d bytes of segment objects across %d reads, before "+
					"serving anything", store.logBytes.Load(), store.logReads.Load())

			// The work moved, it did not vanish — and the segment still reads.
			// Without this the zero above would be satisfied just as well by a
			// tier that never opened at all.
			oldest := adopted.OldestOffset()
			require.GreaterOrEqual(t, oldest, int64(0), "the adopted tier is empty")
			r, err := adopted.NewReader(From(oldest), Uncommitted())
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			msg, off, _, _, err := r.ReadMessage(ctx, make([]byte, HeaderBufferLen))
			require.NoError(t, err)
			require.Equal(t, oldest, off)
			require.Equal(t, fmt.Sprintf("k:%05d", oldest), string(msg.Key()))
			require.Greaterf(t, store.logBytes.Load(), int64(0),
				"the first read fetched nothing, so the open must have fetched "+
					"it after all")

			// The TIMESTAMP lookup reaches the block table by its own route —
			// findEntryByTimestamp, not findEntry — and both take the segment
			// read lock before scanning, so neither can build the table itself.
			// Missing one of the two was a real bug while this was written, and
			// it surfaced as "entry not found" rather than as anything about
			// blocks. A fresh directory, so the table is genuinely unbuilt.
			byTime := filepath.Join(root, "adopt-bytime")
			require.NoError(t, os.MkdirAll(byTime, 0o755))
			cold, err := New(opts(byTime, true))
			require.NoError(t, err)
			defer cold.Close()
			at, err := cold.EarliestOffsetAfterTimestamp(1)
			require.NoError(t, err, "timestamp lookup on an unbuilt block table")
			require.Equal(t, cold.OldestOffset(), at)
		})
	}
}
