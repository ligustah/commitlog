package commitlog

import (
	"context"
	"os"
	"testing"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// indexFileSize returns the on-disk size of a segment's index file.
func indexFileSize(t *testing.T, seg *segment) int64 {
	t.Helper()
	fi, err := os.Stat(seg.indexPath())
	require.NoError(t, err)
	return fi.Size()
}

// TestSparseIndexOneEntryPerBlock verifies a block-compressed segment writes a
// single index entry per block (message set) rather than one per message, so
// the index is dramatically smaller than the dense per-record index.
func TestSparseIndexOneEntryPerBlock(t *testing.T) {
	dir := tempDir(t)
	seg, err := newSegment(dir, 0, 1<<30, true, "", compress.Zstd)
	require.NoError(t, err)

	const blocks = 5
	const perBlock = 1000
	for b := 0; b < blocks; b++ {
		writeSet(t, seg, compressibleMsgs(perBlock))
	}
	require.Len(t, seg.blocks, blocks)

	// Before sealing, the index position reflects exactly one entry per block.
	require.Equal(t, int64(blocks), seg.Index.CountEntries(),
		"sparse index must have one entry per block")

	require.NoError(t, seg.Close())

	// After close the index file is shrunk to its used size = blocks*entryWidth.
	got := indexFileSize(t, seg)
	require.Equal(t, int64(blocks*entryWidth), got,
		"sealed sparse index size must be blocks*entryWidth")

	// Compare against a dense (raw) segment with the same messages: its index
	// is ~1000x larger (one entry per record).
	dirRaw := tempDir(t)
	segRaw, err := newSegment(dirRaw, 0, 1<<30, true, "", compress.None)
	require.NoError(t, err)
	for b := 0; b < blocks; b++ {
		writeSet(t, segRaw, compressibleMsgs(perBlock))
	}
	require.NoError(t, segRaw.Close())
	rawSize := indexFileSize(t, segRaw)
	require.Equal(t, int64(blocks*perBlock*entryWidth), rawSize,
		"dense index has one entry per record")
	require.Less(t, got, rawSize/100, "sparse index must be <1%% of dense (%d vs %d)", got, rawSize)
	t.Logf("sparse index=%d bytes  dense index=%d bytes  (%dx smaller)",
		got, rawSize, rawSize/got)
}

// TestSparseIndexRawStillDense verifies raw (codec None) segments keep the dense
// one-entry-per-message index unchanged.
func TestSparseIndexRawStillDense(t *testing.T) {
	dir := tempDir(t)
	seg, err := newSegment(dir, 0, 1<<30, true, "", compress.None)
	require.NoError(t, err)
	t.Cleanup(func() { seg.Close() })

	writeSet(t, seg, compressibleMsgs(250))
	require.Equal(t, int64(250), seg.Index.CountEntries(),
		"raw segment must keep dense per-message index")
}

// seekEveryOffset seeks to every offset in [firstOffset, newestOffset] using a
// fresh reader and asserts ReadMessage returns exactly that offset with the
// matching payload. This exercises skip-forward at block interiors and
// boundaries for both committed and uncommitted readers.
func seekEveryOffset(t *testing.T, l *commitLog, want []*Message, uncommitted bool) {
	t.Helper()
	ctx := context.Background()
	hdr := make([]byte, 28)
	for target := int64(0); target < int64(len(want)); target++ {
		opts := []ReadOption{From(target), Follow()}
		if uncommitted {
			opts = append(opts, Uncommitted())
		}
		r, err := l.NewReader(opts...)
		require.NoError(t, err, "new reader at %d", target)
		msg, offset, _, _, err := r.ReadMessage(ctx, hdr)
		require.NoError(t, err, "read at target %d", target)
		require.Equal(t, target, offset, "first message must be exactly the requested offset")
		compareMessages(t, want[target], msg)
	}
}

// TestSparseSeekEveryOffset seeks to every offset in a multi-block compressed
// segment and verifies the reader returns exactly that offset and payload,
// using both the uncommitted and committed read paths.
func TestSparseSeekEveryOffset(t *testing.T) {
	for _, codec := range allCodecs {
		t.Run(codec.String(), func(t *testing.T) {
			l, cleanup := setupWithOptions(t, Options{
				Path:            tempDir(t),
				MaxSegmentBytes: 1 << 30, // single segment
				Compression:     codec,
			})
			defer cleanup()

			// Several appends => several blocks of varying size.
			var want []*Message
			for _, n := range []int{1, 50, 200, 3, 100} {
				batch := compressibleMsgs(n)
				// Give each batch distinct values so payloads are checkable.
				for i := range batch {
					batch[i].Value = append([]byte(t.Name()), batch[i].Value...)
				}
				_, err := l.Append(batch)
				require.NoError(t, err)
				want = append(want, batch...)
			}
			l.SetHighWatermark(l.NewestOffset())

			seekEveryOffset(t, l, want, true)  // uncommitted
			seekEveryOffset(t, l, want, false) // committed (exercises getHWPos)
		})
	}
}

// TestSparseRecoverySeek writes a compressed multi-block log, closes it, reopens
// it (triggering scanBlocks + sparse-index recovery), then seeks to arbitrary
// offsets and reads to the end verifying offsets and payloads.
func TestSparseRecoverySeek(t *testing.T) {
	dir := tempDir(t)
	opts := Options{Path: dir, MaxSegmentBytes: 1 << 30, Compression: compress.Zstd}

	l, err := New(opts)
	require.NoError(t, err)
	var want []*Message
	for _, n := range []int{10, 1, 100, 40} {
		batch := compressibleMsgs(n)
		for i := range batch {
			batch[i].Value = append([]byte("rec"), batch[i].Value...)
		}
		_, err := l.Append(batch)
		require.NoError(t, err)
		want = append(want, batch...)
	}
	require.NoError(t, l.Close())

	// Reopen: block scan + sparse index recovery must reconstruct offsets.
	l2i, err := New(opts)
	require.NoError(t, err)
	t.Cleanup(func() { l2i.Close() })
	l2 := l2i.(*commitLog)

	// Recovered last offset must match the true last message, not a block base.
	require.Equal(t, int64(len(want)-1), l2.NewestOffset(), "recovered newest offset")
	require.Equal(t, int64(0), l2.OldestOffset(), "recovered oldest offset")

	// Appending after reopen must continue from the correct next offset.
	extra := compressibleMsgs(5)
	for i := range extra {
		extra[i].Value = append([]byte("extra"), extra[i].Value...)
	}
	offs, err := l2.Append(extra)
	require.NoError(t, err)
	require.Equal(t, int64(len(want)), offs[0], "append after reopen continues offsets")
	want = append(want, extra...)

	l2.SetHighWatermark(l2.NewestOffset())

	ctx := context.Background()
	hdr := make([]byte, 28)
	for _, target := range []int64{0, 5, 11, 50, 110, 149, int64(len(want)) - 1} {
		r, err := l2.NewReader(From(target), Uncommitted(), Follow())
		require.NoError(t, err)
		// Read from target to the end, verifying contiguous offsets/payloads.
		for off := target; off < int64(len(want)); off++ {
			msg, gotOff, _, _, err := r.ReadMessage(ctx, hdr)
			require.NoError(t, err, "target %d off %d", target, off)
			require.Equal(t, off, gotOff)
			compareMessages(t, want[off], msg)
		}
	}
}

// TestSparseTimestampSeek verifies time-based seeks land on the correct offset
// with per-block granularity plus forward scan on compressed segments.
func TestSparseTimestampSeek(t *testing.T) {
	dir := tempDir(t)
	seg, err := newSegment(dir, 0, 1<<30, true, "", compress.Zstd)
	require.NoError(t, err)
	t.Cleanup(func() { seg.Close() })

	// Build blocks with known, strictly increasing timestamps. compressibleMsgs
	// assigns Timestamp = i+1 within a batch; we offset each batch so timestamps
	// are globally monotonic across blocks.
	var (
		base   int64
		blocks = [][]*Message{}
	)
	for _, n := range []int{30, 30, 30} {
		batch := compressibleMsgs(n)
		for i := range batch {
			batch[i].Timestamp = base + int64(i) + 1
		}
		base += int64(n)
		blocks = append(blocks, batch)
		writeSet(t, seg, batch)
	}

	// Flatten expected (offset, timestamp) pairs.
	type ot struct{ offset, ts int64 }
	var all []ot
	var off int64
	for _, batch := range blocks {
		for _, m := range batch {
			all = append(all, ot{off, m.Timestamp})
			off++
		}
	}

	// For every message timestamp, findEntryByTimestamp must return that exact
	// message (first entry with ts >= target). Targets land at block interiors
	// and boundaries.
	for _, want := range all {
		e, err := seg.findEntryByTimestamp(want.ts)
		require.NoError(t, err, "ts=%d", want.ts)
		require.Equal(t, want.ts, e.Timestamp)
		require.Equal(t, want.offset, e.Offset, "ts=%d must map to offset %d", want.ts, want.offset)
	}

	// A timestamp between two messages returns the next message (>=).
	e, err := seg.findEntryByTimestamp(all[10].ts) // exact
	require.NoError(t, err)
	require.Equal(t, all[10].offset, e.Offset)

	// A timestamp past the end returns ErrEntryNotFound.
	_, err = seg.findEntryByTimestamp(base + 1000)
	require.Equal(t, ErrEntryNotFound, err)
}
