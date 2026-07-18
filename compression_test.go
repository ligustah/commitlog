package commitlog

import (
	"bytes"
	"context"
	"crypto/rand"
	"strconv"
	"testing"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// allCodecs is every codec the segment layer can be configured with.
var allCodecs = []compress.Codec{compress.None, compress.Snappy, compress.S2, compress.Zstd}

// compressibleMsgs builds n messages that share a large, repeated prefix (the
// shape of gocdc view records, whose embedded schema repeats every record), so
// a batch of them compresses dramatically.
func compressibleMsgs(n int) []*Message {
	schema := []byte(`{"operation":"insert","relation":"catalog","schema":[{"key":true,"name":"product_id"},{"name":"product"},{"name":"price_cents"},{"name":"seller"}],"after":`)
	msgs := make([]*Message, n)
	for i := 0; i < n; i++ {
		v := append(append([]byte{}, schema...), []byte(strconv.Itoa(i)+`}`)...)
		msgs[i] = &Message{Value: v, Timestamp: int64(i + 1), LeaderEpoch: 1}
	}
	return msgs
}

// incompressibleMsg builds a single message with a large random payload. With
// minimal framing redundancy the whole message set is incompressible, forcing
// the block's raw-storage fallback.
func incompressibleMsg(t *testing.T) []*Message {
	v := make([]byte, 8192)
	_, err := rand.Read(v)
	require.NoError(t, err)
	return []*Message{{Value: v, Timestamp: 1, LeaderEpoch: 1}}
}

// writeSet appends msgs as one message set (one block) to seg and returns the
// raw message-set bytes, i.e. the logical bytes the block must decode back to.
func writeSet(t *testing.T, seg *segment, msgs []*Message) []byte {
	ms, entries, err := newMessageSetFromProto(seg.NextOffset(), seg.Position(), msgs, false)
	require.NoError(t, err)
	require.NoError(t, seg.WriteMessageSet(ms, entries))
	return ms
}

// readAllLogical reads the whole logical byte space of seg using chunk-sized
// ReadAt calls, exercising cross-block and partial/EOF reads.
func readAllLogical(t *testing.T, seg *segment, chunk int) []byte {
	var out []byte
	buf := make([]byte, chunk)
	var off int64
	for {
		n, err := seg.ReadAt(buf, off)
		out = append(out, buf[:n]...)
		off += int64(n)
		if err != nil {
			// io.EOF (or short read) ends the scan.
			break
		}
	}
	return out
}

// TestSegmentBlockTransparency is the core invariant: for any codec, reading the
// segment's logical byte space (in any chunk size, across block boundaries)
// yields exactly the concatenation of the message sets that were written.
func TestSegmentBlockTransparency(t *testing.T) {
	for _, codec := range allCodecs {
		for _, chunk := range []int{1, 7, 64, 1000, 65536} {
			t.Run(codec.String()+"/chunk="+strconv.Itoa(chunk), func(t *testing.T) {
				dir := tempDir(t)
				seg, err := newSegment(dir, 0, 1<<30, true, "", codec)
				require.NoError(t, err)
				t.Cleanup(func() { seg.Close() })

				var want []byte
				// Several sets of varying size => several blocks.
				for _, n := range []int{1, 50, 3, 200, 1} {
					want = append(want, writeSet(t, seg, compressibleMsgs(n))...)
				}
				require.Equal(t, int64(len(want)), seg.Position(),
					"logical position must equal total message-set bytes")

				got := readAllLogical(t, seg, chunk)
				require.True(t, bytes.Equal(want, got),
					"read-back mismatch (%d want vs %d got)", len(want), len(got))
			})
		}
	}
}

// TestSegmentBlockRecovery verifies scanBlocks reconstructs the block index and
// logical position after reopening a compressed segment from disk.
func TestSegmentBlockRecovery(t *testing.T) {
	for _, codec := range allCodecs {
		t.Run(codec.String(), func(t *testing.T) {
			dir := tempDir(t)
			seg, err := newSegment(dir, 0, 1<<30, true, "", codec)
			require.NoError(t, err)

			var want []byte
			for _, n := range []int{10, 100, 5} {
				want = append(want, writeSet(t, seg, compressibleMsgs(n))...)
			}
			wantPos := seg.Position()
			require.NoError(t, seg.Close())

			// Reopen (isNew=false) — triggers format detection + scanBlocks.
			seg2, err := newSegment(dir, 0, 1<<30, false, "", codec)
			require.NoError(t, err)
			t.Cleanup(func() { seg2.Close() })

			require.Equal(t, wantPos, seg2.Position(), "recovered logical position")
			require.Equal(t, codec != compress.None, seg2.blockMode, "recovered block mode")
			got := readAllLogical(t, seg2, 4096)
			require.True(t, bytes.Equal(want, got), "recovered read-back mismatch")
		})
	}
}

// TestSegmentIncompressibleStoredRaw verifies a batch that does not compress is
// stored raw (codec None) so incompressible data is never inflated, yet still
// roundtrips.
func TestSegmentIncompressibleStoredRaw(t *testing.T) {
	dir := tempDir(t)
	seg, err := newSegment(dir, 0, 1<<30, true, "", compress.Zstd)
	require.NoError(t, err)
	t.Cleanup(func() { seg.Close() })

	want := writeSet(t, seg, incompressibleMsg(t))
	require.Len(t, seg.blocks, 1)
	require.Equal(t, compress.None, seg.blocks[0].codec,
		"incompressible block must fall back to raw storage")
	// Physical block is the payload plus only the 10-byte header.
	require.Equal(t, int64(len(want))+blockHeaderLen, seg.blocks[0].physLen)
	// Guarantee: a block never inflates past its logical size + header.
	require.LessOrEqual(t, seg.blocks[0].payloadLen(), seg.blocks[0].logicalLen)

	got := readAllLogical(t, seg, 512)
	require.True(t, bytes.Equal(want, got))
}

// TestSegmentMixedBlocks verifies a segment that interleaves compressible and
// incompressible blocks reads back correctly (per-block codec).
func TestSegmentMixedBlocks(t *testing.T) {
	dir := tempDir(t)
	seg, err := newSegment(dir, 0, 1<<30, true, "", compress.Zstd)
	require.NoError(t, err)
	t.Cleanup(func() { seg.Close() })

	var want []byte
	want = append(want, writeSet(t, seg, compressibleMsgs(100))...)
	want = append(want, writeSet(t, seg, incompressibleMsg(t))...)
	want = append(want, writeSet(t, seg, compressibleMsgs(50))...)

	require.Len(t, seg.blocks, 3)
	require.Equal(t, compress.Zstd, seg.blocks[0].codec)
	require.Equal(t, compress.None, seg.blocks[1].codec)
	require.Equal(t, compress.Zstd, seg.blocks[2].codec)

	got := readAllLogical(t, seg, 333)
	require.True(t, bytes.Equal(want, got))
}

// TestSegmentReadAtOffsets verifies reads at arbitrary logical offsets (mid-block
// and spanning blocks) return the right slice, matching a raw-mode segment.
func TestSegmentReadAtOffsets(t *testing.T) {
	dir := tempDir(t)
	seg, err := newSegment(dir, 0, 1<<30, true, "", compress.Zstd)
	require.NoError(t, err)
	t.Cleanup(func() { seg.Close() })

	var want []byte
	for _, n := range []int{30, 30, 30} {
		want = append(want, writeSet(t, seg, compressibleMsgs(n))...)
	}
	total := len(want)

	for _, tc := range []struct{ off, size int }{
		{0, 1}, {5, 10}, {total / 2, 100}, {total - 1, 1}, {total - 50, 200},
	} {
		buf := make([]byte, tc.size)
		n, _ := seg.ReadAt(buf, int64(tc.off))
		end := tc.off + tc.size
		if end > total {
			end = total
		}
		require.True(t, bytes.Equal(want[tc.off:end], buf[:n]),
			"ReadAt(off=%d size=%d)", tc.off, tc.size)
	}

	// Reading at or past the logical end is io.EOF.
	buf := make([]byte, 4)
	n, err := seg.ReadAt(buf, int64(total))
	require.Equal(t, 0, n)
	require.Error(t, err)
}

// TestCommitLogCompressionE2E runs the full log (Append + reader) with each
// codec across multiple segment sizes, verifying messages roundtrip and offsets
// are preserved.
func TestCommitLogCompressionE2E(t *testing.T) {
	for _, codec := range allCodecs {
		for _, segSize := range []int64{64, 4096, 1 << 20} {
			t.Run(codec.String()+"/seg="+strconv.FormatInt(segSize, 10), func(t *testing.T) {
				l, cleanup := setupWithOptions(t, Options{
					Path:            tempDir(t),
					MaxSegmentBytes: segSize,
					Compression:     codec,
				})
				defer cleanup()

				want := compressibleMsgs(500)
				_, err := l.Append(want)
				require.NoError(t, err)

				ctx := context.Background()
				r, err := l.NewReader(0, true)
				require.NoError(t, err)
				hdr := make([]byte, 28)
				for i, exp := range want {
					msg, offset, _, _, err := r.ReadMessage(ctx, hdr)
					require.NoError(t, err, "msg %d", i)
					require.Equal(t, int64(i), offset)
					compareMessages(t, exp, msg)
				}
			})
		}
	}
}

// TestCommitLogCompressionRecover verifies a compressed log survives close +
// reopen (segment scan on startup) with all messages intact.
func TestCommitLogCompressionRecover(t *testing.T) {
	dir := tempDir(t)
	opts := Options{Path: dir, MaxSegmentBytes: 8192, Compression: compress.Zstd}

	l, err := New(opts)
	require.NoError(t, err)
	want := compressibleMsgs(300)
	_, err = l.Append(want)
	require.NoError(t, err)
	require.NoError(t, l.Close())

	// Reopen with the same options.
	l2, err := New(opts)
	require.NoError(t, err)
	t.Cleanup(func() { l2.Close() })

	ctx := context.Background()
	r, err := l2.NewReader(0, true)
	require.NoError(t, err)
	hdr := make([]byte, 28)
	for i, exp := range want {
		msg, offset, _, _, err := r.ReadMessage(ctx, hdr)
		require.NoError(t, err, "msg %d", i)
		require.Equal(t, int64(i), offset)
		compareMessages(t, exp, msg)
	}
}

// TestCommitLogCompressionBackwardCompat verifies that a log written without
// compression stays readable after reopening with a codec configured (legacy
// raw segments are detected and kept raw; new segments compress), and that all
// messages — old and new — read back correctly.
func TestCommitLogCompressionBackwardCompat(t *testing.T) {
	dir := tempDir(t)

	// Phase 1: write raw (no compression), small segments so several roll.
	l, err := New(Options{Path: dir, MaxSegmentBytes: 512})
	require.NoError(t, err)
	first := compressibleMsgs(100)
	_, err = l.Append(first)
	require.NoError(t, err)
	require.NoError(t, l.Close())

	// Phase 2: reopen with zstd and append more.
	l2, err := New(Options{Path: dir, MaxSegmentBytes: 512, Compression: compress.Zstd})
	require.NoError(t, err)
	t.Cleanup(func() { l2.Close() })
	second := compressibleMsgs(100)
	// Give the new messages distinct values so we can tell them apart.
	for i := range second {
		second[i].Value = append([]byte("second-"), second[i].Value...)
	}
	_, err = l2.Append(second)
	require.NoError(t, err)

	all := append(append([]*Message{}, first...), second...)
	ctx := context.Background()
	r, err := l2.NewReader(0, true)
	require.NoError(t, err)
	hdr := make([]byte, 28)
	for i, exp := range all {
		msg, offset, _, _, err := r.ReadMessage(ctx, hdr)
		require.NoError(t, err, "msg %d", i)
		require.Equal(t, int64(i), offset)
		compareMessages(t, exp, msg)
	}
}

// TestCompressionSavesSpace is a sanity check that compression actually shrinks
// the on-disk footprint for compressible data (and that logical position stays
// the uncompressed size).
func TestCompressionSavesSpace(t *testing.T) {
	msgs := compressibleMsgs(2000)

	dirRaw := tempDir(t)
	segRaw, err := newSegment(dirRaw, 0, 1<<30, true, "", compress.None)
	require.NoError(t, err)
	t.Cleanup(func() { segRaw.Close() })
	writeSet(t, segRaw, msgs)

	dirZ := tempDir(t)
	segZ, err := newSegment(dirZ, 0, 1<<30, true, "", compress.Zstd)
	require.NoError(t, err)
	t.Cleanup(func() { segZ.Close() })
	writeSet(t, segZ, msgs)

	// A raw-mode (None) segment doesn't track physPosition; its on-disk size
	// equals its logical Position().
	rawBytes := segRaw.Position()
	require.Equal(t, rawBytes, segZ.Position(), "logical size identical")
	require.Less(t, segZ.physPosition, rawBytes/4,
		"zstd should shrink compressible data by >4x (raw=%d zstd=%d)",
		rawBytes, segZ.physPosition)
	t.Logf("raw=%d bytes  zstd=%d bytes  ratio=%.1fx",
		rawBytes, segZ.physPosition, float64(rawBytes)/float64(segZ.physPosition))
}

// --- block header + cache unit tests ---

func TestBlockHeaderRoundtrip(t *testing.T) {
	for _, codec := range allCodecs {
		hdr := encodeBlockHeader(codec, 12345, 6789)
		require.Len(t, hdr, blockHeaderLen)
		gotCodec, u, c, err := parseBlockHeader(hdr)
		require.NoError(t, err)
		require.Equal(t, codec, gotCodec)
		require.Equal(t, uint32(12345), u)
		require.Equal(t, uint32(6789), c)
	}
}

func TestBlockHeaderErrors(t *testing.T) {
	_, _, _, err := parseBlockHeader(make([]byte, blockHeaderLen-1))
	require.Error(t, err, "short header")

	bad := encodeBlockHeader(compress.Zstd, 1, 1)
	bad[0] = 0x00
	_, _, _, err = parseBlockHeader(bad)
	require.Error(t, err, "bad magic")

	unknown := encodeBlockHeader(compress.Zstd, 1, 1)
	unknown[1] = 0xFF
	_, _, _, err = parseBlockHeader(unknown)
	require.Error(t, err, "unknown codec")
}

// TestCompressionCompaction exercises the compaction rewrite path (Cleaned +
// Replace go through newSegment/WriteMessageSet and must produce a valid
// compressed segment) and verifies the compacted, compressed log reads back the
// expected surviving records.
func TestCompressionCompaction(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 120,
		Compact:         true,
		Compression:     compress.Zstd,
	})
	defer cleanup()

	entries := []keyValue{
		{[]byte("foo"), []byte("first")},
		{[]byte("bar"), []byte("first")},
		{[]byte("foo"), []byte("second")},
		{[]byte("foo"), []byte("third")},
		{[]byte("bar"), []byte("second")},
		{[]byte("baz"), []byte("first")},
		{[]byte("baz"), []byte("second")},
		{[]byte("qux"), []byte("first")},
		{[]byte("foo"), []byte("fourth")},
		{[]byte("baz"), []byte("third")},
	}
	appendToLog(t, l, entries, true)
	require.NoError(t, l.Clean())

	expected := []*expectedMsg{
		{Offset: 4, Msg: &Message{Key: []byte("bar"), Value: []byte("second")}},
		{Offset: 7, Msg: &Message{Key: []byte("qux"), Value: []byte("first")}},
		{Offset: 8, Msg: &Message{Key: []byte("foo"), Value: []byte("fourth")}},
		{Offset: 9, Msg: &Message{Key: []byte("baz"), Value: []byte("third")}},
	}
	ctx := context.Background()
	r, err := l.NewReader(0, true)
	require.NoError(t, err)
	hdr := make([]byte, 28)
	for _, exp := range expected {
		msg, offset, _, _, err := r.ReadMessage(ctx, hdr)
		require.NoError(t, err)
		require.Equal(t, exp.Offset, offset)
		compareMessages(t, exp.Msg, msg)
	}
}

// TestCompressionTruncateBefore exercises the boundary-rewrite path (Trimmed +
// Finalize) under compression and verifies durability across reopen.
func TestCompressionTruncateBefore(t *testing.T) {
	dir := tempDir(t)
	opts := Options{Path: dir, MaxSegmentBytes: 120, Compression: compress.Zstd}

	l, err := New(opts)
	require.NoError(t, err)
	offsets := appendMsgs(t, l, 12)
	mid := offsets[5]
	require.NoError(t, l.TruncateBefore(mid))

	got := readAll(t, l)
	for _, off := range got {
		require.GreaterOrEqual(t, off, mid)
	}
	require.Contains(t, got, offsets[11], "newest record must survive")
	require.NoError(t, l.Close())

	// Reopen — trimmed compressed segments must be scanned back correctly.
	l2, err := New(opts)
	require.NoError(t, err)
	t.Cleanup(func() { l2.Close() })
	require.GreaterOrEqual(t, l2.OldestOffset(), mid)
	for _, off := range readAll(t, l2) {
		require.GreaterOrEqual(t, off, mid)
	}
}

func TestBlockCache(t *testing.T) {
	// The cache's buffers are private (blockCopyInto hands out copies made
	// under its lock); only the displacement/reset bookkeeping is visible.
	c := newBlockCache()
	require.EqualValues(t, -1, c.start)
	c.start, c.data = 100, []byte("hello")
	c.reset()
	require.EqualValues(t, -1, c.start)
}
