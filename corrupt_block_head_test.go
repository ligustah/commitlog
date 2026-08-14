package commitlog

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// A block segment with a header that does not parse must refuse to open, not
// throw away everything after it.
//
// scanBlocks walks a chain of block headers, and a header it cannot resolve
// ends the walk rather than failing it — correct for a torn tail, because
// everything before the cut is intact and refusing would take every sealed
// segment down with the active one. But the walk then hands the distance
// between where it stopped and the end of the file to discardTornTail, and that
// distance has no lower bound.
//
//   - At byte 0 the walk got nowhere, so the "tail" was the entire segment. One
//     flipped byte truncated the whole file and New returned nil; the log came
//     up empty and the watermark was clamped down to match — fifty records to
//     none, durably, with a WARN as the only trace.
//   - Mid-file it was every record past the flipped byte, acknowledged ones
//     included: twenty committed records, and again the open succeeded.
//
// The distinction that fixes both is tearing versus corruption. A partial write
// leaves a PREFIX — the bytes that landed are the bytes that were written — so a
// torn tail always runs out of file, and those cases (a header the file is too
// short to hold, a payload shorter than its header promises) still discard. A
// header that is entirely present and wrong is corruption, and dropping bytes we
// merely failed to understand is not recovery.
//
// The corruption is one byte on purpose: magic stays valid, so the segment is
// still classified as block-compressed and scanBlocks still runs. It is
// parseBlockHeader that refuses.
//
// The last case pokes the VERSION byte rather than the codec, and it is not a
// fourth flavour of the same thing. scanBlocks used to special-case
// ErrBlockFormat and return it bare, skipping the wrap that names the byte —
// so the one refusal that DOES carry a caller-facing sentinel was also the one
// that arrived without saying where. errors.Is sees through errors.Wrapf, so
// the arm protected nothing; it only deleted the offset. Every case here now
// asserts the position, which is what makes its removal falsifiable.
func TestACorruptBlockHeaderIsNotATornTail(t *testing.T) {
	// atByte picks which header to corrupt, given the segment's block table;
	// field and to say what to write into it.
	for _, tc := range []struct {
		name       string
		atByte     func(blocks []blockRef) int64
		field      int64 // offset within the header
		val        byte
		wantFormat bool // refused as ErrBlockFormat rather than as damage
	}{
		// Byte 2 of the header is the codec; 0xFE is not one, and Valid()
		// rejects it. Damage in one header, so no sentinel.
		{"first", func([]blockRef) int64 { return 0 }, 2, 0xFE, false},
		{"midSegment", func(b []blockRef) int64 { return b[len(b)/2].physStart }, 2, 0xFE, false},
		{"lastBlock", func(b []blockRef) int64 { return b[len(b)-1].physStart }, 2, 0xFE, false},
		// Byte 1 is the version. This is the arm that used to return early.
		{"versionMidSegment", func(b []blockRef) int64 { return b[len(b)/2].physStart }, 1, 0xFE, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			corruptBlockHeaderIsNotATornTail(t, tc.atByte, tc.field, tc.val, tc.wantFormat)
		})
	}
}

func corruptBlockHeaderIsNotATornTail(t *testing.T, atByte func([]blockRef) int64, field int64, val byte, wantFormat bool) {
	dir := tempDir(t)
	opts := Options{
		Path:            dir,
		MaxSegmentBytes: 1 << 20, // one segment, so nothing else absorbs the loss
		Compression:     compress.Zstd,
	}
	l, err := New(opts)
	require.NoError(t, err)
	cl := l.(*commitLog)

	// One Append per block, so there are distinct headers to choose between.
	const records = 40
	var last int64
	for i := 0; i < records; i++ {
		offs, aerr := cl.Append([]*Message{{
			Value:     []byte(fmt.Sprintf("value-%04d", i)),
			Timestamp: int64(i + 1),
		}})
		require.NoError(t, aerr)
		last = offs[0]
	}
	// Acknowledged, which is what makes losing them a durability bug rather
	// than an inconvenience.
	cl.SetHighWatermark(last)
	require.NoError(t, cl.SyncAll())

	seg := cl.activeSegment()
	require.Greater(t, len(seg.blocks), 4, "too few blocks to tell the cases apart")
	target := atByte(seg.blocks)
	base := seg.BaseOffset
	require.NoError(t, cl.Close())

	logPath := filepath.Join(dir, fmt.Sprintf(fileFormat, base, logFileSuffix))
	fi, err := os.Stat(logPath)
	require.NoError(t, err)
	sizeBefore := fi.Size()
	require.Positive(t, sizeBefore, "the fixture wrote no log bytes to lose")

	// Sealing writes a .blocks sidecar, and loadLocalBlockTable answers from it
	// without reading a single header — so with the sidecar in place scanBlocks,
	// the code under test, never runs and the corruption is never even seen.
	require.NoError(t, os.Remove(filepath.Join(dir, fmt.Sprintf(fileFormat, base, blocksSuffix))))

	f, err := os.OpenFile(logPath, os.O_RDWR, 0666)
	require.NoError(t, err)
	var magic [1]byte
	_, err = f.ReadAt(magic[:], target)
	require.NoError(t, err)
	require.EqualValues(t, blockMagic, magic[0], "byte %d is not a block header", target)
	_, err = f.WriteAt([]byte{val}, target+field)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	l2, err := New(opts)
	if err == nil {
		// Closed first: an open log keeps the file mapped, and the cleanup
		// would fail on Windows before the assertion below could report.
		require.NoError(t, l2.Close())
	}
	require.Error(t, err,
		"a segment with an unparseable block header opened anyway, having "+
			"discarded every record from that byte onward")

	// WHICH refusal, not merely that one happened. The fixture removes a
	// sidecar and rewrites a byte, so plenty of unrelated failures would
	// satisfy a bare require.Error while scanBlocks was never reached — and
	// the byte offset is the one fact that distinguishes "parseBlockHeader
	// refused the header we corrupted" from all of them.
	require.Contains(t, err.Error(), fmt.Sprintf("block header at byte %d", target),
		"the open was refused by something other than the header at byte %d, "+
			"or the refusal arrived without saying where — which is what a "+
			"corrupt header at an unknown offset leaves an operator holding",
		target)

	// A version this build does not write is a whole-store fact a caller acts
	// on before touching anything; the other four refusals are damage in one
	// header. Both directions, because widening the sentinel is the silent
	// half — see ErrBlockFormat's doc.
	if wantFormat {
		require.ErrorIs(t, err, ErrBlockFormat,
			"a version byte this build does not write must reach the caller as "+
				"ErrBlockFormat even though it is now wrapped with its position")
	} else {
		require.NotErrorIs(t, err, ErrBlockFormat)
	}

	fi, err = os.Stat(logPath)
	require.NoError(t, err)
	require.Equal(t, sizeBefore, fi.Size(),
		"the refused open still discarded the log: a header we failed to "+
			"UNDERSTAND is not a torn tail, and truncating to the point the "+
			"walk reached drops committed records when it reached anything "+
			"short of the end")
}
