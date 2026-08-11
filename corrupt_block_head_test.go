package commitlog

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// A block segment whose FIRST header does not parse must refuse to open, not
// throw the file away.
//
// scanBlocks walks a chain of block headers, and a header it cannot resolve
// ends the walk rather than failing it — correct for a torn tail, because
// everything before the cut is intact and refusing would take every sealed
// segment down with the active one. But the walk then compares how far it got
// against the file size and hands the difference to discardTornTail, and when
// the FIRST header is the one that did not parse it got nowhere: the
// "difference" is the entire segment.
//
// So a single flipped byte in this header used to truncate the whole file, and
// the open still SUCCEEDED. The log came up empty, and the high-watermark
// checkpoint was then clamped down to match it — 3450 bytes and fifty records
// to nothing, durably, with a warning as the only trace. A replica that did
// that on restart would come back claiming it had recorded 0.
//
// The corruption is one byte on purpose: magic and version stay valid, so the
// segment is still classified as block-compressed and scanBlocks still runs.
// It is parseBlockHeader that refuses, on the codec field, at position 0.
func TestACorruptFirstBlockHeaderIsNotATornTail(t *testing.T) {
	dir := tempDir(t)
	opts := Options{
		Path:            dir,
		MaxSegmentBytes: 1 << 20, // one segment, so the head IS the log
		Compression:     compress.Zstd,
	}
	l, err := New(opts)
	require.NoError(t, err)
	cl := l.(*commitLog)

	const records = 50
	var last int64
	for i := 0; i < records; i++ {
		offs, aerr := cl.Append([]*Message{{
			Value:     []byte(fmt.Sprintf("value-%04d", i)),
			Timestamp: int64(i + 1),
		}})
		require.NoError(t, aerr)
		last = offs[0]
	}
	cl.SetHighWatermark(last)
	require.NoError(t, cl.SyncAll())
	base := cl.activeSegment().BaseOffset
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
	var hdr [blockHeaderLen]byte
	_, err = f.ReadAt(hdr[:], 0)
	require.NoError(t, err)
	require.EqualValues(t, blockMagic, hdr[0], "the fixture did not write a block segment")
	// Byte 2 is the codec. 0xFE is not one, and Valid() is what rejects it.
	_, err = f.WriteAt([]byte{0xFE}, 2)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	l2, err := New(opts)
	if err == nil {
		// Not require.Error first: leaving a log open would fail the cleanup on
		// Windows, where the mapping keeps the file busy.
		require.NoError(t, l2.Close())
	}
	require.Error(t, err,
		"a segment whose first block header does not parse opened anyway, and "+
			"reported itself empty")

	fi, err = os.Stat(logPath)
	require.NoError(t, err)
	require.Equal(t, sizeBefore, fi.Size(),
		"the refused open discarded the log — a header we failed to UNDERSTAND "+
			"is not a torn tail, and truncating to the point the walk reached "+
			"means truncating everything when it reached nothing")
}
