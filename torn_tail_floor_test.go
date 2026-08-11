package commitlog

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// Recovery may drop a torn tail. It may not drop records the log has already
// acknowledged.
//
// reconcileIndexTailRaw walks the frames the index does not cover and, if the
// walk cannot resolve one, hands everything from there to the end of the file
// to discardTornTail. Above the high watermark that is exactly right — those
// bytes are uncommitted and dropping them is the point. The walk had no floor
// though, and the damage scaled with how early the bad frame sat: a FIRST frame
// whose size field overruns the file resolves nothing, so the discard started
// at 0 and took the whole segment.
//
// The open then succeeded. The watermark, being higher than the now-empty log,
// was clamped down to match it, and the loss was durable before anything could
// notice — the only trace a WARN. A replica doing this on restart comes back
// claiming it holds no records at all, and its leader then truncates to agree.
//
// This is the same defect as the block walk's (see
// TestACorruptFirstBlockHeaderIsNotATornTail) reached through the raw path,
// which needs no compression configured and is therefore the default shape.
func TestATornTailIsNotDiscardedBelowTheWatermark(t *testing.T) {
	dir := tempDir(t)
	opts := Options{
		Path:            dir,
		MaxSegmentBytes: 1 << 20, // one segment, so the first frame IS the log's
		Compression:     compress.None,
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
	// Acknowledged: this is what makes discarding them illegal rather than
	// merely unfortunate.
	cl.SetHighWatermark(last)
	require.NoError(t, cl.SyncAll())
	base := cl.activeSegment().BaseOffset
	require.NoError(t, cl.Close())

	logPath := filepath.Join(dir, fmt.Sprintf(fileFormat, base, logFileSuffix))
	fi, err := os.Stat(logPath)
	require.NoError(t, err)
	sizeBefore := fi.Size()
	require.Positive(t, sizeBefore, "the fixture wrote no log bytes to lose")

	// The index has to go, or setupIndex takes the tail from it and the walk
	// under test never runs. That is also the real-world shape: an index behind
	// its log is what a crash between the frame write and the entry write
	// leaves, and reconciling it is why this walk exists.
	require.NoError(t, os.Remove(filepath.Join(dir, fmt.Sprintf(fileFormat, base, indexFileSuffix))))

	// Make the FIRST frame claim more payload than the file holds, so the walk
	// resolves nothing and startPos stays 0.
	f, err := os.OpenFile(logPath, os.O_RDWR, 0666)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{0x7F, 0xFF, 0xFF, 0xFF}, sizePos)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	l2, err := New(opts)
	if err == nil {
		require.NoError(t, l2.Close())
	}
	require.Error(t, err,
		"a log whose first frame does not resolve opened anyway, having "+
			"discarded every committed record behind it")

	fi, err = os.Stat(logPath)
	require.NoError(t, err)
	require.Equal(t, sizeBefore, fi.Size(),
		"recovery discarded a torn tail that reached below the high watermark: "+
			"the records were acknowledged, and a frame that does not parse is "+
			"not permission to delete the ones before it")
}

// The counterpart, and the reason the floor is a floor rather than a refusal: a
// crash during a segment's very FIRST append leaves a partial frame with
// nothing committed behind it, and that has to keep recovering the way it
// always did. Here the watermark sits below the segment's base, so the segment
// holds nothing acknowledged and the tail is dropped.
func TestATornFirstFrameAboveTheWatermarkStillRecovers(t *testing.T) {
	dir := tempDir(t)
	opts := Options{
		Path:            dir,
		MaxSegmentBytes: 220, // small, so the log rolls and the tail lands alone
		Compression:     compress.None,
	}
	l, err := New(opts)
	require.NoError(t, err)
	cl := l.(*commitLog)

	for i := 0; i < 40; i++ {
		_, aerr := cl.Append([]*Message{{
			Value:     []byte(fmt.Sprintf("value-%04d", i)),
			Timestamp: int64(i + 1),
		}})
		require.NoError(t, aerr)
	}
	active := cl.activeSegment()
	require.Positive(t, active.BaseOffset,
		"the fixture never rolled, so there is no fresh segment to tear")
	// Committed only through the segment BEFORE the active one, so the active
	// segment holds nothing acknowledged — the state a roll leaves.
	cl.SetHighWatermark(active.BaseOffset - 1)
	require.NoError(t, cl.SyncAll())
	base := active.BaseOffset
	require.NoError(t, cl.Close())

	logPath := filepath.Join(dir, fmt.Sprintf(fileFormat, base, logFileSuffix))
	require.NoError(t, os.Remove(filepath.Join(dir, fmt.Sprintf(fileFormat, base, indexFileSuffix))))
	f, err := os.OpenFile(logPath, os.O_RDWR, 0666)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{0x7F, 0xFF, 0xFF, 0xFF}, sizePos)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	l3, err := New(opts)
	require.NoError(t, err,
		"an unclean shutdown during a fresh segment's first append no longer "+
			"recovers: the floor is meant to protect committed records, not to "+
			"refuse every torn tail")
	require.NoError(t, l3.Close())

	fi, err := os.Stat(logPath)
	require.NoError(t, err)
	require.Zero(t, fi.Size(), "the unresolvable, uncommitted tail was kept")
}
