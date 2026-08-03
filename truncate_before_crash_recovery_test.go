package commitlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// buildInterruptedTrim leaves the directory in exactly the state a crash inside
// TruncateBefore does: the boundary segment has been rewritten at a higher base
// offset and renamed into place, and the source it was rewritten FROM is still
// there. It runs the same steps TruncateBefore does, stopping one call short of
// boundary.Delete(). Returns the base offset of the trim and of its source.
func buildInterruptedTrim(t *testing.T, dir string, records int) (trimBase, sourceBase int64) {
	t.Helper()
	// New, not setupWithOptions: its cleanup REMOVES the directory, and the
	// whole point here is to hand the directory on to a reopen.
	cl, err := New(Options{Path: dir, MaxSegmentBytes: 256, DisableAutoClean: true})
	require.NoError(t, err)
	l := cl.(*commitLog)

	for n := int64(0); n < int64(records); n++ {
		offs, err := l.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("k:%d", n%16)),
			Value: []byte(strconv.FormatInt(n, 10) + ":padding to force segment rolls"),
		}})
		require.NoError(t, err)
		l.SetHighWatermark(offs[len(offs)-1])
	}

	l.mu.RLock()
	require.Greater(t, len(l.segments), 2, "need a sealed boundary that is not the active segment")
	boundary := l.segments[1]
	l.mu.RUnlock()
	cut := boundary.BaseOffset + 2
	require.LessOrEqual(t, cut, boundary.LastOffset(), "the cut has to land INSIDE the boundary")

	ss := newSegmentScanner(boundary)
	var kept []messageSet
	for {
		ms, _, err := ss.Scan()
		if err != nil {
			break
		}
		if ms.Offset() >= cut {
			kept = append(kept, ms)
		}
	}
	ss.Close()
	require.NotEmpty(t, kept)

	trimmed, err := boundary.Trimmed(cut)
	require.NoError(t, err)
	for _, ms := range kept {
		require.NoError(t, trimmed.WriteMessageSet(ms, entriesForMessageSet(trimmed.Position(), ms)))
	}
	require.NoError(t, trimmed.Finalize())
	trimmed.Seal()
	// CRASH HERE. boundary.Delete() never runs, so both files survive.
	//
	// A real crash takes the handles with it; this one has to give them back by
	// hand, and on Windows it MUST — a mapped index cannot be unlinked.
	trimmed.Lock()
	require.NoError(t, trimmed.close())
	trimmed.Unlock()
	require.NoError(t, l.Close())
	return cut, boundary.BaseOffset
}

// readAllOffsets drains the log from `from` and returns the offsets it served.
// The deadline is what ends it: an uncommitted reader that reaches the tail of
// the ACTIVE segment parks waiting for an append that is never coming, so there
// is no end-of-data to read towards here.
func readAllOffsets(t *testing.T, l CommitLog, from int64, limit int) []int64 {
	t.Helper()
	r, err := l.NewReader(From(from), Uncommitted())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	hdr := make([]byte, HeaderBufferLen)
	var got []int64
	for range limit {
		_, off, _, _, err := r.ReadMessage(ctx, hdr)
		if err != nil {
			break
		}
		got = append(got, off)
	}
	return got
}

// A crash between Finalize and Delete leaves two .log files whose offset ranges
// OVERLAP — the source [B, L] and the trim [B+2, L]. open() took both and a
// read walked the source to L and then started the trim again at B+2, so
// offsets came back twice, in order, with no error anywhere: 0..7 then 6,7,8,9
// on the first run of this.
//
// The trim is a suffix of the source, so the source alone is the whole log. The
// truncation simply did not happen, and the caller can run it again.
func TestAnInterruptedTrimDoesNotServeRecordsTwice(t *testing.T) {
	dir := tempDir(t)
	trimBase, sourceBase := buildInterruptedTrim(t, dir, 40)
	trimFile := filepath.Join(dir, fmt.Sprintf(fileFormat, trimBase, logSuffix))
	require.FileExists(t, trimFile, "the crash state itself is wrong if this is missing")

	l, err := New(Options{Path: dir, MaxSegmentBytes: 256, DisableAutoClean: true})
	require.NoError(t, err)
	defer l.Close()

	got := readAllOffsets(t, l, 0, 200)
	require.NotEmpty(t, got)
	require.Equal(t, int64(0), got[0], "the source's records are all still owed")
	for i, off := range got {
		require.Equal(t, int64(i), off,
			"offsets must be contiguous from 0; got %v", got)
	}
	require.Equal(t, int64(0), l.OldestOffset(), "nothing was truncated, so nothing is gone")

	// The duplicate is removed, not merely skipped: leaving it would repeat this
	// repair on every open, and leave a file on disk that describes records the
	// log does not reach through it.
	require.NoFileExists(t, trimFile)
	require.NoFileExists(t, filepath.Join(dir, fmt.Sprintf(fileFormat, trimBase, indexSuffix)))
	require.FileExists(t, filepath.Join(dir, fmt.Sprintf(fileFormat, sourceBase, logSuffix)))

	// And the interrupted truncation can just be run again.
	require.NoError(t, l.TruncateBefore(trimBase))
	require.Equal(t, trimBase, l.OldestOffset())
	after := readAllOffsets(t, l, trimBase, 200)
	require.NotEmpty(t, after)
	require.Equal(t, trimBase, after[0])
	for i, off := range after {
		require.Equal(t, trimBase+int64(i), off, "offsets must be contiguous; got %v", after)
	}
}

// Reopening is idempotent: the repair runs once and the second open finds
// nothing to do.
func TestAnInterruptedTrimIsRepairedOnlyOnce(t *testing.T) {
	dir := tempDir(t)
	buildInterruptedTrim(t, dir, 40)

	for i := range 2 {
		l, err := New(Options{Path: dir, MaxSegmentBytes: 256, DisableAutoClean: true})
		require.NoError(t, err, "open %d", i)
		got := readAllOffsets(t, l, 0, 200)
		require.NotEmpty(t, got)
		for j, off := range got {
			require.Equal(t, int64(j), off, "open %d: offsets must be contiguous; got %v", i, got)
		}
		require.NoError(t, l.Close())
	}
}

// An overlap where neither segment contains the other is not something any path
// in this package produces, and there is no repair for it that does not guess.
// Refusing to open says so; opening anyway would serve an offset twice, which
// is the failure the containment case exists to prevent.
func TestAPartialSegmentOverlapRefusesToOpen(t *testing.T) {
	dir := tempDir(t)
	l, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  1 << 20, // one segment, so the whole log is 0..19
		DisableAutoClean: true,
	})
	defer cleanup()

	for n := int64(0); n < 20; n++ {
		offs, err := l.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("k:%d", n)),
			Value: []byte(strconv.FormatInt(n, 10)),
		}})
		require.NoError(t, err)
		l.SetHighWatermark(offs[len(offs)-1])
	}

	// A segment based at 5 holding 5..19, installed under its own name.
	l.mu.RLock()
	seg := l.segments[0]
	l.mu.RUnlock()
	ss := newSegmentScanner(seg)
	trimmed, err := seg.Trimmed(5)
	require.NoError(t, err)
	for {
		ms, _, err := ss.Scan()
		if err != nil {
			break
		}
		if ms.Offset() >= 5 {
			require.NoError(t, trimmed.WriteMessageSet(ms, entriesForMessageSet(trimmed.Position(), ms)))
		}
	}
	ss.Close()
	require.NoError(t, trimmed.Finalize())
	trimmed.Seal()
	require.NoError(t, trimmed.Close())

	// Now cut the ORIGINAL back to 0..9, in place and under its own name, so the
	// two ranges cross instead of nesting. The live log cannot see the segment
	// installed above, so this rewrites only the base-0 files.
	require.NoError(t, l.Truncate(10))
	require.NoError(t, l.Close())

	bad, err := New(Options{Path: dir, MaxSegmentBytes: 1 << 20, DisableAutoClean: true})
	if err == nil {
		// Only reachable when this regresses, and then the handles have to go
		// back or the cleanup fails on Windows instead of on the assertion.
		bad.Close()
	}
	require.Error(t, err)
	require.Contains(t, err.Error(), "overlap")
	require.Contains(t, err.Error(), "serve an offset twice")
}

// The repair must not fire on an ordinary log. Every segment there starts where
// the one before it ends, and an EMPTY active segment reports its base offset as
// its exclusive end — the one place a naive `base <= prev.LastOffset` comparison
// would see an overlap that is not one.
func TestSegmentOverlapRepairLeavesAHealthyLogAlone(t *testing.T) {
	dir := tempDir(t)
	l, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  256,
		DisableAutoClean: true,
	})
	defer cleanup()

	for n := int64(0); n < 40; n++ {
		offs, err := l.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("k:%d", n%16)),
			Value: []byte(strconv.FormatInt(n, 10) + ":padding to force segment rolls"),
		}})
		require.NoError(t, err)
		l.SetHighWatermark(offs[len(offs)-1])
	}
	// Roll to a fresh, EMPTY active segment.
	require.NoError(t, l.split(l.activeSegment()))
	require.NoError(t, l.Close())
	before, err := os.ReadDir(dir)
	require.NoError(t, err)

	l2, err := New(Options{Path: dir, MaxSegmentBytes: 256, DisableAutoClean: true})
	require.NoError(t, err)
	defer l2.Close()

	after, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, after, len(before), "the repair removed a file from a healthy log")

	got := readAllOffsets(t, l2, 0, 200)
	require.Len(t, got, 40)
	for i, off := range got {
		require.Equal(t, int64(i), off)
	}
}
