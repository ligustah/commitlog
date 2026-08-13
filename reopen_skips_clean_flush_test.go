package commitlog

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// A log that is REOPENED and never written to must not fsync a single index on
// the way out.
//
// This is the case v0.78.0 left on the table. Skipping the flush for segments
// THIS process sealed helped a long-lived log, but the shape that hurts most is
// a restart: a broker opens 336 segments, serves reads, and shuts down again,
// paying one device-cache flush per segment for bytes it never touched. Every
// one of those segments arrived with dirtyIndex=true, because "a segment opened
// from disk was written by a process whose flush state we cannot know".
//
// What makes the mark clearable is not a promise about the predecessor. It is
// that open() now reconciles EVERY segment's index tail, so an index that does
// not describe its log is filled in rather than believed — and a reconcile that
// wrote nothing is the proof that this one does describe it.
func TestAReopenedLogFlushesNoIndexAtClose(t *testing.T) {
	dir := tempDir(t)
	opts := Options{Path: dir, MaxSegmentBytes: 256, DisableAutoClean: true}

	l, err := New(opts)
	require.NoError(t, err)
	for i := 0; i < 60; i++ {
		_, err := l.Append([]*Message{{Value: []byte("record-" + strconv.Itoa(i))}})
		require.NoError(t, err)
	}
	require.Greater(t, len(l.(*commitLog).segmentsSnapshot()), 2,
		"the fixture needs sealed segments, or there is nothing to skip")
	require.NoError(t, l.Close())

	l2, err := New(opts)
	require.NoError(t, err)
	segs := l2.(*commitLog).segmentsSnapshot()

	// Counted per segment rather than in total, so a single segment still being
	// flushed cannot hide behind the others.
	before := make([]int64, len(segs))
	for i, s := range segs {
		require.NotNil(t, s.Index, "no segment here is offloaded")
		before[i] = s.Index.flushes.Load()
		require.False(t, s.dirtyIndex,
			"segment %d arrived dirty: its index tail reconciled clean at open, so "+
				"there is nothing about it that needs writing back", s.BaseOffset)
	}

	require.NoError(t, l2.Close())

	for i, s := range segs {
		require.Equal(t, before[i], s.Index.flushes.Load(),
			"segment %d was fsynced at close by a process that never wrote to it; "+
				"on a 336-segment log that is 336 device-cache flushes for bytes "+
				"already on disk", s.BaseOffset)
	}
}

// The other direction: a segment whose index open had to REPAIR is genuinely
// unflushed, and closing it must still flush it.
//
// Without this the skip above reads as "a reopened log never flushes", which is
// the wrong rule and the dangerous one — the entries the reconcile just wrote
// exist nowhere else.
func TestAReopenedLogFlushesAnIndexItHadToRepair(t *testing.T) {
	dir := tempDir(t)
	opts := Options{Path: dir, MaxSegmentBytes: 256, DisableAutoClean: true}

	l, err := New(opts)
	require.NoError(t, err)
	for i := 0; i < 60; i++ {
		_, err := l.Append([]*Message{{Value: []byte("record-" + strconv.Itoa(i))}})
		require.NoError(t, err)
	}
	require.Greater(t, len(l.(*commitLog).segmentsSnapshot()), 2)
	require.NoError(t, l.Close())

	truncateFirstIndexByOneEntry(t, dir)

	l2, err := New(opts)
	require.NoError(t, err)
	segs := l2.(*commitLog).segmentsSnapshot()
	first := segs[0]
	require.True(t, first.dirtyIndex,
		"the reconcile wrote this segment's missing tail entry, so its index is "+
			"unflushed by definition")

	before := first.Index.flushes.Load()
	require.NoError(t, l2.Close())
	require.Greater(t, first.Index.flushes.Load(), before,
		"an index repaired at open was not flushed at close; the repair would have "+
			"to run again on every open until a write happened to flush it")
}
