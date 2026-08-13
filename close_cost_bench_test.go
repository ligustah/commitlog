package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// What does CLOSING a log cost, and how does it scale?
//
// This exists because BenchmarkOpenBlockCompressedLog times New() and Close()
// together — deliberately, since it measures the DELTA between a walked and a
// loaded open and the close cancels out of a difference. Read as an absolute,
// though, that benchmark says an open-and-close of a 44-segment log costs 791ms,
// and it is very easy to read that as the open. It is not: a CPU profile of that
// run puts commitLog.open at 8.6% and Close at 75.7%, with index.Close alone at
// 64% of the whole thing.
//
// So the two are separated here and the open is untimed. Each iteration opens a
// fresh log and times only the teardown, reporting ns-per-SEGMENT alongside
// ns/op because per-segment is the number that scales: durable_streams runs
// 336-segment logs, and a per-segment fsync there is 336 of them at every
// shutdown.
//
// The fixture is closed and sealed before the loop, so every timed iteration
// starts from the same state — sealed segments with shrunk indexes and their
// block tables already on disk. That is what a real shutdown closes, a log whose
// sealed segments this process opened and never wrote a byte to, and it is the
// case where the teardown has the least to do and does it anyway.
//
// A re-open does not re-dirty anything in the sense the append path means, but
// segment.dirtyIndex is initialized to true for every segment opened from disk
// regardless, on the stated ground that "a segment opened from disk was written
// by a process whose flush state we cannot know". That is why these numbers are
// what they are, and anything proposing to reduce them has to argue with that
// claim rather than around it.
//
// Measured (windows, Core Ultra 9 285K, IDLE box, two runs each), 44 segments:
//
//	reopened   254 / 323ms    5.8 / 7.3ms per segment
//	rolled     168 / 151ms    3.8 / 3.4ms per segment
//
// The gap is the per-segment fsync. What is left in the rolled case is the unmap
// and the handle close — the unmap is the floor here, and it is not optional —
// plus the active segment's genuine flush. At 9 segments the ratio is smaller
// (5.4 -> 3.5ms; the one dirty active segment is a bigger share of a shorter
// list) and at 3 it nearly vanishes (11.2 -> 9.6ms), which is the shape to
// expect: this saves work in proportion to how many CLEAN segments the log
// holds, so it is worth most to exactly the long-lived logs that have the most.
//
// Both rows come from ONE run of one binary against one disk. That matters more
// than it looks: an earlier measurement of this benchmark was taken while a
// neighbour was working the same disk and read about 10x high, and the run
// before that compared a treatment holding twice the records against its own
// control. A within-run comparison survives both mistakes; an absolute number
// survives neither.
//
// Before the close path honoured dirtyIndex, both rows were the reopened one —
// 7.6-8.7ms/segment, flat across a 15x range in segment count, and roughly 60%
// of a full open-and-close. It never scaled with records, blocks or bytes, only
// with how many segments the directory held, because every one of them was
// fsynced on the way out whether this process had written to it or not.
//
// Run it on a QUIET box. An earlier run of exactly this benchmark reported
// 85-105ms/segment, a 10x inflation, because a peer was working the same disk;
// the shape (flat per segment) held, the magnitude did not. A single run of a
// benchmark that is almost entirely fsync says as much about the neighbours as
// about the code.
func BenchmarkCloseCleanLog(b *testing.B) {
	for _, c := range []struct {
		records  int
		segBytes int64
	}{
		{8000, 1 << 16},  // many segments, few blocks each
		{40000, 1 << 16}, // more of both
		{40000, 1 << 30}, // one segment, every block in it
	} {
		opts := benchBlockLog(b, c.records, c.segBytes)

		// Counted once, outside any timed run, and counted from a real open
		// rather than derived from records/segBytes — the latter would be
		// arithmetic describing what the split SHOULD have done.
		probe, err := New(opts)
		require.NoError(b, err)
		segments := len(probe.(*commitLog).segments)
		require.NoError(b, probe.Close())
		require.Greater(b, segments, 0)

		// reopened: every segment starts dirtyIndex=true, because a segment opened
		// from disk was written by a process whose flush state this one cannot
		// know. So this case still pays the per-segment fsync, and it is the
		// CONTROL — same binary, same disk, same fixture as the case below.
		//
		// rolled: the log this process just appended to and rolled. Its sealed
		// segments were flushed by seal() and are marked clean, so close skips
		// the fsync. That is the production shape — a log that has been up long
		// enough to accumulate segments — and the difference between the two
		// rows is the whole of what honouring dirtyIndex at close is worth.
		b.Run(itoa(segments)+"seg/reopened", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				l, err := New(opts)
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()

				if err := l.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			// ns/op divided by segments, computed from the harness's own
			// elapsed time so it stays honest if b.N changes underneath.
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(segments),
				"ns/segment")
			b.ReportMetric(float64(segments), "segments")
		})

		b.Run(itoa(segments)+"seg/rolled", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				// An EMPTY directory each time. benchBlockLog would hand back a
				// log that already holds c.records, and appending c.records on
				// top of it closes twice the log the reopened case does while
				// still dividing by the original segment count — which is what
				// the first run of this benchmark did, and it reported the
				// treatment as 1.6x SLOWER than the control.
				fresh := Options{
					Path:             tempDir(b),
					MaxSegmentBytes:  c.segBytes,
					Compression:      opts.Compression,
					DisableAutoClean: true,
				}
				l, err := New(fresh)
				if err != nil {
					b.Fatal(err)
				}
				// Roll the segments in THIS process, which is what marks them
				// clean. Reopening above is what leaves them dirty.
				for j := 0; j < c.records; j++ {
					if _, err := l.Append([]*Message{{Value: []byte("a modest record payload")}}); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()

				if err := l.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(segments),
				"ns/segment")
			b.ReportMetric(float64(segments), "segments")
		})
	}
}
