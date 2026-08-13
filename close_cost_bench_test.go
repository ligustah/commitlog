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
// Measured (windows, Core Ultra 9 285K), against the open benchmark in the same
// process so the two are comparable:
//
//	 3 segments    24.2ms close    8.06ms/segment     54.5ms open+close
//	 9 segments    78.6ms close    8.74ms/segment     96.3ms open+close
//	44 segments   333.9ms close    7.59ms/segment    555.6ms open+close
//
// Flat per segment across a 15x range in segment count, and roughly 60% of an
// open-and-close — which is the point. It does not scale with records, blocks or
// bytes; it scales with how many segments the directory holds, because every one
// of them gets an fsync and a truncate on the way out whether this process wrote
// to it or not.
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

		b.Run(itoa(segments)+"seg", func(b *testing.B) {
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
	}
}
