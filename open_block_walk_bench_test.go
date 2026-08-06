package commitlog

import (
	"testing"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// What does OPENING a block-compressed log cost, and how does that scale?
//
// Opening reads the directory and calls newSegment for every local .log
// (commitlog.go), and newSegment -> initPositions -> scanBlocks walks the block
// chain: each block's 11-byte header gives the length that locates the next, so
// the walk is one ReadAt per block, over every segment, before a single record
// is served. The offloaded half of the same problem was removed already — a
// manifest entry carries blockMode/position/physPosition and the block table is
// its own store object, measured by TestOpeningAnOffloadedTierReadsNoLogObjects
// — but a LOCAL segment still re-derives all of it every time.
//
// The walk scales with the block COUNT, not with bytes, and the append path
// writes one block per message set. cleanBlockTarget's comment records what
// that reaches in practice: 18.6M ~140-byte blocks holding ~900MB. Consolidation
// is the current answer, and it is a full data rewrite that only reaches sealed
// segments a clean pass has visited.
//
// headers/open is the honest measure and needs no instrumentation: scanBlocks
// appends exactly one blockRef per header it reads, so len(seg.blocks) IS the
// number of reads that segment's walk performed. Deliberately NOT measured by
// wrapping the backing — discardTornTail asserts s.backing.(*localBacking) and
// treats a failure as "not local, nothing to do", so a counting wrapper would
// silently disable torn-tail discard for every segment opened under it.
//
// A benchmark rather than a test because there is no correct number to assert
// against today; the assertion arrives with the fix, as the local twin of the
// offloaded test above.
func BenchmarkReopenWalksEveryBlockHeader(b *testing.B) {
	dir := tempDir(b)
	opts := Options{
		Path:            dir,
		MaxSegmentBytes: 1 << 16,
		Compression:     compress.Snappy,
		// The cleaner would consolidate blocks underneath the measurement, which
		// is the mitigation being measured against, not the thing being measured.
		DisableAutoClean: true,
	}

	l, err := New(opts)
	require.NoError(b, err)
	// One Append is one message set is one block — the shape small commits
	// produce, and the reason the count runs ahead of the byte size.
	for i := 0; i < 8000; i++ {
		_, err := l.Append([]*Message{{Value: []byte("a modest record payload")}})
		require.NoError(b, err)
	}
	require.NoError(b, l.Close())

	// Counted once, outside the timed loop, so the number is reported even when
	// the harness settles on a single iteration.
	probe, err := New(opts)
	require.NoError(b, err)
	var segments, headers int
	cl := probe.(*commitLog)
	cl.mu.RLock()
	for _, s := range cl.segments {
		s.RLock()
		if s.blockMode {
			headers += len(s.blocks)
		}
		segments++
		s.RUnlock()
	}
	cl.mu.RUnlock()
	require.NoError(b, probe.Close())
	require.Positive(b, headers,
		"the fixture built no blocks, so this benchmark measures nothing")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reopened, err := New(opts)
		if err != nil {
			b.Fatal(err)
		}
		if err := reopened.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(headers), "headers/open")
	b.ReportMetric(float64(segments), "segments")
}
