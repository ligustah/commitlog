package commitlog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// What does OPENING a block-compressed log cost, with and without the block
// tables its segments persisted at seal?
//
// Opening reads the directory and calls newSegment for every local .log, and
// initPositions rebuilds the block table by walking the chain: each block's
// 11-byte header gives the length that locates the next, so it is one read per
// block, over every segment, before a single record is served. Persisting the
// table at seal removes that walk; this measures what removing it is worth.
//
// The two sub-benchmarks differ ONLY in whether the sidecars are on disk, so the
// delta is the walk and nothing else. "walked" deletes them, which is also the
// path every segment sealed by an older build takes.
//
// The walk scales with the block COUNT, not with bytes — the append path writes
// one block per message set, so the cost is set by how small the commits were.
// cleanBlockTarget's comment records 18.6M ~140-byte blocks in one real run,
// which is why the fixture is parameterised by record count rather than size.
//
// Measured (windows, Core Ultra 9 285K, on a busy machine):
//
//	40000 blocks in 3 segments   86.8ms loaded   209.4ms walked   2.4x
//	40000 blocks in 44 segments   947ms loaded    1150ms walked
//	 8000 blocks in  9 segments   within noise
//
// Read those in that order, because they say something the headline number does
// not. The walk costs roughly 3us per block, so it is worth about 122ms of the
// first row and is simply invisible under the per-segment work in the third. It
// is worth removing because of where it goes from there: at the 18.6M blocks
// that comment records, 3us each is the better part of a minute spent before the
// log serves anything, and no amount of segment-level tuning touches it.
func BenchmarkOpenBlockCompressedLog(b *testing.B) {
	// segBytes is varied deliberately. With small segments the open is dominated
	// by per-SEGMENT work — a file open and an index mmap each — and the walk
	// disappears into it. The walk scales with blocks, so isolating it means
	// putting many blocks behind few segments, which is also the shape the
	// problem takes in practice: cleanBlockTarget's comment records 18.6M
	// ~140-byte blocks, a block count no segment count comes near.
	for _, c := range []struct {
		records  int
		segBytes int64
	}{
		{8000, 1 << 16},  // many segments, few blocks each
		{40000, 1 << 16}, // more of both
		{40000, 1 << 30}, // one segment, every block in it
	} {
		records := c.records
		opts := benchBlockLog(b, records, c.segBytes)

		b.Run("loaded/"+itoa(records), func(b *testing.B) {
			benchReopen(b, opts, false)
		})

		// The same bytes in their own directory, opened with the sidecars
		// removed before every iteration: the open has to walk.
		stripped := copyLogDir(b, opts)
		b.Run("walked/"+itoa(records), func(b *testing.B) {
			benchReopen(b, stripped, true)
		})
	}
}

// benchReopen times a reopen and reports how many block headers it walked.
//
// strip re-deletes the sidecars before each timed open, and is what makes the
// "walked" case measurable at all: opening a log SEALS its non-active segments,
// and seal is where the table is written, so a stripped directory heals itself
// on its first open and every iteration after that would be measuring the loaded
// path under the walked label. (That self-healing is the migration story — a log
// written by a build without this gains its tables the first time this one opens
// it, with no rebuild step and no version gate — but it makes a naive loop lie.)
// The deletion is untimed.
func benchReopen(b *testing.B, opts Options, strip bool) {
	b.Helper()
	if strip {
		stripSidecars(b, opts.Path)
	}
	// Counted from a real open. blocksWalked is the walk's own count — NOT
	// len(seg.blocks), which stays populated either way and would report the
	// table's size as though it were reads performed.
	probe, err := New(opts)
	require.NoError(b, err)
	var walked, segments int
	cl := probe.(*commitLog)
	cl.mu.RLock()
	for _, s := range cl.segments {
		s.RLock()
		walked += s.blocksWalked
		segments++
		s.RUnlock()
	}
	cl.mu.RUnlock()
	require.NoError(b, probe.Close())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if strip {
			b.StopTimer()
			stripSidecars(b, opts.Path)
			b.StartTimer()
		}
		l, err := New(opts)
		if err != nil {
			b.Fatal(err)
		}
		if err := l.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(walked), "headers-walked")
	b.ReportMetric(float64(segments), "segments")
}

func stripSidecars(b *testing.B, dir string) {
	b.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(b, err)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == blocksSuffix {
			require.NoError(b, os.Remove(filepath.Join(dir, e.Name())))
		}
	}
}

// benchBlockLog builds a closed, sealed, block-compressed log.
func benchBlockLog(b *testing.B, records int, segBytes int64) Options {
	b.Helper()
	opts := Options{
		Path:            tempDir(b),
		MaxSegmentBytes: segBytes,
		Compression:     compress.Snappy,
		// A clean would consolidate blocks underneath the measurement, which is
		// the mitigation being measured against, not the thing being measured.
		DisableAutoClean: true,
	}
	l, err := New(opts)
	require.NoError(b, err)
	for i := 0; i < records; i++ {
		// One Append is one message set is one block.
		_, err := l.Append([]*Message{{Value: []byte("a modest record payload")}})
		require.NoError(b, err)
	}
	require.NoError(b, l.Close())
	return opts
}

// copyLogDir clones a log directory WITHOUT its block-table sidecars, so the
// same bytes can be opened both ways.
func copyLogDir(b *testing.B, src Options) Options {
	b.Helper()
	dst := src
	dst.Path = tempDir(b)
	entries, err := os.ReadDir(src.Path)
	require.NoError(b, err)
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) == blocksSuffix {
			continue
		}
		data, err := os.ReadFile(filepath.Join(src.Path, e.Name()))
		require.NoError(b, err)
		require.NoError(b, os.WriteFile(filepath.Join(dst.Path, e.Name()), data, 0666))
	}
	return dst
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
