package commitlog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// walkedOnOpen reopens the log at dir and reports how many block headers the
// open had to walk, and how many segments it opened.
func walkedOnOpen(t *testing.T, opts Options) (walked, segments int) {
	t.Helper()
	l, err := New(opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, l.Close()) }()
	cl := l.(*commitLog)
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	for _, s := range cl.segments {
		s.RLock()
		walked += s.blocksWalked
		segments++
		s.RUnlock()
	}
	return walked, segments
}

// blockCompressedLog builds a log with several sealed block-compressed segments
// and returns its options, closed and ready to reopen.
func blockCompressedLog(t *testing.T, records int) Options {
	t.Helper()
	opts := Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 1 << 16,
		Compression:     compress.Snappy,
		// A clean would consolidate blocks underneath the measurement.
		DisableAutoClean: true,
	}
	l, err := New(opts)
	require.NoError(t, err)
	for i := 0; i < records; i++ {
		// One Append is one message set is one block.
		_, err := l.Append([]*Message{{Value: []byte("a modest record payload")}})
		require.NoError(t, err)
	}
	require.NoError(t, l.Close())
	return opts
}

// Reopening a sealed block-compressed log walks NO block headers.
//
// It used to walk every one of them. Opening calls newSegment for every local
// .log, and initPositions rebuilt the block table by following the chain: each
// block's header carries the length that locates the next, so it is one read per
// block, across every segment, before a single record is served. The walk scales
// with the block COUNT rather than with bytes — the append path writes one block
// per message set — so the cost is set by how small the commits were, not by how
// much data there is. cleanBlockTarget's comment records 18.6M ~140-byte blocks
// in one real run; BenchmarkReopenWalksEveryBlockHeader measures 8000 headers
// and 173ms for a log of 576KiB.
//
// The tier half of this was removed already, and held there by
// TestOpeningAnOffloadedTierReadsNoLogObjects. This is its local twin: a sealed
// segment now persists the table it built, in the same bytes the store object
// uses, and the open reads it instead of rebuilding it.
//
// The ACTIVE segment is covered too, and not by an exemption: closeSegment ends
// in seal(), so shutting a log down seals the segment it was still appending to
// and writes its table with the rest. The measurement below is over EVERY segment
// the reopen produced, active one included, which is why it can require zero
// rather than "zero but for the last".
//
// What is left is the open that follows a CRASH, where no close ran to seal
// anything. That walk is not bookkeeping — it is the same pass that finds the
// torn tail and discards it — so it stays, bounded by one segment.
// TestAWalkAtOpenPersistsTheTableItBuilt holds the part that does not have to
// repeat.
func TestReopeningASealedBlockLogWalksNoBlockHeaders(t *testing.T) {
	opts := blockCompressedLog(t, 4000)

	walked, segments := walkedOnOpen(t, opts)
	require.Greater(t, segments, 2,
		"the fixture built too few segments to be measuring anything")
	require.Zero(t, walked,
		"opening the log walked %d block headers; every sealed segment should "+
			"have loaded the table it persisted at seal", walked)
}

// A missing or damaged sidecar costs a walk, never the log.
//
// The table is DERIVED data: the bytes it describes are on the same disk, so a
// sidecar that cannot be trusted is recomputed rather than refused. This is the
// deliberate opposite of decodeBlockTable's rule for the store object, where
// walking means downloading the object again and a silent fallback would buy a
// slow success while hiding the failure. Locally the walk is what every open did
// before this existed, and refusing to open a log because a file it can
// regenerate is unreadable would be far worse than a slow open.
//
// It matters beyond corruption: every segment sealed by a build without this,
// and every segment whose best-effort write at seal failed, arrives here.
func TestAnUnusableBlockTableFallsBackToTheWalk(t *testing.T) {
	for _, tc := range []struct {
		name   string
		damage func(t *testing.T, path string)
	}{
		{"absent", func(t *testing.T, path string) {
			require.NoError(t, os.Remove(path))
		}},
		{"truncated", func(t *testing.T, path string) {
			require.NoError(t, os.WriteFile(path, []byte{0x42}, 0666))
		}},
		{"corrupt", func(t *testing.T, path string) {
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			data[len(data)-1] ^= 0xff // break the trailing CRC
			require.NoError(t, os.WriteFile(path, data, 0666))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := blockCompressedLog(t, 4000)

			// Damage the OLDEST segment's sidecar; it is sealed, so it has one.
			path := filepath.Join(opts.Path, "00000000000000000000"+blocksSuffix)
			require.FileExists(t, path,
				"a sealed block-compressed segment should have persisted its table")
			tc.damage(t, path)

			walked, _ := walkedOnOpen(t, opts)
			require.Positive(t, walked,
				"the damaged sidecar was accepted instead of being recomputed")

			// And the log still serves its records, which is the point of falling
			// back rather than refusing.
			l, err := New(opts)
			require.NoError(t, err)
			defer func() { require.NoError(t, l.Close()) }()
			require.Positive(t, l.NewestOffset())
		})
	}
}

// A walk at open persists what it built, so a crash costs the chain once.
//
// seal() is the other writer and it covers the log that was closed cleanly —
// closeSegment ends in seal(), so every segment gets a sidecar on the way out.
// The gap is the open that FOLLOWS a crash: the process that would have sealed
// the active segment is gone, and the segment whose best-effort write at seal
// failed has nothing to try again either. Without a write on the far side of the
// walk, the next open rebuilds the same table, and so does the one after it, for
// as long as the crashes keep arriving before a clean close does.
//
// The assertion is made while the log is still OPEN, and that is the whole
// design of the test. Closing it seals every segment and writes the sidecar
// anyway, so a check afterwards passes just as happily against code that
// persists nothing at open — it would be measuring seal, not this.
func TestAWalkAtOpenPersistsTheTableItBuilt(t *testing.T) {
	opts := blockCompressedLog(t, 4000)
	path := filepath.Join(opts.Path, "00000000000000000000"+blocksSuffix)
	require.FileExists(t, path,
		"a sealed block-compressed segment should have persisted its table")
	require.NoError(t, os.Remove(path))

	l, err := New(opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, l.Close()) }()

	cl := l.(*commitLog)
	cl.mu.RLock()
	first := cl.segments[0]
	cl.mu.RUnlock()
	first.RLock()
	walked := first.blocksWalked
	first.RUnlock()
	require.Positive(t, walked,
		"the fixture did not produce a walk, so it cannot be testing what the "+
			"walk leaves behind")

	require.FileExists(t, path,
		"the open walked %d block headers and kept the result to itself; the "+
			"next open after a crash would walk them again", walked)

	// And what it wrote has to be what the reader accepts, or persisting it is a
	// write that changes nothing: loadLocalBlockTable refuses a table that does
	// not account for exactly the bytes of the file beside it.
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	blocks, err := decodeBlockTable(data)
	require.NoError(t, err, "the table written at open does not decode")
	require.Len(t, blocks, walked,
		"the persisted table does not hold the blocks the walk resolved")
	logInfo, err := os.Stat(filepath.Join(opts.Path, "00000000000000000000"+logSuffix))
	require.NoError(t, err)
	_, phys := blockTableExtent(blocks)
	require.Equal(t, logInfo.Size(), phys,
		"the table written at open describes a different number of bytes than "+
			"the log beside it, so the next open would refuse it and walk anyway")
}

// A stale table is refused even though it decodes cleanly.
//
// The sidecar carries no version or generation of its own; the check is that the
// physical extent it accounts for equals the size of the file beside it. A log
// only grows by append, and a trim or a rewrite installs a different file under
// the same name, so a table that decodes AND accounts for exactly the right
// number of bytes is this file's table. Getting this wrong does not fail loudly:
// a table off by a block maps logical offsets onto the wrong records, so the
// segment answers reads with plausible garbage.
func TestABlockTableForDifferentBytesIsRefused(t *testing.T) {
	opts := blockCompressedLog(t, 4000)
	path := filepath.Join(opts.Path, "00000000000000000000"+blocksSuffix)

	// A table built from a DIFFERENT segment's log: well-formed, correct CRC,
	// and describing the wrong number of bytes.
	other := blockCompressedLog(t, 500)
	donor := filepath.Join(other.Path, "00000000000000000000"+blocksSuffix)
	data, err := os.ReadFile(donor)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0666))

	// It must decode, or this test proves nothing about the SIZE check.
	blocks, err := decodeBlockTable(data)
	require.NoError(t, err, "the donor table must be well-formed to be a fixture")
	require.NotEmpty(t, blocks)

	walked, _ := walkedOnOpen(t, opts)
	require.Positive(t, walked,
		"a table describing a different file was believed; reads of that segment "+
			"would land on the wrong records")
}
