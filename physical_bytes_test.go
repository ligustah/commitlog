package commitlog

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ligustah/commitlog/compress"
)

// A BYTE BUDGET IS ABOUT BYTES THAT EXIST.
//
// Three settings bound or report bytes as a RESOURCE — Options.MaxLogBytes,
// Tier.MaxBytes and LocalBytes() — and all three used to sum (*segment).Position,
// which on a block-compressed log is the UNCOMPRESSED extent rather than the size
// of anything. Nothing noticed because the whole byte-retention suite runs on
// uncompressed fixtures, where the two numbers are equal by construction: the
// tests stood exactly where the defect cannot occur.
//
// The error runs both ways, so neither "it is conservative" nor "it is generous"
// covers it. Bytes that compress make the logical number too big and the log
// deletes records the budget had room for; bytes that do not compress are stored
// raw and still carry an 11-byte block header apiece, so the logical number is
// too small and the log overruns the budget it was given.

// compressibleLog builds a block-compressed log out of batches of near-identical
// records, and returns it with its logical and physical totals.
//
// Batched because a block is one WriteMessageSet: per-record appends would give
// per-record blocks, every one of them under compressMinBlock and so stored RAW,
// and the fixture would have a codec configured and no compression happening —
// the premise the assertions below check rather than assume.
func compressibleLog(t *testing.T, opts Options, batches, batch int) (l *commitLog, logical, physical int64) {
	t.Helper()
	opts.Path = tempDir(t)
	opts.Compression = compress.Zstd
	l, cleanup := setupWithOptions(t, opts)
	t.Cleanup(cleanup)

	n := 0
	for b := 0; b < batches; b++ {
		msgs := make([]*Message, 0, batch)
		for i := 0; i < batch; i++ {
			msgs = append(msgs, &Message{
				Key: []byte(fmt.Sprintf("k%06d", n)),
				// Zeroes: the point is a ratio wide enough that the two measures
				// cannot be mistaken for each other, not a realistic corpus.
				Value: make([]byte, 512),
			})
			n++
		}
		offs, err := l.Append(msgs)
		require.NoError(t, err)
		l.SetHighWatermark(offs[len(offs)-1])
	}
	require.NoError(t, l.SyncAll())

	for _, seg := range l.segmentsSnapshot() {
		logical += seg.Position()
		physical += seg.PhysicalSize()
	}
	require.Less(t, physical*4, logical,
		"the fixture did not compress (%d physical, %d logical), so it cannot tell the two measures apart",
		physical, logical)
	return l, logical, physical
}

// LocalBytes on a compressed log must report the compressed bytes, because the
// question it answers is what MOVING the log would cost and a move copies files.
//
// diskLogBytes is the independent measure — it stats the .log files — so this
// disagrees with the implementation rather than restating it. The uncompressed
// twin of this assertion (TestALogsLocalBytesAreTheBytesOnDisk) has been green
// throughout, which is the whole point: with no codec, Position IS the file size.
func TestACompressedLogsLocalBytesAreTheBytesOnDisk(t *testing.T) {
	l, logical, physical := compressibleLog(t,
		Options{Name: "squeezed", MaxSegmentBytes: 128 << 10}, 12, 200)

	got := l.LocalBytes()
	require.Equal(t, diskLogBytes(t, l.Path), got,
		"LocalBytes reported %d; the .log files hold %d", got, diskLogBytes(t, l.Path))
	require.Equal(t, physical, got)
	require.NotEqual(t, logical, got,
		"LocalBytes matched the uncompressed extent, which is the bug")
}

// BYTE RETENTION ON A COMPRESSED LOG MUST NOT DELETE WHAT FITS.
//
// The budget is set BETWEEN the two measures — above everything on disk, below
// the extent those bytes decompress to — so it is the choice of measure alone
// that decides the outcome, and each answer is a different verdict rather than a
// different margin. Measured physically the log is comfortably inside its limit
// and retention has nothing to do; measured logically it is over, and the pass
// deletes records the caller paid for the disk to keep.
//
// A budget that sits above BOTH numbers would pass either way, and one below both
// would delete either way. Only a fixture standing between them has an opinion.
func TestByteRetentionOnACompressedLogCountsTheBytesOnDisk(t *testing.T) {
	l, logical, physical := compressibleLog(t, Options{
		Name:            "retained",
		MaxSegmentBytes: 64 << 10,
		// Cleaning is driven below, so the pass under test is the one this test
		// asked for and not whichever tick happened to land during the appends.
		DisableAutoClean: true,
	}, 24, 200)

	require.Greater(t, len(l.segmentsSnapshot()), 2,
		"one segment is never deleted, so a one-segment log survives any limit and asserts nothing")

	budget := (physical + logical) / 2
	require.Greater(t, budget, physical, "budget must be above the bytes on disk")
	require.Less(t, budget, logical, "budget must be below the uncompressed extent")

	l.MaxLogBytes = budget
	l.deleteCleaner.Retention.Bytes = budget

	oldest := l.OldestOffset()
	require.NoError(t, l.Clean())
	require.Equal(t, oldest, l.OldestOffset(),
		"retention dropped records from a log holding %d bytes under a %d-byte budget",
		physical, budget)
	require.LessOrEqual(t, l.LocalBytes(), budget)
}

// A TIER'S BUDGET IS THE SIZE OF ITS OBJECTS.
//
// The third site, and the one that costs money: Tier.MaxBytes bounds what a
// store HOLDS, and a store bills for the object, not for the extent it
// decompresses to. Same construction as the local test — a budget standing
// between the two measures — over segments that have actually been offloaded, so
// PhysicalSize is answering from the manifest's recorded object size rather than
// from a local file.
func TestTierByteBudgetCountsTheBytesInTheStore(t *testing.T) {
	l, _, _ := compressibleLog(t, Options{
		Name:             "tiered",
		MaxSegmentBytes:  64 << 10,
		DisableAutoClean: true,
	}, 24, 200)
	fs, err := NewFileSegmentStore(filepath.Join(l.Path, "store"))
	require.NoError(t, err)
	l.Tiers = oneTier(fs)
	l.deleteCleaner.Tiers = l.Tiers

	moved, err := l.OffloadBefore(l.ActiveSegmentBase())
	require.NoError(t, err)
	require.Greater(t, moved, 1, "need several tiered segments for a budget to cut between")

	var logical, physical int64
	for _, seg := range l.segmentsSnapshot() {
		seg.RLock()
		off := seg.isOffloaded()
		seg.RUnlock()
		if !off {
			continue
		}
		logical += seg.Position()
		physical += seg.PhysicalSize()
	}
	require.Less(t, physical*4, logical, "the offloaded objects did not compress")

	budget := (physical + logical) / 2
	l.deleteCleaner.Tiers[0].MaxBytes = budget

	oldest := l.OldestOffset()
	require.NoError(t, l.Clean())
	require.Equal(t, oldest, l.OldestOffset(),
		"tier retention reclaimed objects holding %d bytes under a %d-byte budget",
		physical, budget)
}

// AND IT MUST STILL DELETE WHAT DOES NOT FIT.
//
// The guard on the test above: a measure that always answered zero, or one that
// simply never deleted, would satisfy it completely. Same fixture, same codec,
// budget now below the compressed total — the pass has to act.
func TestByteRetentionOnACompressedLogStillDeletes(t *testing.T) {
	l, _, physical := compressibleLog(t, Options{
		Name:             "trimmed",
		MaxSegmentBytes:  64 << 10,
		DisableAutoClean: true,
	}, 24, 200)

	budget := physical / 4
	l.MaxLogBytes = budget
	l.deleteCleaner.Retention.Bytes = budget

	oldest := l.OldestOffset()
	require.NoError(t, l.Clean())
	require.Greater(t, l.OldestOffset(), oldest,
		"a %d-byte budget over %d bytes on disk deleted nothing", budget, physical)
	require.LessOrEqual(t, l.LocalBytes(), budget)
}
