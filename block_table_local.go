package commitlog

import (
	"fmt"
	"os"
	"path/filepath"
)

// A segment's block table, persisted next to its own files.
//
// Opening a log calls newSegment for every local .log, and initPositions walks
// the block chain to rebuild the table: each block's header gives the length
// that locates the next, so it is one read per block, over every segment, before
// a single record is served. The walk scales with the block COUNT rather than
// with bytes — the append path writes one block per message set — and
// cleanBlockTarget's comment records what that reaches in practice: 18.6M
// ~140-byte blocks in one run. Measured on a small log in
// BenchmarkReopenWalksEveryBlockHeader: 8000 headers across 9 segments, 173ms.
//
// The tier already solved its half of this by writing the table as its own
// object (see block_table.go). This is the same table, in the same bytes, for a
// segment whose log is still local — so the two cannot drift into disagreeing
// about the format, and neither has to be taught about the other.
func localBlockTablePath(seg *segment) string {
	return filepath.Join(seg.path,
		fmt.Sprintf(fileFormat, seg.BaseOffset, blocksSuffix+seg.suffix))
}

// writeLocalBlockTable installs the table atomically beside the segment.
//
// Called from seal, which is the moment after which the segment's bytes stop
// changing, and which already carries two best-effort flushes of derived state
// for the same reason. Best-effort here too: a failure costs the next open a
// walk, which is exactly what happened before this existed.
//
// tmp-then-rename rather than a plain write, so the failure cannot be a
// half-written table. Best-effort is only safe while the error path leaves
// nothing behind to find; a torn file would be read back by the next open, and
// it would be the CRC rather than the design that caught it.
func writeLocalBlockTable(seg *segment) error {
	path := localBlockTablePath(seg)
	tmp := path + tmpSuffix
	if err := os.WriteFile(tmp, encodeBlockTable(seg.blocks), 0666); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp) // nolint: errcheck
		return err
	}
	return nil
}

// loadLocalBlockTable returns the table for a segment whose log is physSize
// bytes, or ok=false if there is no usable one and the chain must be walked.
//
// physSize is the whole staleness check, and it needs no field of its own:
// blockTableExtent sums the lengths the table already carries, and a table
// describing a different number of bytes than the file holds is not this file's
// table. A log only ever grows by append, and a trim or a rewrite installs a
// different file under the same name, so there is no way for a stale table to
// account for exactly the right size.
//
// Absence, staleness and corruption all fall back to the walk. That is the
// OPPOSITE of what decodeBlockTable's own comment prescribes, deliberately, and
// the difference is which walk is being avoided. In the store, walking means
// downloading the object again — the whole cost the table exists to remove — so
// a silent fallback there buys a slow success and hides the failure. Locally the
// bytes are already on the disk the table sits on, the table is derived data
// that is legitimately missing whenever its best-effort write failed, and
// refusing to open a log because a file it can regenerate is unreadable would be
// a far worse regression than a slow open. Nothing is trusted that fails to
// verify; it is only recomputed instead of refused.
//
// This used to be justified partly by "every segment sealed before this
// existed", which is a compatibility argument and no longer load-bearing: the
// library is pre-v1 with nothing to migrate, and the failed-write case is a live
// one that keeps the branch on its own. Struck rather than left standing,
// because a reason that only holds for old data invites deleting the branch the
// day someone checks whether any old data exists.
func loadLocalBlockTable(seg *segment, physSize int64) (blocks []blockRef, logical int64, ok bool) {
	data, err := os.ReadFile(localBlockTablePath(seg))
	if err != nil {
		return nil, 0, false
	}
	blocks, err = decodeBlockTable(data)
	if err != nil {
		return nil, 0, false
	}
	logical, phys := blockTableExtent(blocks)
	if phys != physSize {
		return nil, 0, false
	}
	return blocks, logical, true
}

// removeLocalBlockTable drops the sidecar. Litter removal, like
// removeKeyDigest: the table is derived, so a leftover one is only ever wrong
// about a file that no longer exists, and the size check refuses it anyway.
func removeLocalBlockTable(seg *segment) {
	os.Remove(localBlockTablePath(seg)) // nolint: errcheck
}
