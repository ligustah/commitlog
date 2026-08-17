package commitlog

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// A block table survives the round trip, including the starts it does not store.
//
// The starts are the part worth asserting: the format holds only per-block
// LENGTHS and rebuilds every start by accumulation, so this is the test that
// says the accumulation agrees with what scanBlocks produced in the first place.
func TestABlockTableRoundTripsItsDerivedStarts(t *testing.T) {
	in := []blockRef{
		{logicalStart: 0, logicalLen: 4096, physStart: 0, physLen: 1200, codec: compress.Snappy, records: 12},
		{logicalStart: 4096, logicalLen: 8192, physStart: 1200, physLen: 2400, codec: compress.Snappy, records: 30},
		{logicalStart: 12288, logicalLen: 100, physStart: 3600, physLen: 120, codec: compress.None, records: 1},
	}
	out, err := decodeBlockTable(encodeBlockTable(in))
	require.NoError(t, err)
	require.Equal(t, in, out)

	logical, phys := blockTableExtent(out)
	require.Equal(t, int64(12388), logical)
	require.Equal(t, int64(3720), phys)
	require.Equal(t, int64(43), sumBlockRecords(out))

	empty, err := decodeBlockTable(encodeBlockTable(nil))
	require.NoError(t, err)
	require.Empty(t, empty)
}

// Every way a block table can be wrong is REFUSED, and none of them falls back
// to rebuilding the table by walking the object.
//
// The fallback is the tempting thing to write and the thing that must not
// exist. Walking the header chain is a read of the whole object — the cost this
// format exists to remove — so a fallback would turn "the table is damaged" into
// a slow success nobody ever notices, and put the tier back on the boot path
// through a branch only the damaged case reaches.
//
// A wrong table is also not a degraded one. It maps logical offsets onto the
// wrong bytes, so the segment answers reads with plausible garbage rather than
// with an error.
func TestADamagedBlockTableIsRefused(t *testing.T) {
	good := encodeBlockTable([]blockRef{
		{logicalStart: 0, logicalLen: 4096, physStart: 0, physLen: 1200, codec: compress.Snappy, records: 12},
		{logicalStart: 4096, logicalLen: 8192, physStart: 1200, physLen: 2400, codec: compress.Snappy, records: 30},
	})

	corrupt := func(mut func([]byte) []byte) []byte {
		b := make([]byte, len(good))
		copy(b, good)
		return mut(b)
	}

	// `want` names WHICH refusal, and it is not decoration. decodeBlockTable
	// refuses in six ordered steps and every one of them is ErrBlockTableFormat,
	// so ErrorIs alone cannot tell them apart — nine cases could collapse onto
	// three checks and this test would stay green. That is not hypothetical
	// here: a v1 table trips the version check AND the exact-length check,
	// because a v1 entry is 9 bytes against v2's 13, which is the whole reason
	// TestAV1BlockTableIsRefusedByItsVersion has to assert its message too.
	//
	// Two pairs below deliberately share a check — a nil body and a header-only
	// body are both too short, and a bogus count and a trailing byte are both
	// size-vs-count. Sharing on purpose is fine; drifting onto a shared check by
	// accident is what this catches.
	for name, tc := range map[string]struct {
		body []byte
		want string
	}{
		"truncated to nothing": {nil, "block table is 0 bytes"},
		"header only":          {good[:blockTableHeaderLen], "block table is 6 bytes"},
		"wrong magic":          {corrupt(func(b []byte) []byte { b[0] = 'X'; return b }), "magic 0x58"},
		"unknown version":      {corrupt(func(b []byte) []byte { b[1] = 9; return b }), "version 9, want"},
		"count says more than the object holds": {corrupt(func(b []byte) []byte {
			encoding.PutUint32(b[2:], 99)
			return b
		}), "99 blocks need"},
		"a flipped length, crc left alone": {corrupt(func(b []byte) []byte {
			b[blockTableHeaderLen] ^= 0xff
			return b
		}), "crc "},
		"trailing byte": {append(append([]byte{}, good...), 0), "object is 37"},
		"a block shorter than its own header": {encodeBlockTable([]blockRef{
			{logicalLen: 10, physLen: 1, codec: compress.None, records: 1},
		}), "shorter than a header"},
		// No block holds no records, so a zero is a field nobody wrote — and it
		// is the one damaged value that reads LOW, which makes a retention walk
		// keep what it was told to drop rather than refuse to decode.
		"a block claiming no records": {encodeBlockTable([]blockRef{
			{logicalLen: 10, physLen: blockHeaderLen, codec: compress.None},
		}), "claims no records"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := decodeBlockTable(tc.body)
			require.ErrorIs(t, err, ErrBlockTableFormat,
				"a damaged block table must be refused, never approximated")
			require.Contains(t, err.Error(), tc.want,
				"refused by a different check than this case is named for. All "+
					"six refusals are ErrBlockTableFormat, so an earlier one "+
					"answering this fixture looks exactly like the fixture working")
		})
	}
}

// A manifest whose entry claims block compression but names no block table is
// refused, and so is the reverse.
//
// This is the same shape as the unknown-codec refusal: the check belongs where
// the value ARRIVES, because past this point BlockMode and BlocksKey are read by
// different code at different times, and the pair being inconsistent stops
// looking like a manifest problem. The failure it prevents is a block segment
// with nowhere to get its table, which can only be served by rebuilding one.
func TestAManifestEntryPairsBlockModeWithABlockTable(t *testing.T) {
	entry := func(mode bool, blocks string) TierObject {
		return TierObject{
			BaseOffset: 0, Tier: defaultTierName,
			LogKey:    "00000000000000000000.aa.log",
			BlockMode: mode, BlocksKey: blocks,
			LastOffset: 5, PhysPosition: 128, Position: 128,
		}
	}
	for name, tc := range map[string]struct {
		obj     TierObject
		refused bool
	}{
		"block mode, no table":   {entry(true, ""), true},
		"raw, but named a table": {entry(false, "00000000000000000000.aa.blocks"), true},
		"block mode with table":  {entry(true, "00000000000000000000.aa.blocks"), false},
		"raw with none":          {entry(false, ""), false},
		"a table outside the store": {
			entry(true, "../../etc/passwd"), true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			store, err := NewFileSegmentStore(filepath.Join(tempDir(t), "store"))
			require.NoError(t, err)
			body, err := json.Marshal(tierManifest{
				Version: manifestVersion, Segments: []TierObject{tc.obj},
			})
			require.NoError(t, err)
			require.NoError(t, store.Put(manifestKey, bytes.NewReader(body), int64(len(body))))

			_, err = readTierManifest(store)
			if tc.refused {
				require.Error(t, err, "the manifest names a segment nothing can read")
				return
			}
			require.NoError(t, err)
		})
	}
}
