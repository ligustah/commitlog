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
		{logicalStart: 0, logicalLen: 4096, physStart: 0, physLen: 1200, codec: compress.Snappy},
		{logicalStart: 4096, logicalLen: 8192, physStart: 1200, physLen: 2400, codec: compress.Snappy},
		{logicalStart: 12288, logicalLen: 100, physStart: 3600, physLen: 120, codec: compress.None},
	}
	out, err := decodeBlockTable(encodeBlockTable(in))
	require.NoError(t, err)
	require.Equal(t, in, out)

	logical, phys := blockTableExtent(out)
	require.Equal(t, int64(12388), logical)
	require.Equal(t, int64(3720), phys)

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
		{logicalStart: 0, logicalLen: 4096, physStart: 0, physLen: 1200, codec: compress.Snappy},
		{logicalStart: 4096, logicalLen: 8192, physStart: 1200, physLen: 2400, codec: compress.Snappy},
	})

	corrupt := func(mut func([]byte) []byte) []byte {
		b := make([]byte, len(good))
		copy(b, good)
		return mut(b)
	}

	for name, body := range map[string][]byte{
		"truncated to nothing": nil,
		"header only":          good[:blockTableHeaderLen],
		"wrong magic":          corrupt(func(b []byte) []byte { b[0] = 'X'; return b }),
		"unknown version":      corrupt(func(b []byte) []byte { b[1] = 9; return b }),
		"count says more than the object holds": corrupt(func(b []byte) []byte {
			encoding.PutUint32(b[2:], 99)
			return b
		}),
		"a flipped length, crc left alone": corrupt(func(b []byte) []byte {
			b[blockTableHeaderLen] ^= 0xff
			return b
		}),
		"trailing byte": append(append([]byte{}, good...), 0),
		"a block shorter than its own header": func() []byte {
			b := encodeBlockTable([]blockRef{
				{logicalLen: 10, physLen: 1, codec: compress.None},
			})
			return b
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := decodeBlockTable(body)
			require.ErrorIs(t, err, ErrBlockTableFormat,
				"a damaged block table must be refused, never approximated")
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
