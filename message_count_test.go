package commitlog

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// A SEGMENT'S RECORD COUNT IS A STORED FACT, NOT THE DISTANCE BETWEEN ITS ENDS.
//
// MessageCount had one branch that counted (a raw segment's dense index, one
// entry per message) and one that measured: lastOffset - firstOffset + 1, for
// every segment without such an index. The comment defending it called the
// result "an upper bound — acceptable for the retention heuristic that consumes
// it", and both halves of that sentence were wrong.
//
// It is not a heuristic. It is the term in a BUDGET — MaxLogMessages is a
// ceiling on a running total, and applyTotalLimit walks the segments oldest
// first deleting until the total fits. So an upper bound is not the safe side of
// anything: overstating each segment makes the walk reach the ceiling sooner,
// and it deletes MORE than it was asked to. "Upper bound" and "over-deletes" are
// the same sentence here.
//
// The span stops being the count the moment compaction drops a record out of the
// middle of it, because the holes keep their offsets. A compacted snappy log
// measured 381 by span against 138 records actually present — 2.76x. A caller
// asking to keep exactly what it had would have lost two thirds of it.
//
// So: how many records the segments say they hold, against how many are there.
func TestACompactedLogCountsItsRecordsRatherThanItsOffsetSpan(t *testing.T) {
	for _, codec := range []compress.Codec{compress.None, compress.Snappy, compress.Zstd} {
		t.Run(codec.String(), func(t *testing.T) {
			l, cleanup := setupWithOptions(t, compactedLogOptions(t, codec))
			defer cleanup()

			present := fillAndCompact(t, l)

			var reported int64
			for _, seg := range l.segmentsSnapshot() {
				reported += seg.MessageCount()
			}

			// The premise: compaction has to have LEFT GAPS, or the span and the
			// count agree and the assertion below passes on a log that could
			// never have shown the bug. The span over the whole log is what the
			// old code summed to.
			var span int64
			for _, seg := range l.segmentsSnapshot() {
				seg.RLock()
				if seg.lastOffset >= 0 {
					span += seg.lastOffset - seg.firstOffset + 1
				}
				seg.RUnlock()
			}
			require.Greater(t, span, present,
				"compaction left no offset gaps, so this fixture cannot tell a "+
					"stored count from an offset span")

			require.Equal(t, present, reported,
				"the segments report %d records between them and hold %d: retention "+
					"is a budget, so a count this far over deletes that much extra",
				reported, present)
		})
	}
}

// THE CONSEQUENCE, ASKED OF THE THING THAT CONSUMES IT.
//
// The count above is not read by anyone for its own sake — it is summed by
// applyTotalLimit against MaxLogMessages. So the assertion that matters is the
// one a caller would make: set the limit to exactly what the log holds, which is
// a request to keep all of it, and nothing may be deleted.
//
// Under the offset span this failed by construction: the log reported more
// records than existed, the walk saw a total over the ceiling, and it deleted
// the oldest segments to get under a limit the log was already inside.
func TestMessageRetentionKeepsALogThatIsExactlyAtItsLimit(t *testing.T) {
	dir := tempDir(t)
	opts := compactedLogOptions(t, compress.Snappy)
	opts.Path = dir

	l, err := New(opts)
	require.NoError(t, err)
	present := fillAndCompact(t, l.(*commitLog))
	oldest := l.OldestOffset()
	require.NoError(t, l.Close())

	// Reopened with the limit set to exactly what is there. Compaction has
	// already run, so this pass is the retention walk and nothing else.
	limited := opts
	limited.MaxLogMessages = present

	l2, err := New(limited)
	require.NoError(t, err)
	t.Cleanup(func() { l2.Close() })
	require.NoError(t, l2.Clean())

	require.Equal(t, oldest, l2.OldestOffset(),
		"a log holding exactly MaxLogMessages records had its oldest segment "+
			"deleted: the limit was measured against an overstated count")
	require.Equal(t, present, int64(len(readAll(t, l2))),
		"records went missing under a limit equal to the number of records")
}

// AN OFFLOADED SEGMENT ANSWERS FROM THE MANIFEST, WITHOUT FETCHING ANYTHING.
//
// This is the state the fix would be easiest to leave behind: a tiered block
// segment keeps its block table in the store and does not fetch it until the
// first READ, so at the moment tier retention asks for its count there is no
// index and no table resident — exactly the state the offset span used to be the
// fallback for.
//
// Carrying the count on the manifest entry is what closes it, and the property
// worth pinning is that asking costs no round trip: a retention pass over a cold
// tier must not download it. So the table stays pending across the call.
func TestATieredSegmentCountsWithoutFetchingItsBlockTable(t *testing.T) {
	fs, err := NewFileSegmentStore(tempDir(t))
	require.NoError(t, err)
	seg := offloadedBlockSegment(t, fs)

	seg.RLock()
	pending, first, last := seg.blocksPending, seg.firstOffset, seg.lastOffset
	seg.RUnlock()
	require.True(t, pending, "fixture gave a segment whose table is already loaded")

	require.Equal(t, last-first+1, seg.MessageCount(),
		"this segment was appended and never compacted, so its count and its "+
			"offset span agree — the fixture is wrong if they do not")

	// Which is exactly why the agreeing case proves nothing on its own, and the
	// count is moved off the span here. A compacted segment is the real case and
	// it cannot be built through a store fixture without offloading a compacted
	// log, so the state is set instead of reached: this is the field the manifest
	// entry filled in, and the assertion is that it — and not the two offsets
	// either side of it — is what comes back.
	seg.Lock()
	seg.records = 7
	seg.Unlock()
	require.Equal(t, int64(7), seg.MessageCount(),
		"a tiered segment with no index and no block table answered from its "+
			"offset span, which is the count compaction invalidates")

	seg.RLock()
	stillPending := seg.blocksPending
	seg.RUnlock()
	require.True(t, stillPending,
		"asking a cold tiered segment for its record count fetched its block "+
			"table: a retention pass over a tier nothing has read would download "+
			"every table in it")
}

// THE SAME COUNT, CARRIED BY THE MANIFEST INTO A DIRECTORY THAT NEVER HELD IT.
//
// The test above SETS s.records, because a gappy segment cannot be reached
// through a store fixture. It can be reached through an offload: compact first,
// then hand the compacted segments to a tier, and the count that travels is the
// one compaction left behind. Nothing here is poked — it leaves as
// offloadMeta.Records, is published in the manifest entry, and is read back by a
// process holding the STORE and not the directory the records were written in,
// which is the state a tier retention pass runs in.
//
// This is the assertion the bug would have failed at the layer it shipped in:
// the reopened log reports what it holds, rather than the distance between the
// offsets it kept.
func TestATieredCompactedLogReportsItsRecordCountFromTheManifest(t *testing.T) {
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	// The cache is what selects option 2 — index offloaded ALONGSIDE the log —
	// and option 2 is the state this is about. With the index left local
	// (option 1), openOffloadedSegment has to run setupIndex against a local
	// file that a crash could have torn, and that fetches the block table on the
	// spot; the count would then come from the resident blocks and the manifest
	// field would never be read. Option 2 reads nothing at all on open, which is
	// both the cheaper path and the only one where the manifest is the sole
	// authority. See openOffloadedSegment.
	cache, err := NewRemoteIndexCache(filepath.Join(dir, "idxcache"), 1<<30)
	require.NoError(t, err)
	t.Cleanup(func() { cache.Close() })

	opts := compactedLogOptions(t, compress.Snappy)
	opts.Path = dir
	opts.Tiers = oneTier(store)
	opts.RemoteIndexCache = cache

	l, err := New(opts)
	require.NoError(t, err)
	fillAndCompact(t, l.(*commitLog))
	n, err := l.OffloadBefore(l.NewestOffset())
	require.NoError(t, err)
	require.Positive(t, n, "the fixture needs offloaded segments to have a manifest to read")
	require.NoError(t, l.Close())

	// A different directory over the same store: no index, no block table, no
	// local segment. Whatever it says about its size, it says from the manifest.
	adopting := opts
	adopting.Path = tempDir(t)
	l2, err := New(adopting)
	require.NoError(t, err)
	t.Cleanup(func() { l2.Close() })

	segments := l2.(*commitLog).segmentsSnapshot()
	require.NotEmpty(t, segments, "the adopting log picked up no segments")

	var reported, span int64
	var fetched []int
	for i, seg := range segments {
		reported += seg.MessageCount()
		seg.RLock()
		if seg.lastOffset >= 0 {
			span += seg.lastOffset - seg.firstOffset + 1
		}
		if !seg.blocksPending {
			fetched = append(fetched, i)
		}
		seg.RUnlock()
	}

	present := int64(len(readAll(t, l2)))
	require.Greater(t, span, present,
		"the offloaded segments have no offset gaps, so this fixture cannot tell "+
			"a manifest count from an offset span")
	require.Equal(t, present, reported,
		"a log reopened over its tier reported %d records and holds %d: the "+
			"manifest's count was not what answered", reported, present)
	// Exactly one table is fetched, the NEWEST segment's — the log establishes
	// its own tail there, and that is the one segment an adopting process cannot
	// treat as merely historical. The other 47 stay unfetched, which is the
	// property that makes the manifest count worth carrying: they are counted
	// without being downloaded.
	//
	// Asserted as the exact set rather than as "not all of them". A regression
	// that fetched a second table would satisfy any weaker form of this while
	// putting the per-segment round trip back.
	require.Equal(t, []int{len(segments) - 1}, fetched,
		"the tables fetched during open were %v, not just the newest segment's: "+
			"the counts above may have come from resident blocks rather than "+
			"from the manifest, and a retention pass over a tier nothing has "+
			"read would download every table it touched", fetched)
}

// A BLOCK TABLE THAT DISAGREES WITH THE MANIFEST ABOUT THE COUNT IS REFUSED.
//
// Fetching the table is the moment a tiered segment stops answering
// MessageCount from the manifest entry and starts answering it from the blocks.
// If the two were allowed to disagree, a segment's record count would CHANGE the
// first time somebody read it — the retention pass before the read and the one
// after would be measuring different logs, and nothing would say which was
// right.
//
// They cannot disagree honestly: the same offload wrote both, from the same
// segment. So a disagreement means one of them is describing something else, and
// that is refused for the same reason the extents beside it already are.
func TestABlockTableThatDisagreesWithTheManifestCountIsRefused(t *testing.T) {
	fs, err := NewFileSegmentStore(tempDir(t))
	require.NoError(t, err)
	seg := offloadedBlockSegment(t, fs)

	seg.Lock()
	seg.records++ // one more than the object holds
	seg.Unlock()

	err = seg.ensureBlocksLoaded()
	require.Error(t, err,
		"a block table accounting for a different number of records than the "+
			"manifest entry beside it was accepted")
	require.Contains(t, err.Error(), "records",
		"the refusal must name what disagreed, or it reads as an outage")
}

// compactedLogOptions is a log that will actually roll and actually compact.
//
// MaxSegmentBytes has to be small enough that the appends below ROLL — a single
// segment is the active one, compaction skips it, and the fixture then measures
// a log nothing rewrote. DisableAutoClean keeps the only compaction the one this
// test asks for, so the counts are read at a known point rather than whenever a
// ticker last fired.
func compactedLogOptions(t *testing.T, codec compress.Codec) Options {
	t.Helper()
	return Options{
		Path:             tempDir(t),
		MaxSegmentBytes:  512,
		Compact:          true,
		Compression:      codec,
		DisableAutoClean: true,
	}
}

// fillAndCompact writes a key set that compaction has to REWRITE rather than
// delete, runs one pass, and returns how many records are readable afterwards.
//
// The survivors are what make it a rewrite. A segment whose keys are all
// superseded is deleted whole, and a log made only of those has no gaps in it
// afterwards — every remaining segment is one nothing touched. Mixing a unique
// key into every fourth record leaves each segment with something to keep, so
// the rewritten segments carry the holes the churn left behind.
func fillAndCompact(t *testing.T, l *commitLog) int64 {
	t.Helper()
	var last int64
	for i := 0; i < 240; i++ {
		key := []byte(fmt.Sprintf("hot-%d", i%3))
		if i%4 == 0 {
			key = []byte(fmt.Sprintf("unique-%d", i))
		}
		offs, err := l.Append([]*Message{{
			Key:   key,
			Value: []byte("a value long enough to roll the segment reasonably often"),
		}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)
	require.NoError(t, l.Clean())

	present := int64(len(readAll(t, l)))
	require.Positive(t, present, "compaction left nothing to count")
	return present
}
