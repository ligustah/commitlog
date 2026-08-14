package commitlog

import (
	"fmt"
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
