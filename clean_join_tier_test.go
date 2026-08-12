package commitlog

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// The tier a joined run lives in throughout this file. Deliberately not
// defaultTierName: a hard-coded tier name has already shipped once in this
// package (see TestNoManifestAPassPublishesEverDropsALiveSegment), and the join
// adds another publish that has to get the name from the segment.
const joinTierName = "hot"

// tieredJoinSpec joins runs in that tier and leaves local segments alone, which
// is also the assertion that TierJoinBelow does not fall back to JoinBelow.
var tieredJoinSpec = CleanSpec{TierJoinBelow: map[string]int64{joinTierName: 8 << 10}}

// tieredJoinFixture builds a log whose sealed segments have all been offloaded
// into one tier, and returns it with the manifest recorder over that tier's
// store and every record it holds.
func tieredJoinFixture(t *testing.T, records int) (*commitLog, *manifestRecorder, map[int64]SerializedMessage) {
	t.Helper()
	dir := tempDir(t)
	backing, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)
	store := &manifestRecorder{SegmentStore: backing}

	l, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  2 << 10,
		Tiers:            []Tier{{Name: joinTierName, Store: store}},
		DisableAutoClean: true,
	})
	t.Cleanup(cleanup)

	for i := range records {
		_, err := l.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("key-%04d", i%16)),
			Value: []byte(fmt.Sprintf("value-%04d-padding-padding-padding", i)),
		}})
		require.NoError(t, err)
	}
	l.SetHighWatermark(l.NewestOffset())

	n, err := l.OffloadBefore(l.NewestOffset())
	require.NoError(t, err)
	require.Positive(t, n, "the fixture needs offloaded segments")

	offloaded := offloadedBaseOffsets(l)
	require.Greater(t, len(offloaded), 4, "the fixture needs several offloaded segments to join")
	return l, store, readAllMsgs(t, l)
}

// The tiered contract, and the same one the local path has: every record of
// every input is in the result, and the log serves them.
//
// Worth stating why this is not just the local test with a store attached. The
// local install is a rename, so the result IS the first input by the time
// anything looks. Here the result is an object the first input starts serving,
// and the records only become readable if the upload, the manifest write and the
// repoint all agree about which object that is.
func TestATieredJoinCarriesEveryRecordOfTheRun(t *testing.T) {
	l, _, before := tieredJoinFixture(t, 400)

	pre := liveSegments(l)
	runs := planJoins(pre, tieredJoinSpec)
	require.NotEmpty(t, runs, "the fixture must give the planner something to join")
	for _, r := range runs {
		require.True(t, r.tiered, "the fixture's runs must be tiered")
		require.Equal(t, joinTierName, r.tier)
	}

	_, err := l.CleanWithSpec(tieredJoinSpec)
	require.NoError(t, err)

	after := readAllMsgs(t, l)
	require.Equal(t, len(before), len(after), "the join changed how many records the log holds")
	for off, want := range before {
		require.Equal(t, want, after[off], "record at offset %d did not survive the join", off)
	}
	require.Less(t, len(liveSegments(l)), len(pre), "the pass joined nothing")
}

// Hazard 2 in its tiered form: a store has no rename, so the commit is a
// manifest write — and it has to be ONE write that starts naming the result and
// stops naming every input together.
//
// The assertion is on every manifest the pass publishes, not the one it settles
// on. Each must describe a log whose records are all reachable: an offset may
// never be named by two entries (an input and the result that absorbed it), and
// never by none. A two-write commit would show up here as a manifest in between
// that violates one or the other, which is exactly the state a crash would
// preserve.
func TestATieredJoinCommitsInOneManifestWrite(t *testing.T) {
	l, store, _ := tieredJoinFixture(t, 400)

	pre := liveSegments(l)
	runs := planJoins(pre, tieredJoinSpec)
	require.NotEmpty(t, runs)

	// Every input of every run, the base offset its result keeps, and the offset
	// the result has to reach before the inputs above it may stop being named.
	//
	// That last one is what makes this test about RECORDS rather than about
	// bookkeeping. Base offsets alone cannot tell the states apart: the result
	// keeps the first input's, so an entry naming the pre-join object and one
	// naming the joined object are the same number. A manifest that had already
	// retired inputs 2..N while that entry still described the first input's
	// object would look perfectly consistent and would have lost every record
	// above it.
	type run struct {
		result  int64
		through int64
		inputs  []int64
	}
	var planned []run
	for _, r := range runs {
		cur := run{result: pre[r.first].BaseOffset, through: segLastOffset(pre[r.last])}
		for i := r.first; i <= r.last; i++ {
			cur.inputs = append(cur.inputs, pre[i].BaseOffset)
		}
		planned = append(planned, cur)
	}

	store.record(true)
	_, err := l.CleanWithSpec(tieredJoinSpec)
	require.NoError(t, err)

	published := store.published()
	require.NotEmpty(t, published, "the pass must have published at least one manifest")

	// Once a run has been committed, no later manifest in the same pass may name
	// its inputs again. A pass joins several runs, and a segment stays in
	// l.segments until the splice at the very end — so a later run's commit,
	// which rebuilds the manifest from the log's own view, will republish an
	// earlier run's retired inputs unless retiring them took them out of that
	// view. Resurrecting an entry for objects already queued for reclamation is
	// how a manifest comes to name bytes that are about to be deleted.
	retiredBy := make(map[int64]int)

	for i, m := range published {
		named := make(map[int64]TierObject, len(m))
		for _, o := range m {
			named[o.BaseOffset] = o
		}
		for base, at := range retiredBy {
			_, back := named[base]
			require.Falsef(t, back,
				"manifest %d of %d names base offset %d again, which manifest %d had "+
					"already retired; its objects are queued for reclamation",
				i+1, len(published), base, at)
		}
		for _, r := range planned {
			// The result's base offset is the run's first input's, so "the result
			// is named" and "the first input is named" are the same fact — which
			// is precisely what makes the commit a replace rather than an add.
			// The question is only ever about the inputs ABOVE it.
			var present, absent []int64
			for _, in := range r.inputs[1:] {
				if _, ok := named[in]; ok {
					present = append(present, in)
				} else {
					absent = append(absent, in)
				}
			}
			require.Truef(t, len(present) == 0 || len(absent) == 0,
				"manifest %d of %d caught the run at %d half-committed: it still names "+
					"%v but has already dropped %v; the commit must be one write",
				i+1, len(published), r.result, present, absent)

			survivor, ok := named[r.result]
			require.Truef(t, ok,
				"manifest %d of %d does not name the run's surviving base offset %d, "+
					"so the records of %v are named by nothing",
				i+1, len(published), r.result, r.inputs)

			// The real invariant: whatever this manifest stops naming, the entry
			// that survives must already cover. Checked in offsets rather than in
			// keys so it fails for the RIGHT reason — an unreachable record —
			// rather than because a key changed.
			if len(absent) > 0 {
				require.GreaterOrEqualf(t, survivor.LastOffset, r.through,
					"manifest %d of %d has dropped %v from the run at %d while the entry "+
						"that replaces them only reaches offset %d of %d; those records "+
						"are named by nothing",
					i+1, len(published), absent, r.result, survivor.LastOffset, r.through)
				for _, base := range absent {
					if _, seen := retiredBy[base]; !seen {
						retiredBy[base] = i + 1
					}
				}
			}
		}
	}
}

// A joined-away input's objects are QUEUED for reclamation, not deleted.
//
// segment.Delete would have been the obvious way to retire them and goes
// straight to store.Delete — and the objects a join absorbs are exactly the ones
// a scanner may still be mid-read of, since the join itself holds every input's
// backing open until after the install. Deleting one there turns a join into a
// read error for anyone who started before it.
//
// So the assertion is that the objects OUTLIVE the pass. drainReclaim runs at
// the start of a later pass and only once nothing holds the pin, which is what
// makes the deferral safe rather than a leak.
func TestATieredJoinQueuesItsInputsObjectsRatherThanDeletingThem(t *testing.T) {
	l, store, _ := tieredJoinFixture(t, 400)

	pre := liveSegments(l)
	runs := planJoins(pre, tieredJoinSpec)
	require.NotEmpty(t, runs)

	// The log objects of every input a run absorbs — the ones above the first,
	// which are the segments that cease to exist.
	var absorbed []string
	for _, r := range runs {
		for i := r.first + 1; i <= r.last; i++ {
			pre[i].RLock()
			absorbed = append(absorbed, pre[i].storeKey)
			pre[i].RUnlock()
		}
	}
	require.NotEmpty(t, absorbed, "the fixture must have inputs to absorb")

	_, err := l.CleanWithSpec(tieredJoinSpec)
	require.NoError(t, err)

	present, err := store.List()
	require.NoError(t, err)
	held := make(map[string]bool, len(present))
	for _, k := range present {
		held[k] = true
	}
	for _, key := range absorbed {
		require.Truef(t, held[key],
			"the join deleted %s outright; a reader that took its backing before the "+
				"join is still on it, and the reclaim queue is what waits for them", key)
	}
}

// A join whose commit is refused must queue NOTHING for reclamation.
//
// uploadReplacement hands back the first input's CURRENT objects, and they only
// stop being current when swapReplacement repoints the segment away from them.
// Returning them alongside the error queues live bytes for deletion behind a
// segment that is still serving them — and drainReclaim will oblige, because its
// safety argument ("for a superseded backing, refs can only fall") holds only for
// entries that a swap put there. Nothing was swapped here, so a refcount of zero
// is an ordinary lull rather than a terminal state, and the delete lands on the
// object the log reads from.
//
// The failure is bounded to the commit itself so the pass's end-of-pass
// republish still lands: a failed republish sets tierManifestStale, which stops
// the queue draining at all and would hide the bug instead of exposing it.
func TestATieredJoinThatCannotCommitQueuesNothing(t *testing.T) {
	l, store, before := tieredJoinFixture(t, 400)

	pre := liveSegments(l)
	require.NotEmpty(t, planJoins(pre, tieredJoinSpec), "the fixture must plan runs")

	// Every object the log is serving at the moment the join starts. All of them,
	// not just the runs' — a pass that deletes any of these has lost records.
	live := make(map[string]bool)
	for _, s := range pre {
		s.RLock()
		if s.storeKey != "" {
			live[s.storeKey] = true
		}
		s.RUnlock()
	}
	require.NotEmpty(t, live, "the fixture needs offloaded segments")

	store.failNext(1)
	_, err := l.CleanWithSpec(tieredJoinSpec)
	require.Error(t, err, "the join's commit was refused, so the pass must report it")

	// The queue is drained directly rather than by another pass: the point is
	// what the failed run put there, and a second pass would join successfully
	// and supersede objects for real.
	l.drainReclaim()

	present, err := store.List()
	require.NoError(t, err)
	held := make(map[string]bool, len(present))
	for _, k := range present {
		held[k] = true
	}
	for key := range live {
		require.Truef(t, held[key],
			"the failed join queued %s for reclamation; the segment never repointed "+
				"away from it, so the log is still reading it", key)
	}
	require.Equal(t, before, readAllMsgs(t, l),
		"the log lost records to a join that never committed")
}

// segLastOffset is the highest offset a segment holds.
func segLastOffset(s *segment) int64 {
	s.RLock()
	defer s.RUnlock()
	return s.lastOffset
}

// A tier this log does not own takes no writes, manifest or object, and a join
// is a write like any other. Leaving a read-only tier out of TierJoinBelow is
// how it stays untouched by default — but absence only refuses the callers who
// did not ask, and this one asks.
func TestATieredJoinRefusesATierThisLogDoesNotOwn(t *testing.T) {
	l, _, before := tieredJoinFixture(t, 400)

	pre := liveSegments(l)
	require.NotEmpty(t, planJoins(pre, tieredJoinSpec), "the fixture must plan runs")

	require.NoError(t, l.SetTierReadOnly(joinTierName, true))

	_, err := l.CleanWithSpec(tieredJoinSpec)
	require.NoError(t, err, "a refused join is not a failed pass")

	require.Equal(t, len(pre), len(liveSegments(l)),
		"the pass joined segments in a tier this log does not own")
	require.Equal(t, before, readAllMsgs(t, l))
}
