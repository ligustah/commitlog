package commitlog

// The offload analogue of FuzzCompactionRecovery: a seeded sweep over the
// interactions FuzzOffloadCrashConsistency deliberately scoped out — offload
// interleaved with compaction (CleanWithSpec) and retention (TruncateBefore),
// across crash and reopen.
//
// A sibling target rather than more entropy inside FuzzOffloadCrashConsistency:
// that one asserts read transparency across a crash, this one asserts
// latest-per-key survival across a lifecycle, and a shared entropy stream would
// halve the workload each of them drives. Separate targets also keep each
// corpus meaningful.
//
// Verified against the code, per the interactions worth covering. Two of these
// premises were TRUE WHEN WRITTEN and are no longer, which is worth recording
// rather than quietly deleting — the assertions still hold, but for different
// reasons:
//   - compaction used to skip offloaded segments entirely, so latest-per-key
//     held across a partially-offloaded log because the store bytes never
//     moved. It now rewrites them as a new generation of their objects, so the
//     same assertion is testing something strictly harder: that the survivors
//     are right ACROSS a rewrite, not merely preserved by not doing one.
//   - RemoteIndexCache used to have no invalidate-by-key, which was safe only
//     because an offloaded segment was never rewritten in place: a dead entry
//     could occupy budget but never be served. It now has one, and a rewrite
//     calls it — so this target also exercises that path.
//   - segment.Delete removes BOTH store objects (log + index), so retention
//     over offloaded segments must not orphan objects — asserted here against
//     store.List() rather than assumed. Still true, and now load-bearing for
//     superseded generations too.

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fzOffCompSetup is fzOffloadSetup plus compaction enabled — the cleaners are
// still driven explicitly by the harness, never by the internal loop.
func fzOffCompSetup(t *testing.T, s *fzStream) (*commitLog, Options, *fzFaultStore) {
	cl, opts, store := fzOffloadSetup(t, s)
	require.NoError(t, cl.Close())
	// Turning compaction on for a log created without it is a retune of what the
	// log keeps, so it needs the explicit opt-in — the harness is doing on
	// purpose the thing the descriptor exists to stop happening by accident.
	opts.Compact = true
	opts.AdoptOptions = true
	l, err := New(opts)
	require.NoError(t, err)
	return l.(*commitLog), opts, store
}

// fzStoreBaseOffsets returns the base offsets the store currently holds objects
// for, so a leaked object outlives its segment visibly.
func fzStoreBaseOffsets(t *testing.T, store *fzFaultStore, cl *commitLog) map[int64]bool {
	t.Helper()
	keys, err := store.List()
	require.NoError(t, err)

	// Objects the log has queued for reclamation are expected to outlive their
	// segment for a while: a rewrite supersedes one, retention may then drop the
	// segment entirely, and the superseded object waits for a later pass. It is
	// accounted for rather than lost, which fzNoQueuedLeak proves by draining it.
	queued := map[string]bool{}
	cl.tierMu.Lock()
	for _, e := range cl.reclaim {
		queued[e.key] = true
	}
	cl.tierMu.Unlock()

	out := map[int64]bool{}
	for _, k := range keys {
		// The store holds objects that describe the log alongside the ones that
		// hold it; `ParseInt` on a descriptive key is a spurious failure.
		if isStoreMetaKey(k) || queued[k] {
			continue
		}
		stem := k
		if i := strings.IndexByte(stem, '.'); i >= 0 {
			stem = stem[:i]
		}
		off, err := strconv.ParseInt(stem, 10, 64)
		require.NoError(t, err, "unparseable store key %q", k)
		out[off] = true
	}
	return out
}

// fzNoQueuedLeak drains the reclamation queue and proves it empties: every
// object the log deferred is deleted, and no object survives without a segment.
//
// The fault store can refuse a delete, and a refused one stays queued by design
// — so this retries a bounded number of passes rather than demanding the first
// one succeed.
func fzNoQueuedLeak(t *testing.T, cl *commitLog, store *fzFaultStore) {
	t.Helper()
	queued := func() int {
		cl.tierMu.Lock()
		defer cl.tierMu.Unlock()
		return len(cl.reclaim)
	}
	for i := 0; i < 8 && queued() > 0; i++ {
		hw := cl.HighWatermark()
		if _, err := cl.CleanWithSpec(CleanSpec{
			Ceiling: At(hw + 1), TombstoneGCBelow: hw + 1, TombstoneRetention: time.Hour,
		}); err != nil {
			return // an injected fault; the pass's own assertions cover that
		}
	}
	if queued() > 0 {
		return // still faulting; not this check's business
	}
	for base := range fzStoreBaseOffsets(t, store, cl) {
		found := false
		cl.mu.RLock()
		for _, sg := range cl.segments {
			if sg.BaseOffset == base {
				found = true
				break
			}
		}
		cl.mu.RUnlock()
		require.True(t, found,
			"after draining, the store still holds an object for base offset %d "+
				"with no such segment — the deferred reclamation leaked it", base)
	}
}

func FuzzOffloadCompactionRetention(f *testing.F) {
	f.Add([]byte{1, 0, 2, 1, 3, 0, 1, 2})
	f.Add([]byte{0, 3, 1, 2, 0, 1, 3, 2, 1})
	f.Add([]byte{2, 2, 1, 0, 3, 3, 0, 1, 2, 3})
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1})

	f.Fuzz(func(t *testing.T, data []byte) {
		s := &fzStream{b: data}
		cl, opts, store := fzOffCompSetup(t, s)
		closed := false
		t.Cleanup(func() {
			if !closed {
				cl.Close()
			}
		})

		// latest[key] = the offset and value of the key's newest committed write.
		// A key survives compaction iff that offset is still above the log's
		// oldest offset; below it, retention is entitled to have removed it.
		type latest struct {
			off int64
			val string
		}
		live := map[string]latest{}
		valc := 0
		app := func() {
			valc++
			k := fmt.Sprintf("k%d", s.intn(4))
			v := fmt.Sprintf("v%d", valc)
			o, err := cl.Append([]*Message{{Key: []byte(k), Value: []byte(v)}})
			require.NoError(t, err)
			cl.SetHighWatermark(o[0])
			live[k] = latest{off: o[0], val: v}
		}

		// The combined oracle: every key whose newest write still sits at or
		// above the oldest surviving offset must be readable with exactly that
		// value, whatever mixture of local and offloaded segments holds it.
		assertOracle := func(l *commitLog, stage string) {
			got := fzReadAll(t, l)
			oldest := l.OldestOffset()
			// Keep the HIGHEST-offset copy per key: got is keyed by offset and
			// map iteration is unordered, so taking whatever comes last would
			// compare against an arbitrary superseded copy.
			seenOff := map[string]int64{}
			seen := map[string]string{}
			for off, m := range got {
				k := string(m.Key())
				if k == "" {
					continue
				}
				if prev, ok := seenOff[k]; !ok || off > prev {
					seenOff[k], seen[k] = off, string(m.Value())
				}
			}
			for k, want := range live {
				if want.off < oldest {
					continue // legitimately truncated away
				}
				gotVal, ok := seen[k]
				require.True(t, ok, "%s: key %q lost (newest write at %d, oldest %d)",
					stage, k, want.off, oldest)
				require.Equal(t, want.val, gotVal, "%s: key %q has a stale value", stage, k)
			}
		}

		for ops := 4 + s.intn(20); ops > 0; ops-- {
			app()
		}
		require.NoError(t, cl.SyncAll())
		assertOracle(cl, "initial")

		// Interleave offload, compaction and retention over several cycles.
		for cycles := 1 + s.intn(3); cycles > 0; cycles-- {
			newest := cl.NewestOffset()

			switch s.intn(4) {
			case 0: // offload a prefix
				if newest > 0 {
					_, _ = cl.OffloadBefore(int64(s.intn(int(newest) + 1)))
				}
			case 1: // compact
				hw := cl.HighWatermark()
				_, err := cl.CleanWithSpec(CleanSpec{
					Ceiling:            At(hw + 1),
					TombstoneGCBelow:   hw + 1,
					TombstoneRetention: time.Hour,
				})
				require.NoError(t, err)
				// Nothing to delete here. The log reclaims the generations its
				// own rewrites supersede, on a later pass once no reader holds
				// them. This harness used to do it by hand, which is precisely
				// the caller obligation that turned out to be misplaced: the
				// caller cannot know when a reader has finished, and the log can.
			case 2: // retain (drop a prefix)
				if newest > 1 {
					require.NoError(t, cl.TruncateBefore(int64(1+s.intn(int(newest)))))
				}
			case 3: // more writes
				for extra := 1 + s.intn(6); extra > 0; extra-- {
					app()
				}
			}

			assertOracle(cl, "mid-cycle")

			// No store object may outlive the segment it belonged to: Delete
			// removes the log and index objects together, so anything the store
			// still holds below the oldest surviving offset is a leak.
			oldest := cl.OldestOffset()
			for base := range fzStoreBaseOffsets(t, store, cl) {
				found := false
				cl.mu.RLock()
				for _, sg := range cl.segments {
					if sg.BaseOffset == base {
						found = true
						break
					}
				}
				cl.mu.RUnlock()
				require.True(t, found,
					"store holds an object for base offset %d with no such segment (oldest=%d)",
					base, oldest)
			}
		}

		// The queue is a deferral, not a dustbin. With no reader holding
		// anything, a pass must drain it completely and leave nothing behind —
		// which is what makes exempting queued objects above a postponement of
		// the leak check rather than a hole in it.
		fzNoQueuedLeak(t, cl, store)

		// Crash and reopen: offloaded segments come back from the store, the
		// local tail recovers, and the surviving committed state is unchanged.
		before := fzReadAll(t, cl)
		beforeOldest := cl.OldestOffset()
		require.NoError(t, cl.Close())
		closed = true

		l2, err := New(opts)
		require.NoError(t, err)
		defer l2.Close()
		cl2 := l2.(*commitLog)
		require.NoError(t, cl2.RecoverTail())

		require.Equal(t, beforeOldest, cl2.OldestOffset(), "reopen moved the oldest offset")
		require.Equal(t, before, fzReadAll(t, cl2), "reopen changed the surviving records")
		assertOracle(cl2, "reopen")
	})
}
