package commitlog

// A seeded property/fuzz harness for the tiered-storage offload path
// (OffloadBefore -> FileSegmentStore + RemoteIndexCache, options 1 & 2),
// complementing Offload.tla (which proved the abstract transparency/pin
// invariants) by checking the REAL bytes at REAL crash points. Same shape and
// guardrails as FuzzCompactionRecovery: a []byte entropy stream drives the
// workload, plain `go test` replays the seed corpus, `-fuzz` runs the sweep,
// and a failing input persists to testdata/fuzz.
//
// Phase 1 (this file): offload read-transparency + reopen. Crash injection
// (fault-injecting store + marker/local-file mutation) follows.

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fzOffloadSetup builds a log wired to a filesystem SegmentStore, with the
// index cache enabled (option 2) or not (option 1) per the entropy stream.
func fzOffloadSetup(t *testing.T, s *fzStream) (*commitLog, Options) {
	base := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(base, "store"))
	require.NoError(t, err)

	opts := Options{
		Path:                 filepath.Join(base, "log"),
		MaxSegmentBytes:      64, // roll constantly so there are sealed segments to offload
		SegmentStore:         store,
		DisableAutoClean:     true,
		HWCheckpointInterval: time.Hour,
		CleanerInterval:      time.Hour,
	}
	if s.bool() {
		// Option 2: offload the index too, served from a small (eviction-heavy)
		// process-wide LRU cache.
		cache, err := NewRemoteIndexCache(filepath.Join(base, "idxcache"), 4<<10)
		require.NoError(t, err)
		opts.RemoteIndexCache = cache
	}

	l, err := New(opts)
	require.NoError(t, err)
	return l.(*commitLog), opts
}

func FuzzOffloadCrashConsistency(f *testing.F) {
	f.Add([]byte{1, 0, 1, 2, 3, 0, 1, 2})
	f.Add([]byte{0, 2, 2, 2, 1, 1, 0, 3, 3})
	f.Add([]byte{1, 3, 0, 1, 2, 3, 0, 1, 2, 3, 0, 1})
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

	f.Fuzz(func(t *testing.T, data []byte) {
		s := &fzStream{b: data}
		cl, opts := fzOffloadSetup(t, s)

		app := func(k, v string) int64 {
			o, err := cl.Append([]*Message{{Key: []byte(k), Value: []byte(v)}})
			require.NoError(t, err)
			cl.SetHighWatermark(o[0])
			return o[0]
		}

		// Build a committed log; a few keys so a later compaction would have
		// superseded versions (compaction is added in a later phase).
		valc := 0
		for ops := 3 + s.intn(24); ops > 0; ops-- {
			valc++
			app(fmt.Sprintf("k%d", s.intn(4)), fmt.Sprintf("v%d", valc))
		}
		require.NoError(t, cl.SyncAll())

		// Reference committed state: reads BEFORE any offload (all local).
		pre := fzReadAll(t, cl)

		// Offload rounds: move sealed segments below an entropy-chosen floor into
		// the store. Reads must stay byte-identical (transparency) through the
		// store + prefetch buffer + index cache.
		for rounds := s.intn(4); rounds >= 0; rounds-- {
			newest := cl.NewestOffset()
			if newest > 0 {
				_, err := cl.OffloadBefore(int64(s.intn(int(newest) + 1)))
				require.NoError(t, err)
			}
			require.Equal(t, pre, fzReadAll(t, cl), "offload changed a committed read")
		}

		// Reopen: offloaded segments must reopen from the store and read
		// identically; the active tail reopens locally.
		require.NoError(t, cl.Close())
		l2, err := New(opts)
		require.NoError(t, err)
		defer l2.Close()
		cl2 := l2.(*commitLog)
		require.Equal(t, pre, fzReadAll(t, cl2), "reopen after offload changed reads")

		// Idempotent reopen.
		require.NoError(t, cl2.Close())
		l3, err := New(opts)
		require.NoError(t, err)
		defer l3.Close()
		require.Equal(t, pre, fzReadAll(t, l3.(*commitLog)), "second reopen changed reads")
	})
}
