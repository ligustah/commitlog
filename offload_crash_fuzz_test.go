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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fzFaultStore wraps a SegmentStore and can fail the next Put to model a crash
// at the store commit point. Because Put is temp+rename, a failure either
// leaves a partial ".tmp" (crash before rename) or nothing — never a
// half-committed object. On any failure OffloadBefore aborts and the segment
// stays fully local, so no committed data is ever lost.
type fzFaultStore struct {
	inner    SegmentStore
	dir      string
	failNext bool
	leaveTmp bool
}

func (s *fzFaultStore) Put(key string, r io.Reader, size int64) error {
	if s.failNext {
		s.failNext = false
		if s.leaveTmp {
			// Partial temp object, as a crash between temp-write and rename leaves.
			f, err := os.Create(filepath.Join(s.dir, key+".tmp"))
			if err == nil {
				_, _ = io.CopyN(f, r, size/2)
				_ = f.Close()
			}
		}
		return errors.New("fz: injected Put failure")
	}
	return s.inner.Put(key, r, size)
}

func (s *fzFaultStore) ReadAt(key string, p []byte, off int64) (int, error) {
	return s.inner.ReadAt(key, p, off)
}
func (s *fzFaultStore) Size(key string) (int64, error) { return s.inner.Size(key) }
func (s *fzFaultStore) List() ([]string, error)        { return s.inner.List() }
func (s *fzFaultStore) Delete(key string) error        { return s.inner.Delete(key) }
func (s *fzFaultStore) LiveRead() bool                 { return s.inner.LiveRead() }

// fzOffloadSetup builds a log wired to a filesystem SegmentStore (behind a
// fault-injecting wrapper), with the index cache enabled (option 2) or not
// (option 1) per the entropy stream.
func fzOffloadSetup(t *testing.T, s *fzStream) (*commitLog, Options, *fzFaultStore) {
	base := tempDir(t)
	storeDir := filepath.Join(base, "store")
	inner, err := NewFileSegmentStore(storeDir)
	require.NoError(t, err)
	store := &fzFaultStore{inner: inner, dir: storeDir}

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
	return l.(*commitLog), opts, store
}

func FuzzOffloadCrashConsistency(f *testing.F) {
	f.Add([]byte{1, 0, 1, 2, 3, 0, 1, 2})
	f.Add([]byte{0, 2, 2, 2, 1, 1, 0, 3, 3})
	f.Add([]byte{1, 3, 0, 1, 2, 3, 0, 1, 2, 3, 0, 1})
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

	f.Fuzz(func(t *testing.T, data []byte) {
		s := &fzStream{b: data}
		cl, opts, store := fzOffloadSetup(t, s)

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
			// Sometimes crash at the store commit point (a failed Put, optionally
			// leaving a partial .tmp). OffloadBefore then aborts and the segment
			// stays fully local — no committed data may be lost.
			if s.bool() {
				store.failNext = true
				store.leaveTmp = s.bool()
			}
			newest := cl.NewestOffset()
			if newest > 0 {
				// A failed Put surfaces as an error; the log must stay intact
				// either way (offloaded or fallen back to local).
				_, _ = cl.OffloadBefore(int64(s.intn(int(newest) + 1)))
			}
			store.failNext = false // clear any injection this round didn't consume
			require.Equal(t, pre, fzReadAll(t, cl),
				"offload (or its injected failure) changed a committed read")
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
