package commitlog

import (
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// The coalesce defaults were chosen by argument, not measurement. This measures
// the two quantities the argument is actually about — REQUESTS ISSUED and BYTES
// TRANSFERRED — which is what a store bills for and what the breakeven
// gap = 1e9 * C_req / C_GB is expressed in.
//
// It deliberately does not measure wall-clock. These tests run against a
// filesystem store, so timings here would describe a local disk and say nothing
// about the object store the setting exists for. Request and byte counts are
// hardware-independent: they are the same numbers whatever the store is, and
// they are the inputs a deployment needs to compute its own budget.

// costStore records every request made of the store and every byte it hands
// back, including bytes pulled through a Stream after it was opened.
type costStore struct {
	*FileSegmentStore

	mu       sync.Mutex
	requests int
	bytes    int64
}

func (s *costStore) add(reqs int, n int64) {
	s.mu.Lock()
	s.requests += reqs
	s.bytes += n
	s.mu.Unlock()
}

func (s *costStore) reset() {
	s.mu.Lock()
	s.requests, s.bytes = 0, 0
	s.mu.Unlock()
}

func (s *costStore) totals() (int, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests, s.bytes
}

func (s *costStore) ReadAt(key string, p []byte, off int64) (int, error) {
	n, err := s.FileSegmentStore.ReadAt(key, p, off)
	s.add(1, int64(n))
	return n, err
}

func (s *costStore) Stream(key string, off int64) (io.ReadCloser, error) {
	rc, err := s.FileSegmentStore.Stream(key, off)
	if err != nil {
		return nil, err
	}
	// The request is charged when the stream is opened; the bytes accrue as the
	// caller drains it, which is exactly how a store bills a ranged GET.
	s.add(1, 0)
	return &costReadCloser{rc: rc, store: s}, nil
}

type costReadCloser struct {
	rc    io.ReadCloser
	store *costStore
}

func (r *costReadCloser) Read(p []byte) (int, error) {
	n, err := r.rc.Read(p)
	r.store.add(0, int64(n))
	return n, err
}

func (r *costReadCloser) Close() error { return r.rc.Close() }

// costLog builds a tiered log where one record in every `every` carries a key
// under "want:", and offloads all its sealed segments to a counting store.
func costLog(t *testing.T, records, every int) (*commitLog, *costStore, int64) {
	t.Helper()
	dir := tempDir(t)
	fs, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)
	store := &costStore{FileSegmentStore: fs}

	l, cleanup := setupWithOptions(t, Options{
		Path: dir,
		// ~280 records per segment at the record size below, so even the
		// sparse density lands several hits in each segment. With segments too
		// small for that, every hit is alone in its own segment, there is
		// nothing to coalesce, and the budget provably cannot matter — the
		// measurement would look flat and mean nothing.
		MaxSegmentBytes:  64 << 10,
		Compact:          true,
		Tiers:            oneTier(store),
		DisableAutoClean: true,
	})
	t.Cleanup(cleanup)

	app := func(m *Message) {
		offs, err := l.Append([]*Message{m})
		require.NoError(t, err)
		l.SetHighWatermark(offs[0])
	}
	for i := 0; i < records; i++ {
		if i%every == 0 {
			app(&Message{Key: []byte(fmt.Sprintf("want:%05d", i)), Value: []byte("hit")})
			continue
		}
		// Padding wide enough that gaps between hits are measured in KB, which
		// is the range the tier budget actually discriminates over.
		app(&Message{
			Key:   []byte(fmt.Sprintf("other:%05d", i)),
			Value: make([]byte, 200),
		})
	}
	for i := 0; i < 20; i++ {
		app(&Message{Key: []byte(fmt.Sprintf("pad:%03d", i)), Value: make([]byte, 200)})
	}

	bound := l.ActiveSegmentBase() - 1
	n, err := l.OffloadBefore(l.ActiveSegmentBase())
	require.NoError(t, err)
	require.NotZero(t, n)
	return l, store, bound
}

// Requests and bytes must trade off MONOTONICALLY against the coalesce budget.
// That is the whole premise of the setting: a larger budget buys fewer requests
// with wasted bytes. If it did not hold, the breakeven formula in
// Options.PrefixReadCoalesceBytes would be describing something the code does
// not do.
func TestPrefixReadCostProfile(t *testing.T) {
	for _, density := range []struct {
		name  string
		every int
	}{
		{"dense 1-in-4", 4},
		{"sparse 1-in-40", 40},
	} {
		t.Run(density.name, func(t *testing.T) {
			l, store, bound := costLog(t, 4000, density.every)
			opts := []ReadOption{KeyPrefix([]byte("want:")), Until(bound)}
			spec, err := l.resolve(opts)
			require.NoError(t, err)
			want := scanFiltered(t, l, spec)
			require.NotEmpty(t, want)

			// Guard against a vacuous measurement: with at most one hit per
			// segment there is nothing for a budget to coalesce, and a flat
			// profile would say nothing about the setting.
			segs := l.segmentsSnapshot()
			require.Greater(t, len(want), len(segs),
				"need more hits (%d) than segments (%d), or coalescing has nothing to do",
				len(want), len(segs))

			budgets := []int64{-1, 1 << 10, 4 << 10, 16 << 10, 64 << 10, 1 << 20}
			type point struct {
				budget   int64
				requests int
				bytes    int64
			}
			var points []point

			for _, b := range budgets {
				l.Options.PrefixReadTierCoalesceBytes = b
				store.reset()
				r, err := l.NewReader(opts...)
				require.NoError(t, err)
				requireRecsEq(t, want, drainReader(t, r), fmt.Sprintf("budget=%d", b))
				reqs, bytes := store.totals()
				points = append(points, point{b, reqs, bytes})
			}

			t.Logf("%d records, 1 hit in %d, %d hits returned",
				2000, density.every, len(want))
			t.Logf("%12s %10s %12s %14s", "budget", "requests", "bytes", "bytes/request")
			for _, p := range points {
				var per int64
				if p.requests > 0 {
					per = p.bytes / int64(p.requests)
				}
				label := fmt.Sprintf("%d", p.budget)
				if p.budget < 0 {
					label = "none"
				}
				t.Logf("%12s %10d %12d %14d", label, p.requests, p.bytes, per)
			}

			// Monotonic in both directions: never more requests as the budget
			// grows, never fewer bytes.
			for i := 1; i < len(points); i++ {
				prev, cur := points[i-1], points[i]
				require.LessOrEqual(t, cur.requests, prev.requests,
					"budget %d issued MORE requests (%d) than the smaller budget %d (%d)",
					cur.budget, cur.requests, prev.budget, prev.requests)
				require.GreaterOrEqual(t, cur.bytes, prev.bytes,
					"budget %d transferred FEWER bytes (%d) than the smaller budget %d (%d)",
					cur.budget, cur.bytes, prev.budget, prev.bytes)
			}

			// And the trade must be real at the extremes, or the setting is
			// doing nothing on this shape and the numbers above are noise.
			first, last := points[0], points[len(points)-1]
			require.Less(t, last.requests, first.requests,
				"the largest budget saved no requests at all")
			require.Greater(t, last.bytes, first.bytes,
				"the largest budget wasted no bytes at all")
		})
	}
}
