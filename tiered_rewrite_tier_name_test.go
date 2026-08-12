package commitlog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// A rewrite of an offloaded segment must republish it into the manifest of the
// tier it actually lives in.
//
// The rewrite's publish passes a PENDING entry, because at that moment the
// segment has not switched to its new objects yet, and publishTierManifests
// groups entries by TierObject.Tier. So the tier NAME on that pending entry
// decides which manifest the segment lands in — and because the override is
// keyed by base offset, a wrong name does not merely misfile the entry, it
// CONSUMES the correct one from tierState, and the real tier's manifest is
// published without the segment at all: naming neither its old objects nor its
// new ones.
//
// clean() ends with an unconditional republish rebuilt from tierState alone,
// which repairs the entry and hides this behind a crash window. So the assertion
// here is not about the manifest the pass settles on. It records EVERY manifest
// the pass publishes and requires each one to name the segments the log is
// serving out of that tier — which is the invariant a per-segment publish exists
// to hold, since a manifest is read at open and a process can die anywhere.
//
// Invisible to every other tiered test in the package: they build their chain
// with oneTier, which names it defaultTierName, which was exactly the name the
// publish hard-coded. The tier here is named anything else on purpose.
func TestNoManifestAPassPublishesEverDropsALiveSegment(t *testing.T) {
	dir := tempDir(t)
	backing, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)
	store := &manifestRecorder{SegmentStore: backing}

	const tierName = "hot" // NOT defaultTierName — that is the whole test

	l, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  128,
		Compact:          true,
		Tiers:            []Tier{{Name: tierName, Store: store}},
		DisableAutoClean: true,
	})
	defer cleanup()

	// Every segment needs BOTH something compaction removes and something that
	// survives: a segment whose records are all superseded is deleted by the
	// pass rather than rewritten, and one with nothing to remove converges and
	// is not rewritten at all. Neither reaches the publish under test.
	var last int64
	for i := 0; i < 30; i++ {
		offs, err := l.Append([]*Message{
			{Key: []byte("shared"), Value: []byte("superseded padding")},
			{Key: []byte(fmt.Sprintf("unique-%02d", i)), Value: []byte("survivor padding")},
		})
		require.NoError(t, err)
		last = offs[len(offs)-1]
	}
	l.SetHighWatermark(last)

	n, err := l.OffloadBefore(last)
	require.NoError(t, err)
	require.Positive(t, n, "sealed segments must have offloaded")

	live := offloadedBaseOffsets(l)
	require.NotEmpty(t, live, "the fixture needs offloaded segments")

	// Only the clean pass is under test; the offload above published manifests of
	// its own while the segment set was still changing.
	store.record(true)

	hw := l.HighWatermark()
	_, err = l.CleanWithSpec(CleanSpec{
		Ceiling:          At(hw + 1),
		TombstoneGCBelow: hw + 1,
	})
	require.NoError(t, err)

	// Retention dropped nothing, so every segment offloaded before the pass is
	// still being served out of the tier after it. Asserted rather than assumed:
	// a fixture that lost them would make the loop below vacuous.
	require.Equal(t, live, offloadedBaseOffsets(l),
		"the fixture must not lose segments; this test is about the manifest")

	published := store.published()
	require.NotEmpty(t, published, "the pass must have published at least one manifest")

	for i, m := range published {
		named := make(map[int64]string, len(m))
		for _, o := range m {
			named[o.BaseOffset] = o.Tier
		}
		for _, base := range live {
			tier, ok := named[base]
			require.Truef(t, ok,
				"manifest %d of %d published during the pass does not name segment %d, "+
					"which the log is serving out of tier %q; a crash here loses it",
				i+1, len(published), base, tierName)
			require.Equalf(t, tierName, tier,
				"manifest %d names segment %d under tier %q", i+1, base, tier)
		}
	}
}

// manifestRecorder keeps a copy of every manifest published through it, so a
// test can assert about the intermediate states a pass passes through rather
// than only the one it settles on.
type manifestRecorder struct {
	SegmentStore
	mu       sync.Mutex
	on       bool
	captured [][]TierObject
}

func (r *manifestRecorder) record(on bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.on = on
}

func (r *manifestRecorder) published() [][]TierObject {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]TierObject(nil), r.captured...)
}

func (r *manifestRecorder) Put(key string, rd io.Reader, size int64) error {
	r.mu.Lock()
	on := r.on
	r.mu.Unlock()
	if key != manifestKey || !on {
		return r.SegmentStore.Put(key, rd, size)
	}
	// Read it out so it can be decoded, then hand the SAME bytes to the real
	// store: intercepting a publish must not change what gets published.
	body, err := io.ReadAll(rd)
	if err != nil {
		return err
	}
	var m tierManifest
	if err := json.Unmarshal(body, &m); err != nil {
		return err
	}
	r.mu.Lock()
	r.captured = append(r.captured, m.Segments)
	r.mu.Unlock()
	return r.SegmentStore.Put(key, bytes.NewReader(body), int64(len(body)))
}

// offloadedBaseOffsets is the base offsets of the log's offloaded segments.
func offloadedBaseOffsets(l *commitLog) []int64 {
	var out []int64
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, s := range l.segments {
		s.RLock()
		if s.store != nil {
			out = append(out, s.BaseOffset)
		}
		s.RUnlock()
	}
	return out
}
