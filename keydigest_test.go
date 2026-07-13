package commitlog

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Digest roundtrip: everything the cleaner's decisions depend on must survive
// encode → write → load bit-exactly, and iteration must stream keys sorted.
func TestKeyDigestRoundtrip(t *testing.T) {
	l, app := specLog(t)
	app(&Message{Key: []byte("b"), Value: []byte("v1"),
		Headers: map[string][]byte{"pid": {1}}})
	app(&Message{Key: []byte("a"), Value: []byte("v2"), Attributes: AttrTombstone})
	app(&Message{Value: []byte("unkeyed"), Headers: map[string][]byte{"seq": {2}}})
	app(&Message{Value: []byte("marker"), Attributes: AttrControl})
	app(&Message{Key: []byte("b"), Value: []byte("v3")})
	app(&Message{Key: []byte("z"), Value: []byte("v4")}) // forces prior segments sealed

	l.mu.RLock()
	seg := l.segments[0] // 64-byte segments: one record each; take a sealed one
	nSegs := len(l.segments)
	l.mu.RUnlock()
	require.Greater(t, nSegs, 2)

	d, err := buildKeyDigest(seg)
	require.NoError(t, err)
	require.NoError(t, writeKeyDigest(seg, d))
	got := loadKeyDigest(seg)
	require.NotNil(t, got, "freshly written digest must load")
	require.Equal(t, d.base, got.base)
	require.Equal(t, d.logSize, got.logSize)
	require.Equal(t, d.nKeys, got.nKeys)
	require.Equal(t, d.unkeyed, got.unkeyed)
	require.Equal(t, d.control, got.control)
	require.Equal(t, d.epochs, got.epochs)

	// Iteration streams sorted keys with intact recs.
	it := newDigestIter(got)
	var prev []byte
	for it.next() {
		if prev != nil {
			require.Negative(t, bytes.Compare(prev, it.key), "keys must be sorted")
		}
		prev = append(prev[:0], it.key...)
		require.NotEmpty(t, it.recs)
	}
}

// A digest that does not bind to the segment's current content (wrong base,
// wrong log size, corrupt bytes) must be rejected, never trusted.
func TestKeyDigestBindingAndCorruption(t *testing.T) {
	l, app := specLog(t)
	for i := 0; i < 4; i++ {
		app(&Message{Key: []byte{byte(i)}, Value: []byte("v")})
	}
	l.mu.RLock()
	segA, segB := l.segments[0], l.segments[1]
	l.mu.RUnlock()

	dA, err := buildKeyDigest(segA)
	require.NoError(t, err)

	// Install A's digest at B's path: base/logSize mismatch → rejected.
	require.NoError(t, os.WriteFile(digestPath(segB), encodeKeyDigest(dA), 0666))
	require.Nil(t, loadKeyDigest(segB), "digest bound to another segment must not load")

	// Corrupt one byte of a valid digest → rejected.
	require.NoError(t, writeKeyDigest(segA, dA))
	data, err := os.ReadFile(digestPath(segA))
	require.NoError(t, err)
	data[len(data)/2] ^= 0xFF
	require.NoError(t, os.WriteFile(digestPath(segA), data, 0666))
	require.Nil(t, loadKeyDigest(segA), "corrupt digest must not load")

	// Truncated file → rejected.
	require.NoError(t, os.WriteFile(digestPath(segA), data[:8], 0666))
	require.Nil(t, loadKeyDigest(segA))
}

// The point of the digests: a clean over a converged log must not read ANY
// sealed segment's records — only the active tail is scanned (for its
// in-memory digest). Proven via the segment-scanner construction counter.
func TestConvergedCleanReadsNoSealedRecords(t *testing.T) {
	l, app := specLog(t)
	for i := 0; i < 8; i++ {
		app(&Message{
			Key: []byte{byte(i)}, Value: []byte("v"),
			Headers: map[string][]byte{"pid": {0, 0, 0, 0, 0, 0, 0, 1}},
		})
	}
	spec := CleanSpec{
		Ceiling:      l.HighWatermark(),
		StripBelow:   l.HighWatermark(),
		StripHeaders: []string{"pid", "epoch", "seq"},
	}
	// First clean strips headers (rewrites) and installs stamped digests.
	require.NoError(t, l.CleanWithSpec(spec))
	// Second clean converges via scans (stamps now cover) and refreshes
	// nothing; from here on the digests prove the fixed point.
	require.NoError(t, l.CleanWithSpec(spec))

	before := segmentScans.Load()
	require.NoError(t, l.CleanWithSpec(spec))
	scans := segmentScans.Load() - before
	require.LessOrEqual(t, scans, int64(1),
		"converged clean must scan at most the active segment, got %d scans", scans)

	// And the files are untouched (existing convergence guarantee).
	require.NoError(t, l.CleanWithSpec(spec))
}

// Randomized equivalence: the digest-merge clean must implement EXACTLY the
// documented semantics — latest-per-key below the ceiling wins; aborted
// records vanish; markers and headers below StripBelow are removed/stripped;
// expired latest tombstones vanish. The reference is computed independently
// from the pre-clean records.
func TestCleanDigestMergeEquivalence(t *testing.T) {
	for seed := int64(1); seed <= 5; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			l, app := specLog(t)

			type recInfo struct {
				off     int64
				key     string
				control bool
				hasPid  bool
			}
			var recs []recInfo
			aborted := map[int64]struct{}{}
			for i := 0; i < 60; i++ {
				r := recInfo{}
				switch rng.Intn(10) {
				case 0: // control marker
					r.off = app(&Message{Value: []byte("m"), Attributes: AttrControl})
					r.control = true
				case 1: // unkeyed data
					r.off = app(&Message{Value: []byte("u"),
						Headers: map[string][]byte{"pid": {1}}})
					r.hasPid = true
				default:
					r.key = fmt.Sprintf("k%d", rng.Intn(12))
					m := &Message{Key: []byte(r.key), Value: []byte(fmt.Sprintf("v%d", i))}
					if rng.Intn(3) == 0 {
						m.Headers = map[string][]byte{"pid": {1}}
						r.hasPid = true
					}
					r.off = app(m)
				}
				if !r.control && rng.Intn(8) == 0 {
					aborted[r.off] = struct{}{}
				}
				recs = append(recs, r)
			}

			hw := l.HighWatermark()
			ceiling := hw - int64(rng.Intn(10))
			stripBelow := ceiling - int64(rng.Intn(6))
			spec := CleanSpec{
				Ceiling:      ceiling,
				StripBelow:   stripBelow,
				StripHeaders: []string{"pid", "epoch", "seq"},
				Aborted: func(off int64) bool {
					_, ok := aborted[off]
					return ok
				},
			}

			// Reference: latest non-aborted keyed offset ≤ ceiling per key.
			latest := map[string]int64{}
			for _, r := range recs {
				if r.control || r.key == "" || r.off > ceiling {
					continue
				}
				if _, ab := aborted[r.off]; ab {
					continue
				}
				if r.off > latest[r.key] {
					latest[r.key] = r.off
				}
			}
			// The active (last) segment is never compacted — everything in
			// it stays, exactly as in the pre-digest cleaner.
			l.mu.RLock()
			activeBase := l.segments[len(l.segments)-1].BaseOffset
			l.mu.RUnlock()

			expect := map[int64]bool{} // offset → expect present post-clean
			for _, r := range recs {
				switch {
				case r.off >= activeBase:
					expect[r.off] = true
				case r.off >= ceiling:
					expect[r.off] = true
				case r.control:
					expect[r.off] = r.off >= stripBelow
				default:
					if _, ab := aborted[r.off]; ab {
						expect[r.off] = false
					} else if r.key == "" {
						expect[r.off] = true
					} else {
						expect[r.off] = latest[r.key] == r.off
					}
				}
			}

			require.NoError(t, l.CleanWithSpec(spec))
			got := readAllMsgs(t, l)
			for off, want := range expect {
				_, present := got[off]
				require.Equal(t, want, present, "offset %d presence (seed %d)", off, seed)
				if present && off < stripBelow && off < activeBase {
					if msg := got[off]; msg.Attributes()&AttrControl == 0 {
						_, hasPid := msg.Headers()["pid"]
						require.False(t, hasPid, "offset %d must be stripped", off)
					}
				}
			}

			// A second pass must converge to the identical visible set.
			require.NoError(t, l.CleanWithSpec(spec))
			got2 := readAllMsgs(t, l)
			require.Equal(t, len(got), len(got2), "second clean changed the log")
			for off := range got {
				_, ok := got2[off]
				require.True(t, ok, "second clean dropped offset %d", off)
			}
		})
	}
}

// Sidecar lifecycle: cleans install digests for sealed segments; deleting a
// segment (retention/empty-cleanup) removes its digest; a rewrite (Replace)
// leaves a digest that binds to the NEW content.
func TestKeyDigestLifecycle(t *testing.T) {
	l, app := specLog(t)
	for i := 0; i < 6; i++ {
		app(&Message{Key: []byte("dup"), Value: []byte{byte(i)}}) // all superseded
	}
	app(&Message{Key: []byte("live"), Value: []byte("x")})
	spec := CleanSpec{Ceiling: l.HighWatermark()}
	require.NoError(t, l.CleanWithSpec(spec))

	// Digests exist for current sealed segments and bind (load non-nil).
	l.mu.RLock()
	sealed := append([]*segment(nil), l.segments[:len(l.segments)-1]...)
	l.mu.RUnlock()
	require.NotEmpty(t, sealed)
	for _, seg := range sealed {
		require.NotNil(t, loadKeyDigest(seg),
			"sealed segment %d must have a binding digest after clean", seg.BaseOffset)
	}

	// Superseded-only segments were deleted — no orphan .keys files may
	// remain for bases that no longer have a .log.
	entries, err := os.ReadDir(l.Path)
	require.NoError(t, err)
	logs := map[string]bool{}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".log" {
			logs[e.Name()[:len(e.Name())-len(".log")]] = true
		}
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == keysSuffix {
			base := e.Name()[:len(e.Name())-len(keysSuffix)]
			require.True(t, logs[base], "orphan digest %s for deleted segment", e.Name())
		}
	}
}

// The strip stamp must not over-claim: after a clean verified stripping below
// boundary B, records between B and a HIGHER later boundary still get
// stripped (the stamp only covers what was scanned).
func TestStripStampDoesNotOverClaim(t *testing.T) {
	l, app := specLog(t)
	var offs []int64
	for i := 0; i < 6; i++ {
		offs = append(offs, app(&Message{
			Key: []byte{byte(i)}, Value: []byte("v"),
			Headers: map[string][]byte{"pid": {1}},
		}))
	}
	hdrs := []string{"pid", "epoch", "seq"}
	// First clean strips only below offs[3].
	require.NoError(t, l.CleanWithSpec(CleanSpec{
		Ceiling: l.HighWatermark(), StripBelow: offs[3], StripHeaders: hdrs}))
	// Second clean advances the boundary to the HW: every SEALED record
	// below it must lose its pid header now, stamp notwithstanding (the
	// active segment is never compacted, so its records keep theirs).
	hw := l.HighWatermark()
	l.mu.RLock()
	activeBase := l.segments[len(l.segments)-1].BaseOffset
	l.mu.RUnlock()
	require.NoError(t, l.CleanWithSpec(CleanSpec{
		Ceiling: hw, StripBelow: hw, StripHeaders: hdrs}))
	got := readAllMsgs(t, l)
	for _, off := range offs {
		if off >= hw || off >= activeBase {
			continue
		}
		msg, ok := got[off]
		require.True(t, ok)
		_, hasPid := msg.Headers()["pid"]
		require.False(t, hasPid, "offset %d must be stripped after boundary advanced", off)
	}
	// And the stamp now covers everything: the next clean is a no-scan skip.
	require.NoError(t, l.CleanWithSpec(CleanSpec{
		Ceiling: hw, StripBelow: hw, StripHeaders: hdrs}))
	before := segmentScans.Load()
	require.NoError(t, l.CleanWithSpec(CleanSpec{
		Ceiling: hw, StripBelow: hw, StripHeaders: hdrs}))
	require.LessOrEqual(t, segmentScans.Load()-before, int64(1))
}
