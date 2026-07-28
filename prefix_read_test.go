package commitlog

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// scanPrefix is an INDEPENDENT answer to the same question, computed the dumb
// way: walk every record in every sealed segment and keep the last one seen per
// key. It shares no code with the digest merge beyond the scanner itself, so a
// bug in the merge, the digest, the prefix bounds or the fetch shows up as a
// disagreement rather than as both sides being wrong together.
func scanPrefix(t *testing.T, l *commitLog, prefix []byte, bound int64) []PrefixRecord {
	t.Helper()
	l.mu.RLock()
	segments := make([]*segment, len(l.segments))
	copy(segments, l.segments)
	l.mu.RUnlock()
	if len(segments) == 0 {
		return nil
	}

	type win struct {
		off int64
		msg SerializedMessage
	}
	latest := map[string]win{}
	for _, seg := range segments[:len(segments)-1] {
		ss := newSegmentScanner(seg)
		for ms, _, err := ss.Scan(); err == nil; ms, _, err = ss.Scan() {
			off, msg := ms.Offset(), ms.Message()
			if off > bound {
				continue
			}
			if msg.Attributes()&AttrControl != 0 {
				continue
			}
			key := msg.Key()
			if key == nil || !bytes.HasPrefix(key, prefix) {
				continue
			}
			if w, ok := latest[string(key)]; ok && w.off > off {
				continue
			}
			cp := make(SerializedMessage, len(msg))
			copy(cp, msg)
			latest[string(key)] = win{off: off, msg: cp}
		}
		ss.Close() // nolint: errcheck
	}

	keys := make([]string, 0, len(latest))
	for k := range latest {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]PrefixRecord, 0, len(keys))
	for _, k := range keys {
		out = append(out, PrefixRecord{Offset: latest[k].off, Message: latest[k].msg})
	}
	return out
}

// requirePrefixEq compares record-for-record, and reports the KEY on failure —
// an offset alone says nothing about which key was lost.
func requirePrefixEq(t *testing.T, want, got []PrefixRecord, msg string) {
	t.Helper()
	wantKeys := make([]string, len(want))
	for i, r := range want {
		wantKeys[i] = string(r.Message.Key())
	}
	gotKeys := make([]string, len(got))
	for i, r := range got {
		gotKeys[i] = string(r.Message.Key())
	}
	require.Equal(t, wantKeys, gotKeys, "%s: keys differ", msg)
	for i := range want {
		require.Equal(t, want[i].Offset, got[i].Offset, "%s: offset for key %q", msg, wantKeys[i])
		require.Equal(t, want[i].Message.Value(), got[i].Message.Value(), "%s: value for key %q", msg, wantKeys[i])
		require.Equal(t, want[i].Message.Attributes(), got[i].Message.Attributes(),
			"%s: attributes for key %q (tombstone flag)", msg, wantKeys[i])
	}
}

func removeAllDigests(t *testing.T, l *commitLog) int {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(l.Options.Path, "*"+keysSuffix))
	require.NoError(t, err)
	for _, p := range paths {
		require.NoError(t, os.Remove(p))
	}
	return len(paths)
}

// The basic contract: latest-per-key within the prefix, in key order, from
// sealed segments only, with tombstones present.
func TestReadKeyPrefixBasics(t *testing.T) {
	l, app := specLog(t)

	app(&Message{Key: []byte("user:1"), Value: []byte("v1")})
	app(&Message{Key: []byte("order:1"), Value: []byte("o1")})
	offUser1 := app(&Message{Key: []byte("user:1"), Value: []byte("v2")}) // supersedes
	app(&Message{Key: []byte("user:2"), Value: []byte("w1")})
	offUser2 := app(&Message{Key: []byte("user:2"), Value: []byte("gone"), Attributes: AttrTombstone})
	offUser3 := app(&Message{Key: []byte("user:3"), Value: []byte("x1")})
	app(&Message{Key: []byte("zzz"), Value: []byte("pad")}) // active segment

	got, through, err := l.ReadKeyPrefix([]byte("user:"), -1)
	require.NoError(t, err)

	require.Equal(t, l.ActiveSegmentBase()-1, through,
		"completeThrough must be the sealed boundary, not the log end")

	keys := make([]string, len(got))
	for i, r := range got {
		keys[i] = string(r.Message.Key())
	}
	require.Equal(t, []string{"user:1", "user:2", "user:3"}, keys,
		"prefix must select its range, in key order, and nothing else")

	require.Equal(t, offUser1, got[0].Offset, "must return the LATEST copy of user:1")
	require.Equal(t, "v2", string(got[0].Message.Value()))

	require.Equal(t, offUser2, got[1].Offset)
	require.NotZero(t, got[1].Message.Attributes()&AttrTombstone,
		"tombstone must be returned AS a tombstone: the destination has to delete the key")

	require.Equal(t, offUser3, got[2].Offset)
}

// Records in the active segment are never returned, and completeThrough says
// so. Anything else would make the boundary a moving target.
func TestReadKeyPrefixExcludesActiveSegment(t *testing.T) {
	l, app := specLog(t)

	offSealed := app(&Message{Key: []byte("k:1"), Value: []byte("sealed")})
	app(&Message{Key: []byte("pad"), Value: []byte("pad")})
	// Whatever lands last is in the active segment.
	offActive := app(&Message{Key: []byte("k:1"), Value: []byte("active")})

	got, through, err := l.ReadKeyPrefix([]byte("k:"), -1)
	require.NoError(t, err)
	require.Less(t, through, offActive, "completeThrough must sit below the active segment")
	require.Len(t, got, 1)
	require.Equal(t, offSealed, got[0].Offset,
		"the active segment's newer copy must not be returned")
	require.Equal(t, "sealed", string(got[0].Message.Value()))
}

// upTo is the caller's commit boundary: records above it are invisible, exactly
// as they would be to a reader stopping there.
func TestReadKeyPrefixRespectsUpTo(t *testing.T) {
	l, app := specLog(t)

	offOld := app(&Message{Key: []byte("k:1"), Value: []byte("old")})
	offNew := app(&Message{Key: []byte("k:1"), Value: []byte("new")})
	app(&Message{Key: []byte("k:2"), Value: []byte("later")})
	app(&Message{Key: []byte("pad"), Value: []byte("pad")})

	got, through, err := l.ReadKeyPrefix([]byte("k:"), offOld)
	require.NoError(t, err)
	require.Equal(t, offOld, through)
	require.Len(t, got, 1, "k:2 is above the bound and must not appear")
	require.Equal(t, offOld, got[0].Offset, "must return the latest copy AT OR BELOW the bound")
	require.Equal(t, "old", string(got[0].Message.Value()))
	require.NotEqual(t, offNew, got[0].Offset)
}

// An empty prefix means every key.
func TestReadKeyPrefixEmptyPrefixMatchesAll(t *testing.T) {
	l, app := specLog(t)
	app(&Message{Key: []byte("a"), Value: []byte("1")})
	app(&Message{Key: []byte("b"), Value: []byte("2")})
	app(&Message{Key: []byte("c"), Value: []byte("3")})
	app(&Message{Key: []byte("pad"), Value: []byte("pad")})

	got, _, err := l.ReadKeyPrefix(nil, -1)
	require.NoError(t, err)
	requirePrefixEq(t, scanPrefix(t, l, nil, l.ActiveSegmentBase()-1), got, "empty prefix")
}

// THE constraint: the digests are an optimisation, so the answer must not
// depend on them existing. Deleting every sidecar must change nothing.
func TestReadKeyPrefixIdenticalWithoutDigests(t *testing.T) {
	l, app := specLog(t)
	for i := 0; i < 40; i++ {
		app(&Message{Key: []byte(fmt.Sprintf("k:%02d", i%13)), Value: []byte(fmt.Sprintf("v%d", i))})
	}
	app(&Message{Key: []byte("k:07"), Value: []byte("del"), Attributes: AttrTombstone})
	app(&Message{Key: []byte("pad"), Value: []byte("pad")})

	// Force sidecars to exist: a clean writes one per sealed segment.
	requireCleanOK(t, l, CleanSpec{Ceiling: l.HighWatermark()})
	bound := l.ActiveSegmentBase() - 1

	withDigests, through1, err := l.ReadKeyPrefix([]byte("k:"), -1)
	require.NoError(t, err)
	require.NotEmpty(t, withDigests)

	n := removeAllDigests(t, l)
	require.NotZero(t, n, "test is vacuous unless sidecars were actually there")

	withoutDigests, through2, err := l.ReadKeyPrefix([]byte("k:"), -1)
	require.NoError(t, err)

	require.Equal(t, through1, through2)
	requirePrefixEq(t, withDigests, withoutDigests, "digest present vs absent")
	requirePrefixEq(t, scanPrefix(t, l, []byte("k:"), bound), withoutDigests, "scan vs digest-free read")
}

// A corrupt sidecar must be rebuilt rather than believed. Silently trusting one
// would ship whatever it happened to say as the destination's state.
func TestReadKeyPrefixSurvivesCorruptDigest(t *testing.T) {
	l, app := specLog(t)
	for i := 0; i < 20; i++ {
		app(&Message{Key: []byte(fmt.Sprintf("k:%02d", i%7)), Value: []byte(fmt.Sprintf("v%d", i))})
	}
	app(&Message{Key: []byte("pad"), Value: []byte("pad")})
	requireCleanOK(t, l, CleanSpec{Ceiling: l.HighWatermark()})

	want, _, err := l.ReadKeyPrefix([]byte("k:"), -1)
	require.NoError(t, err)
	require.NotEmpty(t, want)

	paths, err := filepath.Glob(filepath.Join(l.Options.Path, "*"+keysSuffix))
	require.NoError(t, err)
	require.NotEmpty(t, paths)
	for _, p := range paths {
		data, err := os.ReadFile(p)
		require.NoError(t, err)
		data[len(data)/2] ^= 0xFF // break the CRC
		require.NoError(t, os.WriteFile(p, data, 0666))
	}

	got, _, err := l.ReadKeyPrefix([]byte("k:"), -1)
	require.NoError(t, err)
	requirePrefixEq(t, want, got, "corrupt sidecar must be rebuilt, not believed")
}

// The two fetch strategies must produce the SAME answer. PrefixReadCoalesceBytes
// only trades requests against discarded bytes, so it is a cost decision and
// must never be a correctness one — but it does pick genuinely different code
// paths (read through the gap vs re-locate through the index), and nothing else
// here forces the jump: the default gap is megabytes and these segments are
// bytes, so without this every test takes the skip path.
func TestReadKeyPrefixCoalesceStrategiesAgree(t *testing.T) {
	l, app := specLog(t)
	var wantKeys []string
	for i := 0; i < 120; i++ {
		if i%7 == 0 {
			k := fmt.Sprintf("want:%03d", i)
			wantKeys = append(wantKeys, k)
			app(&Message{Key: []byte(k), Value: []byte(fmt.Sprintf("hit%03d", i))})
			continue
		}
		app(&Message{Key: []byte(fmt.Sprintf("other:%03d", i)), Value: []byte("miss")})
	}
	app(&Message{Key: []byte("pad"), Value: []byte("padpadpadpad")})
	bound := l.ActiveSegmentBase() - 1
	want := scanPrefix(t, l, []byte("want:"), bound)
	require.NotEmpty(t, want)

	// Set on the open log deliberately: the point is the SAME data read two
	// ways, which two separately built logs could not guarantee.
	for _, coalesce := range []int64{1, 1 << 30} {
		l.Options.PrefixReadCoalesceBytes = coalesce
		got, _, err := l.ReadKeyPrefix([]byte("want:"), -1)
		require.NoError(t, err, "coalesce=%d", coalesce)
		requirePrefixEq(t, want, got, fmt.Sprintf("coalesce=%d", coalesce))

		gotKeys := make([]string, len(got))
		for i, r := range got {
			gotKeys[i] = string(r.Message.Key())
		}
		require.Equal(t, wantKeys, gotKeys, "coalesce=%d", coalesce)
	}
}

// Sparse hits over many segments, with the concurrent per-segment fetch.
func TestReadKeyPrefixSparseHitsAcrossSegments(t *testing.T) {
	l, app := specLog(t)
	// One wanted key buried every 20 records, so consecutive hits are far
	// enough apart to exceed prefixFetchSkipRecords.
	var wantKeys []string
	for i := 0; i < 200; i++ {
		if i%20 == 0 {
			k := fmt.Sprintf("want:%03d", i)
			wantKeys = append(wantKeys, k)
			app(&Message{Key: []byte(k), Value: []byte("hit")})
			continue
		}
		app(&Message{Key: []byte(fmt.Sprintf("other:%03d", i)), Value: []byte("miss")})
	}
	app(&Message{Key: []byte("pad"), Value: []byte("pad")})

	got, _, err := l.ReadKeyPrefix([]byte("want:"), -1)
	require.NoError(t, err)

	gotKeys := make([]string, len(got))
	for i, r := range got {
		gotKeys[i] = string(r.Message.Key())
		require.Equal(t, "hit", string(r.Message.Value()), "fetched the wrong record for %q", gotKeys[i])
	}
	require.Equal(t, wantKeys, gotKeys)
	requirePrefixEq(t, scanPrefix(t, l, []byte("want:"), l.ActiveSegmentBase()-1), got, "sparse hits")
}

// Differential test across many prefixes and shapes: the digest read and an
// independent scan must agree exactly, with and without sidecars present.
func TestReadKeyPrefixDifferentialAgainstScan(t *testing.T) {
	l, app := specLog(t)

	// A key space with shared prefixes, nested prefixes, supersessions,
	// tombstones, an empty-valued live record and keys either side of the
	// ranges being asked for.
	app(&Message{Key: []byte("a"), Value: []byte("bare")})
	app(&Message{Key: []byte("ab"), Value: []byte("1")})
	app(&Message{Key: []byte("abc"), Value: []byte("1")})
	app(&Message{Key: []byte("abc"), Value: []byte("2")}) // supersedes
	app(&Message{Key: []byte("abd"), Value: nil})         // live, empty value
	app(&Message{Key: []byte("abe"), Value: []byte("x"), Attributes: AttrTombstone})
	app(&Message{Key: []byte("b"), Value: []byte("after")})
	app(&Message{Key: []byte("ac"), Value: []byte("sibling")})
	for i := 0; i < 30; i++ {
		app(&Message{Key: []byte(fmt.Sprintf("ab%02d", i)), Value: []byte(fmt.Sprintf("n%d", i))})
	}
	app(&Message{Key: []byte("pad"), Value: []byte("pad")})

	prefixes := [][]byte{
		nil, []byte(""), []byte("a"), []byte("ab"), []byte("abc"), []byte("abd"),
		[]byte("ac"), []byte("b"), []byte("zzz-absent"), []byte("ab0"),
	}
	bound := l.ActiveSegmentBase() - 1

	check := func(stage string) {
		for _, p := range prefixes {
			got, through, err := l.ReadKeyPrefix(p, -1)
			require.NoError(t, err, "%s: prefix %q", stage, p)
			require.Equal(t, bound, through, "%s: prefix %q", stage, p)
			requirePrefixEq(t, scanPrefix(t, l, p, bound), got,
				fmt.Sprintf("%s: prefix %q", stage, p))
		}
		// Bounded reads must agree too, at every offset in range.
		for upTo := int64(0); upTo <= bound; upTo++ {
			got, through, err := l.ReadKeyPrefix([]byte("ab"), upTo)
			require.NoError(t, err, "%s: upTo %d", stage, upTo)
			require.Equal(t, upTo, through)
			requirePrefixEq(t, scanPrefix(t, l, []byte("ab"), upTo), got,
				fmt.Sprintf("%s: upTo %d", stage, upTo))
		}
	}

	check("freshly built digests")
	requireCleanOK(t, l, CleanSpec{Ceiling: l.HighWatermark()})
	check("after a clean persisted sidecars")
	removeAllDigests(t, l)
	check("with no sidecars at all")
}

// The acceleration itself, not just the answer. Everything else here would
// still pass if the read scanned every record in the log, so this pins the
// actual claim: with sidecars present, records are read only from the segments
// that hold hits.
func TestReadKeyPrefixDoesNotScanSegmentsWithoutHits(t *testing.T) {
	l, app := specLog(t)
	// Distinct keys throughout, so compaction supersedes nothing and the
	// segments survive the clean that installs the sidecars.
	for i := 0; i < 60; i++ {
		app(&Message{Key: []byte(fmt.Sprintf("other:%03d", i)), Value: []byte("miss")})
	}
	offWant := app(&Message{Key: []byte("want:1"), Value: []byte("hit")})
	// Enough padding that the hit is definitely SEALED: segments roll on a
	// byte budget, so one short record does not necessarily close one.
	for i := 0; i < 4; i++ {
		app(&Message{Key: []byte(fmt.Sprintf("pad:%d", i)), Value: []byte("padpadpadpad")})
	}
	require.Less(t, offWant, l.ActiveSegmentBase(), "the hit must be in a sealed segment")

	requireCleanOK(t, l, CleanSpec{Ceiling: l.HighWatermark()})
	sealed := len(l.Segments()) - 1
	require.Greater(t, sealed, 10, "test is only meaningful with many sealed segments")

	before := segmentScans.Load()
	got, _, err := l.ReadKeyPrefix([]byte("want:"), -1)
	require.NoError(t, err)
	scans := segmentScans.Load() - before

	require.Len(t, got, 1)
	require.Equal(t, "hit", string(got[0].Message.Value()))
	require.LessOrEqual(t, scans, int64(1),
		"prefix read scanned %d segments for 1 hit across %d sealed segments — "+
			"the digests are not being used", scans, sealed)
}

// denseSegLog rolls segments that hold MANY records, so several wanted records
// land in one segment — which is the case that distinguishes per-run fan-out
// from per-segment fan-out. specLog's 64-byte segments cannot show it: there,
// every record is already its own segment.
func denseSegLog(t *testing.T) (*commitLog, func(m *Message) int64) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 2000,
		Compact:         true,
	})
	t.Cleanup(cleanup)
	app := func(m *Message) int64 {
		offs, err := l.Append([]*Message{m})
		require.NoError(t, err)
		l.SetHighWatermark(offs[0])
		return offs[0]
	}
	return l, app
}

// The fan-out claim: concurrency is bounded by RUNS, not by segments, so hits
// concentrated in a few segments still parallelise. Without this, nothing here
// would notice the fetch collapsing back to one request per segment.
func TestPlanRunsFanOutIsNotCappedBySegmentCount(t *testing.T) {
	l, app := denseSegLog(t)
	for i := 0; i < 300; i++ {
		if i%5 == 0 {
			app(&Message{Key: []byte(fmt.Sprintf("want:%03d", i)), Value: []byte("hit")})
			continue
		}
		app(&Message{Key: []byte(fmt.Sprintf("other:%03d", i)), Value: []byte("miss")})
	}
	for i := 0; i < 60; i++ { // seal the tail
		app(&Message{Key: []byte(fmt.Sprintf("pad:%03d", i)), Value: []byte("padpadpadpad")})
	}

	segs := l.Segments()
	sealed := segs[:len(segs)-1]
	require.Greater(t, len(sealed), 2)

	// Collect the wanted offsets per segment the way the read does.
	hits, err := mergePrefix(digestsFor(t, sealed), []byte("want:"), l.ActiveSegmentBase()-1)
	require.NoError(t, err)
	require.NotEmpty(t, hits)

	bySeg := map[int][]int64{}
	for _, h := range hits {
		bySeg[h.segIdx] = append(bySeg[h.segIdx], h.offset)
	}
	segsWithHits := len(bySeg)
	require.Greater(t, len(hits), segsWithHits,
		"test needs segments holding MORE than one hit to be meaningful")

	countRuns := func(coalesce int64) int {
		n := 0
		for segIdx, offs := range bySeg {
			sort.Slice(offs, func(i, j int) bool { return offs[i] < offs[j] })
			runs, err := planRuns(sealed[segIdx], segIdx, offs, coalesce)
			require.NoError(t, err)
			n += len(runs)
		}
		return n
	}

	// Cheap requests / expensive bytes: split aggressively, so the fan-out is
	// far wider than the segment count.
	split := countRuns(0)
	require.Greater(t, split, segsWithHits,
		"per-run planning must exceed the %d-segment ceiling, got %d runs", segsWithHits, split)

	// Expensive requests / cheap bytes: coalesce into one pass per segment.
	merged := countRuns(1 << 30)
	require.Equal(t, segsWithHits, merged,
		"a large coalesce budget must collapse to one run per segment")
}

// digestsFor is the read's digest step, for tests that drive the merge directly.
func digestsFor(t *testing.T, segs []*segment) []*keyDigest {
	t.Helper()
	out := make([]*keyDigest, len(segs))
	for i, seg := range segs {
		if d := loadKeyDigest(seg); d != nil {
			out[i] = d
			continue
		}
		d, err := buildKeyDigest(seg, newBlockCache())
		require.NoError(t, err)
		out[i] = d
	}
	return out
}

// Whatever the coalesce budget does to the request pattern, the records must be
// identical — it is a cost knob, never a correctness one.
func TestReadKeyPrefixCoalesceIsCostOnlyOnDenseSegments(t *testing.T) {
	l, app := denseSegLog(t)
	for i := 0; i < 300; i++ {
		if i%5 == 0 {
			app(&Message{Key: []byte(fmt.Sprintf("want:%03d", i)), Value: []byte(fmt.Sprintf("hit%03d", i))})
			continue
		}
		app(&Message{Key: []byte(fmt.Sprintf("other:%03d", i)), Value: []byte("miss")})
	}
	for i := 0; i < 60; i++ {
		app(&Message{Key: []byte(fmt.Sprintf("pad:%03d", i)), Value: []byte("padpadpadpad")})
	}

	want := scanPrefix(t, l, []byte("want:"), l.ActiveSegmentBase()-1)
	require.NotEmpty(t, want)
	for _, coalesce := range []int64{1, 64, 1 << 12, 1 << 30} {
		for _, conc := range []int{1, 4, 64} {
			l.Options.PrefixReadCoalesceBytes = coalesce
			l.Options.PrefixReadConcurrency = conc
			got, _, err := l.ReadKeyPrefix([]byte("want:"), -1)
			require.NoError(t, err)
			requirePrefixEq(t, want, got, fmt.Sprintf("coalesce=%d conc=%d", coalesce, conc))
		}
	}
}

// offloadedPrefixLog builds a log whose sealed segments are offloaded to a
// SegmentStore, and returns it with the bound its sealed content ends at.
func offloadedPrefixLog(t *testing.T) (*commitLog, int64) {
	t.Helper()
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	l, cleanup := setupWithOptions(t, Options{
		Path: dir,
		// Big enough that a segment holds SEVERAL wanted records: with one hit
		// per segment there is nothing for a coalesce budget to merge, and the
		// per-tier settings become indistinguishable.
		MaxSegmentBytes:  512,
		Compact:          true,
		SegmentStore:     store,
		DisableAutoClean: true,
	})
	t.Cleanup(cleanup)

	app := func(m *Message) {
		offs, err := l.Append([]*Message{m})
		require.NoError(t, err)
		l.SetHighWatermark(offs[0])
	}
	for i := 0; i < 240; i++ {
		if i%4 == 0 {
			app(&Message{Key: []byte(fmt.Sprintf("want:%03d", i)), Value: []byte(fmt.Sprintf("hit%03d", i))})
			continue
		}
		app(&Message{Key: []byte(fmt.Sprintf("other:%03d", i)), Value: []byte("miss padding")})
	}
	app(&Message{Key: []byte("want:tomb"), Value: []byte("x"), Attributes: AttrTombstone})
	for i := 0; i < 8; i++ {
		app(&Message{Key: []byte(fmt.Sprintf("pad:%03d", i)), Value: []byte("pad padding here")})
	}

	bound := l.ActiveSegmentBase() - 1
	// ActiveSegmentBase, not bound: a segment ending at bound has NextOffset
	// == ActiveSegmentBase, so offloading "before bound" leaves that last
	// sealed segment local.
	n, err := l.OffloadBefore(l.ActiveSegmentBase())
	require.NoError(t, err)
	require.NotZero(t, n, "test is vacuous unless segments were actually offloaded")

	segs := l.Segments()
	var tiered, local int
	for _, s := range segs[:len(segs)-1] {
		if s.tiered() {
			tiered++
		} else {
			local++
		}
	}
	require.NotZero(t, tiered, "expected offloaded segments to read as tiered")
	require.Zero(t, local,
		"every sealed segment must be tiered for the per-tier assertions to mean anything")
	return l, bound
}

// The read must work against segments whose bytes live in a SegmentStore, which
// is the case the whole design is aimed at — and nothing else here covers it,
// since every other test reads local files. Also exercises the reader's claim
// on a store backing.
func TestReadKeyPrefixOverTieredSegments(t *testing.T) {
	l, bound := offloadedPrefixLog(t)
	l.Options.PrefixReadConcurrency = 2
	l.Options.PrefixReadTierConcurrency = 16

	want := scanPrefix(t, l, []byte("want:"), bound)
	require.NotEmpty(t, want)

	got, through, err := l.ReadKeyPrefix([]byte("want:"), -1)
	require.NoError(t, err)
	require.Equal(t, bound, through)
	requirePrefixEq(t, want, got, "tiered segments")

	// The tombstone must survive the trip through the store as a tombstone.
	var sawTomb bool
	for _, r := range got {
		if string(r.Message.Key()) == "want:tomb" {
			sawTomb = true
			require.NotZero(t, r.Message.Attributes()&AttrTombstone)
		}
	}
	require.True(t, sawTomb, "tombstone must be returned from a tiered segment too")
}

// The per-tier settings must actually be routed by tier. segmentScans counts
// one scanner per RUN, so the request pattern is observable: tightening the
// TIER budget on an offloaded log must split it into more runs, and changing
// the LOCAL budget must do nothing at all.
func TestPrefixReadTierBudgetGovernsTieredSegments(t *testing.T) {
	l, bound := offloadedPrefixLog(t)
	want := scanPrefix(t, l, []byte("want:"), bound)
	require.NotEmpty(t, want)

	runsFor := func() int64 {
		before := segmentScans.Load()
		got, _, err := l.ReadKeyPrefix([]byte("want:"), -1)
		require.NoError(t, err)
		requirePrefixEq(t, want, got, "tier budget must not change the answer")
		return segmentScans.Load() - before
	}

	// One pass per segment: everything coalesced.
	l.Options.PrefixReadTierCoalesceBytes = 1 << 30
	coalesced := runsFor()

	// Never coalesce: every isolated record becomes its own request.
	l.Options.PrefixReadTierCoalesceBytes = -1
	split := runsFor()
	require.Greater(t, split, coalesced,
		"a negative tier budget must split runs (%d) beyond the coalesced count (%d)", split, coalesced)

	// The LOCAL budget must not touch offloaded segments, at either extreme.
	l.Options.PrefixReadTierCoalesceBytes = 1 << 30
	for _, local := range []int64{-1, 1, 1 << 30} {
		l.Options.PrefixReadCoalesceBytes = local
		require.Equal(t, coalesced, runsFor(),
			"local budget %d changed the read pattern of TIERED segments", local)
	}
}

func TestCoalesceBudgetResolution(t *testing.T) {
	// Zero takes the default, as everywhere else in Options.
	require.Equal(t, int64(4096), coalesceBudget(0, 4096))
	// Negative is the escape hatch: never coalesce.
	require.Equal(t, int64(0), coalesceBudget(-1, 4096))
	// Anything positive is taken literally, including a very small budget.
	require.Equal(t, int64(1), coalesceBudget(1, 4096))
	require.Equal(t, int64(9999), coalesceBudget(9999, 4096))

	require.Equal(t, 8, concurrencyBudget(0, 8))
	require.Equal(t, 8, concurrencyBudget(-3, 8))
	require.Equal(t, 32, concurrencyBudget(32, 8))
}

func TestPrefixUpperBound(t *testing.T) {
	require.Nil(t, prefixUpperBound(nil), "an empty prefix has no upper bound")
	require.Nil(t, prefixUpperBound([]byte{}))
	require.Equal(t, []byte("b"), prefixUpperBound([]byte("a")))
	require.Equal(t, []byte("ac"), prefixUpperBound([]byte("ab")))
	// Trailing 0xFF carries: the range runs to the next representable key.
	require.Equal(t, []byte{'a', 0xFF}, prefixUpperBound([]byte{'a', 0xFE}))
	require.Equal(t, []byte{'b'}, prefixUpperBound([]byte{'a', 0xFF}))
	require.Nil(t, prefixUpperBound([]byte{0xFF, 0xFF}), "all-0xFF runs to the end")
}
