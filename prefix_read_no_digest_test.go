package commitlog

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// A sealed segment with no digest must be served by ONE pass over it.
//
// This is a steady state, not a transient one, which is the reason it is worth a
// cost test of its own. The compact cleaner is the only thing that ever persists
// a .keys sidecar, so on a log that never compacts — Compact disabled, or
// compaction switched on but never yet run, as here — no sealed segment has a
// digest and none ever will. Every KeyPrefix read pays whatever this path costs,
// forever.
//
// It used to cost strictly more than the scan it was avoiding. planSegment
// answered a missing sidecar by calling buildKeyDigest, which reads every record
// in the segment AND builds a map over every distinct key in it, then threw the
// digest away and read the planned offsets a SECOND time. Two passes and the
// cleaner's whole per-segment key map, to answer one read. The map is the part
// that mattered: loadOrBuildDigests caps itself at two concurrent builds because
// ten of them over ~40MB segments measured >1GB, and nothing capped this path at
// all — the number in flight was however many readers were doing prefix reads.
//
// Equality, not a bound. "At most one pass per segment" would still be satisfied
// by a build-then-fetch that happened to coalesce into a single run, so it would
// not notice the old shape coming back.
func TestPrefixReadWithoutDigestsScansEachSegmentOnce(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 512,
		Compact:         true,
		// Nothing runs a pass on its own, so nothing writes a sidecar. That is
		// the state under test rather than an inconvenience to work around.
		DisableAutoClean: true,
	})
	t.Cleanup(cleanup)

	app := func(m *Message) {
		offs, err := l.Append([]*Message{m})
		require.NoError(t, err)
		l.SetHighWatermark(offs[0])
	}
	for i := 0; i < 200; i++ {
		if i%4 == 0 {
			app(&Message{Key: []byte(fmt.Sprintf("want:%03d", i)), Value: []byte("hit")})
			continue
		}
		app(&Message{Key: []byte(fmt.Sprintf("other:%03d", i)), Value: []byte("miss padding")})
	}
	// An unkeyed record and a tombstone, so the filtering the scan does is not
	// only the prefix comparison.
	app(&Message{Value: []byte("unkeyed")})
	app(&Message{Key: []byte("want:tomb"), Value: []byte("x"), Attributes: AttrTombstone})
	for i := 0; i < 8; i++ {
		app(&Message{Key: []byte(fmt.Sprintf("pad:%03d", i)), Value: []byte("pad padding")})
	}

	// The premise. If a sidecar ever appears here the test is measuring the
	// planned path and its numbers mean something else entirely.
	sidecars, err := filepath.Glob(filepath.Join(l.Options.Path, "*"+keysSuffix))
	require.NoError(t, err)
	require.Empty(t, sidecars,
		"a log that never cleans must have no key digests; found %v", sidecars)

	bound := l.ActiveSegmentBase() - 1
	sealed := int64(len(l.segmentsSnapshot()) - 1)
	require.Greater(t, sealed, int64(3),
		"test is vacuous unless several segments are sealed, got %d", sealed)

	opts := []ReadOption{KeyPrefix([]byte("want:")), Until(bound)}
	spec, err := l.resolve(opts)
	require.NoError(t, err)
	want := scanFiltered(t, l, spec)
	require.NotEmpty(t, want)

	before := segmentScans.Load()
	r, err := l.NewReader(opts...)
	require.NoError(t, err)
	got := drainReader(t, r)
	scans := segmentScans.Load() - before

	requireRecsEq(t, want, got, "a digest-less prefix read must return the same records")
	require.Equal(t, sealed, scans,
		"expected exactly one pass over each of the %d sealed segments, got %d — "+
			"more than one means the read is planning from a digest it built and discarded",
		sealed, scans)
}

// The same claim for SkipSuperseded, which is the one filter the scan path has
// to reproduce rather than simply apply.
//
// digestHits picks the newest copy of a key WITHIN the segment by looking at
// every offset the digest holds for it. A forward scan has no such lookahead, so
// it keeps the latest copy as it goes and retires the earlier one — and the
// thing that can go wrong is not which records survive but WHERE: reusing the
// retired record's slot would return the newer record at the older one's
// position, out of offset order, which the reader's ascending contract forbids.
func TestPrefixReadWithoutDigestsSkipsSupersededInOffsetOrder(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:             tempDir(t),
		MaxSegmentBytes:  512,
		Compact:          true,
		DisableAutoClean: true,
	})
	t.Cleanup(cleanup)

	app := func(m *Message) {
		offs, err := l.Append([]*Message{m})
		require.NoError(t, err)
		l.SetHighWatermark(offs[0])
	}
	// Keys deliberately rewritten out of order, so the newest copy of an EARLY
	// key lands after the only copy of a later one.
	for round := 0; round < 6; round++ {
		for _, k := range []string{"want:a", "want:b", "want:c", "want:d"} {
			app(&Message{Key: []byte(k), Value: []byte(fmt.Sprintf("%s-r%d", k, round))})
			app(&Message{Key: []byte("other:pad"), Value: []byte("padding padding")})
		}
	}
	for i := 0; i < 8; i++ {
		app(&Message{Key: []byte(fmt.Sprintf("pad:%03d", i)), Value: []byte("pad padding")})
	}

	sidecars, err := filepath.Glob(filepath.Join(l.Options.Path, "*"+keysSuffix))
	require.NoError(t, err)
	require.Empty(t, sidecars)

	bound := l.ActiveSegmentBase() - 1
	opts := []ReadOption{KeyPrefix([]byte("want:")), SkipSuperseded(), Until(bound)}
	spec, err := l.resolve(opts)
	require.NoError(t, err)
	want := scanFiltered(t, l, spec)
	require.NotEmpty(t, want)

	r, err := l.NewReader(opts...)
	require.NoError(t, err)
	got := drainReader(t, r)
	requireRecsEq(t, want, got, "digest-less SkipSuperseded must match the scan")

	for i := 1; i < len(got); i++ {
		require.Greater(t, got[i].off, got[i-1].off,
			"records must arrive in ascending offset order, got %d after %d",
			got[i].off, got[i-1].off)
	}
}
