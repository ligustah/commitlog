package commitlog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// readRec is one record as a reader returned it.
type readRec struct {
	off int64
	msg SerializedMessage
}

// drainReader reads until io.EOF (or ctx expiry) and returns everything.
func drainReader(t *testing.T, r *Reader) []readRec {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var out []readRec
	headers := make([]byte, 28)
	for {
		msg, off, _, _, err := r.ReadMessage(ctx, headers)
		if err != nil {
			require.True(t, errors.Is(err, io.EOF), "unexpected read error: %v", err)
			return out
		}
		cp := make(SerializedMessage, len(msg))
		copy(cp, msg)
		out = append(out, readRec{off: off, msg: cp})
	}
}

// scanFiltered is an INDEPENDENT answer to the same question, computed the dumb
// way: walk every record in every segment and keep the ones that match. It
// shares no code with the digest planning, so a bug in the digest, the prefix
// bounds, the run planning or the fetch shows up as a disagreement rather than
// as both sides being wrong together.
func scanFiltered(t *testing.T, l *commitLog, spec readSpec) []readRec {
	t.Helper()
	l.mu.RLock()
	segments := make([]*segment, len(l.segments))
	copy(segments, l.segments)
	l.mu.RUnlock()

	bound := spec.until
	if !spec.uncommitted {
		if hw := l.HighWatermark(); bound < 0 || hw < bound {
			bound = hw
		}
	}

	// Pass 1: every record in range, in offset order.
	var all []readRec
	for _, seg := range segments {
		ss := newSegmentScanner(seg)
		for ms, _, err := ss.Scan(); err == nil; ms, _, err = ss.Scan() {
			off, msg := ms.Offset(), ms.Message()
			if off < spec.offset || (bound >= 0 && off > bound) {
				continue
			}
			cp := make(SerializedMessage, len(msg))
			copy(cp, msg)
			all = append(all, readRec{off: off, msg: cp})
		}
		ss.Close() // nolint: errcheck
	}

	// Pass 2: apply the filter, and the per-segment supersession rule if asked.
	segOf := func(off int64) int64 {
		base := int64(-1)
		for _, s := range segments {
			if s.BaseOffset <= off {
				base = s.BaseOffset
			}
		}
		return base
	}
	lastInSeg := map[string]int64{} // segment|key -> highest offset seen
	if spec.skipSuperseded {
		for _, rec := range all {
			if !spec.matchesPrefix(rec.msg) || rec.msg.Key() == nil {
				continue
			}
			k := fmt.Sprintf("%d|%s", segOf(rec.off), rec.msg.Key())
			if rec.off > lastInSeg[k] {
				lastInSeg[k] = rec.off
			}
		}
	}

	var out []readRec
	for _, rec := range all {
		if !spec.matchesPrefix(rec.msg) {
			continue
		}
		if spec.skipSuperseded && rec.msg.Key() != nil {
			k := fmt.Sprintf("%d|%s", segOf(rec.off), rec.msg.Key())
			if lastInSeg[k] != rec.off {
				continue
			}
		}
		out = append(out, rec)
	}
	return out
}

func requireRecsEq(t *testing.T, want, got []readRec, msg string) {
	t.Helper()
	wantDesc := make([]string, len(want))
	for i, r := range want {
		wantDesc[i] = fmt.Sprintf("%d:%s=%s", r.off, r.msg.Key(), r.msg.Value())
	}
	gotDesc := make([]string, len(got))
	for i, r := range got {
		gotDesc[i] = fmt.Sprintf("%d:%s=%s", r.off, r.msg.Key(), r.msg.Value())
	}
	require.Equal(t, wantDesc, gotDesc, "%s", msg)
	for i := range want {
		require.Equal(t, want[i].msg.Attributes(), got[i].msg.Attributes(),
			"%s: attributes differ for offset %d (tombstone flag)", msg, want[i].off)
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

// ---- basics ----

// Offset order, every matching copy, unkeyed records and markers dropped.
func TestReaderKeyPrefixBasics(t *testing.T) {
	l, app := specLog(t)

	app(&Message{Key: []byte("user:1"), Value: []byte("v1")})
	app(&Message{Key: []byte("order:1"), Value: []byte("o1")})
	app(&Message{Key: []byte("user:1"), Value: []byte("v2")}) // NOT superseded away
	app(&Message{Value: []byte("unkeyed")})
	app(&Message{Attributes: AttrControl, Value: []byte("marker")})
	app(&Message{Key: []byte("user:2"), Value: []byte("gone"), Attributes: AttrTombstone})
	app(&Message{Key: []byte("pad"), Value: []byte("padpadpad")})

	r, err := l.NewReader(KeyPrefix([]byte("user:")))
	require.NoError(t, err)
	got := drainReader(t, r)

	var desc []string
	for _, rec := range got {
		desc = append(desc, fmt.Sprintf("%s=%s", rec.msg.Key(), rec.msg.Value()))
	}
	require.Equal(t, []string{"user:1=v1", "user:1=v2", "user:2=gone"}, desc,
		"every matching copy, in offset order, unkeyed and markers dropped")

	for i := 1; i < len(got); i++ {
		require.Greater(t, got[i].off, got[i-1].off, "must be offset-ordered")
	}
	require.NotZero(t, got[2].msg.Attributes()&AttrTombstone,
		"tombstone must arrive as a tombstone")
}

// The filter must never change which records exist, only which are returned.
func TestReaderKeyPrefixMatchesScan(t *testing.T) {
	l, app := specLog(t)
	app(&Message{Key: []byte("a"), Value: []byte("bare")})
	app(&Message{Key: []byte("ab"), Value: []byte("1")})
	app(&Message{Key: []byte("abc"), Value: []byte("1")})
	app(&Message{Key: []byte("abc"), Value: []byte("2")})
	app(&Message{Key: []byte("abd"), Value: nil})
	app(&Message{Key: []byte("abe"), Value: []byte("x"), Attributes: AttrTombstone})
	app(&Message{Value: []byte("unkeyed")})
	app(&Message{Key: []byte("b"), Value: []byte("after")})
	for i := 0; i < 40; i++ {
		app(&Message{Key: []byte(fmt.Sprintf("ab%02d", i)), Value: []byte(fmt.Sprintf("n%d", i))})
	}
	app(&Message{Key: []byte("pad"), Value: []byte("padpadpad")})

	prefixes := [][]byte{
		{}, []byte("a"), []byte("ab"), []byte("abc"), []byte("abd"),
		[]byte("b"), []byte("zzz-absent"), []byte("ab0"),
	}
	check := func(stage string) {
		for _, p := range prefixes {
			for _, skip := range []bool{false, true} {
				opts := []ReadOption{KeyPrefix(p)}
				if skip {
					opts = append(opts, SkipSuperseded())
				}
				r, err := l.NewReader(opts...)
				require.NoError(t, err)
				got := drainReader(t, r)

				spec, err := l.resolve(opts)
				require.NoError(t, err)
				requireRecsEq(t, scanFiltered(t, l, spec), got,
					fmt.Sprintf("%s: prefix %q skipSuperseded=%v", stage, p, skip))
			}
		}
	}

	check("freshly built digests")
	requireCleanOK(t, l, CleanSpec{Ceiling: l.HighWatermark()})
	check("after a clean persisted sidecars")
	removeAllDigests(t, l)
	check("with no sidecars at all")
}

// THE constraint: digests are an optimisation, so deleting or corrupting them
// must not change a single record returned.
func TestReaderKeyPrefixIdenticalWithoutDigests(t *testing.T) {
	l, app := specLog(t)
	for i := 0; i < 60; i++ {
		app(&Message{Key: []byte(fmt.Sprintf("k:%02d", i%13)), Value: []byte(fmt.Sprintf("v%d", i))})
	}
	app(&Message{Key: []byte("k:07"), Value: []byte("del"), Attributes: AttrTombstone})
	app(&Message{Key: []byte("pad"), Value: []byte("padpadpad")})
	requireCleanOK(t, l, CleanSpec{Ceiling: l.HighWatermark()})

	read := func() []readRec {
		r, err := l.NewReader(KeyPrefix([]byte("k:")))
		require.NoError(t, err)
		return drainReader(t, r)
	}

	withDigests := read()
	require.NotEmpty(t, withDigests)

	// Corrupt every sidecar: must be rebuilt rather than believed.
	paths, err := filepath.Glob(filepath.Join(l.Options.Path, "*"+keysSuffix))
	require.NoError(t, err)
	require.NotEmpty(t, paths)
	for _, p := range paths {
		data, err := os.ReadFile(p)
		require.NoError(t, err)
		data[len(data)/2] ^= 0xFF
		require.NoError(t, os.WriteFile(p, data, 0666))
	}
	requireRecsEq(t, withDigests, read(), "corrupt sidecars must be rebuilt, not believed")

	n := removeAllDigests(t, l)
	require.NotZero(t, n)
	requireRecsEq(t, withDigests, read(), "digest present vs absent")
}

// ---- bounds, follow, commit boundary ----

func TestReaderFromAndUntil(t *testing.T) {
	l, app := specLog(t)
	var offs []int64
	for i := 0; i < 12; i++ {
		offs = append(offs, app(&Message{Key: []byte(fmt.Sprintf("k:%d", i)), Value: []byte("v")}))
	}
	app(&Message{Key: []byte("pad"), Value: []byte("padpadpad")})

	r, err := l.NewReader(From(offs[3]), Until(offs[7]), KeyPrefix([]byte("k:")))
	require.NoError(t, err)
	got := drainReader(t, r)
	require.Len(t, got, 5, "inclusive at both ends")
	require.Equal(t, offs[3], got[0].off)
	require.Equal(t, offs[7], got[len(got)-1].off)

	_, err = l.NewReader(From(10), Until(5))
	require.Error(t, err, "an inverted range must be refused, not silently empty")
}

// Terminating is the default; Follow is opt-in. A reader that unexpectedly
// blocks forever is the failure this default prevents.
func TestReaderTerminatesByDefault(t *testing.T) {
	l, app := specLog(t)
	app(&Message{Key: []byte("k:1"), Value: []byte("v")})
	app(&Message{Key: []byte("pad"), Value: []byte("padpadpad")})

	done := make(chan struct{})
	go func() {
		defer close(done)
		r, err := l.NewReader()
		require.NoError(t, err)
		drainReader(t, r)
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("default reader blocked at the end of the log instead of returning io.EOF")
	}
}

// Follow picks up records appended after the reader caught up — including
// through a prefix filter, where the tail has no digest to plan from.
func TestReaderFollowSeesLaterAppends(t *testing.T) {
	l, app := specLog(t)
	app(&Message{Key: []byte("k:1"), Value: []byte("first")})
	app(&Message{Key: []byte("pad"), Value: []byte("padpadpad")})

	r, err := l.NewReader(KeyPrefix([]byte("k:")), Follow())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	headers := make([]byte, 28)

	msg, _, _, _, err := r.ReadMessage(ctx, headers)
	require.NoError(t, err)
	require.Equal(t, "first", string(msg.Value()))

	go func() {
		time.Sleep(100 * time.Millisecond)
		app(&Message{Key: []byte("other"), Value: []byte("skipped")})
		app(&Message{Key: []byte("k:2"), Value: []byte("second")})
	}()

	msg, _, _, _, err = r.ReadMessage(ctx, headers)
	require.NoError(t, err)
	require.Equal(t, "second", string(msg.Value()),
		"a following filtered reader must skip non-matching tail records and return the next match")
}

// Committed-only is the default; uncommitted must be asked for.
func TestReaderCommittedByDefault(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{Path: tempDir(t), MaxSegmentBytes: 64, Compact: true})
	defer cleanup()

	offs, err := l.Append([]*Message{{Key: []byte("k:1"), Value: []byte("committed")}})
	require.NoError(t, err)
	l.SetHighWatermark(offs[0])
	_, err = l.Append([]*Message{{Key: []byte("k:2"), Value: []byte("uncommitted")}})
	require.NoError(t, err)

	r, err := l.NewReader()
	require.NoError(t, err)
	got := drainReader(t, r)
	require.Len(t, got, 1, "the record above the high watermark must not be returned")
	require.Equal(t, "committed", string(got[0].msg.Value()))

	r, err = l.NewReader(Uncommitted())
	require.NoError(t, err)
	require.Len(t, drainReader(t, r), 2, "Uncommitted must reveal it")
}

// ---- the refused combination ----

func TestReaderRefusesUnclassifiableCombination(t *testing.T) {
	l, app := specLog(t)
	app(&Message{Key: []byte("k:1"), Value: []byte("v")})
	app(&Message{Key: []byte("pad"), Value: []byte("padpadpad")})

	_, err := l.NewReader(KeyPrefix([]byte("k:")), Uncommitted())
	require.Error(t, err, "reading past the commit boundary with markers filtered out is unusable")
	require.Contains(t, err.Error(), "cannot classify")

	// Either escape hatch makes it legal.
	_, err = l.NewReader(KeyPrefix([]byte("k:")), Uncommitted(), Until(l.HighWatermark()))
	require.NoError(t, err, "bounding the read at a commit boundary must be accepted")
	_, err = l.NewReader(KeyPrefix([]byte("k:")), Uncommitted(), IncludeControl())
	require.NoError(t, err, "taking the markers must be accepted")

	// And neither is needed when the read stays below the boundary.
	_, err = l.NewReader(KeyPrefix([]byte("k:")))
	require.NoError(t, err)
}

func TestReaderIncludeControlReturnsMarkers(t *testing.T) {
	l, app := specLog(t)
	app(&Message{Key: []byte("k:1"), Value: []byte("v")})
	app(&Message{Attributes: AttrControl, Value: []byte("marker")})
	app(&Message{Key: []byte("k:2"), Value: []byte("w")})
	app(&Message{Key: []byte("pad"), Value: []byte("padpadpad")})

	r, err := l.NewReader(KeyPrefix([]byte("k:")))
	require.NoError(t, err)
	for _, rec := range drainReader(t, r) {
		require.Zero(t, rec.msg.Attributes()&AttrControl, "markers are keyless and must be dropped")
	}

	r, err = l.NewReader(KeyPrefix([]byte("k:")), IncludeControl())
	require.NoError(t, err)
	var markers int
	var offs []int64
	for _, rec := range drainReader(t, r) {
		offs = append(offs, rec.off)
		if rec.msg.Attributes()&AttrControl != 0 {
			markers++
		}
	}
	require.Equal(t, 1, markers, "IncludeControl must return the marker")
	for i := 1; i < len(offs); i++ {
		require.Greater(t, offs[i], offs[i-1], "markers must arrive in offset order with the rest")
	}
}

// ---- SkipSuperseded ----

func TestSkipSupersededDropsWithinSegmentOnly(t *testing.T) {
	// Segments large enough to hold many copies of a key.
	l, cleanup := setupWithOptions(t, Options{
		Path: tempDir(t), MaxSegmentBytes: 2000, Compact: true,
	})
	defer cleanup()
	app := func(m *Message) int64 {
		offs, err := l.Append([]*Message{m})
		require.NoError(t, err)
		l.SetHighWatermark(offs[0])
		return offs[0]
	}
	for i := 0; i < 200; i++ {
		app(&Message{Key: []byte("k:same"), Value: []byte(fmt.Sprintf("v%03d", i))})
	}
	for i := 0; i < 20; i++ {
		app(&Message{Key: []byte(fmt.Sprintf("pad:%d", i)), Value: make([]byte, 200)})
	}

	plain, err := l.NewReader(KeyPrefix([]byte("k:")))
	require.NoError(t, err)
	all := drainReader(t, plain)
	require.Len(t, all, 200)

	skipped, err := l.NewReader(KeyPrefix([]byte("k:")), SkipSuperseded())
	require.NoError(t, err)
	got := drainReader(t, skipped)

	require.Less(t, len(got), len(all), "supersessions within a segment must be dropped")
	sealed := len(l.Segments()) - 1
	require.LessOrEqual(t, len(got), sealed+1,
		"at most one copy per key per segment: got %d over %d sealed segments", len(got), sealed)
	require.Equal(t, "v199", string(got[len(got)-1].msg.Value()),
		"the newest copy must always survive")

	// It never returns a stale value for a key it reports at all: each returned
	// copy is the last one in its own segment.
	spec, err := l.resolve([]ReadOption{KeyPrefix([]byte("k:")), SkipSuperseded()})
	require.NoError(t, err)
	requireRecsEq(t, scanFiltered(t, l, spec), got, "skipSuperseded vs independent scan")
}

// The documented asymmetry: what counts as superseded depends on where the read
// began, so a resuming reader can return MORE records than one that read the
// whole segment — never fewer, and never a stale value for a key it reports.
func TestSkipSupersededResumeReturnsNoFewer(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path: tempDir(t), MaxSegmentBytes: 2000, Compact: true,
	})
	defer cleanup()
	app := func(m *Message) int64 {
		offs, err := l.Append([]*Message{m})
		require.NoError(t, err)
		l.SetHighWatermark(offs[0])
		return offs[0]
	}
	var offs []int64
	for i := 0; i < 120; i++ {
		offs = append(offs, app(&Message{
			Key: []byte(fmt.Sprintf("k:%d", i%3)), Value: []byte(fmt.Sprintf("v%03d", i)),
		}))
	}
	for i := 0; i < 20; i++ {
		app(&Message{Key: []byte(fmt.Sprintf("pad:%d", i)), Value: make([]byte, 200)})
	}

	full, err := l.NewReader(KeyPrefix([]byte("k:")), SkipSuperseded())
	require.NoError(t, err)
	fromStart := drainReader(t, full)

	for _, resume := range []int64{offs[10], offs[50], offs[100]} {
		r, err := l.NewReader(From(resume), KeyPrefix([]byte("k:")), SkipSuperseded())
		require.NoError(t, err)
		got := drainReader(t, r)

		var tailOfFull []readRec
		for _, rec := range fromStart {
			if rec.off >= resume {
				tailOfFull = append(tailOfFull, rec)
			}
		}
		require.GreaterOrEqual(t, len(got), len(tailOfFull),
			"resuming at %d returned FEWER records than the same suffix of a full read", resume)

		// Every record the full read reported at or after the resume point must
		// still be there: resuming may add, never remove.
		have := map[int64]bool{}
		for _, rec := range got {
			have[rec.off] = true
		}
		for _, rec := range tailOfFull {
			require.True(t, have[rec.off],
				"resuming at %d lost offset %d, which a full read returned", resume, rec.off)
		}
	}
}

// ---- acceleration ----

// The point of the digests: a filtered read must not read records from segments
// that hold no match. Everything else here would pass if it scanned the log.
func TestReaderKeyPrefixSkipsSegmentsWithoutHits(t *testing.T) {
	l, app := specLog(t)
	for i := 0; i < 60; i++ {
		app(&Message{Key: []byte(fmt.Sprintf("other:%03d", i)), Value: []byte("miss")})
	}
	offWant := app(&Message{Key: []byte("want:1"), Value: []byte("hit")})
	for i := 0; i < 4; i++ {
		app(&Message{Key: []byte(fmt.Sprintf("pad:%d", i)), Value: []byte("padpadpadpad")})
	}
	require.Less(t, offWant, l.ActiveSegmentBase())
	requireCleanOK(t, l, CleanSpec{Ceiling: l.HighWatermark()})

	sealed := len(l.Segments()) - 1
	require.Greater(t, sealed, 10)

	before := segmentScans.Load()
	r, err := l.NewReader(KeyPrefix([]byte("want:")), Until(offWant))
	require.NoError(t, err)
	got := drainReader(t, r)
	scans := segmentScans.Load() - before

	require.Len(t, got, 1)
	require.Equal(t, "hit", string(got[0].msg.Value()))
	require.LessOrEqual(t, scans, int64(1),
		"scanned %d segments for 1 hit across %d sealed segments — digests unused", scans, sealed)
}

// ---- tiered ----

func TestReaderKeyPrefixOverTieredSegments(t *testing.T) {
	l, bound := offloadedPrefixLog(t)
	l.Options.PrefixReadConcurrency = 2
	l.Options.PrefixReadTierConcurrency = 16

	r, err := l.NewReader(KeyPrefix([]byte("want:")), Until(bound))
	require.NoError(t, err)
	got := drainReader(t, r)

	spec, err := l.resolve([]ReadOption{KeyPrefix([]byte("want:")), Until(bound)})
	require.NoError(t, err)
	requireRecsEq(t, scanFiltered(t, l, spec), got, "tiered segments")

	var sawTomb bool
	for _, rec := range got {
		if bytes.Equal(rec.msg.Key(), []byte("want:tomb")) {
			sawTomb = true
			require.NotZero(t, rec.msg.Attributes()&AttrTombstone)
		}
	}
	require.True(t, sawTomb, "a tombstone must survive the trip through the store")
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

// ---- per-tier routing ----

// segmentScans counts one scanner per RUN, so the request pattern is
// observable: tightening the TIER budget on an offloaded log must split it into
// more runs, and changing the LOCAL budget must do nothing at all.
func TestPrefixReadTierBudgetGovernsTieredSegments(t *testing.T) {
	l, bound := offloadedPrefixLog(t)
	opts := []ReadOption{KeyPrefix([]byte("want:")), Until(bound)}
	spec, err := l.resolve(opts)
	require.NoError(t, err)
	want := scanFiltered(t, l, spec)
	require.NotEmpty(t, want)

	runsFor := func() int64 {
		before := segmentScans.Load()
		r, err := l.NewReader(opts...)
		require.NoError(t, err)
		requireRecsEq(t, want, drainReader(t, r), "tier budget must not change the answer")
		return segmentScans.Load() - before
	}

	l.Options.PrefixReadTierCoalesceBytes = 1 << 30
	coalesced := runsFor()

	l.Options.PrefixReadTierCoalesceBytes = -1
	split := runsFor()
	require.Greater(t, split, coalesced,
		"a negative tier budget must split runs (%d) beyond the coalesced count (%d)", split, coalesced)

	l.Options.PrefixReadTierCoalesceBytes = 1 << 30
	for _, local := range []int64{-1, 1, 1 << 30} {
		l.Options.PrefixReadCoalesceBytes = local
		require.Equal(t, coalesced, runsFor(),
			"local budget %d changed the read pattern of TIERED segments", local)
	}
}

// Whatever the budgets do to the request pattern, the records must be
// identical: they are cost knobs, never correctness ones.
func TestPrefixReadBudgetsAreCostOnly(t *testing.T) {
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

	opts := []ReadOption{KeyPrefix([]byte("want:"))}
	spec, err := l.resolve(opts)
	require.NoError(t, err)
	want := scanFiltered(t, l, spec)
	require.NotEmpty(t, want)

	for _, coalesce := range []int64{-1, 1, 64, 1 << 12, 1 << 30} {
		for _, conc := range []int{1, 4, 64} {
			l.Options.PrefixReadCoalesceBytes = coalesce
			l.Options.PrefixReadConcurrency = conc
			r, err := l.NewReader(opts...)
			require.NoError(t, err)
			requireRecsEq(t, want, drainReader(t, r),
				fmt.Sprintf("coalesce=%d conc=%d", coalesce, conc))
		}
	}
}

// denseSegLog rolls segments that hold MANY records, so several wanted records
// land in one segment — the case that distinguishes per-run fan-out from
// per-segment fan-out.
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

// Concurrency is bounded by RUNS, not by segments, so hits concentrated in a
// few segments still parallelise.
func TestPlanRunsFanOutIsNotCappedBySegmentCount(t *testing.T) {
	l, app := denseSegLog(t)
	for i := 0; i < 300; i++ {
		if i%5 == 0 {
			app(&Message{Key: []byte(fmt.Sprintf("want:%03d", i)), Value: []byte("hit")})
			continue
		}
		app(&Message{Key: []byte(fmt.Sprintf("other:%03d", i)), Value: []byte("miss")})
	}
	for i := 0; i < 60; i++ {
		app(&Message{Key: []byte(fmt.Sprintf("pad:%03d", i)), Value: []byte("padpadpadpad")})
	}

	segs := l.Segments()
	sealed := segs[:len(segs)-1]
	require.Greater(t, len(sealed), 2)

	spec, err := l.resolve([]ReadOption{KeyPrefix([]byte("want:"))})
	require.NoError(t, err)

	var segsWithHits, totalHits int
	countRuns := func(coalesce int64) int {
		n := 0
		for _, seg := range sealed {
			d := loadKeyDigest(seg)
			if d == nil {
				d, err = buildKeyDigest(seg, newBlockCache())
				require.NoError(t, err)
			}
			hits, err := digestHits(d, spec, 0, -1)
			require.NoError(t, err)
			if len(hits) == 0 {
				continue
			}
			runs, err := planRuns(seg, 0, hits, coalesce)
			require.NoError(t, err)
			n += len(runs)
		}
		return n
	}
	for _, seg := range sealed {
		d := loadKeyDigest(seg)
		if d == nil {
			d, err = buildKeyDigest(seg, newBlockCache())
			require.NoError(t, err)
		}
		hits, err := digestHits(d, spec, 0, -1)
		require.NoError(t, err)
		if len(hits) > 0 {
			segsWithHits++
			totalHits += len(hits)
		}
	}
	require.Greater(t, totalHits, segsWithHits,
		"test needs segments holding MORE than one hit to be meaningful")

	split := countRuns(0)
	require.Greater(t, split, segsWithHits,
		"per-run planning must exceed the %d-segment ceiling, got %d runs", segsWithHits, split)

	merged := countRuns(1 << 30)
	require.Equal(t, segsWithHits, merged,
		"a large coalesce budget must collapse to one run per segment")
}

func TestCoalesceBudgetResolution(t *testing.T) {
	require.Equal(t, int64(4096), coalesceBudget(0, 4096))
	require.Equal(t, int64(0), coalesceBudget(-1, 4096))
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
	require.Equal(t, []byte{'a', 0xFF}, prefixUpperBound([]byte{'a', 0xFE}))
	require.Equal(t, []byte{'b'}, prefixUpperBound([]byte{'a', 0xFF}))
	require.Nil(t, prefixUpperBound([]byte{0xFF, 0xFF}), "all-0xFF runs to the end")
}
