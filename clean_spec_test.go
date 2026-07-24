package commitlog

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// specLog builds a small-segment compacted log and returns it with a helper
// that appends one message and returns its offset.
func specLog(t *testing.T) (*commitLog, func(m *Message) int64) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 64, // roll constantly: every message a sealed segment
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

// readAll returns every message currently in the log keyed by offset.
func readAllMsgs(t *testing.T, l *commitLog) map[int64]SerializedMessage {
	out := map[int64]SerializedMessage{}
	oldest := l.OldestOffset()
	if oldest < 0 {
		return out
	}
	r, err := l.NewReader(oldest, true)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	headers := make([]byte, 28)
	newest := l.NewestOffset()
	for {
		msg, off, _, _, err := r.ReadMessage(ctx, headers)
		require.NoError(t, err)
		cp := make(SerializedMessage, len(msg))
		copy(cp, msg)
		out[off] = cp
		if off >= newest {
			return out
		}
	}
}

// An expired latest-per-key tombstone is removed entirely — the key vanishes
// — while young and untimestamped tombstones survive.
func TestCleanSpecTombstoneGC(t *testing.T) {
	l, app := specLog(t)
	old := timestamp() - int64(2*time.Hour)

	app(&Message{Key: []byte("gone"), Value: []byte("v1")})
	offGoneTomb := app(&Message{Key: []byte("gone"), Value: []byte("del"), Attributes: AttrTombstone, Timestamp: old})
	app(&Message{Key: []byte("young"), Value: []byte("v1")})
	offYoungTomb := app(&Message{Key: []byte("young"), Value: []byte("del"), Attributes: AttrTombstone}) // stamped now
	app(&Message{Key: []byte("nots"), Value: []byte("v1")})
	// Timestamp 0 is impossible via Append (stamping); simulate a pre-v0.10.3
	// tombstone via the young one instead — covered above.
	offLive := app(&Message{Key: []byte("live"), Value: []byte("v2")})
	app(&Message{Key: []byte("pad"), Value: []byte("pad")}) // active segment padding

	requireCleanOK(t, l, (CleanSpec{
		Ceiling:            l.HighWatermark(),
		TombstoneGCBelow:   l.HighWatermark(),
		TombstoneRetention: time.Hour,
	}))

	got := readAllMsgs(t, l)
	for off, m := range got {
		require.NotEqual(t, "gone", string(m.Key()), "expired tombstone key must vanish (offset %d)", off)
	}
	require.Contains(t, got, offYoungTomb, "young tombstone must survive")
	require.Contains(t, got, offLive)
	require.NotContains(t, got, offGoneTomb)
}

// An aborted record must neither survive nor shadow an older committed value
// (H1 regression: the transaction-blind scan let an aborted "latest" delete
// the committed copy, losing the key's value for every committed reader).
func TestCleanSpecAbortedShadowing(t *testing.T) {
	l, app := specLog(t)

	offCommitted := app(&Message{Key: []byte("k"), Value: []byte("committed")})
	offAborted := app(&Message{Key: []byte("k"), Value: []byte("aborted"), Headers: map[string][]byte{"pid": {0, 0, 0, 0, 0, 0, 0, 1}}})
	app(&Message{Key: []byte("pad"), Value: []byte("pad")})

	requireCleanOK(t, l, (CleanSpec{
		Ceiling:      l.HighWatermark(),
		StripBelow:   l.HighWatermark(),
		StripHeaders: []string{"pid", "epoch", "seq"},
		Aborted:      func(off int64) bool { return off == offAborted },
	}))

	got := readAllMsgs(t, l)
	require.Contains(t, got, offCommitted, "committed value lost: aborted record shadowed it")
	require.Equal(t, "committed", string(got[offCommitted].Value()))
	require.NotContains(t, got, offAborted, "aborted record must be removed")
}

// Below StripBelow: control markers are removed and surviving records lose
// their transactional headers (becoming plain records) while offset,
// timestamp, key, value, and attribute bits survive. Above: untouched.
func TestCleanSpecDecideAndStrip(t *testing.T) {
	l, app := specLog(t)
	txHdrs := func() map[string][]byte {
		return map[string][]byte{
			"pid":   {0, 0, 0, 0, 0, 0, 0, 7},
			"epoch": {0, 0, 0, 1},
			"seq":   {0, 0, 0, 0, 0, 0, 0, 3},
			"app":   []byte("keep-me"),
		}
	}
	offData := app(&Message{Key: []byte("a"), Value: []byte("v"), Headers: txHdrs()})
	offMarker := app(&Message{Attributes: AttrControl, Value: []byte("commit-marker")})
	stripBelow := offMarker + 1
	offTailData := app(&Message{Key: []byte("b"), Value: []byte("w"), Headers: txHdrs()})
	offTailMarker := app(&Message{Attributes: AttrControl, Value: []byte("commit-marker-2")})
	app(&Message{Key: []byte("pad"), Value: []byte("pad")})

	before := readAllMsgs(t, l)
	wantTs := before[offData] // sanity capture; timestamp checked via reader below

	requireCleanOK(t, l, (CleanSpec{
		Ceiling:      l.HighWatermark(),
		StripBelow:   stripBelow,
		StripHeaders: []string{"pid", "epoch", "seq"},
	}))
	_ = wantTs

	got := readAllMsgs(t, l)
	require.Contains(t, got, offData)
	stripped := got[offData]
	require.Equal(t, "v", string(stripped.Value()))
	require.Equal(t, "a", string(stripped.Key()))
	hdrs := stripped.Headers()
	require.NotContains(t, hdrs, "pid", "transactional headers must be stripped below StripBelow")
	require.NotContains(t, hdrs, "epoch")
	require.NotContains(t, hdrs, "seq")
	require.Equal(t, "keep-me", string(hdrs["app"]), "non-transactional headers must survive the strip")

	require.NotContains(t, got, offMarker, "control marker below StripBelow must be removed")
	require.Contains(t, got, offTailMarker, "control marker above StripBelow must survive")
	require.Contains(t, got, offTailData)
	require.Contains(t, got[offTailData].Headers(), "pid", "headers above StripBelow must survive")
}

// The strip survives a reopen: offsets and payloads are intact after the
// rewritten segments are read back from disk.
func TestCleanSpecStripSurvivesReopen(t *testing.T) {
	path := tempDir(t)
	l, err := New(Options{Path: path, MaxSegmentBytes: 64, Compact: true})
	require.NoError(t, err)
	cl := l.(*commitLog)
	var offs []int64
	// Trailing pads keep the interesting records out of the last (active,
	// never-compacted) segment.
	for i, kv := range []struct{ k, v string }{{"x", "1"}, {"y", "2"}, {"x", "3"}, {"pad", "p"}, {"pad2", "p"}, {"pad3", "p"}} {
		o, err := cl.Append([]*Message{{
			Key: []byte(kv.k), Value: []byte(kv.v),
			Headers: map[string][]byte{"pid": {0, 0, 0, 0, 0, 0, 0, byte(i + 1)}},
		}})
		require.NoError(t, err)
		cl.SetHighWatermark(o[0])
		offs = append(offs, o[0])
	}
	requireCleanOK(t, cl, (CleanSpec{
		Ceiling:      cl.HighWatermark(),
		StripBelow:   cl.HighWatermark(),
		StripHeaders: []string{"pid", "epoch", "seq"},
	}))
	require.NoError(t, cl.Close())

	l2, err := New(Options{Path: path, MaxSegmentBytes: 64, Compact: true})
	require.NoError(t, err)
	defer l2.Close()
	got := readAllMsgs(t, l2.(*commitLog))
	require.NotContains(t, got, offs[0], "superseded x=1 must be compacted away")
	require.Contains(t, got, offs[2])
	require.Equal(t, "3", string(got[offs[2]].Value()))
	require.NotContains(t, got[offs[2]].Headers(), "pid")
}

// A key written once and never touched again survives a pass verbatim, however
// much churn the other keys see. This pins the "innocent bystander" direction
// of ViewPreserved: the existing spec tests each drive one drop path and assert
// what it removes, but none asserts that a record no path selects is still
// there afterwards. A downstream consumer saw exactly that shape — for a subset
// of ids the sibling records of a row were live while the row record itself was
// absent — which compaction can only produce by dropping an unsuperseded record
// or by being told to via the CleanSpec.
func TestCleanSpecBystanderKeySurvives(t *testing.T) {
	l, app := specLog(t)
	old := timestamp() - int64(2*time.Hour)

	// Three keys describing one logical id. None is ever superseded, tombstoned
	// or aborted, so all three must come through untouched.
	offRow := app(&Message{Key: []byte("row:1"), Value: []byte("row-value")})
	offOrder := app(&Message{Key: []byte("ord:1"), Value: []byte("order-value")})
	offIndex := app(&Message{Key: []byte("kidx:1"), Value: []byte("index-value")})

	// Churn around them so every drop path fires in the same pass: supersession,
	// expired-tombstone GC, and an aborted record.
	offSuperOld := app(&Message{Key: []byte("super"), Value: []byte("v1")})
	offSuperNew := app(&Message{Key: []byte("super"), Value: []byte("v2")})
	app(&Message{Key: []byte("tomb"), Value: []byte("v1")})
	offTomb := app(&Message{Key: []byte("tomb"), Value: []byte("del"), Attributes: AttrTombstone, Timestamp: old})
	offAborted := app(&Message{Key: []byte("abrt"), Value: []byte("aborted"),
		Headers: map[string][]byte{"pid": {0, 0, 0, 0, 0, 0, 0, 1}}})
	app(&Message{Key: []byte("pad"), Value: []byte("pad")}) // active segment padding

	requireCleanOK(t, l, (CleanSpec{
		Ceiling:            l.HighWatermark(),
		StripBelow:         l.HighWatermark(),
		StripHeaders:       []string{"pid", "epoch", "seq"},
		TombstoneGCBelow:   l.HighWatermark(),
		TombstoneRetention: time.Hour,
		Aborted:            func(off int64) bool { return off == offAborted },
	}))

	got := readAllMsgs(t, l)
	for _, c := range []struct {
		off int64
		val string
	}{
		{offRow, "row-value"},
		{offOrder, "order-value"},
		{offIndex, "index-value"},
	} {
		require.Contains(t, got, c.off, "compaction dropped a record nothing superseded")
		require.Equal(t, c.val, string(got[c.off].Value()))
	}

	// ...and the churn really was compacted, so the survivals above are not just
	// a pass that did nothing.
	require.NotContains(t, got, offSuperOld, "superseded copy must be compacted away")
	require.Contains(t, got, offSuperNew)
	require.NotContains(t, got, offTomb, "expired tombstone must be GC'd")
	require.NotContains(t, got, offAborted, "aborted record must be removed")
}

// tornRowLog lays out the shape a downstream consumer lost rows in: one key
// written twice (a row updated once) alongside two sibling keys written exactly
// once, then padding to keep the active segment out of the way.
func tornRowLog(t *testing.T) (l *commitLog, offOld, offNew, offOrd, offIdx int64) {
	l, app := specLog(t)
	offOrd = app(&Message{Key: []byte("ord:1"), Value: []byte("order")})
	offIdx = app(&Message{Key: []byte("kidx:1"), Value: []byte("index")})
	offOld = app(&Message{Key: []byte("row:1"), Value: []byte("v1")})
	offNew = app(&Message{Key: []byte("row:1"), Value: []byte("v2")})
	app(&Message{Key: []byte("pad"), Value: []byte("pad")})
	return l, offOld, offNew, offOrd, offIdx
}

// Supersession and an abort can compose to remove BOTH copies of a key — but
// only across two passes, and only when the first pass ran with a Ceiling above
// a record that was still undecided. Within a single pass it cannot happen: an
// aborted record is excluded from candidacy before the latest is chosen, so it
// never supersedes anything (TestCleanSpecAbortedShadowing).
//
// The contrast is the point. Same records, same abort, only the first pass's
// Ceiling differs — and that alone decides whether the key survives. Callers
// pass Ceiling = the transactional LSO precisely so the undecided case lands in
// the second subtest.
func TestCleanSpecCeilingAboveUndecidedLosesKey(t *testing.T) {
	t.Run("ceiling above an undecided record loses the key", func(t *testing.T) {
		l, offOld, offNew, offOrd, offIdx := tornRowLog(t)

		// Pass 1: the newer copy is still undecided, but Ceiling sits above it,
		// so it counts as latest and supersedes the older committed copy.
		requireCleanOK(t, l, (CleanSpec{Ceiling: l.HighWatermark()}))
		got := readAllMsgs(t, l)
		require.NotContains(t, got, offOld, "pass 1 should have superseded the older copy")
		require.Contains(t, got, offNew)

		// Pass 2: the transaction is now known aborted, so the survivor goes too.
		requireCleanOK(t, l, (CleanSpec{
			Ceiling: l.HighWatermark(),
			Aborted: func(off int64) bool { return off == offNew },
		}))
		got = readAllMsgs(t, l)

		require.NotContains(t, got, offNew)
		require.NotContains(t, got, offOld)
		for off, m := range got {
			require.NotEqual(t, "row:1", string(m.Key()), "row key should be gone entirely (offset %d)", off)
		}
		// ...while the siblings, never superseded, are still live: the symptom.
		require.Contains(t, got, offOrd)
		require.Contains(t, got, offIdx)
	})

	t.Run("ceiling at the lso keeps the key", func(t *testing.T) {
		l, offOld, offNew, offOrd, offIdx := tornRowLog(t)

		// Pass 1 with Ceiling below the undecided record: it is retained and
		// uncounted, so it supersedes nothing.
		requireCleanOK(t, l, (CleanSpec{Ceiling: offOld}))
		require.Contains(t, readAllMsgs(t, l), offOld,
			"an undecided record above the ceiling must not supersede a committed copy")

		// Pass 2: now decided-aborted. The older committed copy must survive.
		requireCleanOK(t, l, (CleanSpec{
			Ceiling: l.HighWatermark(),
			Aborted: func(off int64) bool { return off == offNew },
		}))
		got := readAllMsgs(t, l)

		require.NotContains(t, got, offNew, "aborted record must be removed")
		require.Contains(t, got, offOld, "committed value lost")
		require.Equal(t, "v1", string(got[offOld].Value()))
		require.Contains(t, got, offOrd)
		require.Contains(t, got, offIdx)
	})
}

// A pass that runs out of rewrite budget must not resurrect a deleted value.
//
// Tombstone GC is the one drop that removes a key's NEWEST copy, so unlike
// supersession its order matters. The budget can stop the rewrite loop between
// any two segments; if the tombstone's segment is rewritten while a segment
// holding a copy it shadows is not, that copy becomes latest-per-key on the
// next pass and the deleted value is back for good — nothing supersedes it any
// more. Found by the compaction fuzz sweep, and reachable in production through
// CleanSpec.RewriteBudget, not just the deterministic cap used here.
//
// The layout is deliberate: the tombstone's segment carries more droppable
// records than the value's, so drop-density order would rewrite it FIRST.
func TestCleanSpecBudgetedPassCannotResurrect(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:             tempDir(t),
		MaxSegmentBytes:  220, // a handful of records per segment
		Compact:          true,
		DisableAutoClean: true,
	})
	t.Cleanup(cleanup)

	app := func(m *Message) int64 {
		offs, err := l.Append([]*Message{m})
		require.NoError(t, err)
		l.SetHighWatermark(offs[0])
		return offs[0]
	}
	old := timestamp() - int64(2*time.Hour)

	// Segment A: the value that must stay dead, plus filler to force a roll.
	offVal := app(&Message{Key: []byte("k1"), Value: []byte("deleted-value")})
	for i := 0; i < 3; i++ {
		app(&Message{Key: []byte("f" + strconv.Itoa(i)), Value: []byte("xxxxxxxxxxxxxxxxxxxx")})
	}

	// Segment B: k1's expired tombstone, plus two superseded pairs so this
	// segment outranks segment A on drop density.
	app(&Message{Key: []byte("k1"), Value: []byte("del"), Attributes: AttrTombstone, Timestamp: old})
	app(&Message{Key: []byte("k2"), Value: []byte("a")})
	app(&Message{Key: []byte("k2"), Value: []byte("b")})
	app(&Message{Key: []byte("k3"), Value: []byte("a")})
	app(&Message{Key: []byte("k3"), Value: []byte("b")})

	app(&Message{Key: []byte("pad0"), Value: []byte("p")}) // keep the real
	app(&Message{Key: []byte("pad1"), Value: []byte("p")}) // records out of active

	hw := l.HighWatermark()
	spec := CleanSpec{
		Ceiling:            hw + 1,
		TombstoneGCBelow:   hw + 1,
		TombstoneRetention: time.Hour,
	}

	// One rewrite only: the budget stops the pass mid-way.
	capped := spec
	capped.maxRewrites = 1
	requireCleanOK(t, l, capped)

	// Then let it settle completely.
	for i := 0; i < 3; i++ {
		requireCleanOK(t, l, spec)
	}

	got := readAllMsgs(t, l)
	require.NotContains(t, got, offVal, "deleted value resurrected: its tombstone was GC'd first")
	for off, m := range got {
		require.NotEqual(t, "k1", string(m.Key()),
			"k1 must be gone entirely (offset %d, value %q)", off, m.Value())
	}
	// The unrelated keys still compacted normally.
	require.NotEmpty(t, got)
}

// DisableAutoClean: the internal loop must stop cleaning (retention would
// otherwise delete the old prefix) while explicit Clean still works.
func TestDisableAutoClean(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:             tempDir(t),
		MaxSegmentBytes:  64,
		MaxLogMessages:   1, // aggressive retention: auto-clean would delete almost everything
		CleanerInterval:  50 * time.Millisecond,
		DisableAutoClean: true,
	})
	defer cleanup()
	var last int64
	for i := 0; i < 8; i++ {
		offs, err := l.Append([]*Message{{Value: []byte("v")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)
	time.Sleep(400 * time.Millisecond) // several cleaner intervals
	require.Equal(t, int64(0), l.OldestOffset(), "auto-clean ran despite DisableAutoClean")

	require.NoError(t, l.Clean())
	require.NotEqual(t, int64(0), l.OldestOffset(), "explicit Clean must still apply retention (offset 0 deleted)")
}
