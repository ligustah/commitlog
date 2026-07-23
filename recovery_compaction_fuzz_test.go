package commitlog

// A seeded property/fuzz harness that sweeps the compaction + recovery space
// (latest-per-key, abort-shadowing, tombstone-GC, convergence, and — in later
// phases — crash + RecoverTail). It generalizes the fixed-point regressions in
// clean_spec_test.go / clean_convergence_test.go / recover_tail_test.go into a
// randomized sweep driven by a []byte entropy stream: plain `go test` replays
// the seed corpus deterministically; `go test -fuzz=FuzzCompaction...` runs the
// coverage-guided sweep. A failing input auto-persists to testdata/fuzz and
// reproduces exactly, no printed seed needed.
//
// Invariant → code map:
//   latest-per-key / superseded-drop ...... compact_cleaner.go mergeDigests
//   abort-shadowing exclusion ............. compact_cleaner.go (Aborted skip)
//   tombstone GC (retention-gated) ........ compact_cleaner.go + CleanSpec
//   control-marker + header stripping ..... CleanSpec.StripBelow
//   convergence (fixed point) ............. compact_cleaner.go cleanSegment
//   (phase 2+) torn-tail / stale-ckpt ..... commitlog.go RecoverTail

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ---- entropy stream: the fuzz input drives every workload decision ----

type fzStream struct {
	b []byte
	i int
}

func (s *fzStream) next() byte {
	if s.i >= len(s.b) {
		return 0
	}
	v := s.b[s.i]
	s.i++
	return v
}

func (s *fzStream) intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(s.next()) % n
}

func (s *fzStream) bool() bool { return s.next()&1 == 1 }

func (s *fzStream) more() bool { return s.i < len(s.b) }

// ---- oracle: the committed latest-per-key state a reader must observe ----

const (
	fzKeyKind  = 0 // latest committed record for the key is a data value
	fzTombKind = 1 // latest committed record for the key is a tombstone
)

type fzKeyState struct {
	kind    int    // fzKeyKind | fzTombKind
	val     string // for fzKeyKind: the exact surviving value
	tombOld bool   // for fzTombKind: stamped old enough to be GC-eligible
}

const (
	fzNumKeys       = 3
	fzMaxOps        = 40 // per-input cap keeps each iteration sub-second
	fzTombRetention = time.Hour
)

func fzKey(i int) string { return fmt.Sprintf("k%d", i) }

// txnHeaders returns the pid/epoch/seq headers a transactional record carries,
// which StripBelow removes and Aborted keys off.
func fzTxnHeaders() map[string][]byte {
	return map[string][]byte{
		"pid":   {0, 0, 0, 0, 0, 0, 0, 7},
		"epoch": {0, 0, 0, 1},
		"seq":   {0, 0, 0, 0, 0, 0, 0, 3},
	}
}

func FuzzCompactionRecovery(f *testing.F) {
	// Seed corpus: one curated input per known failure shape. Exact op decoding
	// is entropy-driven, so these just need to drive varied workloads; the
	// coverage-guided sweep explores the rest.
	f.Add([]byte{0, 0, 1, 1, 2, 2})                      // superseded values, same keys
	f.Add([]byte{2, 0, 0, 2, 0, 1})                      // tombstones + data
	f.Add([]byte{3, 0, 3, 0, 1, 1})                      // aborted records (shadow risk)
	f.Add([]byte{4, 0, 1, 4, 2, 5})                      // control markers + a sync fence
	f.Add([]byte{0, 1, 2, 3, 4, 5, 0, 1, 2, 3, 4, 5})    // one of everything
	f.Add([]byte{2, 1, 2, 1, 0, 1, 3, 2, 5, 0, 0, 4, 1}) // mixed, longer

	f.Fuzz(func(t *testing.T, data []byte) {
		s := &fzStream{b: data}

		dir := tempDir(t)
		opts := Options{
			Path:                 dir,
			MaxSegmentBytes:      64, // roll constantly: ~one record per segment
			Compact:              true,
			DisableAutoClean:     true,      // the harness drives every clean
			HWCheckpointInterval: time.Hour, // no background checkpoint races
			CleanerInterval:      time.Hour,
		}
		var l *commitLog
		reopen := func() {
			ll, err := New(opts)
			require.NoError(t, err)
			l = ll.(*commitLog)
		}
		reopen()
		t.Cleanup(func() {
			if l != nil {
				l.Close()
			}
		})

		expect := map[string]fzKeyState{}
		aborted := map[int64]bool{}
		valCounter := 0

		app := func(m *Message) int64 {
			offs, err := l.Append([]*Message{m})
			require.NoError(t, err)
			l.SetHighWatermark(offs[0])
			return offs[0]
		}

		// ---- build a randomized, committed workload ----
		for ops := 0; s.more() && ops < fzMaxOps; ops++ {
			switch s.intn(6) {
			case 0, 1: // committed data record
				k := fzKey(s.intn(fzNumKeys))
				valCounter++
				v := fmt.Sprintf("v%d", valCounter)
				app(&Message{Key: []byte(k), Value: []byte(v)})
				expect[k] = fzKeyState{kind: fzKeyKind, val: v}
			case 2: // tombstone (young or old)
				k := fzKey(s.intn(fzNumKeys))
				old := s.bool()
				m := &Message{Key: []byte(k), Value: []byte("del"), Attributes: AttrTombstone}
				if old {
					m.Timestamp = timestamp() - int64(2*time.Hour)
				}
				app(m)
				expect[k] = fzKeyState{kind: fzTombKind, tombOld: old}
			case 3: // aborted transactional record (must never shadow/survive)
				k := fzKey(s.intn(fzNumKeys))
				valCounter++
				off := app(&Message{
					Key: []byte(k), Value: []byte(fmt.Sprintf("v%d", valCounter)),
					Headers: fzTxnHeaders(),
				})
				aborted[off] = true // expect[k] intentionally unchanged
			case 4: // transactional control marker (stripped below StripBelow)
				app(&Message{Value: []byte("marker"), Attributes: AttrControl})
			case 5: // durability fence
				require.NoError(t, l.SyncAll())
			}
		}

		// Trailing unique pads keep every real record out of the active (never
		// compacted) segment, so the oracle sees fully-compacted survivors.
		app(&Message{Key: []byte("pad0"), Value: []byte("p")})
		app(&Message{Key: []byte("pad1"), Value: []byte("p")})

		hw := l.HighWatermark()
		spec := CleanSpec{
			Ceiling:            hw + 1,
			StripBelow:         hw + 1,
			StripHeaders:       []string{"pid", "epoch", "seq"},
			Aborted:            func(off int64) bool { return aborted[off] },
			TombstoneGCBelow:   hw + 1,
			TombstoneRetention: fzTombRetention,
		}

		// Budget-deferred passes: cap rewrites per pass (the deterministic
		// maxRewrites cap) so dense segments defer to a later pass, exercising
		// the partial-rewrite / debt-carry path. A superseded record dropped by
		// a partial pass must stay dropped; a live one is never lost.
		for p := s.intn(4); p > 0; p-- {
			bs := spec
			bs.maxRewrites = 1 + s.intn(2) // 1..2 segment rewrites this pass
			_, berr := l.CleanWithSpec(bs)
			require.NoError(t, berr)
		}

		// A final unbounded pass fully compacts, then the oracle must hold.
		_, err := l.CleanWithSpec(spec)
		require.NoError(t, err)
		got := fzReadAll(t, l)
		fzAssertOracle(t, got, expect, aborted)

		// Convergence: a second pass must not change the observable state.
		_, err = l.CleanWithSpec(spec)
		require.NoError(t, err)
		got2 := fzReadAll(t, l)
		require.Equal(t, got, got2, "compaction did not converge: second pass changed the log")
		fzAssertOracle(t, got2, expect, aborted)

		// ---- crash + recovery stage ----
		// A durability fence: fsync every segment and checkpoint the HW, so all
		// current data is durable. The tear then only ever damages bytes written
		// after the fence (a torn NEW record), or moves the checkpoint back —
		// never destroys durable data. So recovery must restore the log exactly.
		require.NoError(t, l.SyncAll())
		preCrash := fzReadAll(t, l)
		newestBefore := l.NewestOffset()
		require.NoError(t, l.Close())
		l = nil

		switch s.intn(3) {
		case 0: // torn new record: garbage at the tail of the active segment
			fzTearGarbage(t, dir)
		case 1: // stale checkpoint: HW persisted below the true durable tail
			fzStaleCheckpoint(t, dir, newestBefore, s)
		case 2: // both at once
			fzTearGarbage(t, dir)
			fzStaleCheckpoint(t, dir, newestBefore, s)
		}

		reopen()
		require.NoError(t, l.RecoverTail())
		require.EqualValues(t, newestBefore, l.NewestOffset(), "recovery changed the tail")
		require.EqualValues(t, newestBefore, l.HighWatermark(), "HW not restored to the durable tail")
		require.Equal(t, preCrash, fzReadAll(t, l), "committed state changed across crash+recovery")

		// Idempotent recovery: a second reopen (Close persisted the recovered HW)
		// recovers nothing and changes nothing.
		require.NoError(t, l.Close())
		l = nil
		reopen()
		require.NoError(t, l.RecoverTail())
		require.EqualValues(t, newestBefore, l.NewestOffset())
		require.Equal(t, preCrash, fzReadAll(t, l), "second reopen was not a no-op")
	})
}

// fzLogFiles returns the segment .log files in dir, sorted oldest-first (the
// last is the active segment).
func fzLogFiles(t *testing.T, dir string) []string {
	logs, err := filepath.Glob(filepath.Join(dir, "*"+logSuffix))
	require.NoError(t, err)
	sort.Strings(logs)
	return logs
}

// fzTearGarbage appends a partial/garbage frame to the active segment — a torn
// write of a new record. It never removes existing (durable) records.
func fzTearGarbage(t *testing.T, dir string) {
	logs := fzLogFiles(t, dir)
	if len(logs) == 0 {
		return
	}
	f, err := os.OpenFile(logs[len(logs)-1], os.O_APPEND|os.O_WRONLY, 0666)
	require.NoError(t, err)
	_, werr := f.Write([]byte{0xDE, 0xAD, 0xBE})
	require.NoError(t, werr)
	require.NoError(t, f.Close())
}

// fzStaleCheckpoint rewrites the HW checkpoint to a value at/below the true
// durable tail, modelling a checkpoint that fell behind before the crash.
// RecoverTail must forward-scan and extend the HW back to the real tail.
func fzStaleCheckpoint(t *testing.T, dir string, trueTail int64, s *fzStream) {
	stale := int64(s.intn(int(trueTail+2))) - 1 // in [-1, trueTail]
	require.NoError(t, os.WriteFile(filepath.Join(dir, hwFileName),
		[]byte(strconv.FormatInt(stale, 10)), 0666))
}

// fzReadAll returns every message currently in the log, keyed by offset.
func fzReadAll(t *testing.T, l *commitLog) map[int64]SerializedMessage {
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
		require.NoError(t, err, "log must be readable end to end")
		cp := make(SerializedMessage, len(msg))
		copy(cp, msg)
		out[off] = cp
		if off >= newest {
			return out
		}
	}
}

// fzAssertOracle checks the compacted survivors against the committed
// latest-per-key oracle: correct value survives, aborted never survives,
// tombstones obey retention-gated GC.
func fzAssertOracle(t *testing.T, got map[int64]SerializedMessage, expect map[string]fzKeyState, aborted map[int64]bool) {
	// No aborted record survived below the ceiling.
	for off := range got {
		require.False(t, aborted[off], "aborted record survived compaction at offset %d", off)
	}

	// Collapse survivors to latest-per-key for the real keys.
	type surv struct {
		off    int64
		isTomb bool
		val    string
	}
	live := map[string]surv{}
	for off, m := range got {
		k := string(m.Key())
		if k == "" || k == "pad0" || k == "pad1" {
			continue
		}
		cur, ok := live[k]
		if !ok || off > cur.off {
			live[k] = surv{off: off, isTomb: m.Attributes()&AttrTombstone != 0, val: string(m.Value())}
		}
	}

	for i := 0; i < fzNumKeys; i++ {
		k := fzKey(i)
		want, tracked := expect[k]
		s, present := live[k]
		switch {
		case !tracked:
			require.False(t, present, "key %q was never committed but survived", k)
		case want.kind == fzKeyKind:
			require.True(t, present, "committed value for key %q lost", k)
			require.False(t, s.isTomb, "value for key %q became a tombstone", k)
			require.Equal(t, want.val, s.val, "wrong survivor value for key %q", k)
		case want.kind == fzTombKind && want.tombOld:
			require.False(t, present, "expired tombstone for key %q was not GC'd", k)
		case want.kind == fzTombKind && !want.tombOld:
			require.True(t, present, "young tombstone for key %q was dropped", k)
			require.True(t, s.isTomb, "young tombstone for key %q lost its tombstone bit", k)
		}
	}
}
