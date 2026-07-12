package commitlog

import (
	"os"
	"testing"
	"time"
)

// A second clean over an already-converged log must not rewrite any sealed
// segment: compaction reaches a fixed point (nothing removed, nothing
// stripped ⇒ keep the ORIGINAL segment file). Without this, every clean
// rewrote + fsynced the entire decided prefix on every cadence tick —
// measured as multi-second commit stalls on large steady-state logs.
func TestCleanConvergesToNoRewrites(t *testing.T) {
	l, app := specLog(t)
	for i := 0; i < 6; i++ {
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
	if err := l.CleanWithSpec(spec); err != nil {
		t.Fatal(err)
	}

	// Record the sealed segments' file identities after the first clean.
	type ident struct {
		path string
		mod  time.Time
		size int64
	}
	sealedIdents := func() []ident {
		l.mu.RLock()
		defer l.mu.RUnlock()
		var out []ident
		for _, seg := range l.segments[:len(l.segments)-1] {
			fi, err := os.Stat(seg.logPath())
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, ident{seg.logPath(), fi.ModTime(), fi.Size()})
		}
		return out
	}
	before := sealedIdents()
	if len(before) == 0 {
		t.Fatal("test setup: expected sealed segments")
	}

	// Second clean: converged — same files, untouched.
	if err := l.CleanWithSpec(spec); err != nil {
		t.Fatal(err)
	}
	after := sealedIdents()
	if len(after) != len(before) {
		t.Fatalf("converged clean changed segment count: %d -> %d", len(before), len(after))
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("converged clean rewrote segment %s (%+v -> %+v)", before[i].path, before[i], after[i])
		}
	}
}
