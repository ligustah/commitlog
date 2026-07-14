package commitlog

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// The compressed (block-mode) format must be OPERATIONALLY equivalent to the
// raw format: the same interleaving of appends, watermark moves, compaction
// passes, retention truncations and crash-style reopens (RecoverTail) must
// leave the same visible records. The sqlcdc soak diverged by a few rows the
// moment zstd was enabled; this test is the minimal layer that can indict
// the block machinery.
func TestCompressedOperationalEquivalence(t *testing.T) {
	for seed := int64(1); seed <= 3; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			results := map[string]map[int64]string{}
			for _, codec := range []compress.Codec{compress.None, compress.Zstd} {
				results[codecName(codec)] = runTorture(t, seed, codec)
			}
			raw, zstd := results["none"], results["zstd"]
			require.Equal(t, len(raw), len(zstd),
				"visible record count diverged: raw %d vs zstd %d", len(raw), len(zstd))
			for off, v := range raw {
				zv, ok := zstd[off]
				require.True(t, ok, "offset %d visible raw-only", off)
				require.Equal(t, v, zv, "offset %d content diverged", off)
			}
		})
	}
}

func codecName(c compress.Codec) string {
	if c == compress.Zstd {
		return "zstd"
	}
	return "none"
}

// runTorture drives one deterministic operation sequence and returns the
// final visible records (offset → key|value).
func runTorture(t *testing.T, seed int64, codec compress.Codec) map[int64]string {
	t.Helper()
	dir := t.TempDir()
	rng := rand.New(rand.NewSource(seed))
	opts := Options{
		Path:             dir,
		MaxSegmentBytes:  512, // roll constantly: many sealed segments
		Compact:          true,
		Compression:      codec,
		DisableAutoClean: true,
	}
	l, err := New(opts)
	require.NoError(t, err)
	defer func() { l.Close() }()

	nextVal := 0
	appendBatch := func(n int) {
		msgs := make([]*Message, n)
		for i := range msgs {
			nextVal++
			m := &Message{
				Key:   []byte(fmt.Sprintf("k%02d", rng.Intn(40))),
				Value: []byte(fmt.Sprintf("v%06d", nextVal)),
			}
			if rng.Intn(6) == 0 {
				m.Attributes |= AttrTombstone
			}
			if rng.Intn(2) == 0 {
				m.Headers = map[string][]byte{"pid": {1}, "seq": {byte(i)}}
			}
			msgs[i] = m
		}
		offs, err := l.Append(msgs)
		require.NoError(t, err)
		l.SetHighWatermark(offs[len(offs)-1])
	}

	for step := 0; step < 120; step++ {
		switch rng.Intn(10) {
		case 0, 1, 2, 3, 4, 5: // mostly appends
			appendBatch(1 + rng.Intn(8))
		case 6: // compaction under the daemon's spec shape: strip + tombstone GC
			hw := l.HighWatermark()
			require.NoError(t, l.CleanWithSpec(CleanSpec{
				Ceiling: hw, StripBelow: hw, StripHeaders: []string{"pid", "epoch", "seq"},
				TombstoneGCBelow: hw, TombstoneRetention: time.Nanosecond,
			}))
		case 7: // retention: drop a prefix
			if old := l.OldestOffset(); old >= 0 && l.HighWatermark() > old+20 {
				require.NoError(t, l.TruncateBefore(old+int64(rng.Intn(10))))
			}
		case 8: // crash-style reopen: stale checkpoint + RecoverTail
			hw := l.HighWatermark()
			require.NoError(t, l.Close())
			// The checkpoint file holds whatever the last graceful close wrote;
			// simulate staleness by rewriting it a little behind, like a kill
			// between checkpoint ticks.
			stale := hw - int64(rng.Intn(5))
			if stale < -1 {
				stale = -1
			}
			writeCheckpoint(t, dir, stale)
			l, err = New(opts)
			require.NoError(t, err)
			require.NoError(t, l.(*commitLog).RecoverTail())
		case 9: // read a random window (exercises the read path mid-life)
			readAllVisible(t, l)
		}
	}
	fhw := l.HighWatermark()
	require.NoError(t, l.CleanWithSpec(CleanSpec{
		Ceiling: fhw, StripBelow: fhw, StripHeaders: []string{"pid", "epoch", "seq"},
		TombstoneGCBelow: fhw, TombstoneRetention: time.Nanosecond,
	}))
	return readAllVisible(t, l)
}

func writeCheckpoint(t *testing.T, dir string, hw int64) {
	t.Helper()
	require.NoError(t,
		os.WriteFile(dir+"/replication-offset-checkpoint", []byte(fmt.Sprintf("%d", hw)), 0666))
}

func readAllVisible(t *testing.T, l CommitLog) map[int64]string {
	t.Helper()
	out := map[int64]string{}
	oldest := l.OldestOffset()
	if oldest < 0 {
		return out
	}
	r, err := l.NewReader(oldest, true)
	require.NoError(t, err)
	headers := make([]byte, 28)
	newest := l.NewestOffset()
	ctx := context.Background()
	for {
		msg, off, _, _, err := r.ReadMessage(ctx, headers)
		if err != nil {
			if errors.Is(err, ErrCommitLogReadonly) {
				break
			}
			t.Fatalf("read at %d: %v", off, err)
		}
		sm := SerializedMessage(msg)
		out[off] = string(sm.Key()) + "|" + string(sm.Value())
		if off >= newest {
			break
		}
	}
	return out
}
