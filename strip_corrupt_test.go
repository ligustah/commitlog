package commitlog

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Compaction must not LAUNDER a corrupt record.
//
// stripFrame re-encodes a message and recomputes its CRC. Handed a record that
// is already damaged, it signs the damage: the rewritten record carries a fresh,
// valid checksum over wrong bytes, and every later read — including the
// CRC-verifying one — reports it as sound. Worse than serving corruption once,
// because the evidence that the record was ever damaged is destroyed by the
// rewrite and cannot be recovered afterwards.
//
// Reported by durable_streams, who reproduced it against v0.39.0: their Clean
// always sets StripHeaders + StripBelow, so every pass ran the re-framing path.
//
// The record is carried through untouched instead. It keeps its failing
// checksum, stays as damaged as it was, and readers keep getting
// ErrCorruptRecord from it. The clean is NOT failed: the cleaner runs unattended
// on a timer, so refusing to finish would wedge compaction and retention behind
// one bad record until someone noticed — turning an unreadable record into a
// full disk.
func TestCompactionDoesNotResignCorruptRecords(t *testing.T) {
	dir := tempDir(t)
	opts := Options{Path: dir, MaxSegmentBytes: 256, Compact: true, DisableAutoClean: true}

	l, err := New(opts)
	require.NoError(t, err)
	cl := l.(*commitLog)

	txHeaders := func() map[string][]byte {
		return map[string][]byte{
			"pid":   {0, 0, 0, 0, 0, 0, 0, 7},
			"epoch": {0, 0, 0, 1},
			"seq":   {0, 0, 0, 0, 0, 0, 0, 3},
		}
	}
	const marker = "LAUNDER-005-ZZZZZZZZ"
	var last int64
	for i := 0; i < 40; i++ {
		value := fmt.Sprintf("payload-%03d-xxxxxxxx", i)
		if i == 5 {
			value = marker
		}
		offs, err := cl.Append([]*Message{{
			Key: []byte(fmt.Sprintf("k:%03d", i)), Value: []byte(value), Headers: txHeaders(),
		}})
		require.NoError(t, err)
		last = offs[0]
	}
	cl.SetHighWatermark(last)
	require.NoError(t, cl.Close())

	// Damage one record in a sealed segment, leaving length and file size alone.
	logs, err := filepath.Glob(filepath.Join(dir, "*.log"))
	require.NoError(t, err)
	var corrupted bool
	for _, p := range logs {
		data, err := os.ReadFile(p)
		require.NoError(t, err)
		idx := bytes.Index(data, []byte(marker))
		if idx < 0 {
			continue
		}
		data[idx+8] = 'Q'
		require.NoError(t, os.WriteFile(p, data, 0666))
		corrupted = true
		break
	}
	require.True(t, corrupted, "the marker value was not found raw on disk")

	l2, err := New(opts)
	require.NoError(t, err)
	cl2 := l2.(*commitLog)
	defer cl2.Close() // nolint: errcheck
	cl2.SetHighWatermark(last)

	// The pass that used to re-sign it. It must SUCCEED — refusing to finish
	// would wedge an unattended cleaner behind one bad record.
	hw := cl2.HighWatermark()
	_, err = cl2.CleanWithSpec(CleanSpec{
		Ceiling:      hw + 1,
		StripBelow:   hw + 1,
		StripHeaders: []string{"pid", "epoch", "seq"},
	})
	require.NoError(t, err, "one corrupt record must not fail the whole clean")

	// Read every record back. The corrupt one must still be REFUSED, and the
	// others must be untouched by its presence.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := cl2.NewReader(From(cl2.OldestOffset()), Uncommitted())
	require.NoError(t, err)

	var (
		hdr        = make([]byte, HeaderBufferLen)
		served     int
		corruptErr error
	)
	for i := 0; i < 60; i++ {
		_, _, _, _, rerr := r.ReadMessage(ctx, hdr)
		if rerr != nil {
			corruptErr = rerr
			break
		}
		served++
	}

	require.ErrorIs(t, corruptErr, ErrCorruptRecord,
		"compaction re-signed a corrupt record: it read back as valid after the strip")
	require.Equal(t, 5, served,
		"the records before the corrupt one must survive the pass unharmed")
}
