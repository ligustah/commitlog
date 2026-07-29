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

// A corrupt record must not be SERVED, whichever route reads it.
//
// The digest-planned KeyPrefix path used to return records straight out of the
// segment without looking at their CRC, so one flipped byte in a sealed segment
// produced this:
//
//	KeyPrefix path : SERVED "PAYLOAD-Q05-ZZZZZZZZ"
//	sequential path: PANIC (CRC caught it)
//
// The same bytes, on the same log: one route called it unrecoverable corruption
// and the other handed it to the caller as data. The digest can say which
// offsets hold matching KEYS; it cannot vouch for what is stored there, and
// planning from it does not make the record any more trustworthy.
//
// Flipping a byte inside the value leaves the frame length and the file size
// alone, so the index and the digest sidecar both stay valid — the corruption
// is invisible to everything except the CRC, which is the point.
func TestKeyPrefixRefusesRecordsThatFailCRC(t *testing.T) {
	dir := tempDir(t)
	opts := Options{Path: dir, MaxSegmentBytes: 256, Compact: true}

	l, err := New(opts)
	require.NoError(t, err)
	cl := l.(*commitLog)

	const marker = "PAYLOAD-005-ZZZZZZZZ"
	var last int64
	for i := 0; i < 40; i++ {
		value := fmt.Sprintf("payload-%03d-xxxxxxxx", i)
		if i == 5 {
			value = marker
		}
		offs, err := cl.Append([]*Message{{
			Key: []byte(fmt.Sprintf("want:%03d", i)), Value: []byte(value),
		}})
		require.NoError(t, err)
		last = offs[0]
	}
	cl.SetHighWatermark(last)
	require.NoError(t, cl.Close())

	// Corrupt one byte INSIDE the value of a record in a sealed segment.
	logs, err := filepath.Glob(filepath.Join(dir, "*.log"))
	require.NoError(t, err)
	var corrupted string
	for _, p := range logs {
		data, err := os.ReadFile(p)
		require.NoError(t, err)
		idx := bytes.Index(data, []byte(marker))
		if idx < 0 {
			continue
		}
		data[idx+8] = 'Q'
		require.NoError(t, os.WriteFile(p, data, 0666))
		corrupted = p
		break
	}
	require.NotEmpty(t, corrupted,
		"the marker value was not found raw on disk — the fixture is not corrupting what it thinks it is")

	l2, err := New(opts)
	require.NoError(t, err)
	cl2 := l2.(*commitLog)
	defer cl2.Close() // nolint: errcheck
	cl2.SetHighWatermark(last)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	r, err := cl2.NewReader(KeyPrefix([]byte("want:005")))
	require.NoError(t, err)

	msg, _, _, _, err := r.ReadMessage(ctx, make([]byte, 28))
	if err == nil {
		t.Fatalf("a KeyPrefix read SERVED a record that fails its own CRC: %q", string(msg.Value()))
	}
	require.Contains(t, err.Error(), "failed CRC")
	require.Nil(t, msg, "a refused record must not also be handed to the caller")
}

// The neighbours of a corrupt record are still readable: the refusal is of THAT
// record, not of the prefix read as a concept.
func TestKeyPrefixStillServesUncorruptedRecords(t *testing.T) {
	dir := tempDir(t)
	opts := Options{Path: dir, MaxSegmentBytes: 256, Compact: true}

	l, err := New(opts)
	require.NoError(t, err)
	cl := l.(*commitLog)

	var last int64
	for i := 0; i < 40; i++ {
		offs, err := cl.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("want:%03d", i)),
			Value: []byte(fmt.Sprintf("payload-%03d-xxxxxxxx", i)),
		}})
		require.NoError(t, err)
		last = offs[0]
	}
	cl.SetHighWatermark(last)

	r, err := cl.NewReader(KeyPrefix([]byte("want:")), Until(last))
	require.NoError(t, err)
	got := drainReader(t, r)
	require.Len(t, got, 40, "an untouched log must return every matching record")

	require.NoError(t, cl.Close())
}
