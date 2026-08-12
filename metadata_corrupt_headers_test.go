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

// The metadata read must refuse a corrupt record rather than panic on it.
//
// ReadMessageMetadata documents that it does NOT CRC-validate the payload: "a
// record corrupted on disk is returned here as data, where ReadMessage refuses
// it". Returned as data is the promise. What it actually did was hand the bytes
// to parseHeadersAfterValue, which indexes the payload with no bounds check at
// all — key length at buf[6:10], then buf[keyEnd:keyEnd+4], then a loop of
// buf[n:n+size] — so a damaged length field indexes past the end and takes the
// caller's process with it.
//
// Nothing upstream stands in the way. The frame header's CRC covers the record's
// IDENTITY — offset, timestamp, epoch, size — and the key length lives in the
// payload, which this path deliberately does not check. So the one field that
// decides how far parsing reaches is the one nothing verifies.
//
// This is the same defect the sequential path was fixed for, in a function that
// exists precisely so a caller can scan cheaply: LSO rebuild reads every record
// in the log through it. See TestSequentialReadReturnsCorruptRecordRatherThanPanicking
// for the argument about why a library must not panic in someone else's binary.
func TestMetadataReadRefusesACorruptRecordRatherThanPanicking(t *testing.T) {
	dir := tempDir(t)
	opts := Options{Path: dir, MaxSegmentBytes: 1 << 20}

	l, err := New(opts)
	require.NoError(t, err)
	cl := l.(*commitLog)

	const marker = "KEYMARKER-005-ZZZZ"
	var last int64
	for i := range 20 {
		key := fmt.Sprintf("k:%03d-xxxxxxxx", i)
		if i == 5 {
			key = marker
		}
		offs, err := cl.Append([]*Message{{
			Key:     []byte(key),
			Value:   []byte(fmt.Sprintf("value-%03d", i)),
			Headers: map[string][]byte{"h": []byte("v")},
		}})
		require.NoError(t, err)
		last = offs[0]
	}
	cl.SetHighWatermark(last)
	require.NoError(t, cl.Close())

	// The key length sits in the four bytes immediately before the key itself,
	// inside the payload — past everything the frame header's CRC covers.
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
		require.Greater(t, idx, 4, "no room for a key-length field before the key")
		// Far past the end of any record this log holds.
		encoding.PutUint32(data[idx-4:idx], 1<<20)
		require.NoError(t, os.WriteFile(p, data, 0666))
		corrupted = true
		break
	}
	require.True(t, corrupted, "the marker key was not found raw on disk")

	l2, err := New(opts)
	require.NoError(t, err)
	cl2 := l2.(*commitLog)
	defer cl2.Close() // nolint: errcheck
	cl2.SetHighWatermark(last)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	r, err := cl2.NewReader(From(0), Uncommitted())
	require.NoError(t, err)

	var (
		readErr  error
		panicked any
		served   int
	)
	func() {
		defer func() { panicked = recover() }()
		hdr := make([]byte, HeaderBufferLen)
		var payload []byte
		for range 20 {
			meta, buf, err := r.ReadMessageMetadata(ctx, hdr, payload)
			if err != nil {
				readErr = err
				return
			}
			payload = buf
			_ = meta.Headers
			served++
		}
	}()

	require.Nil(t, panicked, "a corrupt record panicked instead of returning an error")
	require.ErrorIs(t, readErr, ErrCorruptRecord)
	// It fails AT the bad record: the five ahead of it are ordinary reads.
	require.Equal(t, 5, served, "the records before the corrupt one must still be served")
}
