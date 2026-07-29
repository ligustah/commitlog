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

// A corrupt record must not take the caller's process down.
//
// readMessage used to panic here — "data on disk is corrupted which means the
// server is in an unrecoverable state". That was a true statement about the
// server this package was extracted from, and a false one about a library
// embedded in somebody else's binary: the host had several good answers
// available (skip the record, fail the read, resync the stream) and the panic
// took the choice away, along with the process.
//
// A read is exactly where a caller is positioned to choose, so it now gets an
// error it can match on. Reported by durable_streams, who were recovering the
// panic at their own boundary to keep a corrupt record from killing their host.
//
// The test recovers deliberately rather than trusting the read to return: if the
// panic ever comes back, this reports it as a failure instead of taking the test
// binary down with it.
func TestSequentialReadReturnsCorruptRecordRatherThanPanicking(t *testing.T) {
	dir := tempDir(t)
	opts := Options{Path: dir, MaxSegmentBytes: 1 << 20}

	l, err := New(opts)
	require.NoError(t, err)
	cl := l.(*commitLog)

	const marker = "SEQUENTIAL-005-ZZZZ"
	var last int64
	for i := 0; i < 20; i++ {
		value := fmt.Sprintf("payload-%03d-xxxxxxxx", i)
		if i == 5 {
			value = marker
		}
		offs, err := cl.Append([]*Message{{
			Key: []byte(fmt.Sprintf("k:%03d", i)), Value: []byte(value),
		}})
		require.NoError(t, err)
		last = offs[0]
	}
	cl.SetHighWatermark(last)
	require.NoError(t, cl.Close())

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
		data[idx+11] = 'Q'
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
		hdr := make([]byte, 28)
		for i := 0; i < 20; i++ {
			if _, _, _, _, err := r.ReadMessage(ctx, hdr); err != nil {
				readErr = err
				return
			}
			served++
		}
	}()

	require.Nil(t, panicked, "a corrupt record panicked instead of returning an error")
	require.ErrorIs(t, readErr, ErrCorruptRecord)
	// It fails AT the bad record, not before it: the five good ones ahead of it
	// were returned normally, so this is a per-record refusal rather than the
	// whole log becoming unreadable.
	require.Equal(t, 5, served,
		"the records before the corrupt one must still be served")
}
