package commitlog_test

import (
	"context"
	"fmt"
	"os"

	"github.com/ligustah/commitlog"
	"github.com/ligustah/commitlog/compress"
)

// The README's usage snippet, as a compiled and executed example. Prose can
// drift from the API without anything noticing; this cannot — `go test` builds
// it and checks its output, so a signature change breaks the build and a
// behavioural change fails the assertion.
func Example() {
	dir, err := os.MkdirTemp("", "commitlog-example")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	log, err := commitlog.New(commitlog.Options{
		Path:        dir,
		Compression: compress.Zstd,
		Compact:     true,
	})
	if err != nil {
		panic(err)
	}
	defer log.Close()

	offsets, err := log.Append([]*commitlog.Message{
		{Key: []byte("order-1"), Value: []byte("placed")},
		{Key: []byte("order-2"), Value: []byte("placed")},
	})
	if err != nil {
		panic(err)
	}

	// Nothing is visible to a committed reader until the high watermark moves:
	// appending and committing are deliberately separate steps, so a caller
	// replicating elsewhere decides when a record counts as durable.
	log.SetHighWatermark(offsets[len(offsets)-1])

	r, err := log.NewReader(commitlog.From(0), commitlog.Follow()) // committed reader, from the start
	if err != nil {
		panic(err)
	}

	hdr := make([]byte, commitlog.HeaderBufferLen)
	for range offsets {
		msg, offset, _, _, err := r.ReadMessage(context.Background(), hdr)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%d %s=%s\n", offset, msg.Key(), msg.Value())
	}

	// Output:
	// 0 order-1=placed
	// 1 order-2=placed
}
