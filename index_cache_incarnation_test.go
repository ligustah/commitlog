package commitlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// The RemoteIndexCache is process-wide and outlives any one log. It used to be
// keyed by the segment's log PATH and base offset, which is unique across every
// log in a process but not across time: delete a log's directory, create a new
// log at the same path, and its segments restart at base offset 0 and produce
// byte-identical keys. A hit then returned the DEAD log's index without
// consulting the store at all.
//
// That is not a stale read. An index says "the record at offset X is at byte
// position P", and applied to a different log's bytes it is simply wrong — a
// seek lands mid-stream and the read comes back in order, with no error, having
// skipped records. Reported from durable_streams against v0.48.0: a read asking
// for offset 5 began at offset 7.
//
// The second generation writes SHORTER values than the first on purpose. Equal
// record sizes would make the dead index's byte positions accidentally right,
// and the test would pass while the bug was still there.
func TestARecreatedLogDoesNotSeekWithTheDeletedLogsCachedIndex(t *testing.T) {
	root := tempDir(t)
	logPath := filepath.Join(root, "events")

	// One store per incarnation, one cache for the process. That is the shape
	// that matters: the STORE goes with the stream, so a recreated stream gets a
	// clean tier — but the CACHE is process-wide by design and outlives both, so
	// it is the only thing left holding the dead log's data.
	newStore := func(gen string) SegmentStore {
		fs, err := NewFileSegmentStore(filepath.Join(root, "store-"+gen))
		require.NoError(t, err)
		return fs
	}
	cache, err := NewRemoteIndexCache(filepath.Join(root, "idxcache"), 1<<30)
	require.NoError(t, err)
	defer cache.Close()

	// Mid-segment offsets: a read from a segment's base offset starts at byte
	// zero and needs no seek, so it never consults an index and never meets
	// this. Only a seek into the middle does.
	probes := []int64{5, 13, 22, 31}

	run := func(gen string, value func(int64) string) []string {
		store := newStore(gen)
		l, err := New(Options{
			Name:             "events",
			Path:             logPath,
			MaxSegmentBytes:  512,
			Tiers:            oneTier(store),
			RemoteIndexCache: cache,
			DisableAutoClean: true,
		})
		require.NoError(t, err)

		var last int64
		for n := int64(0); n < 60; n++ {
			offs, err := l.Append([]*Message{{
				Key:   []byte(fmt.Sprintf("k:%d", n)),
				Value: []byte(value(n)),
			}})
			require.NoError(t, err)
			last = offs[len(offs)-1]
			l.SetHighWatermark(last)
		}
		n, err := l.OffloadBefore(last) // keep the active tail local
		require.NoError(t, err)
		require.Greater(t, n, 0, "%s: nothing was offloaded, so no index is cached", gen)

		got := make([]string, 0, len(probes))
		for _, want := range probes {
			r, err := l.NewReader(From(want), Uncommitted())
			require.NoError(t, err, "%s: opening a reader at %d", gen, want)
			msg, off, _, _, err := r.ReadMessage(context.Background(), make([]byte, HeaderBufferLen))
			require.NoError(t, err, "%s: reading at %d", gen, want)
			require.Equal(t, want, off,
				"%s: a seek to %d landed on %d — the cache served another "+
					"incarnation's index", gen, want, off)
			got = append(got, string(msg.Value()))
		}
		require.NoError(t, l.Close())
		return got
	}

	gen1 := run("gen1", func(n int64) string {
		return fmt.Sprintf("gen1-%d-%s", n, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
	})
	for i, v := range gen1 {
		require.Contains(t, v, "gen1-", "gen1 probe %d read the wrong record", i)
	}

	// The log goes, directory and all — exactly what a stream delete does. The
	// store keeps the first generation's objects; nothing tells the cache.
	require.NoError(t, os.RemoveAll(logPath))

	gen2 := run("gen2", func(n int64) string {
		return fmt.Sprintf("gen2-%d", n) // deliberately shorter
	})
	for i, v := range gen2 {
		require.Contains(t, v, "gen2-",
			"probe %d read a record from the DELETED log", i)
	}
}
