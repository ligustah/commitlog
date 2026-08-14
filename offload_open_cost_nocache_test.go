package commitlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// The same "opening a log reads none of its offloaded segments' bytes" claim as
// TestOpeningAnOffloadedTierReadsNoLogObjects, for a tier configured WITHOUT a
// RemoteIndexCache.
//
// It is a separate test rather than a case in that one because the two
// configurations do not make the same promise. With a cache the index is in the
// store and both boot paths are free. Without one the index stays on local disk,
// which splits them:
//
//   - Reopening the directory the log offloaded from is free, and must be.
//     Offloading removes the local .log and keeps the .index, so the index is
//     right there, complete, and already loaded by the time anything asks.
//   - Adopting the tier into a FRESH directory genuinely has to rebuild, because
//     the index exists nowhere else. That download is real and this test asserts
//     it happens, rather than quietly claiming a zero it has no right to.
//
// The first of those was not free. A block segment's index anchors BLOCKS, not
// records, so its last entry is a block's first message and setupIndex found the
// segment's end by reading that final block — out of the store, once per segment,
// during open. 40,947 bytes across 8 requests for the 90k-record case here, for
// two numbers every manifest entry carries. Nothing contradicted the sibling
// test's zero because that test only ever ran with a cache configured, which is
// the configuration where boundaries never come from an index at all.
func TestReopeningACacheLessOffloadedTierReadsNoLogObjects(t *testing.T) {
	for _, tc := range []struct {
		name    string
		codec   compress.Codec
		records int
	}{
		{"raw", compress.None, 20000},
		{"block", compress.Snappy, 90000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := tempDir(t)
			fs, err := NewFileSegmentStore(filepath.Join(root, "store"))
			require.NoError(t, err)
			store := &countingLogStore{FileSegmentStore: fs}

			// No RemoteIndexCache: option 1, the index stays local.
			opts := func(path string, adopt bool) Options {
				return Options{
					Name: "cost-nocache", Path: path, MaxSegmentBytes: 1 << 20,
					Compression: tc.codec, Tiers: oneTier(store),
					AdoptOptions: adopt,
				}
			}

			dir := filepath.Join(root, "log")
			l, err := New(opts(dir, false))
			require.NoError(t, err)
			for i := 0; i < tc.records; i += 200 {
				batch := make([]*Message, 0, 200)
				for j := i; j < i+200 && j < tc.records; j++ {
					batch = append(batch, &Message{
						Key:   []byte(fmt.Sprintf("k:%05d", j)),
						Value: []byte(fmt.Sprintf("v:%08d:%s", j, strings.Repeat("x", 40))),
					})
				}
				_, aerr := l.Append(batch)
				require.NoError(t, aerr)
			}
			offloaded, err := l.OffloadBefore(l.NewestOffset())
			require.NoError(t, err)
			require.Greater(t, offloaded, 0, "nothing was offloaded, so this proves nothing")
			require.NoError(t, l.Close())

			// The local index files must still be there — the premise of the
			// whole test. Asserted rather than assumed, because if offloading
			// ever started removing them the reopen below would rebuild and the
			// zero it checks would become unreachable for a reason that has
			// nothing to do with what is under test.
			idx, err := filepath.Glob(filepath.Join(dir, "*"+indexSuffix))
			require.NoError(t, err)
			require.NotEmpty(t, idx,
				"an option-1 offload must leave the local index behind")

			store.reset()
			same, err := New(opts(dir, false))
			require.NoError(t, err)
			require.Zerof(t, store.logBytes.Load(),
				"reopening a cache-less tier read %d bytes of segment objects across "+
					"%d reads, before serving anything — the local index and the "+
					"manifest between them describe every segment without it",
				store.logBytes.Load(), store.logReads.Load())

			// The reopened log still serves the records, which is what makes the
			// zero above mean "did not need to read" rather than "did not open".
			oldest := same.OldestOffset()
			require.GreaterOrEqual(t, oldest, int64(0), "the reopened tier is empty")
			r, err := same.NewReader(From(oldest), Uncommitted())
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			msg, off, _, _, err := r.ReadMessage(ctx, make([]byte, HeaderBufferLen))
			require.NoError(t, err)
			require.Equal(t, oldest, off)
			require.Equal(t, fmt.Sprintf("k:%05d", oldest), string(msg.Key()))
			require.Greaterf(t, store.logBytes.Load(), int64(0),
				"the first read fetched nothing, so the open must have fetched it "+
					"after all")

			// Read the whole log, not just its first record. The open now takes
			// each block segment's LAST offset from the manifest instead of
			// deriving it by reading the final block, and a wrong last offset is
			// invisible to a single read of the first one: it shows up as a
			// sequential read that stops early or refuses to cross into the next
			// segment. Counting every record is what makes those reachable.
			n := 1
			for {
				_, _, _, _, rerr := r.ReadMessage(ctx, make([]byte, HeaderBufferLen))
				if rerr != nil {
					break
				}
				n++
			}
			require.Equalf(t, tc.records, n,
				"reading the reopened log end to end produced %d of %d records, so a "+
					"segment boundary taken from the manifest is wrong", n, tc.records)
			require.NoError(t, same.Close())

			// The other half: a directory that has never held these indexes has
			// to rebuild them, and that cost is real. Asserted so the rebuild
			// cannot be optimised away on the strength of the zero above — a
			// fresh directory that skipped it would open every segment with an
			// empty index and report a tier holding no records.
			fresh := filepath.Join(root, "adopt")
			require.NoError(t, os.MkdirAll(fresh, 0o755))
			store.reset()
			adopted, err := New(opts(fresh, true))
			require.NoError(t, err)
			defer adopted.Close()
			require.Greaterf(t, store.logBytes.Load(), int64(0),
				"adopting a cache-less tier into a fresh directory read nothing, so "+
					"the indexes it has never held cannot have been rebuilt")
			require.Equal(t, oldest, adopted.OldestOffset(),
				"the adopted tier must hold the same records as the one it came from")
			ar, err := adopted.NewReader(From(oldest), Uncommitted())
			require.NoError(t, err)
			amsg, aoff, _, _, err := ar.ReadMessage(ctx, make([]byte, HeaderBufferLen))
			require.NoError(t, err)
			require.Equal(t, oldest, aoff)
			require.Equal(t, fmt.Sprintf("k:%05d", oldest), string(amsg.Key()))
		})
	}
}
