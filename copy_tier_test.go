package commitlog

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// putOrderStore records the order objects were written in, so a test can assert
// what was committed when rather than only what ended up there.
type putOrderStore struct {
	*FileSegmentStore
	mu   sync.Mutex
	puts []string
}

func (s *putOrderStore) Put(key string, r io.Reader, size int64) error {
	if err := s.FileSegmentStore.Put(key, r, size); err != nil {
		return err
	}
	s.mu.Lock()
	s.puts = append(s.puts, key)
	s.mu.Unlock()
	return nil
}

func (s *putOrderStore) order() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.puts...)
}

// copyTierSource builds a log with several offloaded segments and returns its
// store, the offsets written, and the values they hold.
func copyTierSource(t *testing.T, root string) (*FileSegmentStore, []int64, map[int64]string) {
	t.Helper()
	store, err := NewFileSegmentStore(filepath.Join(root, "src"))
	require.NoError(t, err)
	cache, err := NewRemoteIndexCache(filepath.Join(root, "idxcache"), 1<<26)
	require.NoError(t, err)
	t.Cleanup(func() { cache.Close() })

	l, err := New(Options{
		Name: "copytier", Path: filepath.Join(root, "log"), MaxSegmentBytes: 1024,
		Tiers: oneTier(store), RemoteIndexCache: cache,
	})
	require.NoError(t, err)

	var offs []int64
	want := map[int64]string{}
	for i := range 300 {
		v := fmt.Sprintf("v:%04d:%s", i, strings.Repeat("y", 32))
		written, err := l.Append([]*Message{{Key: []byte(fmt.Sprintf("k:%04d", i)), Value: []byte(v)}})
		require.NoError(t, err)
		offs = append(offs, written[0])
		want[written[0]] = v
	}
	n, err := l.OffloadBefore(l.NewestOffset())
	require.NoError(t, err)
	require.Positive(t, n, "nothing was offloaded, so there is no tier to copy")
	require.NoError(t, l.Close())
	return store, offs, want
}

// A copied tier is a tier: a log opened over the destination with an EMPTY
// directory serves the same records.
//
// That is the whole claim. A tier that only works beside the directory it was
// written from is not something ownership can move.
func TestACopiedTierServesTheSameRecords(t *testing.T) {
	root := tempDir(t)
	src, offs, want := copyTierSource(t, root)

	dst, err := NewFileSegmentStore(filepath.Join(root, "dst"))
	require.NoError(t, err)
	require.NoError(t, CopyTier(src, dst))

	cache, err := NewRemoteIndexCache(filepath.Join(root, "idxcache2"), 1<<26)
	require.NoError(t, err)
	defer cache.Close()
	l, err := New(Options{
		Name: "copytier", Path: filepath.Join(root, "adopted"), MaxSegmentBytes: 1024,
		Tiers: oneTier(dst), RemoteIndexCache: cache,
	})
	require.NoError(t, err)
	defer l.Close()

	srcManifest, err := readTierManifest(src)
	require.NoError(t, err)
	dstManifest, err := l.TierManifest()
	require.NoError(t, err)
	require.Equal(t, srcManifest, dstManifest, "the copied manifest describes a different tier")

	r, err := l.NewReader(From(offs[0]), Uncommitted())
	require.NoError(t, err)
	hdr := make([]byte, HeaderBufferLen)
	for _, off := range offs[:len(offs)/2] {
		m, gotOff, _, _, err := r.ReadMessage(context.Background(), hdr)
		require.NoErrorf(t, err, "reading offset %d back from the copied tier", off)
		require.Equal(t, off, gotOff)
		require.Equal(t, want[off], string(m.Value()))
	}
}

// The manifest is written LAST, and that is the only reason a half-finished copy
// is safe.
//
// Until it lands nothing in the destination is claimed by anything, so a copy
// that dies partway leaves objects that UnreferencedObjects will list and a
// rerun will simply overwrite. Publish it first and the window inverts: the
// destination claims records whose bytes are not there yet, and a reader that
// trusts the manifest — which is the only thing there is to trust — opens a key
// that does not exist.
//
// The source's own List() happens to sort "manifest" after the digit-prefixed
// segment keys, which is what made a hand-rolled copy appear to work. That is an
// accident of one store's ordering, not a property of stores.
func TestACopiedManifestIsWrittenLast(t *testing.T) {
	root := tempDir(t)
	src, _, _ := copyTierSource(t, root)

	fs, err := NewFileSegmentStore(filepath.Join(root, "dst"))
	require.NoError(t, err)
	dst := &putOrderStore{FileSegmentStore: fs}
	require.NoError(t, CopyTier(src, dst))

	order := dst.order()
	require.Greater(t, len(order), 2, "the copy wrote almost nothing")
	require.Equal(t, manifestKey, order[len(order)-1],
		"the manifest was not the last object written: %v", order)
	require.Equal(t, descriptorKey, order[len(order)-2],
		"the descriptor was not written immediately before the manifest: %v", order)
}

// A destination that already holds a tier is refused, both ways it can say so.
//
// Copying over one would strand every object already there: the incoming
// manifest names none of them, so nothing references them and nothing this
// package runs would ever delete them either.
func TestCopyTierRefusesADestinationThatAlreadyHoldsOne(t *testing.T) {
	root := tempDir(t)
	src, _, _ := copyTierSource(t, root)

	dst, err := NewFileSegmentStore(filepath.Join(root, "dst"))
	require.NoError(t, err)
	require.NoError(t, CopyTier(src, dst))

	err = CopyTier(src, dst)
	require.Error(t, err, "a second copy over the same destination was accepted")

	// And with only a descriptor present — a store that belongs to a log that
	// has not offloaded anything yet is still that log's store.
	bare, err := NewFileSegmentStore(filepath.Join(root, "bare"))
	require.NoError(t, err)
	desc, err := readStoreDescriptor(src)
	require.NoError(t, err)
	require.NoError(t, writeStoreDescriptor(bare, desc))
	require.Error(t, CopyTier(src, bare),
		"a destination holding another log's descriptor was accepted")
}

// An object the manifest names but the source does not hold fails the copy
// rather than being carried across as a dangling entry.
//
// The source log tolerates that in one narrow window — a crash between a
// manifest that dropped entries and the delete that removed them — but in that
// window the CURRENT manifest no longer names them. A missing object under a
// current manifest is not that case, and a handover is not the place to decide
// it is close enough.
func TestCopyTierRefusesAManifestEntryTheSourceCannotServe(t *testing.T) {
	root := tempDir(t)
	src, _, _ := copyTierSource(t, root)

	objs, err := readTierManifest(src)
	require.NoError(t, err)
	require.NotEmpty(t, objs)
	require.NoError(t, src.Delete(objs[0].LogKey))

	dst, err := NewFileSegmentStore(filepath.Join(root, "dst"))
	require.NoError(t, err)
	err = CopyTier(src, dst)
	require.Error(t, err, "a manifest entry with no object behind it was copied anyway")

	// And it stopped before committing, so the destination is collectable rather
	// than a tier that claims records it cannot serve.
	_, statErr := dst.Size(manifestKey)
	require.ErrorIs(t, statErr, ErrObjectNotFound,
		"a failed copy published a manifest")
}

// A store with no descriptor is not a log's store, and copying its objects would
// hand the new owner a tier it cannot identify — which is the silent adoption
// the descriptor exists to prevent.
//
// The source is a REAL tier with its descriptor deleted, not an empty store, and
// the error is checked for what refused it. An empty source names nothing, so a
// copy of it is a copy of nothing and "returned an error" is satisfied by
// whatever CopyTier happens to look at first — which is not this claim, and
// would go on passing with the descriptor read deleted outright.
func TestCopyTierRefusesASourceWithNoDescriptor(t *testing.T) {
	root := tempDir(t)
	src, _, _ := copyTierSource(t, root)
	require.NoError(t, src.Delete(descriptorKey))

	dst, err := NewFileSegmentStore(filepath.Join(root, "dst"))
	require.NoError(t, err)

	err = CopyTier(src, dst)
	require.Error(t, err,
		"a store that never belonged to a log was copied as though it did")
	require.ErrorContains(t, err, "descriptor",
		"the copy was refused, but not for the reason under test: %v", err)

	// And refused BEFORE copying anything: the descriptor is read ahead of the
	// first object precisely so a rejected handover leaves nothing behind.
	keys, err := dst.List()
	require.NoError(t, err)
	require.Empty(t, keys, "a refused copy wrote to the destination anyway")
}
