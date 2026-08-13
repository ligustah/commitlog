package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// lyingSizeStore reports a huge size for the descriptor object and nothing else,
// which is what an unverified length field looks like from the caller's side: a
// remote store whose answer is taken at face value.
//
// It EMBEDS a real store rather than faking the whole interface, so every other
// call behaves normally and the test isolates the one answer under test — see
// the note in [a test double can disable the code under test]: a fake that
// replaces a concrete type switches off code paths that assert on it.
type lyingSizeStore struct {
	SegmentStore
	size int64
}

func (s *lyingSizeStore) Size(key string) (int64, error) {
	if key == descriptorKey {
		return s.size, nil
	}
	return s.SegmentStore.Size(key)
}

// The size steering readStoreDescriptor's allocation is the STORE's answer and
// nothing verifies it, so a store reporting a large descriptor allocates that
// much in the caller's process during New, before a byte is parsed.
//
// Asserted as a refusal rather than as "does not crash", because a test that
// only checks for a panic passes whether or not the bound exists: the runtime
// happily allocates a few hundred MB. What must be true is that it never tries.
func TestAnOversizedStoreDescriptorIsRefusedNotAllocated(t *testing.T) {
	real, err := NewFileSegmentStore(tempDir(t))
	require.NoError(t, err)

	// Create the log so a real descriptor is published into the store.
	l, err := New(Options{
		Name: "bound", Path: tempDir(t), MaxSegmentBytes: 1 << 20,
		Tiers: []Tier{{Name: "hot", Store: real}},
	})
	require.NoError(t, err)
	require.NoError(t, l.Close())

	// Now reopen through a store that lies about the descriptor's size.
	liar := &lyingSizeStore{SegmentStore: real, size: 8 << 30} // 8 GiB
	_, err = New(Options{
		Name: "bound", Path: tempDir(t), MaxSegmentBytes: 1 << 20,
		Tiers: []Tier{{Name: "hot", Store: liar}},
	})
	require.Error(t, err, "an 8 GiB descriptor must be refused, not allocated")
	require.Contains(t, err.Error(), "over the")
	require.Contains(t, err.Error(), "maximum")
}

// The bound must not refuse a descriptor that could actually parse. It is
// derived from bufio.Scanner's default token limit, which is what
// parseDescriptor reads with, so anything at or under it is fair game — and a
// real descriptor is two orders of magnitude smaller than that.
func TestAnOrdinaryStoreDescriptorIsWellUnderTheBound(t *testing.T) {
	store, err := NewFileSegmentStore(tempDir(t))
	require.NoError(t, err)

	l, err := New(Options{
		Name: "bound", Path: tempDir(t), MaxSegmentBytes: 1 << 20,
		Tiers:    []Tier{{Name: "hot", Store: store}},
		Identity: []byte("an identity of entirely unremarkable length"),
	})
	require.NoError(t, err)
	require.NoError(t, l.Close())

	size, err := store.Size(descriptorKey)
	require.NoError(t, err)
	require.Less(t, size, int64(maxDescriptorBytes),
		"a real descriptor must be under the bound, or the bound is wrong")
	// Not merely under it — nowhere near it, which is what makes the bound safe
	// to state as derived rather than tuned.
	require.Less(t, size, int64(maxDescriptorBytes/100))

	// And it still opens through the store.
	l2, err := New(Options{
		Name: "bound", Path: tempDir(t), MaxSegmentBytes: 1 << 20,
		Tiers:    []Tier{{Name: "hot", Store: store}},
		Identity: []byte("an identity of entirely unremarkable length"),
	})
	require.NoError(t, err)
	require.Nil(t, l2.IdentityConflict())
	require.NoError(t, l2.Close())
}
