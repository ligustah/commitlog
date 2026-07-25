package commitlog

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// oneMessageSet builds a single-message set ready for WriteMessageSet against
// seg's current tail.
func oneMessageSet(t *testing.T, seg *segment) ([]byte, []*entry) {
	t.Helper()
	ms, entries, err := newMessageSetFromProto(seg.NextOffset(), seg.Position(),
		[]*Message{{Value: []byte("v")}}, false)
	require.NoError(t, err)
	return ms, entries
}

// blockingBacking wraps a real backing and holds Sync open until released, so a
// test can observe what the rest of the segment can do while an fsync is in
// flight.
type blockingBacking struct {
	segmentBacking
	entered chan struct{}
	release chan struct{}
}

func (b *blockingBacking) Sync() error {
	close(b.entered)
	<-b.release
	return b.segmentBacking.Sync()
}

// An fsync must not stall appends to the same segment. The append path and the
// sync path both need the segment lock, so a sync that holds it across the
// fsync blocks every concurrent append for the fsync's whole duration — which
// silently defeats a caller's group commit, because the appends that would form
// its next batch are exactly the ones arriving during the in-flight sync.
func TestSyncDoesNotBlockAppends(t *testing.T) {
	dir := tempDir(t)
	seg := createSegment(t, dir, 0, 1024)

	blocking := &blockingBacking{
		segmentBacking: seg.backing,
		entered:        make(chan struct{}),
		release:        make(chan struct{}),
	}
	seg.Lock()
	seg.backing = blocking
	seg.dirty = true
	seg.Unlock()

	syncDone := make(chan error, 1)
	go func() { syncDone <- seg.Sync() }()

	// Wait until the sync is genuinely inside the fsync, still unreleased.
	select {
	case <-blocking.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("sync never reached the fsync")
	}

	appendDone := make(chan error, 1)
	go func() {
		ms, entries := oneMessageSet(t, seg)
		appendDone <- seg.WriteMessageSet(ms, entries)
	}()

	select {
	case err := <-appendDone:
		require.NoError(t, err, "append must land while an fsync is in flight")
	case <-time.After(5 * time.Second):
		close(blocking.release)
		<-syncDone
		t.Fatal("append blocked behind an in-flight fsync: the segment lock is " +
			"still held across the sync, which defeats caller-side group commit")
	}

	close(blocking.release)
	require.NoError(t, <-syncDone)
}

// An append landing mid-fsync is not covered by that fsync — the group-commit
// contract — so the segment must stay dirty and the NEXT sync must flush it.
// Clearing the dirty mark after the fsync instead of before would drop it.
func TestAppendDuringSyncStaysDirty(t *testing.T) {
	dir := tempDir(t)
	seg := createSegment(t, dir, 0, 1024)

	blocking := &blockingBacking{
		segmentBacking: seg.backing,
		entered:        make(chan struct{}),
		release:        make(chan struct{}),
	}
	seg.Lock()
	seg.backing = blocking
	seg.dirty = true
	seg.Unlock()

	syncDone := make(chan error, 1)
	go func() { syncDone <- seg.Sync() }()
	<-blocking.entered

	ms, entries := oneMessageSet(t, seg)
	require.NoError(t, seg.WriteMessageSet(ms, entries))

	close(blocking.release)
	require.NoError(t, <-syncDone)

	seg.RLock()
	dirty := seg.dirty
	seg.RUnlock()
	require.True(t, dirty,
		"a record appended during a sync is not covered by it and must be flushed by the next one")
}

// A segment with nothing appended since its last sync is already on stable
// storage, so a durability pass must not pay an fsync for it.
func TestSyncSkipsCleanSegment(t *testing.T) {
	dir := tempDir(t)
	seg := createSegment(t, dir, 0, 1024)

	ms, entries := oneMessageSet(t, seg)
	require.NoError(t, seg.WriteMessageSet(ms, entries))
	require.NoError(t, seg.Sync())

	counting := &countingBacking{segmentBacking: seg.backing}
	seg.Lock()
	seg.backing = counting
	seg.Unlock()

	require.NoError(t, seg.Sync())
	require.Zero(t, counting.syncs, "a segment with nothing new must not be fsynced")

	require.NoError(t, seg.WriteMessageSet(oneMessageSet(t, seg)))
	require.NoError(t, seg.Sync())
	require.Equal(t, 1, counting.syncs, "a segment appended to must be fsynced")
}

type countingBacking struct {
	segmentBacking
	syncs int
}

func (b *countingBacking) Sync() error {
	b.syncs++
	return b.segmentBacking.Sync()
}

