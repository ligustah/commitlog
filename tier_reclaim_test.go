package commitlog

import (
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// reclaimFixture builds a log whose sealed segments are offloaded and full of
// superseded copies of one key, so a compaction pass is guaranteed to rewrite a
// tiered segment onto a new object and leave the old one behind.
func reclaimFixture(t *testing.T) (*commitLog, *writeCountingStore, func()) {
	t.Helper()
	dir := tempDir(t)
	fs, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)
	store := &writeCountingStore{FileSegmentStore: fs}

	l, cleanup := setupWithOptions(t, Options{
		Path:             dir,
		MaxSegmentBytes:  128,
		Compact:          true,
		SegmentStore:     store,
		DisableAutoClean: true,
	})

	var last int64
	for i := 0; i < 30; i++ {
		offs, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("value padding")}})
		require.NoError(t, err)
		last = offs[0]
	}
	// Pads, so the live records are not stranded in the never-compacted active
	// segment.
	for _, k := range []string{"pad0", "pad1"} {
		offs, err := l.Append([]*Message{{Key: []byte(k), Value: []byte("p")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)

	n, err := l.OffloadBefore(last)
	require.NoError(t, err)
	require.Positive(t, n, "the fixture needs offloaded segments")
	return l, store, cleanup
}

// cleanAll runs a full pass over everything committed.
func cleanAll(t *testing.T, l *commitLog) {
	t.Helper()
	hw := l.HighWatermark()
	_, err := l.CleanWithSpec(CleanSpec{Ceiling: hw + 1, TombstoneGCBelow: hw + 1})
	require.NoError(t, err)
}

func queuedKeys(l *commitLog) []string {
	l.tierMu.Lock()
	defer l.tierMu.Unlock()
	keys := make([]string, 0, len(l.reclaim))
	for _, e := range l.reclaim {
		keys = append(keys, e.key)
	}
	return keys
}

// THE reason reclamation could not be done at the point of rewrite, and so the
// reason it used to be handed to the caller: a reader that opened the segment
// before the rewrite is still reading the OLD object. Deleting it turns a
// rewrite into a read error for that reader.
//
// The pin is what the log knows and the caller does not. While a scan holds it
// the object survives every pass; once the scan closes, the next pass takes it.
func TestReclamationWaitsForTheReaderHoldingTheOldObject(t *testing.T) {
	l, store, cleanup := reclaimFixture(t)
	defer cleanup()

	// Readers open every tiered segment and hold them — a scanner takes the
	// backing at construction, exactly as a live scan would. All of them,
	// because which segment a pass chooses to rewrite is its business; what
	// matters is that whichever it picks is one somebody is reading.
	scanners := openTieredScans(t, l)
	require.NotEmpty(t, scanners, "the fixture needs offloaded segments")

	// The rewrite supersedes an object a reader is on.
	cleanAll(t, l)
	held := pinnedQueuedKeys(l)
	require.NotEmpty(t, held,
		"a rewrite must queue the object its reader is still holding")

	// Pass after pass, it stays: the reader has not finished.
	for i := 0; i < 3; i++ {
		cleanAll(t, l)
		keys, err := store.List()
		require.NoError(t, err)
		for _, k := range held {
			require.Contains(t, keys, k,
				"pass %d deleted an object a live reader is still reading", i+1)
			require.Contains(t, queuedKeys(l), k, "it must stay queued, not be forgotten")
		}
	}

	// The deferred objects still SERVE, which is the whole point: reclaiming one
	// under its reader is what would turn a rewrite into a read error.
	//
	// Read through the pinned backing, and only for what reclamation deferred.
	// A segment REMOVED outright — retention dropping it, or compaction finding
	// every record in it superseded — takes its objects with it, and a reader
	// then gets ErrSegmentClosed from the segment rather than a missing object.
	// That is a different claim, and a deliberate one: those records are gone by
	// decision. A rewrite decides nothing of the sort — the records survive, only
	// the object holding them changed — which is why it may not break a reader.
	for _, e := range pinnedQueued(l) {
		buf := make([]byte, 8)
		_, err := e.pin.ReadAt(buf, 0)
		require.NoError(t, err,
			"a deferred object must still be readable from the store: %s", e.key)
	}

	// Once the readers are done, the next pass takes them.
	for _, sc := range scanners {
		require.NoError(t, sc.Close())
	}
	cleanAll(t, l)

	keys, err := store.List()
	require.NoError(t, err)
	for _, k := range held {
		require.NotContains(t, keys, k,
			"a released object must be reclaimed rather than waiting for a caller")
		require.NotContains(t, queuedKeys(l), k)
	}
}

// openTieredScans starts one scan per offloaded segment, each pinning the object
// it reads.
func openTieredScans(t *testing.T, l *commitLog) []*segmentScanner {
	t.Helper()
	l.mu.RLock()
	defer l.mu.RUnlock()
	var scanners []*segmentScanner
	for _, s := range l.segments {
		s.RLock()
		offloaded := s.store != nil
		s.RUnlock()
		if !offloaded {
			continue
		}
		sc := newSegmentScanner(s)
		require.NotNil(t, sc.pin, "a scan of a tiered segment must pin its object")
		scanners = append(scanners, sc)
	}
	return scanners
}

// pinnedQueued is the queued entries a reader still holds — the ones no pass may
// delete.
func pinnedQueued(l *commitLog) []pendingReclaim {
	l.tierMu.Lock()
	defer l.tierMu.Unlock()
	var held []pendingReclaim
	for _, e := range l.reclaim {
		if e.pin != nil && e.pin.referenced() {
			held = append(held, e)
		}
	}
	return held
}

func pinnedQueuedKeys(l *commitLog) []string {
	var keys []string
	for _, e := range pinnedQueued(l) {
		keys = append(keys, e.key)
	}
	return keys
}

// A pin is taken under the segment's read lock and released on Close, so a
// scanner that comes and goes leaves no residue. Without the release, one scan
// of a rewritten segment would pin its predecessor forever and the queue would
// grow without bound — a leak wearing the costume of caution.
func TestScanReleasesItsPinOnClose(t *testing.T) {
	l, _, cleanup := reclaimFixture(t)
	defer cleanup()

	l.mu.RLock()
	var tiered *segment
	for _, s := range l.segments {
		s.RLock()
		offloaded := s.store != nil
		s.RUnlock()
		if offloaded {
			tiered = s
			break
		}
	}
	l.mu.RUnlock()
	require.NotNil(t, tiered)

	sc := newSegmentScanner(tiered)
	pin := sc.pin
	require.True(t, pin.referenced(), "an open scan must hold its backing")
	require.NoError(t, sc.Close())
	require.False(t, pin.referenced(), "a closed scan must release it")

	// Idempotent: Scan releases the stream early on EOF and a caller's defer
	// then closes again. A double release would drop the count below zero and
	// declare a pinned object free.
	require.NoError(t, sc.Close())
	require.False(t, pin.referenced())
}

// A local segment has nothing to reclaim: a local rewrite renames over the
// source and a reader's file handle stays valid, so counting there would be
// bookkeeping for an object that does not exist.
func TestLocalSegmentsAreNotPinned(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:             tempDir(t),
		MaxSegmentBytes:  128,
		Compact:          true,
		DisableAutoClean: true,
	})
	defer cleanup()

	for i := 0; i < 20; i++ {
		_, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("value padding")}})
		require.NoError(t, err)
	}

	l.mu.RLock()
	seg := l.segments[0]
	l.mu.RUnlock()

	sc := newSegmentScanner(seg)
	require.Nil(t, sc.pin, "a local segment must not be pinned")
	require.NoError(t, sc.Close())
}

// A read-only tier owns none of its store's writes, and a delete is a write.
// The queue is held, not dropped: going read-only is a handover, not a licence
// to forget what this log superseded while it did own the tier.
func TestReadOnlyTierDefersReclamationRatherThanDroppingIt(t *testing.T) {
	l, store, cleanup := reclaimFixture(t)
	defer cleanup()

	cleanAll(t, l)
	queued := queuedKeys(l)
	require.NotEmpty(t, queued, "the fixture must have superseded something")

	l.SetTierReadOnly(true)
	store.deletes = 0
	cleanAll(t, l)

	require.Zero(t, store.deletes, "a read-only tier must take no deletes at all")
	require.Subset(t, queuedKeys(l), queued, "the queue must survive read-only")

	keys, err := store.List()
	require.NoError(t, err)
	require.Subset(t, keys, queued, "nothing may have been reclaimed")

	// Coming back out of read-only, reclamation resumes where it left off.
	l.SetTierReadOnly(false)
	cleanAll(t, l)

	keys, err = store.List()
	require.NoError(t, err)
	for _, k := range queued {
		require.NotContains(t, keys, k,
			"reclamation must resume once this log owns the tier again")
	}
}

// Publish the manifest, THEN delete. A manifest that failed to publish may
// still name an object this pass superseded, and deleting it would leave a
// reader that trusts the manifest opening a key that is gone. Holding the queue
// instead trades a dangling reference for an orphan, which costs storage and
// which UnreferencedObjects reports.
func TestReclamationHoldsOffWhileTheManifestMayStillNameTheObject(t *testing.T) {
	l, store, cleanup := reclaimFixture(t)
	defer cleanup()

	cleanAll(t, l)
	queued := queuedKeys(l)
	require.NotEmpty(t, queued)

	// The pass that would have republished the manifest did not manage to.
	l.tierMu.Lock()
	l.tierManifestStale = true
	l.tierMu.Unlock()

	store.deletes = 0
	cleanAll(t, l)
	require.Zero(t, store.deletes,
		"nothing may be deleted while the manifest may still name it")
	require.Subset(t, queuedKeys(l), queued, "the entries must be held, not dropped")

	// A pass whose manifest publish lands clears the flag and reclamation
	// resumes — the hold is on the failure, not on the queue.
	keys, err := store.List()
	require.NoError(t, err)
	require.Subset(t, keys, queued)

	cleanAll(t, l)
	keys, err = store.List()
	require.NoError(t, err)
	for _, k := range queued {
		require.NotContains(t, keys, k,
			"a successful manifest publish must let reclamation resume")
	}
}

// Scans and passes run together: nothing a scan has pinned may go missing while
// it reads, however many rewrites and reclamations happen underneath.
//
// What this does NOT pin down is the acquire-under-the-segment-lock ordering.
// Moving the acquire after the RUnlock leaves this test green — deferring the
// drain to a later pass makes that window nearly unreachable, since the reader
// would have to be descheduled across an entire subsequent pass to lose its
// object. Nearly is not a guarantee and the correct ordering costs nothing, so
// the code takes the claim under the lock; but that ordering is argued rather
// than tested, and this comment is here so the next person does not read a
// green run as proof of it.
func TestConcurrentScansNeverLoseTheObjectTheyAreReading(t *testing.T) {
	l, _, cleanup := reclaimFixture(t)
	defer cleanup()

	var (
		wg      sync.WaitGroup
		stop    = make(chan struct{})
		failure atomic.Value
	)

	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				l.mu.RLock()
				segs := append([]*segment(nil), l.segments...)
				l.mu.RUnlock()
				for _, s := range segs {
					s.RLock()
					offloaded := s.store != nil
					s.RUnlock()
					if !offloaded {
						continue
					}
					sc := newSegmentScannerCache(s, newBlockCache())
					if sc.pin != nil {
						// Read through the pinned backing: the claim says this
						// object stays until Close, whatever the passes do.
						buf := make([]byte, 8)
						if _, err := sc.pin.ReadAt(buf, 0); err != nil && !errors.Is(err, io.EOF) {
							failure.Store(err.Error())
						}
					}
					_ = sc.Close()
				}
			}
		}()
	}

	for i := 0; i < 40; i++ {
		hw := l.HighWatermark()
		_, err := l.CleanWithSpec(CleanSpec{Ceiling: hw + 1, TombstoneGCBelow: hw + 1})
		require.NoError(t, err)
	}
	close(stop)
	wg.Wait()

	if err := failure.Load(); err != nil {
		t.Fatalf("a scan lost the object it had pinned: %v", err)
	}
}
