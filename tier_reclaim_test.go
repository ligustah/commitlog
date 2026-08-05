package commitlog

import (
	"bytes"
	"io"
	"path/filepath"
	"runtime/debug"
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
	var store *writeCountingStore
	l, cleanup := reclaimFixtureStore(t, func(fs *FileSegmentStore) SegmentStore {
		store = &writeCountingStore{FileSegmentStore: fs}
		return store
	})
	return l, store, cleanup
}

// reclaimFixtureStore is reclaimFixture over a caller-chosen wrapper around the
// file store, for tests that need to observe the store's deletes as they land
// rather than only count them afterwards.
func reclaimFixtureStore(t *testing.T, wrap func(*FileSegmentStore) SegmentStore) (*commitLog, func()) {
	t.Helper()
	dir := tempDir(t)
	fs, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)
	store := wrap(fs)

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
	return l, cleanup
}

// cleanAll runs a full pass over everything committed.
func cleanAll(t *testing.T, l *commitLog) {
	t.Helper()
	hw := l.HighWatermark()
	_, err := l.CleanWithSpec(CleanSpec{Ceiling: At(hw + 1), TombstoneGCBelow: hw + 1})
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

// pinAuditStore records whether a delete lands on a key while a scan says it is
// still holding it. hold/drop bracket the window a scanner is pinning a key.
//
// The bookkeeping is deliberately CONSERVATIVE at both ends: hold is called
// after the scanner has already acquired, and drop before it releases. So the
// audited window is strictly inside the real pin — this can miss a violation,
// but it cannot invent one, which is the right way round for a concurrent test.
// It is scoped to deletes issued by drainReclaim. A segment REMOVED outright —
// compaction finding every record in it superseded — deletes its objects from
// cleanupEmptySegment while a scan may well be holding them, and that is
// deliberate (see TestReclamationWaitsForTheReaderHoldingTheOldObject): those
// records are gone by decision, and the reader gets ErrSegmentClosed from the
// segment. A rewrite decides nothing of the sort, which is the claim here.
type pinAuditStore struct {
	*FileSegmentStore

	mu sync.Mutex
	// pinned counts scans currently holding each key.
	pinned map[string]int
	// reclaimDeletes counts deletes that came from drainReclaim. Asserted
	// POSITIVE: if drainReclaim is ever renamed, this attribution silently stops
	// matching and the audit below would pass by covering nothing, which is the
	// exact failure this whole test is being fixed for.
	reclaimDeletes int
	racy           []string // reclaim-path keys deleted while a scan held them
}

func newPinAuditStore(fs *FileSegmentStore) *pinAuditStore {
	return &pinAuditStore{FileSegmentStore: fs, pinned: map[string]int{}}
}

func (s *pinAuditStore) hold(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pinned[key]++
}

func (s *pinAuditStore) drop(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pinned[key]--; s.pinned[key] <= 0 {
		delete(s.pinned, key)
	}
}

func (s *pinAuditStore) Delete(key string) error {
	viaReclaim := bytes.Contains(debug.Stack(), []byte("drainReclaim"))
	s.mu.Lock()
	if viaReclaim {
		s.reclaimDeletes++
		if s.pinned[key] > 0 {
			s.racy = append(s.racy, key)
		}
	}
	s.mu.Unlock()
	return s.FileSegmentStore.Delete(key)
}

// Scans and reclamation passes run together, and must interleave without a
// race, a read error, or a delete that skips the deferred path.
//
// Read the name literally, because the obvious stronger reading is NOT what
// this proves. It was called TestConcurrentScansNeverLoseTheObjectTheyAreReading
// and asserted that reads through the pinned backing kept succeeding — and it
// passed with the pin check in drainReclaim deleted outright. Two reasons, both
// worth keeping:
//
//   - The read could not observe the loss. storeBacking reads ahead
//     prefetchSize (1 MiB) and these objects are a few hundred bytes, so the
//     first touch buffers the whole object and every later ReadAt is served
//     from memory without going near the store. It was measuring the read-ahead
//     buffer, not the pin.
//   - Free-running scans cannot reach the window anyway. Reclamation is
//     deliberately deferred to a LATER pass, so by the time a delete is issued
//     the scanner that held the old object has released it several passes ago.
//     Holding a pin across a drain is something a test has to arrange on
//     purpose, which is what TestReclamationWaitsForTheReaderHoldingTheOldObject
//     does — that is where the pin guarantee is proven, and this test is not a
//     second copy of it.
//
// What is left here is still worth running. The store audit below is scoped to
// the reclaim path and is opportunistic — it fires only if a delete happens to
// land inside a hold window, which the deferral makes rare — but the count of
// reclaim-path deletes is asserted positive, and THAT is deterministic: delete
// superseded objects at the point of rewrite instead of queueing them, and this
// test fails. So it holds the deferral itself in place, under concurrency, with
// -race watching the interleaving.
//
// It also does not pin down the acquire-under-the-segment-lock ordering. Moving
// the acquire after the RUnlock leaves this test green, for the same deferral
// reason: the reader would have to be descheduled across an entire subsequent
// pass to lose its object. Nearly unreachable is not a guarantee and the correct
// ordering costs nothing, so the code takes the claim under the lock; but that
// ordering is argued rather than tested.
func TestConcurrentScansAndReclamationInterleaveSafely(t *testing.T) {
	var store *pinAuditStore
	l, cleanup := reclaimFixtureStore(t, func(fs *FileSegmentStore) SegmentStore {
		store = newPinAuditStore(fs)
		return store
	})
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
					if sc.pin == nil {
						_ = sc.Close()
						continue
					}
					key := sc.pin.key
					store.hold(key)
					// The read is still worth doing — it exercises the backing
					// while a pass runs — but the guarantee is the store audit
					// above, for the read-ahead reason in the doc comment.
					buf := make([]byte, 8)
					if _, err := sc.pin.ReadAt(buf, 0); err != nil && !errors.Is(err, io.EOF) {
						failure.Store(err.Error())
					}
					store.drop(key)
					_ = sc.Close()
				}
			}
		}()
	}

	for i := 0; i < 40; i++ {
		hw := l.HighWatermark()
		_, err := l.CleanWithSpec(CleanSpec{Ceiling: At(hw + 1), TombstoneGCBelow: hw + 1})
		require.NoError(t, err)
	}
	close(stop)
	wg.Wait()

	if err := failure.Load(); err != nil {
		t.Fatalf("a scan lost the object it had pinned: %v", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	// Opportunistic — see the doc comment. A green run here is not proof that
	// pinned objects survive; it is the absence of the one symptom this shape
	// can see.
	require.Empty(t, store.racy, "reclamation deleted an object a scan was holding")
	// The deterministic half: a run that reclaimed nothing — or whose deletes
	// stopped going through drainReclaim — would satisfy every line above by
	// covering no deletes at all.
	require.Positive(t, store.reclaimDeletes,
		"no delete was attributed to drainReclaim: either nothing was reclaimed, or "+
			"reclamation stopped going through the deferred path")
}
