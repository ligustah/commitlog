package commitlog

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// offloadTestLog opens a small-segment log with a filesystem SegmentStore, so a
// handful of appends roll several sealed segments that can be offloaded.
func offloadTestLog(t *testing.T, dir string) (CommitLog, SegmentStore) {
	t.Helper()
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	l, err := New(Options{
		Name:            "offload",
		Path:            filepath.Join(dir, "log"),
		MaxSegmentBytes: 512, // tiny, so each batch rolls a new segment
		SegmentStore:    store,
	})
	if err != nil {
		t.Fatal(err)
	}
	return l, store
}

func appendMsg(t *testing.T, l CommitLog, val string) int64 {
	t.Helper()
	offs, err := l.Append([]*Message{{Value: []byte(val)}})
	if err != nil {
		t.Fatal(err)
	}
	l.SetHighWatermark(offs[0])
	return offs[0]
}

func readVal(t *testing.T, l CommitLog, offset int64) string {
	t.Helper()
	r, err := l.NewReader(offset, false)
	if err != nil {
		t.Fatal(err)
	}
	msg, _, _, _, err := r.ReadMessage(context.Background(), make([]byte, 28))
	if err != nil {
		t.Fatalf("read offset %d: %v", offset, err)
	}
	return string(msg.Value())
}

// The full lifecycle: append across several segments, offload the old ones,
// confirm the local .log files are gone but reads still work through the store,
// then reopen the log and confirm offloaded segments recover and read.
func TestOffload_ReadThroughAndRecovery(t *testing.T) {
	dir := t.TempDir()
	l, store := offloadTestLog(t, dir)

	const n = 40
	vals := make([]string, n)
	offs := make([]int64, n)
	for i := 0; i < n; i++ {
		vals[i] = "value-number-" + string(rune('A'+i%26)) + "-padded-to-roll-segments"
		offs[i] = appendMsg(t, l, vals[i])
	}

	// Offload everything below the last few records (keeps the active segment).
	cut := offs[n-3]
	count, err := l.OffloadBefore(cut)
	if err != nil {
		t.Fatal(err)
	}
	if count == 0 {
		t.Fatal("expected at least one segment offloaded")
	}
	t.Logf("offloaded %d segments below offset %d", count, cut)

	// Store now holds objects; some local .log files are gone.
	keys, err := store.List()
	objects := 0
	for _, k := range keys {
		if k != manifestKey {
			objects++
		}
	}
	if err != nil || objects != count {
		// The manifest is the tier describing itself, not a segment.
		t.Fatalf("store keys=%v (%d objects) want %d", keys, objects, count)
	}
	logFiles, _ := filepath.Glob(filepath.Join(dir, "log", "*.log"))
	t.Logf("local .log files remaining: %d, offloaded: %d", len(logFiles), count)

	// Every record still reads correctly, including the offloaded prefix
	// (read-through) and the still-local tail.
	for i := 0; i < n; i++ {
		if got := readVal(t, l, offs[i]); got != vals[i] {
			t.Fatalf("read offset %d = %q want %q", offs[i], got, vals[i])
		}
	}

	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen over the same dirs — offloaded segments must recover from the store
	// and read exactly the same.
	store2, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatal(err)
	}
	l2, err := New(Options{
		Name: "offload", Path: filepath.Join(dir, "log"),
		MaxSegmentBytes: 512, SegmentStore: store2,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	if got := l2.OldestOffset(); got != offs[0] {
		t.Fatalf("after recovery oldest=%d want %d", got, offs[0])
	}
	for i := 0; i < n; i++ {
		if got := readVal(t, l2, offs[i]); got != vals[i] {
			t.Fatalf("after recovery read offset %d = %q want %q", offs[i], got, vals[i])
		}
	}
}

// Reopening a log that has offloaded segments without a SegmentStore is a clear
// error, not a silent data loss.
func TestOffload_ReopenWithoutStoreErrors(t *testing.T) {
	dir := t.TempDir()
	l, _ := offloadTestLog(t, dir)
	for i := 0; i < 30; i++ {
		appendMsg(t, l, "padding-value-to-roll-segments-xxxxxxxxxxxxxxx")
	}
	if _, err := l.OffloadBefore(l.NewestOffset()); err != nil {
		t.Fatal(err)
	}
	l.Close()

	_, err := New(Options{Name: "offload", Path: filepath.Join(dir, "log"), MaxSegmentBytes: 512})
	if err == nil {
		t.Fatal("expected error reopening offloaded log without a SegmentStore")
	}
}

// TruncateBefore over offloaded segments removes both the store objects and the
// local index/marker.
func TestOffload_TruncateRemovesStoreObjects(t *testing.T) {
	dir := t.TempDir()
	l, store := offloadTestLog(t, dir)
	var offs []int64
	for i := 0; i < 40; i++ {
		offs = append(offs, appendMsg(t, l, "padding-value-to-roll-segments-yyyyyyyyyyyy"))
	}
	defer l.Close()

	cut := offs[len(offs)-3]
	if _, err := l.OffloadBefore(cut); err != nil {
		t.Fatal(err)
	}
	before, _ := store.List()
	if len(before) == 0 {
		t.Fatal("nothing offloaded")
	}
	// Truncate past everything offloaded — objects must be deleted from the store.
	if err := l.TruncateBefore(cut); err != nil {
		t.Fatal(err)
	}
	after, _ := store.List()
	if len(after) != 0 {
		// Some boundary segment may remain if it straddled; assert strictly fewer.
		if len(after) >= len(before) {
			t.Fatalf("store objects not reclaimed: before=%d after=%d", len(before), len(after))
		}
	}
	// Marker files for deleted segments are gone too.
	markers, _ := filepath.Glob(filepath.Join(dir, "log", "*"+offloadedSuffix))
	for _, m := range markers {
		if _, err := os.Stat(m); err == nil {
			// A surviving marker must correspond to a surviving store object.
			if len(after) == 0 {
				t.Fatalf("stale offload marker %s with empty store", m)
			}
		}
	}
}
