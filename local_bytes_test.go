package commitlog

import (
	"os"
	"path/filepath"
	"testing"
)

// diskLogBytes sums the .log files under a directory, which is what LocalBytes
// claims to report. Measured independently rather than derived from the same
// segment positions the implementation adds up — a test that asked the log the
// same question twice would agree with any answer.
func diskLogBytes(t *testing.T, dir string) int64 {
	t.Helper()
	var n int64
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || filepath.Ext(path) != ".log" {
			return nil
		}
		n += info.Size()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// A LOG'S SIZE IS WHAT MOVING IT WOULD COST, so it has to be the bytes that are
// actually here.
//
// This backs a placement decision, and being wrong in either direction is
// expensive in a different way: reported too small, a rebalancer copies a
// terabyte to correct a rounding error; too large, it refuses to move anything
// and the cluster stays uneven. Zero for everything — which is what the broker
// reported for every partition before this existed — reads as the first.
func TestALogsLocalBytesAreTheBytesOnDisk(t *testing.T) {
	dir := t.TempDir()
	l, err := New(Options{Name: "sized", Path: dir, MaxSegmentBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	if got := l.LocalBytes(); got != 0 {
		t.Fatalf("an empty log holds %d bytes, want 0", got)
	}

	for i := 0; i < 40; i++ {
		appendMsg(t, l, "value-number-"+string(rune('A'+i%26))+"-padded-to-roll-segments")
	}
	if err := l.SyncAll(); err != nil {
		t.Fatal(err)
	}

	got, want := l.LocalBytes(), diskLogBytes(t, dir)
	if got != want {
		t.Fatalf("LocalBytes reported %d, the .log files on disk hold %d", got, want)
	}
	if got == 0 {
		t.Fatal("forty records reported as zero bytes, which is the failure this exists to catch")
	}
}

// OFFLOADED BYTES ARE NOT THIS BROKER'S TO MOVE.
//
// A tiered log's cold segments live in a store whoever takes the log over reads
// the same way, so they cost nothing to hand on. Counting them would report a
// tiered partition — the one that is cheapest to move — as the most expensive
// in the cluster, and MaxBytes would refuse to move it for as long as the tier
// kept growing.
func TestOffloadedSegmentsAreNotLocalBytes(t *testing.T) {
	dir := t.TempDir()
	l, _ := offloadTestLog(t, dir)
	defer l.Close()

	const n = 40
	var offs [n]int64
	for i := 0; i < n; i++ {
		offs[i] = appendMsg(t, l, "value-number-"+string(rune('A'+i%26))+"-padded-to-roll-segments")
	}
	if err := l.SyncAll(); err != nil {
		t.Fatal(err)
	}
	before := l.LocalBytes()
	if before == 0 {
		t.Fatal("nothing local before offloading")
	}

	count, err := l.OffloadBefore(offs[n-3])
	if err != nil {
		t.Fatal(err)
	}
	if count == 0 {
		t.Fatal("nothing was offloaded, so this test asserts nothing")
	}

	after := l.LocalBytes()
	if after >= before {
		t.Fatalf("offloading %d segments left LocalBytes at %d, was %d — the tier is being counted as local",
			count, after, before)
	}
	if got, want := after, diskLogBytes(t, filepath.Join(dir, "log")); got != want {
		t.Fatalf("LocalBytes reported %d after offloading, the local .log files hold %d", got, want)
	}
}

// SPACE GIVEN BACK IS SPACE THIS STOPS REPORTING.
//
// Retention is the one thing that makes a log smaller while it is open, and a
// size that only ever grew would be a size that describes the log's history
// rather than the log. The rebalancer would go on refusing to move a partition
// that had been trimmed to nothing.
func TestRetentionLowersALogsLocalBytes(t *testing.T) {
	dir := t.TempDir()
	l, err := New(Options{Name: "trimmed", Path: dir, MaxSegmentBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	const n = 40
	var offs [n]int64
	for i := 0; i < n; i++ {
		offs[i] = appendMsg(t, l, "value-number-"+string(rune('A'+i%26))+"-padded-to-roll-segments")
	}
	if err := l.SyncAll(); err != nil {
		t.Fatal(err)
	}
	before := l.LocalBytes()

	if err := l.TruncateBefore(offs[n-3]); err != nil {
		t.Fatal(err)
	}
	after := l.LocalBytes()
	if after >= before {
		t.Fatalf("after trimming to the last three records LocalBytes is %d, was %d", after, before)
	}
	if got, want := after, diskLogBytes(t, dir); got != want {
		t.Fatalf("LocalBytes reported %d after retention, the .log files hold %d", got, want)
	}
}
