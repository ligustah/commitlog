package commitlog

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// localRetentionLog builds a log with several sealed segments, all written at a
// fixed instant, and returns it with the clock still frozen there. The caller
// advances the clock to decide which segments are past the horizon.
//
// The clock is stubbed rather than slept on: the horizon is a subtraction on
// record timestamps, so a test that sleeps is measuring the scheduler while
// claiming to measure the policy.
func localRetentionLog(t *testing.T, age time.Duration) (*commitLog, func(time.Duration)) {
	t.Helper()
	dir := tempDir(t)
	store, err := NewFileSegmentStore(filepath.Join(dir, "store"))
	require.NoError(t, err)

	real := timestamp
	now := real()
	timestamp = func() int64 { return now }
	t.Cleanup(func() { timestamp = real })

	l, err := New(Options{
		Path:              dir,
		MaxSegmentBytes:   1 << 12,
		Tiers:             oneTier(store),
		LocalRetentionAge: age,
		DisableAutoClean:  true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { l.Close() })

	var last int64
	for i := 0; i < 400; i++ {
		offs, err := l.Append([]*Message{{Value: []byte("padding value for a segment")}})
		require.NoError(t, err)
		last = offs[0]
	}
	l.SetHighWatermark(last)

	cl := l.(*commitLog)
	require.Greater(t, len(cl.segmentsSnapshot()), 2,
		"the fixture needs sealed segments to offload")
	require.Zero(t, offloadedCount(cl), "nothing should be offloaded yet")

	return cl, func(d time.Duration) { timestamp = func() int64 { return now + int64(d) } }
}

func offloadedCount(l *commitLog) int {
	n := 0
	for _, s := range l.segmentsSnapshot() {
		if s.isOffloaded() {
			n++
		}
	}
	return n
}

// A clean offloads what LocalRetentionAge has put past the horizon, with no
// caller driving it.
//
// This is the whole point of the option: the schedule used to live outside the
// log, which meant the caller also had to reproduce the "may this process write
// to the store" rule that OffloadBefore already applies. Every input was
// already here.
func TestACleanOffloadsPastTheLocalRetentionHorizon(t *testing.T) {
	l, advance := localRetentionLog(t, time.Hour)

	// Not yet: every record is younger than the horizon.
	_, err := l.CleanWithSpec(CleanSpec{})
	require.NoError(t, err)
	require.Zero(t, offloadedCount(l),
		"a clean offloaded segments that are younger than LocalRetentionAge")

	advance(2 * time.Hour)
	_, err = l.CleanWithSpec(CleanSpec{})
	require.NoError(t, err)
	require.Positive(t, offloadedCount(l),
		"a clean did not offload segments written two hours before a one-hour "+
			"horizon; nothing else was going to")

	// The records are still readable, which is what makes this a schedule and
	// not a retention limit — the bytes moved, the log did not shrink.
	require.Equal(t, int64(0), l.OldestOffset())
}

// Zero never offloads, which is what a log with no local retention policy must
// keep doing.
func TestAZeroLocalRetentionAgeNeverOffloads(t *testing.T) {
	l, advance := localRetentionLog(t, 0)
	advance(1000 * time.Hour)

	_, err := l.CleanWithSpec(CleanSpec{})
	require.NoError(t, err)
	require.Zero(t, offloadedCount(l),
		"a zero LocalRetentionAge offloaded anyway; zero means never")
}

// The offload runs OUTSIDE the pass's lock.
//
// CleanWithSpec holds cleanMu for the whole pass and OffloadBefore takes cleanMu
// for itself, so scheduling the offload inside the pass does not produce a
// subtly wrong answer — it deadlocks the log, and every later call on it. The
// test is that this returns at all; a regression hangs here rather than failing.
func TestTheScheduledOffloadDoesNotDeadlockTheCleanPass(t *testing.T) {
	l, advance := localRetentionLog(t, time.Minute)
	advance(time.Hour)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = l.CleanWithSpec(CleanSpec{})
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("CleanWithSpec did not return: the scheduled offload is taking " +
			"cleanMu while the pass still holds it")
	}
	require.Positive(t, offloadedCount(l))
}

// A log that does not own its tier runs the same schedule and offloads nothing.
//
// Deliberately not an error. OffloadBefore answers (0, nil) for a read-only
// tier so that every process can run the same schedule regardless of role, and
// this must not reintroduce a reason for the caller to know its own role.
func TestAReadOnlyTierSchedulesTheOffloadAndDoesNothing(t *testing.T) {
	l, advance := localRetentionLog(t, time.Minute)
	require.NoError(t, l.SetTierReadOnly(defaultTierName, true))
	advance(time.Hour)

	_, err := l.CleanWithSpec(CleanSpec{})
	require.NoError(t, err,
		"a log that does not own its tier must skip the offload silently, not fail")
	require.Zero(t, offloadedCount(l))
}
