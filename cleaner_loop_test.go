package commitlog

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A cleaner tick that rolls a segment still cleans.
//
// cleanerLoop rolled the active segment first and then, if it had rolled one,
// skipped the clean, on the stated premise that the cleaner "already ran".
// checkAndPerformSplit does not run the cleaner and never did; Clean has exactly
// one caller. So a rolling tick was a skipped pass, not a redundant one.
//
// Which logs that hurt depended entirely on load, which is why it survived: a
// quiet log rarely has a segment ready to roll and cleaned every tick. A log
// under continuous write always has one. And the usual pairing makes it certain
// rather than likely — CheckSplit is true once the active segment reaches
// MaxSegmentAge, so a log with MaxSegmentAge at or below CleanerInterval has a
// roll pending at EVERY tick. durable_streams measured exactly that: a 4.5GB
// compacted log over 5.5 hours, 336 segments, 239 live keys, ~66 ticks, zero
// rewrites, and every key digest stamped in the final minute.
//
// It drives ONE TICK rather than the ticker. Every other compaction test in this
// package calls Clean() directly — the one path production never takes, which is
// how a broken loop around a working pass went unseen. Driving the ticker
// instead would put the assertion at the mercy of how long a pass takes against
// how often it fires; both are worth knowing, and neither is this claim.
func TestATickThatRollsASegmentStillCleans(t *testing.T) {
	l, err := New(Options{
		Name: "rolling", Path: tempDir(t), Compact: true,
		// The roll below is age-driven, as the reported one was: the segment is
		// nowhere near full, and MaxSegmentAge is what makes it due.
		MaxSegmentBytes: 64 << 20,
		MaxSegmentAge:   time.Millisecond,
		CleanerInterval: time.Hour, // the loop must not race this test
	})
	require.NoError(t, err)
	defer l.Close()

	// One key, rewritten: every record but the newest is superseded, so a pass
	// that runs has work and a pass that is skipped leaves the head where it is.
	appendRun := func(prefix string, n int) {
		for i := 0; i < n; i++ {
			batch := make([]*Message, 0, 100)
			for j := 0; j < 100; j++ {
				batch = append(batch, &Message{
					Key:   []byte("k"),
					Value: []byte(fmt.Sprintf("%s:%08d", prefix, i*100+j)),
				})
			}
			_, err := l.Append(batch)
			require.NoError(t, err)
		}
		l.SetHighWatermark(l.NewestOffset())
	}

	cl := l.(*commitLog)
	appendRun("head", 40)
	// More than one segment, so there is a prefix for compaction to retire.
	require.NoError(t, cl.split(cl.activeSegment()))
	appendRun("tail", 5)

	// Past MaxSegmentAge, so the tick is certain to roll — which is the whole
	// condition under test.
	time.Sleep(5 * time.Millisecond)
	require.True(t, cl.activeSegment().CheckSplit(cl.MaxSegmentAge),
		"the fixture must present the tick with a segment that is due to roll, "+
			"or it is not testing a rolling tick at all")

	before := l.OldestOffset()
	cl.cleanerTick()

	require.Greaterf(t, l.OldestOffset(), before,
		"the tick rolled a segment and compacted nothing: oldest stayed at %d "+
			"on a log whose every record but the newest is superseded", before)
}

// The automatic pass is bounded, and a caller can turn that off.
//
// The budget existed and the spec-less path could not reach it: CleanSpec has
// RewriteBudget, Clean() built an empty spec, and the cleaner goroutine is the
// only caller of Clean(). So the one pass nobody drives by hand was the one pass
// that could run for as long as it liked — 6m42s against a 5m interval, on the
// log this came from.
// segmentBytes is what the log currently occupies on disk, which is the only
// thing a caller who wanted compaction actually cares about.
func segmentBytes(t *testing.T, dir string) int64 {
	t.Helper()
	logs, err := filepath.Glob(filepath.Join(dir, "*"+logFileSuffix))
	require.NoError(t, err)
	var total int64
	for _, p := range logs {
		fi, serr := os.Stat(p)
		require.NoError(t, serr)
		total += fi.Size()
	}
	return total
}

// A log cleans at OPEN, without waiting out an interval first.
//
// NewTicker does not fire until t+interval and nothing on disk records when the
// last pass ran, so the loop waited a full CleanerInterval before its first pass
// and the clock restarted on every process start. A process that lives less than
// the interval never cleaned AT ALL — not rarely, never, however much there was
// to reclaim. sqlcdc measured it: 149 restarts averaging 95.9s against a 5m
// interval, zero passes in four hours, and the one pass that did fire reclaimed
// 69%.
//
// Written as a REOPEN because that is the shape of the bug. The data is already
// on disk from a previous run and the question is whether the process that
// inherits it ever acts on it; a test that opened an empty log and appended
// would be asking something else. CleanerInterval is an hour, so the open pass
// is the only thing that can possibly be what cleaned.
//
// Note the fixture is built with DisableAutoClean and the assertion made after
// reopening WITHOUT it. That is deliberate: it means the "before" measurement is
// of a log nothing has cleaned, so the drop cannot be work that had already
// happened.
func TestALogCleansAtOpenWithoutWaitingForATick(t *testing.T) {
	dir := tempDir(t)
	build := Options{
		Name: "reopen", Path: dir, Compact: true,
		MaxSegmentBytes:  4 << 10,
		CleanerInterval:  time.Hour,
		DisableAutoClean: true,
	}

	l, err := New(build)
	require.NoError(t, err)
	cl := l.(*commitLog)
	// One key, rewritten: everything but the newest record is superseded, so a
	// pass that runs has a great deal to reclaim and a pass that never runs
	// leaves every byte where it is.
	for i := 0; i < 40; i++ {
		batch := make([]*Message, 0, 50)
		for j := 0; j < 50; j++ {
			batch = append(batch, &Message{
				Key: []byte("k"), Value: []byte(fmt.Sprintf("v:%08d", i*50+j)),
			})
		}
		_, aerr := cl.Append(batch)
		require.NoError(t, aerr)
	}
	cl.SetHighWatermark(cl.NewestOffset())
	require.NoError(t, cl.Close())

	before := segmentBytes(t, dir)
	require.Positive(t, before, "the fixture wrote nothing, so nothing could be reclaimed")

	// Reopen with the cleaner live. Its first tick is an hour away.
	reopen := build
	reopen.DisableAutoClean = false
	l2, err := New(reopen)
	require.NoError(t, err)
	t.Cleanup(func() { l2.Close() })

	require.Eventually(t, func() bool {
		return segmentBytes(t, dir) < before
	}, 30*time.Second, 50*time.Millisecond,
		"the log still occupies %d bytes after reopening; the cleaner is waiting "+
			"out a %s interval and a process that restarts sooner than that will "+
			"never clean at all", before, reopen.CleanerInterval)
}

func TestTheAutomaticCleanIsBounded(t *testing.T) {
	open := func(o Options) *commitLog {
		o.Name, o.Path = "budget", tempDir(t)
		l, err := New(o)
		require.NoError(t, err)
		t.Cleanup(func() { l.Close() })
		return l.(*commitLog)
	}

	// Unset: the budget is the tick, so a pass cannot outlive the gap between
	// two of them without being asked to.
	l := open(Options{Compact: true, CleanerInterval: 90 * time.Second})
	require.Equal(t, 90*time.Second, l.Options.CleanRewriteBudget)

	// Set: taken verbatim.
	l = open(Options{Compact: true, CleanerInterval: 90 * time.Second,
		CleanRewriteBudget: 3 * time.Second})
	require.Equal(t, 3*time.Second, l.Options.CleanRewriteBudget)

	// Negative: no budget at all, which is what every spec-less pass used to
	// have. It must survive the zero-means-default rule rather than be rounded
	// back into it.
	l = open(Options{Compact: true, CleanerInterval: 90 * time.Second,
		CleanRewriteBudget: -1})
	require.Negative(t, l.Options.CleanRewriteBudget)
}
