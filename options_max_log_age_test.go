package commitlog

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Options.MaxLogAge is the only public way to ask for retention by age, and a
// single line in New — cleanerOpts.Retention.Age = opts.MaxLogAge — is the
// whole of its wiring. Nothing exercised that line.
//
// Both ends were covered and the join between them was not: every age-retention
// test builds a deleteCleanerOptions by hand and assigns Retention.Age directly,
// or reaches into l.deleteCleaner to set it. The only test that mentioned
// MaxLogAge at all set it to -1s to assert New REFUSES, so no test ever gave it
// a value that does anything. Its four siblings — MaxLogBytes, MaxLogMessages,
// MaxSegmentAge, LocalRetentionAge — are each set from Options somewhere; this
// one was the orphan.
//
// A break there is silent in the worst way. The caller sets a limit, the cleaner
// is never asked to apply one, noRetentionLimits reports that retention is
// configured, and the log grows without bound with no error anywhere.
//
// computeTTL is the observation point because it is the ONLY thing the age
// limit does with the configured duration: the value it receives IS
// Retention.Age. That makes this exact rather than inferred from which segments
// happened to survive, and independent of the clock, the retention floor, and
// how many bytes a record occupies — the three things that made the sibling
// assertion in append_timestamp_test.go have to be written as an inequality.
func TestOptionsMaxLogAgeReachesTheCleaner(t *testing.T) {
	// Not a round number, and not equal to any other duration this test sets, so
	// a wiring line that picked up the wrong field cannot coincidentally pass.
	const wantAge = 73 * time.Minute

	var (
		mu     sync.Mutex
		called int
		gotAge time.Duration
	)
	restore := computeTTL
	computeTTL = func(age time.Duration) int64 {
		mu.Lock()
		called++
		gotAge = age
		mu.Unlock()
		// Now, so every sealed segment is older than the TTL and the limit has
		// real work to do instead of stopping at its first comparison.
		return time.Now().UnixNano()
	}
	t.Cleanup(func() { computeTTL = restore })

	l, err := New(Options{
		Name:            "maxlogage",
		Path:            tempDir(t),
		MaxSegmentBytes: 64,
		MaxLogAge:       wantAge,
		// The cleaner loop would call computeTTL on its own timer, which would
		// satisfy the assertions below without this test's Clean() having done
		// anything. Driving the pass explicitly is the point.
		DisableAutoClean: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })

	for i := 0; i < 12; i++ {
		_, err := l.Append([]*Message{{Value: []byte("a record long enough to roll a segment")}})
		require.NoError(t, err)
	}
	cl := l.(*commitLog)
	require.Greater(t, len(cl.segments), 1,
		"fixture is vacuous: applyAgeLimit returns immediately on a single segment, "+
			"so the age limit would never reach computeTTL however it was configured")

	require.NoError(t, l.Clean())

	mu.Lock()
	defer mu.Unlock()
	require.Positive(t, called,
		"the age limit never ran, so Retention.Age was not above zero: "+
			"Options.MaxLogAge did not reach the delete cleaner")
	require.Equal(t, wantAge, gotAge,
		"the age limit ran with %s, not the %s given as Options.MaxLogAge", gotAge, wantAge)
}
