package commitlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A negative CompactMaxGoroutines is refused where it arrives.
//
// Zero means "unset" and selects the default, so the guard for it was a test
// for zero — and a test for zero accepts every other value as one the caller
// meant, including a negative. That value is passed to make(chan struct{}, n)
// in loadOrBuildDigests, which panics "makechan: size out of range".
//
// The panic is the reason this is refused in New rather than clamped there.
// Cleans run on a ticker in their own goroutine, so the panic is not an error
// any caller can be handed — it takes the process down, at the first clean,
// arbitrarily far from the line that set the option. Clamping would hide the
// mistake instead; New already refuses an unknown compression codec for the
// same reason, and this is the same shape one field along.
func TestANegativeCompactMaxGoroutinesIsRefused(t *testing.T) {
	_, err := New(Options{
		Path:                 tempDir(t),
		Compact:              true,
		CompactMaxGoroutines: -1,
	})
	require.Error(t, err,
		"New accepted a negative CompactMaxGoroutines; it reaches make(chan) "+
			"in the cleaner goroutine and takes the process down on the first clean")
	require.Contains(t, err.Error(), "CompactMaxGoroutines")
}

// Zero still means "use the default", which is the whole reason the check has
// to be < 0 and not != 0.
func TestAZeroCompactMaxGoroutinesIsTheDefault(t *testing.T) {
	l, err := New(Options{Path: tempDir(t), Compact: true})
	require.NoError(t, err)
	t.Cleanup(func() { l.Close() })
	require.Equal(t, defaultCompactMaxGoroutines,
		l.(*commitLog).compactCleaner.MaxGoroutines)
}
