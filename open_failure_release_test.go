package commitlog

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// A log that fails to open holds nothing afterwards.
//
// open() adds segments to the log one at a time and returns on the first that
// will not open, and New() throws the half-built log away. Every segment it got
// through by then holds an *os.File on its .log and a mapping of its .index, and
// nothing gave them back — so opening a directory whose thirtieth segment is
// damaged cost twenty-nine handles and twenty-nine mappings, and the retry a
// supervisor or a broker does cost that many again, every attempt, for the life
// of the process.
//
// The damage used here is a segment marked offloaded with no SegmentStore to
// serve it. It is picked for being an unambiguous refusal that owes nothing to
// any recovery path — the leak belongs to every early return in open(), not to
// the torn-tail work that happened to expose it, and a test that reached it
// through a torn tail would stop testing this the moment that case learned to
// recover.
func TestALogThatFailsToOpenHoldsNothingAfterwards(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{Path: dir, MaxSegmentBytes: 1024})
	require.NoError(t, err)
	for i := range 200 {
		_, err := l.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("k:%04d", i)),
			Value: []byte(fmt.Sprintf("v:%08d:%s", i, strings.Repeat("x", 32))),
		}})
		require.NoError(t, err)
	}
	require.NoError(t, l.Close())

	// Mark the LAST segment offloaded, so every segment below it is opened
	// before the refusal — that suffix is exactly what leaked.
	base, _, _ := activeSegment(t, dir)
	require.Positive(t, base, "the damage must sit above at least one whole segment")
	marker := filepath.Join(dir, fmt.Sprintf("%020d%s", base, offloadedSuffix))
	require.NoError(t, os.WriteFile(marker, []byte("{}"), 0o644))

	// Repeatedly, because one leaked handle and a hundred are the same bug and
	// only the hundred is visible: a retry loop is how this is met in practice.
	for i := range 60 {
		reopened, err := New(Options{Path: dir, MaxSegmentBytes: 1024})
		require.Error(t, err, "attempt %d: the log opened despite an offloaded "+
			"segment it has no store for", i)
		require.Nil(t, reopened)
	}

	if fds, err := os.ReadDir("/proc/self/fd"); err == nil {
		// Where the kernel will say so, say it directly: 60 refused opens over
		// ~19 segments each is over a thousand descriptors if none are given
		// back, against a default limit that is often 1024.
		require.Less(t, len(fds), 200,
			"a refused open kept its descriptors: %d are held after 60 attempts",
			len(fds))
	}

	// And where it will not, the directory answers instead. On Windows a mapped
	// index makes its file undeletable, so a leak from any of those attempts is
	// still being held right now and this fails. (On Unix an open file unlinks
	// happily, which is why the descriptor count above is the assertion that
	// carries there.)
	require.NoError(t, os.RemoveAll(dir),
		"the log directory could not be removed after 60 refused opens, so "+
			"something from them is still holding a file")
}
