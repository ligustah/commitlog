package commitlog

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// ReadMessageSet must tell a follower that the bytes are damaged.
//
// It is the replication fetch: the caller appends what it gets and continues
// from the last offset it appended. So an empty set with a nil error — which is
// what a scan failure used to produce — sends that caller back to the SAME
// offset forever. No progress, no error, nothing to distinguish "you are caught
// up with this segment" from "this segment is damaged and you will never get
// past it". This is precisely the caller ErrSegmentUnreadable's doc describes:
// one with a peer to copy from, which a retry of the same call cannot help.
//
// Both halves are asserted, because the rule is not "any damage is an error":
//
//   - a fetch that read whole frames BEFORE meeting the damage returns them with
//     no error. That is real progress, and the follower's next call starts at the
//     damaged frame.
//   - a fetch that starts AT the damage has nothing to hand back, and that is the
//     one that must say so.
func TestReadMessageSetReportsDamageRatherThanAnEmptySet(t *testing.T) {
	dir := tempDir(t)
	l, cleanup := setupWithOptions(t, Options{
		Path:            dir,
		MaxSegmentBytes: 4 << 10,
	})
	defer cleanup()

	for i := range 400 {
		offs, err := l.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("k:%04d", i)),
			Value: []byte(fmt.Sprintf("v:%04d:xxxxxxxxxxxxxxxxxxxxxxxxxxxx", i)),
		}})
		require.NoError(t, err)
		l.SetHighWatermark(offs[0])
	}

	// A sealed segment in the middle: one with records on both sides of it, and
	// not the one still being appended to.
	l.mu.RLock()
	require.Greater(t, len(l.segments), 3, "need a sealed segment to damage")
	victim := l.segments[len(l.segments)/2]
	l.mu.RUnlock()

	// A frame boundary a few records in, so there are whole frames before the
	// damage and whole frames after it. Taken from the index rather than computed
	// from a record size, which would silently stop being a boundary the moment
	// the framing changes.
	damagedOffset := victim.BaseOffset + 3
	require.Greater(t, victim.NextOffset(), damagedOffset+1, "victim segment too short")
	e, err := victim.findEntry(damagedOffset)
	require.NoError(t, err)
	require.Greater(t, e.Position, int64(0), "need frames before the damage")

	// In place, under a live log, leaving the length alone — damage on a closed
	// log is caught when it opens.
	path := filepath.Join(dir, fmt.Sprintf(fileFormat, victim.BaseOffset, logFileSuffix))
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	require.NoError(t, err)
	garbage := make([]byte, msgSetHeaderLen)
	for i := range garbage {
		garbage[i] = 0xA5
	}
	_, err = f.WriteAt(garbage, e.Position)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Starting BELOW the damage: the frames before it are whole and are progress.
	before, err := l.ReadMessageSet(victim.BaseOffset, 1<<20)
	require.NoError(t, err,
		"a fetch that met damage after reading whole frames must return them")
	require.NotEmpty(t, before, "the frames before the damage are readable")

	// Starting AT it: nothing to return, so it has to say why.
	got, err := l.ReadMessageSet(damagedOffset, 1<<20)
	require.ErrorIs(t, err, ErrSegmentUnreadable,
		"a fetch with nothing to return answered a damaged segment with a nil error")
	require.Empty(t, got)
}
