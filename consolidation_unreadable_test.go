package commitlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// The consolidation-only pass must refuse a segment it cannot read to the end,
// exactly as the compaction pass does.
//
// consolidateOne walks a sealed segment and copies every record into a working
// copy, then Replace renames that copy over the source's files and closes the
// source. So a walk that stops early installs a PREFIX and deletes the file that
// held the rest. cleanSegment has guarded against this since ErrSegmentUnreadable
// existed; its sibling drove the same loop as
//
//	for ms, _, err := ss.Scan(); err == nil; ms, _, err = ss.Scan() {
//
// which cannot tell io.EOF from a read failure — both leave the loop, and what
// follows the loop is the install. The pass then returned nil: silent loss,
// reported as success.
//
// This is the DEFAULT configuration, not a corner. consolidateSegments is the
// else-branch of `if l.Compact` in clean.go, so it is the pass every
// non-compacted log runs on every automatic clean tick.
//
// See TestDamageInOneSegmentDoesNotKillTheProcess for the same argument on the
// three paths that already had the check — it runs with Compact: true, which is
// precisely why it never reached this one.
func TestConsolidationRefusesASegmentItCannotReadToTheEnd(t *testing.T) {
	dir := tempDir(t)
	opts := Options{
		Path:            dir,
		MaxSegmentBytes: 256 << 10,
		Compact:         false, // the consolidation-only path
		Compression:     compress.Zstd,
	}
	l, cleanup := setupWithOptions(t, opts)
	defer cleanup()

	const records = 12000
	for i := range records {
		_, err := l.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("key-%06d", i%64)),
			Value: []byte(fmt.Sprintf("value-%06d-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", i)),
		}})
		require.NoError(t, err)
	}
	l.SetHighWatermark(l.NewestOffset())

	// A sealed segment in the middle, carrying real consolidation debt — one
	// the pass will actually pick up and rewrite.
	l.mu.RLock()
	require.Greater(t, len(l.segments), 3, "need sealed segments to damage")
	victim := l.segments[len(l.segments)/2]
	l.mu.RUnlock()
	require.True(t, victim.needsBlockConsolidation(),
		"the victim must be a segment this pass would rewrite")

	// The victim's own extent, which is what the defect destroys: the truncated
	// copy ends at the damage and the records above it go with the original.
	//
	// A sequential read cannot see that. It stops AT the damaged frame either
	// way, so it reports the tail missing whether the tail exists or not — and
	// worse, the pre-fix log reads FURTHER, because deleting the damaged bytes is
	// what let the reader walk on into the next segment. That is why
	// TestDamageInOneSegmentDoesNotKillTheProcess, which measures exactly that
	// way, is blind to this: its measure improves when records are destroyed.
	victimBase, victimNext := victim.BaseOffset, victim.NextOffset()
	lastInVictim := victimNext - 1
	require.Greater(t, lastInVictim, victimBase, "victim segment holds one record")

	// In place, under a live log, leaving the file's length alone: damage on a
	// CLOSED log is caught when it opens, which never puts a damaged sealed
	// segment in front of the rewrite. Past the first block, so the walk gets
	// records out before it fails — a copy that stops at zero would be caught by
	// any assertion at all.
	path := filepath.Join(dir, fmt.Sprintf(fileFormat, victim.BaseOffset, logFileSuffix))
	st, err := os.Stat(path)
	require.NoError(t, err)
	require.Greater(t, st.Size(), int64(4096), "victim segment too small to damage mid-way")
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	require.NoError(t, err)
	garbage := make([]byte, 512)
	for i := range garbage {
		garbage[i] = 0xA5
	}
	_, err = f.WriteAt(garbage, st.Size()/2)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Whatever it decides, it must SAY so. A pass that meets unreadable bytes
	// and reports success has already thrown away whatever came after them.
	_, err = l.CleanWithSpec(CleanSpec{})
	t.Logf("consolidation over a damaged segment returned: %v", err)

	// Asserted BEFORE the error, deliberately. The loss is the severe claim and
	// the error only its symptom, so checking loss first says which of the two
	// failed when both do. The pass removes nothing by design — it rewrites
	// records verbatim — so unlike compaction there is no class of record it is
	// entitled to drop.
	//
	// The segment must still cover the range it covered: a failed pass leaves the
	// source exactly as it found it. Pre-fix this segment came back ending at the
	// damage, ~1200 records short.
	l.mu.RLock()
	var got *segment
	for _, s := range l.segments {
		if s.BaseOffset == victimBase {
			got = s
		}
	}
	l.mu.RUnlock()
	require.NotNil(t, got, "the victim segment left the log entirely")
	require.Equal(t, victimNext, got.NextOffset(),
		"consolidation truncated the segment it could not read: %d records lost",
		victimNext-got.NextOffset())

	// And end-to-end, not just in the bookkeeping: the last record of the victim
	// sits past the damage in an undamaged block, so an indexed seek still
	// reaches it — unless the rewrite threw it away.
	r, err2 := l.NewReader(From(lastInVictim), Uncommitted())
	require.NoError(t, err2)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, gotOff, _, _, err2 := r.ReadMessage(ctx, make([]byte, HeaderBufferLen))
	require.NoError(t, err2, "the victim's last record did not survive a failed pass")
	require.Equal(t, lastInVictim, gotOff)

	// Whatever it decides, it must SAY so.
	require.ErrorIs(t, err, ErrSegmentUnreadable,
		"the consolidation pass met unreadable bytes and called it done")
}
