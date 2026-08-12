package commitlog

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// A compaction pass that fails part-way still publishes the rewrites it
// installed.
//
// The rewrite phase installs one segment at a time, and installing means
// Replace: the rewrite's files are renamed OVER the source's, and the source is
// closed. From that moment the rewrite is what that base offset is, on disk.
// The pass then used to answer any later failure with `return nil, 0, -1, err`,
// and the caller swapped in the delete stage's list instead — the closed
// sources. Every rewrite the pass had already installed was left named by
// nothing: not in l.segments, so closeSegments never walked it, so its file
// handle and its index mapping were held for the life of the process. Readers
// still worked, because current() redirects through the source's replacement
// link, which is exactly why nothing noticed.
//
// What noticed was Windows. durable_streams runs maintenance concurrently with
// Close — a pass reading a segment the close just shut fails precisely this way
// — and reported a data directory that could not be removed after a Close that
// returned nil, blocked by "00000000000000000160.index: The process cannot
// access the file because it is being used by another process".
//
// The fixture closes a segment underneath the pass, which is that race made
// deterministic. Density order is what makes it a PARTIAL failure rather than an
// immediate one: the rewrite phase spends its budget on the densest segment
// first, so the segment with five superseded records is rewritten and installed
// before the one with a single one fails.
func TestAFailedCompactionPassPublishesTheRewritesItInstalled(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{
		Name: "partial", Path: dir, Compact: true, DisableAutoClean: true,
		// Rolls happen where this test asks for them, not by size.
		MaxSegmentBytes: 64 << 20,
	})
	require.NoError(t, err)
	cl := l.(*commitLog)

	app := func(key, value string) {
		off, err := cl.Append([]*Message{{Key: []byte(key), Value: []byte(value)}})
		require.NoError(t, err)
		// Compaction only considers records at or below the ceiling, and the
		// spec-less pass takes the high watermark as one. Without this the pass
		// has nothing to drop and installs nothing.
		cl.SetHighWatermark(off[0])
	}

	// Segment 0: five superseded copies of one key.
	for i := range 6 {
		app("dense", fmt.Sprintf("v%d", i))
	}
	require.NoError(t, cl.split(cl.activeSegment()))
	// Segment 1: exactly one.
	app("sparse", "v0")
	app("sparse", "v1")
	require.NoError(t, cl.split(cl.activeSegment()))
	// The active segment is never rewritten; it is here so the two above are not.
	app("tail", "v0")

	cl.mu.RLock()
	before := append([]*segment(nil), cl.segments...)
	cl.mu.RUnlock()
	require.Len(t, before, 3, "the fixture needs two sealed segments and an active one")

	// Both sealed segments get their key-digest sidecar, which is what a segment
	// a pass has already scanned carries. Without them the pass builds the
	// digests by scanning, and the sabotage below would fail it there — before
	// the rewrite phase this test is about ever starts.
	for _, seg := range before[:2] {
		d, err := buildKeyDigest(seg, newBlockCache())
		require.NoError(t, err)
		require.NoError(t, writeKeyDigest(seg, d))
	}

	// The sabotage: segment 1 is shut, so the scan that rewrites it fails —
	// after segment 0, the denser of the two, has already been installed.
	require.NoError(t, before[1].Close())

	require.Error(t, cl.Clean(),
		"the fixture must fail the pass, or it asserts nothing about a failed one")

	// The rewrite that FAILED leaks the same way one level down: its working copy
	// is an open segment with its own files and its own index mapping, and once
	// the rewrite returns nothing can reach it to close it. Only the scan-failure
	// path used to dispose of one; the .cleaned artifacts on disk are what makes
	// the leak visible from outside.
	strays, err := filepath.Glob(filepath.Join(dir, "*"+cleanedSuffix))
	require.NoError(t, err)
	require.Empty(t, strays,
		"the failed rewrite left its working copy behind: it is open, mapped, and "+
			"nothing names it")

	before[0].RLock()
	rewrite := before[0].replacement
	before[0].RUnlock()
	require.NotNil(t, rewrite,
		"the fixture must install a rewrite BEFORE the failure, or the leak it "+
			"is about cannot happen: check the drop-density ordering above")

	cl.mu.RLock()
	published := append([]*segment(nil), cl.segments...)
	cl.mu.RUnlock()
	require.Contains(t, published, rewrite,
		"the pass failed and dropped the rewrite it had already installed: it is "+
			"reachable only through the source's replacement link, so nothing will "+
			"ever close it")
	require.NotContains(t, published, before[0],
		"the source of an installed rewrite is closed and its files are gone; "+
			"republishing it leaves the log serving a segment that only current() "+
			"can rescue")

	require.NoError(t, cl.Close())

	rewrite.RLock()
	closed := rewrite.closed
	rewrite.RUnlock()
	require.True(t, closed,
		"Close reported success with the rewrite's index still mapped")

	// The downstream symptom, on the platform that reports it. A no-op elsewhere,
	// and it costs nothing to state the claim in the form the bug was found in.
	require.NoError(t, os.RemoveAll(dir),
		"the log directory could not be removed after a Close that reported success")
}
