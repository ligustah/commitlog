package commitlog

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Ensure LeaderEpochCache correctly stores epoch offsets.
func TestLeaderEpochCache(t *testing.T) {
	dir := tempDir(t)
	defer remove(t, dir)

	l, err := newLeaderEpochCache("foo", dir)
	require.NoError(t, err)

	// An empty cache cannot answer at all, which is a different thing from
	// answering -1. The caller substitutes the log end for the first and obeys
	// the second literally, so the flag is the whole difference between "keep
	// everything" and "discard everything".
	off, found := l.LastOffsetForLeaderEpoch(0)
	require.False(t, found, "an empty cache has no entry to answer from")
	require.Equal(t, int64(-1), off)
	require.Equal(t, uint64(0), l.LastLeaderEpoch())

	require.NoError(t, l.Assign(1, 0))

	off, found = l.LastOffsetForLeaderEpoch(1)
	require.False(t, found, "nothing above epoch 1 is recorded, so there is no entry to read")
	require.Equal(t, int64(-1), off)

	off, found = l.LastOffsetForLeaderEpoch(0)
	require.True(t, found, "epoch 1's entry answers a probe for epoch 0")
	require.Equal(t, int64(0), off)

	require.NoError(t, l.Assign(1, 10))

	off, found = l.LastOffsetForLeaderEpoch(0)
	require.True(t, found)
	require.Equal(t, int64(0), off)

	require.NoError(t, l.Assign(2, 10))
	require.NoError(t, l.Assign(3, 15))
	require.NoError(t, l.Assign(4, 30))
	require.NoError(t, l.Assign(5, 40))

	require.Equal(t, uint64(5), l.LastLeaderEpoch())

	require.NoError(t, l.ClearLatest(100))

	require.Equal(t, uint64(5), l.LastLeaderEpoch())

	require.NoError(t, l.ClearLatest(20))

	require.Equal(t, uint64(3), l.LastLeaderEpoch())
	require.Equal(t, int64(0), l.earliestOffset())
	require.Equal(t, int64(15), l.latestOffset())

	require.NoError(t, l.ClearEarliest(0))

	require.Equal(t, int64(0), l.earliestOffset())

	require.NoError(t, l.ClearEarliest(15))

	require.Equal(t, int64(15), l.earliestOffset())
	require.Equal(t, int64(15), l.latestOffset())

	require.NoError(t, l.ClearEarliest(16))

	require.Equal(t, int64(16), l.earliestOffset())
	require.Equal(t, int64(16), l.latestOffset())
}

// Ensure readLeaderEpochOffsets can correctly parse leader epoch checkpoint
// files.
func TestReadLeaderEpochOffsets(t *testing.T) {
	dir := tempDir(t)
	defer remove(t, dir)

	l, err := newLeaderEpochCache("foo", dir)
	require.NoError(t, err)

	require.NoError(t, l.Assign(1, 0))
	require.NoError(t, l.Assign(2, 10))
	require.NoError(t, l.Assign(3, 15))
	require.NoError(t, l.Assign(4, 30))
	require.NoError(t, l.Assign(5, 40))

	expected := []*epochOffset{
		{1, 0},
		{2, 10},
		{3, 15},
		{4, 30},
		{5, 40},
	}

	f, err := os.Open(filepath.Join(dir, leaderEpochFileName))
	require.NoError(t, err)
	defer f.Close()

	offsets, err := readLeaderEpochOffsets(f)
	require.NoError(t, err)
	require.Equal(t, expected, offsets)
}

// A checkpoint declaring a negative VERSION is corrupt and must be refused.
//
// The sibling test below covers the same laundering one field over. This one
// exists because the version gate was written as `version > leaderEpochFileV0`
// — "reject anything newer" — and Atoi is signed, so it also said "accept
// anything older". v0 is the FIRST version, so there is no older version to be
// tolerant of and that arm could only ever be reached by a corrupt file, which
// it then waved through to be parsed as v0.
//
// It matters because of what this file is not: it carries no checksum, so the
// parse is the only integrity check it ever gets, and a gate that admits a
// whole half of the integer range is not one.
func TestALeaderEpochCheckpointWithANegativeVersionIsRefused(t *testing.T) {
	dir := tempDir(t)
	defer remove(t, dir)

	l, err := newLeaderEpochCache("foo", dir)
	require.NoError(t, err)
	require.NoError(t, l.Assign(1, 0))
	require.NoError(t, l.Assign(2, 10))

	file := filepath.Join(dir, leaderEpochFileName)
	good, err := os.ReadFile(file)
	require.NoError(t, err)
	require.True(t, bytes.HasPrefix(good, []byte("0\n")),
		"the version is the first line; this test corrupts it in place")

	// Same file, version line flipped negative. Everything after it is intact,
	// so a parse that gets past the gate succeeds — which is the point: without
	// the gate this returns entries and no error.
	corrupt := append([]byte("-1\n"), good[len("0\n"):]...)
	require.NoError(t, os.WriteFile(file, corrupt, 0666))

	f, err := os.Open(file)
	require.NoError(t, err)
	defer f.Close()

	_, err = readLeaderEpochOffsets(f)
	require.Error(t, err, "a negative version was accepted and read as v0")
	require.Contains(t, err.Error(), "unknown version")

	// And the log refuses to open on it, rather than opening from a file whose
	// format it never actually established — the path that runs at startup.
	_, err = newLeaderEpochCache("foo", dir)
	require.Error(t, err)
}

// A checkpoint holding a negative epoch is corrupt and must be refused.
//
// An epoch is uint64 everywhere in this package, so nothing can write one. The
// hazard is entirely on the way back in: this parsed with ParseInt and then
// converted, which turned "-1" into 2^64-1 — a well-formed epoch larger than
// any real one. It becomes latestEpoch() permanently, outranks every epoch a
// leader will ever assign, and reads back indistinguishable from a genuine
// value. The parse is the only place the damage is still visible.
func TestALeaderEpochCheckpointWithANegativeEpochIsRefused(t *testing.T) {
	dir := tempDir(t)
	defer remove(t, dir)

	l, err := newLeaderEpochCache("foo", dir)
	require.NoError(t, err)
	require.NoError(t, l.Assign(1, 0))
	require.NoError(t, l.Assign(2, 10))

	file := filepath.Join(dir, leaderEpochFileName)
	good, err := os.ReadFile(file)
	require.NoError(t, err)

	// Same file, one epoch flipped negative.
	corrupt := bytes.Replace(good, []byte("2 10"), []byte("-1 10"), 1)
	require.NotEqual(t, good, corrupt, "the corruption must actually apply")
	require.NoError(t, os.WriteFile(file, corrupt, 0666))

	f, err := os.Open(file)
	require.NoError(t, err)
	defer f.Close()

	_, err = readLeaderEpochOffsets(f)
	require.Error(t, err, "a negative epoch parsed as 2^64-1 instead of failing")
	require.Contains(t, err.Error(), "invalid leader epoch value")

	// And the log refuses to open on it, rather than opening with a poisoned
	// epoch — this is the path that actually runs at startup.
	_, err = newLeaderEpochCache("foo", dir)
	require.Error(t, err)
}
