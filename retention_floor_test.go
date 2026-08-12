package commitlog

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Retention deletes whole segments by age, bytes or message count. None of
// those measures knows anything about a caller that is still USING the records
// in them — a transactional caller's staged records sit in the log for as long
// as its transaction is open, and a limit reached in the meantime collected
// them out from under it. The commit that followed then referred to offsets
// that no longer existed.
//
// CleanSpec.RetentionFloor is the bound: a segment is eligible only if the
// whole of it lies strictly below the floor. Deletion is per segment, so a
// segment holding one protected record is protected entire.

// oneMessagePerSegment builds n segments at base offsets 0..n-1, one record
// each, so a segment's base offset IS the offset it holds. That makes every
// assertion below a statement about offsets rather than about indices.
func oneMessagePerSegment(t *testing.T, n int) []*segment {
	t.Helper()
	dir := tempDir(t)
	segs := make([]*segment, n)
	for i := range n {
		segs[i] = createSegment(t, dir, int64(i), 20)
		writeToSegment(t, segs[i], int64(i), []byte("blah"))
	}
	return segs
}

// The message limit stops at the floor instead of deleting through it.
func TestTheMessageLimitStopsAtTheRetentionFloor(t *testing.T) {
	opts := deleteCleanerOptions{Name: "floor"}
	opts.Retention.Messages = 5
	cleaner := newDeleteCleaner(opts)

	segs := oneMessagePerSegment(t, 20)

	// Unbounded, this limit keeps five segments (offsets 15..19) — that is what
	// the same fixture does with a nil floor, and it is what the floor has to
	// override rather than coincide with.
	actual, err := cleaner.Clean(segs, nil, At(12))
	require.NoError(t, err)

	require.Equal(t, int64(12), actual[0].BaseOffset,
		"retention deleted the segment holding the floor offset")
	require.Len(t, actual, 8)
}

// The bytes limit, same story. Each limit computes its prefix differently and
// they are applied in sequence, so covering one proves nothing about the next.
func TestTheBytesLimitStopsAtTheRetentionFloor(t *testing.T) {
	segs := oneMessagePerSegment(t, 20)

	opts := deleteCleanerOptions{Name: "floor"}
	// Room for two segments, derived from what one actually occupies rather
	// than hard-coded (a literal here would encode the frame header's size).
	opts.Retention.Bytes = 2 * segs[0].Position()
	cleaner := newDeleteCleaner(opts)

	actual, err := cleaner.Clean(segs, nil, At(7))
	require.NoError(t, err)

	require.Equal(t, int64(7), actual[0].BaseOffset)
	require.Len(t, actual, 13)
}

// And the age limit, which runs FIRST and so decides what the other two see.
func TestTheAgeLimitStopsAtTheRetentionFloor(t *testing.T) {
	before := computeTTL
	computeTTL = func(age time.Duration) int64 { return 200 - int64(age) }
	defer func() { computeTTL = before }()

	opts := deleteCleanerOptions{Name: "floor"}
	opts.Retention.Age = 100
	cleaner := newDeleteCleaner(opts)

	dir := tempDir(t)
	segs := make([]*segment, 20)
	for i := range 20 {
		segs[i] = createSegment(t, dir, int64(i), 20)
		ms, entries, err := newMessageSetFromProto(int64(i), 0,
			[]*Message{{Timestamp: int64(i * 10)}})
		require.NoError(t, err)
		require.NoError(t, segs[i].WriteMessageSet(ms, entries))
	}

	// Unbounded, this expires the first ten (see TestDeleteCleanerAge).
	actual, err := cleaner.Clean(segs, nil, At(4))
	require.NoError(t, err)

	require.Equal(t, int64(4), actual[0].BaseOffset)
	require.Len(t, actual, 16)
}

// A floor of ZERO protects the whole log, and it has to: a transaction that
// began at offset 0 is an ordinary transaction — the first one a fresh log
// ever sees — and it is the case every plausible int64 sentinel would get
// wrong, because 0 is both "the beginning of the log" and the zero value of
// the field. That is why the spec takes a Bound, and this is the test that
// fails if someone later decides a plain int64 would have been tidier.
func TestARetentionFloorOfZeroProtectsTheWholeLog(t *testing.T) {
	opts := deleteCleanerOptions{Name: "floor"}
	opts.Retention.Messages = 5
	opts.Retention.Bytes = 1
	cleaner := newDeleteCleaner(opts)

	segs := oneMessagePerSegment(t, 20)

	actual, err := cleaner.Clean(segs, nil, At(0))
	require.NoError(t, err)
	require.Len(t, actual, 20, "a floor at offset 0 leaves nothing eligible")
	require.Equal(t, segs, actual)
}

// No floor is the behaviour every existing caller has: the limits apply in
// full. The fix must not quietly turn retention off for anyone who never asked
// for a floor — an unbounded log nothing complains about until a disk fills is
// a worse bug than the one being fixed.
func TestNoRetentionFloorLeavesTheLimitsAlone(t *testing.T) {
	opts := deleteCleanerOptions{Name: "floor"}
	opts.Retention.Messages = 5
	cleaner := newDeleteCleaner(opts)

	segs := oneMessagePerSegment(t, 20)

	actual, err := cleaner.Clean(segs, nil, Bound{})
	require.NoError(t, err)
	require.Len(t, actual, 5)
	require.Equal(t, int64(15), actual[0].BaseOffset)
}

// A floor ABOVE everything the log holds is not a licence to delete the log.
// The active segment is retained as it always was — the floor only ever
// removes eligibility, it never adds any.
func TestAFloorAboveTheLogStillKeepsTheActiveSegment(t *testing.T) {
	opts := deleteCleanerOptions{Name: "floor"}
	opts.Retention.Messages = 1
	cleaner := newDeleteCleaner(opts)

	segs := oneMessagePerSegment(t, 5)

	actual, err := cleaner.Clean(segs, nil, At(9999))
	require.NoError(t, err)
	require.Len(t, actual, 1)
	require.Equal(t, int64(4), actual[0].BaseOffset)
}

// The tier is bound by the same floor. A transaction open long enough for its
// segment to be OFFLOADED is unusual, but the tier limits delete objects on
// exactly the same reasoning the local ones do, and they have one more way to
// go wrong: a tier has no active segment to protect, so every one of its
// segments is eligible by default.
func TestTierRetentionStopsAtTheRetentionFloor(t *testing.T) {
	dir := tempDir(t)
	var segs []*segment
	for i := range 4 {
		s := createSegment(t, dir, int64(i*10), 4096)
		ms, entries, err := newMessageSetFromProto(int64(i*10), 0,
			[]*Message{{Value: []byte("0123456789")}})
		require.NoError(t, err)
		require.NoError(t, s.WriteMessageSet(ms, entries))
		segs = append(segs, s)
	}
	fakeOffloaded(segs[0])
	fakeOffloaded(segs[1])
	fakeOffloaded(segs[2])

	deleted := map[int64]bool{}
	restore := deleteSegment
	deleteSegment = func(s *segment) error { deleted[s.BaseOffset] = true; return nil }
	defer func() { deleteSegment = restore }()

	// A budget with room for one tiered segment: unbounded, this deletes the
	// two oldest objects (see TestTierRetentionDropsOffloadedSegments). The
	// floor sits inside the second, so only the first may go.
	c := tierCleaner(t, func(o *deleteCleanerOptions) {
		o.Tiers[0].MaxBytes = segs[2].Position()
	})
	out, err := c.Clean(segs, nil, At(15))
	require.NoError(t, err)

	require.True(t, deleted[int64(0)], "the oldest object is entirely below the floor")
	require.False(t, deleted[int64(10)], "this object holds the floor offset")
	require.Len(t, out, 3)
}

// A floor in the LOCAL half must not protect the tier. The tier's eligibility
// is measured against the whole log, not against its own half — measuring the
// half alone would either lose the boundary at its newest segment or hold
// objects nobody is using.
func TestAFloorInTheLocalHalfLeavesTheTierEligible(t *testing.T) {
	dir := tempDir(t)
	var segs []*segment
	for i := range 4 {
		s := createSegment(t, dir, int64(i*10), 4096)
		ms, entries, err := newMessageSetFromProto(int64(i*10), 0,
			[]*Message{{Value: []byte("0123456789")}})
		require.NoError(t, err)
		require.NoError(t, s.WriteMessageSet(ms, entries))
		segs = append(segs, s)
	}
	fakeOffloaded(segs[0])
	fakeOffloaded(segs[1])

	deleted := map[int64]bool{}
	restore := deleteSegment
	deleteSegment = func(s *segment) error { deleted[s.BaseOffset] = true; return nil }
	defer func() { deleteSegment = restore }()

	// TierBytes of 1 admits nothing, so both objects are over budget. The floor
	// is at offset 25, in the last (local, active) segment — above every tiered
	// record, so the tier is unconstrained by it.
	c := tierCleaner(t, func(o *deleteCleanerOptions) { o.Tiers[0].MaxBytes = 1 })
	_, err := c.Clean(segs, nil, At(25))
	require.NoError(t, err)

	require.True(t, deleted[int64(0)])
	require.True(t, deleted[int64(10)],
		"a floor above the tier entirely must not hold its objects")
}

// End to end, through the log's own pass: a spec carrying a floor bounds
// retention, and the same spec without one does not. The unit tests above
// drive the cleaner directly; this one proves the field is actually wired
// through CleanWithSpec, which is the only way a caller can reach it.
func TestCleanWithSpecHonoursTheRetentionFloor(t *testing.T) {
	l, err := New(Options{
		Path:             tempDir(t),
		MaxSegmentBytes:  32,
		MaxLogMessages:   5,
		DisableAutoClean: true,
	})
	require.NoError(t, err)
	defer l.Close()

	for i := range 40 {
		_, err := l.Append([]*Message{{Value: []byte("payload")}})
		require.NoError(t, err, "append %d", i)
	}
	require.Zero(t, l.OldestOffset(), "the fixture must start with a full history")

	floor := l.NewestOffset() / 2
	_, err = l.CleanWithSpec(CleanSpec{RetentionFloor: At(floor)})
	require.NoError(t, err)
	require.LessOrEqual(t, l.OldestOffset(), floor,
		"retention deleted past the floor the spec set")

	// And without one, the same limit collects as it always has.
	_, err = l.CleanWithSpec(CleanSpec{})
	require.NoError(t, err)
	require.Greater(t, l.OldestOffset(), int64(0),
		"retention collected nothing on a log 8x over its message limit")
}
