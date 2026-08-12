package commitlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// joinSpec groups roughly four of the fixture's segments per run.
var joinSpec = CleanSpec{JoinBelow: 8 << 10}

// joinFixture builds a log with several small sealed segments and returns it
// alongside every record it holds, keyed by offset.
func joinFixture(t *testing.T, records int) (*commitLog, map[int64]SerializedMessage) {
	t.Helper()
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 2 << 10,
	})
	t.Cleanup(cleanup)
	for i := range records {
		_, err := l.Append([]*Message{{
			Key:   []byte(fmt.Sprintf("key-%04d", i%16)),
			Value: []byte(fmt.Sprintf("value-%04d-padding-padding-padding", i)),
		}})
		require.NoError(t, err)
	}
	l.SetHighWatermark(l.NewestOffset())
	l.mu.RLock()
	n := len(l.segments)
	l.mu.RUnlock()
	require.Greater(t, n, 4, "fixture needs several sealed segments to join")
	return l, readAllMsgs(t, l)
}

func liveSegments(l *commitLog) []*segment {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return append([]*segment(nil), l.segments...)
}

// A join must carry every record of every input into one segment — that is the
// entire contract, and the thing a partial walk or an off-by-one run boundary
// would break silently.
//
// Driven through the whole pass rather than through joinOne, because the result
// is only correct once something OWNS it: the stage returns the list the pass
// publishes, and a result the pass does not publish is an open segment with a
// live index mapping that nothing will ever close.
func TestAJoinCarriesEveryRecordOfTheRun(t *testing.T) {
	l, before := joinFixture(t, 400)

	pre := liveSegments(l)
	runs := planJoins(pre, joinSpec)
	require.NotEmpty(t, runs, "the fixture must give the planner something to join")

	_, err := l.CleanWithSpec(joinSpec)
	require.NoError(t, err)

	after := readAllMsgs(t, l)
	require.Equal(t, len(before), len(after), "the join changed how many records the log holds")
	for off, want := range before {
		require.Equal(t, want, after[off], "record at offset %d did not survive the join", off)
	}

	published := liveSegments(l)
	require.Less(t, len(published), len(pre), "the pass joined nothing")
	// No offset is described by two published segments: the run's inputs all left
	// and its result arrived in the same swap.
	for i := 1; i < len(published); i++ {
		require.GreaterOrEqual(t, published[i].BaseOffset, published[i-1].NextOffset(),
			"segments %d and %d both describe offset %d after a join",
			published[i-1].BaseOffset, published[i].BaseOffset, published[i].BaseOffset)
	}

	for _, r := range runs {
		var (
			first  = pre[r.first]
			result *segment
		)
		for _, s := range published {
			if s.BaseOffset == first.BaseOffset {
				result = s
			}
		}
		require.NotNil(t, result,
			"the run's lowest base offset must survive the join; it is the segment's identity")
		require.Equal(t, pre[r.last].NextOffset(), result.NextOffset(),
			"the joined segment does not span its whole run")

		// Every input above the first is retired WITH a link. A segment marked as
		// having left and carrying no link is the retention case — reader, skip
		// me, those records are gone — and taking that path here would skip
		// records sitting in the result.
		for i := r.first + 1; i <= r.last; i++ {
			in := pre[i]
			in.RLock()
			left, next := in.left, in.replacement
			in.RUnlock()
			require.True(t, left, "input %d is still published as current", in.BaseOffset)
			require.Same(t, result, next,
				"input %d left with no link to the segment holding its records", in.BaseOffset)
			require.NotContains(t, published, in,
				"a joined-away segment is still in the published list, so LocalBytes "+
					"counts the result once per input")
			require.NoFileExists(t, filepath.Join(l.Path,
				fmt.Sprintf(fileFormat, in.BaseOffset, logFileSuffix)),
				"a joined-away segment's log duplicates bytes now in the result")
		}
	}

	strays, err := filepath.Glob(filepath.Join(l.Path, "*"+joinedSuffix))
	require.NoError(t, err)
	require.Empty(t, strays, "the join left its working copy behind")
}

// The same duty every other scan site in this package carries, and the loudest
// case of it: a join reads each input to its end and then DELETES it, so a walk
// that mistook a read failure for end-of-data would write a prefix and unlink
// the file holding the rest. The rewrite paths at least leave the damaged bytes
// under the source's name; this one collects them.
func TestAJoinRefusesAnInputItCannotReadToTheEnd(t *testing.T) {
	l, _ := joinFixture(t, 400)
	inputs := liveSegments(l)[0:3]

	// The MIDDLE input, so the walk gets a whole segment out before it fails — a
	// join that stopped at zero would be caught by any assertion at all.
	victim := inputs[1]
	path := filepath.Join(l.Path, fmt.Sprintf(fileFormat, victim.BaseOffset, logFileSuffix))
	st, err := os.Stat(path)
	require.NoError(t, err)
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	require.NoError(t, err)
	garbage := make([]byte, 256)
	for i := range garbage {
		garbage[i] = 0xA5
	}
	_, err = f.WriteAt(garbage, st.Size()/2)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	joined, err := joinOne(inputs, &blockWriter{}, newBlockCache())
	require.Nil(t, joined)
	require.ErrorIs(t, err, ErrSegmentUnreadable,
		"the join met unreadable bytes and called it done")

	// A refusal must leave the run exactly as it found it. The commit point is
	// the rename, and this failed before reaching it, so nothing was retired and
	// nothing was renamed over.
	for _, in := range inputs {
		in.RLock()
		left := in.left
		in.RUnlock()
		require.False(t, left, "a refused join retired input %d anyway", in.BaseOffset)
		require.FileExists(t, filepath.Join(l.Path,
			fmt.Sprintf(fileFormat, in.BaseOffset, logFileSuffix)),
			"a refused join deleted input %d", in.BaseOffset)
	}
	strays, err := filepath.Glob(filepath.Join(l.Path, "*"+joinedSuffix))
	require.NoError(t, err)
	require.Empty(t, strays,
		"the failed join left its working copy behind: it is open, mapped, and nothing names it")

	// The first input is untouched, so its records still read end to end. Only
	// that one: a reader walking further stops at the damage in the second, which
	// is correct behaviour and not what this is about.
	r, err := l.NewReader(From(inputs[0].BaseOffset), Uncommitted())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for want := inputs[0].BaseOffset; want < inputs[0].NextOffset(); want++ {
		_, got, _, _, err := r.ReadMessage(ctx, make([]byte, HeaderBufferLen))
		require.NoError(t, err, "a refused join disturbed an input it never reached")
		require.Equal(t, want, got)
	}
}

// A join costs a full read and write of every byte in the run, so it draws on
// the pass's rewrite budget like any other rewrite. A stage that ignored the
// budget would turn one clean tick on a 336-segment log into an unbounded
// rewrite of the whole log.
func TestAJoinSpendsTheRewriteBudget(t *testing.T) {
	l, before := joinFixture(t, 600)

	pre := liveSegments(l)
	spec := joinSpec
	spec.maxRewrites = 1
	runs := planJoins(pre, spec)
	require.Greater(t, len(runs), 1,
		"the fixture must offer more runs than the budget allows, or the bound "+
			"is not what stops the second one")

	_, err := l.CleanWithSpec(spec)
	require.NoError(t, err)

	// Exactly one run collapsed: the published list is shorter by that run's
	// inputs minus its single result, and no more.
	published := liveSegments(l)
	require.Equal(t, len(pre)-(runs[0].len()-1), len(published),
		"the join stage spent more than the budget allowed")

	after := readAllMsgs(t, l)
	require.Equal(t, len(before), len(after))
	for off, want := range before {
		require.Equal(t, want, after[off], "record at offset %d did not survive the join", off)
	}
}

// An offloaded segment has no local log to rename over, so the rename that
// commits a local join would install the result under a name the segment does
// not read from — and retire the inputs anyway. The tiered commit point is a
// manifest write, and until it exists the refusal is what keeps a caller from
// reaching this path.
//
// The store is attached to a real local segment rather than faked wholesale,
// because a fake would switch off the very field under test: isOffloaded asks
// whether `store` is set, and a test double standing in for the segment would
// answer for it.
func TestAJoinRefusesAnOffloadedInput(t *testing.T) {
	l, _ := joinFixture(t, 400)
	inputs := liveSegments(l)[0:3]

	victim := inputs[1]
	victim.Lock()
	victim.store, victim.tier = stubJoinStore{}, "cold"
	victim.Unlock()
	t.Cleanup(func() {
		victim.Lock()
		victim.store, victim.tier = nil, ""
		victim.Unlock()
	})

	joined, err := joinOne(inputs, &blockWriter{}, newBlockCache())
	require.Nil(t, joined)
	require.ErrorContains(t, err, "offloaded")
	for _, in := range inputs {
		in.RLock()
		left := in.left
		in.RUnlock()
		require.False(t, left, "a refused join retired input %d anyway", in.BaseOffset)
	}
	strays, err := filepath.Glob(filepath.Join(l.Path, "*"+joinedSuffix))
	require.NoError(t, err)
	require.Empty(t, strays, "the refused join left a working copy behind")
}

// The claim joinOne's commit point rests on: between the rename that installs a
// result and the unlink of its inputs, the inputs are segments whose offset
// range is entirely contained in the result — and that is a state the log
// already knows how to resolve, because an interrupted truncation leaves the
// same shape. If it did not, a crash in that window would leave a log serving
// some offsets twice.
//
// The window is reconstructed rather than raced for, so the test's meaning does
// not depend on the scheduler: the files the join deletes are put back after the
// pass returns, which is exactly what a process killed mid-window leaves behind.
func TestAJoinInterruptedBeforeItsInputsAreGoneResolvesOnOpen(t *testing.T) {
	l, before := joinFixture(t, 400)
	dir := l.Path

	pre := liveSegments(l)
	runs := planJoins(pre, joinSpec)
	require.NotEmpty(t, runs)

	// Copies of exactly what the pass is about to unlink.
	type saved struct {
		name string
		data []byte
	}
	var (
		stash   []saved
		retired []*segment
	)
	for _, r := range runs {
		for i := r.first + 1; i <= r.last; i++ {
			retired = append(retired, pre[i])
			for _, suffix := range []string{logFileSuffix, indexFileSuffix} {
				name := fmt.Sprintf(fileFormat, pre[i].BaseOffset, suffix)
				data, err := os.ReadFile(filepath.Join(dir, name))
				require.NoError(t, err)
				stash = append(stash, saved{name: name, data: data})
			}
		}
	}
	require.NotEmpty(t, stash)

	_, err := l.CleanWithSpec(joinSpec)
	require.NoError(t, err)
	require.NoError(t, l.Close())

	for _, s := range stash {
		require.NoError(t, os.WriteFile(filepath.Join(dir, s.name), s.data, 0o666))
	}

	reopened, cleanup := setupWithOptions(t, Options{Path: dir, MaxSegmentBytes: 2 << 10})
	defer cleanup()

	segs := liveSegments(reopened)
	for i := 1; i < len(segs); i++ {
		require.GreaterOrEqual(t, segs[i].BaseOffset, segs[i-1].NextOffset(),
			"segments %d and %d both describe offset %d after an interrupted join",
			segs[i-1].BaseOffset, segs[i].BaseOffset, segs[i].BaseOffset)
	}
	for _, in := range retired {
		require.NoFileExists(t, filepath.Join(dir,
			fmt.Sprintf(fileFormat, in.BaseOffset, logFileSuffix)),
			"the duplicate left by an interrupted join survived the open")
	}

	// And the records themselves: resolving the duplicate must not cost any.
	reopened.SetHighWatermark(reopened.NewestOffset())
	after := readAllMsgs(t, reopened)
	require.Equal(t, len(before), len(after),
		"resolving an interrupted join changed how many records the log holds")
	for off, want := range before {
		require.Equal(t, want, after[off],
			"record at offset %d was lost resolving an interrupted join", off)
	}
}
