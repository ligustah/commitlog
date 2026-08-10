package commitlog

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A NotifyLEO waiter parked on the active segment must be woken by the roll
// that seals it.
//
// NotifyLEO is a read-then-act across two INDEPENDENT loads of vActiveSegment:
// one to pick the segment to park on, one inside NewestOffset to decide whether
// parking is still right. A roll landing between them hands the second load the
// NEW segment, whose NextOffset equals the old LEO because a roll writes no
// records — so the LEO check agrees and the waiter parks on a segment that will
// never be appended to again.
//
// That is survivable, and the reason is worth a test rather than only a
// comment. seal() does two things under the segment's own lock: it sets sealed,
// and it closes every channel already registered. So the two orders are both
// covered — register first and seal wakes you, seal first and waitForData sees
// sealed and hands back a closed channel — and the segment lock is what makes
// it a choice between exactly those two. Take either half away and this parks
// forever with nothing left to wake it.
//
// The roll here is AGE-driven and driven by one tick, so nothing appends: an
// append notifies waiters itself, which would wake the channel for a reason
// that has nothing to do with the property under test.
func TestANotifyLEOWaiterWakesOnTheRollThatSealsItsSegment(t *testing.T) {
	l, err := New(Options{
		Name: "leo-roll", Path: tempDir(t),
		// Nowhere near full: the roll must be the age one, and the segment must
		// not be over maxBytes, which waitForData treats as a reason to close
		// immediately and would hide a lost wakeup.
		MaxSegmentBytes:      64 << 20,
		MaxSegmentAge:        time.Millisecond,
		CleanerInterval:      time.Hour, // the loop must not race the tick below
		HWCheckpointInterval: time.Hour,
	})
	require.NoError(t, err)
	defer l.Close() // nolint: errcheck

	for i := 0; i < 8; i++ {
		_, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte(fmt.Sprintf("v%d", i))}})
		require.NoError(t, err)
	}
	cl := l.(*commitLog)
	leo := cl.NewestOffset()

	ch := cl.NotifyLEO("waiter", leo)
	select {
	case <-ch:
		t.Fatal("the waiter did not park; this test proves nothing about waking it")
	default:
	}

	// Past MaxSegmentAge, so the tick is certain to roll.
	time.Sleep(5 * time.Millisecond)
	require.True(t, cl.activeSegment().CheckSplit(cl.MaxSegmentAge),
		"the fixture must present the tick with a segment that is due to roll")

	sealed := cl.activeSegment()
	cl.cleanerTick()
	require.NotSame(t, sealed, cl.activeSegment(), "the tick did not roll")

	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("a waiter parked on the segment the roll sealed was never woken; " +
			"nothing will ever append to that segment again")
	}

	// The other order, which the same two lines of seal() have to cover: a
	// waiter arriving AFTER the seal must be handed a channel that is already
	// closed, since there is now nothing left to wake it. Registered straight on
	// the sealed segment, because that is what NotifyLEO does when its first
	// load of vActiveSegment lost the race to the roll.
	late := sealed.WaitForLEO("late", leo, leo)
	select {
	case <-late:
	default:
		t.Fatal("a waiter arriving after the seal was parked on a sealed segment; " +
			"the wake it is now relying on has already happened")
	}
}
