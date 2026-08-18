package commitlog

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// A read starting BELOW the oldest surviving offset is served from the oldest
// survivor rather than refused. This is what lets a reader survive retention:
// the offsets it asked for are gone, and the next surviving records are the
// right answer.
//
// Pinned because a downstream caller asked whether it is a guarantee or an
// accident, and was carrying two defensive clamps against it being an accident.
// Nothing exercised it, so the honest answer was "behaviour, not promise".
//
// TestNewScanReaderClampsBelowOldest covers the same guarantee for the scan
// reader. This one is deliberately not a duplicate of it: that test uses From(0)
// on the uncommitted path and reads a single record, so it cannot see a
// committed-path divergence, cannot distinguish an explicit request from an
// unset one, and cannot see a survivor being skipped after the first.
//
// The mechanism is one branch, and both reader constructions carry it:
// findSegmentContains returns the first segment whose next offset exceeds the
// requested one, plus contains=false when that segment's base is ABOVE it. Both
// newReaderUncommitted and newReaderCommitted read that false as "start at
// position 0" — the beginning of the oldest survivor.
func TestReadBelowOldestServesFromOldest(t *testing.T) {
	// Every start offset here is below the post-trim oldest. 0 is the zero
	// value, so on its own it could not distinguish an explicit request from an
	// unset one; 1 and 2 are set values that no default could produce; -1 is
	// included because a consumer stack passes negative offsets through to this
	// library as a "from the beginning" sentinel of its own, and was told here
	// that it works incidentally. It does not — a negative offset is below the
	// oldest surviving record, which is exactly the case the contract covers,
	// and the committed path reaches it through a branch that names negative
	// offsets explicitly.
	for _, from := range []int64{-1, 0, 1, 2} {
		for _, committed := range []bool{false, true} {
			name := fmt.Sprintf("from=%d/committed=%v", from, committed)
			t.Run(name, func(t *testing.T) {
				l, cleanup := setupWithOptions(t, Options{
					Path:             tempDir(t),
					MaxSegmentBytes:  20,
					DisableAutoClean: true,
				})
				defer cleanup()
				defer l.Close()

				appendMsgs(t, l, 5)
				l.SetHighWatermark(4)
				require.NoError(t, l.TruncateBefore(3))

				oldest := l.OldestOffset()
				require.Greater(t, oldest, from,
					"the trim must leave oldest above the start offset or this proves nothing")
				newest := l.NewestOffset()

				opts := []ReadOption{From(from), Follow()}
				if !committed {
					opts = append(opts, Uncommitted())
				}
				r, err := l.NewReader(opts...)
				require.NoError(t, err, "a below-oldest start must not be refused")

				hdr := make([]byte, HeaderBufferLen)
				var got []int64
				for {
					_, off, _, _, err := r.ReadMessage(context.Background(), hdr)
					require.NoError(t, err)
					got = append(got, off)
					if off >= newest {
						break
					}
				}

				require.Equal(t, []int64{3, 4}, got,
					"the read is served from the oldest survivor and covers every survivor")
				require.Equal(t, oldest, got[0],
					"the first record returned is the oldest survivor, not the offset asked for")
			})
		}
	}
}
