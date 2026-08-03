package commitlog

import (
	"bytes"
	"errors"
	"io"
	"os"
	"sort"
	"time"

	atomic_file "github.com/natefinch/atomic"
	pkgErrors "github.com/pkg/errors"
)

// findSegment returns the first segment whose next assignable offset is
// greater than the given offset. Returns nil and the index where the segment
// would be if there is no such segment.
func findSegment(segments []*segment, offset int64) (*segment, int) {
	n := len(segments)
	idx := sort.Search(n, func(i int) bool {
		return segments[i].NextOffset() > offset
	})
	// current(), not the slice entry: a compaction pass in flight has already
	// rewritten or removed the segments it has processed so far, and the log
	// does not publish the result until the pass ends. Skipping forward over the
	// removed ones is the same thing a reader does after retention — the offsets
	// are gone, and the next segment holds the next surviving records.
	//
	// The index still refers to the SLICE, so a caller using it to walk or
	// splice segments is unaffected; only the segment handed back is the live
	// one. The two can differ solely mid-pass, and anything holding cleanMu sees
	// them identical.
	for ; idx < n; idx++ {
		seg, ok := segments[idx].current()
		if !ok {
			continue // removed by the pass in flight
		}
		// Re-apply the search predicate to the RESOLVED segment. A rewrite drops
		// superseded records, so a replacement can end below where its source
		// did — and an offset in the gap belongs to the next segment, not to a
		// segment that no longer reaches it. Without this the reader resolved to
		// the replacement and failed with "entry not found".
		if seg.NextOffset() <= offset {
			continue
		}
		return seg, idx
	}
	return nil, n
}

// findSegmentContains returns the first segment whose next assignable offset
// is greater than the given offset and a bool indicating if the returned
// segment contains the offset, meaning the offset is between the segment's
// base offset and next assignable offset. Note that because the segment could
// be compacted, "contains" does not guarantee the offset is actually present,
// only that it's within the bounds.
func findSegmentContains(segments []*segment, offset int64) (*segment, bool) {
	seg, _ := findSegment(segments, offset)
	if seg == nil {
		return nil, false
	}
	return seg, seg.BaseOffset <= offset
}

// findSegmentIndexByTimestamp returns the index of the first segment whose
// base timestamp is greater than the given timestamp. Returns the index where
// the segment would be if there is no segment whose base timestamp is greater,
// i.e. the length of the slice.
func findSegmentIndexByTimestamp(segments []*segment, timestamp int64) (int, error) {
	var (
		n   = len(segments)
		err error
	)
	idx := sort.Search(n, func(i int) bool {
		// Read the first entry in the segment to determine the base timestamp.
		var entry entry
		switch e := segments[i].Index.ReadEntryAtLogOffset(&entry, 0); {
		case e == nil:
			return entry.Timestamp > timestamp
		case errors.Is(e, io.EOF):
			// The segment is EMPTY — it has no first entry, not a broken one.
			// That is the normal state of the active segment in the window just
			// after a roll, so it must not be reported as a failure.
			//
			// Sorting it after every timestamp is where an empty segment
			// belongs: it holds no record, so no record in it is at or before
			// anything, and the search lands on the last segment that does hold
			// one.
			//
			// This used to set err, which EarliestOffsetAfterTimestamp
			// special-cased and LatestOffsetBeforeTimestamp did not — so the
			// latter failed with a bare "EOF" whenever the search happened to
			// touch a just-rolled segment. Timing-dependent, which is why
			// concurrent readers made it show up.
			return true
		default:
			err = e
			return true
		}
	})
	return idx, err
}

// findSegmentByBaseOffset returns the first segment whose base offset is
// greater than or equal to the given offset. Returns nil if there is no such
// segment.
//
// Deliberately does NOT resolve through current(), unlike findSegment. This is
// the scan's end-of-segment jump, and a segment a pass has already rewritten or
// deleted answers it with ErrSegmentReplaced, which readOne recovers from by
// rebuilding the reader against the current log. Skipping ahead here would
// short-circuit that at the cost of a new failure the old shape did not have: a
// tail of gone segments would return nil, and the caller turns nil into a hard
// "no segment to consume" where the retry would have resolved it.
func findSegmentByBaseOffset(segments []*segment, offset int64) *segment {
	n := len(segments)
	idx := sort.Search(n, func(i int) bool {
		return segments[i].BaseOffset >= offset
	})
	if idx == n {
		return nil
	}
	return segments[idx]
}

func roundDown(total, factor int64) int64 {
	return factor * (total / factor)
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func removeAllWithRetry(path string) error {
	var err error
	for i := 0; i < 100; i++ {
		if err = os.RemoveAll(path); err == nil {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return err
}

// Bound for AtomicWriteFileWithRetry. Long enough to cover a handle that is on
// its way out (those clear in milliseconds), short enough that a genuinely
// conflicted write still fails promptly rather than stalling a checkpoint tick.
const (
	atomicWriteRetries    = 25
	atomicWriteRetryDelay = 20 * time.Millisecond
)

// AtomicWriteFileWithRetry writes a file atomically, retrying briefly. The retry
// exists for one platform reason, which is why it is in the name: on Windows the
// underlying ReplaceFile fails with "Access is denied" when any open handle to
// the destination was not opened with FILE_SHARE_DELETE. That handle need not be
// yours — a virus scanner or the search indexer opening the file after your
// previous write is enough, as is a process that has just exited and not yet
// been reaped.
//
// The condition clears in milliseconds, while a real conflict (a second live
// writer, a read-only file) never does, so the bound — 25 attempts, 20ms apart,
// so ~500ms worst case — keeps that case failing instead of hiding it behind a
// stall. On Unix rename is atomic and the first attempt always succeeds, so
// nothing is added there.
//
// The payload is buffered up front, and that part is load-bearing rather than
// incidental: a retry has to write the SAME bytes again, and the underlying
// WriteFile consumes the reader, so retrying with the original one would replace
// the file with nothing. Any reimplementation that streams instead of buffering
// is silently wrong on exactly the path this exists for.
//
// Exported for callers outside this package that write small config or
// checkpoint files next to a log and hit the same Windows failure.
func AtomicWriteFileWithRetry(path string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return pkgErrors.Wrap(err, "failed to buffer atomic write payload")
	}
	for i := 0; ; i++ {
		err = atomic_file.WriteFile(path, bytes.NewReader(data))
		if err == nil || i >= atomicWriteRetries {
			return err
		}
		time.Sleep(atomicWriteRetryDelay)
	}
}

// IsDeleted returns true if the commit log has been deleted.
