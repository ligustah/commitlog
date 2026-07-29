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
	if idx == n {
		return nil, idx
	}
	return segments[idx], idx
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

// Bound for atomicWriteWithRetry. Long enough to cover a handle that is on its
// way out (those clear in milliseconds), short enough that a genuinely
// conflicted write still fails promptly rather than stalling a checkpoint tick.
const (
	atomicWriteRetries    = 25
	atomicWriteRetryDelay = 20 * time.Millisecond
)

// atomicWriteWithRetry writes a file atomically, retrying briefly. On Windows
// the underlying ReplaceFile can transiently fail with "Access is denied" when
// some other handle to the destination has not been released yet — a process
// that just exited, or a scanner that opened the file after the previous write.
// The condition clears in milliseconds, while a real conflict (a second live
// writer, a read-only file) never does, so the bound keeps that case failing
// instead of hiding it. On Unix rename is atomic and the first attempt always
// succeeds, so nothing is added there.
//
// The payload is buffered up front because a retry has to write the SAME bytes
// again: atomic_file.WriteFile consumes the reader, so retrying with the
// original one would replace the file with nothing.
func atomicWriteWithRetry(path string, r io.Reader) error {
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
