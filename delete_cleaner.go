package commitlog

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/pkg/errors"
)

// computeTTL calculates the age cutoff for messages when there is an age
// retention policy. This function exists for mocking purposes.
var computeTTL = func(age time.Duration) int64 {
	return time.Now().Add(-age).UnixNano()
}

// deleteSegment removes a segment's backing files. This function exists for
// mocking purposes (fault injection in partial-failure tests).
var deleteSegment = func(s *segment) error { return s.Delete() }

// dropOldestPrefix deletes the drop segments OLDEST FIRST and returns the
// surviving log: keep on full success, or — if a deletion fails — the failed
// segment plus everything newer, alongside the error. Deleting in ascending
// order means a partial failure removes a pure prefix of the log: the
// surviving slice is always contiguous (no holes for readers to fall into),
// and every deleted segment is gone from the returned read path.
func dropOldestPrefix(drop, keep []*segment) ([]*segment, error) {
	for j, s := range drop {
		if err := deleteSegment(s); err != nil {
			return append(drop[j:], keep...), err
		}
	}
	return keep, nil
}

// deleteCleanerOptions contains configuration settings for the DeleteCleaner.
type deleteCleanerOptions struct {
	Retention struct {
		Bytes    int64
		Messages int64
		Age      time.Duration
	}
	Name string
}

// deleteCleaner implements the delete cleanup policy which deletes old log
// segments based on the retention policy.
type deleteCleaner struct {
	deleteCleanerOptions
}

// newDeleteCleaner returns a new cleaner which enforces log retention
// policies by deleting segments.
func newDeleteCleaner(opts deleteCleanerOptions) *deleteCleaner {
	return &deleteCleaner{opts}
}

// Clean will enforce the log retention policy by deleting old segments.
// Deletion only occurs at the segment granularity.
func (c *deleteCleaner) Clean(segments []*segment) ([]*segment, error) {
	var err error
	if len(segments) == 0 || c.noRetentionLimits() {
		return segments, nil
	}

	slog.Debug(
		"Cleaning log based on retention policy",
		slog.String("name", c.Name),
		slog.String("policy", fmt.Sprintf("%+v", c.Retention)),
	)
	defer slog.Debug("Finished cleaning log", slog.String("name", c.Name))

	// A partial deletion failure still returns the surviving segments (the
	// apply functions delete oldest-first, so the survivors are a contiguous
	// suffix): the caller MUST swap them in even on error, or its read path
	// keeps referencing deleted files.

	// Limit by age first.
	if c.Retention.Age > 0 {
		segments, err = c.applyAgeLimit(segments)
		if err != nil {
			return segments, errors.Wrap(err, "failed to apply age retention limit")
		}
	}

	// Next limit by number of messages.
	if c.Retention.Messages > 0 {
		segments, err = c.applyMessagesLimit(segments)
		if err != nil {
			return segments, errors.Wrap(err, "failed to apply message retention limit")
		}
	}

	// Lastly limit by number of bytes.
	if c.Retention.Bytes > 0 {
		segments, err = c.applyBytesLimit(segments)
		if err != nil {
			return segments, errors.Wrap(err, "failed to apply bytes retention limit")
		}
	}

	return segments, nil
}

func (c *deleteCleaner) noRetentionLimits() bool {
	return c.Retention.Bytes == 0 && c.Retention.Messages == 0 && c.Retention.Age == 0
}

func (c *deleteCleaner) applyMessagesLimit(segments []*segment) ([]*segment, error) {
	// We must retain at least the active segment.
	if len(segments) <= 1 {
		return segments, nil
	}

	// We start at the most recent segment and work our way backwards until we
	// meet the retention size.
	var (
		lastSeg         = segments[len(segments)-1]
		cleanedSegments = []*segment{lastSeg}
		totalMessages   = lastSeg.MessageCount()
	)

	var i int
	for i = len(segments) - 2; i > -1; i-- {
		s := segments[i]
		totalMessages += s.MessageCount()
		if totalMessages > c.Retention.Messages {
			break
		}
		cleanedSegments = append([]*segment{s}, cleanedSegments...)
	}
	return dropOldestPrefix(segments[:i+1], cleanedSegments)
}

func (c *deleteCleaner) applyBytesLimit(segments []*segment) ([]*segment, error) {
	// We must retain at least the active segment.
	if len(segments) <= 1 {
		return segments, nil
	}

	// We start at the most recent segment and work our way backwards until we
	// meet the retention size.
	var (
		lastSeg         = segments[len(segments)-1]
		cleanedSegments = []*segment{lastSeg}
		totalBytes      = lastSeg.Position()
	)

	var i int
	for i = len(segments) - 2; i > -1; i-- {
		s := segments[i]
		totalBytes += s.Position()
		if totalBytes > c.Retention.Bytes {
			break
		}
		cleanedSegments = append([]*segment{s}, cleanedSegments...)
	}
	return dropOldestPrefix(segments[:i+1], cleanedSegments)
}

func (c *deleteCleaner) applyAgeLimit(segments []*segment) ([]*segment, error) {
	// We must retain at least the active segment.
	if len(segments) <= 1 {
		return segments, nil
	}

	var (
		ttl = computeTTL(c.Retention.Age)
		idx int
	)

	// Drop the prefix of segments whose last-written timestamp is less than
	// the TTL, always retaining the active (last) segment.
	for i, seg := range segments {
		if i == len(segments)-1 || seg.lastWriteTime >= ttl {
			idx = i
			break
		}
	}
	return dropOldestPrefix(segments[:idx], segments[idx:])
}
