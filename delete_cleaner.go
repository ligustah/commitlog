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
var deleteSegment = func(s *segment) error {
	return s.Delete()
}

// dropOldestPrefix deletes the oldest idx segments — CLAMPED to maxDrop, what
// the retention floor permits — and returns the surviving log: the untouched
// suffix on full success, or, if a deletion fails, the failed segment plus
// everything newer, alongside the error. Deleting in ascending order means a
// partial failure removes a pure prefix of the log: the surviving slice is
// always contiguous (no holes for readers to fall into), and every deleted
// segment is gone from the returned read path.
//
// The clamp is here rather than at each limit because there are four of them
// and they compute their prefix in different ways; one of them forgetting the
// floor would delete records a caller is still using, and the failure would
// surface far from the limit that caused it.
func dropOldestPrefix(segments []*segment, idx, maxDrop int) ([]*segment, error) {
	if idx > maxDrop {
		idx = maxDrop
	}
	drop, keep := segments[:idx], segments[idx:]
	for j, s := range drop {
		if err := deleteSegment(s); err != nil {
			return append(drop[j:], keep...), err
		}
	}
	return keep, nil
}

// deletablePrefix is how many of the OLDEST segments retention may delete without
// removing a record at or above the floor: all of them when there is none.
//
// A segment is eligible only if the whole of it lies below the floor, which is
// to say the NEXT segment starts at or below it — deletion happens at segment
// granularity, so a segment holding one protected record is protected entire.
// The last segment is never eligible, which costs nothing: every local limit
// already retains it as the active one.
func deletablePrefix(segments []*segment, floor *int64) int {
	if floor == nil {
		return len(segments)
	}
	n := 0
	for i := 1; i < len(segments); i++ {
		if segments[i].BaseOffset > *floor {
			break
		}
		n = i
	}
	return n
}

// deleteCleanerOptions contains configuration settings for the DeleteCleaner.
type deleteCleanerOptions struct {
	Retention struct {
		Bytes    int64
		Messages int64
		Age      time.Duration
		// Tier* bound the segments whose bytes live in a SegmentStore,
		// separately from the ones on local disk. Retention becomes PER TIER: a
		// segment over the local budget is not deleted if it also exists in a
		// store — it has simply left the tier the budget governs — and the
		// record is gone only when the last tier's limit is reached.
		//
		// Zero means a tier keeps everything. A log with no store has no
		// offloaded segments, so these never apply to it at all and only the
		// limits above govern.
		TierBytes    int64
		TierMessages int64
		TierAge      time.Duration
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
	return &deleteCleaner{deleteCleanerOptions: opts}
}

// Clean will enforce the log retention policy by deleting old segments.
// Deletion only occurs at the segment granularity.
// skipTiered leaves the tier untouched: no tier retention, because deleting a
// tier's copy is a write to storage that may be shared with other replicas.
func (c *deleteCleaner) Clean(segments []*segment, skipTiered bool, floor *int64) ([]*segment, error) {
	var err error
	if len(segments) == 0 || (c.noRetentionLimits() && c.noTierLimits()) {
		return segments, nil
	}

	// Split the tiers before applying anything. Offloaded segments are always
	// the oldest contiguous prefix, so this is a cut rather than a filter, and
	// the two halves keep their order.
	//
	// The local limits deliberately do NOT see the offloaded prefix. A segment
	// whose bytes are in a store is not occupying the disk those limits govern,
	// so counting it would delete records to reclaim space that was already
	// reclaimed — the budget it belongs to is the tier's.
	tiered, local := splitOffloadedPrefix(segments)
	if len(tiered) > 0 && !skipTiered && !c.noTierLimits() {
		// The tier's eligibility is computed against the FULL log, then capped
		// at the half: the last tiered segment's records run up to the first
		// local one's base, and a floor inside the local half leaves every
		// tiered segment eligible. Measuring the half alone would either lose
		// that boundary or protect an object nothing is using.
		tiered, err = c.cleanTier(tiered, min(deletablePrefix(segments, floor), len(tiered)))
		if err != nil {
			return joinTiers(tiered, local), errors.Wrap(err, "failed to apply tier retention limit")
		}
	}
	if c.noRetentionLimits() {
		return joinTiers(tiered, local), nil
	}
	local, err = c.cleanLocal(local, floor)
	return joinTiers(tiered, local), err
}

// cleanLocal applies the local-disk retention limits, which is what Clean did
// in full before retention became per-tier.
func (c *deleteCleaner) cleanLocal(segments []*segment, floor *int64) ([]*segment, error) {
	var err error

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

	// maxDrop is recomputed for each limit rather than carried as a remaining
	// budget: the slice shrinks as a limit deletes, so an index from before is
	// an index into a different log. Recomputing is a walk over a handful of
	// base offsets and cannot drift.

	// Limit by age first.
	if c.Retention.Age > 0 {
		segments, err = c.applyAgeLimit(segments, deletablePrefix(segments, floor))
		if err != nil {
			return segments, errors.Wrap(err, "failed to apply age retention limit")
		}
	}

	// Next limit by number of messages.
	if c.Retention.Messages > 0 {
		segments, err = c.applyMessagesLimit(segments, deletablePrefix(segments, floor))
		if err != nil {
			return segments, errors.Wrap(err, "failed to apply message retention limit")
		}
	}

	// Lastly limit by number of bytes.
	if c.Retention.Bytes > 0 {
		segments, err = c.applyBytesLimit(segments, deletablePrefix(segments, floor))
		if err != nil {
			return segments, errors.Wrap(err, "failed to apply bytes retention limit")
		}
	}

	return segments, nil
}

func (c *deleteCleaner) noRetentionLimits() bool {
	return c.Retention.Bytes == 0 && c.Retention.Messages == 0 && c.Retention.Age == 0
}

func (c *deleteCleaner) noTierLimits() bool {
	return c.Retention.TierBytes == 0 && c.Retention.TierMessages == 0 &&
		c.Retention.TierAge == 0
}

// splitOffloadedPrefix cuts segments into the offloaded prefix and the rest.
// Offloaded segments are always the oldest, contiguous prefix — OffloadBefore
// works oldest-first and never touches the active segment — so this is a cut at
// the first local segment, and both halves stay in offset order.
func splitOffloadedPrefix(segments []*segment) (tiered, local []*segment) {
	for i, s := range segments {
		s.RLock()
		off := s.isOffloaded()
		s.RUnlock()
		if !off {
			return segments[:i], segments[i:]
		}
	}
	return segments, nil
}

// joinTiers reassembles the two halves without aliasing either: appending onto
// the tiered slice would write into the caller's backing array, which still
// holds the segments the local half is about to be read from.
func joinTiers(tiered, local []*segment) []*segment {
	out := make([]*segment, 0, len(tiered)+len(local))
	out = append(out, tiered...)
	return append(out, local...)
}

// cleanTier applies the tier limits to the offloaded segments. It is separate
// from the local pass for one reason beyond the different budgets: there is no
// active segment in a tier, so every one of them is eligible. The local pass
// must always retain the last segment because it is the one being appended to;
// a tier has no such thing, and forcing it to keep one would mean the oldest
// object could never be reclaimed.
func (c *deleteCleaner) cleanTier(segments []*segment, maxTierDrop int) ([]*segment, error) {
	var err error
	if c.Retention.TierAge > 0 {
		ttl := computeTTL(c.Retention.TierAge)
		idx := len(segments)
		for i, seg := range segments {
			seg.RLock()
			live := seg.lastWriteTime >= ttl
			seg.RUnlock()
			if live {
				idx = i
				break
			}
		}
		before := len(segments)
		if segments, err = dropOldestPrefix(segments, idx, maxTierDrop); err != nil {
			return segments, err
		}
		// Spent, not recomputed: unlike the local half, the tier's allowance was
		// measured against the full log and cannot be re-derived from this slice
		// alone once its own prefix is gone.
		maxTierDrop -= before - len(segments)
	}
	if c.Retention.TierMessages > 0 {
		before := len(segments)
		segments, err = c.applyTierLimit(segments, c.Retention.TierMessages,
			(*segment).MessageCount, maxTierDrop)
		maxTierDrop -= before - len(segments)
		if err != nil {
			return segments, err
		}
	}
	if c.Retention.TierBytes > 0 {
		segments, err = c.applyTierLimit(segments, c.Retention.TierBytes,
			(*segment).Position, maxTierDrop)
	}
	return segments, err
}

// applyTierLimit keeps the newest segments whose total, by the given measure,
// fits the limit, and drops the older prefix.
func (c *deleteCleaner) applyTierLimit(segments []*segment, limit int64,
	measure func(*segment) int64, maxTierDrop int) ([]*segment, error) {

	total := int64(0)
	for i := len(segments) - 1; i >= 0; i-- {
		total += measure(segments[i])
		if total > limit {
			return dropOldestPrefix(segments, i+1, maxTierDrop)
		}
	}
	return segments, nil
}

func (c *deleteCleaner) applyMessagesLimit(segments []*segment, maxDrop int) ([]*segment, error) {
	// We must retain at least the active segment.
	if len(segments) <= 1 {
		return segments, nil
	}

	// We start at the most recent segment and work our way backwards until we
	// meet the retention size.
	totalMessages := segments[len(segments)-1].MessageCount()

	var i int
	for i = len(segments) - 2; i > -1; i-- {
		totalMessages += segments[i].MessageCount()
		if totalMessages > c.Retention.Messages {
			break
		}
	}
	return dropOldestPrefix(segments, i+1, maxDrop)
}

func (c *deleteCleaner) applyBytesLimit(segments []*segment, maxDrop int) ([]*segment, error) {
	// We must retain at least the active segment.
	if len(segments) <= 1 {
		return segments, nil
	}

	// We start at the most recent segment and work our way backwards until we
	// meet the retention size.
	totalBytes := segments[len(segments)-1].Position()

	var i int
	for i = len(segments) - 2; i > -1; i-- {
		totalBytes += segments[i].Position()
		if totalBytes > c.Retention.Bytes {
			break
		}
	}
	return dropOldestPrefix(segments, i+1, maxDrop)
}

func (c *deleteCleaner) applyAgeLimit(segments []*segment, maxDrop int) ([]*segment, error) {
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
		// LastWriteTime(), not the bare field: cleanTier's identical read a few
		// lines up takes the segment's read lock for it, and the two disagreeing
		// is how one of them ends up being the wrong one. Safe here only because
		// the active segment short-circuits first and a sealed one is not being
		// written — which is a property of the loop, not of the field, and the
		// next edit to either could quietly remove it.
		if i == len(segments)-1 || seg.LastWriteTime() >= ttl {
			idx = i
			break
		}
	}
	return dropOldestPrefix(segments, idx, maxDrop)
}
