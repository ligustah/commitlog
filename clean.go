package commitlog

// Cleaning is the log's SUPERVISION of the cleaners, not a cleaner itself. The
// policies live in compact_cleaner.go and delete_cleaner.go; what is here is the
// part that only the log can do: hold the lock, decide which segments a pass may
// see, install the result, and republish the tier manifest afterwards.
//
// CleanSpec sits here for the same reason. It is the log's public contract for a
// pass, and — see its own doc — it is parameterized by things this layer cannot
// verify, which is a property of the boundary rather than of either cleaner.

import (
	"log/slog"
	"time"
)

func (l *commitLog) cleanerLoop() {
	ticker := time.NewTicker(l.CleanerInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-l.closed:
			return
		}

		// Check to see if the active segment should be split.
		split, err := l.checkAndPerformSplitLocked()
		if err != nil {
			slog.Error(
				"Failed to split log",
				slog.String("path", l.Path),
				slog.String("error", err.Error()),
			)
			continue
		}

		// If we rolled a new segment, we don't need to run the cleaner since
		// it already ran.
		if split {
			continue
		}
		if l.DisableAutoClean {
			continue
		}

		if err := l.Clean(); err != nil {
			slog.Error(
				"Failed to clean log",
				slog.String("path", l.Path),
				slog.String("error", err.Error()),
			)
		}
	}
}

// CleanSpec parameterizes a transaction-aware clean. The commitlog provides
// the mechanism; a transactional layer (e.g. durable_streams) supplies the
// policy: which records' transactions aborted, where the decided prefix
// ends, and which per-message headers make a record transactional.
type CleanSpec struct {
	// Ceiling is the compaction bound: records at or above it are always
	// retained verbatim and never counted latest-per-key (they may be
	// undecided). <=0 falls back to the high watermark. Transactional
	// callers pass their LSO so open transactions can never shadow or be
	// compacted.
	Ceiling int64
	// StripBelow: records strictly below it are DECIDED, and nothing above
	// the log needs their per-record bookkeeping any more. Compaction removes
	// control records (AttrControl) below it, removes aborted data records,
	// and rewrites the survivors without StripHeaders. Offsets, timestamps,
	// leader epochs, keys, values and attribute bits survive the rewrite.
	StripBelow int64
	// StripHeaders are the per-message header keys removed below StripBelow.
	// Empty disables stripping, and with it marker removal — the two are only
	// safe together.
	//
	// The log attaches no meaning to these keys: they are whatever the caller
	// wrote and has since finished with, removed as bytes. A transactional
	// caller typically names the headers carrying its producer, epoch and
	// sequence, so that a decided record costs its readers nothing to skip —
	// but that is an example of the mechanism, not its definition, and a
	// caller with entirely different per-record bookkeeping uses it the same
	// way.
	StripHeaders []string
	// Aborted reports whether the data record at offset belongs to an
	// aborted transaction. Consulted only below Ceiling; must be safe for
	// concurrent use. Aborted records are removed and never counted
	// latest-per-key (an aborted record must not shadow a committed value).
	Aborted func(offset int64) bool
	// TombstoneGCBelow: a latest-per-key record carrying AttrTombstone at an
	// offset strictly below this, whose timestamp is older than
	// TombstoneRetention, is removed entirely — the key vanishes.
	TombstoneGCBelow int64
	// TombstoneRetention guards tombstone GC; zero disables it. Records with
	// timestamp 0 (pre-stamping logs) are never considered old enough.
	TombstoneRetention time.Duration
	// maxRewrites is an unexported deterministic rewrite cap for tests;
	// production callers bound passes by RewriteBudget.
	maxRewrites int
	// RewriteBudget bounds how long one pass may spend REWRITING segments
	// (digest skips stay free): once exceeded, remaining debt defers to the
	// next pass, so a pass always finishes inside a short-lived process's
	// kill window while reclamation scales to any inflow. The budget is
	// spent in drop-density order. 0 = unbounded. At least one rewrite
	// always proceeds.
	RewriteBudget time.Duration
	// TierRewriteBudget bounds, separately, how long one pass may spend
	// rewriting segments whose bytes live in a SegmentStore. Zero falls back to
	// RewriteBudget, so a caller that sets nothing sees no change.
	//
	// It is separate because the two rewrites cost wildly different things. A
	// local rewrite reads and writes local disk; a tiered one downloads the
	// object, rewrites, and uploads the result — orders of magnitude slower
	// against remote storage, and metered. Sharing one wall-clock budget lets a
	// single slow tiered rewrite consume the whole pass and starve local
	// compaction indefinitely, which is the case this exists to prevent: local
	// debt would grow while the pass spends its seconds on one remote object.
	//
	// Both budgets still guarantee at least one rewrite, so debt in either tier
	// always drains rather than deadlocking under a small budget.
	TierRewriteBudget time.Duration
	// skipTiered leaves segments whose bytes live in a SegmentStore entirely
	// alone: no rewrite, and no tier retention. Local segments compact and
	// retain as usual.
	//
	// Unexported deliberately. Whether this log may write to its store is a
	// property of the LOG, not of one pass — see Options.TierReadOnly and
	// SetTierReadOnly — and offering both invited a caller to set them to
	// disagree. CleanWithSpec derives this from the log's current mode.
	skipTiered bool
	// TierWriter is the identity stamped into any store objects this pass
	// writes. Set from the log's current value by CleanWithSpec; a caller
	// setting it directly overrides that for the pass.
	TierWriter string
	// RetentionFloor is the lowest offset RETENTION may not delete: a segment
	// is eligible for deletion only if every record in it lies strictly below
	// it. Nil — the zero value — means no floor, which is what every caller had
	// before this existed.
	//
	// It bounds the DELETE cleaner only. Compaction is already bounded by
	// Ceiling, and a caller's floor is at or above its ceiling by construction:
	// the records this protects are precisely the ones not yet decided.
	//
	// The reason it exists is that retention and settlement answer different
	// questions. A transactional caller's staged records sit in the log for as
	// long as its transaction is open, and nothing about a segment's age, bytes
	// or message count knows that — so a long transaction over a small
	// retention limit had its own staged records collected out from under it,
	// and its commit then referred to offsets that no longer existed.
	//
	// A POINTER rather than a sentinel, deliberately. Every obvious sentinel is
	// a real floor: 0 protects the whole log, which is exactly what a
	// transaction that began at offset 0 needs, and a caller writing
	// `RetentionFloor: floor` from a tracker that returns 0 for that case would
	// silently get "no protection" from an int64 field whose zero value had to
	// mean unset. Nil cannot be confused with an offset.
	RetentionFloor *int64
}

// Clean applies retention and compaction rules against the log, if applicable.
func (l *commitLog) Clean() error {
	spec := CleanSpec{}
	if l.Options.CompactTombstoneRetention > 0 {
		// Spec-less tombstone GC for non-transactional compacted logs,
		// bounded like the rest of the spec-less compaction.
		spec.TombstoneGCBelow = l.HighWatermark()
		spec.TombstoneRetention = l.Options.CompactTombstoneRetention
	}
	_, err := l.CleanWithSpec(spec)
	return err
}

// CleanWithSpec applies retention and a transaction-aware compaction pass.
// See the interface doc for the returned verified floor.
func (l *commitLog) CleanWithSpec(spec CleanSpec) (int64, error) {
	l.cleanMu.Lock()
	defer l.cleanMu.Unlock()
	l.mu.RLock()
	oldSegments := l.segments
	l.mu.RUnlock()
	if !l.tierWritable() {
		// A read-only tier is left entirely alone, whatever the spec asked for:
		// a rewrite and a tier retention delete are both writes to a store this
		// log does not own. Local compaction proceeds exactly as usual.
		spec.skipTiered = true
	}
	// Reclaim what EARLIER passes superseded, before this pass adds to the queue.
	// Deliberately first: the objects queued below are the ones this pass is
	// still installing, and the manifest that stops naming them is not written
	// until the end of it.
	l.drainReclaim()

	// Collected per pass, under cleanMu, so the entries queued belong to this
	// call.
	l.compactCleaner.superseded = nil
	cleaned, epochCache, verified, cleanErr := l.clean(spec, oldSegments)
	superseded := l.compactCleaner.superseded
	l.compactCleaner.superseded = nil
	if cleaned == nil {
		// Nothing was installed, but a rewrite may still have landed new objects
		// before the pass failed, and the segments it swapped are on them. The
		// superseded ones are this log's garbage either way.
		l.queueReclaim(superseded)
		return -1, cleanErr
	}
	l.mu.Lock()
	if l.segmentsClosed {
		// The log closed while this pass was rewriting, which it does outside
		// l.mu. Installing now would hand the log a set of freshly built
		// segments that closeSegments has already walked past, so nothing would
		// ever close them — the same leak a roll racing a close used to cause.
		// Close what this pass built instead; close() is idempotent, so the
		// segments carried over unchanged from the old set are unaffected.
		for _, segment := range cleaned {
			_ = segment.Close()
		}
		l.mu.Unlock()
		l.queueReclaim(superseded)
		return -1, ErrCommitLogClosed
	}
	newSegments := l.segments
	if len(newSegments) > len(oldSegments) {
		// New segments were added while cleaning. Rebase the new segments onto
		// the cleaned ones.
		rebase := newSegments[len(oldSegments):]
		cleaned = l.rebaseSegments(rebase, cleaned, epochCache)
	}
	l.segments = cleaned
	// Update the leader epoch offset cache to account for deleted segments. If
	// compaction ran, we need to regenerate the cache using the one returned
	// from compaction.
	var err error
	if epochCache != nil {
		err = l.leaderEpochCache.Replace(epochCache)
	} else {
		err = l.leaderEpochCache.ClearEarliest(l.segments[0].BaseOffset)
	}
	l.mu.Unlock()

	// Republish what the tier now holds. A pass can rewrite a segment onto new
	// objects and retention can drop others, so the manifest is stale the
	// moment either happens — and a stale manifest is worse than none, since it
	// names objects a reader would then fail to open.
	// Not when the pass skipped the tier: a read-only tier takes ZERO store
	// writes, and a manifest Put is a store write like any other. Nothing in
	// the tier changed, so the manifest is still accurate anyway.
	if !spec.skipTiered {
		manifestErr := l.writeTierManifest()
		// A manifest that did not land may still name what this pass superseded,
		// so reclamation holds off until one does. Recorded even when another
		// error is already being reported: whether the objects are safe to delete
		// does not depend on which error the caller sees.
		l.tierMu.Lock()
		l.tierManifestStale = manifestErr != nil
		l.tierMu.Unlock()
		if manifestErr != nil && err == nil && cleanErr == nil {
			err = manifestErr
		}
	}

	// Queued AFTER the manifest, so an entry is only ever considered for
	// deletion once a published manifest has stopped naming it.
	l.queueReclaim(superseded)

	// A partial retention failure (cleanErr) still swapped in the surviving
	// segments above; report it once the read path is consistent.
	if cleanErr != nil {
		return -1, cleanErr
	}
	return verified, err
}

// rebaseSegments adds the segments in from to the end of the slice of segments
// in to and adds any leader epoch offsets to the given leaderEpochCache.
func (l *commitLog) rebaseSegments(from, to []*segment, epochCache *leaderEpochCache) []*segment {
	to = append(to, from...)
	// Rebase any leader epoch offsets also. We don't check the error returned
	// here because Rebase can't return an error since epochCache is not
	// file-backed. The epoch cache is nil if compaction didn't run, in which
	// case skip this.
	if epochCache != nil {
		epochCache.Rebase(l.leaderEpochCache, from[0].BaseOffset) // nolint: errcheck
	}
	return to
}

// clean returns the cleaned segments, the pass's verified floor (see
// CleanWithSpec; -1 when compaction did not run) and, if compaction ran, a
// *leaderEpochCache maintaining the start offset for each new leader epoch.
func (l *commitLog) clean(spec CleanSpec, segments []*segment) ([]*segment, *leaderEpochCache, int64, error) {
	// Offloaded segments used to be held aside here as an immutable prefix,
	// because the rewriters build a local working segment and rewrite in place
	// and an offloaded segment has no local file to rewrite. That exclusion is
	// why whatever garbage a segment held when it offloaded was frozen there
	// permanently — a tombstone that offloaded before it could be collected
	// never took effect, and every value it shadowed was kept with it.
	//
	// They are no longer excluded. A rewrite of an offloaded segment becomes the
	// fresh store objects instead (see ReplaceOffloaded), and
	// retention is per tier, so their bytes count toward the tier's budget
	// rather than escaping every limit.
	cleaned, err := l.deleteCleaner.Clean(segments, spec.skipTiered, spec.RetentionFloor)
	if err != nil {
		// A partial retention failure still hands back the surviving
		// segments; propagate them so the caller swaps them in — the deleted
		// prefix must leave the read path even when the clean errs.
		return cleaned, nil, -1, err
	}
	verified := int64(-1)
	var epochCache *leaderEpochCache
	if l.Compact {
		if spec.Ceiling <= 0 {
			spec.Ceiling = l.HighWatermark()
		}
		compacted, cache, v, err := l.compactCleaner.CompactSpec(spec, cleaned)
		if err != nil {
			// Keep the delete stage's result: its removals are already on
			// disk regardless of the compaction failure.
			return cleaned, nil, -1, err
		}
		cleaned, epochCache, verified = compacted, cache, v
	} else if consolidated, err := consolidateSegments(cleaned, spec.maxRewrites, spec.RewriteBudget); err != nil {
		// Non-compacted logs still owe block-layout maintenance: their
		// per-append tiny blocks otherwise accumulate blockRef memory and
		// open-time header walks forever (an uncompacted, append-heavy log
		// was observed gathering 16k-block segments across a long-running
		// soak). The consolidation-only pass rewrites records
		// VERBATIM — content, offsets and epochs untouched — into
		// cleanBlockTarget-sized blocks, budgeted like compaction rewrites.
		return cleaned, nil, -1, err
	} else {
		cleaned = consolidated
	}
	return cleaned, epochCache, verified, nil
}
