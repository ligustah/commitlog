package commitlog

// CommitLog is the durable write-ahead log interface used to back each stream.
type CommitLog interface {
	// Delete closes the log and removes all data associated with it from the
	// filesystem.
	Delete() error

	// NewReader creates a new Reader starting at the given offset. If
	// uncommitted is true, the Reader will read uncommitted messages from the
	// log. Otherwise, it will only return committed messages.
	NewReader(offset int64, uncommitted bool) (*Reader, error)

	// NewScanReader creates a Reader for sweeping a STATIC range: it reads
	// uncommitted records and returns io.EOF the moment it drains the readable
	// bytes, rather than parking until an append arrives or the high watermark
	// advances.
	//
	// Use it for any pass that must terminate at the tail — an abort scan, a
	// sequence rebuild, a consistency sweep. The readers from NewReader are
	// TAILING readers: reaching the end of the data is not an end condition for
	// them, so a pass that expects to finish there instead blocks forever if
	// nothing further is ever appended or committed. That is not hypothetical —
	// it is how RecoverTail could hang before v0.18.0, and a consumer hit the
	// same shape independently in its own abort scan.
	//
	// The range is whatever is readable when each read happens; the reader does
	// not snapshot. Records above the high watermark are visible, so a caller
	// that cares about the commit boundary must bound the scan itself.
	//
	// Termination contract, both halves of which callers must handle:
	//   - the scan ends when ReadMessage returns an error satisfying
	//     errors.Is(err, io.EOF). The EOF is WRAPPED, so compare with errors.Is
	//     and not ==.
	//   - construction returns ErrSegmentNotFound only when the log holds no
	//     segments at all. A start offset merely BELOW the oldest surviving
	//     record clamps up to it, exactly as NewReader does, so sweeping from 0
	//     over a log that retention has since trimmed is fine and starts at the
	//     oldest record still present.
	//
	// So the single case a caller must handle itself is the empty log. That is
	// deliberately an error rather than a reader that instantly ends: "there is
	// nothing here" and "the range you asked for held nothing" are different
	// answers, and collapsing them would let a sweep report success having
	// covered no data at all.
	NewScanReader(offset int64) (*Reader, error)

	// Truncate removes all messages from the log starting at the given offset.
	Truncate(offset int64) error

	// Sync makes the log durable through offset: once it returns, a reopened log
	// recovers every record up to and including offset. Pass an offset returned
	// by Append — typically the last of a commit — or NewestOffset to cover
	// everything written so far.
	//
	// This is the durability primitive; use it to make a commit durable.
	// CONCURRENT CALLERS SHARE ONE FSYNC: a caller whose offset a flush already
	// covers returns without issuing another, so N commits landing together cost
	// one fsync rather than N. That is what makes per-commit durability
	// affordable, and it is why callers should NOT batch above this — the log is
	// the only layer that knows what a given fsync actually covered.
	//
	// It flushes log bytes ONLY: not the index, and not the high-watermark
	// checkpoint. Both are states recovery already repairs — a stale checkpoint
	// is walked forward on open, and an index behind its log is rebuilt, since
	// the append path writes the log frame before the index entry. A segment's
	// index is flushed when it seals, which is what keeps that repair confined to
	// the active segment. Reach for SyncAll when the checkpoint itself must be
	// current.
	//
	// Safe to call concurrently with appends. A record appended while a flush is
	// in flight is not claimed by it and is covered by the next one.
	Sync(offset int64) error

	// SyncAll makes everything appended so far durable (as Sync) and ALSO
	// checkpoints the high watermark. Used before externally-visible filesystem
	// operations on the log's directory (e.g. an atomic stream promote via
	// rename) whose observers must never see the log roll back past this point.
	// For plain durability prefer Sync, which skips the checkpoint's extra fsync
	// and file rewrite.
	SyncAll() error

	// TruncateBefore removes all messages from the log with offset strictly less
	// than minOffset, freeing the disk space they occupied. After this call,
	// OldestOffset() >= minOffset. Sealed segments entirely before minOffset are
	// deleted; a boundary sealed segment is rewritten to keep only records at or
	// after minOffset. The active (most-recent) segment is never rewritten.
	TruncateBefore(minOffset int64) error

	// OffloadBefore moves the log bytes of every sealed segment entirely below
	// minOffset (LastOffset < minOffset) into the configured SegmentStore,
	// freeing local log-file space while the data stays readable through the
	// store. The index stays local and the active segment is never offloaded.
	// It is a no-op (returns 0) when no SegmentStore is configured. Returns the
	// number of segments offloaded. Reads of an offloaded segment continue to
	// work transparently; a restart reopens them from the store.
	OffloadBefore(minOffset int64) (int, error)

	// NewestOffset returns the offset of the last message in the log or -1 if
	// empty.
	NewestOffset() int64

	// OldestOffset returns the offset of the first message in the log or -1 if
	// empty.
	OldestOffset() int64

	// EarliestOffsetAfterTimestamp returns the earliest offset whose timestamp
	// is greater than or equal to the given timestamp.
	EarliestOffsetAfterTimestamp(timestamp int64) (int64, error)

	// LatestOffsetBeforeTimestamp returns the latest offset whose timestamp is
	// less than or equal to the given timestamp.
	LatestOffsetBeforeTimestamp(timestamp int64) (int64, error)

	// SetHighWatermark sets the high watermark on the log. All messages up to
	// and including the high watermark are considered committed.
	SetHighWatermark(hw int64)

	// OverrideHighWatermark sets the high watermark on the log using the given
	// value, even if the value is less than the current HW. This is used for
	// unit testing purposes.
	OverrideHighWatermark(hw int64)

	// HighWatermark returns the high watermark for the log.
	HighWatermark() int64

	// NewLeaderEpoch indicates the log is entering a new leader epoch.
	NewLeaderEpoch(epoch uint64) error

	// LastOffsetForLeaderEpoch returns the start offset of the first leader
	// epoch larger than the provided one or the log end offset if the current
	// epoch equals the provided one.
	LastOffsetForLeaderEpoch(epoch uint64) int64

	// LastLeaderEpoch returns the latest leader epoch for the log.
	LastLeaderEpoch() uint64

	// Append writes the given batch of messages to the log and returns their
	// corresponding offsets in the log. This will return ErrCommitLogReadonly
	// if the log is in readonly mode.
	Append(msg []*Message) ([]int64, error)

	// AppendMessageSet writes the given message set data to the log and
	// returns the corresponding offsets in the log. This can be called even if
	// the log is in readonly mode to allow for reconciliation, e.g. when
	// replicating from another log.
	AppendMessageSet(ms []byte) ([]int64, error)

	// Clean applies retention and compaction rules against the log, if
	// applicable.
	Clean() error

	// CleanWithSpec applies retention and a transaction-aware compaction
	// pass parameterized by the caller (see CleanSpec). It returns the
	// pass's VERIFIED FLOOR: the highest offset at or below which the log
	// now provably holds no transactional headers, no control markers and
	// no aborted records — the prefix an open-time LSO/seq/abort rebuild
	// may skip entirely. -1 = no such prefix (nothing verified, or the spec
	// carried no strip semantics). The floor covers only the consecutive
	// run of sealed segments this pass rewrote or digest-proved converged,
	// capped at StripBelow-1 — never the active segment or an age-protected
	// one, whose records keep their headers and abort markers even below
	// the LSO.
	CleanWithSpec(spec CleanSpec) (int64, error)

	// NotifyLEO registers and returns a channel which is closed when messages
	// past the given log end offset are added to the log. If the given offset
	// is no longer the log end offset, the channel is closed immediately.
	// Waiter is an opaque value that uniquely identifies the entity waiting
	// for data.
	NotifyLEO(waiter interface{}, leo int64) <-chan struct{}

	// SetReadonly marks the log as readonly. When in readonly mode, new
	// messages cannot be added to the log with Append and committed readers
	// will read up to the log end offset (LEO), if the HW allows so, and then
	// will receive an ErrCommitLogReadonly error. This will unblock committed
	// readers waiting for data if they are at the LEO. Readers will continue
	// to block if the HW is less than the LEO. This does not affect
	// uncommitted readers. Messages can still be written to the log with
	// AppendMessageSet for reconciliation purposes, e.g. when replicating from
	// another log.
	SetReadonly(readonly bool)

	// IsReadonly indicates if the log is in readonly mode.
	IsReadonly() bool

	// IsConcurrencyControlEnabled indicates if the log should check for concurrency before appending messages
	IsConcurrencyControlEnabled() bool

	// PutSidecar atomically writes a small named metadata file owned by the
	// log's client into the log directory (e.g. a recovery checkpoint). The
	// name must not collide with the log's own files.
	PutSidecar(name string, data []byte) error

	// GetSidecar reads a client sidecar file; the error satisfies
	// os.IsNotExist when the sidecar is absent.
	GetSidecar(name string) ([]byte, error)

	// RemoveSidecar deletes a client sidecar file; removing an absent
	// sidecar is a no-op.
	RemoveSidecar(name string) error

	// Close closes each log segment file and stops the background goroutine
	// checkpointing the high watermark to disk.
	Close() error
}
