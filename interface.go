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
	// recovers every record up to and including offset.
	//
	// Pass an offset RETURNED BY APPEND — typically the last of a commit. Do not
	// reach for NewestOffset out of habit: the tail advances with every append,
	// so it is never covered by a flush already in flight and every caller ends
	// up leading one of its own. Asking for what you actually need is what lets
	// the coalescing below work; asking for the tail quietly defeats it.
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
	//
	// An offset the log no longer reaches — retention moved the tail below it
	// after the caller appended there — returns nil rather than waiting to be
	// covered: those records are gone, so there is nothing left to make durable.
	Sync(offset int64) error

	// SyncAll is the wider fence: it fsyncs every segment's log AND index, then
	// checkpoints the high watermark. Used before externally-visible filesystem
	// operations on the log's directory (e.g. an atomic stream promote via
	// rename) whose observers must never see the log roll back past this point.
	//
	// It does strictly more than Sync, not merely Sync plus a checkpoint: Sync
	// leaves indexes to seal and to recovery, and SyncAll does not. For plain
	// durability prefer Sync, which skips both the index flushes and the
	// checkpoint's extra fsync and file rewrite, and which coalesces.
	SyncAll() error

	// TruncateBefore frees the disk space held by messages below minOffset.
	// Sealed segments entirely before it are deleted, and a boundary SEALED
	// segment is rewritten to keep only records at or after it.
	//
	// Reclamation is segment-granular and best-effort, NOT record-exact, because
	// the active segment is never rewritten. A log whose records all still live
	// in one active segment therefore frees nothing and leaves OldestOffset
	// unchanged — that is success, not failure. Budget disk against segment size
	// rather than against the floor, and do NOT gate anything on OldestOffset
	// reaching minOffset; it may never get there.
	//
	// The guarantee is directional: nothing at or above minOffset is ever
	// discarded, and a floor below the oldest surviving record is a no-op rather
	// than an error.
	//
	// Retention is unpoliced — the log does not check minOffset against any
	// consumer's progress, since only the caller knows what it has finished
	// with.
	TruncateBefore(minOffset int64) error

	// OffloadBefore moves the log bytes of every sealed segment entirely below
	// minOffset (LastOffset < minOffset) into the configured SegmentStore,
	// freeing local log-file space while the data stays readable through the
	// store. The index stays local and the active segment is never offloaded.
	// It is a no-op (returns 0) when no SegmentStore is configured. Returns the
	// number of segments offloaded. Reads of an offloaded segment continue to
	// work transparently; a restart reopens them from the store.
	OffloadBefore(minOffset int64) (int, error)

	// SetTierWriter updates the identity stamped into the object keys this log
	// writes to its SegmentStore, and fenced against when it deletes them. Call
	// it when ownership of tier writes moves — a new leader epoch, a new node.
	//
	// It exists because consensus cannot fence a write to an external store. A
	// node is leader at the moment it DECIDES, not at the moment its PUT lands;
	// its view of leadership lags, its in-flight requests cannot be observed or
	// cancelled, and no amount of waiting makes the claim it needs ("I will
	// still be the owner when this lands") a statement about the past. Stamping
	// the identity into the key sidesteps that entirely: two owners cannot
	// address the same object, so a stale one produces garbage to reclaim
	// rather than a silent overwrite of live data. The generation alone does
	// NOT achieve this — it is read from each writer's own local marker, so two
	// writers both compute the same next generation.
	//
	// The id must be letters, digits, '-' or '_' (max 64). A '.' or '/' does
	// not survive the key format and is refused rather than silently mangled.
	// Empty means unstamped, which is the right setting for the single-writer
	// case: keys keep their original form and no delete is fenced.
	//
	// Changing the id does NOT abandon existing objects: reads resolve keys
	// from the offload marker verbatim, never by recomputing them, so objects
	// written under a previous identity stay readable. They do, however, become
	// undeletable through the fence — see UnreferencedObjects for reclaiming
	// them.
	SetTierWriter(id string) error

	// DeleteStoreObjects removes objects from the SegmentStore, refusing any
	// key stamped by a writer other than this log's current one.
	//
	// This is the fenced counterpart to the keys CleanWithSpec hands back: a
	// rewrite cannot delete the object it supersedes (a reader that opened the
	// segment first is still reading it), so the caller deletes them once no
	// such reader can remain. That deletion is the dangerous half of the tier
	// protocol — a clobbered upload leaves the old bytes recoverable somewhere,
	// a delete leaves nothing — so it goes through the same fence as every
	// other removal rather than a raw store call.
	//
	// Deleting is idempotent: a key that is already gone is not an error.
	// Unstamped keys are NOT fenced, since they predate any identity and no
	// writer can be shown not to own them. Neither are keys THIS log
	// superseded: a superseded object is one this log's own marker named until
	// a rewrite replaced it, so its lineage is not in doubt whatever identity
	// wrote the bytes. Fencing those would refuse the caller the very keys
	// CleanWithSpec just handed it — which, after an identity change, is every
	// rewrite from then on.
	//
	// Anything else stamped by another identity is refused, and needs an
	// explicit AdoptTierWriters claim.
	DeleteStoreObjects(keys []string) ([]string, error)

	// AdoptTierWriters declares identities whose store objects this log may
	// also reclaim, for the objects the fence would otherwise strand: those
	// written under an identity this process no longer holds and did not
	// supersede in its current lifetime — a crash between a rewrite and the
	// deletion of what it replaced, or an offload whose marker was lost.
	//
	// This is deliberately an assertion the CALLER makes, not something the log
	// infers. Nothing observable at this layer establishes it: an identity that
	// looks idle may be a process about to come back from a pause with a PUT
	// already in flight. Whatever retires the previous epoch — a consensus term
	// that has moved on, a lease that has expired, an operator who confirmed
	// the node is gone — lives above the log, so the claim comes from there.
	//
	// The claim required is that the identity is no longer SERVING those
	// objects, which is stronger than "no longer writing" and is the condition
	// that actually matters. Where several processes share a store, each keeps
	// its offload markers locally: a process that lost ownership still holds
	// markers naming its objects and still reads through them, and nothing
	// tells it to stop. Adopting a demoted-but-live peer's identity therefore
	// lets this log delete objects that peer is actively reading — the failure
	// the delete fence exists to prevent, arrived at by permission instead of
	// by race.
	//
	// Adopting an identity that can still write is worse again: it reintroduces
	// the hazard the stamp removes, with the damage now a delete rather than an
	// overwrite, and a delete leaves nothing to recover.
	AdoptTierWriters(ids ...string) error

	// ExportTierState returns this log's tier bookkeeping — which store object
	// currently holds each offloaded segment, and everything needed to place
	// that segment without reading it.
	//
	// The bookkeeping is otherwise LOCAL to this process (one marker file per
	// offloaded segment), which is fine while a store has one writer and fatal
	// when ownership moves: the next owner holds no markers for anything its
	// predecessor uploaded, so it cannot read those objects through the log,
	// cannot avoid uploading a second copy of the same bytes, and can never
	// reclaim them.
	//
	// Recovering it from the store instead does not work, which is worth
	// stating because it looks like it should. Generations are per-writer, so
	// one base offset may have objects from two writers and NOTHING in the
	// store orders them — not the keys, not the sizes, not the timestamps.
	//
	// Replicate this through whatever gives the cluster a total order, and hand
	// it to the next owner via ImportTierState.
	ExportTierState() ([]TierObject, error)

	// ImportTierState installs tier bookkeeping the caller says is current,
	// returning how many segments changed. The caller's state is authoritative:
	// this log cannot tell which of two objects is current, and does not try.
	//
	// For each entry the log must already hold the segment at that base offset.
	// A segment whose bytes are local becomes offloaded, pointing at the object
	// instead of uploading a duplicate; one already offloaded is repointed, and
	// the objects it stops referencing join this log's lineage so they can be
	// reclaimed later. The offsets must match what the segment holds — dropping
	// local bytes for an object covering something else would swap a reader's
	// data underneath it.
	//
	// The whole batch is validated before any of it is applied, including that
	// the objects exist. A half-applied import would leave the log in a state
	// the caller never described and cannot name in order to correct.
	//
	// Naming the active segment is an error, as is naming a segment this log
	// does not have: extending the log's offset range with records it has never
	// held is not something an import can do safely.
	ImportTierState(objs []TierObject) (int, error)

	// UnreferencedObjects lists objects in the SegmentStore that none of this
	// log's offload markers point at — the garbage the tier protocol
	// deliberately produces. Every one of these is a leak by design:
	//
	//   - a rewrite that superseded an object whose key was never deleted,
	//   - an upload that succeeded before a crash lost the marker naming it,
	//   - an object written under a previous identity, which the delete fence
	//     now refuses to remove.
	//
	// Fencing without this would trade a corruption bug for an unbounded
	// storage bill, which is a better failure but still a failure.
	//
	// IMPORTANT: unreferenced means unreferenced BY THIS LOG, and that
	// distinction is load-bearing rather than cautionary. Offload markers are
	// LOCAL to each process, so where a store is shared, another process's
	// markers routinely name objects this log has never heard of — everything
	// uploaded before this one took ownership, for a start. Deleting those
	// would break a reader that is still serving them.
	//
	// Only the caller knows whether the store is shared and who else is live,
	// so this reports rather than deletes; feed the result to
	// DeleteStoreObjects once it is known to be safe.
	UnreferencedObjects() ([]string, error)

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

	// ReadMessageSet returns the log's own framing VERBATIM, starting at
	// offset — the read counterpart to AppendMessageSet, so a follower can
	// replicate bytes without reconstructing the framing itself. The frames
	// this returns are exactly what AppendMessageSet accepts.
	//
	// It returns WHOLE frames only, up to roughly maxBytes. A partial message
	// set is not something a follower can append, so a maxBytes smaller than
	// the first frame yields that frame rather than a truncation the caller
	// cannot use: starving a follower is worse than overshooting its budget
	// once.
	//
	// Records ABOVE the high watermark are included. Replication is how the
	// watermark advances in the first place, so withholding them would deadlock
	// it; a follower that cares about the commit boundary applies its own.
	//
	// A single call does not cross a segment boundary — it returns what the
	// segment holding offset can give, and the caller continues from the last
	// offset it appended. An offset below the oldest surviving record clamps up
	// to it, as the readers do, so a follower resuming from a position
	// retention has passed carries on from what remains rather than failing.
	// ErrSegmentNotFound if the log holds no segment at or after offset.
	ReadMessageSet(offset int64, maxBytes int) ([]byte, error)

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
	//
	// It also returns the SUPERSEDED STORE OBJECTS: when the pass rewrote a
	// segment whose bytes live in a SegmentStore, the rewrite became a new
	// generation of that segment's objects and the previous generation's keys
	// are returned here. They are deliberately NOT deleted:
	//   - a reader that opened the segment before the rewrite holds a backing
	//     over the old key and is entitled to finish; deleting underneath it
	//     would turn a rewrite into a read error, and only the caller knows
	//     when no such reader remains;
	//   - where replicas share a tier, writes to those objects belong to
	//     whichever node holds tier-write ownership, which this layer does not
	//     know about.
	// Deleting them is therefore the caller's call, and must be explicit — a
	// rewrite that empties a segment leaves objects with nothing to overwrite
	// them. Empty for a log with no SegmentStore.
	CleanWithSpec(spec CleanSpec) (verified int64, superseded []string, err error)

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
