package commitlog

// CommitLog is the durable write-ahead log interface used to back each stream.
//
// # Single-writer contract for a SegmentStore
//
// A log with a SegmentStore assumes it is the ONLY process writing to that
// store. It does not stamp its identity into object keys, does not fence its
// deletes against anyone else's, and cannot detect a second writer. This is a
// deliberate boundary: who owns a store is a question about cluster
// membership, and nothing the log can observe answers it.
//
// Express ownership with SetTierReadOnly — a process that does not own the tier
// runs read-only, and ownership moves by the previous owner going read-only
// before the next one comes out of it.
//
// # What actually goes wrong if two processes write at once
//
// Worth being precise about, because it is NOT silent data loss and a caller
// that believes otherwise will build machinery it does not need.
//
// Every upload goes to its own key (see newStoreKeys), so two writers cannot
// address the same object. There is no lost update to worry about: they produce
// two objects, each described by its own writer's markers. What that costs is
// storage, not data — the segment is uploaded twice, and the copy nobody's
// markers name is garbage until something reclaims it.
//
// This is also why an overlapping handover is survivable rather than
// catastrophic. A demoted leader acts on a lagging view of membership and may
// still upload, and a request already with the storage client will land
// whichever way its sender's beliefs have since gone. That produces a duplicate
// object. It does not destroy the current one.
//
// The hazard that IS destructive is deletion, and it does not arise from
// racing: it arrives by permission. DeleteStoreObjects is unfenced — it removes
// exactly the keys it is given. UnreferencedObjects on a SHARED store lists
// every object this log's markers do not name, which includes everything
// another live process uploaded. Feeding one to the other on a shared store
// deletes data that process is serving. See both methods for the detail; the
// short version is that "unreferenced by me" is not "unreferenced".
//
// tla/MultiWriter.tla models these outcomes. It is evidence for the contract
// rather than a description of a defence, since the log has none — including
// the deterministic-key overwrite that keys used to permit and no longer do,
// kept because it is why they are shaped as they are.
//
// A log with no SegmentStore is unaffected by any of this.
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

	// SetTierReadOnly grants or withdraws this log's right to write to its
	// SegmentStore. While read-only it will not offload, will not rewrite a
	// tiered segment, will not apply tier retention, and refuses
	// DeleteStoreObjects. Reads are unaffected, and a tiered read stays
	// transparent.
	//
	// This is how ownership of a shared store is expressed. commitlog assumes
	// it is the ONLY writer to its store — see the contract on this interface —
	// so a process that does not own the tier runs read-only, and a handover is
	// the previous owner going read-only before the next one comes out of it.
	//
	// Going read-only takes effect for operations that START after it returns.
	// It does not cancel a write already in flight, because nothing can: once a
	// request is with the storage client it will land whether or not this log
	// still believes it owns anything. Sizing the handover so those writes have
	// drained is the caller's job, and getting it wrong is the failure the
	// contract describes.
	SetTierReadOnly(readOnly bool)

	// DeleteStoreObjects removes objects from the SegmentStore, returning those
	// it removed. It is refused outright while the tier is read-only.
	//
	// This is the counterpart to the keys CleanWithSpec hands back: a rewrite
	// cannot delete the object it supersedes, because a reader that opened the
	// segment first is still reading it, so the caller deletes them once no such
	// reader can remain. Deleting is idempotent — a key that is already gone is
	// not an error — so a caller retrying after a partial failure does not have
	// to tell the cases apart.
	DeleteStoreObjects(keys []string) ([]string, error)

	// TierManifest returns what the STORE says its tier holds, read from the
	// store itself rather than from this log's local bookkeeping.
	//
	// The tier describes itself: a manifest object, written after the segment
	// objects it names, records which object holds which segment and the offset
	// and time ranges each covers. So "what is in this tier" is answerable by
	// anyone holding the store, and a log opening over a store it has no local
	// markers for picks the offloaded segments up automatically.
	//
	// Being written last also makes it the tier's commit point: an object no
	// manifest names was never committed — a crash between an upload and its
	// manifest — so it is a recognisable orphan rather than something a reader
	// has to guess about.
	//
	// Returns nil for a log with no store, and for a store with nothing
	// offloaded or one written before manifests existed.
	TierManifest() ([]TierObject, error)

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
	// stating because it looks like it should. Every upload allocates its own
	// key, so a store may hold several objects claiming one base offset — a
	// rewrite's predecessor, an upload orphaned by a crash — and NOTHING in
	// the store says which is current: not the keys, not the sizes, not the
	// timestamps.
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
	//   - an object a previous owner of the store uploaded and never removed.
	//
	//
	// Live is judged against the STORE'S MANIFEST as well as this log's own
	// segments, so an object another process offloaded is not called garbage
	// merely because this log has not adopted it. That is what makes the result
	// usable on a shared store, and it is why the manifest exists.
	//
	// The remaining caveat is timing rather than locality: an object uploaded by
	// a process that has not yet published its manifest is not yet named by
	// anything, and will be listed. Collect on a store with a writer mid-offload
	// and that upload is the one at risk. Anything already published is safe.
	//
	// It reports rather than deletes because only the caller knows whether a
	// concurrent writer is running at all; feed the result to
	// DeleteStoreObjects when none is.
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
	// objects of that segment, and the keys it stops referencing
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
