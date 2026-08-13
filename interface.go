package commitlog

// CommitLog is the durable write-ahead log interface used to back each stream.
//
// # Single-writer contract for a tier
//
// A log with tiers configured assumes it is the ONLY process writing to each
// tier's store. It does not stamp its identity into object keys, does not fence
// its deletes against anyone else's, and cannot detect a second writer. This is
// a deliberate boundary: who owns a store is a question about cluster
// membership, and nothing the log can observe answers it.
//
// Ownership is per tier, so the contract is too: this log may own writes to one
// tier and read another that a different process owns. Express it with
// SetTierReadOnly — a process that does not own a tier runs read-only on that
// tier, and ownership moves by the previous owner going read-only before the
// next one comes out of it.
//
// # What actually goes wrong if two processes write at once
//
// Worth being precise about, because it is NOT silent data loss and a caller
// that believes otherwise will build machinery it does not need.
//
// Every upload goes to its own key (see newStoreKeys), so two writers cannot
// address the same object. There is no lost update to worry about: they produce
// two objects, and whichever manifest was published last says which one the tier
// holds. What that costs is storage, not data — the segment is uploaded twice,
// and the copy the manifest does not name is garbage until something reclaims it.
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
// every object neither the tier manifests nor this log's own segments name,
// which includes everything another live process has uploaded since this log
// last read those manifests. Feeding one to the other on a shared store deletes
// data that process is serving. See both methods for the detail; the short
// version is that "unreferenced by me" is not "unreferenced".
//
// A log DOES delete on its own, in one narrow case: an object its own rewrite
// superseded, which it uploaded, which it published, and which nothing it
// serves reads any more. That is the narrowest claim available — it is not
// "unreferenced by me", it is "written by me and since replaced by me" — and it
// only ever happens while this log owns tier writes. A reader in ANOTHER process
// that adopted the manifest before the rewrite is on the superseded object and
// must re-read the manifest to follow it, exactly as it always had to; what the
// log guarantees it is a published manifest naming the replacement, at least one
// clean pass before the old object goes.
//
// tla/MultiWriter.tla models these outcomes. It is evidence for the contract
// rather than a description of a defence, since the log has none — including
// the deterministic-key overwrite that keys used to permit and no longer do,
// kept because it is why they are shaped as they are.
//
// A log with no tiers is unaffected by any of this.
type CommitLog interface {
	// Delete closes the log and removes all data associated with it from the
	// filesystem.
	Delete() error

	// NewReader opens a Reader over the log. With no options it reads every
	// COMMITTED record from the oldest surviving one and returns io.EOF at the
	// end of the data. Configure it with From, Until, Follow, Uncommitted,
	// KeyPrefix, SkipSuperseded and IncludeControl.
	//
	// Two of its defaults are the safe direction rather than the convenient one:
	//
	//   - it TERMINATES rather than follows. Reaching the end of the data is an
	//     end condition unless Follow() says otherwise. The failure modes are
	//     not symmetric: a reader that unexpectedly ends returns io.EOF and its
	//     caller notices, while one that unexpectedly follows blocks forever.
	//     That is not hypothetical — it is how RecoverTail could hang before
	//     v0.18.0, and a consumer hit the same shape in its own abort scan.
	//   - it reads COMMITTED data only. Uncommitted() is a named option because
	//     an unlabelled bool at the call site told a reader nothing.
	//
	// The read is not a snapshot: the range is whatever is readable when each
	// read happens. Uncommitted() makes records above the high watermark
	// visible, so a caller that cares about the commit boundary bounds the read
	// itself with Until.
	//
	// Termination contract, both halves of which callers must handle:
	//   - a read ends when ReadMessage returns an error satisfying
	//     errors.Is(err, io.EOF). The EOF is WRAPPED, so compare with errors.Is
	//     and not ==.
	//   - construction returns ErrSegmentNotFound only when the log holds no
	//     segments at all. A start offset merely BELOW the oldest surviving
	//     record clamps up to it, so reading from 0 over a log that retention
	//     has since trimmed is fine and starts at the oldest record present.
	//
	// So the single case a caller must handle itself is the empty log. That is
	// deliberately an error rather than a reader that instantly ends: "there is
	// nothing here" and "the range you asked for held nothing" are different
	// answers, and collapsing them would let a sweep report success having
	// covered no data at all.
	//
	// KeyPrefix is refused together with Uncommitted unless the caller also
	// passes Until or IncludeControl — see NewReader's own documentation for
	// why that combination cannot produce a usable answer.
	NewReader(opts ...ReadOption) (*Reader, error)

	// Truncate removes all messages from the log starting at the given offset.
	//
	// Cutting at or below the high watermark is allowed, and LOWERS it to the
	// new end of the log. A follower told to roll back past what it had locally
	// committed is an ordinary outcome of an unclean leader election, so this
	// refuses nothing — but it will not leave the watermark naming records it
	// just removed, and it logs a warning when it moves it.
	//
	// So a caller truncating below the watermark has nothing to pair this with.
	// SetHighWatermark could not have done it anyway, being monotonic.
	Truncate(offset int64) error

	// RecoverTail walks the records above the high-watermark checkpoint and
	// keeps every structurally valid one, truncating only a torn suffix from a
	// power loss mid-write.
	//
	// It is what makes records written since the last checkpoint survive a
	// restart, so a caller that persists offsets elsewhere (a state WAL,
	// producer-id records) runs it at open. Discarding the whole suffix instead
	// would leave those markers overstating what the log holds.
	//
	// Visibility above the watermark stays gated by transaction markers: a
	// dangling open transaction is aborted by recovery exactly as before, so
	// recovering a record is not the same as committing it.
	RecoverTail() error

	// ActiveSegmentBase returns the base offset of the active (unsealed)
	// segment. Cleaning passes only rewrite segments that were sealed before
	// they started, so offsets at or above this value are untouched by any
	// concurrently running clean — which is what makes it usable as a floor by
	// a caller that must not have its records rewritten underneath it.
	ActiveSegmentBase() int64

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
	// minOffset (LastOffset < minOffset) into the log's primary tier,
	// freeing local log-file space while the data stays readable through the
	// store. The index stays local and the active segment is never offloaded.
	// It is a no-op (returns 0) when no tier is configured. Returns the
	// number of segments offloaded. Reads of an offloaded segment continue to
	// work transparently; a restart reopens them from the store.
	OffloadBefore(minOffset int64) (int, error)

	// SetTierReadOnly grants or withdraws this log's right to write to ONE
	// named tier. While a tier is read-only it will not offload into it, will
	// not rewrite a segment it holds, will not apply that tier's retention,
	// will not publish its manifest or descriptor, and refuses
	// DeleteStoreObjects naming it. Reads are unaffected, and a tiered read
	// stays transparent. A name that is not in Options.Tiers is an error, so a
	// caller that misnames the tier it is handing over finds out.
	//
	// Per tier, because ownership is: a node can own the tier it writes and not
	// the archive under it. One flag for the whole chain would make it choose
	// between offloading nothing and claiming a store it does not own.
	//
	// This is how ownership of a shared store is expressed. commitlog assumes
	// it is the ONLY writer to each of its stores — see the contract on this
	// interface — so a process that does not own a tier runs read-only on it,
	// and a handover is the previous owner going read-only before the next one
	// comes out of it.
	//
	// Going read-only takes effect for operations that START after it returns.
	// It does not cancel a write already in flight, because nothing can: once a
	// request is with the storage client it will land whether or not this log
	// still believes it owns anything. Sizing the handover so those writes have
	// drained is the caller's job, and getting it wrong is the failure the
	// contract describes.
	SetTierReadOnly(tier string, readOnly bool) error

	// DeleteStoreObjects removes objects from the tiers naming them, returning
	// those it removed.
	//
	// Every object's tier is resolved and checked for ownership BEFORE anything
	// is deleted, so a batch naming one tier this log does not own — or one not
	// in Options.Tiers — deletes nothing and returns no objects. Only a failure
	// from the store itself can leave a batch half applied, and that one returns
	// what did get through.
	//
	// An OPERATOR TOOL, not part of the normal path: a log reclaims what its own
	// rewrites supersede (see CleanWithSpec). What is left for this is garbage
	// this log did not create and cannot reason about — objects orphaned by a
	// crash, or left on a shared store by a process that is gone. Pair it with
	// UnreferencedObjects, and read the caveat there about what "unreferenced"
	// can and cannot mean on a store with more than one writer.
	//
	// Deleting is idempotent — a key that is already gone is not an error — so a
	// caller retrying after a partial failure does not have to tell the cases
	// apart.
	DeleteStoreObjects(objs []StoreObject) ([]StoreObject, error)

	// UnreferencedObjects lists the objects in every configured tier that
	// nothing this log can see names. It REPORTS; it never deletes.
	//
	// "Live" is the union of two sets, because each alone is wrong in a
	// different way. The tier manifests alone would miss an object this log is
	// reading but has not republished yet — a rewrite installs objects and then
	// publishes, and in between the segment is on a key no manifest names. This
	// log's own segments alone would miss everything another process offloaded
	// since this one opened. Each tier's listing is compared against the whole
	// live set, not that tier's slice of it.
	//
	// A manifest that exists but cannot be read is an ERROR, not an empty set:
	// "we do not know what is live" must never read as "nothing is live". The
	// manifests and the descriptor are themselves never reported — nothing
	// references what a store says ABOUT itself, so a rule built from
	// references alone would collect the two objects that make the tier
	// readable and identifiable.
	//
	// The caveat that matters on a SHARED store: this answers "unreferenced by
	// me", which is not "unreferenced". Every object a live peer has uploaded
	// since this log last read the manifests is in the list. Feeding it
	// straight to DeleteStoreObjects there deletes data that peer is serving.
	UnreferencedObjects() ([]StoreObject, error)

	// TierManifest returns what the STORES say the tiers hold, read from the
	// stores themselves rather than from this log's in-memory segments.
	//
	// Each tier describes itself: a manifest object, written after the segment
	// objects it names, records which object holds which segment and the offset
	// and time ranges each covers. So "what is in this tier" is answerable by
	// anyone holding the store, and a log opening over a store it has never seen
	// before picks the offloaded segments up automatically.
	//
	// Every configured tier is read and the results are merged into one slice
	// ordered by base offset. A segment two tiers both claim is a move that
	// committed and did not get to release: it is resolved from the claims
	// themselves, by TierObject.MovedFrom, so every process resolves it the same
	// way. A double claim the claims do not explain is an ERROR, not a pick —
	// one log's segments being in two stores is not a state to paper over.
	//
	// Being written last also makes it the tier's commit point: an object no
	// manifest names was never committed — a crash between an upload and its
	// manifest — so it is a recognisable orphan rather than something a reader
	// has to guess about.
	//
	// The manifest is one of two objects that describe the tier rather than
	// hold it; the other is the log's descriptor, which says what the log IS.
	// Neither is named by the manifest, and neither is ever garbage.
	//
	// Returns nil for a log with no tiers, and when no tier has anything
	// offloaded.
	TierManifest() ([]TierObject, error)

	// NewestOffset returns the offset of the last message in the log or -1 if
	// empty.
	NewestOffset() int64

	// OldestOffset returns the offset of the first message in the log or -1 if
	// empty.
	OldestOffset() int64

	// LocalBytes reports how many bytes of log data this log holds on LOCAL
	// disk — what copying it elsewhere would cost.
	//
	// Offloaded segments are excluded: their bytes are in a SegmentStore any
	// other process reads the same way, so a copy does not move them. Indexes
	// are excluded too, being derived rather than transferred.
	//
	// Arithmetic over the segment list, not a filesystem walk, so it is cheap
	// enough to ask on a timer.
	LocalBytes() int64

	// EarliestOffsetAfterTimestamp returns the earliest offset whose timestamp
	// is greater than or equal to the given timestamp.
	EarliestOffsetAfterTimestamp(timestamp int64) (int64, error)

	// LatestOffsetBeforeTimestamp returns the latest offset whose timestamp is
	// less than or equal to the given timestamp.
	//
	// It has no caller today — durable_streams' own log interface declares only
	// the After direction, for seek-by-time on a subscription — and that is
	// recorded here so a caller survey does not have to rediscover it and guess
	// what the silence means. It is the other half of a pair, not a stray: the
	// two answer different questions on a log with holes, and only this one can
	// answer "the last record that existed as of T", which is the shape a
	// point-in-time restore or an as-of read needs. Compaction and retention
	// both create the case where no record sits exactly at T, and the After
	// lookup lands past it.
	//
	// Kept for that reason rather than for a caller. See the v0.70.0 sweep in
	// CHANGELOG.md, which removed a neighbouring zero-caller feature and left
	// this one, and Options.ConcurrencyControl there for what the difference was.
	LatestOffsetBeforeTimestamp(timestamp int64) (int64, error)

	// SetHighWatermark sets the high watermark on the log. All messages up to
	// and including the high watermark are considered committed.
	//
	// MONOTONIC: a value below the current watermark is ignored, not applied.
	// Committed data does not become uncommitted just because a caller passed a
	// smaller number, and a late or reordered call must not walk the watermark
	// backwards under readers that have already been told what is committed.
	SetHighWatermark(hw int64)

	// OverrideHighWatermark sets the high watermark using the given value, even
	// when it is BELOW the current one. The deliberate exception to
	// SetHighWatermark's monotonicity.
	//
	// It exists to construct a log holding records ABOVE the commit boundary,
	// and nothing else in this API can produce that state: appending does not,
	// because a caller that commits as it appends ends with the watermark at the
	// newest record, and SetHighWatermark cannot walk it back down. That state is
	// what a fetch path serving only committed records has to be tested against.
	//
	// Not needed after Truncate, which lowers the watermark itself as part of
	// removing the records — there the records really are gone, and that is the
	// ordinary way the boundary moves backwards.
	//
	// This method was deleted in the v0.68.0 sweep and restored before release,
	// which is worth recording because the reasoning failed in an avoidable way.
	// Its previous doc justified it with "a caller that has some other reason",
	// and a doc that will not name its caller reads as one having no caller —
	// so the sweep looked for production call sites, found none, and read the
	// remaining test uses as incidental. One of them was not: it was the only
	// construction of the above-watermark state, doing a real lowering, and
	// substituting SetHighWatermark silently no-opped and turned a downstream
	// fetch test red. A capability whose only consumers are tests is still a
	// capability; what made it invisible was the doc declining to say so.
	OverrideHighWatermark(hw int64)

	// HighWatermark returns the high watermark for the log.
	HighWatermark() int64

	// NewLeaderEpoch indicates the log is entering a new leader epoch.
	NewLeaderEpoch(epoch uint64) error

	// LastOffsetForLeaderEpoch returns the start offset of the first leader
	// epoch larger than the named one, or the log end offset when no recorded
	// epoch is larger — the probing follower is level with this log or ahead of
	// it, and has nothing to discard.
	//
	// An Epoch that names nothing is refused with ErrUnknownLeaderEpoch rather
	// than answered. The caller of this truncates to the answer, so an offset
	// returned to a question the log cannot answer is a deletion instruction the
	// log invented; see Epoch.
	LastOffsetForLeaderEpoch(epoch Epoch) (int64, error)

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
	//
	// Unlike Append, the offsets come from the CALLER's framing rather than
	// from the log, so they are checked against the tail: the set must hold at
	// least one whole frame, its first offset must be strictly above the log's
	// newest, and its offsets must strictly ascend. Anything else returns
	// ErrMessageSetRefused and writes nothing.
	//
	// Strictly above, not exactly next. A compacted source has holes, and
	// ReadMessageSet serves the survivors, so a follower resuming from one
	// legitimately appends across a gap. What cannot be tolerated is a set that
	// starts at or below the tail: those offsets already name records, and
	// writing them again is how a replica ends up holding one record twice.
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
	// pass parameterized by the caller (see CleanSpec). RETENTION is
	// parameterized too: CleanSpec.RetentionFloor holds the age, bytes and
	// message limits off a prefix the caller is still using, which is the
	// only way a caller can stop them collecting records its own open
	// transactions staged. It returns the
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
	// A pass that rewrites a segment whose bytes live in a tier leaves the
	// objects it stopped referencing behind, and reclaims them ITSELF: the log
	// tracks the readers still on a superseded object and deletes it on a later
	// pass, once none remain and a published manifest has stopped naming it.
	// Nothing about that is the caller's to drive. Read-only is per tier and so
	// is the reclaim: a read-only tier's objects are left standing while the
	// rest are collected, since deleting is a store write like any other. A
	// reclaim interrupted by a crash leaves an orphan — costing storage, not
	// correctness, and reported by UnreferencedObjects.
	//
	// It returns an ERROR, without cleaning anything, if the spec carries a
	// Ceiling while the log's own cleaner is still running: the automatic pass
	// is spec-less, bounds itself at the high watermark, and would compact the
	// records the ceiling exists to protect. Set Options.DisableAutoClean.
	CleanWithSpec(spec CleanSpec) (verified int64, err error)

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

	// PutSidecar atomically writes a small named metadata file owned by the
	// log's client into the log directory (e.g. a recovery checkpoint). The
	// name must be a plain file name that does not collide with the log's own
	// files; all three calls REFUSE one that is not, with
	// ErrInvalidSidecarName, rather than acting on it.
	PutSidecar(name string, data []byte) error

	// GetSidecar reads a client sidecar file; the error satisfies
	// os.IsNotExist when the sidecar is absent.
	GetSidecar(name string) ([]byte, error)

	// RemoveSidecar deletes a client sidecar file; removing an absent
	// sidecar is a no-op.
	RemoveSidecar(name string) error

	// IdentityConflict returns the disagreement between Options.Identity and
	// the identity stored beside the log, found when this log was opened, or
	// nil if there was none.
	//
	// A conflict means the caller believes these bytes belong to one of its
	// entities and the log says otherwise — typically that a name was reused
	// and this copy predates the reuse. The log is fully open and usable; what
	// to do about it is the caller's, since only the caller knows what its
	// identities mean.
	//
	// The conflict is NOT written back, so it is still there on the next open.
	// That is deliberate: a signal consumed at open time is lost by a crash
	// immediately after, which moves the window instead of closing it.
	// AdoptOptions re-stamps the log and is how a caller says "these are mine
	// after all".
	IdentityConflict() *IdentityConflict

	// SegmentBlockCounts reports each segment's in-memory block-index size,
	// oldest first; a raw (uncompressed) segment reports 0. An observability
	// hook over the block-consolidation machinery, not a contract about how
	// many blocks a segment ought to have.
	SegmentBlockCounts() []int

	// Close closes each log segment file and stops the background goroutine
	// checkpointing the high watermark to disk.
	Close() error

	// IsClosed reports whether Close has run against this log.
	IsClosed() bool

	// IsDeleted reports whether Delete has run against this log. Distinct from
	// IsClosed because Delete also closes: a deleted log is closed, and a
	// closed one may still have its data.
	IsDeleted() bool
}
