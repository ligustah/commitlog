package commitlog

import (
	"context"
	"errors"
	"hash/crc32"
	"io"
	"sync"

	pkgErrors "github.com/pkg/errors"
)

// ErrCommitLogReadonly is returned when the end of a readonly CommitLog has
// been reached.
var ErrCommitLogReadonly = errors.New("end of readonly log")

type contextReader interface {
	Read(context.Context, []byte) (int, error)
	// segmentBounds reports the offset range [base, next) of the segment this
	// reader is currently positioned in, and whether it has one yet.
	//
	// It is read AFTER a record, never before. A frame never straddles a
	// segment boundary, so once a record has been read the reader is positioned
	// in the segment that record came from — and `next` reflects any append that
	// landed meanwhile. Capturing before the read gets both of those wrong: it
	// would attribute a record to the previous segment when the read crossed a
	// boundary, and it would reject a legitimately-new record at the live tail
	// whose offset had not been assigned when the bounds were taken.
	segmentBounds() (base, next int64, ok bool)
}

// segmentCursor is where a reader is: the segment it is positioned in, how far
// into it, and the buffer over it.
//
// Embedded by both readers, which is how segmentBounds comes to exist once. It
// was written out twice — byte for byte, over exactly these fields — and a pair
// of identical copies is the shape this package keeps paying for: reading either
// one confirms the other, so nothing looks wrong until a fix lands in one of
// them and the reader that needed it is the other.
//
// The mutex lives here rather than on the readers because this is the state it
// exists to protect: segmentBounds is the only method reachable while Read is
// in flight. It guards each embedder's own mutable fields too — committedReader
// takes it over hw, hwSeg and hwPos — which is why it is a named field and not
// an embedded sync.Mutex: promoting Lock/Unlock onto the readers would advertise
// a lock callers have no business taking.
//
// Deliberately not the readers' whole shared state. `cl` and `noWait` are in
// both as well, and noWait is the reason to stop here: it names a different
// thing in each (do not park for appends / do not park for the watermark), so
// one shared field would carry one doc comment for two behaviours.
type segmentCursor struct {
	mu  sync.Mutex
	seg *segment
	pos int64
	br  bufReader
}

// seekTo positions the cursor at pos in seg. All three fields move together —
// that is the entire rule, and it was spelled out inline at each of the places
// that seek, in more than one statement order. Two of them were the boundary
// advance, `seekTo(next, 0)`, one in each reader, and they were textually
// identical enough that a guard anchored on one matched the other and reported
// SKIP.
//
// No count of call sites here on purpose: a comment that says "the N places
// that do X" is a claim about the call graph with nothing checking it, and this
// package shipped a wrong one for a release. Ask the compiler — every seek goes
// through this function now, which is the property worth stating.
//
// Callers hold the cursor's mutex; this does not take it. Both readers seek from
// inside Read, which already holds it.
func (c *segmentCursor) seekTo(seg *segment, pos int64) {
	c.seg = seg
	c.pos = pos
	c.br.reset(seg, pos)
}

// refill re-anchors the buffered reader where the cursor already is, without
// moving it. Used on first use, and after a wait that may have put more bytes
// behind the current position — the read that follows is what decides whether
// there are any.
func (c *segmentCursor) refill() {
	c.br.reset(c.seg, c.pos)
}

// segmentBounds implements contextReader.
func (c *segmentCursor) segmentBounds() (int64, int64, bool) {
	c.mu.Lock()
	seg := c.seg
	c.mu.Unlock()
	if seg == nil {
		return 0, 0, false
	}
	return seg.BaseOffset, seg.NextOffset(), true
}

// Reader reads messages atomically from a CommitLog. Readers should not be
// used concurrently.
type Reader struct {
	ctxReader contextReader
	offset    int64
	log       *commitLog
	// spec is the whole of the caller's request. There were two more fields
	// here, `uncommitted` and `noWait`, copied out of it at construction and then
	// never read by anything — the readers that act on those answers
	// (uncommittedReader, committedReader) carry their own. Write-only state,
	// which is invisible to staticcheck because a write counts as a use, and
	// invisible to review because a field named after a real concept reads as
	// though something consults it.
	spec readSpec
	// prefix serves a KeyPrefix read, skipping past non-matching records over
	// sealed segments instead of reading them. Nil for an unfiltered read, and
	// exhausted (then nil) once the read reaches the live tail.
	prefix *prefixSource
}

// NewReader opens a Reader over the log. With no options it reads every
// committed record from the oldest surviving one and returns io.EOF at the end
// of the data; see From, Until, Follow, Uncommitted, KeyPrefix, SkipSuperseded
// and IncludeControl.
//
// Two defaults are the opposite of the constructors this replaces, on purpose.
// It TERMINATES rather than follows, because a reader that unexpectedly ends is
// noticed by its caller while one that unexpectedly follows blocks forever. And
// it reads COMMITTED data only, which was previously an unlabelled bool at the
// call site.
//
// One combination is refused: KeyPrefix with Uncommitted, and neither Until nor
// IncludeControl. Reading past the commit boundary yields records whose
// transactions are undecided, and the markers that say which committed are
// keyless — the filter drops them. The caller would hold records it cannot
// classify, silently. Bound the read at your commit boundary with Until, or take
// the markers with IncludeControl.
func (l *commitLog) NewReader(opts ...ReadOption) (*Reader, error) {
	spec, err := l.resolve(opts)
	if err != nil {
		return nil, err
	}
	r := &Reader{
		offset: spec.offset,
		log:    l,
		spec:   spec,
	}
	if spec.prefixSet {
		r.prefix = newPrefixSource(l, spec)
	}
	r.ctxReader, err = l.newSourceReader(spec)
	return r, err
}

// readerResolveAttempts bounds how many times building a reader re-resolves
// against a fresh segment snapshot. One compaction swap costs one retry, so this
// is generous; it exists so a log tearing itself down cannot spin here.
const readerResolveAttempts = 8

// newSourceReader builds the underlying sequential reader for a spec.
//
// Building one means two steps: find the segment holding the offset, then look
// the offset up in that segment's index. Compaction can replace the segment
// between them, and Replace CLOSES the old one — so the lookup fails with
// ErrSegmentClosed for an offset that is entirely valid and whose record is
// sitting in the replacement, already on disk.
//
// The scan path has always handled this: readOne re-resolves on
// ErrSegmentReplaced and reads on. Construction never learned to, and handed the
// raw error back as though the read were impossible — so an ordinary Read
// against a compacting log failed, at random, with "segment has been closed".
// Retrying here rather than at each call site is the point: every caller would
// otherwise need to know that a storage-level swap is not a read failure.
func (l *commitLog) newSourceReader(spec readSpec) (contextReader, error) {
	var err error
	for range readerResolveAttempts {
		var cr contextReader
		if spec.uncommitted {
			cr, err = l.newReaderUncommitted(spec.offset, !spec.follow)
		} else {
			cr, err = l.newReaderCommitted(spec.offset, !spec.follow)
		}
		// Each attempt takes its own segmentsSnapshot(), so a retry is
		// resolving against the post-swap log rather than repeating the same
		// lookup. A log that is closing or gone reports that instead: there is
		// no replacement coming, and spinning would turn a clean shutdown into a
		// hang.
		if err == nil || !segmentSwapped(err) || l.IsClosed() || l.IsDeleted() {
			return cr, err
		}
	}
	return nil, err
}

// segmentSwapped reports whether err is the storage layer saying the segment we
// resolved against was replaced underneath us. Both spellings count: the index
// answers a closed segment with ErrSegmentClosed and the log path answers a
// replaced one with ErrSegmentReplaced, and Replace produces BOTH states on the
// same segment — which one surfaces depends only on where the caller happened to
// touch it.
// errors.Is alone. This used to say the same thing twice — `pkgErrors.Cause(err)
// == X` OR `errors.Is(err, X)` — and the first half reaches nothing the second
// does not: pkg/errors' wrappers implement Unwrap, so errors.Is walks a `causer`
// chain as well as a `%w` one. Stating a predicate twice invites the two halves
// to be maintained apart, which is how a sentinel gets added to one of them.
func segmentSwapped(err error) bool {
	return errors.Is(err, ErrSegmentClosed) || errors.Is(err, ErrSegmentReplaced)
}

// newRecoveryReader returns an uncommitted reader that does NOT block waiting
// for future appends: it returns io.EOF as soon as it drains the readable
// bytes. RecoverTail scans a static tail (not a live writer), so blocking for
// appends that will never come is never correct there — this guarantees the
// scan always terminates even if the reconstructed LEO overshoots the log on
// disk.
func (l *commitLog) newRecoveryReader(offset int64) (*Reader, error) {
	return l.NewReader(From(offset), Uncommitted())
}

// ReadMessage reads a single message from the underlying CommitLog or blocks
// until one is available. It returns the SerializedMessage in addition to its
// offset, timestamp, and leader epoch. This may return uncommitted messages if
// the reader was created with the uncommitted flag set to true.
//
// ReadMessage should not be called concurrently, and headersBuf must have a
// LENGTH of at least HeaderBufferLen — capacity is not enough, since the header
// is read into the slice itself. A longer buffer is fine and reads the same:
// only the first HeaderBufferLen bytes are filled.
//
// The bundled-return form this used to ask for as a TODO exists, as
// ReadMessageMetadata — added beside this rather than replacing it, which is the
// answer to the question and the reason it is not folded in here. That path
// returns everything in one MessageMetadata, and pays for it: it does not
// CRC-validate the payload, and its bytes are borrowed from the caller's buffer
// rather than owned. This one checks the payload CRC and hands back memory the
// caller keeps. Merging them would force that trade on every caller, so the
// five return values stay.
func (r *Reader) ReadMessage(ctx context.Context, headersBuf []byte) (SerializedMessage, int64, int64, uint64, error) {
	for {
		msg, offset, timestamp, leaderEpoch, err := r.readOne(ctx, headersBuf)
		if err != nil {
			return nil, 0, 0, 0, err
		}
		if r.spec.untilSet && offset > r.spec.until {
			// Past the caller's bound: end as if the data had run out, so a
			// bounded read terminates identically whether the bound or the log
			// end came first.
			r.offset = offset
			return nil, 0, 0, 0, io.EOF
		}
		// The sealed portion is already filtered by the digest; this catches
		// the tail, which has no digest to filter with.
		if !r.spec.matchesPrefix(msg) {
			continue
		}
		return msg, offset, timestamp, leaderEpoch, nil
	}
}

// readOne returns the next record from whichever source is live: the planned,
// digest-driven one while sealed segments remain, then the sequential reader.
func (r *Reader) readOne(
	ctx context.Context, headersBuf []byte) (SerializedMessage, int64, int64, uint64, error) {

	if r.prefix != nil {
		rec, ok, err := r.prefix.pop()
		if err != nil {
			return nil, 0, 0, 0, err
		}
		if ok {
			r.offset = rec.offset + 1
			return rec.msg, rec.offset, rec.ts, rec.epoch, nil
		}
		// Sealed segments exhausted: hand over to the sequential reader,
		// positioned where planning stopped. From here the read is at the live
		// tail, where records arrive one at a time and there is nothing to
		// plan from anyway.
		if r.prefix.next > r.offset {
			r.offset = r.prefix.next
		}
		r.prefix = nil
		cr, rerr := r.log.newSourceReader(r.specAt(r.offset))
		if rerr != nil {
			return nil, 0, 0, 0, pkgErrors.Wrap(rerr, "failed to reposition reader at the tail")
		}
		r.ctxReader = cr
	}
RETRY:
	msg, offset, timestamp, leaderEpoch, err := readMessage(ctx, r.ctxReader, headersBuf)
	if err != nil {
		if r.log.IsDeleted() {
			// The log was deleted while we were trying to read.
			return nil, 0, 0, 0, ErrCommitLogDeleted
		} else if r.log.IsClosed() {
			// The log was closed while we were trying to read.
			return nil, 0, 0, 0, ErrCommitLogClosed
		} else if errors.Is(err, ErrCommitLogReadonly) && r.log.IsReadonly() {
			// The log was set to readonly while we were trying to read.
			return nil, 0, 0, 0, ErrCommitLogReadonly
		} else if errors.Is(err, ErrSegmentReplaced) {
			// ErrSegmentReplaced indicates we attempted to read from a log
			// segment that was replaced due to compaction, so reinitialize the
			// contextReader and try again to read from the new segment.
			//
			// errors.Is, not pkgErrors.Cause(err) ==. Cause walks a `causer`
			// chain and stops at a `%w` one, and this package writes
			// `fmt.Errorf("%w: ...")` in a dozen places. Nothing between the
			// segment and here does today — which is exactly what makes the
			// comparison work and makes it fragile: one %w added anywhere on this
			// path turns an ordinary compaction swap into a hard read failure for
			// a record sitting on disk in the replacement.
			if r.ctxReader, err = r.log.newSourceReader(r.specAt(r.offset)); err != nil {
				return nil, 0, 0, 0, pkgErrors.Wrap(err, "failed to reinitialize reader")
			}
			goto RETRY
		} else {
			return nil, 0, 0, 0, err
		}
	}
	// A record must carry an offset belonging to the segment it was found in;
	// anything else is a fabricated identity, and a caller that resumes from a
	// reported offset resumes from nowhere.
	//
	// This is NOT the damage check any more — the header carries its own CRC now
	// (headerCrcPos), verified in readFrameHeader before a single field is used,
	// so a corrupted offset fails there and never reaches here. It was written
	// when that CRC did not exist, and the reason it survived the CRC is the
	// residue: a header that is self-consistent still says nothing about WHERE it
	// was found. A stale index position, a partially installed Replace, a seek
	// into the neighbouring file — each hands back a frame that passes its own
	// checksum and belongs to another segment. The CRC cannot see that, because
	// nothing is damaged.
	//
	// Bounds are taken AFTER the read, deliberately: see segmentBounds.
	if base, next, ok := r.ctxReader.segmentBounds(); ok && (offset < base || offset >= next) {
		return nil, 0, 0, 0, pkgErrors.Wrapf(ErrCorruptRecord,
			"record claims offset %d, outside its segment's range [%d, %d)",
			offset, base, next)
	}
	r.offset = offset + 1
	return msg, offset, timestamp, leaderEpoch, err
}

// specAt is this reader's spec repositioned to offset, for rebuilding the
// underlying reader without losing follow/committed semantics.
func (r *Reader) specAt(offset int64) readSpec {
	s := r.spec
	s.offset = offset
	return s
}

// ReadMessageMetadata reads a single message and returns its metadata — offset,
// attributes, and headers. headersBuf must have a LENGTH of at least
// HeaderBufferLen; a longer one is fine and only its first HeaderBufferLen bytes
// are filled. The payloadBuf slice is reused across calls; callers should
// pass the returned slice back on the next call to avoid per-message
// allocations.
//
// This is intended for metadata-only scans such as LSO rebuild where only the
// Attributes byte and message headers (producer ID, epoch, sequence) are needed.
//
// TWO THINGS IT DOES NOT DO, both of which ReadMessage does:
//
//   - It does NOT CRC-validate the payload. A record corrupted on disk is
//     returned here as data, where ReadMessage refuses it. Reading a value
//     through this path means reading it unverified.
//   - It does NOT give you memory you own. Raw — and Key(), Value() and the
//     Headers values taken from it — point INTO payloadBuf, so the next call
//     overwrites them in place. Copy anything you keep past that call.
//
// The second one is quiet when it bites: a shorter following record overwrites
// only the HEAD of a retained value and leaves its tail alone, so what you hold
// is still the right length and still parses, with another record's bytes at the
// front. Nothing errors. Decode as you go, or copy.
func (r *Reader) ReadMessageMetadata(ctx context.Context, headersBuf []byte, payloadBuf []byte) (MessageMetadata, []byte, error) {
RETRY:
	meta, newBuf, err := readMessageMetadata(ctx, r.ctxReader, headersBuf, payloadBuf)
	if err != nil {
		if r.log.IsDeleted() {
			return MessageMetadata{}, newBuf, ErrCommitLogDeleted
		} else if r.log.IsClosed() {
			return MessageMetadata{}, newBuf, ErrCommitLogClosed
		} else if errors.Is(err, ErrCommitLogReadonly) && r.log.IsReadonly() {
			return MessageMetadata{}, newBuf, ErrCommitLogReadonly
			// errors.Is for the reason readOne's copy of this arm gives.
		} else if errors.Is(err, ErrSegmentReplaced) {
			if r.ctxReader, err = r.log.newSourceReader(r.specAt(r.offset)); err != nil {
				return MessageMetadata{}, newBuf, pkgErrors.Wrap(err, "failed to reinitialize reader")
			}
			payloadBuf = newBuf
			goto RETRY
		} else {
			return MessageMetadata{}, newBuf, err
		}
	}
	r.offset = meta.Offset + 1
	return meta, newBuf, nil
}

type uncommittedReader struct {
	segmentCursor
	cl *commitLog
	// noWait makes the reader return io.EOF the moment it drains the readable
	// bytes instead of parking for future appends. RecoverTail uses it to scan
	// a static tail so recovery can never hang if the reconstructed LEO
	// overshoots the log actually on disk (an index-ahead-of-log inconsistency).
	noWait bool
}

func (r *uncommittedReader) Read(ctx context.Context, p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var readSize int

	// Initialise buffered reader on first use.
	if r.br.seg == nil {
		r.refill()
	}

	for {
		readSize, err = r.br.Read(p[n:])
		n += readSize
		r.pos = r.br.pos
		if err != nil && err != io.EOF {
			break
		}
		if n == len(p) {
			break
		}
		if readSize != 0 && err == nil {
			continue
		}

		// This segment has no more bytes for us. Exactly two things can be true:
		// the log rolled and the rest is in the next segment, or nothing further
		// has been written yet. Ask, in that order, and park only when the answer
		// is neither.
		//
		// This used to be two arms with a `waiting` flag deciding between them —
		// take the next segment if there is one, else park; and, on a second EOF
		// after parking, take the next segment or park again until there is one.
		// Two spellings of one rule, with the flag existing only to say which had
		// already been tried. They had drifted apart in the way duplicated advances
		// always do: this one set r.pos explicitly, that one left it to the next
		// iteration's `r.pos = r.br.pos`. Both were right, which is why the pair
		// survived so long, and neither had a test until the roll pair in
		// reader_roll_test.go.
		//
		// The snapshot is re-taken here rather than once at the top. A reader
		// parked at the tail can hold its snapshot for as long as the writer is
		// idle, and a roll is precisely the event it must not miss.
		if nextSeg := findSegmentAfter(r.cl.segmentsSnapshot(), r.seg); nextSeg != nil {
			r.seekTo(nextSeg, 0)
			continue
		}
		if werr := r.waitForData(ctx, r.seg); werr != nil {
			err = werr
			break
		}
		// Woken, and deliberately without deciding why. Bytes may have landed in
		// this segment, or a roll may have sealed it — refilling and reading again
		// answers that, because a sealed segment reads EOF a second time and takes
		// the advance above on the next pass. Deciding here is what needed the
		// flag.
		r.refill()
	}

	return n, err
}

// waitForData parks until the segment has more bytes. It returns nil when there
// may be more to read, and otherwise the reason there is not — io.EOF for a
// genuine end, or the context's error when the CALLER gave up.
//
// Those two were one answer (a bool) and the caller turned false into io.EOF, so
// a cancellation arrived at the caller as end-of-data. See
// committedReader.waitForHW for why that is the wrong thing to tell someone
// tailing a log.
func (r *uncommittedReader) waitForData(ctx context.Context, seg *segment) error {
	if r.noWait {
		// Recovery scan: the readable bytes are drained and no more are coming.
		// Return end-of-data instead of parking for appends that never arrive.
		return io.EOF
	}
	wait := seg.WaitForData(r, r.pos)
	select {
	case <-r.cl.closed:
		seg.removeWaiter(r)
		// io.EOF here is NOT end-of-data — records past r.pos may well exist. It
		// survives only because Reader.ReadMessage turns any error into
		// ErrCommitLogClosed when IsClosed(), and IsClosed() reads this same
		// channel, so the conversion cannot lose the race. A second consumer of
		// contextReader that skipped that conversion would inherit a reader that
		// reports a closed log as a fully drained one.
		return io.EOF
	case <-ctx.Done():
		seg.removeWaiter(r)
		return ctx.Err()
	case <-wait:
		return nil
	}
}

// newReaderUncommitted returns a contextReader which reads data from the log
// starting at the given offset.
func (l *commitLog) newReaderUncommitted(offset int64, noWait bool) (contextReader, error) {
	seg, contains := findSegmentContains(l.segmentsSnapshot(), offset)
	if seg == nil {
		return nil, ErrSegmentNotFound
	}
	position := int64(0)
	if contains {
		e, err := seg.findEntry(offset)
		if err != nil {
			return nil, err
		}
		position = e.Position
	}
	return &uncommittedReader{
		segmentCursor: segmentCursor{
			seg: seg,
			pos: position,
			br:  bufReader{seg: seg, pos: position, bufStart: position},
		},
		cl:     l,
		noWait: noWait,
	}, nil
}

type committedReader struct {
	segmentCursor
	cl    *commitLog
	hwSeg *segment
	hwPos int64
	hw    int64
	// noWait ends the read at the high watermark instead of parking for it to
	// advance. This is what a non-Follow committed reader needs: "committed
	// data ran out" is an end condition for a bounded pass, not something to
	// wait through.
	noWait bool
}

func (r *committedReader) Read(ctx context.Context, p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	segments := r.cl.segmentsSnapshot()

	if r.seg == nil {
		// Fixed BEFORE the watermark moves: this reader has served everything up
		// to r.hw, so the record it owes next is r.hw+1 whatever syncHW finds.
		offset := r.hw + 1
		segments, err = r.syncHW(ctx)
		if err != nil {
			return 0, err
		}
		r.seg, _ = findSegment(segments, offset)
		if r.seg == nil {
			return 0, ErrSegmentNotFound
		}
		entry, err := r.seg.findEntry(offset)
		if err != nil {
			return 0, err
		}
		r.seekTo(r.seg, entry.Position)
	}

	return r.readLoop(ctx, p, segments)
}

func (r *committedReader) readLoop(
	ctx context.Context, p []byte, segments []*segment) (n int, err error) {

	var readSize int
	for {
		lim := int64(len(p[n:]))
		// A nil hwSeg is not "the watermark is elsewhere", it is "this reader
		// does not know where the watermark is" — and equality with r.seg
		// cannot tell those apart, so an unset one used to leave the read
		// unbounded and let a COMMITTED reader walk into uncommitted bytes.
		// Fall into the sync branch below instead, which establishes the bound
		// or parks until there is one.
		if r.hwSeg == nil || r.seg == r.hwSeg {
			lim = min(lim, r.hwPos-r.pos)
		}
		if lim <= 0 {
			// HW boundary reached — sync.
			segments, err = r.syncHW(ctx)
			if err != nil {
				break
			}
			continue
		}

		// For small reads within our buffered window, use br. For reads
		// that cross the HW boundary or are larger than the buffer, fall
		// through to direct ReadAt.
		if r.br.seg != nil && lim <= int64(bufReadSize) {
			buf := p[n:int(int64(n)+lim)]
			readSize, err = r.br.Read(buf)
			n += readSize
			r.pos = r.br.pos
		} else {
			readSize, err = r.seg.ReadAt(p[n:int(int64(n)+lim)], r.pos)
			n += readSize
			r.pos += int64(readSize)
			// Keep the buffered reader in sync: the next small read must
			// continue after the bytes consumed here, not from the buffer's
			// stale pre-ReadAt position.
			r.br.pos = r.pos
		}
		if err != nil && err != io.EOF {
			break
		}
		if n == len(p) {
			break
		}
		if readSize != 0 && err == nil {
			continue
		}

		// We hit the end of the segment, so jump to the next one.
		if err == io.EOF {
			nextSeg := findSegmentAfter(segments, r.seg)
			if nextSeg == nil {
				// Name the state. A bare "no segment to consume" cost a long
				// investigation and sent two people at the wrong theory — a
				// stale segment snapshot — when what the numbers actually said
				// was that the reader had no high watermark at all.
				var hwBase int64 = -1
				if r.hwSeg != nil {
					hwBase = r.hwSeg.BaseOffset
				}
				err = pkgErrors.Errorf(
					"no segment to consume after segment %d at position %d "+
						"(hw %d in segment %d at position %d, %d segments)",
					r.seg.BaseOffset, r.pos, r.hw, hwBase, r.hwPos,
					len(segments))
				break
			}
			r.seekTo(nextSeg, 0)
			continue
		}

		// We hit the HW, so sync the latest.
		segments, err = r.syncHW(ctx)
		if err != nil {
			break
		}
	}

	return n, err
}

// syncHW advances this reader to the log's current high watermark and re-locates
// it, returning the segment snapshot the caller should keep walking. It waits
// for the watermark to move unless the reader was told not to.
//
// Three copies of this used to sit inline, in Read and twice in readLoop, and
// they had drifted apart in two ways that both mattered.
//
// Only two honoured noWait, so a bounded read that reached the watermark through
// the third parked for an advance it had been built not to wait for.
//
// And two of them wrote `hwSeg, hwPos, err := getHWPos(...)`. All three names are
// new in that scope, so the err being tested was NOT readLoop's named return —
// the `break` under it left the loop with the outer err still nil, and the read
// returned (n, nil). That is not a survivable way to fail here: readMessage
// ignores n and parses headersBuf, which still holds the PREVIOUS record's header
// — valid, CRC-checked, describing a payload already served — so the reader asked
// for that payload, agreed with the watermark on the way back, and parked. A
// follower hanging forever on a healthy log, with the actual reason (most often
// ErrSegmentReplaced, the one error the reader knows how to retry) discarded a
// frame earlier.
func (r *committedReader) syncHW(ctx context.Context) ([]*segment, error) {
	hw := r.cl.HighWatermark()
	for hw == r.hw {
		if r.noWait {
			return nil, io.EOF
		}
		if err := r.waitForHW(ctx, hw); err != nil {
			return nil, err
		}
		hw = r.cl.HighWatermark()
	}
	r.hw = hw
	// Re-snapshotted AFTER the wait: the append that moved the watermark may have
	// rolled a segment, and a snapshot taken before it cannot hold the one the
	// watermark now lives in.
	segments := r.cl.segmentsSnapshot()
	hwSeg, hwPos, err := getHWPos(segments, r.hw)
	if err != nil {
		return nil, err
	}
	r.hwSeg, r.hwPos = hwSeg, hwPos
	return segments, nil
}

func (r *committedReader) waitForHW(ctx context.Context, hw int64) error {
	wait := r.cl.waitForHW(r, hw)
	select {
	case <-r.cl.closed:
		r.cl.removeHWWaiter(r)
		// See uncommittedReader.waitForData: end-of-data is the wrong statement
		// here too, and the same upstream conversion is what makes it safe.
		return io.EOF
	case <-ctx.Done():
		r.cl.removeHWWaiter(r)
		// The CALLER's context, not the log's state. Returning io.EOF here made a
		// cancellation indistinguishable from "the data ran out" — and io.EOF is
		// this package's documented end-of-read signal, so a caller reading with a
		// per-read deadline would treat a timeout as end-of-stream and stop
		// consuming, silently, at the tail. Whether more data is coming is not
		// something a cancelled context says anything about.
		return ctx.Err()
	case readonly := <-wait:
		if readonly {
			return ErrCommitLogReadonly
		}
		return nil
	}
}

// newReaderCommitted returns a contextReader which reads only committed data
// from the log starting at the given offset. With noWait it ends at the high
// watermark instead of parking for it to advance.
func (l *commitLog) newReaderCommitted(offset int64, noWait bool) (contextReader, error) {
	var (
		hw       = l.HighWatermark()
		segments = l.segmentsSnapshot()
	)

	// If offset exceeds HW, wait for the next message. This also covers the
	// case when the log is empty.
	//
	// hw == -1 is its own reason and not a special case of offset > hw. It says
	// nothing is committed, so there is nothing this reader may serve whatever
	// offset it was asked for — including a NEGATIVE one, which is how the
	// condition used to be escaped. A caller that reads OldestOffset() on an
	// empty log gets -1 and passes it here; by the time this runs, records may
	// have landed, so `l.OldestOffset() == -1` is false, and `-1 > -1` is false
	// as well. Both clauses miss, and the fall-through below then SKIPS
	// computing the high watermark position (it is guarded by hw != -1) while
	// still handing back a non-nil segment.
	//
	// A committed reader in that state has no bound: readLoop only clamps its
	// read when r.seg == r.hwSeg, and a nil hwSeg is equal to nothing. So it
	// read the whole segment regardless of what was committed and ran off the
	// end of it — surfacing as "no segment to consume", and able to serve
	// uncommitted records before it got there.
	if hw == -1 || offset > hw || l.OldestOffset() == -1 {
		// Unpositioned, and it says so here rather than through a var block's
		// zero values: no segment, no located watermark, and a byte position that
		// cannot be mistaken for one. Read resolves all three on first use.
		return &committedReader{
			segmentCursor: segmentCursor{
				seg: nil,
				pos: -1,
			},
			cl:     l,
			hwSeg:  nil,
			hwPos:  -1,
			hw:     hw,
			noWait: noWait,
		}, nil
	}

	// Unconditional: the branch above returns for every hw == -1, so the guard
	// this used to carry could not be false. It read as though an unset watermark
	// still reached here, which is exactly the state described above as the one
	// that must not.
	hwSeg, hwPos, err := getHWPos(segments, hw)
	if err != nil {
		return nil, err
	}

	position := int64(0)
	seg, contains := findSegmentContains(segments, offset)
	if contains {
		entry, err := seg.findEntry(offset)
		if err != nil {
			return nil, err
		}
		position = entry.Position
	}
	return &committedReader{
		segmentCursor: segmentCursor{
			seg: seg,
			pos: position,
			br:  bufReader{seg: seg, pos: position, bufStart: position},
		},
		cl:     l,
		hwSeg:  hwSeg,
		hwPos:  hwPos,
		hw:     hw,
		noWait: noWait,
	}, nil
}

// getHWPos returns the segment holding the high watermark and the byte position
// just past it.
//
// It hands back the SEGMENT rather than its index on purpose. findSegment
// redirects a mid-compaction source segment to its replacement, and every caller
// used to throw that away by re-indexing the raw slice — reinstating the closed
// segment the redirect exists to avoid.
func getHWPos(segments []*segment, hw int64) (*segment, int64, error) {
	hwSeg, _ := findSegment(segments, hw)
	if hwSeg == nil {
		return nil, 0, ErrSegmentNotFound
	}
	hwEntry, err := hwSeg.findEntry(hw)
	if err != nil {
		return nil, 0, err
	}
	return hwSeg, hwEntry.Position + int64(hwEntry.Size), nil
}

// maxPayloadChunk bounds how far a frame's declared size is TRUSTED before any
// of it has been read.
//
// The size field is not covered by any checksum — the CRC lives inside the
// payload it describes — so a torn or damaged frame can declare any length up to
// 4GiB, and allocating that up front hands an out-of-memory kill to whoever
// embedded this log. Found by FuzzTornLogServesOnlyAPrefix, which truncates a
// segment and leaves the reader parsing a size out of whatever bytes follow: the
// fuzzing worker died with "terminated unexpectedly" rather than any assertion.
//
// So the payload is read in chunks and the buffer grows only as bytes actually
// ARRIVE. A frame claiming 4GiB of a file that holds a hundred bytes now costs
// one chunk and an error, instead of the process.
const maxPayloadChunk = 1 << 20

// readPayload fills size bytes from reader, growing into reuse when it can. It
// never allocates more than one chunk beyond what has already been read, so a
// bogus size costs a chunk rather than the address space.
func readPayload(ctx context.Context, reader contextReader, size int, reuse []byte) ([]byte, error) {
	buf := reuse[:0]
	for len(buf) < size {
		want := size - len(buf)
		if want > maxPayloadChunk {
			want = maxPayloadChunk
		}
		have := len(buf)
		if cap(buf) < have+want {
			grown := make([]byte, have, have+want)
			copy(grown, buf)
			buf = grown
		}
		buf = buf[:have+want]
		if _, err := reader.Read(ctx, buf[have:]); err != nil {
			return reuse[:0], err
		}
	}
	return buf, nil
}

// frameHeader is what a record's frame says about itself before its payload is
// read: which record it is, when it was written, under which leader, and how
// many bytes follow.
type frameHeader struct {
	offset      int64
	timestamp   int64
	leaderEpoch uint64
	size        uint32
}

// readFrameHeader reads one frame header into the caller's buffer, verifies it,
// and returns the fields it carries.
//
// The two readers below — the full one and the metadata-only one — begin
// identically, and this is that beginning. They had it written out twice, which
// is the arrangement that produced the drift this replaces: the same seven-line
// comment appeared above both copies verbatim, and the error strings under them
// had already diverged into "failed to ready message payload" on one side and
// "failed to read message payload" on the other. Two of the three shared steps
// were already cross-referenced with "See readMessage" rather than copied, so
// the file was of two minds about it; this settles it the same way.
//
// The caller supplies the buffer and gets its error shape back to wrap, because
// that is all the two ever genuinely disagreed about.
func readFrameHeader(ctx context.Context, reader contextReader, headersBuf []byte) (frameHeader, error) {
	// A buffer too small to hold a frame header cannot be read into, and must not
	// be indexed past. Reported by durable_streams: 24 of their call sites still
	// allocated 28 bytes, and on v0.41.0 every one PANICKED inside
	// storedHeaderCrc — an index past the end of the caller's slice.
	//
	// That is a caller mistake and still not something to crash their process
	// over, which is the whole point of ErrCorruptRecord replacing a panic
	// earlier. It also cannot be left to the Read below: Read fills whatever it
	// is given, so a short buffer silently consumes a partial header and
	// desynchronises the stream, and the failure then surfaces somewhere else
	// entirely.
	//
	// The error names HeaderBufferLen rather than a number, so the fix is
	// copy-pasteable and stays correct if the header changes again.
	if len(headersBuf) < msgSetHeaderLen {
		return frameHeader{}, pkgErrors.Errorf(
			"commitlog: headersBuf is %d bytes, need at least HeaderBufferLen (%d)",
			len(headersBuf), msgSetHeaderLen)
	}
	// Exactly the header, never the whole slice. The doc asks for a buffer of AT
	// LEAST HeaderBufferLen and the readers below fill whatever they are handed,
	// so reading headersBuf entire consumed the front of the PAYLOAD on any
	// buffer bigger than a header, and the next frame then began mid-record.
	//
	// The symptom of that is the log calling itself corrupt: the following header
	// fails its CRC on data that is perfectly intact. Silent, and reached by a
	// caller doing exactly what the doc invited — the mirror of the too-SMALL
	// buffer durable_streams reported, which at least panicked where you could
	// see it.
	hdr := headersBuf[:msgSetHeaderLen]
	if _, err := reader.Read(ctx, hdr); err != nil {
		return frameHeader{}, pkgErrors.Wrap(err, "failed to read message headers")
	}
	// Verify the header before trusting anything in it. offset, timestamp, leader
	// epoch and size are a record's IDENTITY, and until the header carried its own
	// checksum a damaged one was reported as fact — see headerCrcPos. The payload
	// CRC cannot help: it covers the value, and a swapped offset leaves the value
	// perfectly intact.
	//
	// Checked before `size` is used, because size is one of the fields being
	// verified: trusting it first is how a corrupt length becomes a bad
	// allocation.
	if want, got := storedHeaderCrc(hdr), headerCrc(hdr); want != got {
		return frameHeader{}, pkgErrors.Wrapf(ErrCorruptRecord,
			"frame header failed CRC: expected 0x%08x, got 0x%08x", want, got)
	}
	return frameHeader{
		offset:      int64(encoding.Uint64(hdr[offsetPos:])),
		timestamp:   int64(encoding.Uint64(hdr[timestampPos:])),
		leaderEpoch: encoding.Uint64(hdr[leaderEpochPos:]),
		size:        encoding.Uint32(hdr[sizePos:]),
	}, nil
}

// readMessage reads a single message from the reader or blocks until one is
// available. It returns the Message in addition to its offset, timestamp, and
// leader epoch. This may return uncommitted messages if the reader was created
// with the uncommitted flag set to true.
func readMessage(ctx context.Context, reader contextReader, headersBuf []byte) (SerializedMessage, int64, int64, uint64, error) {
	fh, err := readFrameHeader(ctx, reader, headersBuf)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	offset, timestamp, leaderEpoch := fh.offset, fh.timestamp, fh.leaderEpoch
	buf, err := readPayload(ctx, reader, int(fh.size), nil)
	if err != nil {
		return nil, 0, 0, 0, pkgErrors.Wrap(err, "failed to read message payload")
	}
	m := SerializedMessage(buf)
	// The frame's size field is not covered by any checksum — the CRC lives
	// INSIDE the payload it describes — so a torn or damaged frame can claim a
	// length too short to hold that checksum. Reading it would index past the
	// end: Crc() takes m[0:4], and on a size of 0 that panicked out of the
	// caller's process, which is exactly what a log embedded in someone else's
	// binary must not do.
	//
	// Anything this short cannot be a record. encode never emits fewer than a
	// checksum, a magic byte, an attribute byte and two length prefixes, so
	// refusing here rejects only frames that were already impossible, and every
	// longer malformation is left for the CRC below to catch.
	//
	// NOW SHADOWED, and kept as defence in depth. Since the frame header carries
	// its own checksum, a damaged `size` fails that check before this line is
	// reached, so the only way here is a header that verifies while declaring a
	// size encode cannot produce — which nothing writes. guardcheck used to cover
	// this via the torn-log target and reported it uncovered the moment the header
	// CRC landed; it is not untested by oversight, it is unreachable. See the roll
	// CAS in split() for the same situation and the same reasoning.
	if len(m) < 4 {
		return nil, 0, 0, 0, pkgErrors.Wrapf(ErrCorruptRecord,
			"record at offset %d: frame claims %d bytes, too short to hold a checksum", offset, len(m))
	}
	// Check the CRC on the message. Returned, not panicked: see ErrCorruptRecord
	// for why a library embedded in someone else's process must not take it down
	// over a record the caller could have skipped.
	crc := m.Crc()
	if c := crc32.Checksum(m[4:], crc32cTable); crc != c {
		return nil, 0, 0, 0, pkgErrors.Wrapf(ErrCorruptRecord,
			"record at offset %d: expected CRC 0x%08x, got 0x%08x", offset, crc, c)
	}
	return m, offset, timestamp, leaderEpoch, nil
}

// readMessageMetadata reads a message from the log, parses headers and
// attributes, and returns them without CRC-validating the payload. The
// payloadBuf slice is reused across calls to avoid per-message allocations.
// Callers should pass the returned slice back on the next call.
//
// This is intended for metadata-only scans (LSO rebuild, offset tracking)
// where the value bytes are not needed and full deserialization is wasteful.
func readMessageMetadata(ctx context.Context, reader contextReader, hdrBuf []byte, payloadBuf []byte) (MessageMetadata, []byte, error) {
	fh, err := readFrameHeader(ctx, reader, hdrBuf)
	if err != nil {
		return MessageMetadata{}, payloadBuf, err
	}
	offset := fh.offset
	// Chunked for the same reason as readMessage: an unchecksummed size field
	// must not turn a torn frame into a 4GiB allocation. See maxPayloadChunk.
	buf, err := readPayload(ctx, reader, int(fh.size), payloadBuf)
	if err != nil {
		return MessageMetadata{}, payloadBuf, pkgErrors.Wrap(err, "failed to read message payload")
	}
	payloadBuf = buf
	// Nothing has vouched for these bytes. This path skips the payload CRC by
	// design, and the frame header's CRC covers the record's identity rather than
	// its contents — so the length fields below are unverified, and the parse has
	// to be the thing that refuses them. Both checks used to be absent: a short
	// frame indexed buf[5] off the end, and a damaged key length took
	// parseHeadersAfterValue past it.
	if len(buf) < 6 {
		return MessageMetadata{}, payloadBuf, pkgErrors.Wrapf(ErrCorruptRecord,
			"record at offset %d: frame claims %d bytes, too short to hold a record",
			offset, len(buf))
	}
	headers, err := parseHeadersAfterValue(buf)
	if err != nil {
		return MessageMetadata{}, payloadBuf, pkgErrors.Wrapf(ErrCorruptRecord,
			"record at offset %d: header parse %v", offset, err)
	}
	// Every Raw handed out here has therefore survived a bounds-checked parse,
	// which is what makes a later Raw.Headers() — the same walk, unchecked — safe
	// on it.
	return MessageMetadata{
		Offset:      offset,
		Timestamp:   fh.timestamp,
		LeaderEpoch: fh.leaderEpoch,
		Attributes:  int8(buf[5]),
		Headers:     headers,
		Raw:         SerializedMessage(buf),
	}, payloadBuf, nil
}
