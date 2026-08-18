package commitlog

import (
	"bytes"

	"github.com/pkg/errors"
)

// Reader construction takes functional options rather than a spec struct
// because the zero value of a read setting is MEANINGFUL: offset 0 is a real
// offset, committed-only is a real choice, and a bound has no natural "none"
// value. A struct has to invent sentinels for all three; options let "unset"
// be genuinely absent, with every default stated once, here.
//
// This is the opposite conclusion to CleanSpec, deliberately. That is data a
// transactional layer computes, passes down, and may want to log or compare —
// it should be inspectable. Options are not inspectable, so they only ever
// CONSTRUCT the readSpec below; the struct is still what the reader holds and
// what gets logged.

// ReadOption configures a Reader. See CommitLog.NewReader.
type ReadOption func(*readSpec)

// readSpec is the resolved configuration a Reader holds.
type readSpec struct {
	offset    int64
	offsetSet bool
	until     int64
	untilSet  bool

	follow      bool
	uncommitted bool

	keyPrefix      []byte
	prefixSet      bool
	skipSuperseded bool
	includeControl bool
}

// From starts the read at offset. Defaults to the log's oldest surviving
// record, so a caller that just wants everything need not ask for it.
//
// An offset BELOW the oldest surviving record is served from the oldest
// survivor rather than refused. This is a guarantee, not an accident: it is
// what lets a reader outlive retention and compaction. The offsets it asked
// for are gone and the next surviving records are the right answer. Both a
// committed and an uncommitted reader behave this way.
//
// The cost is that the read STARTS LATER THAN ASKED and says nothing about it.
// A caller for whom the gap matters — one tracking a replica's position, or
// resuming a consumer — cannot learn it from the error (there is none) and must
// compare the first offset it receives against the one it requested. Clamping
// the request to OldestOffset() first does not avoid this: retention moves the
// floor under a live reader, so the clamp only narrows the window in which the
// two can disagree.
func From(offset int64) ReadOption {
	return func(s *readSpec) { s.offset, s.offsetSet = offset, true }
}

// Until stops the read after offset: the record AT offset is returned, and the
// read then ends as if it had reached the end of the log. Defaults to
// unbounded.
//
// For a KeyPrefix read that is also Uncommitted, this is REQUIRED unless
// IncludeControl is set — see NewReader.
func Until(offset int64) ReadOption {
	return func(s *readSpec) { s.until, s.untilSet = offset, true }
}

// Follow parks for appends at the end of the log instead of returning io.EOF.
//
// Opt-in, where the old NewReader made it the default. The failure modes are
// not symmetric: a reader that unexpectedly ends returns io.EOF and its caller
// notices, while one that unexpectedly follows blocks forever. The hanging
// case should be the one you have to ask for.
func Follow() ReadOption {
	return func(s *readSpec) { s.follow = true }
}

// Uncommitted reads past the high watermark, returning records the log holds
// but has not committed. Defaults to committed-only.
func Uncommitted() ReadOption {
	return func(s *readSpec) { s.uncommitted = true }
}

// KeyPrefix returns only records whose key begins with prefix. An empty (but
// non-nil) prefix matches every KEYED record; not calling this at all returns
// every record, keyed or not.
//
// Over a sealed segment that has a key digest this is served from it, so only
// matching records are read rather than every record being read and tested.
// Without one the segment is scanned and filtered in a single pass, which is
// what the active segment always gets — the acceleration is a property of
// having a digest, not of the API.
//
// Worth knowing which logs have them: the compact cleaner is the only thing
// that writes a digest, so on a log with Compact disabled NO sealed segment has
// one and a prefix read costs a scan per segment, every time. That is a correct
// read and a permanent cost, not a warm-up.
//
// Unkeyed records cannot match and are dropped. So are control markers, which
// have no key at all; see IncludeControl.
func KeyPrefix(prefix []byte) ReadOption {
	return func(s *readSpec) { s.keyPrefix, s.prefixSet = prefix, true }
}

// SkipSuperseded drops copies of a key that a LATER copy in the SAME segment
// supersedes. A key rewritten a thousand times inside one segment yields one
// record; across the log it yields at most one per segment.
//
// An optimisation, never a guarantee. Duplicates still arrive across segments
// and from the active tail, and that is fine: compaction is itself
// asynchronous, so any consumer already has to tolerate more than one copy of
// a key. This only declines to ship copies it can cheaply prove are stale.
//
// It is decided from the digest alone — whether a copy is the last for its key
// WITHIN its segment needs no lookahead — which is why it streams and can
// follow.
//
// One asymmetry worth knowing: what counts as superseded depends on where the
// read began. A reader resuming mid-segment sees the copies before its start
// as absent rather than as supersessions, so it can return a record that a
// reader of the whole segment would have skipped. It returns MORE records,
// never fewer, and never a stale value for a key it reports — but two readers
// at different offsets need not agree on the record count.
func SkipSuperseded() ReadOption {
	return func(s *readSpec) { s.skipSuperseded = true }
}

// IncludeControl keeps transactional control markers (AttrControl) in a
// KeyPrefix read. They are keyless, so a key filter would otherwise drop them.
//
// Needed only by a caller that reads UNCOMMITTED and does its own
// transactional filtering: markers are what decide undecided records, and
// below the commit boundary compaction has already removed aborted records and
// stripped the survivors, so there is nothing left for a marker to say.
func IncludeControl() ReadOption {
	return func(s *readSpec) { s.includeControl = true }
}

// resolve applies the options over the defaults and rejects combinations that
// cannot produce a usable answer.
func (l *commitLog) resolve(opts []ReadOption) (readSpec, error) {
	s := readSpec{until: -1}
	for _, opt := range opts {
		if opt != nil {
			opt(&s)
		}
	}
	if !s.offsetSet {
		s.offset = l.OldestOffset()
		if s.offset < 0 {
			s.offset = 0
		}
	}
	if s.untilSet && s.until < s.offset {
		return s, errors.Errorf(
			"commitlog: Until(%d) is below From(%d) — the range is empty", s.until, s.offset)
	}
	// The one refused combination. Reading past the commit boundary means
	// records whose transactions are undecided, and the only thing that could
	// say which committed is the markers a key filter drops. The caller is
	// then holding records it cannot classify, and nothing reports it.
	//
	// commitlog cannot check that a stated bound really is the caller's commit
	// boundary — it has no notion of decidedness, exactly as CleanSpec.Ceiling
	// is an input it must trust. What it can insist on is that the caller
	// considered the boundary at all.
	if s.prefixSet && s.uncommitted && !s.untilSet && !s.includeControl {
		return s, errors.New(
			"commitlog: KeyPrefix with Uncommitted returns records the caller cannot " +
				"classify: control markers are keyless and were filtered out, so nothing " +
				"says which transactions committed. Pass Until(lso) to bound the read at " +
				"your commit boundary, or IncludeControl() to filter transactions yourself")
	}
	return s, nil
}

// matchesPrefix reports whether a record belongs in a filtered read.
func (s *readSpec) matchesPrefix(msg SerializedMessage) bool {
	if !s.prefixSet {
		return true
	}
	if msg.Attributes()&AttrControl != 0 {
		return s.includeControl
	}
	key := msg.Key()
	if key == nil {
		return false // unkeyed: nothing a prefix could match
	}
	return bytes.HasPrefix(key, s.keyPrefix)
}
