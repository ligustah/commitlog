package commitlog

import (
	"fmt"
	"io"
	"os"

	"github.com/ligustah/commitlog/compress"
	"github.com/pkg/errors"
)

// Reading a segment file WITHOUT opening the log that wrote it.
//
// This exists because both consumers of this package had already written it
// themselves, and both got it wrong. One decoded block headers a byte short and
// read every record as the wrong shape; the other hard-coded the 0xC1 magic and
// a format version into PRODUCT code to avoid importing this package. When a
// corruption report arrived, those two mirrors disagreed with each other and
// with the log, and the days spent reconciling them were spent on nothing: the
// records were fine and the decoders were not.
//
// A format has exactly one correct reader, and it belongs in the package that
// writes the format. Anything else is a copy that goes stale silently — the
// mirror above was written against a header layout that changed in v0.15.0.
//
// Scope is deliberately narrow: read-only, over files nothing is writing, with
// no index, no compaction and no recovery. It is for looking at bytes that are
// already on disk — a forensic tool, not a second read path.
//
// NON-MUTATING, which is the property a diagnostic most needs and the one that
// makes New unsuitable for this. Opening a log runs recovery, may adopt a
// descriptor and may rewrite segments, so pointing it at evidence alters the
// evidence — which is why the mirrors this replaces all carried a warning to
// work on a COPY of the data directory. This reads one file, once, and writes
// nothing, so it can be aimed at the original.
//
// It also takes no Options. Nothing about Path, Name, Compact or descriptor
// adoption is load-bearing here: a segment file describes itself, so a caller
// inspecting a foreign directory has nothing to configure and nothing to guess.

// BlockInfo describes one physical block in a segment file.
//
// For a segment written without block compression there are no blocks, and
// InspectSegment reports none — the records are framed directly.
type BlockInfo struct {
	// Offset of the block header within the file.
	FileOffset int64
	// Codec the payload is compressed with; compress.None for a stored block.
	Codec compress.Codec
	// UncompressedLen and CompressedLen are the header's own claims, not
	// measurements. A block whose payload does not decompress to
	// UncompressedLen bytes is reported by Blocks as an error rather than
	// silently trusted.
	UncompressedLen uint32
	CompressedLen   uint32
}

// RecordInfo is one record as it sits in the file.
//
// Key and Value alias the decoded buffer for the block they came from, which
// Records reuses. Copy anything kept past the callback.
type RecordInfo struct {
	Offset      int64
	Timestamp   int64
	LeaderEpoch uint64
	Attributes  int8
	Key         []byte
	Value       []byte
	// CRCValid reports whether the record matches its own checksum. Records
	// does NOT stop at a record that fails: an inspector's job is to show what
	// is there, and a caller looking for damage needs to see the damaged one
	// rather than an error where it should have been.
	CRCValid bool
}

// SegmentFormat is how a segment file is framed, as told by its first two
// bytes.
//
// This exists so that "which format is this directory in" can be answered
// without reading the segment. InspectSegment answers it too, but only as a
// side effect of loading the whole file — so a caller probing a data dir at
// boot had the choice between reading gigabytes it does not want and copying
// the magic byte into its own code. Both consumers chose the copy, which is the
// failure the doc at the top of this file describes. A cheap correct answer is
// what removes the incentive.
type SegmentFormat struct {
	// Blocked reports whether the file is block-framed rather than a flat
	// sequence of record frames. Decided by the magic byte, not by any naming
	// convention.
	Blocked bool
	// Version is the block format version claimed by the header, and is
	// meaningful only when Blocked. It is reported rather than judged: an
	// unrecognised version is exactly what a caller probing a foreign
	// directory needs to SEE, and refusing to hand it back would leave it
	// asking the question this type exists to answer. Use Readable to judge.
	Version byte
}

// Readable reports whether this build can decode the file's blocks.
//
// A flat segment is always readable here — this concerns block framing only,
// and says nothing about whether the records inside are intact.
func (f SegmentFormat) Readable() bool {
	return !f.Blocked || f.Version == BlockFormatVersion
}

// ClassifySegment reports how a segment .log file is framed, reading only its
// header and never its body.
//
// Two bytes off the front, which is the entire point: this is for a process
// deciding at startup whether it understands a data directory, where
// InspectSegment's whole-file read is the cost that drove callers to hard-code
// 0xC1 instead.
//
// An empty file is reported as flat and not an error. A zero-length segment is
// a legitimate segment with no records in it, and a probe that treats it as
// damage would fail on a log that has just been created.
//
// A file that starts with the block magic but is too short to carry a version
// byte IS an error, and deliberately not a SegmentFormat with Version 0. There
// is no version in that file to report, and answering with a zero would be
// answering a question the bytes did not settle — the caller cannot tell that
// apart from a real version byte that happened to be 0.
func ClassifySegment(path string) (SegmentFormat, error) {
	f, err := os.Open(path)
	if err != nil {
		return SegmentFormat{}, errors.Wrap(err, "open segment file")
	}
	defer f.Close()

	var hdr [2]byte
	n, err := io.ReadFull(f, hdr[:])
	switch {
	case errors.Is(err, io.EOF):
		// Empty file: no magic, so flat. n is 0 here by ReadFull's contract.
		return SegmentFormat{}, nil
	case errors.Is(err, io.ErrUnexpectedEOF):
		// Exactly one byte. Only interesting if it is the magic; a one-byte
		// flat segment is malformed for other reasons that are not this
		// function's to diagnose.
		if hdr[0] == blockMagic {
			return SegmentFormat{}, errors.Errorf(
				"commitlog: %s begins with the block magic but is %d byte; "+
					"there is no version byte to read", path, n)
		}
		return SegmentFormat{}, nil
	case err != nil:
		return SegmentFormat{}, errors.Wrap(err, "read segment header")
	}
	if !isBlockFramed(hdr[:]) {
		return SegmentFormat{}, nil
	}
	return SegmentFormat{Blocked: true, Version: hdr[1]}, nil
}

// isBlockFramed is the one place the magic byte decides anything. Both the
// header-only classifier and the whole-file inspector route through it so they
// cannot drift into disagreeing about what a segment is.
func isBlockFramed(b []byte) bool { return len(b) > 0 && b[0] == blockMagic }

// SegmentFile is an open, read-only view of one segment's .log file.
type SegmentFile struct {
	path string
	raw  []byte
	// blocked records whether the file is block-framed, decided by the magic
	// byte of the first header rather than by any naming convention.
	blocked bool
}

// InspectSegment opens a segment .log file for reading. The file must not be
// being written: this takes a snapshot and never re-reads.
//
// It does not need the log, its index, or any sidecar, and it never writes.
func InspectSegment(path string) (*SegmentFile, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.Wrap(err, "read segment file")
	}
	s := &SegmentFile{path: path, raw: raw, blocked: isBlockFramed(raw)}
	return s, nil
}

// Format reports how this file is framed, from the header already in memory.
//
// Same answer ClassifySegment gives for the same file, and by construction:
// both decide "blocked" through isBlockFramed. The difference is only what it
// cost to get here.
//
// A block-framed file too short to carry a version byte reports Version 0 here,
// where ClassifySegment refuses. That is not an inconsistency: this call has
// already read the whole file, so the caller can see the truncation directly
// and Blocks names it precisely — an inspector's job is to show what is there,
// the same reason a bad checksum is a RecordInfo field and not an error.
func (s *SegmentFile) Format() SegmentFormat {
	if !s.blocked {
		return SegmentFormat{}
	}
	var v byte
	if len(s.raw) > 1 {
		v = s.raw[1]
	}
	return SegmentFormat{Blocked: true, Version: v}
}

// Blocked reports whether the file is block-framed (compressed or stored
// blocks) rather than a flat sequence of record frames.
func (s *SegmentFile) Blocked() bool { return s.blocked }

// Size is the file's physical length in bytes.
func (s *SegmentFile) Size() int64 { return int64(len(s.raw)) }

// Blocks walks the block headers.
//
// A header this build does not understand comes back as ErrBlockFormat naming
// both versions — the one in the file and the one this build writes. It does
// not guess which build wrote the file: an unreadable version byte looks the
// same whether the writer was older or newer, so the two numbers are the whole
// of what is actually known.
func (s *SegmentFile) Blocks() ([]BlockInfo, error) {
	if !s.blocked {
		return nil, nil
	}
	var (
		out []BlockInfo
		pos int64
	)
	for pos < int64(len(s.raw)) {
		if rem := int64(len(s.raw)) - pos; rem < blockHeaderLen {
			return out, errors.Errorf(
				"commitlog: truncated block header at %d (%d bytes left, need %d)",
				pos, rem, blockHeaderLen)
		}
		codec, uLen, cLen, err := parseBlockHeader(s.raw[pos : pos+blockHeaderLen])
		if err != nil {
			return out, errors.Wrapf(err, "%s: block at %d", s.path, pos)
		}
		out = append(out, BlockInfo{
			FileOffset: pos, Codec: codec, UncompressedLen: uLen, CompressedLen: cLen,
		})
		// The payload has to BE there. Without this the walk simply added cLen to
		// pos, and a header claiming more bytes than the file holds stepped clean
		// over the end — the loop condition then ended the walk and reported
		// success, listing the overrunning block as though it were fine.
		//
		// Truncation is the corruption this is most likely to be pointed at: a
		// short write, a partial upload, a download cut off mid-object. Reporting
		// the file as sound is the one answer that sends the investigation
		// somewhere else entirely.
		//
		// Records already refused it, which is what makes this worth stating: the
		// two walks gave OPPOSITE answers about the same bytes, one describing the
		// layout as intact while the other could not read it. That is the failure
		// the note at the top of this file is about, reproduced between two
		// functions in the same package rather than between two repos. Same bound
		// and same wording as recordsBlocked, so they cannot drift apart again.
		start := pos + blockHeaderLen
		if end := start + int64(cLen); end > int64(len(s.raw)) {
			return out, errors.Errorf(
				"commitlog: %s: block at %d claims %d payload bytes, file holds %d",
				s.path, pos, cLen, int64(len(s.raw))-start)
		}
		pos += blockHeaderLen + int64(cLen)
	}
	return out, nil
}

// Records calls fn for every record in the file, in offset order, decompressing
// blocks as it goes.
//
// It reports a record that fails its checksum rather than refusing it — see
// RecordInfo.CRCValid. Stop early by returning an error from fn; that error is
// returned unchanged.
func (s *SegmentFile) Records(fn func(RecordInfo) error) error {
	if s.blocked {
		return s.recordsBlocked(fn)
	}
	return walkFrames(s.raw, fn)
}

func (s *SegmentFile) recordsBlocked(fn func(RecordInfo) error) error {
	var pos int64
	for pos < int64(len(s.raw)) {
		if rem := int64(len(s.raw)) - pos; rem < blockHeaderLen {
			return errors.Errorf("commitlog: truncated block header at %d", pos)
		}
		codec, uLen, cLen, err := parseBlockHeader(s.raw[pos : pos+blockHeaderLen])
		if err != nil {
			return errors.Wrapf(err, "block at %d", pos)
		}
		start := pos + blockHeaderLen
		end := start + int64(cLen)
		if end > int64(len(s.raw)) {
			return errors.Errorf(
				"commitlog: block at %d claims %d payload bytes, file holds %d",
				pos, cLen, int64(len(s.raw))-start)
		}
		payload, derr := codec.Decompress(s.raw[start:end])
		if derr != nil {
			return errors.Wrapf(derr, "decompress block at %d", pos)
		}
		if uint32(len(payload)) != uLen {
			return errors.Errorf(
				"commitlog: block at %d decompressed to %d bytes, header claims %d",
				pos, len(payload), uLen)
		}
		if err := walkFrames(payload, fn); err != nil {
			return err
		}
		pos = end
	}
	return nil
}

// walkFrames iterates the message-set frames in a logical byte range, using the
// same accessors the log itself reads with, so an inspector cannot drift from
// the real framing.
func walkFrames(buf []byte, fn func(RecordInfo) error) error {
	var pos int64
	for pos < int64(len(buf)) {
		if rem := int64(len(buf)) - pos; rem < msgSetHeaderLen {
			return errors.Errorf(
				"commitlog: truncated record frame at %d (%d bytes left)", pos, rem)
		}
		ms := messageSet(buf[pos:])
		size := int64(ms.Size())
		end := pos + msgSetHeaderLen + size
		if size < 0 || end > int64(len(buf)) {
			return errors.Errorf(
				"commitlog: record frame at %d claims %d bytes, %d left",
				pos, size, int64(len(buf))-pos-msgSetHeaderLen)
		}
		msg := messageSet(buf[pos:end]).Message()
		rec := RecordInfo{
			Offset:      ms.Offset(),
			Timestamp:   ms.Timestamp(),
			LeaderEpoch: ms.LeaderEpoch(),
		}
		if len(msg) >= 4 {
			rec.Attributes = msg.Attributes()
			rec.Key = msg.Key()
			rec.Value = msg.Value()
			rec.CRCValid = msg.crcMatches()
		}
		if err := fn(rec); err != nil {
			return err
		}
		pos = end
	}
	return nil
}

// String renders a one-line summary, for a tool that just wants to say what a
// file is.
func (s *SegmentFile) String() string {
	kind := "raw frames"
	if s.blocked {
		kind = "block-framed"
	}
	return fmt.Sprintf("%s: %d bytes, %s", s.path, len(s.raw), kind)
}
