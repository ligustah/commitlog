package commitlog

import (
	"fmt"
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
	s := &SegmentFile{path: path, raw: raw}
	if len(raw) > 0 && raw[0] == blockMagic {
		s.blocked = true
	}
	return s, nil
}

// Blocked reports whether the file is block-framed (compressed or stored
// blocks) rather than a flat sequence of record frames.
func (s *SegmentFile) Blocked() bool { return s.blocked }

// Size is the file's physical length in bytes.
func (s *SegmentFile) Size() int64 { return int64(len(s.raw)) }

// Blocks walks the block headers.
//
// The error is the interesting part of this call. A segment written before
// v0.15.0 has a 10-byte header with NO version byte — magic, codec, then the
// lengths — so this build reads its codec byte as a version and refuses it with
// ErrBlockFormat. That is correct (the layouts really are incompatible) but the
// message alone sent one consumer looking for a writer that never existed, so
// the case is named explicitly here.
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
			if errors.Is(err, ErrBlockFormat) && pos == 0 {
				return out, errors.Wrapf(err,
					"%s: if this file predates v0.15.0 its header is 10 bytes with no "+
						"version field (magic, codec, lengths) and the byte read as a "+
						"version is the CODEC; such segments are not readable by this build",
					s.path)
			}
			return out, errors.Wrapf(err, "block at %d", pos)
		}
		out = append(out, BlockInfo{
			FileOffset: pos, Codec: codec, UncompressedLen: uLen, CompressedLen: cLen,
		})
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
