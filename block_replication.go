package commitlog

import (
	"fmt"
	"io"
	"sort"

	"github.com/pkg/errors"
)

// Block is one on-disk block as ReadBlocks hands it out and AppendBlock takes
// it: the header and payload verbatim, plus what a caller needs to account for
// it without decoding.
type Block struct {
	// Data is the block header and payload exactly as the source stores them.
	Data []byte
	// FirstOffset is the offset of the block's first record.
	FirstOffset int64
	// LastOffset is the highest offset this block accounts for: a follower
	// resuming at LastOffset+1 misses nothing. On a compacted source it can
	// name an offset the block does not hold — it is the offset before the
	// next block's first record, since the block's own last record is inside
	// the payload and finding it would cost the decode this type exists to
	// avoid.
	LastOffset int64
	// Records is how many records the block holds, from its header.
	Records int
	// LogicalLen is the payload's uncompressed length, from its header.
	LogicalLen int
}

// ReadBlocks is ReadMessageSet for a caller that stores blocks verbatim. See
// the interface doc for the contract.
func (l *commitLog) ReadBlocks(offset int64, maxBytes int) (head []byte, blocks []Block, err error) {
	if maxBytes <= 0 {
		return nil, nil, errors.Wrap(ErrInvalidOptions, "maxBytes must be positive")
	}
	err = l.readResolving(func() (err error) {
		head, blocks, err = l.readBlocksOnce(offset, maxBytes)
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	return head, blocks, nil
}

func (l *commitLog) readBlocksOnce(offset int64, maxBytes int) ([]byte, []Block, error) {
	seg, contains := findSegmentContains(l.segmentsSnapshot(), offset)
	if seg == nil {
		return nil, nil, ErrSegmentNotFound
	}
	return readBlocksFrom(seg, contains, offset, maxBytes)
}

// readBlocksFrom reads one segment, already resolved, in the units the segment
// is stored in: the frames from offset to the end of its block as head, then
// whole blocks.
func readBlocksFrom(seg *segment, contains bool, offset int64, maxBytes int) (head []byte, blocks []Block, err error) {
	// Before anything asks for the layout: a tiered segment fetches its block
	// table on demand, and the clamped path below never goes through findEntry,
	// which is where every other reader has it loaded.
	if err := seg.ensureBlocksLoaded(); err != nil {
		return nil, nil, err
	}
	if !seg.BlockMode() {
		// Not block-framed: the segment's bytes ARE the logical framing, so
		// everything it can give is head.
		head, err := readMessageSetFrom(seg, contains, offset, maxBytes)
		return head, nil, err
	}
	start := int64(0)
	if contains {
		// The sparse index first, and findEntry only when it has to be. On a
		// block segment findEntry scans forward from the anchor to the exact
		// record, which DECODES the block — and an offset that IS an anchor is
		// answered by the index alone. The scan is paid only for an offset
		// inside a block, where the head it produces needs the decode anyway.
		a, err := seg.anchorEntryForOffset(offset)
		if err != nil {
			return nil, nil, err
		}
		start = a.Position
		if a.Offset < offset {
			e, err := seg.findEntry(offset)
			if err != nil {
				return nil, nil, err
			}
			start = e.Position
		}
	}
	layout, ok := seg.blockTailFrom(start)
	if !ok {
		return nil, nil, fmt.Errorf("%w: block table unavailable for the segment at %d",
			ErrSegmentUnreadable, seg.BaseOffset)
	}
	if len(layout.blocks) == 0 {
		return nil, nil, nil
	}
	if first := layout.blocks[0]; start > first.logicalStart {
		head, err = readFramesBetween(seg, offset, start, first.logicalStart+first.logicalLen)
		if err != nil {
			return nil, nil, err
		}
		layout.blocks = layout.blocks[1:]
	}
	br := seg.newBlockReader()
	defer br.close()
	budget := maxBytes - len(head)
	var next int64 // the next block's first offset, once looked up
	for i, b := range layout.blocks {
		// The first unit is returned whole even when it alone exceeds the
		// budget, as ReadMessageSet does, so a caller is never starved.
		if (len(head) > 0 || len(blocks) > 0) && b.physLen > int64(budget) {
			break
		}
		first := next
		if i == 0 {
			if first, err = seg.blockAnchor(b.logicalStart); err != nil {
				return nil, nil, err
			}
		}
		last := layout.lastOffset
		if i+1 < len(layout.blocks) {
			if next, err = seg.blockAnchor(layout.blocks[i+1].logicalStart); err != nil {
				return nil, nil, err
			}
			last = next - 1
		}
		data, err := br.read(b)
		if err != nil {
			return nil, nil, err
		}
		blocks = append(blocks, Block{
			Data:        data,
			FirstOffset: first,
			LastOffset:  last,
			Records:     int(b.records),
			LogicalLen:  int(b.logicalLen),
		})
		budget -= int(b.physLen)
	}
	return head, blocks, nil
}

// readFramesBetween returns the whole frames from logical position start up to
// end, which is a block boundary. offset names the record at start, for the
// error.
func readFramesBetween(seg *segment, offset, start, end int64) ([]byte, error) {
	ss := newSegmentScannerCache(seg, newBlockCache())
	defer ss.Close() // nolint: errcheck — read-only
	ss.pos = start
	out := make([]byte, 0, end-start)
	for ss.pos < end {
		ms, _, err := ss.Scan()
		if err != nil {
			// The classification readMessageSetFrom gives, for its reasons: a
			// swap is the log compacting and is re-resolved above; anything
			// else that stops the scan short of a boundary the layout promised
			// is damage a follower should hear about.
			if segmentSwapped(err) {
				return nil, err
			}
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("%w: message set at offset %d: %w",
				ErrSegmentUnreadable, offset, err)
		}
		out = append(out, ms...)
	}
	return out, nil
}

// AppendBlock is AppendMessageSet for a block obtained from ReadBlocks on
// another log. See the interface doc for the contract.
func (l *commitLog) AppendBlock(b Block) ([]int64, error) {
	codec, uLen, cLen, records, err := parseBlockHeader(b.Data)
	if err != nil {
		if errors.Is(err, ErrBlockFormat) {
			// Another build's bytes, exactly as its writer meant them. The
			// remedy is the right binary, not a different block.
			return nil, err
		}
		return nil, errors.Wrapf(ErrMessageSetRefused, "block header: %v", err)
	}
	if want := blockHeaderLen + int(cLen); len(b.Data) != want {
		return nil, errors.Wrapf(ErrMessageSetRefused,
			"block is %d bytes, its header promises %d", len(b.Data), want)
	}
	// Decoded before the append lock, and decoded at all: the checks below are
	// AppendMessageSet's, and they read offsets out of the framing. The bytes
	// written are still b.Data.
	logical, err := codec.DecompressInto(nil, b.Data[blockHeaderLen:])
	if err != nil {
		return nil, errors.Wrapf(ErrMessageSetRefused, "block payload does not decode: %v", err)
	}
	if len(logical) != int(uLen) {
		return nil, errors.Wrapf(ErrMessageSetRefused,
			"block decodes to %d bytes, its header promises %d", len(logical), uLen)
	}

	l.appendMu.Lock()
	defer l.appendMu.Unlock()
	if _, err := l.checkAndPerformSplit(); err != nil {
		return nil, err
	}
	segment := l.activeSegment()
	entries := entriesForMessageSet(segment.Position(), logical)
	if err := checkAppendedSet(segment.NextOffset()-1, entries, len(logical)); err != nil {
		return nil, err
	}
	if len(entries) != int(records) {
		// The count is what the segment reports without decoding — a retention
		// walk budgets on it — so a header that miscounts its own payload is
		// refused rather than stored as a fact.
		return nil, errors.Wrapf(ErrMessageSetRefused,
			"block header claims %d records, its payload frames %d", records, len(entries))
	}
	if !segment.BlockMode() {
		// A raw segment has no block layout to append to; its bytes are the
		// framing, so the decoded framing is what goes in.
		return l.append(segment, logical, entries)
	}
	return l.appendWith(segment, entries, func() error {
		return segment.WriteBlock(b.Data, codec, int64(uLen), entries)
	})
}

// blockTail is the part of a segment's block table from one logical position
// on, with the segment's last offset as of the same moment.
type blockTail struct {
	blocks     []blockRef
	lastOffset int64
}

// blockTailFrom copies out the blocks from the one holding logical position
// pos to the end of the table, and the segment's last offset under the same
// lock — so the last block's LastOffset is the tail as of THIS table, not one
// an append has since moved. ok is false for a segment with no block layout.
//
// The caller has run ensureBlocksLoaded.
func (s *segment) blockTailFrom(pos int64) (layout blockTail, ok bool) {
	s.RLock()
	defer s.RUnlock()
	if !s.blockMode || s.blocksPending {
		return blockTail{}, false
	}
	from := sort.Search(len(s.blocks), func(i int) bool {
		return s.blocks[i].logicalStart+s.blocks[i].logicalLen > pos
	})
	blocks := make([]blockRef, len(s.blocks)-from)
	copy(blocks, s.blocks[from:])
	return blockTail{blocks: blocks, lastOffset: s.lastOffset}, true
}

// anchorEntryForOffset returns the sparse-index anchor of the block that holds
// offset: the block's first record, with its offset and logical position. When
// every anchor is past offset — the record is below the first one the segment
// still holds — the first anchor is returned, which is where a read clamped up
// to the oldest survivor starts.
func (s *segment) anchorEntryForOffset(offset int64) (entry, error) {
	s.RLock()
	defer s.RUnlock()
	var e entry
	err := s.withIndex(func(idx *index) error {
		i, n, err := idx.searchEntries(&e, func(e *entry) bool { return e.Offset > offset })
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("%w: no index anchors for the segment at %d",
				ErrSegmentUnreadable, s.BaseOffset)
		}
		if i > 0 {
			i--
		}
		return idx.ReadEntryAtLogOffset(&e, int64(i))
	})
	return e, err
}

// blockAnchor returns the first offset of the block starting at logical
// position pos, from the sparse index — which holds exactly one entry per
// block, anchored at the block's first record.
func (s *segment) blockAnchor(pos int64) (int64, error) {
	s.RLock()
	defer s.RUnlock()
	var e entry
	err := s.withIndex(func(idx *index) error {
		i, n, err := idx.searchEntries(&e, func(e *entry) bool { return e.Position >= pos })
		if err != nil {
			return err
		}
		if i == n || e.Position != pos {
			return fmt.Errorf("%w: no index anchor for the block at position %d",
				ErrSegmentUnreadable, pos)
		}
		return nil
	})
	return e.Offset, err
}

// blockReader reads blocks' physical bytes from a segment's backing, holding a
// claim on it for as long as it is open — the claim newSegmentScannerCache
// registers, for the reason prefix_read.go gives: a tiered object a read holds
// no claim on can be reclaimed underneath it.
type blockReader struct {
	seg     *segment
	backing segmentBacking
	pin     *storeBacking
}

func (s *segment) newBlockReader() *blockReader {
	s.RLock()
	defer s.RUnlock()
	return &blockReader{seg: s, backing: s.backing, pin: acquireBacking(s.backing)}
}

// read returns the block's header and payload verbatim, with no decode.
func (br *blockReader) read(b blockRef) ([]byte, error) {
	br.seg.RLock()
	err := br.seg.shutErrorLocked()
	br.seg.RUnlock()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, b.physLen)
	if _, err := br.backing.ReadAt(buf, b.physStart); err != nil {
		return nil, errors.Wrap(err, "read block failed")
	}
	return buf, nil
}

func (br *blockReader) close() {
	if br.pin != nil {
		br.pin.release()
		br.pin = nil
	}
}
