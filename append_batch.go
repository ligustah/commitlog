package commitlog

import (
	"hash/crc32"

	"github.com/ligustah/commitlog/blockv3"
	"github.com/ligustah/commitlog/compress"
	"github.com/pkg/errors"
)

// BatchMeta is the producer identity of one appended batch: record i of the
// batch has sequence BaseSequence+i, and Nonce names its transaction, zero for
// none.
type BatchMeta struct {
	ProducerID    uint64
	ProducerEpoch uint32
	BaseSequence  int32
	Nonce         uint64
}

// AppendBatch implements CommitLog.
func (l *commitLog) AppendBatch(meta BatchMeta, msgs []*Message) ([]int64, error) {
	if l.IsReadonly() {
		return nil, ErrCommitLogReadonly
	}
	if len(msgs) == 0 {
		return nil, errors.Wrap(ErrMessageSetRefused, "empty batch")
	}
	for i, m := range msgs {
		for _, k := range []string{hdrProducerID, hdrProducerEpoch, hdrSequence, hdrNonce} {
			if _, taken := m.Headers[k]; taken {
				return nil, errors.Wrapf(ErrMessageSetRefused,
					"message %d carries header %q, which the batch identity writes", i, k)
			}
		}
	}
	if l.BlockFormat != blockv3.Version {
		return l.appendStamped(meta, msgs)
	}
	var (
		stamp   int64
		epoch   = msgs[0].LeaderEpoch
		control = msgs[0].Attributes&AttrControl != 0
	)
	for i, m := range msgs {
		// One block is one timestamp, one epoch and one kind: a version-3
		// header holds each once for every record under it.
		if m.Timestamp != 0 && stamp != 0 && m.Timestamp != stamp {
			return nil, errors.Wrapf(ErrMessageSetRefused,
				"message %d is stamped %d, the batch %d; a batch carries one timestamp", i, m.Timestamp, stamp)
		}
		if m.Timestamp != 0 {
			stamp = m.Timestamp
		}
		if m.LeaderEpoch != epoch {
			return nil, errors.Wrapf(ErrMessageSetRefused,
				"message %d is in epoch %d, the batch in %d", i, m.LeaderEpoch, epoch)
		}
		if (m.Attributes&AttrControl != 0) != control {
			return nil, errors.Wrapf(ErrMessageSetRefused,
				"message %d mixes control and data records in one batch", i)
		}
	}

	l.appendMu.Lock()
	defer l.appendMu.Unlock()
	// Under appendMu, for the reason Append reads its clock there.
	if stamp == 0 {
		stamp = timestamp()
	}
	for _, m := range msgs {
		if m.Timestamp == 0 {
			m.Timestamp = stamp
		}
	}
	if _, err := l.checkAndPerformSplit(); err != nil {
		return nil, err
	}
	segment := l.activeSegment()
	h := blockv3.Header{
		Codec:         l.Compression,
		BaseOffset:    segment.NextOffset(),
		LeaderEpoch:   epoch,
		BaseTimestamp: stamp,
		ProducerID:    int64(meta.ProducerID),
		ProducerEpoch: int32(meta.ProducerEpoch),
		BaseSequence:  meta.BaseSequence,
		TxNonce:       meta.Nonce,
	}
	if control {
		h.Flags |= blockv3.FlagControl
	}
	if meta.Nonce != 0 {
		h.Flags |= blockv3.FlagTransactional
	}
	recs := make([]blockv3.Record, len(msgs))
	for i, m := range msgs {
		recs[i] = blockv3.Record{
			Attributes:  byte(m.Attributes) & blockv3.AttrTombstone,
			OffsetDelta: uint32(i),
			Key:         m.Key,
			Value:       m.Value,
			Headers:     m.Headers,
		}
	}
	block, h := encodeV3Block(h, recs)
	return l.writeV3(segment, block, h, recs)
}

// appendStamped is AppendBatch on a log writing version-2 blocks: the identity
// goes on each record as the headers a version-3 block decodes to, so a reader
// sees one thing whichever format stored it. The caller's messages are not
// touched beyond what Append does to them.
func (l *commitLog) appendStamped(meta BatchMeta, msgs []*Message) ([]int64, error) {
	stamped := make([]*Message, len(msgs))
	for i, m := range msgs {
		headers := make(map[string][]byte, len(m.Headers)+4)
		for k, v := range m.Headers {
			headers[k] = v
		}
		headers[hdrProducerID] = be64(meta.ProducerID)
		headers[hdrProducerEpoch] = be32(meta.ProducerEpoch)
		headers[hdrSequence] = be32(uint32(meta.BaseSequence + int32(i)))
		if meta.Nonce != 0 {
			headers[hdrNonce] = be64(meta.Nonce)
		}
		c := *m
		c.Headers = headers
		stamped[i] = &c
	}
	offsets, err := l.Append(stamped)
	if err != nil {
		return nil, err
	}
	for i, m := range msgs {
		m.Timestamp = stamped[i].Timestamp
	}
	return offsets, nil
}

// encodeV3Block frames recs under h with the codec decision appendBlock makes
// for version-2 blocks: payloads under compressMinBlock, and ones the codec
// cannot shrink, are stored raw. The returned header is the one in the block.
func encodeV3Block(h blockv3.Header, recs []blockv3.Record) ([]byte, blockv3.Header) {
	logical := blockv3.EncodeRecords(nil, recs)
	payload := logical
	if len(logical) < compressMinBlock {
		h.Codec = compress.None
	} else if h.Codec != compress.None {
		if c := h.Codec.Compress(logical); len(c) < len(logical) {
			payload = c
		} else {
			h.Codec = compress.None
		}
	}
	h.Records = uint32(len(recs))
	h.LastOffsetDelta = recs[len(recs)-1].OffsetDelta
	h.UncompressedLen = uint32(len(logical))
	h.CompressedLen = uint32(len(payload))
	h.PayloadCRC = crc32.Checksum(payload, crc32cTable)
	block := blockv3.EncodeHeader(make([]byte, 0, blockv3.HeaderLen+len(payload)), h)
	return append(block, payload...), h
}

// AppendPreframed implements CommitLog.
func (l *commitLog) AppendPreframed(block []byte) ([]int64, error) {
	if l.IsReadonly() {
		return nil, ErrCommitLogReadonly
	}
	if l.BlockFormat != blockv3.Version {
		return nil, errors.Wrapf(ErrMessageSetRefused,
			"this log writes block format %d", l.BlockFormat)
	}
	// Decoded once, before the lock: the offsets and the per-record checks
	// below need the records, and a reader would otherwise be the first to
	// find a payload that does not parse. The bytes written are still block.
	h, recs, err := blockv3.DecodeBlock(block)
	if err != nil {
		return nil, errors.Wrapf(ErrMessageSetRefused, "block: %v", err)
	}
	if h.Flags&blockv3.FlagStripped == 0 {
		// Sequence is by index while the identity stands, so the records must
		// BE their indexes. Asserted, not assumed: a producer that skipped a
		// delta would have its later records answer to the wrong sequence.
		for i, r := range recs {
			if r.OffsetDelta != uint32(i) {
				return nil, errors.Wrapf(ErrMessageSetRefused,
					"record %d has offset delta %d; an identity-bearing block is sequence-by-index", i, r.OffsetDelta)
			}
		}
	}
	if (h.Flags&blockv3.FlagTransactional != 0) != (h.TxNonce != 0) {
		// The reader emits the nonce on the flag; a block where the two
		// disagree would store a transaction it does not report, or report
		// one it cannot name.
		return nil, errors.Wrapf(ErrMessageSetRefused,
			"transactional flag %t with nonce %d", h.Flags&blockv3.FlagTransactional != 0, h.TxNonce)
	}

	l.appendMu.Lock()
	defer l.appendMu.Unlock()
	if _, err := l.checkAndPerformSplit(); err != nil {
		return nil, err
	}
	segment := l.activeSegment()
	h.BaseOffset = segment.NextOffset()
	h.LeaderEpoch = l.leaderEpochCache.LastLeaderEpoch()
	h.BaseTimestamp = timestamp()
	if err := blockv3.Rewrite(block, h.BaseOffset, h.LeaderEpoch, h.BaseTimestamp); err != nil {
		return nil, errors.Wrapf(ErrMessageSetRefused, "block: %v", err)
	}
	return l.writeV3(segment, block, h, recs)
}

// writeV3 appends one version-3 block, h being its header as stored and recs
// its records. The block goes in verbatim; what the log's logical byte space
// holds for it is the framing the records decode to, which is what the tail
// check, the index and the epoch history read.
func (l *commitLog) writeV3(segment *segment, block []byte, h blockv3.Header, recs []blockv3.Record) ([]int64, error) {
	logical, err := synthesizeFraming(nil, h, recs)
	if err != nil {
		return nil, errors.Wrap(ErrMessageSetRefused, err.Error())
	}
	entries := entriesForMessageSet(segment.Position(), logical)
	if err := checkAppendedSet(segment.NextOffset()-1, entries, len(logical)); err != nil {
		return nil, err
	}
	if !segment.BlockMode() {
		// A raw segment has no block layout to append to; its bytes are the
		// framing, so the decoded framing is what goes in.
		return l.append(segment, logical, entries)
	}
	return l.appendWith(segment, entries, func() error {
		return segment.WriteBlock(block, h.Codec, int64(len(logical)), entries)
	})
}
