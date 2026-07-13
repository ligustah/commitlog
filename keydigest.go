package commitlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"

	"github.com/pkg/errors"
)

// A key digest is a per-segment sidecar (<base>.keys, alongside the offset
// index) summarizing everything compaction needs to know about a SEALED
// segment without reading its records: every keyed data record's offset,
// tombstone flag, header presence and timestamp — sorted by key so cleans can
// stream a k-way merge across segments instead of materializing a global
// latest-per-key map — plus the unkeyed data offsets, control-marker offsets
// and leader-epoch assignments that the clean's remove/strip/skip decisions
// depend on.
//
// Validity is bound to the segment's .log byte size: any rewrite (Replace,
// Trimmed) changes it, so a stale digest can never describe replaced content.
// A missing or invalid digest is rebuilt from a segment scan; it is never
// required for correctness, only to avoid the scan.

const (
	keysSuffix = ".keys"

	digestMagic   uint32 = 0x434C4B44 // "CLKD"
	digestVersion byte   = 1

	digestFlagTombstone  byte = 1 << 0
	digestFlagHasHeaders byte = 1 << 1
)

// digestRec is one data record's entry in a digest.
type digestRec struct {
	offset int64
	flags  byte
	ts     int64
}

type digestEpoch struct {
	epoch       uint64
	firstOffset int64
}

type keyDigest struct {
	base    int64
	logSize int64
	baseTs  int64
	// Strip stamp: a previous clean scanned this segment and verified that
	// stripping stripHdrs off records below stripVerifiedBelow changes
	// nothing (the headers present are not the strip targets). Lets the skip
	// decision trust hasHeaders records without rescanning every pass.
	stripVerifiedBelow int64
	stripHdrs          []string

	epochs  []digestEpoch
	keyed   []byte // encoded keyed section, entries sorted by key
	nKeys   int
	unkeyed []digestRec // offsets ascending
	control []int64     // offsets ascending
}

func digestPath(seg *segment) string {
	return filepath.Join(seg.path, fmt.Sprintf(fileFormat, seg.BaseOffset, keysSuffix))
}

// stripStampCovers reports whether the digest's strip stamp proves that
// stripping hdrs below boundary is a no-op for this segment. The cleaner
// stamps MaxInt64 when a scan found no strippable headers anywhere in the
// segment, else the StripBelow it verified under.
func (d *keyDigest) stripStampCovers(boundary int64, hdrs []string) bool {
	if d.stripVerifiedBelow < boundary {
		return false
	}
	if len(hdrs) != len(d.stripHdrs) {
		return false
	}
	for i, h := range hdrs {
		if d.stripHdrs[i] != h {
			return false
		}
	}
	return true
}

// ---- building ----

// buildKeyDigest scans a segment and produces its digest. The scan is the
// same pass the pre-digest cleaner made; it runs once per sealed segment
// (or after a rewrite) instead of on every clean.
func buildKeyDigest(seg *segment) (*keyDigest, error) {
	d := &keyDigest{
		base:               seg.BaseOffset,
		logSize:            seg.Position(),
		baseTs:             seg.FirstWriteTime(),
		stripVerifiedBelow: -1,
	}
	type keyRecs struct {
		key  []byte
		recs []digestRec
	}
	var (
		byKey     = make(map[string]*keyRecs)
		lastEpoch = uint64(0)
		haveEpoch = false
		ss        = newSegmentScanner(seg)
	)
	for ms, _, err := ss.Scan(); err == nil; ms, _, err = ss.Scan() {
		var (
			offset = ms.Offset()
			msg    = ms.Message()
			attrs  = msg.Attributes()
		)
		if le := ms.LeaderEpoch(); !haveEpoch || le > lastEpoch {
			d.epochs = append(d.epochs, digestEpoch{epoch: le, firstOffset: offset})
			lastEpoch, haveEpoch = le, true
		}
		if attrs&AttrControl != 0 {
			d.control = append(d.control, offset)
			continue
		}
		rec := digestRec{offset: offset, ts: ms.Timestamp()}
		if attrs&AttrTombstone != 0 {
			rec.flags |= digestFlagTombstone
		}
		if len(msg.Headers()) > 0 {
			rec.flags |= digestFlagHasHeaders
		}
		key := msg.Key()
		if key == nil {
			d.unkeyed = append(d.unkeyed, rec)
			continue
		}
		kr, ok := byKey[string(key)]
		if !ok {
			kr = &keyRecs{key: append([]byte(nil), key...)}
			byKey[string(key)] = kr
		}
		kr.recs = append(kr.recs, rec)
	}

	sorted := make([]*keyRecs, 0, len(byKey))
	for _, kr := range byKey {
		sorted = append(sorted, kr)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i].key, sorted[j].key) < 0
	})

	var buf bytes.Buffer
	var scratch [binary.MaxVarintLen64]byte
	putUvarint := func(v uint64) {
		n := binary.PutUvarint(scratch[:], v)
		buf.Write(scratch[:n])
	}
	putVarint := func(v int64) {
		n := binary.PutVarint(scratch[:], v)
		buf.Write(scratch[:n])
	}
	for _, kr := range sorted {
		putUvarint(uint64(len(kr.key)))
		buf.Write(kr.key)
		putUvarint(uint64(len(kr.recs)))
		for _, r := range kr.recs {
			putUvarint(uint64(r.offset - d.base))
			buf.WriteByte(r.flags)
			putVarint(r.ts - d.baseTs)
		}
	}
	d.keyed = buf.Bytes()
	d.nKeys = len(sorted)
	return d, nil
}

// ---- encoding ----

func encodeKeyDigest(d *keyDigest) []byte {
	var buf bytes.Buffer
	var scratch [binary.MaxVarintLen64]byte
	putUvarint := func(v uint64) {
		n := binary.PutUvarint(scratch[:], v)
		buf.Write(scratch[:n])
	}
	putVarint := func(v int64) {
		n := binary.PutVarint(scratch[:], v)
		buf.Write(scratch[:n])
	}
	var fixed [8]byte
	putU32 := func(v uint32) { encoding.PutUint32(fixed[:4], v); buf.Write(fixed[:4]) }
	putI64 := func(v int64) { encoding.PutUint64(fixed[:], uint64(v)); buf.Write(fixed[:8]) }

	putU32(digestMagic)
	buf.WriteByte(digestVersion)
	putI64(d.base)
	putI64(d.logSize)
	putI64(d.baseTs)
	putI64(d.stripVerifiedBelow)
	putUvarint(uint64(len(d.stripHdrs)))
	for _, h := range d.stripHdrs {
		putUvarint(uint64(len(h)))
		buf.WriteString(h)
	}
	putUvarint(uint64(len(d.epochs)))
	for _, e := range d.epochs {
		putUvarint(e.epoch)
		putVarint(e.firstOffset - d.base)
	}
	putUvarint(uint64(d.nKeys))
	putUvarint(uint64(len(d.keyed)))
	buf.Write(d.keyed)
	putUvarint(uint64(len(d.unkeyed)))
	for _, r := range d.unkeyed {
		putUvarint(uint64(r.offset - d.base))
		buf.WriteByte(r.flags)
		putVarint(r.ts - d.baseTs)
	}
	putUvarint(uint64(len(d.control)))
	for _, off := range d.control {
		putUvarint(uint64(off - d.base))
	}
	putU32(crc32.ChecksumIEEE(buf.Bytes()))
	return buf.Bytes()
}

// writeKeyDigest persists the digest atomically next to the segment files.
func writeKeyDigest(seg *segment, d *keyDigest) error {
	data := encodeKeyDigest(d)
	path := digestPath(seg)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0666); err != nil {
		return errors.Wrap(err, "write key digest")
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp) // nolint: errcheck
		return errors.Wrap(err, "install key digest")
	}
	return nil
}

func removeKeyDigest(seg *segment) {
	os.Remove(digestPath(seg)) // nolint: errcheck — best-effort litter removal
}

// ---- decoding ----

type digestDecoder struct {
	data []byte
	pos  int
	err  error
}

func (r *digestDecoder) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.data[r.pos:])
	if n <= 0 {
		r.err = errIndexCorrupt
		return 0
	}
	r.pos += n
	return v
}

func (r *digestDecoder) varint() int64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Varint(r.data[r.pos:])
	if n <= 0 {
		r.err = errIndexCorrupt
		return 0
	}
	r.pos += n
	return v
}

func (r *digestDecoder) bytes(n int) []byte {
	if r.err != nil {
		return nil
	}
	if r.pos+n > len(r.data) {
		r.err = errIndexCorrupt
		return nil
	}
	b := r.data[r.pos : r.pos+n]
	r.pos += n
	return b
}

func (r *digestDecoder) u32() uint32 {
	b := r.bytes(4)
	if r.err != nil {
		return 0
	}
	return encoding.Uint32(b)
}

func (r *digestDecoder) i64() int64 {
	b := r.bytes(8)
	if r.err != nil {
		return 0
	}
	return int64(encoding.Uint64(b))
}

func (r *digestDecoder) byte() byte {
	b := r.bytes(1)
	if r.err != nil {
		return 0
	}
	return b[0]
}

// loadKeyDigest reads and validates the segment's digest. Returns nil (no
// error) when the digest is missing, corrupt, or does not bind to the
// segment's current content — the caller falls back to building one.
func loadKeyDigest(seg *segment) *keyDigest {
	data, err := os.ReadFile(digestPath(seg))
	if err != nil {
		return nil
	}
	if len(data) < 4+1+8*4+4 {
		return nil
	}
	body, crcBytes := data[:len(data)-4], data[len(data)-4:]
	if crc32.ChecksumIEEE(body) != encoding.Uint32(crcBytes) {
		return nil
	}
	r := &digestDecoder{data: body}
	if r.u32() != digestMagic || r.byte() != digestVersion {
		return nil
	}
	d := &keyDigest{}
	d.base = r.i64()
	d.logSize = r.i64()
	d.baseTs = r.i64()
	d.stripVerifiedBelow = r.i64()
	if d.base != seg.BaseOffset || d.logSize != seg.Position() {
		return nil // content changed since build (rewrite/trim) — stale
	}
	nHdrs := r.uvarint()
	if nHdrs > 64 {
		return nil
	}
	d.stripHdrs = make([]string, 0, nHdrs)
	for i := uint64(0); i < nHdrs; i++ {
		hl := r.uvarint()
		d.stripHdrs = append(d.stripHdrs, string(r.bytes(int(hl))))
	}
	nEpochs := r.uvarint()
	if r.err != nil || nEpochs > uint64(len(body)) {
		return nil
	}
	if nEpochs > 0 {
		d.epochs = make([]digestEpoch, 0, nEpochs)
	}
	for i := uint64(0); i < nEpochs; i++ {
		e := r.uvarint()
		off := r.varint() + d.base
		d.epochs = append(d.epochs, digestEpoch{epoch: e, firstOffset: off})
	}
	d.nKeys = int(r.uvarint())
	keyedLen := r.uvarint()
	d.keyed = r.bytes(int(keyedLen))
	nUnkeyed := r.uvarint()
	if r.err != nil || nUnkeyed > uint64(len(body)) {
		return nil
	}
	if nUnkeyed > 0 {
		d.unkeyed = make([]digestRec, 0, nUnkeyed)
	}
	for i := uint64(0); i < nUnkeyed; i++ {
		off := int64(r.uvarint()) + d.base
		flags := r.byte()
		ts := r.varint() + d.baseTs
		d.unkeyed = append(d.unkeyed, digestRec{offset: off, flags: flags, ts: ts})
	}
	nControl := r.uvarint()
	if r.err != nil || nControl > uint64(len(body)) {
		return nil
	}
	if nControl > 0 {
		d.control = make([]int64, 0, nControl)
	}
	for i := uint64(0); i < nControl; i++ {
		d.control = append(d.control, int64(r.uvarint())+d.base)
	}
	if r.err != nil {
		return nil
	}
	return d
}

// ---- iteration (keyed section) ----

// digestIter streams a digest's keyed entries in key order. key and recs are
// valid until the next call to next().
type digestIter struct {
	d    *digestDecoder
	base int64
	bTs  int64
	rem  int

	key  []byte
	recs []digestRec
}

func newDigestIter(d *keyDigest) *digestIter {
	return &digestIter{
		d:    &digestDecoder{data: d.keyed},
		base: d.base,
		bTs:  d.baseTs,
		rem:  d.nKeys,
	}
}

func (it *digestIter) next() bool {
	if it.rem == 0 || it.d.err != nil {
		return false
	}
	it.rem--
	klen := it.d.uvarint()
	it.key = it.d.bytes(int(klen))
	n := it.d.uvarint()
	it.recs = it.recs[:0]
	for i := uint64(0); i < n; i++ {
		off := int64(it.d.uvarint()) + it.base
		flags := it.d.byte()
		ts := it.d.varint() + it.bTs
		it.recs = append(it.recs, digestRec{offset: off, flags: flags, ts: ts})
	}
	return it.d.err == nil
}
