package commitlog

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
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
// latest-per-key map — plus the unkeyed data offsets and control-marker
// offsets that the clean's remove/strip/skip decisions depend on.
//
// Validity is bound to the segment's .log byte size: any rewrite (Replace,
// Trimmed) changes it, so a stale digest can never describe replaced content.
// A missing or invalid digest is rebuilt from a segment scan; it is never
// required for correctness, only to avoid the scan.

const (
	keysSuffix = ".keys"

	digestMagic uint32 = 0x434C4B44 // "CLKD"
	// v2 dropped the leader-epoch section: nothing read it once compaction
	// stopped rebuilding the epoch cache from record stamps (see
	// CleanWithSpec). A version a reader does not recognize is not an error —
	// loadKeyDigest returns nil and the digest is rebuilt from a scan.
	digestVersion byte = 2

	digestFlagTombstone  byte = 1 << 0
	digestFlagHasHeaders byte = 1 << 1
)

// digestRec is one data record's entry in a digest.
type digestRec struct {
	offset int64
	flags  byte
	ts     int64
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

	// The keyed section (entries sorted by key) lives in exactly one of two
	// places: freshly built digests hold the encoded bytes in `keyed`; loaded
	// sidecars record only the section's file position (path/keyedOff/
	// keyedLen) and iteration streams it from disk, so a clean never holds
	// every segment's keys in memory at once.
	keyed    []byte
	path     string
	keyedOff int64
	keyedLen int
	nKeys    int
	unkeyed  []digestRec // offsets ascending
	control  []int64     // offsets ascending
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
func buildKeyDigest(seg *segment, sc *blockCache) (*keyDigest, error) {
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
		byKey = make(map[string]*keyRecs)
		ss    = newSegmentScannerCache(seg, sc)
	)
	defer ss.Close()
	for {
		ms, _, err := ss.Scan()
		if err != nil {
			// A digest that covers only what the scan could reach is worse than
			// no digest: it is written down, trusted on every later tick, and
			// says of the records past the damage that they are not in this
			// segment — which is the input a rewrite uses to decide what may be
			// dropped.
			if !errors.Is(err, io.EOF) {
				return nil, fmt.Errorf("%w: digest for segment %d: %w",
					ErrSegmentUnreadable, seg.BaseOffset, err)
			}
			break
		}
		var (
			offset = ms.Offset()
			msg    = ms.Message()
			attrs  = msg.Attributes()
		)
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

	var w digestWriter
	for _, kr := range sorted {
		w.uvarint(uint64(len(kr.key)))
		w.buf.Write(kr.key)
		w.uvarint(uint64(len(kr.recs)))
		for _, r := range kr.recs {
			w.rec(d, r)
		}
	}
	d.keyed = w.buf.Bytes()
	d.nKeys = len(sorted)
	return d, nil
}

// ---- encoding ----

// digestWriter is the digest's encoder: the varint and fixed-width writes, and
// the scratch array they share.
//
// A type rather than the closures that were here because there were two sets of
// them, in buildKeyDigest and encodeKeyDigest, character for character. Both
// write the same format — the keyed section one builds is a run of exactly the
// records the other appends for the unkeyed ones — so an encoding that differed
// between them would be a digest that decodes wrong, and the only thing keeping
// them equal was that nobody had edited one.
type digestWriter struct {
	buf     bytes.Buffer
	scratch [binary.MaxVarintLen64]byte
	fixed   [8]byte
}

func (w *digestWriter) uvarint(v uint64) {
	n := binary.PutUvarint(w.scratch[:], v)
	w.buf.Write(w.scratch[:n])
}

func (w *digestWriter) varint(v int64) {
	n := binary.PutVarint(w.scratch[:], v)
	w.buf.Write(w.scratch[:n])
}

func (w *digestWriter) u32(v uint32) {
	encoding.PutUint32(w.fixed[:4], v)
	w.buf.Write(w.fixed[:4])
}

func (w *digestWriter) i64(v int64) {
	encoding.PutUint64(w.fixed[:], uint64(v))
	w.buf.Write(w.fixed[:8])
}

// rec writes one record entry: offset and timestamp DELTAS against the digest's
// bases, with the flag byte between them. Both sections encode a record this
// way, and the decoder reads them with one routine, so this is the third place
// that agreement has to hold rather than a convenience.
func (w *digestWriter) rec(d *keyDigest, r digestRec) {
	w.uvarint(uint64(r.offset - d.base))
	w.buf.WriteByte(r.flags)
	w.varint(r.ts - d.baseTs)
}

func encodeKeyDigest(d *keyDigest) []byte {
	var w digestWriter
	w.u32(digestMagic)
	w.buf.WriteByte(digestVersion)
	w.i64(d.base)
	w.i64(d.logSize)
	w.i64(d.baseTs)
	w.i64(d.stripVerifiedBelow)
	w.uvarint(uint64(len(d.stripHdrs)))
	for _, h := range d.stripHdrs {
		w.uvarint(uint64(len(h)))
		w.buf.WriteString(h)
	}
	w.uvarint(uint64(d.nKeys))
	w.uvarint(uint64(len(d.keyed)))
	w.buf.Write(d.keyed)
	w.uvarint(uint64(len(d.unkeyed)))
	for _, r := range d.unkeyed {
		w.rec(d, r)
	}
	w.uvarint(uint64(len(d.control)))
	for _, off := range d.control {
		w.uvarint(uint64(off - d.base))
	}
	w.u32(crc32.ChecksumIEEE(w.buf.Bytes()))
	return w.buf.Bytes()
}

// writeKeyDigest persists the digest atomically next to the segment files.
//
// The rename is the publisher's end of the Windows window openWithRetry covers
// from the reader's end: anything holding the destination open — the scanner,
// the indexer, a reader of the previous digest that has not been reaped — makes
// the rename itself fail with "Access is denied", and retrying only the readers
// moves the error to the writer rather than removing it.
//
// tickWriteRetryBudget, not the caller-waited one, because a lost digest is
// free: every caller treats it as best-effort and buildKeyDigest regenerates it
// from the segment when it is absent. Five seconds of a compaction pass spent
// on a file it can rebuild would be paying the caller's price for the tick's
// failure.
func writeKeyDigest(seg *segment, d *keyDigest) error {
	data := encodeKeyDigest(d)
	path := digestPath(seg)
	tmp := path + tmpSuffix
	if err := os.WriteFile(tmp, data, 0666); err != nil {
		return errors.Wrap(err, "write key digest")
	}
	if err := renameWithin(tmp, path, tickWriteRetryBudget); err != nil {
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
	d.nKeys = int(r.uvarint())
	keyedLen := r.uvarint()
	if r.err != nil || keyedLen > uint64(len(body)) {
		return nil
	}
	// Don't retain the keyed bytes — remember where they live and stream
	// them at merge time. CRC over the whole file was already verified, and
	// the sidecar is immutable for the duration of a clean (single cleaner;
	// rewrites replace the file only after the merge).
	d.path = digestPath(seg)
	d.keyedOff = int64(r.pos)
	d.keyedLen = int(keyedLen)
	r.bytes(int(keyedLen))
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
// valid until the next call to next(). Freshly built digests iterate their
// in-memory bytes; loaded sidecars stream the keyed section from disk, so a
// merge across N segments holds N read buffers, not N keyed sections.
type digestIter struct {
	r    digestByteReader
	f    *os.File
	rerr error
	base int64
	bTs  int64
	rem  int

	key    []byte
	keyBuf []byte
	recs   []digestRec
}

// digestByteReader is what digestIter needs from either source: a
// bytes.Reader over a freshly built digest's in-memory keyed section, or a
// bufio.Reader streaming a loaded sidecar's from disk.
type digestByteReader interface {
	io.Reader
	io.ByteReader
}

func newDigestIter(d *keyDigest) (*digestIter, error) {
	it := &digestIter{base: d.base, bTs: d.baseTs, rem: d.nKeys}
	if d.keyed != nil || d.path == "" || d.nKeys == 0 {
		it.r = bytes.NewReader(d.keyed)
		return it, nil
	}
	f, err := os.Open(d.path)
	if err != nil {
		return nil, errors.Wrap(err, "open key digest for merge")
	}
	if _, err := f.Seek(d.keyedOff, io.SeekStart); err != nil {
		f.Close() // nolint: errcheck
		return nil, errors.Wrap(err, "seek key digest keyed section")
	}
	it.f = f
	// 8KB per reader: the k-way merge holds one iterator PER SEGMENT
	// simultaneously, so this buffer multiplies by the segment count —
	// run 31's anomaly capture measured 79MB of these across ~1200
	// state-WAL segments at 64KB each. Digest entries are tens of bytes;
	// 8KB still amortizes syscalls fine.
	it.r = bufio.NewReaderSize(f, 8<<10)
	return it, nil
}

// close releases the sidecar file handle. Must run before the clean rewrites
// any digest: Windows won't rename over a file that is still open.
func (it *digestIter) close() {
	if it.f != nil {
		it.f.Close() // nolint: errcheck — read-only handle
		it.f = nil
	}
}

// err reports a read/decode failure. Exhaustion-by-error is safe for the
// merge (unseen copies are merely not dropped) but callers surface it so a
// clean never silently under-compacts.
func (it *digestIter) err() error { return it.rerr }

func (it *digestIter) uvarint() uint64 {
	if it.rerr != nil {
		return 0
	}
	v, err := binary.ReadUvarint(it.r)
	if err != nil {
		it.rerr = err
		return 0
	}
	return v
}

func (it *digestIter) varint() int64 {
	if it.rerr != nil {
		return 0
	}
	v, err := binary.ReadVarint(it.r)
	if err != nil {
		it.rerr = err
		return 0
	}
	return v
}

func (it *digestIter) readByte() byte {
	if it.rerr != nil {
		return 0
	}
	b, err := it.r.ReadByte()
	if err != nil {
		it.rerr = err
		return 0
	}
	return b
}

func (it *digestIter) readKey(n int) []byte {
	if it.rerr != nil {
		return nil
	}
	if cap(it.keyBuf) < n {
		it.keyBuf = make([]byte, n)
	}
	it.keyBuf = it.keyBuf[:n]
	if _, err := io.ReadFull(it.r, it.keyBuf); err != nil {
		it.rerr = err
		return nil
	}
	return it.keyBuf
}

func (it *digestIter) next() bool {
	if it.rem == 0 || it.err() != nil {
		return false
	}
	it.rem--
	klen := it.uvarint()
	if klen > 1<<24 { // CRC-verified data can't sanely reach this; guard the alloc
		it.rerr = errIndexCorrupt
		return false
	}
	it.key = it.readKey(int(klen))
	n := it.uvarint()
	it.recs = it.recs[:0]
	for i := uint64(0); i < n; i++ {
		off := int64(it.uvarint()) + it.base
		flags := it.readByte()
		ts := it.varint() + it.bTs
		it.recs = append(it.recs, digestRec{offset: off, flags: flags, ts: ts})
	}
	return it.err() == nil
}
