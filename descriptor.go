package commitlog

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ligustah/commitlog/compress"
	atomic_file "github.com/natefinch/atomic"
	"github.com/pkg/errors"
)

const (
	// descriptorFileName is where a log's identity lives when it has no
	// SegmentStore: beside its segments, in its own directory.
	descriptorFileName = "log-descriptor"
	// descriptorKey is where it lives when the log HAS a store — an object in
	// the store, beside the tier manifest.
	//
	// These are two locations for one fact, but never at the same time: a log
	// consults exactly one of them, chosen by whether it has a store. The
	// choice follows from what each place can answer. A store-backed log's data
	// outlives any particular directory — that is what a tier is for — so a
	// process that has the store and not the directory has the log, and it must
	// be able to ask what the log IS from the same place it asks what the log
	// HOLDS. The directory can only answer for logs that live entirely in it.
	descriptorKey    = "log-descriptor"
	descriptorFileV0 = 0
)

// ErrDescriptorMismatch is returned by New when the log was created with
// compaction settings that disagree with the ones passed, or when it exists and
// has no descriptor at all. Both mean the same thing: the caller and the log
// disagree about what the log IS, and continuing would silently apply a
// retention policy the log was not created with.
//
// Resolve it deliberately with AdoptOptions, which rewrites the descriptor to
// match. That is a retune — "I know what this log is, record it" — and it is
// the answer to both shapes, since neither can be settled from what is on disk.
var ErrDescriptorMismatch = errors.New("commitlog: options disagree with the log's descriptor")

// descriptor is the record of what a log IS, kept beside the thing that owns
// its data — the segments for a plain log, the tier manifest for a store-backed
// one. It exists because compaction behaviour otherwise lives only in the
// Options a caller happens to pass at open time, so reopening a log with
// different — or absent — options silently changes what gets deleted. The zero
// values of the compaction settings mean NO protection rather than "disabled",
// which makes an accidentally empty config maximally destructive: that is the
// failure this turns into an error.
//
// It is a human-readable sidecar in the style of the existing
// leader-epoch-checkpoint and replication-offset-checkpoint files, so a log can
// be identified without the code that created it — which is worth as much in a
// store as it is in a directory.
type descriptor struct {
	Compact                   bool
	CompactMinAge             time.Duration
	CompactTombstoneRetention time.Duration
	Compression               compress.Codec
	MaxSegmentBytes           int64
}

func descriptorFromOptions(opts Options) descriptor {
	return descriptor{
		Compact:                   opts.Compact,
		CompactMinAge:             opts.CompactMinAge,
		CompactTombstoneRetention: opts.CompactTombstoneRetention,
		Compression:               opts.Compression,
		MaxSegmentBytes:           opts.MaxSegmentBytes,
	}
}

// enforced reports whether d and other agree on the settings that GATE an open:
// the ones whose disagreement silently destroys data.
//
// Compression and MaxSegmentBytes are deliberately not among them. Both can
// change safely on an existing log — segments keep whatever format and size
// they were written with — so gating on them would refuse opens that are
// entirely correct. They are recorded to describe the log, not to police it.
func (d descriptor) enforced(other descriptor) bool {
	return d.Compact == other.Compact &&
		d.CompactMinAge == other.CompactMinAge &&
		d.CompactTombstoneRetention == other.CompactTombstoneRetention
}

// describeDifference renders the disagreeing enforced settings for an error
// message, so a caller sees which knob is wrong rather than just that one is.
func (d descriptor) describeDifference(other descriptor) string {
	var diffs []string
	if d.Compact != other.Compact {
		diffs = append(diffs, fmt.Sprintf("compact: log has %t, options have %t",
			d.Compact, other.Compact))
	}
	if d.CompactMinAge != other.CompactMinAge {
		diffs = append(diffs, fmt.Sprintf("compact_min_age: log has %s, options have %s",
			d.CompactMinAge, other.CompactMinAge))
	}
	if d.CompactTombstoneRetention != other.CompactTombstoneRetention {
		diffs = append(diffs, fmt.Sprintf(
			"compact_tombstone_retention: log has %s, options have %s",
			d.CompactTombstoneRetention, other.CompactTombstoneRetention))
	}
	return strings.Join(diffs, "; ")
}

func descriptorPath(path string) string {
	return filepath.Join(path, descriptorFileName)
}

// readDescriptor loads the descriptor a log with no store keeps in its
// directory. A missing file returns os.ErrNotExist so the caller can tell "this
// log has no descriptor" from "a descriptor exists and is unreadable" — the
// first is answerable with AdoptOptions, the second is corruption and must not
// be papered over.
func readDescriptor(path string) (descriptor, error) {
	f, err := os.Open(descriptorPath(path))
	if err != nil {
		return descriptor{}, err
	}
	defer f.Close() // nolint: errcheck
	return parseDescriptor(f)
}

// readStoreDescriptor loads the descriptor a store-backed log published into its
// store. A store with no descriptor object returns os.ErrNotExist, so the caller
// tells "this store has never held a log" from "the descriptor is unreadable"
// exactly as it does for the file.
//
// Absence must be ASSERTED by the store, via ErrObjectNotFound. Any other
// failure is propagated, because the caller acts on absence: logIsNew turns it
// into "this log is new", and a new log records the settings it was handed
// without checking them. A store that timed out would otherwise let a node
// adopting someone else's tier install a retention policy nobody configured.
func readStoreDescriptor(store SegmentStore) (descriptor, error) {
	size, err := store.Size(descriptorKey)
	if errors.Is(err, ErrObjectNotFound) {
		return descriptor{}, os.ErrNotExist
	}
	if err != nil {
		return descriptor{}, errors.Wrap(err, "stat log descriptor in store")
	}
	if size <= 0 {
		return descriptor{}, errors.New("commitlog: log descriptor in store is empty")
	}
	body := make([]byte, size)
	if _, err := store.ReadAt(descriptorKey, body, 0); err != nil {
		return descriptor{}, errors.Wrap(err, "read log descriptor from store")
	}
	return parseDescriptor(bytes.NewReader(body))
}

func parseDescriptor(r io.Reader) (descriptor, error) {
	var d descriptor
	scanner := bufio.NewScanner(r)
	if !scanner.Scan() {
		return d, errors.New("descriptor is empty")
	}
	version, err := strconv.Atoi(strings.TrimSpace(scanner.Text()))
	if err != nil {
		return d, errors.Wrap(err, "parse descriptor version")
	}
	if version != descriptorFileV0 {
		return d, errors.Errorf("unsupported descriptor version %d", version)
	}
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return d, errors.Errorf("malformed descriptor line %q", line)
		}
		if err := d.set(strings.TrimSpace(key), strings.TrimSpace(value)); err != nil {
			return d, err
		}
	}
	if err := scanner.Err(); err != nil {
		return d, errors.Wrap(err, "read descriptor")
	}
	return d, nil
}

// set applies one key/value pair. An unknown key is an error, and a known key
// with an unparseable value is too — both mean this file is not a descriptor
// this build wrote.
//
// Tolerating an unknown key would buy forward-readability and cost the three
// cases where being told is the whole value of reading the file: a typo, a
// half-written line, and a key whose name changed all look exactly like a field
// from the future. The version line on the first line is what makes a real
// format change detectable, and that is the right place for it.
func (d *descriptor) set(key, value string) error {
	var err error
	switch key {
	case "compact":
		d.Compact, err = strconv.ParseBool(value)
	case "compact_min_age":
		d.CompactMinAge, err = time.ParseDuration(value)
	case "compact_tombstone_retention":
		d.CompactTombstoneRetention, err = time.ParseDuration(value)
	case "compression":
		d.Compression, err = compress.Parse(value)
	case "max_segment_bytes":
		d.MaxSegmentBytes, err = strconv.ParseInt(value, 10, 64)
	default:
		return errors.Errorf("unknown descriptor field %q", key)
	}
	if err != nil {
		return errors.Wrapf(err, "parse descriptor field %q", key)
	}
	return nil
}

// renderDescriptor is the on-the-wire form, shared by both places it can be
// stored so the two cannot drift into writing different files.
func renderDescriptor(d descriptor) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d\n", descriptorFileV0)
	fmt.Fprintf(&b, "compact=%t\n", d.Compact)
	fmt.Fprintf(&b, "compact_min_age=%s\n", d.CompactMinAge)
	fmt.Fprintf(&b, "compact_tombstone_retention=%s\n", d.CompactTombstoneRetention)
	fmt.Fprintf(&b, "compression=%s\n", d.Compression)
	fmt.Fprintf(&b, "max_segment_bytes=%d\n", d.MaxSegmentBytes)
	return b.String()
}

// writeDescriptor persists d atomically, so a crash mid-write never leaves a
// log with a torn descriptor it would then refuse to open.
func writeDescriptor(path string, d descriptor) error {
	body := renderDescriptor(d)
	if err := atomic_file.WriteFile(descriptorPath(path), strings.NewReader(body)); err != nil {
		return errors.Wrap(err, "write log descriptor")
	}
	return nil
}

// writeStoreDescriptor publishes d into the store, where a process that has the
// store and not the directory can read it.
//
// No atomic_file wrapper here, because the store already promises what that
// wrapper buys: Put is required to leave the object fully present or absent and
// never half-written — FileSegmentStore writes a temp file and renames, and an
// object store's PUT is atomic per object. That promise is load-bearing for
// segment objects already, and it is the same promise this needs.
func writeStoreDescriptor(store SegmentStore, d descriptor) error {
	body := []byte(renderDescriptor(d))
	if err := store.Put(descriptorKey, bytes.NewReader(body), int64(len(body))); err != nil {
		return errors.Wrap(err, "put log descriptor")
	}
	return nil
}

// logIsNew reports whether this log exists yet, which is what distinguishes
// "create" from "open" — New has a single entry point for both, and a new log
// simply records what it was created with instead of being checked against
// something that isn't there.
//
// It asks whatever owns the log's data. For a store-backed log that is the
// store: a published descriptor means the log exists, whatever this particular
// directory happens to contain. For a log with no store it is the directory,
// and a log exists there as soon as it has a segment — the other leftovers a
// directory may hold (checkpoints, working copies) do not make one.
//
// The distinction matters because the two answers disagree in exactly the case
// that matters. A node ADOPTING a tier has a store full of segments and an
// empty directory. Asked of the directory, it is a new log, and a new log skips
// the check entirely — so the one moment a process is picking up someone else's
// log is the one moment its retention settings were never compared. Asked of
// the store, it is an existing log, and it gets checked.
func logIsNew(opts Options) (bool, error) {
	if opts.SegmentStore != nil {
		_, err := readStoreDescriptor(opts.SegmentStore)
		if os.IsNotExist(err) {
			return true, nil
		}
		// An unreadable descriptor is not an absent one. Reporting "new" here
		// would overwrite it with the caller's options, which is the silent
		// adoption this whole mechanism exists to prevent.
		return false, nil
	}
	entries, err := os.ReadDir(opts.Path)
	if err != nil {
		return false, errors.Wrap(err, "read log directory")
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, logFileSuffix) || strings.HasSuffix(name, offloadedSuffix) {
			return false, nil
		}
	}
	return true, nil
}

// reconcileDescriptor settles the log's descriptor against the caller's Options
// before anything is opened, and is the whole point of the feature: it is where
// "the caller and the log disagree about what this log is" becomes an error
// instead of silent data loss.
//
// isNew reports whether the log existed — a genuinely new one simply records
// what it was created with.
func reconcileDescriptor(opts Options, isNew bool) error {
	want := descriptorFromOptions(opts)
	if isNew || opts.AdoptOptions {
		return publishDescriptor(opts, want)
	}
	got, err := loadDescriptor(opts)
	if err != nil {
		if os.IsNotExist(err) {
			// The log exists and its identity does not. Nothing here can
			// reconstruct it, and adopting whatever the caller happens to pass
			// is exactly the behaviour this prevents, so it is the same refusal
			// as a mismatch and takes the same deliberate opt-in.
			return errors.Wrapf(ErrDescriptorMismatch,
				"%s has no descriptor; set AdoptOptions to record the "+
					"settings it should have been created with", descriptorHome(opts))
		}
		return err
	}
	if !got.enforced(want) {
		return errors.Wrapf(ErrDescriptorMismatch, "%s: %s",
			descriptorHome(opts), got.describeDifference(want))
	}
	// Agreed on what gates. Keep the non-gating fields current so the descriptor
	// still describes the log after a legitimate compression or segment-size
	// change.
	if got.Compression != want.Compression || got.MaxSegmentBytes != want.MaxSegmentBytes {
		return publishDescriptor(opts, want)
	}
	return nil
}

// descriptorHome names the place the descriptor was read from, for an error
// message. A store-backed log's local directory is often a scratch path this
// process picked — "log at C:\tmp\x9f31" reads as a fault in a directory nobody
// cares about, when the disagreement is with the tier the whole cluster shares.
func descriptorHome(opts Options) string {
	if opts.SegmentStore != nil {
		return fmt.Sprintf("the tier behind the log at %s", opts.Path)
	}
	return fmt.Sprintf("log at %s", opts.Path)
}

// loadDescriptor reads the log's identity from wherever this log keeps it.
func loadDescriptor(opts Options) (descriptor, error) {
	if opts.SegmentStore != nil {
		return readStoreDescriptor(opts.SegmentStore)
	}
	return readDescriptor(opts.Path)
}

// publishDescriptor writes it back to the same place.
//
// A log that does not own its tier does not write to it — that is what
// TierReadOnly means, and a descriptor is not an exception to it. Such a process
// is a follower: it has already been checked against whatever the owner
// published, and if the owner published nothing there is nothing for it to
// disagree with. Silently declining to write is right here in a way it would not
// be for segment data, because the descriptor is a claim about the log rather
// than part of it.
func publishDescriptor(opts Options, d descriptor) error {
	if opts.SegmentStore == nil {
		return writeDescriptor(opts.Path, d)
	}
	if opts.TierReadOnly {
		return nil
	}
	return writeStoreDescriptor(opts.SegmentStore, d)
}
