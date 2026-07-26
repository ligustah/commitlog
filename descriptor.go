package commitlog

import (
	"bufio"
	"fmt"
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
	descriptorFileName = "log-descriptor"
	descriptorFileV0   = 0
)

// ErrDescriptorMismatch is returned by New when the log on disk was created
// with compaction settings that disagree with the ones passed, or when it
// predates the descriptor and so has none. Both mean the same thing: the caller
// and the log disagree about what the log IS, and continuing would silently
// apply a retention policy the log was not created with.
//
// Resolve it deliberately with AdoptOptions, which rewrites the descriptor to
// match — that is both the retune path and the one-time migration path for a
// log created before descriptors existed.
var ErrDescriptorMismatch = errors.New("commitlog: options disagree with the log's descriptor")

// descriptor is the on-disk record of what a log IS, written into the log
// directory beside the segments. It exists because compaction behaviour
// otherwise lives only in the Options a caller happens to pass at open time, so
// reopening a directory with different — or absent — options silently changes
// what gets deleted. The zero values of the compaction settings mean NO
// protection rather than "disabled", which makes an accidentally empty config
// maximally destructive: that is the failure this turns into an error.
//
// It is a human-readable sidecar in the style of the existing
// leader-epoch-checkpoint and replication-offset-checkpoint files, so a
// directory can be identified without the code that created it.
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

// readDescriptor loads the log's descriptor. A missing file returns
// os.ErrNotExist so the caller can tell "this log predates descriptors" from "a
// descriptor exists and is unreadable" — the first is a migration, the second
// is corruption and must not be papered over.
func readDescriptor(path string) (descriptor, error) {
	var d descriptor
	f, err := os.Open(descriptorPath(path))
	if err != nil {
		return d, err
	}
	defer f.Close() // nolint: errcheck

	scanner := bufio.NewScanner(f)
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

// set applies one key/value pair. An UNKNOWN key is ignored rather than
// rejected, so a descriptor written by a newer version stays readable by an
// older one; a known key with an unparseable value is still an error, since
// that is corruption rather than a version skew.
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
		return nil
	}
	if err != nil {
		return errors.Wrapf(err, "parse descriptor field %q", key)
	}
	return nil
}

// writeDescriptor persists d atomically, so a crash mid-write never leaves a
// log with a torn descriptor it would then refuse to open.
func writeDescriptor(path string, d descriptor) error {
	var b strings.Builder
	fmt.Fprintf(&b, "%d\n", descriptorFileV0)
	fmt.Fprintf(&b, "compact=%t\n", d.Compact)
	fmt.Fprintf(&b, "compact_min_age=%s\n", d.CompactMinAge)
	fmt.Fprintf(&b, "compact_tombstone_retention=%s\n", d.CompactTombstoneRetention)
	fmt.Fprintf(&b, "compression=%s\n", d.Compression)
	fmt.Fprintf(&b, "max_segment_bytes=%d\n", d.MaxSegmentBytes)
	if err := atomic_file.WriteFile(descriptorPath(path), strings.NewReader(b.String())); err != nil {
		return errors.Wrap(err, "write log descriptor")
	}
	return nil
}

// logIsNew reports whether the directory holds no log yet, which is what
// distinguishes "create" from "open" — New has a single entry point for both.
// A log exists as soon as it has a segment, local or offloaded; the leftovers a
// directory may also contain (checkpoints, working copies) do not make one.
func logIsNew(path string) (bool, error) {
	entries, err := os.ReadDir(path)
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
// isNew reports whether the directory held no log yet — a genuinely new log
// simply records what it was created with.
func reconcileDescriptor(opts Options, isNew bool) error {
	want := descriptorFromOptions(opts)
	if isNew || opts.AdoptOptions {
		return writeDescriptor(opts.Path, want)
	}
	got, err := readDescriptor(opts.Path)
	if err != nil {
		if os.IsNotExist(err) {
			// A log from before descriptors existed. Adopting whatever the caller
			// happens to pass is exactly the behaviour being removed, so this is
			// the same refusal as a mismatch — resolved with the same opt-in.
			return errors.Wrapf(ErrDescriptorMismatch,
				"log at %s predates the descriptor; set AdoptOptions to record the "+
					"settings it should have been created with", opts.Path)
		}
		return err
	}
	if !got.enforced(want) {
		return errors.Wrapf(ErrDescriptorMismatch, "log at %s: %s",
			opts.Path, got.describeDifference(want))
	}
	// Agreed on what gates. Keep the non-gating fields current so the file still
	// describes the log after a legitimate compression or segment-size change.
	if got.Compression != want.Compression || got.MaxSegmentBytes != want.MaxSegmentBytes {
		return writeDescriptor(opts.Path, want)
	}
	return nil
}
