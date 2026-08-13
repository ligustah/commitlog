package commitlog

import (
	"bufio"
	"bytes"
	"encoding/hex"
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
	descriptorKey = "log-descriptor"
	// descriptorFileV1 is the only descriptor format: the one this build writes
	// and the only one it reads.
	//
	// V0 — the same file without the optional identity line — was read for one
	// release so v0.79.x directories kept opening across the v0.80.0 upgrade.
	// Both downstream repos confirmed they hold no v0.79.x data worth keeping
	// ("every dir is created by a test or soak run and thrown away with it"), so
	// it went in v0.82.0 rather than becoming permanent. Pre-v1, a format this
	// package reads but never writes is a branch with no live input.
	//
	// The version line stays, and is not ceremony. set() refuses an unknown key
	// on purpose, so a build handed a file from a NEWER writer would otherwise
	// report "unknown descriptor field" — technically loud, but it names a field
	// instead of a version and reads like corruption. The version line turns that
	// into "this file is newer than me", which is the thing that is actually
	// true. That value is about future formats and does not depend on any past
	// one still being readable.
	descriptorFileV1 = 1
	// maxDescriptorBytes bounds what readStoreDescriptor will allocate for an
	// object the store claims is the descriptor. Equal to bufio.Scanner's
	// default maximum token, which is what parseDescriptor reads with — so it
	// is the size past which no descriptor can parse anyway, and not a limit
	// invented for this check. See readStoreDescriptor.
	maxDescriptorBytes = 64 << 10
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
	// Identity is the caller's opaque stamp; see Options.Identity. Deliberately
	// absent from enforced(): it does not gate the open, because a caller whose
	// identity disagrees still needs the log open to do anything about it.
	Identity []byte
}

func descriptorFromOptions(opts Options) descriptor {
	return descriptor{
		Compact:                   opts.Compact,
		CompactMinAge:             opts.CompactMinAge,
		CompactTombstoneRetention: opts.CompactTombstoneRetention,
		Compression:               opts.Compression,
		MaxSegmentBytes:           opts.MaxSegmentBytes,
		Identity:                  opts.Identity,
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
	// The size steering this allocation is the STORE's answer, and nothing has
	// verified it — the same shape as any length field read before the thing it
	// describes. A remote store reporting a large object here allocates it
	// entirely into memory before a single byte is parsed, during New, in the
	// caller's process.
	//
	// The bound is derived rather than picked. parseDescriptor reads with a
	// bufio.Scanner at its default 64KiB maximum token, so a descriptor holding
	// any line longer than that cannot parse whatever else is true of it; and a
	// descriptor is one short line per field. Reading past this point can only
	// ever end in a parse error, so refusing early costs nothing that could have
	// succeeded.
	//
	// The local path has no equivalent because it never had the bug: it hands
	// the open file to the same scanner and streams, so an enormous file on disk
	// fails on the first oversized token without being read into memory.
	if size > maxDescriptorBytes {
		return descriptor{}, errors.Errorf(
			"commitlog: log descriptor in store is %d bytes, over the %d-byte maximum",
			size, maxDescriptorBytes)
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
	if version != descriptorFileV1 {
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
	case "identity":
		// Hex because the bytes are the CALLER's and this file is line-based:
		// an identity containing a newline or an "=" would otherwise write a
		// descriptor that does not parse back, turning a caller's choice of
		// bytes into an unopenable log. Hex has no such byte, and an odd or
		// non-hex value is a parse error rather than a silent truncation.
		d.Identity, err = hex.DecodeString(value)
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
	fmt.Fprintf(&b, "%d\n", descriptorFileV1)
	fmt.Fprintf(&b, "compact=%t\n", d.Compact)
	fmt.Fprintf(&b, "compact_min_age=%s\n", d.CompactMinAge)
	fmt.Fprintf(&b, "compact_tombstone_retention=%s\n", d.CompactTombstoneRetention)
	fmt.Fprintf(&b, "compression=%s\n", d.Compression)
	fmt.Fprintf(&b, "max_segment_bytes=%d\n", d.MaxSegmentBytes)
	// Omitted entirely when unset, so a caller that does not use identity gets
	// the same file it always did. An empty line would round-trip to []byte{}
	// rather than nil, which is a distinction identityConflict would then have
	// to know about for no benefit.
	if len(d.Identity) > 0 {
		fmt.Fprintf(&b, "identity=%s\n", hex.EncodeToString(d.Identity))
	}
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
	if len(opts.Tiers) > 0 {
		// Asked of every tier, and any answer settles it. A node adopting ONE
		// tier of a chain has that tier's descriptor and not the others', and
		// treating it as a new log because the first store it looked in was
		// empty is the silent adoption this mechanism exists to prevent.
		for _, t := range opts.Tiers {
			_, err := readStoreDescriptor(t.Store)
			if os.IsNotExist(err) {
				continue
			}
			// An unreadable descriptor is not an absent one. Reporting "new"
			// here would overwrite it with the caller's options.
			return false, nil
		}
		return true, nil
	}
	entries, err := os.ReadDir(opts.Path)
	if err != nil {
		return false, errors.Wrap(err, "read log directory")
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, logFileSuffix) {
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
// It also returns the identity disagreement, if any, for the caller to report
// through IdentityConflict. A conflict is NOT an error and NOT written back:
// see Options.Identity for why both of those would be worse than reporting it.
func reconcileDescriptor(opts Options, isNew bool) (*IdentityConflict, error) {
	want := descriptorFromOptions(opts)
	if isNew || opts.AdoptOptions {
		return nil, publishDescriptor(opts, want)
	}
	got, err := loadDescriptor(opts)
	if err != nil {
		if os.IsNotExist(err) {
			// The log exists and its identity does not. Nothing here can
			// reconstruct it, and adopting whatever the caller happens to pass
			// is exactly the behaviour this prevents, so it is the same refusal
			// as a mismatch and takes the same deliberate opt-in.
			return nil, errors.Wrapf(ErrDescriptorMismatch,
				"%s has no descriptor; set AdoptOptions to record the "+
					"settings it should have been created with", descriptorHome(opts))
		}
		return nil, err
	}
	if !got.enforced(want) {
		return nil, errors.Wrapf(ErrDescriptorMismatch, "%s: %s",
			descriptorHome(opts), got.describeDifference(want))
	}
	conflict := identityConflict(got.Identity, want.Identity)
	// Agreed on what gates. Keep the non-gating fields current so the descriptor
	// still describes the log after a legitimate compression or segment-size
	// change.
	//
	// What gets published is the STORED record with those two fields refreshed
	// on top — never `want`, whose Identity is the CALLER's. Publishing `want`
	// makes the same field wrong in both directions, and the two look nothing
	// alike from here:
	//
	//   - a caller with a DIFFERENT identity re-stamps the log and destroys the
	//     disagreement this open just found — adopt-on-open by the back door;
	//   - a caller with NO identity publishes an empty one, which renders as no
	//     identity line at all and ERASES a stamp someone else relies on. That
	//     is the worse half: identity exists to stop unstamped copies existing,
	//     and this manufactures one.
	//
	// Neither is reachable once the record is built from `got`, because the
	// stored identity is then carried by construction rather than by a
	// condition somebody has to remember to extend. The gating fields are equal
	// here (enforced passed), so `got` and `want` differ only in the two fields
	// refreshed below and the one that must not be taken.
	fresh := got
	fresh.Compression = want.Compression
	fresh.MaxSegmentBytes = want.MaxSegmentBytes
	// The conflict gate stays, for a reason that outlives the erase: while the
	// caller and the log disagree about what this log IS, the caller's opinion
	// about how to encode it is not one to act on either.
	if conflict == nil &&
		(got.Compression != want.Compression || got.MaxSegmentBytes != want.MaxSegmentBytes) {
		return nil, publishDescriptor(opts, fresh)
	}
	return conflict, nil
}

// IdentityConflict reports that a log was opened with an Options.Identity that
// disagrees with the one stored beside it: the caller believes these bytes
// belong to one of its entities, and the log says they belong to another.
//
// Stored is nil when the log carries no identity at all — a log created before
// its caller used the feature, or by a caller that does not. That is a
// different fact from "belongs to someone else" and is kept distinguishable,
// because the two warrant opposite actions: unidentified data may still be the
// caller's own, while data stamped for another owner is not.
type IdentityConflict struct {
	// Stored is the identity found beside the log, or nil if it had none.
	Stored []byte
	// Opened is the Options.Identity this open was given.
	Opened []byte
}

// identityConflict compares a stored identity with the one the caller opened
// with, returning nil when there is nothing to report.
//
// A caller that passes no identity conflicts with nothing, whatever is stored.
// The feature is opt-in, and a process that does not use identity has no
// opinion to disagree with — reporting one would make every existing tool that
// opens a stamped log start receiving a conflict it cannot interpret.
func identityConflict(stored, opened []byte) *IdentityConflict {
	if len(opened) == 0 || bytes.Equal(stored, opened) {
		return nil
	}
	return &IdentityConflict{Stored: stored, Opened: opened}
}

// descriptorHome names the place the descriptor was read from, for an error
// message. A store-backed log's local directory is often a scratch path this
// process picked — "log at C:\tmp\x9f31" reads as a fault in a directory nobody
// cares about, when the disagreement is with the tier the whole cluster shares.
func descriptorHome(opts Options) string {
	if len(opts.Tiers) > 0 {
		return fmt.Sprintf("the tier behind the log at %s", opts.Path)
	}
	return fmt.Sprintf("log at %s", opts.Path)
}

// loadDescriptor reads the log's identity from wherever this log keeps it.
func loadDescriptor(opts Options) (descriptor, error) {
	// The nearest tier holds the identity the caller is checked against. The
	// others are checked by carrying the same descriptor, not by being asked
	// separately: two tiers of one log disagreeing about what the log IS means
	// one store was attached to the wrong log, and that is a refusal rather
	// than something to reconcile — see reconcileDescriptor.
	for _, t := range opts.Tiers {
		return readStoreDescriptor(t.Store)
	}
	return readDescriptor(opts.Path)
}

// publishDescriptor writes it back to the same place.
//
// A log that does not own a tier does not write to it — that is what
// Tier.ReadOnly means, and a descriptor is not an exception to it. Such a
// process is a follower on that tier: it has already been checked against
// whatever the owner published, and if the owner published nothing there is
// nothing for it to disagree with. Silently declining to write is right here in
// a way it would not be for segment data, because the descriptor is a claim
// about the log rather than part of it.
func publishDescriptor(opts Options, d descriptor) error {
	if len(opts.Tiers) == 0 {
		return writeDescriptor(opts.Path, d)
	}
	// Every tier it owns, because every tier must be able to say which log it
	// belongs to. A store that cannot is not self-describing, and a node
	// adopting it alone would have nothing to be checked against. Skipping the
	// read-only ones individually rather than the whole chain: a node that owns
	// its hot tier and not the archive must still describe the tier it owns.
	for _, t := range opts.Tiers {
		if t.ReadOnly {
			continue
		}
		if err := writeStoreDescriptor(t.Store, d); err != nil {
			return err
		}
	}
	return nil
}
