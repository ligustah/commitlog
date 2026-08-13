package commitlog

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// Every field of the descriptor survives a render and a parse.
//
// renderDescriptor and set() are an enumeration of the struct's fields, kept in
// two places, by hand — the same shape as the reserved-name lists the sidecar
// prefix replaced, and it rots the same way. Every other test in this file goes
// through New and Options, so each covers only the fields it happens to set;
// none of them can notice a field that persists nowhere.
//
// The three ways it breaks, and what each one does:
//
//   - in the struct, not in renderDescriptor: silently not persisted. The
//     reopen then reads back a zero value for it, so if it is one of the fields
//     reconcileDescriptor keeps current, "the log disagrees with the caller" is
//     true on EVERY open and the descriptor is rewritten every time.
//   - in the struct, not in set(): renderDescriptor writes a key its own reader
//     refuses by design, so the build that wrote the file cannot open it.
//   - in both, wrong format either side: the value comes back changed.
//
// Completeness of the fixture is checked by REFLECTION and its values are
// written by hand, because those need different things. Reflection is what
// makes a newly added field appear here without anyone remembering to add it;
// hand-written values are what keep each one legal — Compression has to be a
// codec compress.Parse accepts, and a generic "set it to something non-zero"
// would render a value that fails to parse and report the wrong defect.
func TestADescriptorRoundTripsEveryField(t *testing.T) {
	full := descriptor{
		Compact:                   true,
		CompactMinAge:             90 * time.Minute,
		CompactTombstoneRetention: 36 * time.Hour,
		Compression:               compress.Zstd,
		MaxSegmentBytes:           12345,
		Identity:                  []byte{0xde, 0xad, 0xbe, 0xef},
	}

	v := reflect.ValueOf(full)
	for i := range v.NumField() {
		require.False(t, v.Field(i).IsZero(),
			"descriptor.%s is at its zero value in this fixture, so the round trip below "+
				"cannot tell whether it is persisted at all — give it a distinctive value, "+
				"and add it to renderDescriptor and set() if it is not there yet",
			v.Type().Field(i).Name)
	}

	got, err := parseDescriptor(strings.NewReader(renderDescriptor(full)))
	require.NoError(t, err, "the descriptor this build writes is one it cannot read")
	require.Equal(t, full, got, "a descriptor field did not survive the round trip")
}

// Every disagreement enforced() refuses, describeDifference names.
//
// The two methods enumerate the same fields, separately — the third copy of the
// descriptor's field list in this file, after renderDescriptor and set(). A
// field added to enforced() and not to describeDifference is not a crash: the
// open is refused with ErrDescriptorMismatch and an EMPTY explanation, so the
// caller is told their options disagree with the log and not which knob. That
// error is the one thing standing between a caller and a silently different
// retention policy, and it is read by a human at exactly the moment they have
// the least context.
//
// Found the same way as the two lists the sidecar prefix replaced, and derived
// the same way: perturb one field of a full descriptor at a time, ask enforced()
// whether it cares, and require an explanation whenever it does. The fields come
// from the struct, so a new one arrives here without anyone adding it.
func TestEveryEnforcedDisagreementIsNamed(t *testing.T) {
	base := descriptor{
		Compact:                   true,
		CompactMinAge:             90 * time.Minute,
		CompactTombstoneRetention: 36 * time.Hour,
		Compression:               compress.Zstd,
		MaxSegmentBytes:           12345,
		Identity:                  []byte{0xde, 0xad, 0xbe, 0xef},
	}

	enforcedFields := 0
	bt := reflect.TypeOf(base)
	for i := range bt.NumField() {
		name := bt.Field(i).Name
		other := base
		field := reflect.ValueOf(&other).Elem().Field(i)
		switch field.Kind() {
		case reflect.Bool:
			field.SetBool(!field.Bool())
		case reflect.Int, reflect.Int64:
			field.SetInt(field.Int() + 1)
		case reflect.Uint, reflect.Uint8:
			field.SetUint(field.Uint() + 1)
		case reflect.Slice:
			field.SetBytes([]byte{0x01})
		default:
			// Loud rather than skipped. A field this test does not know how to
			// perturb is a field it silently reports nothing about, which is the
			// exact failure it exists to catch, one level up.
			t.Fatalf("descriptor.%s has kind %s and this test does not know how to "+
				"change it, so it is asserting nothing about that field", name, field.Kind())
		}

		if base.enforced(other) {
			continue // Not a gating field; enforced() is entitled not to care.
		}
		enforcedFields++
		require.NotEmpty(t, base.describeDifference(other),
			"descriptor.%s gates the open — enforced() refuses a log that disagrees "+
				"about it — but describeDifference says nothing, so New returns "+
				"ErrDescriptorMismatch with an empty explanation and the caller is not "+
				"told which knob is wrong", name)
	}

	// enforced() comparing nothing would make every field skip the assertion and
	// leave this green over an empty loop.
	require.GreaterOrEqual(t, enforcedFields, 3,
		"only %d descriptor fields gate the open; enforced() is comparing less than it "+
			"should, or this fixture no longer differs where it needs to", enforcedFields)
}

// appendOne writes a record so the directory holds a real log.
func appendOne(t *testing.T, l CommitLog) {
	t.Helper()
	_, err := l.Append([]*Message{{Key: []byte("k"), Value: []byte("v")}})
	require.NoError(t, err)
}

// compactedOpts is the shape the incident involved: a compacted log whose
// horizons are the only thing standing between it and destructive compaction.
func compactedOpts(dir string) Options {
	return Options{
		Path:                      dir,
		Compact:                   true,
		CompactMinAge:             time.Hour,
		CompactTombstoneRetention: 24 * time.Hour,
	}
}

// A new log records what it was created with, and reopening with the same
// options is unremarkable.
func TestDescriptorWrittenOnCreateAndAcceptedOnReopen(t *testing.T) {
	dir := tempDir(t)
	l, err := New(compactedOpts(dir))
	require.NoError(t, err)
	appendOne(t, l)
	require.NoError(t, l.Close())

	require.FileExists(t, filepath.Join(dir, descriptorFileName))
	d, err := readDescriptor(dir)
	require.NoError(t, err)
	require.True(t, d.Compact)
	require.Equal(t, time.Hour, d.CompactMinAge)
	require.Equal(t, 24*time.Hour, d.CompactTombstoneRetention)

	l2, err := New(compactedOpts(dir))
	require.NoError(t, err, "reopening with the same options must just work")
	require.NoError(t, l2.Close())
}

// The incident itself: reopening a compacted log with an EMPTY config. The zero
// values mean no horizon and no tombstone retention, so compaction would run
// unprotected and delete records the caller still needs. It must refuse.
func TestDescriptorRefusesReopenWithEmptyConfig(t *testing.T) {
	dir := tempDir(t)
	l, err := New(compactedOpts(dir))
	require.NoError(t, err)
	appendOne(t, l)
	require.NoError(t, l.Close())

	_, err = New(Options{Path: dir})
	require.ErrorIs(t, err, ErrDescriptorMismatch,
		"a compacted log reopened with no config is the data-loss case")
}

// Each enforced setting gates independently.
func TestDescriptorRefusesEachEnforcedMismatch(t *testing.T) {
	for name, mutate := range map[string]func(*Options){
		"compact":                     func(o *Options) { o.Compact = false },
		"compact_min_age":             func(o *Options) { o.CompactMinAge = 2 * time.Hour },
		"compact_tombstone_retention": func(o *Options) { o.CompactTombstoneRetention = time.Minute },
	} {
		t.Run(name, func(t *testing.T) {
			dir := tempDir(t)
			l, err := New(compactedOpts(dir))
			require.NoError(t, err)
			appendOne(t, l)
			require.NoError(t, l.Close())

			opts := compactedOpts(dir)
			mutate(&opts)
			_, err = New(opts)
			require.ErrorIs(t, err, ErrDescriptorMismatch)
			require.Contains(t, err.Error(), name,
				"the error must name the setting that disagrees")
		})
	}
}

// Settings that can change safely on an existing log must NOT gate: segments
// keep the format and size they were written with, so refusing here would block
// opens that are entirely correct. The descriptor follows the change.
func TestDescriptorAllowsCompressionAndSegmentSizeChange(t *testing.T) {
	dir := tempDir(t)
	opts := compactedOpts(dir)
	opts.MaxSegmentBytes = 1024
	l, err := New(opts)
	require.NoError(t, err)
	appendOne(t, l)
	require.NoError(t, l.Close())

	changed := compactedOpts(dir)
	changed.MaxSegmentBytes = 4096
	changed.Compression = compress.Zstd
	l2, err := New(changed)
	require.NoError(t, err, "compression and segment size may change on an existing log")
	require.NoError(t, l2.Close())

	d, err := readDescriptor(dir)
	require.NoError(t, err)
	require.Equal(t, compress.Zstd, d.Compression, "the descriptor must follow the change")
	require.EqualValues(t, 4096, d.MaxSegmentBytes)
}

// A log that exists and has no descriptor cannot say what it is, and nothing on
// disk can reconstruct it. Silently adopting whatever the caller passes is
// precisely the behaviour being removed, so it is the same refusal as a
// mismatch — and the same opt-in resolves it.
func TestDescriptorRefusesLogWithoutOne(t *testing.T) {
	dir := tempDir(t)
	l, err := New(compactedOpts(dir))
	require.NoError(t, err)
	appendOne(t, l)
	require.NoError(t, l.Close())

	// Take its identity away and leave its data.
	require.NoError(t, os.Remove(filepath.Join(dir, descriptorFileName)))

	_, err = New(compactedOpts(dir))
	require.ErrorIs(t, err, ErrDescriptorMismatch)
	require.Contains(t, err.Error(), "AdoptOptions", "the error must say how to resolve it")
}

// AdoptOptions is the deliberate resolution for both refusals: a log that has
// no descriptor, and a genuine retune of one that does. It says the same thing
// in both — "I know what this log is, record it" — which is why one switch
// answers both.
func TestAdoptOptionsResolvesBothRefusals(t *testing.T) {
	t.Run("no descriptor", func(t *testing.T) {
		dir := tempDir(t)
		l, err := New(compactedOpts(dir))
		require.NoError(t, err)
		appendOne(t, l)
		require.NoError(t, l.Close())
		require.NoError(t, os.Remove(filepath.Join(dir, descriptorFileName)))

		// Establish that it IS refused first. Without this the subtest passes in
		// a world where nothing is enforced at all — it would only be showing
		// that AdoptOptions opens a log, which proves nothing about the refusal
		// it is named for.
		_, err = New(compactedOpts(dir))
		require.ErrorIs(t, err, ErrDescriptorMismatch)

		opts := compactedOpts(dir)
		opts.AdoptOptions = true
		l2, err := New(opts)
		require.NoError(t, err)
		require.NoError(t, l2.Close())

		// And the log is now normally openable without the opt-in.
		l3, err := New(compactedOpts(dir))
		require.NoError(t, err, "adopting must leave a log that opens plainly")
		require.NoError(t, l3.Close())
	})

	t.Run("retune", func(t *testing.T) {
		dir := tempDir(t)
		l, err := New(compactedOpts(dir))
		require.NoError(t, err)
		appendOne(t, l)
		require.NoError(t, l.Close())

		retuned := compactedOpts(dir)
		retuned.CompactMinAge = 6 * time.Hour
		retuned.AdoptOptions = true
		l2, err := New(retuned)
		require.NoError(t, err)
		require.NoError(t, l2.Close())

		d, err := readDescriptor(dir)
		require.NoError(t, err)
		require.Equal(t, 6*time.Hour, d.CompactMinAge)

		// The OLD settings are now the mismatch.
		_, err = New(compactedOpts(dir))
		require.ErrorIs(t, err, ErrDescriptorMismatch)
	})
}

// A descriptor that exists but is unreadable is corruption, not an absence, and
// must not be silently overwritten by the caller's options.
func TestDescriptorCorruptionIsNotTreatedAsMissing(t *testing.T) {
	dir := tempDir(t)
	l, err := New(compactedOpts(dir))
	require.NoError(t, err)
	appendOne(t, l)
	require.NoError(t, l.Close())

	require.NoError(t, os.WriteFile(filepath.Join(dir, descriptorFileName),
		[]byte("not a descriptor\n"), 0666))

	_, err = New(compactedOpts(dir))
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrDescriptorMismatch,
		"corruption is a different failure from a disagreement")
}

// A key this build does not know is an error, the same as a value it cannot
// parse. Both mean the file is not a descriptor this build wrote.
//
// Unknown keys used to be ignored, so a newer writer stayed readable by an older
// reader. Pre-v1 there is no older reader to keep working, and the tolerance
// covered more than it was aimed at: a typo, a renamed key, and a line mangled
// by a partial write all look like "a field from the future" and were all
// accepted, leaving a descriptor that reads as valid while describing a log
// nobody configured. The version line is what makes a real format change
// detectable, and it stays.
//
// The fixtures carry version 1 deliberately. They read "0" until v0.82.0 dropped
// V0, and leaving them there would have kept this test GREEN for the wrong
// reason: every body would fail on the version check without ever reaching the
// key parsing the test exists to exercise, and require.Error cannot tell those
// apart.
func TestDescriptorRefusesUnknownKeysAndBadValues(t *testing.T) {
	dir := tempDir(t)

	for name, body := range map[string]string{
		"an unknown key":       "1\ncompact=true\nsomething_new=42\n",
		"a typo'd key":         "1\ncompact_min_ag=1h\n",
		"an unparseable value": "1\ncompact_min_age=not-a-duration\n",
	} {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, os.WriteFile(filepath.Join(dir, descriptorFileName),
				[]byte(body), 0666))
			_, err := readDescriptor(dir)
			require.Error(t, err)
		})
	}

	// What this build writes still round-trips, which is the thing the rule
	// above must not break.
	require.NoError(t, os.WriteFile(filepath.Join(dir, descriptorFileName),
		[]byte(renderDescriptor(descriptor{Compact: true, CompactMinAge: time.Hour})), 0666))
	d, err := readDescriptor(dir)
	require.NoError(t, err)
	require.True(t, d.Compact)
	require.Equal(t, time.Hour, d.CompactMinAge)
}
