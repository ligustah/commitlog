package commitlog

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

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

// A log created before descriptors existed has none. Silently adopting whatever
// the caller passes is precisely the behaviour being removed, so it is the same
// refusal as a mismatch — and the same opt-in resolves it.
func TestDescriptorRefusesLogWithoutOne(t *testing.T) {
	dir := tempDir(t)
	l, err := New(compactedOpts(dir))
	require.NoError(t, err)
	appendOne(t, l)
	require.NoError(t, l.Close())

	// Reduce it to a pre-descriptor log.
	require.NoError(t, os.Remove(filepath.Join(dir, descriptorFileName)))

	_, err = New(compactedOpts(dir))
	require.ErrorIs(t, err, ErrDescriptorMismatch)
	require.Contains(t, err.Error(), "AdoptOptions", "the error must say how to resolve it")
}

// AdoptOptions is the deliberate resolution for both refusals: the migration of
// a log that has no descriptor, and a genuine retune of one that does.
func TestAdoptOptionsResolvesBothRefusals(t *testing.T) {
	t.Run("migration", func(t *testing.T) {
		dir := tempDir(t)
		l, err := New(compactedOpts(dir))
		require.NoError(t, err)
		appendOne(t, l)
		require.NoError(t, l.Close())
		require.NoError(t, os.Remove(filepath.Join(dir, descriptorFileName)))

		opts := compactedOpts(dir)
		opts.AdoptOptions = true
		l2, err := New(opts)
		require.NoError(t, err)
		require.NoError(t, l2.Close())

		// And the log is now normally openable without the opt-in.
		l3, err := New(compactedOpts(dir))
		require.NoError(t, err, "the migration must leave a log that opens plainly")
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

// A descriptor that exists but is unreadable is corruption, not a migration,
// and must not be silently overwritten by the caller's options.
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

// An unknown key from a newer writer is ignored rather than rejected, so a
// descriptor stays readable across versions; a known key with a bad value is
// still corruption.
func TestDescriptorToleratesUnknownKeysButNotBadValues(t *testing.T) {
	dir := tempDir(t)
	require.NoError(t, os.WriteFile(filepath.Join(dir, descriptorFileName),
		[]byte("0\ncompact=true\nsomething_new=42\n"), 0666))
	d, err := readDescriptor(dir)
	require.NoError(t, err, "an unknown key must not break an older reader")
	require.True(t, d.Compact)

	require.NoError(t, os.WriteFile(filepath.Join(dir, descriptorFileName),
		[]byte("0\ncompact_min_age=not-a-duration\n"), 0666))
	_, err = readDescriptor(dir)
	require.Error(t, err)
}
