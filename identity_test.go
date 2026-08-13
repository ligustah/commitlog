package commitlog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// openWithIdentity opens (or creates) a log at dir stamped with id.
func openWithIdentity(t *testing.T, dir string, id []byte, adopt bool) CommitLog {
	t.Helper()
	l, err := New(Options{
		Name: "identity", Path: dir, MaxSegmentBytes: 1 << 20,
		Identity: id, AdoptOptions: adopt,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// The whole reason Options.Identity exists: the stamp must be on disk by the
// time New returns, with no window in which the log exists unidentified.
//
// A caller stamping after New has that window, and a crash inside it leaves
// bytes nothing identifies — a state that cannot be cleaned up later, because
// an unstamped copy and a stale one look the same and only one of them should
// be destroyed. That is what durable_streams was carrying before this.
//
// Asserted against the descriptor file rather than through the API, because the
// claim is specifically about what is DURABLE, and a value held in memory would
// satisfy any check that went back through the log.
func TestIdentityIsOnDiskAsSoonAsTheLogExists(t *testing.T) {
	dir := tempDir(t)
	openWithIdentity(t, dir, []byte("stream-7"), false)

	body, err := os.ReadFile(filepath.Join(dir, descriptorFileName))
	require.NoError(t, err, "a created log must have a descriptor")
	require.Contains(t, string(body), "identity="+"73747265616d2d37",
		"the identity was not written with the descriptor")
}

func TestReopeningWithTheSameIdentityIsNotAConflict(t *testing.T) {
	dir := tempDir(t)
	l := openWithIdentity(t, dir, []byte("stream-7"), false)
	require.NoError(t, l.Close())

	l2 := openWithIdentity(t, dir, []byte("stream-7"), false)
	require.Nil(t, l2.IdentityConflict())
}

// A caller that does not use identity must not start receiving conflicts from
// logs someone else stamped: it has no opinion to disagree with.
func TestOpeningAStampedLogWithNoIdentityIsNotAConflict(t *testing.T) {
	dir := tempDir(t)
	l := openWithIdentity(t, dir, []byte("stream-7"), false)
	require.NoError(t, l.Close())

	l2 := openWithIdentity(t, dir, nil, false)
	require.Nil(t, l2.IdentityConflict())
}

// The signal durable_streams asked for: a mismatch is REPORTED, and the log is
// open and usable while it is reported. Refusing the open would take a
// partition offline over bookkeeping.
func TestADifferentIdentityIsReportedAndTheLogStillOpens(t *testing.T) {
	dir := tempDir(t)
	l := openWithIdentity(t, dir, []byte("stream-7"), false)
	_, err := l.Append([]*Message{{Value: []byte("a")}})
	require.NoError(t, err)
	require.NoError(t, l.Close())

	l2 := openWithIdentity(t, dir, []byte("stream-9"), false)
	c := l2.IdentityConflict()
	require.NotNil(t, c, "reopening under a different identity must be reported")
	require.Equal(t, []byte("stream-7"), c.Stored)
	require.Equal(t, []byte("stream-9"), c.Opened)

	// Usable, not merely constructed.
	_, err = l2.Append([]*Message{{Value: []byte("b")}})
	require.NoError(t, err, "a conflict must not disable the log")
}

// The point of not writing the conflict back. A signal consumed at open time is
// lost to a crash immediately after, which moves the window rather than closing
// it — so the disagreement has to still be there on the next open, and the one
// after that.
func TestAConflictSurvivesEveryReopenUntilItIsResolved(t *testing.T) {
	dir := tempDir(t)
	l := openWithIdentity(t, dir, []byte("stream-7"), false)
	require.NoError(t, l.Close())

	for attempt := range 3 {
		l2 := openWithIdentity(t, dir, []byte("stream-9"), false)
		c := l2.IdentityConflict()
		require.NotNil(t, c, "conflict vanished on reopen %d: it was consumed", attempt)
		require.Equal(t, []byte("stream-7"), c.Stored,
			"the stored identity was overwritten on reopen %d", attempt)
		require.NoError(t, l2.Close())
	}

	// AdoptOptions is the deliberate resolution, and the only one.
	l3 := openWithIdentity(t, dir, []byte("stream-9"), true)
	require.Nil(t, l3.IdentityConflict())
	require.NoError(t, l3.Close())

	l4 := openWithIdentity(t, dir, []byte("stream-9"), false)
	require.Nil(t, l4.IdentityConflict(), "adopting did not stick")
}

// The back door: reconcileDescriptor republishes the descriptor when a
// non-gating field changed, and the descriptor it republishes carries the
// CALLER's identity. Without the conflict guard, opening a conflicted log while
// also changing the codec re-stamps it and destroys the disagreement — an
// adopt-on-open that only fires on the subset of opens that retune something
// else, which is the hardest kind to ever notice.
func TestAConflictIsNotErasedByAnUnrelatedSettingChange(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{
		Name: "identity", Path: dir, MaxSegmentBytes: 1 << 20,
		Identity: []byte("stream-7"),
	})
	require.NoError(t, err)
	require.NoError(t, l.Close())

	// Same log, different identity AND a different segment size, which is a
	// legitimate non-gating change that triggers the republish.
	l2, err := New(Options{
		Name: "identity", Path: dir, MaxSegmentBytes: 2 << 20,
		Identity: []byte("stream-9"),
	})
	require.NoError(t, err)
	c := l2.IdentityConflict()
	require.NotNil(t, c, "the republish swallowed the conflict")
	require.Equal(t, []byte("stream-7"), c.Stored)
	require.NoError(t, l2.Close())

	body, err := os.ReadFile(filepath.Join(dir, descriptorFileName))
	require.NoError(t, err)
	require.Contains(t, string(body), "identity="+"73747265616d2d37",
		"the stored identity was overwritten by the republish")
}

// An unidentified log is a different fact from one belonging to someone else,
// and they warrant opposite actions: unidentified data may still be the
// caller's, data stamped for another owner is not. Stored == nil is how a
// caller tells them apart.
func TestAnUnstampedLogReportsAConflictWithNoStoredIdentity(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{Name: "identity", Path: dir, MaxSegmentBytes: 1 << 20})
	require.NoError(t, err)
	_, err = l.Append([]*Message{{Value: []byte("a")}})
	require.NoError(t, err)
	require.NoError(t, l.Close())

	l2 := openWithIdentity(t, dir, []byte("stream-9"), false)
	c := l2.IdentityConflict()
	require.NotNil(t, c)
	require.Nil(t, c.Stored, "an unstamped log must be distinguishable from a differently-stamped one")
	require.Equal(t, []byte("stream-9"), c.Opened)
}

// The identity is the CALLER's bytes and the descriptor is line-based, so a
// caller choosing a newline or an "=" must not be able to write a descriptor
// that will not parse back — which would turn a legal choice of bytes into an
// unopenable log.
func TestAnIdentityWithFileFormatBytesRoundTrips(t *testing.T) {
	dir := tempDir(t)
	nasty := []byte("a=b\nc\r\nmax_segment_bytes=1\x00\xff")
	l := openWithIdentity(t, dir, nasty, false)
	require.NoError(t, l.Close())

	l2 := openWithIdentity(t, dir, nasty, false)
	require.Nil(t, l2.IdentityConflict(), "the identity did not survive the round trip")
	require.NoError(t, l2.Close())

	l3 := openWithIdentity(t, dir, []byte("other"), false)
	require.Equal(t, nasty, l3.IdentityConflict().Stored)
}

// A descriptor written before identity existed carries no version line for it
// and must still open — that is what makes the field additive rather than a
// migration. The log simply has no identity, which is exactly what it means.
func TestAVersion0DescriptorStillOpens(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{Name: "identity", Path: dir, MaxSegmentBytes: 1 << 20})
	require.NoError(t, err)
	require.NoError(t, l.Close())

	// Rewrite the descriptor in the old format, exactly as an older build left
	// it: version 0 and no identity line.
	path := filepath.Join(dir, descriptorFileName)
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	old := strings.Replace(string(body), "1\n", "0\n", 1)
	require.NotEqual(t, string(body), old, "the fixture did not actually downgrade the version")
	require.NoError(t, os.WriteFile(path, []byte(old), 0644))

	l2, err := New(Options{Name: "identity", Path: dir, MaxSegmentBytes: 1 << 20})
	require.NoError(t, err, "a v0 descriptor must still open")
	require.Nil(t, l2.IdentityConflict())
	require.NoError(t, l2.Close())
}
