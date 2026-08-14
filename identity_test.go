package commitlog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// openWithIdentity opens (or creates) a log at dir stamped with id. reStamp
// asks for the deliberate re-stamp — every caller that passes true is resolving
// a conflict, which is the only thing AdoptIdentity is for.
func openWithIdentity(t *testing.T, dir string, id []byte, reStamp bool) CommitLog {
	t.Helper()
	l, err := New(Options{
		Name: "identity", Path: dir, MaxSegmentBytes: 1 << 20,
		Identity: id, AdoptIdentity: reStamp,
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

// The other half of the same republish, and the half that was missed: a caller
// with NO identity. It conflicts with nothing by design — it has no opinion to
// disagree with — so the conflict guard above lets the republish through, and
// the record it published carried the caller's empty identity. renderDescriptor
// omits an empty identity entirely, so the stamp did not become wrong, it
// ceased to exist.
//
// That is worse than the adopt case it sits next to. Options.Identity exists to
// stop unstamped copies from being a state that occurs at all, because
// durable_streams cannot reclaim one: an unstamped copy and a stale one look
// identical and only one should be destroyed. An erase here manufactures
// exactly that, from a log that was correctly stamped, on an open that did
// nothing wrong.
//
// Reachable by any tool that opens a stamped log without using identity and
// retunes a codec or a segment size — a repair utility, a compaction job, a
// different service on the same directory.
func TestAStampSurvivesAnOpenByACallerThatHasNoIdentity(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{
		Name: "identity", Path: dir, MaxSegmentBytes: 1 << 20,
		Identity: []byte("stream-7"),
	})
	require.NoError(t, err)
	require.NoError(t, l.Close())

	// No identity, and a non-gating change so the republish actually fires.
	// Without the change this is TestOpeningAStampedLogWithNoIdentityIsNotA-
	// Conflict, which passes either way because no write path is taken.
	l2, err := New(Options{
		Name: "identity", Path: dir, MaxSegmentBytes: 2 << 20,
	})
	require.NoError(t, err)
	require.Nil(t, l2.IdentityConflict(), "a caller with no identity has nothing to conflict with")
	require.NoError(t, l2.Close())

	body, err := os.ReadFile(filepath.Join(dir, descriptorFileName))
	require.NoError(t, err)
	require.Contains(t, string(body), "identity="+"73747265616d2d37",
		"a caller with no identity erased the stamp by republishing its own empty one")

	// And the refresh still did its job — this must not be fixed by declining
	// to republish at all.
	require.Contains(t, string(body), "max_segment_bytes=2097152",
		"the non-gating refresh stopped happening, which is not the fix")

	// The owner sees no conflict on its next open, which is the outcome that
	// actually matters: an erased stamp reads as Stored == nil, i.e. "these
	// bytes belong to nobody".
	l3 := openWithIdentity(t, dir, []byte("stream-7"), false)
	require.Nil(t, l3.IdentityConflict(), "the owner was told its own log is not its own")
}

// Adopting means "I know what this log is, record it" — a statement about the
// log's SETTINGS. An identity is not one of them, supplied or not.
//
// This began as the erase: AdoptOptions republished the CALLER's record
// wholesale, so a caller with no identity adopting to retune compaction wiped
// the stamp every time. It was first fixed by reading the stored record and
// carrying the identity across, which held for the empty case only. The flags
// are now separate and the branch builds from the STORED record like every
// other, so there is nothing to carry and nothing to remember: adopting cannot
// reach Identity at all. See TestAdoptingSettingsStillReportsAnIdentityConflict
// for the case the carry-over could not cover.
func TestAdoptingWithNoIdentityKeepsTheStoredStamp(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{
		Name: "identity", Path: dir, MaxSegmentBytes: 1 << 20,
		Identity: []byte("stream-7"), CompactMinAge: time.Hour,
	})
	require.NoError(t, err)
	require.NoError(t, l.Close())

	// Retuning a GATING field, which is the case AdoptOptions exists for: it
	// cannot be settled from what is stored, so the caller has to say so.
	l2, err := New(Options{
		Name: "identity", Path: dir, MaxSegmentBytes: 1 << 20,
		CompactMinAge: 2 * time.Hour, AdoptOptions: true,
	})
	require.NoError(t, err)
	require.NoError(t, l2.Close())

	body, err := os.ReadFile(filepath.Join(dir, descriptorFileName))
	require.NoError(t, err)
	require.Contains(t, string(body), "identity="+"73747265616d2d37",
		"adopting with no identity erased the stamp")
	// The adopt still has to have happened — this must not be fixed by
	// declining to publish.
	require.Contains(t, string(body), "compact_min_age=2h0m0s",
		"the adopt stopped taking effect, which is not the fix")

	// The owner's own next open, carrying the settings that were just adopted so
	// this asserts the identity and not the retune.
	l3, err := New(Options{
		Name: "identity", Path: dir, MaxSegmentBytes: 1 << 20,
		CompactMinAge: 2 * time.Hour, Identity: []byte("stream-7"),
	})
	require.NoError(t, err)
	require.Nil(t, l3.IdentityConflict(), "the owner was told its own log is not its own")
	require.NoError(t, l3.Close())
}

// The other half, which must keep working: a caller that has decided the data
// is its own re-stamps the log, and the preservation above must not swallow it.
func TestAdoptIdentityReStamps(t *testing.T) {
	dir := tempDir(t)
	l := openWithIdentity(t, dir, []byte("stream-7"), false)
	require.NoError(t, l.Close())

	l2 := openWithIdentity(t, dir, []byte("stream-9"), true)
	require.NoError(t, l2.Close())
	require.Nil(t, l2.IdentityConflict(),
		"the open that resolved the conflict still reported one")

	body, err := os.ReadFile(filepath.Join(dir, descriptorFileName))
	require.NoError(t, err)
	require.Contains(t, string(body), "identity="+"73747265616d2d39",
		"the deliberate re-stamp through AdoptIdentity stopped working")
}

// The case that made identity unusable for a caller whose settings come from a
// catalog rather than a config file. Such a caller passes AdoptOptions on EVERY
// open — there is no other way to say "the catalog is authoritative about this
// log's settings" — and while the two were one flag, that answered every
// identity question with "no conflict" before anything compared. The signal was
// suppressed by the one thing every open did, so the feature could not be used
// at all by the caller that needed it most.
//
// Adopting settings and adopting an identity are two statements. Making them
// two flags is what lets this open be both: authoritative about the settings,
// and told about the disagreement.
func TestAdoptingSettingsStillReportsAnIdentityConflict(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{
		Name: "identity", Path: dir, MaxSegmentBytes: 1 << 20,
		Identity: []byte("stream-7"), CompactMinAge: time.Hour,
	})
	require.NoError(t, err)
	require.NoError(t, l.Close())

	// A catalog-sourced open: authoritative about the settings, and carrying an
	// identity that turns out to disagree with what is stored.
	l2, err := New(Options{
		Name: "identity", Path: dir, MaxSegmentBytes: 1 << 20,
		CompactMinAge: 2 * time.Hour,
		Identity:      []byte("stream-9"), AdoptOptions: true,
	})
	require.NoError(t, err)
	conflict := l2.IdentityConflict()
	require.NotNil(t, conflict,
		"adopting the settings swallowed the identity disagreement, which is "+
			"the whole defect: this caller adopts on every open")
	require.Equal(t, []byte("stream-7"), conflict.Stored)
	require.Equal(t, []byte("stream-9"), conflict.Opened)
	require.NoError(t, l2.Close())

	// Nothing was written. Not the identity — that would consume the signal a
	// crash could then lose — and not the settings either, because a caller
	// holding the wrong log's identity is a caller whose settings this log has
	// no reason to trust. The conflict is still there on the next open, which
	// is what makes it something a caller can act on when it is ready to.
	body, err := os.ReadFile(filepath.Join(dir, descriptorFileName))
	require.NoError(t, err)
	require.Contains(t, string(body), "identity="+"73747265616d2d37",
		"the stored stamp was overwritten by a caller that was told it disagreed")
	require.Contains(t, string(body), "compact_min_age=1h0m0s",
		"settings were adopted from a caller whose identity disagreed")
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

// A store-backed log keeps its descriptor in the STORE, not in the directory —
// that is the whole point of the store being self-describing, since a process
// holding the store and not the directory still has the log. The identity rides
// the same write, so it has to work there too, and every other test in this file
// exercises only the directory path.
//
// This is also the case that matters most for the feature. A tiered log's data
// outlives any particular directory, so "these bytes belong to a different
// incarnation of the name" is a question a node ADOPTING a tier has to be able
// to ask — and it has no local directory to have stamped.
func TestIdentityRoundTripsThroughTheTierStore(t *testing.T) {
	store, err := NewFileSegmentStore(filepath.Join(tempDir(t), "tier"))
	require.NoError(t, err)
	tiers := []Tier{{Name: "hot", Store: store}}

	open := func(dir string, id []byte) CommitLog {
		t.Helper()
		l, err := New(Options{
			Name: "identity", Path: dir, MaxSegmentBytes: 1 << 20,
			Tiers: tiers, Identity: id,
		})
		require.NoError(t, err)
		return l
	}

	l := open(tempDir(t), []byte("stream-7"))
	require.Nil(t, l.IdentityConflict())
	require.NoError(t, l.Close())

	// A DIFFERENT directory, as an adopting node would have: the identity can
	// only have come from the store.
	l2 := open(tempDir(t), []byte("stream-7"))
	require.Nil(t, l2.IdentityConflict(), "the identity did not reach the store")
	require.NoError(t, l2.Close())

	l3 := open(tempDir(t), []byte("stream-9"))
	c := l3.IdentityConflict()
	require.NotNil(t, c, "an adopting node must be told the tier holds someone else's data")
	require.Equal(t, []byte("stream-7"), c.Stored)
	require.NoError(t, l3.Close())
}

// A plain reopen must not rewrite the descriptor.
//
// This began as a downstream compatibility question — does a v0.80.0 process
// rewrite a v0.79.x log's descriptor and make it unreadable to the older build?
// v0.82.0 dropped V0, so that framing is gone, but the property it was checking
// is not about old readers at all: a reopen that changes nothing must not
// TOUCH the file. The rewrite path is reachable (a legitimate compression or
// segment-size change republishes), and every write is a window a crash can land
// in, so the case where nothing changed is the case where writing is pure risk.
//
// Kept rather than deleted with the V0 support that motivated it, because a test
// whose stated reason expires is not the same as a test whose property expires —
// see the note about assertions outliving their justification in
// docs/sweep-2026-08-13-complexity.md.
func TestReopeningAnUnchangedLogDoesNotRewriteItsDescriptor(t *testing.T) {
	dir := tempDir(t)
	opts := Options{Name: "identity", Path: dir, MaxSegmentBytes: 1 << 20}
	l, err := New(opts)
	require.NoError(t, err)
	_, err = l.Append([]*Message{{Value: []byte("a")}})
	require.NoError(t, err)
	require.NoError(t, l.Close())

	path := filepath.Join(dir, descriptorFileName)
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	before, err := os.Stat(path)
	require.NoError(t, err)

	l2, err := New(opts)
	require.NoError(t, err)
	require.NoError(t, l2.Close())

	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, string(body), string(after),
		"an unchanged reopen rewrote the descriptor")
	afterStat, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, before.Size(), afterStat.Size())

	// The mtime too, because equal CONTENT would be satisfied by a rewrite that
	// happened to produce the same bytes — and a rewrite is exactly what must
	// not happen, not merely a change.
	require.Equal(t, before.ModTime(), afterStat.ModTime(),
		"the descriptor was rewritten with identical bytes; the write is the "+
			"thing under test, not the content")
}

// V0 is refused now, and refused LOUDLY: it must name the version rather than
// fail somewhere downstream as a parse error or an unknown field.
//
// This replaces TestAVersion0DescriptorStillOpens, which asserted the opposite
// until v0.82.0. Deleting it without putting this in its place would have left
// the drop untested in both directions — nothing would have noticed if the
// version check had been removed entirely along with the constant, since a V0
// file's remaining lines all parse.
func TestAVersion0DescriptorIsRefusedByVersion(t *testing.T) {
	dir := tempDir(t)
	l, err := New(Options{Name: "identity", Path: dir, MaxSegmentBytes: 1 << 20})
	require.NoError(t, err)
	require.NoError(t, l.Close())

	path := filepath.Join(dir, descriptorFileName)
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	old := strings.Replace(string(body), "1\n", "0\n", 1)
	require.NotEqual(t, string(body), old, "the fixture did not actually downgrade the version")
	require.NoError(t, os.WriteFile(path, []byte(old), 0644))

	_, err = New(Options{Name: "identity", Path: dir, MaxSegmentBytes: 1 << 20})
	require.Error(t, err, "a v0 descriptor must be refused")
	require.Contains(t, err.Error(), "unsupported descriptor version 0",
		"the refusal must name the version; every other line in a v0 file parses "+
			"fine, so any other error means the version check is not what caught it")
}
