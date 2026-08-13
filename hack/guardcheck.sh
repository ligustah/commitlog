#!/usr/bin/env bash
#
# guardcheck — does each guard actually have a test that fails without it?
#
# Fuzzing explores INPUTS. It cannot tell you whether an assertion bites, so it
# cannot catch a test that reads as coverage and checks the wrong property. This
# repo produced three of those in a single day:
#
#   - a concurrency test that passed with the reclamation pin check DELETED,
#     because it was measuring a read-ahead buffer rather than the pin;
#   - a strip test that proved stripFrame preserves the values it is GIVEN, and
#     so said nothing about one arriving already corrupt — the exact defect that
#     shipped;
#   - five hand-picked fuzz seeds that all passed with the digest's sidecar CRC
#     check deleted.
#
# Each was caught the same way: break the guard, run the test, watch. That is
# mechanical, so it belongs in a script rather than in someone's discipline.
#
# For every guard below: delete it, run the test that claims to cover it, and
# require that the test FAILS. A guard whose removal leaves its test green has no
# coverage, whatever the test is named.
#
# Usage:  hack/guardcheck.sh          (every guard this platform can check)
#         hack/guardcheck.sh crc      (only guards whose name matches)
#
#         GUARDCHECK_SET=platform hack/guardcheck.sh
#                                     (only the guards that REQUIRE this OS)
#
# It edits tracked files in place and restores them on any exit path, including
# Ctrl-C. It refuses to start on a dirty tree so a failed restore can never be
# confused with your own work.

set -uo pipefail
cd "$(dirname "$0")/.."

# GitHub's ubuntu runners ship python3 but not always a bare `python`.
PY_BIN="${PYTHON:-}"
if [ -z "$PY_BIN" ]; then
  if command -v python3 >/dev/null 2>&1; then PY_BIN=python3; else PY_BIN=python; fi
fi

if [ -n "$(git status --porcelain -- '*.go')" ]; then
  echo "guardcheck: working tree has uncommitted .go changes; refusing to edit files in place." >&2
  echo "Commit or stash them first." >&2
  exit 2
fi

TOUCHED=()
restore() {
  if [ ${#TOUCHED[@]} -gt 0 ]; then
    git checkout -- "${TOUCHED[@]}" 2>/dev/null || true
  fi
}
trap restore EXIT INT TERM

filter="${1:-}"
failures=0
checked=0
deferred=0

# Which OS is running this, and which guards it was asked for. GUARDCHECK_SET is
# "platform" for the runner that exists ONLY to check the guards the primary
# runner cannot; anything else means "everything checkable here".
goos="$(go env GOOS)"
set_sel="${GUARDCHECK_SET:-all}"

# name | file | old text | replacement text | test regex | [race]
#
# The removal is a literal replacement rather than a patch, so it breaks loudly
# if the guarded code moves — which is the point: a guard that has been rewritten
# needs its claim re-checked, not silently skipped.
#
# A sixth argument of "race" runs the test under the race detector. Some guards
# are not conditionals that reject bad input but disciplines about shared memory
# — the copy-on-write in TruncateBefore is one — and removing those produces a
# test that still PASSES without -race, because the detector is the assertion.
# Without this, such a guard could only be registered as a lie.

# apply_edit FILE OLD NEW — replace OLD with NEW once in FILE. Exit 3 if absent.
apply_edit() {
  local file="$1" old="$2" new="$3"
  TOUCHED+=("$file")
  OLD="$old" NEW="$new" "$PY_BIN" -c '
import os, sys
p = sys.argv[1]
old, new = os.environ["OLD"], os.environ["NEW"]
s = open(p, encoding="utf-8", newline="").read()
# The patterns below are written with LF, because .gitattributes stores Go with
# LF and that is what CI checks out. A Windows working tree can still hold CRLF
# (git only rewrites a file it actually touches), and then every MULTI-LINE
# pattern silently fails to match — the guard reports "did it move?" and the
# check is skipped on the one platform whose file handling this repo most needs
# verified. Normalize first: the file is restored from git on the way out
# regardless, so rewriting it with LF here costs nothing.
if old not in s and "\r\n" in s:
    s = s.replace("\r\n", "\n")
n = s.count(old)
if n == 0:
    sys.exit(3)
# More than one match is AMBIGUOUS, not a licence to take the first. Two guards
# in this file are byte-identical on their `if` line -- the high watermark clamp
# on reopen and the one in Truncate -- so a pattern matching both would have
# neutralized the wrong one and reported on a guard it never touched. Caught
# while falsifying that very clamp by hand, where it silently proved nothing.
if n > 1:
    sys.exit(4)
open(p, "w", encoding="utf-8", newline="").write(s.replace(old, new, 1))
' "$file"
}

# guard_platform GOOS NAME — decide whether this guard can be checked HERE.
# Returns 1 when it cannot, or when this run did not ask for it.
#
# A guard inside a `//go:build windows` file is unfalsifiable on Linux, and NOT
# in a way that shows up as a skip: the file is never compiled, the test named
# for it does not exist, and `go test -run` with nothing to run exits 0. This
# script reads that 0 as "the test passed with the guard removed" and reports NO
# COVERAGE — which is what the ubuntu job did the day the first Windows-only
# guard landed. The guard was fine; the runner was wrong.
#
# So such a guard is DEFERRED here and checked by a second job on the OS it
# belongs to. Deferral is only honest while that job exists, so the summary
# names the platform and says outright that this run did not cover it — the one
# thing a coverage tool must never do is stay quiet about what it skipped.
guard_platform() {
  local want="$1" name="$2"
  if [ -n "$filter" ] && [[ "$name" != *"$filter"* ]]; then
    return 1
  fi
  if [ "$set_sel" = "platform" ] && [ -z "$want" ]; then
    return 1
  fi
  if [ -n "$want" ] && [ "$want" != "$goos" ]; then
    printf '  %-34s deferred to %s\n' "$name" "$want"
    deferred=$((deferred + 1))
    return 1
  fi
  return 0
}

# The OS the guard being registered requires, empty for the portable majority.
# Set by run_guard_windows around its call and read by guard_start, so that the
# platform decision is made in ONE place no matter which entry point was used.
guard_want=""

# guard_start NAME — filter, count and print. Returns 1 if this guard is filtered out.
guard_start() {
  guard_platform "$guard_want" "$1" || return 1
  checked=$((checked + 1))
  printf '  %-34s ' "$1"
  return 0
}

# guard_finish TEST_RE MODE FILE... — build, run the test, report, restore.
guard_finish() {
  local test_re="$1" mode="$2"
  shift 2
  local -a extra=()
  if [ "$mode" = "race" ]; then extra=(-race); fi

  # A build failure must NOT count as coverage. The first version of this
  # script neutralized guards with `if false {`, which orphaned imports; the
  # package then failed to COMPILE, `go test` returned non-zero, and every
  # guard reported as covered — including one deliberately pointed at a test
  # that does not cover it. The replacements above keep each symbol used, and
  # this check makes the remaining case loud instead of green.
  if ! go build ./... >/dev/null 2>&1; then
    echo "HARNESS ERROR (package does not build with the guard neutralized)"
    failures=$((failures + 1))
    git checkout -- "$@"
    return 0
  fi
  if go test -run "$test_re" -count=1 -timeout 300s ${extra[@]+"${extra[@]}"} . >/dev/null 2>&1; then
    echo "NO COVERAGE — $test_re passed without it"
    failures=$((failures + 1))
  else
    echo "ok (test fails without it)"
  fi
  git checkout -- "$@"
}

# why_edit_failed EXITCODE — 3 is "gone", 4 is "matches more than one place".
why_edit_failed() {
  if [ "$1" = "4" ]; then
    echo "SKIP (guard text matches more than one place — narrow it)"
  else
    echo "SKIP (guard text not found — did it move?)"
  fi
}

run_guard() {
  local name="$1" file="$2" old="$3" new="$4" test_re="$5" mode="${6:-}"
  guard_start "$name" || return 0
  # Status captured on its own line: inside `if ! cmd; then`, $? is the NEGATED
  # status, so it reads 0 and every failure would report the same reason.
  apply_edit "$file" "$old" "$new"
  local rc=$?
  if [ "$rc" -ne 0 ]; then
    why_edit_failed "$rc"
    failures=$((failures + 1))
    git checkout -- "$file"
    return 0
  fi
  guard_finish "$test_re" "$mode" "$file"
}

# run_guard_windows — a guard only a Windows runner can falsify. Same arguments
# as run_guard. Elsewhere it is announced as deferred rather than checked; see
# guard_platform for why silence is not an option here.
#
# What decides this is the TEST, not the guarded file: a guard in portable code
# whose only failure mode is a Windows one is checked by a `//go:build windows`
# test, and that test does not exist on Linux either.
run_guard_windows() {
  guard_want=windows
  run_guard "$@"
  guard_want=""
}

# Some guards are ONE defence implemented at two sites, where each site masks
# the other and neither is observable alone. Registering either half separately
# would report NO COVERAGE and be right to: the honest unit is the pair, so
# remove both and require the test to fail.
run_guard_pair() {
  local name="$1" f1="$2" o1="$3" n1="$4" f2="$5" o2="$6" n2="$7" test_re="$8" mode="${9:-}"
  guard_start "$name" || return 0
  apply_edit "$f1" "$o1" "$n1"
  local rc=$?
  if [ "$rc" -eq 0 ]; then
    apply_edit "$f2" "$o2" "$n2"
    rc=$?
  fi
  if [ "$rc" -ne 0 ]; then
    why_edit_failed "$rc"
    failures=$((failures + 1))
    git checkout -- "$f1" "$f2"
    return 0
  fi
  guard_finish "$test_re" "$mode" "$f1" "$f2"
}

echo "guardcheck: removing each guard and requiring its test to fail"

run_guard "prefix-read CRC" prefix_read.go \
  'if want, got := cp.Crc(), crc32.Checksum(cp[4:], crc32cTable); want != got {' \
  'if want, got := cp.Crc(), crc32.Checksum(cp[4:], crc32cTable); want != got && false {' \
  '^TestKeyPrefixRefuses'

run_guard "stripFrame CRC (no laundering)" compact_cleaner.go \
  'if want, got := msg.Crc(), crc32.Checksum(msg[4:], crc32cTable); want != got {' \
  'if want, got := msg.Crc(), crc32.Checksum(msg[4:], crc32cTable); want != got && false {' \
  '^TestCompactionDoesNotResignCorruptRecords$'

run_guard "readMessage CRC" reader.go \
  'if c := crc32.Checksum(m[4:], crc32cTable); crc != c {' \
  'if c := crc32.Checksum(m[4:], crc32cTable); crc != c && false {' \
  '^TestSequentialReadReturnsCorruptRecordRatherThanPanicking$'

run_guard "digest sidecar CRC" keydigest.go \
  'if crc32.ChecksumIEEE(body) != encoding.Uint32(crcBytes) {' \
  'if crc32.ChecksumIEEE(body) != encoding.Uint32(crcBytes) && false {' \
  '^FuzzCorruptDigestNeverChangesTheAnswer$'

run_guard "reclamation pin" tier_state.go \
  'if e.pin != nil && e.pin.referenced() {' \
  'if e.pin != nil && e.pin.referenced() && false {' \
  '^TestReclamationWaitsForTheReaderHoldingTheOldObject$'

# Anchored on the comment above it: `l.appendMu.Lock()` with its deferred unlock
# appears at four sites in this file, and the bare pair matched all of them. It
# happened to hit the intended one first, so this guard was accidentally valid
# rather than actually valid — and would have silently moved to another site the
# first time anyone added a lock earlier in the file.
run_guard "append tail-under-lock" commitlog.go   '	// Reading the tail and writing to it must be one step; see appendMu.
	l.appendMu.Lock()
	defer l.appendMu.Unlock()'   '	// BREAK: tail read and write no longer one step'   '^TestConcurrentAppends'

# The read path's frame-header CRC. Anchored on the RETURN under it, not on the
# condition alone: readMessage and readMessageMetadata now decode out of an
# identically-named local, so the bare condition matches both and apply_edit
# refuses an ambiguous anchor. The bare form silently expired the moment those
# two lines became the same text.
run_guard "frame-header CRC" reader.go   'if want, got := storedHeaderCrc(hdr), headerCrc(hdr); want != got {
		return nil, 0, 0, 0, pkgErrors.Wrapf(ErrCorruptRecord,'   'if want, got := storedHeaderCrc(hdr), headerCrc(hdr); want != got && false {
		return nil, 0, 0, 0, pkgErrors.Wrapf(ErrCorruptRecord,'   '^FuzzCorruptFrameHeaderIsNeverServedAsTruth$'

# scanForward must report a failed read rather than calling it "entry not found",
# because both timestamp lookups turn not-found into a plausible offset. The
# neutralization keeps errors.Is evaluated (`|| true`) so no import is orphaned.
run_guard "scanForward read failure" segment.go \
  'if errors.Is(err, io.EOF) {
				return nil, ErrEntryNotFound
			}' \
  'if errors.Is(err, io.EOF) || true {
				return nil, ErrEntryNotFound
			}' \
  '^TestTimestampLookupsRefuseAFailedReadInsteadOfGuessing$'

# The scan path's own frame-header CRC. The read path has checked this since the
# header CRC existed; the scan never did, which is why the paths that walk a
# segment to REWRITE it — compaction, Truncate, TruncateBefore — were the ones a
# caller's damaged data could crash, by sizing an allocation from a header nobody
# had validated.
run_guard "scan frame-header CRC" segment.go \
  'if want, got := storedHeaderCrc(header), headerCrc(header); want != got {' \
  'if want, got := storedHeaderCrc(header), headerCrc(header); want != got && false {' \
  '^TestDamageInOneSegmentDoesNotKillTheProcess$'

# A frame cannot be longer than what follows its own header. The second line of
# the same defence, and it needs its own test: the damage test above never
# reaches here, because random bytes break the header CRC and the check above
# catches them first. Only a header that PASSES its CRC while lying about length
# isolates this one, which is what the named test forges.
run_guard "scan frame length vs extent" segment.go \
  'if remaining := s.s.Position() - (s.pos + msgSetHeaderLen); int64(size) > remaining {' \
  'if remaining := s.s.Position() - (s.pos + msgSetHeaderLen); int64(size) > remaining && false {' \
  '^TestAFrameCannotDeclareMoreThanTheSegmentHolds$'

# The committed-reader watermark defect, fixed at both levels it can be caught:
# "nothing is committed" is its own reason to park rather than a special case of
# offset > hw, and an unset hwSeg means "this reader does not know where the
# watermark is" rather than "the watermark is elsewhere", which equality with
# r.seg cannot express.
#
# Registered as a pair because neither half is observable alone, and that is not
# a gap in the test. Remove the constructor clause and readLoop's nil check
# clamps the read to hwPos of -1, ending it correctly. Remove readLoop's check
# and the constructor never lets a nil hwSeg reach it — getHWPos cannot return a
# nil segment with a nil error, so hw == -1 is the only way to get one. Each is
# genuinely redundant while the other stands, which is what defence in depth
# means; the pair is the honest unit of coverage.
run_guard_pair "committed reader watermark" \
  reader.go \
  'if hw == -1 || offset > hw || l.OldestOffset() == -1 {' \
  'if offset > hw || l.OldestOffset() == -1 {' \
  reader.go \
  'if r.hwSeg == nil || r.seg == r.hwSeg {' \
  'if r.seg == r.hwSeg {' \
  '^TestACommittedReaderNeverServesWhatIsNotCommitted$'

# Never install a segment into a set that has already been walked by close. Both
# the maintenance paths and the roll path hold this invariant; a segment
# published past the walk is one nothing will ever close.
run_guard "truncate refuses a closed log" commitlog.go \
  '	if closed {
		return ErrCommitLogClosed
	}
	seg, idx := findSegment(snapshot, offset)' \
  '	if closed && false {
		return ErrCommitLogClosed
	}
	seg, idx := findSegment(snapshot, offset)' \
  '^TestMaintenanceOnAClosedLogBuildsNothing$'

run_guard "roll refuses a closed log" commitlog.go \
  '	if l.segmentsClosed {
		// The log closed underneath this append. Do not publish into a set that' \
  '	if l.segmentsClosed && false {
		// The log closed underneath this append. Do not publish into a set that' \
  '^TestASegmentRolledWhileTheLogClosesIsStillClosed$'

# Copy-on-write, not a conditional: Segments() hands out the slice HEADER, so
# writing an element in place races every lock-free reader holding a snapshot.
# The neutralization aliases the old backing array instead of building a fresh
# one, and the `survivors[0] = trimmed` two lines down then writes into it.
# Needs -race — without it the neutralized version passes, which is the whole
# reason this runner learned the flag.
run_guard "TruncateBefore copy-on-write" commitlog.go \
  '	survivors := make([]*segment, 0, len(newSegments)-firstKept)
	survivors = append(survivors, oldSegments[firstKept:]...)' \
  '	survivors := oldSegments[firstKept:]' \
  '^TestRetentionNeverWritesIntoASliceAReaderIsHolding$' race

# A truncation that cuts below the watermark must bring the watermark down with
# it. Note the two-line pattern: the `if` line alone is byte-identical to the
# reopen clamp above it in the same file, so a one-line pattern would neutralize
# whichever came first and report on a guard it never touched.
run_guard "truncate clamps the watermark" commitlog.go \
  '	if newest := activeSegment.NextOffset() - 1; l.hw > newest {
		slog.Warn("commitlog: truncation cut below the high watermark; clamping",' \
  '	if newest := activeSegment.NextOffset() - 1; l.hw > newest && false {
		slog.Warn("commitlog: truncation cut below the high watermark; clamping",' \
  '^TestTruncatingBelowTheWatermarkClampsIt$'

# A clean raises the epoch cache's floor, and that is the whole of what it does
# to it. Note which test this is registered against: removing the call leaves
# the cache untouched, which still KEEPS every epoch, so the tests named for the
# epochs surviving a clean cannot see it go. What only this call does is
# re-anchor an entry that now sits below the surviving floor.
run_guard "clean raises the epoch floor" clean.go   '	err := l.leaderEpochCache.ClearEarliest(l.segments[0].BaseOffset)'   '	var err error'   '^TestCleanerKeepsLeaderEpochOffsetsThroughCompaction$'

# A boundary segment trimmed at a new base offset must point readers at the trim.
# Without the link a reader already resolved into it reads a segment that is gone
# with no replacement -- the retention case -- and skips to the NEXT segment, past
# the very records the trim preserved.
#
# Named for a deterministic test, not for the chaos test that first caught this.
# Truncation now publishes the new segment list BEFORE it unlinks, so a reader
# that re-resolves finds the trim already published and never reaches the
# boundary; only a snapshot older than the publish does, and chaos cannot
# manufacture one on demand. The hazard is unchanged, the window is narrower.
run_guard "trimmed boundary redirects" commitlog.go   '			boundary.SupersededBy(trimmed)'   '			_ = trimmed'   '^TestAStaleSegmentSnapshotFollowsATrimmedBoundary$'

# Two segments describing the same records is the state a crash inside
# TruncateBefore leaves -- the trim renamed into place, the source it was
# rewritten from not yet deleted. Without this, open() takes both and a read
# serves those offsets TWICE, in order, with no error anywhere.
run_guard "reopen resolves segment overlaps" commitlog.go   '	if err := l.resolveSegmentOverlaps(); err != nil {'   '	if err := error(nil); err != nil {'   '^TestAnInterruptedTrimDoesNotServeRecordsTwice$'

# TruncateBefore unlinks with l.mu released, and the unlinks are the bulk of a
# retention pass. Taking the lock for them is the regression this catches -- note
# that it does NOT restore the old behaviour wholesale (the rewrite still runs
# unlocked), which is the point: what is caught is partial starvation, not only
# total starvation.
#
# Putting the lock back at the TOP of the call, which is what the original bug
# literally was, cannot be expressed here: the publish step takes l.mu itself, so
# the neutralised build would deadlock rather than fail, and guardcheck would
# report a timeout instead of a red test. The unlinks run after that publish,
# which is why holding the lock to the end of the call is expressible.
#
# The test asks the unlink itself whether the lock is free, from inside the
# store's Delete. It used to assert a read RATE against a baseline, which made
# coverage depend on how loaded the machine was -- see the header comment in
# truncate_lock_determinism_test.go.
run_guard "truncation unlinks outside the lock" commitlog.go   '	for i := 0; i < boundaryIdx; i++ {
		if err := oldSegments[i].Delete(); err != nil {'   '	l.mu.Lock()
	defer l.mu.Unlock()
	for i := 0; i < boundaryIdx; i++ {
		if err := oldSegments[i].Delete(); err != nil {'   '^TestATruncationUnlinksWithTheSegmentLockAvailable$'

# An append can roll while the truncation has the lock down, and the surviving
# list it publishes has to carry what split() appended. Losing it drops records
# that were acknowledged, silently. The test enters that window from INSIDE the
# boundary rewrite rather than racing for it.
run_guard "truncation carries segments rolled under it" commitlog.go   '	if len(newSegments) > len(oldSegments) {'   '	if false {'   '^TestATruncationCarriesASegmentRolledDuringItsRewrite$'

# Truncate unlinks with l.mu released, and on a follower reconciling after an
# unclean election the unlinks are most of the call. The neutralization takes the
# lock around just the delete loop and gives it back -- a MATCHED pair rather
# than a defer, because Truncate publishes under l.mu further down and a deferred
# unlock would deadlock there. Same limitation as the TruncateBefore guard above:
# what this catches is the unlinks moving back under the lock, not the whole call
# doing so. Observed the same way -- the unlink asks whether the lock is free.
run_guard "truncate unlinks outside the lock" commitlog.go   '	deleted := 0
	for i := idx + 1; i < len(snapshot); i++ {
		if err := snapshot[i].Delete(); err != nil {
			dropReplacement()
			return err
		}
		deleted++
	}'   '	deleted := 0
	l.mu.Lock()
	for i := idx + 1; i < len(snapshot); i++ {
		if err := snapshot[i].Delete(); err != nil {
			dropReplacement()
			l.mu.Unlock()
			return err
		}
		deleted++
	}
	l.mu.Unlock()'   '^TestATruncateUnlinksWithTheSegmentLockAvailable$'

# A reader that consumed a segment must advance PAST it, by asking for the
# segment holding its next OFFSET. The neutralization restores the old query --
# the next BASE offset above its own -- which walks straight into the trim that
# replaced the segment it just read, because a trim has a higher base and covers
# a suffix of the same range. That served a record twice, downstream, inside one
# read batch.
#
# Written out rather than calling the helper it used to call: that helper was
# DELETED, precisely so nobody reaches for it again, so the neutralization has to
# carry the wrong query itself. Note what it omits as much as what it does --
# no current() resolution, which is the other half of what findSegment gives.
run_guard "reader advances past its segment" util.go   '	next, _ := findSegment(segments, seg.NextOffset())'   '	nextIdx := sort.Search(len(segments), func(i int) bool { return segments[i].BaseOffset >= seg.BaseOffset+1 })
	var next *segment
	if nextIdx < len(segments) {
		next = segments[nextIdx]
	}'   '^TestASegmentAdvanceSkipsTheTrimOfTheSegmentJustRead$'

# An empty cache has no previous epoch to be monotonic against, so any epoch may
# open the history — including 0, which is what ordinary Append stamps.
# Neutralized by the gate as it stood, where latestEpoch()'s empty-cache answer
# of 0 made `0 > 0` refuse the first assignment on every log, permanently and
# without logging anything.
run_guard "an empty epoch cache accepts any epoch" leader_epoch_cache.go   '	if len(l.epochOffsets) == 0 || (epoch > latestEpoch && offset >= latestOffset) {' '	if epoch > latestEpoch && offset >= latestOffset {'   '^TestLeaderEpochZeroIsRecorded$'

# EVERY refused epoch assignment logs. Neutralized by removing the arm no
# condition anticipated, which is where an assignment of the already-latest
# epoch used to land: warn's two cases are strict comparisons, so it wrote no
# entry, logged nothing and returned nil — indistinguishable from success, both
# at the call and afterwards. Requested by durable_streams after that silence
# cost them an investigation into epoch history their side never held.
run_guard "every refused epoch assignment logs" leader_epoch_cache.go   '	default:
		slog.Warn("Refused a log leader epoch assignment that would reassign an epoch "+' '	case false:
		slog.Warn("Refused a log leader epoch assignment that would reassign an epoch "+'   '^TestLeaderEpochZeroIsRecorded$'

# A leader epoch arriving from the checkpoint file must be parsed as UNSIGNED.
# The neutralization restores the old parse-then-convert, which is the whole bug:
# "-1" is a valid int64, becomes 2^64-1 as a uint64, and that is a well-formed
# epoch higher than any a leader will ever assign -- so a corrupt file opens
# cleanly and pins latestEpoch() at the ceiling for good. Written as two lines so
# the package still COMPILES without the guard; a build failure would be reported
# as a harness error, not as coverage.
run_guard "leader epoch parsed unsigned" leader_epoch_cache.go   '		leaderEpoch, err := strconv.ParseUint(scanner.Text(), 10, 64)'   '		signedEpoch, err := strconv.ParseInt(scanner.Text(), 10, 64)
		leaderEpoch := uint64(signedEpoch)'   '^TestALeaderEpochCheckpointWithANegativeEpochIsRefused$'

# The checkpoint's VERSION gate is exact equality, and the neutralization is the
# form it used to have. `> leaderEpochFileV0` reads as "reject anything newer",
# but Atoi is signed and v0 is the FIRST version, so the other half of that
# comparison is not the empty set: every negative version passed and the file was
# then read as v0. Same laundering as the guard above, one field over, and it
# matters for the same reason -- this file carries no checksum, so the parse is
# the only integrity check it gets.
run_guard "checkpoint version is exact" leader_epoch_cache.go   '	if version != leaderEpochFileV0 {'   '	if version > leaderEpochFileV0 {'   '^TestALeaderEpochCheckpointWithANegativeVersionIsRefused$'

# A probe that names no leader epoch has no safe answer, because the caller
# truncates to whatever it is told. The refusal is the whole reason the argument
# is a type and not a uint64: without it the log falls back to answering for
# epoch 0, which is a real epoch, and on a log whose first recorded epoch is 1
# that answer is offset 0 -- an instruction to delete everything. durable_streams
# lost a node and 450 committed records to exactly that.
#
# `&& false` rather than `if false`, because the neutralized branch still has to
# COMPILE: dropping the read of `known` leaves it declared and unused, which is
# a build failure and reads as a harness error rather than as a missing guard.
run_guard "an unknown epoch probe is refused" commitlog.go   '	if !known {'   '	if !known && false {'   '^TestAnUnknownLeaderEpochProbeIsRefused$'

# A store key names an object INSIDE the store. The manifest is the one place
# keys arrive from outside, and they become an action rather than a description:
# s.storeKey is handed to store.Delete. Neutralizing the boundary check lets a
# manifest name "../../x", which for FileSegmentStore is an os.Remove of a file
# the store never held -- filepath.Join CLEANS the traversal rather than refusing
# it, which is what made the escape silent.
run_guard "manifest refuses a foreign key" manifest.go   '		if err := validStoreKey(o.LogKey); err != nil {'   '		if err := error(nil); err != nil {'   '^TestATierManifestNamingAKeyOutsideTheStoreIsRefused$'

# The same rule one layer down, where the key becomes a path. This is the guard
# that makes the consequence concrete rather than the policy. It now lives in
# util.go: a log sidecar name needs the identical check on a different route, and
# one rule with two callers beats two rules that have to agree.
run_guard "a bare name stays inside its directory" util.go   '	if strings.ContainsAny(name, `/\`) {'   '	if false {'   '^TestAFileSegmentStoreKeyCannotReachOutsideItsDirectory$'

# ...which is the other caller. Same removal, different test: the store's keys
# arrive in a manifest, the log's sidecar names arrive from the log's CLIENT, and
# neither test would notice the check going missing for the other one.
run_guard "a sidecar name stays inside the log" util.go   '	if strings.ContainsAny(name, `/\`) {'   '	if false {'   '^TestASidecarNameCannotReachOutsideTheLog$'

# The plain-name rule is only half of it. A sidecar can be a perfectly ordinary
# file name and still be one the LOG owns -- "00000000000000000000.index" names a
# live index and RemoveSidecar deletes it, "replication-offset-checkpoint"
# overwrites the high watermark, "notes.log" makes the log refuse to open at all
# because openLog scans by suffix and fails on a stem that is not an integer.
# This contract existed for as long as the API did, as a sentence on the
# interface telling the caller not to do it.
run_guard "a sidecar cannot name a log file" sidecar.go   '	for _, suffix := range logOwnedFileSuffixes {'   '	for _, suffix := range []string(nil) {'   '^TestASidecarCannotNameOneOfTheLogsOwnFiles$'

run_guard "a sidecar cannot name a log checkpoint" sidecar.go   '	for _, owned := range logOwnedFileNames {'   '	for _, owned := range []string(nil) {'   '^TestASidecarCannotNameOneOfTheLogsOwnFiles$'

# Same two removals against the test that derives its names from a real log
# directory instead of from the list. Registered separately because the point of
# that test is that it does NOT read the list, and a test which cannot notice the
# list being emptied would have been proving nothing about it.
run_guard "the derived name list is not vacuous" sidecar.go   '	for _, suffix := range logOwnedFileSuffixes {'   '	for _, suffix := range []string(nil) {'   '^TestEveryFileTheLogWritesIsRefusedAsASidecarName$'

run_guard "the derived checkpoint list is not vacuous" sidecar.go   '	for _, owned := range logOwnedFileNames {'   '	for _, owned := range []string(nil) {'   '^TestEveryFileTheLogWritesIsRefusedAsASidecarName$'

# Nothing REFERENCES the descriptor -- it is what the store says about itself,
# not part of what it holds -- so the reference-based garbage rule collects it
# unless told otherwise, and the log then refuses its own next open.
run_guard "the descriptor is never garbage" tier_state.go   '	referenced[descriptorKey] = true'   '	_ = descriptorKey'   '^TestTheDescriptorIsNeverGarbage$'

# A store-backed log's existence is a question for the store, not for whatever
# directory this process happens to be using. Neutralizing this falls back to the
# directory scan -- which is what shipped, and which calls a node adopting a tier
# a NEW log, so the one moment a process picks up someone else's log is the one
# moment its retention settings are never compared.
run_guard "newness comes from the store" descriptor.go   '	if len(opts.Tiers) > 0 {
		// Asked of every tier'   '	if false {
		// Asked of every tier'   '^TestAdoptingATierIsCheckedAgainstTheLogsOwnSettings$'

# Absence is an ANSWER here, so only the store may give it. Neutralizing this
# restores the inference that shipped -- any Size failure means "no descriptor"
# -- and a log with no descriptor is a NEW log, which records the settings it
# was handed without comparing them to anything. Same ending as the guard above,
# reached from a bad minute for the store rather than an empty directory.
run_guard "descriptor absence must be asserted" descriptor.go   '	if errors.Is(err, ErrObjectNotFound) {
		return descriptor{}, os.ErrNotExist
	}'   '	if err != nil {
		return descriptor{}, os.ErrNotExist
	}'   '^TestAStoreThatCannotAnswerIsNotANewLog$'

# The same inference in the manifest reader, with a worse ending. "No manifest"
# means an empty tier, an empty tier means this log holds nothing offloaded, and
# the next publish rebuilds the manifest without those entries -- after which
# every object they named is unreferenced, and the collect-then-delete cycle the
# API documents removes live data.
run_guard "tier absence must be asserted" manifest.go   '	if errors.Is(err, ErrObjectNotFound) {'   '	if err != nil {'   '^TestAStoreThatCannotAnswerIsNotAnEmptyTier$'

# The version field is the manifest's whole integrity check, and `>` let version
# 0 -- what an absent field decodes to -- through. Restoring `>` is restoring
# "any JSON object is a manifest".
run_guard "a manifest must be this version" manifest.go   '	if m.Version != manifestVersion {'   '	if m.Version > manifestVersion {'   '^TestAManifestThatIsNotThisVersionIsRefused$'

# Codec.Compress has no error to return, so its default arm stores the batch raw
# under whatever byte it was given -- and parseBlockHeader, the only other place
# Valid() is called, refuses that byte on the way back. Neutralizing this restores
# a write path that accepts exactly what the read path rejects: appends are taken,
# the descriptor records a codec name nothing can parse, and the log never opens
# again.
run_guard "an unknown codec is refused where it arrives" commitlog.go   '	if !opts.Compression.Valid() {'   '	if false {'   '^TestAnUnknownCompressionCodecIsRefusedAtOpen$'

# The None codec used to return src itself, so the "decompressed" bytes were the
# caller's compressed-payload buffer -- which in the block path is a recycled read
# buffer that the next block refills. decodeBlock carried a pointer-identity check
# against exactly that. Neutralizing the copy restores the alias, and with it a
# result that is correct when returned and wrong a block later.
run_guard "decompressing into dst does not alias src" compress/codec.go   '		return append(dst[:0], src...), nil'   '		return src, nil'   '^TestDecompressIntoNeverAliasesItsInput$'

# The manifest is the commit point, so a COPY of a tier has to write it last for
# the same reason an offload does: until it lands, nothing in the destination is
# claimed by anything, and a copy that dies partway leaves collectable orphans
# rather than a tier claiming records whose bytes are not there. Reordering the
# three steps is exactly the hand-rolled copy that only worked because one store's
# List() happened to sort "manifest" after the segment keys.
run_guard "a copied manifest is published last" copy_tier.go   '	if err := copyTierObjects(src, dst, objs); err != nil {
		return err
	}
	if err := writeStoreDescriptor(dst, desc); err != nil {
		return err
	}
	return publishCopiedManifest(dst, objs)'   '	if err := publishCopiedManifest(dst, objs); err != nil {
		return err
	}
	if err := writeStoreDescriptor(dst, desc); err != nil {
		return err
	}
	return copyTierObjects(src, dst, objs)'   '^TestACopiedManifestIsWrittenLast$'

# A missing file must come back immediately, not be waited out. Neutralizing the
# fast path makes every legitimate absence -- a log with no checkpoint, an
# unwritten sidecar, a first open -- pay the full retry bound before reporting
# what it could have reported at once.
#
# This guards the ABSENT half. The half that waits is guarded separately, by
# "the read retry spends its whole budget" below -- a deny-all handle is Windows
# only and this check runs on Linux, so that guard uses a directory instead,
# which is unreadable everywhere.
run_guard "a missing file is not retried" util.go   $'		b, err := os.ReadFile(path)
		if err == nil || os.IsNotExist(err) || time.Now().After(deadline) {'   $'		b, err := os.ReadFile(path)
		if err == nil || time.Now().After(deadline) {'   '^TestAReadOfAMissingFileDoesNotWaitOutTheRetryBound$'

# The three timestamp-lookup guards below all need a clock COARSER than the
# append rate, so that a run of records shares one instant. Every test that
# predates them gave each record a timestamp of its own, which is the state in
# which none of these three can be observed at all.

# The segment search is by BASE timestamp and strictly greater, so a roll landing
# inside a run of records sharing an instant puts that instant on the new
# segment's base -- and the answer becomes the later segment's first record, with
# every record of the same instant below it skipped. A resume point goes straight
# to a reader, so what that costs is those records, silently.
run_guard "resume walks back over a tie" commitlog.go   '	for at > 0 && l.segments[at-1].LastWriteTime() >= timestamp {'   '	for false && l.segments[at-1].LastWriteTime() >= timestamp {'   '^TestEarliestOffsetAfterTimestampWhenAnInstantSpansSegments$'

# The forward scan for "nothing in the chosen segment is at or after the target"
# has to be able to reach the LAST segment. Bounded at len-1 it cannot, and a
# target in the gap before it -- a roll coinciding with a pause -- answers with
# the next assignable offset, telling a consumer the whole final segment is in
# its past.
run_guard "the last segment is reachable" commitlog.go   '	for i := at; i < len(l.segments); i++ {'   '	for i := at; i < len(l.segments)-1; i++ {'   '^TestEarliestOffsetAfterTimestampInTheGapBeforeTheLastSegment$'

# LatestOffsetBeforeTimestamp is "one below the earliest record STRICTLY after
# T". Drop the +1 and "after" stops being strict: the run carrying T is excluded
# along with T, and the answer walks back to the record before the run instead of
# to its last member.
run_guard "as-of asks for strictly after" commitlog.go   '	after, err := l.earliestOffsetAfterTimestampLocked(timestamp + 1)'   '	after, err := l.earliestOffsetAfterTimestampLocked(timestamp)'   '^TestLatestOffsetBeforeTimestampLandsOnTheLastRecordOfATie$'

# Offsets are handed out under appendMu, so the clock read that stamps them has
# to happen under appendMu too, or a later offset can carry an earlier timestamp.
# The neutralization gives the lock back for exactly the clock read, which is
# what the code literally did -- and which the deferred Unlock still balances, so
# it compiles and runs rather than deadlocking.
run_guard "append stamps under the append lock" commitlog.go   '	now := timestamp()'   '	l.appendMu.Unlock()
	now := timestamp()
	l.appendMu.Lock()'   '^TestAnAppendStampsItsTimeUnderTheAppendLock$'

# The stamp is written back onto the CALLER'S Message, which is the surprising
# direction and the one nothing asserted until v0.70.0. The neutralization stamps
# a COPY instead: every on-disk assertion stays green -- the frames still carry
# the append clock -- and only the write-back goes. That is the point of doing it
# this way rather than deleting the stamp outright, which would take the disk
# tests down with it and prove nothing about who owns the struct.
run_guard "an append stamps the caller's message" commitlog.go   '	for _, m := range msgs {
		if m.Timestamp == 0 {
			m.Timestamp = now
		}
	}'   '	for i, m := range msgs {
		if m.Timestamp == 0 {
			stamped := *m
			stamped.Timestamp = now
			msgs[i] = &stamped
		}
	}'   '^TestAppendStampsTheCallersMessage$'

# Zero is a real ceiling -- "compact nothing" -- and the caller that passes it is
# a transactional one whose oldest open transaction begins at offset 0. The
# neutralization is the sentinel this field used to be, reintroduced inside
# Bound.Or itself: treat 0 as unset, and the narrowest bound a caller can ask for
# arrives as the widest one there is.
run_guard "a ceiling of zero is not unset" clean.go   '	if !b.set {
		return fallback
	}'   '	if !b.set || b.off == 0 {
		return fallback
	}'   '^TestACeilingOfZeroCompactsNothing$'

# The read retry waits out an amount of TIME, because what it waits for -- Windows
# reclaiming a dead process's handles -- takes time that depends on the machine,
# not on how many times it is asked. The neutralization shortens the budget
# without changing its shape, which is the bug it had: a 500ms ceiling nothing
# named, that lost 2 of 86 daemon restarts on a loaded box.
run_guard "the read retry spends its whole budget" util.go   $'func ReadFileWithRetry(path string) ([]byte, error) {
	deadline := time.Now().Add(waitedOnRetryBudget)'   $'func ReadFileWithRetry(path string) ([]byte, error) {
	deadline := time.Now().Add(waitedOnRetryBudget / 10)'   '^TestTheReadRetryBoundIsATimeBudgetNotAnAttemptCount$'

# Delete takes the descriptor LAST. Neutralized back to the plain RemoveAll,
# which records one error and carries on -- so one held file never stopped it
# removing the descriptor, and a directory of segments with no descriptor is a
# state readDescriptor refuses forever. sqlcdc lost a view's name to it.
run_guard_windows "a failed delete keeps the descriptor" commitlog.go   '	return removeLogDir(l.Path)' '	return removeAllWithRetry(l.Path)'   '^TestAFailedDeleteLeavesALogThatStillOpens$'

# The two halves that make NotifyLEO's read-then-act safe. It parks a waiter on
# one load of vActiveSegment while deciding whether to park from another, so a
# roll in between parks it on a segment nothing will append to again. seal() is
# what rescues that, and it takes BOTH of these: waking whoever already
# registered, and telling whoever arrives afterwards not to bother. Each is
# neutralized on its own, because either one alone parks a waiter forever.
run_guard "sealing wakes the waiters already parked" segment.go   '	s.sealed = true
	// Notify any readers waiting for data.
	s.notifyWaiters()' '	s.sealed = true
	// Notify any readers waiting for data.'   '^TestANotifyLEOWaiterWakesOnTheRollThatSealsItsSegment$'

run_guard "a sealed segment parks nobody new" segment.go   '	if s.sealed || s.position > pos || s.position >= s.maxBytes {' '	if s.position > pos || s.position >= s.maxBytes {'   '^TestANotifyLEOWaiterWakesOnTheRollThatSealsItsSegment$'

# Close walks the WHOLE segment set even when one segment refuses. Neutralized
# by returning at the first error, which is the version that left every later
# segment holding its handle and its index mmap for the life of the process --
# and on Windows a mapped index cannot be unlinked, so the directory could not
# be removed either.
run_guard "a refusing segment does not close the rest" commitlog.go   '		if err := segment.Close(); err != nil {
			errs = append(errs, err)
		}' '		if err := segment.Close(); err != nil {
			return err
		}'   '^TestCloseWalksEverySegmentEvenWhenOneRefuses$'

# A durability BARRIER waits longer than a tick. Same checkpoint write, two
# callers with opposite failure economics: the tick has the next tick behind it,
# SyncAll has a caller waiting and nothing. The neutralization hands SyncAll the
# tick's budget, which is the single-budget version that failed a stream
# creation in durable_streams on a handle that cleared moments later.
run_guard_windows "a barrier waits longer than a tick" commitlog.go   '	return l.checkpointHW(waitedOnRetryBudget)' '	return l.checkpointHW(tickWriteRetryBudget)'   '^TestSyncAllRidesOutAHandleTheTickWouldGiveUpOn$'

# Opening an offloaded tier reads NOTHING the manifest names -- not the log
# objects, not the block tables. The manifest entry carries blockMode, position
# and physPosition; the block table is its own object, fetched by the segments
# somebody actually reads. The neutralization fetches every one of them on open,
# which is the shape initPositions had -- 22MB downloaded for a 22-segment tier
# before serving a single read.
run_guard "opening an offloaded segment builds no block table" segment.go   '	s.blocksPending = meta.BlockMode
' '	s.blocksPending = meta.BlockMode
	if err := s.ensureBlocksLoaded(); err != nil {
		return nil, errors.Wrap(err, "eager block table")
	}
'   '^TestOpeningAnOffloadedTierReadsNoLogObjects$'

# ensureBlocksLoaded runs BEFORE the read lock, at every entry that maps a logical
# offset onto the file -- and findEntry is one of them, because it reaches
# readAtLocked through scanForward under a lock it already holds and so never
# passes ReadAt. Missing this call was a real bug while the lazy table was
# written, and it surfaced as "entry not found", saying nothing about blocks.
run_guard "findEntry builds the block table it is about to scan" segment.go   '	// Before the read lock: a block-mode search runs scanForward, which reads
	// through readAtLocked and so cannot build the table itself.
	if err := s.ensureBlocksLoaded(); err != nil {
		return nil, err
	}
	s.RLock()'   '	s.RLock()'   '^TestOpeningAnOffloadedTierReadsNoLogObjects$'

# A walk at open keeps what it built. seal() covers the log that closed cleanly --
# closeSegment ends in seal(), so even the active segment gets a sidecar -- but the
# open AFTER a crash has no close behind it, and without this write the next open
# walks the same chain, and the one after that. The neutralization makes the write
# unreachable while still compiling, which is the state every open was in before.
run_guard "a walk at open keeps the table it built" segment.go   '	if s.blocksWalked > 0 {' '	if s.blocksWalked < 0 {'   '^TestAWalkAtOpenPersistsTheTableItBuilt$'

# dirtyIndex means "these bytes are on stable storage", and seal must only say so
# when the flush it just attempted RETURNED NIL -- the error is discarded by
# design, so an unconditional clear asserts a durability that did not happen. It
# was harmless only while closeSegment fsynced every index again on the way out;
# now that close honours the mark, a failed seal would skip the last chance to
# get those bytes down, and nothing repairs a short index on a SEALED segment.
# The neutralization is the exact line that shipped before the fix.
run_guard "seal keeps the dirty mark when the flush fails" segment.go   '	if s.Index.Sync() == nil {' '	if s.Index.Sync() != nil || true {'   '^TestSealKeepsTheDirtyMarkWhenTheFlushFails$'

# The other half: close must actually consult the mark, or the fsync it exists to
# skip is paid on every segment forever. Neutralizing to the unconditional Close
# is what the code did before, so the test that asserts a clean segment is closed
# WITHOUT a flush has to go red.
run_guard "close honours the index dirty mark" segment.go   '		case s.dirtyIndex:' '		case s.dirtyIndex || true:'   '^TestACleanIndexIsClosedWithoutAFlush$'

# A block table is refused when damaged, never approximated and never rebuilt by
# walking the object -- that walk is the cost the table exists to remove, so a
# fallback would turn damage into a slow success. The neutralization drops the
# checksum, which is the check that catches a flipped byte the length fields
# still agree with.
run_guard "a damaged block table is refused" block_table.go   '	if got, exp := crc32.ChecksumIEEE(body), encoding.Uint32(buf[want-4:]); got != exp {' '	if got, exp := crc32.ChecksumIEEE(body), crc32.ChecksumIEEE(body); got != exp {'   '^TestADamagedBlockTableIsRefused$'

# BlockMode and BlocksKey are one fact stored twice, and past readTierManifest
# they are read by different code at different times -- so the pair is checked
# where it ARRIVES, like the unknown codec. A block-compressed entry naming no
# table is a segment nothing can read without rebuilding its table.
run_guard "a block entry names its block table" manifest.go   '		if o.BlockMode != (o.BlocksKey != "") {' '		if false {'   '^TestAManifestEntryPairsBlockModeWithABlockTable$'

# A cleaner tick cleans whether or not it rolled a segment. It used to return
# early on a roll, on the premise that the cleaner "already ran" -- it does not,
# and Clean has exactly one caller. The neutralization is that premise put back:
# a log with MaxSegmentAge at or below CleanerInterval then has a roll pending at
# every tick and never compacts at all, which is a 4.5GB log and 66 empty ticks.
run_guard "a rolling tick still cleans" clean.go   '	if _, err := l.checkAndPerformSplitLocked(); err != nil {' '	split, err := l.checkAndPerformSplitLocked()
	if split {
		return
	}
	if err != nil {'   '^TestATickThatRollsASegmentStillCleans$'

# A failed shrink leaves the index READABLE. shrink must unmap before it can
# truncate on Windows, so the truncate fails inside a window where the mapping is
# already gone; returning straight out left no mapping behind a non-zero
# position. Nothing re-opens a segment's index, so that is permanent -- every
# read of the segment answers "corrupt index file" for the life of the process,
# and seal discards this error by design. The neutralization is that early
# return put back.
run_guard_windows "a failed shrink leaves the index readable" index_mmap_windows.go   '		return errors.Wrap(stderrors.Join(err, idx.restoreMapping(remap)),
			"truncate failed during shrink")' '		return errors.Wrap(err, "truncate failed during shrink")'   '^TestAFailedShrinkLeavesTheIndexReadable$'

# An expansion asks the MAPPING whether there is room, not the recorded size.
# The two agree only while every expansion completes -- the file grows, then the
# mapping is torn down and rebuilt, and both of those can fail. The
# neutralization is the old test put back: after a refused remap, size still
# claims the room, the next write skips the expansion it needs, and slicing the
# mapping panics inside a library, in the caller's goroutine.
run_guard "index expansion asks the mapping" index.go   '	if pSize := int64(len(p)); offset+pSize >= idx.size || offset+pSize > int64(len(idx.mmap)) {' '	if pSize := int64(len(p)); offset+pSize >= idx.size {'   '^TestAFailedRemapLeavesTheIndexCoherent$'

# Close reports a failure only AFTER releasing the mapping and the handle -- a
# mapped or open index file cannot be unlinked on Windows, so an early return
# makes the segment permanently undeletable. The rule is in closeIndex's own
# comment; it was applied to the flush and not to the shrink after it. The
# neutralization is that early return put back.
run_guard_windows "a failed close still frees the handle" index.go   '	if durable && stderrors.Join(errs...) == nil {
		errs = append(errs, idx.shrink())
	}' '	if durable && stderrors.Join(errs...) == nil {
		if err := idx.shrink(); err != nil {
			return err
		}
	}'   '^TestAFailedShrinkOnCloseStillReleasesTheHandle$'

# A failed Replace puts back what it tore down. The pass that calls it publishes
# nothing on the way out, so a segment left closed stays in the LIVE list, and
# current() hands it to readers as usable because the link that would redirect
# them is written only on success. The neutralization is the bare early return:
# the source segment then answers ErrSegmentClosed for the life of the process.
run_guard_windows "a failed replace reopens the source" segment.go   '	if err := os.Rename(s.logPath(), old.logPath()); err != nil {
		return stderrors.Join(err, old.reopenLocked())
	}' '	if err := os.Rename(s.logPath(), old.logPath()); err != nil {
		return err
	}'   '^TestAFailedReplaceLeavesTheSourceReadable$'

# The offload attach swaps in the store backing whatever the local cleanup
# reports. The commit already happened IN THE STORE -- the caller published the
# manifest naming these objects before calling -- so nothing below can make
# staying local correct. The neutralization is an early return on the removal:
# the segment then holds a closed local backing and no store, against a manifest
# entry that already says its bytes are in the tier, and every read of it fails
# until the process restarts.
run_guard_windows "a failed local cleanup still offloads" segment.go   '		errs = append(errs, errors.Wrap(err, "remove local log"))' '		return errors.Wrap(err, "remove local log")'   '^TestAFailedLocalCleanupStillLeavesTheSegmentOffloaded$'

# A persisted block table is believed only if it accounts for exactly the bytes
# in the file beside it. That check is the WHOLE staleness story -- the sidecar
# carries no generation of its own -- and it is the kind whose absence is silent:
# a table describing different bytes still decodes, still passes its CRC, and
# maps logical offsets onto the wrong records, so the segment answers reads with
# plausible garbage rather than an error. The neutralization keeps the decode and
# drops the extent comparison.
run_guard "a block table must fit its file" block_table_local.go   '	logical, phys := blockTableExtent(blocks)
	if phys != physSize {
		return nil, 0, false
	}
	return blocks, logical, true' '	logical, _ = blockTableExtent(blocks)
	return blocks, logical, true'   '^TestABlockTableForDifferentBytesIsRefused$'

# The two places a block table is dropped because the bytes it describes stopped
# being there. Both are defence in depth over the extent check above -- and both
# exist because that check can be satisfied by accident: a rewrite that dropped
# nothing lands on the same size, and an offload deletes the file entirely. They
# are one-line calls with no return value, which is exactly the kind of thing a
# refactor removes without anything going red.
run_guard "installing a rewrite drops the old table" segment.go   '	removeLocalBlockTable(s)
	removeLocalBlockTable(old)
	s.suffix = ""' '	s.suffix = ""'   '^TestInstallingARewriteDropsTheReplacedBlockTable$'

run_guard "offloading drops the local table" segment.go   '	removeLocalBlockTable(s)

	s.backing = sb' '	s.backing = sb'   '^TestOffloadingASegmentRemovesItsLocalBlockTable$'

# An index with no .log beside it is the NORMAL resting state of an offloaded
# segment, not damage, so open()'s orphan sweep asks the manifest before it
# removes one. Without the consult the sweep looks entirely reasonable -- it
# still only deletes indexes with no log -- and it silently collects the local
# index of every tiered segment on every boot, costing a download of the index
# object on the next read of each. Nothing else notices: the log opens, serves,
# and is correct.
run_guard "an orphan sweep asks the manifest" commitlog.go   '		if !hasLog[stem] && (convErr != nil || !offloaded[int64(base)]) {' '		if !hasLog[stem] && (convErr != nil || !offloaded[int64(base)] || true) {'   '^TestAnOffloadedSegmentsIndexSurvivesOpen$'

# Five options are defaulted by a test for ZERO, so a negative passes the arm
# meant to catch a missing value. Two then panic on a background ticker
# (NewTicker), one panics in make(chan), and MaxSegmentBytes hangs -- none of
# them at the call that set the option. Without this loop New returns a working
# log and the process dies later, somewhere else.
run_guard "a negative option is refused" commitlog.go   '		if c.bad {' '		if false {'   '^TestNegativeOptionsAreRefused$'

# Zero LocalRetentionAge means NEVER offload, and it is the value every log that
# has not opted in is carrying. Lose this arm and horizon becomes "now minus
# nothing", every sealed segment is older than it, and the first clean of every
# log with a SegmentStore pushes the whole thing to the store -- no error, no
# data loss, just every log in the process silently offloading itself.
run_guard "zero local retention never offloads" clean.go   '	if l.Options.LocalRetentionAge <= 0 || !l.hasTier() {' '	if !l.hasTier() {'   '^TestAZeroLocalRetentionAgeNeverOffloads$'

# Removing this returns the walk to adding cLen to pos unchecked, which is what
# let a block overrunning the file be listed as healthy while Records refused the
# same bytes.
run_guard "a block payload must be in the file" inspect.go   '		if end := start + int64(cLen); end > int64(len(s.raw)) {' '		if false {'   '^TestBlocksAndRecordsAgreeOnATruncatedPayload$'

# Neutralized to the laundering answer the check exists to refuse — reporting a
# version the file does not contain — rather than to a no-op, so the guard fails
# for the reason it is about.
run_guard "a magic with no version is refused" inspect.go   '		if hdr[0] == blockMagic {' '		if false {'   '^TestClassifySegmentRefusesAMagicWithNoVersion$'

# Neutralized to a reference rather than a deletion so the method stays used and
# the package still builds: what must fail is the ASSERTION that a reopened log
# reclaims bytes, not the compile.
run_guard "a log cleans at open" clean.go   '	l.cleanAtOpen()' '	_ = l.cleanAtOpen'   '^TestALogCleansAtOpenWithoutWaitingForATick$'
run_guard "a ceiling needs the auto cleaner off" clean.go   '	if _, ok := spec.Ceiling.Get(); ok && !l.DisableAutoClean {' '	if false {'   '^TestACeilingOnAnAutoCleaningLogIsRefused$'


# A named tier with a budget of 0 is refused. Neutralized to `d < 0`, which is
# unreachable and keeps `d` used so the package still builds -- a `false` here
# orphans it and the harness reports a build error instead of an uncovered
# guard. Without the refusal, budgetFor's fallback arm swallows the value: the
# branch a caller reaches by saying NOTHING is also the one reached by saying 0,
# so an explicitly-supplied budget becomes unreachable.
run_guard "a tier budget of zero is refused" clean.go   '		if d == 0 {' '		if d < 0 {'   '^TestATierBudgetOfZeroIsRefused$'

# And the refusal stays about the VALUE. Neutralized to `d >= 0`, which fires on
# every entry -- stricter-looking and wrong, because absence from TierBudgets is
# the documented way to ask for RewriteBudget, so a refusal this wide breaks the
# caller who spelled the default correctly.
run_guard "a tier budget refusal is about the value" clean.go   '		if d == 0 {' '		if d >= 0 {'   '^TestATierAbsentFromTierBudgetsStillRuns$'
# The store's read side and its commit point are ONE window seen from two ends:
# a publish renames over the object path, and on Windows that fails the reader's
# open and a reader fails the publisher's rename. Guarded separately because
# each retry is removable on its own, and removing either moves the error rather
# than removing it.
#
# ReadAt's anchor spans three lines because its `openWithRetry(path)` line is
# byte-identical to Stream's, and apply_edit refuses an ambiguous match rather
# than neutralizing whichever it finds first.
run_guard_windows "store read retries a held object" segment_store.go   $'\tf, err := openWithRetry(path)\n\tif os.IsNotExist(err) {\n\t\treturn 0, ErrObjectNotFound' $'\tf, err := os.Open(path)\n\tif os.IsNotExist(err) {\n\t\treturn 0, ErrObjectNotFound'   '^TestAStoreReadRetriesThroughAHeldObject$'
run_guard_windows "store publish retries a held dest" segment_store.go   '	if err := renameWithRetry(tmp, path); err != nil {' '	if err := os.Rename(tmp, path); err != nil {'   '^TestAStorePublishRetriesThroughAHeldDestination$'

# A version 3 manifest names the tier of every object. Neutralized to `if false`
# rather than deleting the loop: what must fail is the refusal, not the compile,
# and the loop variable is still referenced by the body it no longer reaches.
run_guard "a manifest entry names its tier" manifest.go   '		if o.Tier == "" {' '		if false {'   '^TestAManifestEntryWithNoTierIsRefused$'

# A tier chain New cannot honour is refused at New. Both arms neutralize
# to `if false` so the package still builds and what fails is the refusal.
run_guard "a tier name is unique" tier.go   '		if seen[t.Name] {' '		if false {'   '^TestAChainNewCannotHonourIsRefused$'
run_guard "a tier must be named" tier.go   '		if t.Name == "" {' '		if false {'   '^TestAChainNewCannotHonourIsRefused$'
# Neutralized by making the lookup answer for ANY name — which is precisely the
# fallback the refusal exists to prevent, rather than a deletion that would take
# the return value with it.
run_guard "an object's tier must be configured" tier.go   '		if t.Name == name {' '		if true {'   '^TestAnObjectNamingAnUnconfiguredTierIsRefused$'

# Two tiers claiming one segment. Neutralized by not RECORDING the owner rather
# than by removing the check: the check reads a map the loop fills, so an empty
# map disarms it exactly, and the loop variable stays used so the package still
# builds.
run_guard "one segment lives in one tier" manifest.go   '			claims[o.BaseOffset] = append(claims[o.BaseOffset], tierClaim{tier: tier, obj: o})' '			claims[o.BaseOffset] = []tierClaim{{tier: tier, obj: o}}'   '^TestTwoTiersClaimingOneSegmentIsRefused$'

# An interrupted move is the one double claim that resolves, and it must both
# resolve and stay narrow. Two guards, from opposite directions: refusing
# everything loses the recovery, and resolving everything loses the refusal.
run_guard "an interrupted move resolves" manifest.go   '	if len(claims) != 2 {' '	if true {'   '^TestAnInterruptedMoveResolvesToTheDestination$'
run_guard "only a move resolves a double claim" manifest.go   '	case a.obj.MovedFrom == b.tier && b.obj.MovedFrom != a.tier:' '	case true:'   '^TestOnlyAMoveResolvesADoubleClaim$'

# A block-compressed offloaded segment has THREE objects. The orphan sweep
# builds its live set twice over — from the manifest and from the log's own
# segments — and both halves named the log and the index only, so neither could
# cover for the other. Guarded separately for that reason: each half is
# removable on its own and the other does not notice.
run_guard "a manifest block table is live" tier_state.go   '			if o.BlocksKey != "" {' '			if false {'   '^TestABlockTableIsNotGarbage$'
run_guard "a segment block table is live" tier_state.go   '		if s.blocksKey != "" {' '		if false {'   '^TestABlockTableTheLogIsReadingIsNotGarbage$'

# Retention is per tier, and deletion is a PREFIX operation on the whole log. A
# newer tier deleting while an older one still holds something does not shorten
# the log, it punches a hole out of its middle. Two guards because the two ways
# a run survives — its own budget, and this log not owning it — set the same
# flag from different branches, so either can be removed without the other
# noticing.
run_guard "a surviving tier blocks the newer ones" delete_cleaner.go   $'\t\tif len(survivors) > 0 {\n\t\t\tblocked = true\n\t\t}' ''   '^TestANewerTierWaitsForTheOlderToDrain$'
# The skip branch's flag alone: the append stays so the segments are still
# handed back and what changes is only whether the runs above may delete.
run_guard "a read-only tier blocks the newer ones" delete_cleaner.go   $'\t\t\tkept = append(kept, r.segments...)\n\t\t\tblocked = true' $'\t\t\tkept = append(kept, r.segments...)'   '^TestAReadOnlyTierHoldsTheTiersAboveIt$'
# Neutralized by making the lookup answer for ANY name, the same way the
# storeForTier guard is: a deletion would take the return value with it, and
# answering for any name IS the silent-default fallback the refusal prevents.
run_guard "a segment's tier must be configured" delete_cleaner.go   '		if t.Name == name {' '		if true {'   '^TestASegmentNamingAnUnconfiguredTierIsRefused$'
# One floor allowance for the whole tiered half. Renewing it per run lets a
# two-tier chain delete twice what the caller protected. Neutralized to a
# discard rather than a deletion, which would take `before` with it and fail to
# build instead of failing the test.
run_guard "the floor is spent across tiers" delete_cleaner.go   '		maxDrop -= before - len(survivors)' '		_ = before'   '^TestTheFloorIsSpentAcrossTiersNotPerTier$'

# A move commits the destination before releasing the source, and the marker is
# what makes the window between them survivable. Without it a crash there
# leaves a log that will not open — so the guard's test is a REOPEN, not an
# inspection of the manifest.
run_guard "a move says where it came from" tier_move.go   '	entry.MovedFrom = src' '	entry.MovedFrom = ""'   '^TestAMoveInterruptedAfterItsCommitStillOpens$'
# Both ends of a move are checked before any bytes are copied. Neutralized one
# end at a time: the destination check covers for the source's otherwise, since
# a move a log may not make usually fails at whichever end is asked first.
run_guard "a move may not write the destination" tier_move.go   '	if !l.tierWritable(dst.Name) {' '	if false {'   '^TestAMoveIntoATierThisLogDoesNotOwnIsRefused$'
run_guard "a move may not release the source" tier_move.go   '	if !l.tierWritable(src) {' '	if false {'   '^TestAMoveOutOfATierThisLogDoesNotOwnIsRefused$'
# A placement naming a tier that is not configured stops the pass before
# anything moves. Neutralized by making the lookup answer for ANY name, the way
# the other two tier lookups are: deleting the refusal takes `found` out of use
# and fails to build instead of failing the test.
# The release half of a move publishes exactly ONE tier's manifest. Neutralizing
# the lookup makes a name this log has no tier for publish nothing and report
# success -- and the destination's manifest is already out, so both tiers go on
# claiming the segment permanently. "" is the case that bites: it is what an
# absent Tier field decodes to, and it used to be the publish path's word for
# "every tier".
run_guard "a one-tier publish names a real tier" manifest.go   '	t, err := l.tierByName(tier)' '	t, err := Tier{Name: tier}, error(nil)'   '^TestAOneTierPublishRefusesATierThisLogDoesNotHave$'

run_guard "a placement's tier must be configured" tier_move.go   '		t, err := l.tierByName(name)
		if err != nil {'   '		t, _ := l.tierByName(name)
		if false {'   '^TestAPlacementNamingAnUnconfiguredTierIsRefused$'
# DeleteStoreObjects checks every object's tier before deleting any of them.
# Neutralized by folding the check back into the delete loop, which is the
# order-dependent version this replaced: the batch runs until it REACHES an
# object it may not touch, so what survives depends on the caller's sort.
run_guard "a refused delete batch deletes nothing" tier_state.go   '	stores := make([]SegmentStore, len(objs))
	for i, o := range objs {
		if !l.tierWritable(o.Tier) {
			return nil, errors.Wrapf(errTierReadOnly, "tier %s", o.Tier)
		}
		store, err := l.storeForTier(o.Tier)
		if err != nil {
			return nil, err
		}
		stores[i] = store
	}
	deleted := make([]StoreObject, 0, len(objs))
	for i, o := range objs {'   '	stores := make([]SegmentStore, len(objs))
	deleted := make([]StoreObject, 0, len(objs))
	for i, o := range objs {
		if !l.tierWritable(o.Tier) {
			return deleted, errors.Wrapf(errTierReadOnly, "tier %s", o.Tier)
		}
		store, err := l.storeForTier(o.Tier)
		if err != nil {
			return deleted, err
		}
		stores[i] = store'   '^TestDeleteStoreObjectsRefusesTheWholeBatchWhateverItsOrder$'

# Each tier draws on its own rewrite budget. Neutralized by keying every tier's
# budget the same, which is exactly the single shared budget this replaced.
run_guard "each tier draws its own budget" compact_cleaner.go   '			b = budgetFor(segments[i].tier)' '			b = budgetFor("")'   '^TestOneTiersBudgetDoesNotShrinkAnothers$'

# AppendMessageSet checks the caller's offsets against the tail. Neutralized by
# accepting whatever arrives, which is what the path did before: a set starting
# at or below the tail written verbatim, and the log holding one offset twice.
run_guard "an appended set is checked against the tail" commitlog.go   '	if err := checkAppendedSet(segment.NextOffset()-1, entries); err != nil {
		return nil, err
	}' '	if false {
		return nil, nil
	}'   '^TestAppendMessageSetRefusesOffsetsThatDoNotFitTheTail$'

# The segment's tail only ever moves forward. Neutralized by the assignment it
# used to be, which lowers the field NextOffset is derived from.
run_guard "a segment tail only moves forward" segment.go   '	if last.Offset > s.lastOffset {
		s.lastOffset = last.Offset
		s.lastWriteTime = last.Timestamp
	}' '	s.lastOffset = last.Offset
	s.lastWriteTime = last.Timestamp'   '^TestASegmentsTailNeverMovesBackwards$'

# An empty entry list is refused BEFORE the payload is written. Neutralized by
# dropping the check, which is a panic on a segment already appended to.
run_guard "a write with no entries is refused" segment.go   '	if len(entries) == 0 {
		return 0, errors.Wrap(ErrMessageSetRefused, "write with no entries")
	}' '	if false {
		return 0, nil
	}'   '^TestASegmentRefusesAWriteWithNoEntries$'

# A reconcile whose read of the tail failed must not report success. Neutralized
# by the silent break it used to have, which leaves torn false and falls through
# to `return nil` — a segment that opened having reconciled nothing, with
# lastOffset at the stale index tail for the next append to collide with.
run_guard "a failed tail read fails the reconcile" segment.go   '			return errors.Wrapf(err, "reconcile index tail: read frame header at %d", startPos)' '			break'   '^TestAReconcileThatCannotReadTheTailFails$'

# A torn tail is never discarded below the high watermark. Neutralized by
# dropping the floor, which is the unbounded discard: a first frame that does
# not resolve leaves startPos at 0, so the "tail" is the whole segment, the open
# succeeds, and the watermark is clamped down to the empty log it just made.
run_guard "a torn tail stops at the watermark" segment.go   '	if committedThrough >= s.BaseOffset && s.lastOffset < committedThrough {' '	if false {'   '^TestATornTailIsNotDiscardedBelowTheWatermark$'

# An unparseable block header refuses the open at ANY offset. Neutralized by the
# break it used to share with a torn tail, which discards from that point to the
# end of the file: at byte 0 that is every record in the segment, and mid-file it
# is every record past the flipped byte, acknowledged ones included. Either way
# the open succeeded and the watermark was clamped down to match.
run_guard "a corrupt block header refuses the open" segment.go   '			return errors.Wrapf(err, "block header at byte %d of %d", phys, size)' '			break'   '^TestACorruptBlockHeaderIsNotATornTail$'

# A timestamp lookup searches the segment that currently HOLDS the records, not
# the one the published list names. Neutralized by taking the slice entry as-is,
# which is what the offset path stopped doing when the same symptom was fixed for
# readers: mid-pass the list hands out replaced segments, and searching one
# answers ErrSegmentReplaced, so the lookup fails on a healthy log.
run_guard "a timestamp lookup resolves the segment" commitlog.go   '	for range readerResolveAttempts {
		seg, ok := s.current()' '	for range readerResolveAttempts {
		seg, ok := s, true'   '^TestTimestampLookupsWhileCompactionReplacesSegments$'

# The RETRY in that same helper is deliberately NOT registered here. It covers
# the window between resolving the segment and searching it, which is a few
# instructions wide: with the retry removed the reproduction passed 8 runs out of
# 8, having failed once at 3s in an earlier five-run sample. A guard whose test
# goes red one time in tens is worse than no guard — guardcheck's whole value is
# that a red means something — so the retry stands on the observed failure and on
# newSourceReader's precedent, not on a claim of coverage. See the comment on
# findEntryByTimestampResolving.

# Clean() puts the configured budget into the spec it builds. Neutralized by
# building the empty spec again, which is the unbounded automatic pass this
# replaced — and which the test named for that fix went on passing against,
# because it asserted the OPTION rather than the pass.
run_guard "the automatic pass spends its budget" clean.go   '	if l.Options.CleanRewriteBudget > 0 {
		spec.RewriteBudget = l.Options.CleanRewriteBudget
	}' '	if false {
		spec.RewriteBudget = 0
	}'   '^TestTheAutomaticCleanSpendsTheConfiguredBudget$'

# A segment created while a codec is configured is block-framed. Neutralized by
# never blocking, which is a log that silently ignores its Compression setting
# and writes raw — every message still reads back, so the test that only read
# messages back passed under exactly this break. It now asks the FILES.
run_guard "a codec makes new segments blocked" segment.go   '		s.blockMode = s.codec != compress.None' '		s.blockMode = false'   '^TestTurningCompressionOnLeavesExistingSegmentsRaw$'

# CopyTier reads the source descriptor BEFORE copying a single object, so a
# handover it is going to refuse leaves the destination untouched. Neutralized
# by moving the read after the copy: still refused, but with 300 objects already
# in a destination nothing will ever claim.
run_guard "the descriptor is read before anything is copied" copy_tier.go   '	desc, err := readStoreDescriptor(src)
	if err != nil {
		return errors.Wrap(err, "read source log descriptor")
	}

	// These three lines are the contract, and their ORDER is the contract. The
	// manifest is published last because it is the commit: until it lands,
	// nothing in dst is claimed by anything, so a copy that stops anywhere above
	// leaves collectable orphans instead of a tier missing its records.
	if err := copyTierObjects(src, dst, objs); err != nil {
		return err
	}' '	if err := copyTierObjects(src, dst, objs); err != nil {
		return err
	}

	desc, err := readStoreDescriptor(src)
	if err != nil {
		return errors.Wrap(err, "read source log descriptor")
	}'   '^TestCopyTierRefusesASourceWithNoDescriptor$'

# The high watermark only ever moves forward. Neutralized by applying every
# value, which is what OverrideHighWatermark used to do on purpose — so this
# guard also pins the deletion: bring the exception back and the rule's test is
# what notices.
run_guard "the high watermark only moves forward" commitlog.go   '	if hw > l.hw {' '	if true {'   '^TestTheHighWatermarkNeverGoesBackwards$'

# Two processes on one directory each keep their own tail and index and write
# over each other's frames. The neutralization hands back an UNHELD dirLock --
# zero value, held=false -- so release() stays a no-op and every other line in
# New and Close is unchanged. What goes away is only the claim itself.
run_guard "a live log directory is claimed" commitlog.go   '	lock, err := lockLogDir(path)' '	lock, err := &dirLock{}, error(nil)'   '^TestASecondOpenOfALiveLogDirectoryIsRefused$'

# A lock that outlived its log would make every clean shutdown leave a directory
# nothing could reopen until the process exited. The neutralization keeps the
# Join so err stays used, and drops only the release.
run_guard "closing gives the directory back" commitlog.go   '	return stderrors.Join(err, l.dirLock.release())' '	return stderrors.Join(err, error(nil))'   '^TestClosingALogReleasesItsDirectory$'

# Windows only, and the asymmetry is the whole reason it is a separate guard:
# the lock handle is opened with no sharing, so a Delete that has not released
# it cannot remove the lock file and leaves the directory standing. On unix
# flock does not stop an unlink, so the same removal succeeds either way and the
# test cannot see the difference -- it would pass with the guard removed, which
# is a guard that proves nothing.
run_guard_windows "delete releases before it removes" commitlog.go   '	if err := stderrors.Join(closeErr, l.dirLock.release()); err != nil {' '	if err := stderrors.Join(closeErr, error(nil)); err != nil {'   '^TestDeleteRemovesTheLockFileWithTheDirectory$'

# The same line, neutralized the same way, but checked against the OTHER thing
# it has to do: give the claim back when the close FAILED. A release that only
# ran on the success path would satisfy the guard above and still brick the
# directory for the life of the process, because the caller that just got an
# error drops the log and cannot retry. Not Windows-only: nothing here depends
# on unlink semantics, only on the second open being refused.
run_guard "a failed delete releases too" commitlog.go   '	if err := stderrors.Join(closeErr, l.dirLock.release()); err != nil {' '	if err := stderrors.Join(closeErr, error(nil)); err != nil {'   '^TestAFailedDeleteStillReleasesTheDirectory$'

# The neutralization IS the bug that was there: a best-effort checkpoint
# returning early and taking the mandatory segment close with it. That also
# broke Close's release invariant, since the lock goes back either way -- so
# the window between "let go of the directory" and "still holding files open
# in it" existed for as long as this early return did.
run_guard "a failed checkpoint does not abort the close" commitlog.go   '	return stderrors.Join(l.checkpointHW(waitedOnRetryBudget), l.closeSegmentsOnly())' '	if err := l.checkpointHW(waitedOnRetryBudget); err != nil {
		return err
	}
	return l.closeSegmentsOnly()'   '^TestAFailedCheckpointStillClosesTheLogAndReleasesTheDirectory$'

# The neutralization is the call Delete used to make. It reads like the more
# obvious of the two, which is exactly why this guard exists: nothing at the
# call site says the checkpoint inside closeSegments is both pointless here and
# able to fail the whole delete.
run_guard "delete does not checkpoint a directory it is removing" commitlog.go   '	closeErr := l.closeSegmentsOnly()' '	closeErr := l.closeSegments()'   '^TestDeleteDoesNotCheckpointAndCannotBeStoppedByOne$'

# A failed pass keeps the rewrites it already installed. The neutralization is
# the return that was there, and it is kept alongside the assignment so the
# variable below stays used and the mutation compiles. What it restores is a
# pass that abandons segments whose files are already renamed over their
# sources': named by nothing, so closed by nobody.
run_guard "a failed pass keeps the rewrites it installed" compact_cleaner.go   '			rewriteErr = err
			break' '			rewriteErr = err
			return nil, 0, -1, err'   '^TestAFailedCompactionPassPublishesTheRewritesItInstalled$'

# The other half, at the call site: the partial list has to be the one that gets
# swapped in. Neutralized by the line it replaced, which republished the delete
# stage's segments -- the CLOSED sources of every rewrite this pass installed.
run_guard "a partial compaction result is published" clean.go   '			return compacted, -1, err' '			return cleaned, -1, err'   '^TestAFailedCompactionPassPublishesTheRewritesItInstalled$'

# A rewrite that failed disposes of its working copy. Neutralized by keeping the
# suffix check and never acting on it, which is the state every error path but
# the scan one was in: an open, mapped .cleaned segment nothing can reach.
#
# Anchored on the disposed check above it, not on the bare condition:
# consolidateOne grew the same disposal, so `if working {` now matches twice and
# an ambiguous anchor is a SKIP rather than a failure.
run_guard "a failed rewrite drops its working copy" compact_cleaner.go   '		if disposed {
			return
		}
		cleaned.RLock()
		working := cleaned.suffix != ""
		cleaned.RUnlock()
		if working {' '		if disposed {
			return
		}
		cleaned.RLock()
		working := cleaned.suffix != ""
		cleaned.RUnlock()
		if working && false {'   '^TestAFailedCompactionPassPublishesTheRewritesItInstalled$'

# The consolidation pass must tell io.EOF from a read failure. Neutralized into
# the loop it replaced -- `for ms, _, err := ss.Scan(); err == nil; ...` -- which
# ends on either, and what follows the loop is the install. That is a truncated
# copy renamed over a segment whose original still held the records, with the
# pass returning nil. Reached on the DEFAULT config: this is the else-branch of
# `if l.Compact`.
run_guard "a consolidation read failure stops the pass" compact_cleaner.go   '			if !errors.Is(err, io.EOF) {
				return nil, fmt.Errorf("%w: consolidation of segment %d: %w",' '			if false {
				return nil, fmt.Errorf("%w: consolidation of segment %d: %w",'   '^TestConsolidationRefusesASegmentItCannotReadToTheEnd$'

# The same partial-result duty as the compaction branch, at the consolidation
# call site. Neutralized by the line that was there, which republished the delete
# stage's list and left every rewrite this pass installed named by nothing.
run_guard "a partial consolidation result is published" clean.go   '		return consolidated, -1, err' '		return cleaned, -1, err'   '^TestConsolidationRefusesASegmentItCannotReadToTheEnd$'

# The replication fetch says so when the bytes are damaged. Neutralized into the
# bare `break` it used to be, which answers a damaged segment with an EMPTY set
# and a nil error -- and the caller is a follower, which then retries the same
# offset forever, making no progress and never learning why. Only falsifiable on
# the empty case: a short set is progress and stays a success.
run_guard "a replication fetch reports damaged bytes" commitlog.go   '			if !errors.Is(err, io.EOF) && len(out) == 0 {
				return nil, fmt.Errorf("%w: message set at offset %d: %w",' '			if false {
				return nil, fmt.Errorf("%w: message set at offset %d: %w",'   '^TestReadMessageSetReportsDamageRatherThanAnEmptySet$'

# And the working-copy disposal on that path, which had none at all: every error
# return left an open, mapped .cleaned segment behind. Anchored on the defer so
# it does not collide with cleanSegment's copy above.
run_guard "a failed consolidation drops its working copy" compact_cleaner.go   '	defer func() {
		cleaned.RLock()
		working := cleaned.suffix != ""
		cleaned.RUnlock()
		if working {' '	defer func() {
		cleaned.RLock()
		working := cleaned.suffix != ""
		cleaned.RUnlock()
		if working && false {'   '^TestConsolidationRefusesASegmentItCannotReadToTheEnd$'

# The same duty on the truncation path, where the replacement is NOT yet
# installed and so has to be dropped rather than published. Neutralized by the
# bare return that was there, which left it open, mapped and named by nothing.
run_guard "a failed truncate drops its replacement" commitlog.go   '		if err := snapshot[i].Delete(); err != nil {
			dropReplacement()' '		if err := snapshot[i].Delete(); err != nil {'   '^TestAFailedTruncateDropsTheReplacementItBuilt$'

# A committed reader reports a watermark it cannot locate. Neutralized into the
# shape the three inline copies had: the error is dropped and the caller carries
# on with the watermark it already held. Nothing looks wrong from inside -- the
# damage is that readMessage then re-parses the caller's buffer, which still
# holds the previous header, and parks on a payload already served. The test can
# only end by deadline in that state, which is why it carries one.
run_guard "a committed read reports a watermark it cannot find" reader.go   '	hwSeg, hwPos, err := getHWPos(segments, r.hw)
	if err != nil {
		return nil, err
	}' '	hwSeg, hwPos, err := getHWPos(segments, r.hw)
	if err != nil {
		return segments, nil
	}'   '^TestACommittedReadReportsALostWatermarkRatherThanCorruption$'

# A header read takes the header, not the whole buffer it was handed. Neutralized
# back to reading headersBuf entire, which is what turns a buffer bigger than a
# header into a stream off by the difference -- and then reports the healthy
# record that follows as corrupt.
run_guard "a header read stops at the header" reader.go   '	hdr := headersBuf[:msgSetHeaderLen]
	if _, err := reader.Read(ctx, hdr); err != nil {' '	hdr := headersBuf[:msgSetHeaderLen]
	if _, err := reader.Read(ctx, headersBuf); err != nil {'   '^TestAnOversizedHeaderBufferReadsOnlyTheHeader$'

# The metadata path's header parse is bounds-checked. Neutralized by handing back
# the buffer's own length as the limit, which is what an unchecked walk amounts
# to: every take succeeds, and the slice expression underneath does the panicking
# instead. This is the one parse in the package running on bytes no checksum has
# vouched for.
run_guard "a metadata header parse is bounds-checked" message_set.go   '	if n < 0 || c.n+n < c.n || c.n+n > int64(len(c.buf)) {' '	if false {'   '^TestMetadataReadRefusesACorruptRecordRatherThanPanicking$'

# ---- segment join ----

# A join reads each input to its end and then DELETES it, so a walk that mistook
# a read failure for end-of-data would write a prefix and unlink the file holding
# the rest. Loudest of all the scan sites for that reason: the rewrite paths at
# least leave the damaged bytes under the source's name, and this one collects
# them.
run_guard "a join read failure stops the pass" clean_join.go   '				if !errors.Is(err, io.EOF) {
					return nil, nil, fmt.Errorf("%w: join of segment %d: %w",' '				if false && !errors.Is(err, io.EOF) {
					return nil, nil, fmt.Errorf("%w: join of segment %d: %w",'   '^TestAJoinRefusesAnInputItCannotReadToTheEnd$'

# The working-copy disposal on the join path. Anchored on `joined` so it does not
# collide with the two identically-shaped defers in compact_cleaner.go.
run_guard "a failed join drops its working copy" clean_join.go   '		joined.RLock()
		working := joined.suffix != ""
		joined.RUnlock()
		if working {' '		joined.RLock()
		working := joined.suffix != ""
		joined.RUnlock()
		if working && false {'   '^TestAJoinRefusesAnInputItCannotReadToTheEnd$'

# An input a join did not rename over must leave WITH a link. Marked as left and
# carrying none is the retention case — reader, skip me, those records are gone —
# and taking that path here skips records sitting in the result. Neutralized by
# Delete alone, which sets `left` and no link, which is exactly the wrong half.
run_guard "a joined-away segment links to the result" clean_join.go   '		in.SupersededBy(joined)' '		in.left = true'   '^TestAJoinCarriesEveryRecordOfTheRun$'

# A run never crosses a tier boundary: a join is an optimisation, and one that
# copies bytes between stores is not one. The planner is where that is decided.
run_guard "a join run never crosses a tier boundary" clean_join.go   '		if open && cur.tiered == tiered && cur.tier == tier && cur.bytes+size <= cap {' '		if open && cur.bytes+size <= cap {'   '^TestPlanJoinsGroupsAdjacentSegmentsWithinTheCap$'

# A segment that cannot be joined ENDS the run before it rather than being
# skipped: a run is adjacent by definition, and jumping over one would produce a
# segment whose offset range CONTAINS records it does not hold — which
# findSegment, bounding only from above, would resolve into and find nothing.
run_guard "an unjoinable segment breaks the run" clean_join.go   '		if cap <= 0 || size >= cap {
			flush()
			continue
		}' '		if cap <= 0 || size >= cap {
			continue
		}'   '^TestPlanJoinsGroupsAdjacentSegmentsWithinTheCap$'

# Retiring a joined-away input takes it OUT of the tier, which is what stops
# tierState reporting it. Neutralized by leaving the fields set, which looks
# harmless -- the segment is marked left and linked, and the pass splices it away
# at the end. It is not: a segment stays in l.segments until that splice, so the
# NEXT run's commit rebuilds the manifest from a view that still contains this
# one and republishes an entry for objects already queued for reclamation.
run_guard "a retired join input leaves the tier" segment.go   '	s.store = nil
	s.storeKey, s.indexKey, s.blocksKey = "", "", ""' '	_ = s.storeKey'   '^TestATieredJoinCommitsInOneManifestWrite$'

# A joined-away tiered input's objects are QUEUED, not deleted. Neutralized into
# the call that reads as the obvious way to retire a segment and is the one thing
# this path must not do: Delete on an offloaded segment goes straight to
# store.Delete, and a join holds every input's backing open until after the
# install, so the objects it absorbs are precisely the ones something may still
# be reading.
run_guard "a joined-away tiered input is queued, not deleted" clean_join.go   '		superseded = append(superseded, in.retireIntoJoin(first)...)' '		in.SupersededBy(first)
		_ = in.Delete()'   '^TestATieredJoinQueuesItsInputsObjectsRatherThanDeletingThem$'

# A tiered run is refused in a store this log does not own. Neutralized by
# dropping the ownership half and keeping the wiring half, which is the reading
# that looks equivalent -- TierJoinBelow already has to name the tier for a run
# to be planned. It is not equivalent: absence refuses only the callers who did
# not ask, and this refuses the one who did.
run_guard "a join refuses a tier this log does not own" clean_join.go   '	return t.writable != nil && t.commit != nil && t.writable(tier)' '	return t.writable != nil && t.commit != nil'   '^TestATieredJoinRefusesATierThisLogDoesNotOwn$'

# The tiered commit names the JOINED extent. Neutralized to the first input's own
# span, which is what a manifest write that retired the run's other inputs before
# the result could cover them would publish: every entry present and consistent,
# and every record above the first input named by nothing.
run_guard "a tiered join commits the joined extent" clean_join.go   '	if err := tj.commit(retired, meta.tierObject(first.BaseOffset, tier)); err != nil {' '	mutantEntry := meta.tierObject(first.BaseOffset, tier)
	mutantEntry.LastOffset = mutantEntry.FirstOffset
	if err := tj.commit(retired, mutantEntry); err != nil {'   '^TestATieredJoinCommitsInOneManifestWrite$'

# A join that could not commit hands back NO reclaim entries. Neutralized to the
# reading that looks like careful bookkeeping -- the upload produced them, so
# pass them on -- and is the one that deletes live data. They name the first
# input's CURRENT objects, and they only stop being current when swapReplacement
# repoints the segment away from them. drainReclaim's safety rests on "for a
# superseded backing, refs can only fall", which holds only for entries a swap
# put there; nothing swapped here, so a refcount of zero is an ordinary lull and
# the delete lands on the object the log is reading.
run_guard "a join that cannot commit queues nothing" clean_join.go   '		return nil, nil, errors.Wrapf(err, "commit joined run at %d", first.BaseOffset)' '		return nil, superseded, errors.Wrapf(err, "commit joined run at %d", first.BaseOffset)'   '^TestATieredJoinThatCannotCommitQueuesNothing$'

# ---- tiered rewrite ----

# A rewrite publishes its pending entry under the segment's OWN tier. Neutralized
# back to the hard-coded defaultTierName that shipped, which is not a misfiling:
# the override is keyed by base offset, so it CONSUMES the correct tierState
# entry, and the tier's manifest goes out naming neither the old objects nor the
# new. The end-of-pass republish rebuilds from tierState and repairs it, so the
# test has to watch the manifests the pass publishes along the way rather than
# the one it settles on -- and every other tiered test misses it outright,
# because oneTier names its tier the very thing that was hard-coded.
# Anchored on the ASSIGNMENT, not the call: swapping the argument would leave
# segTier declared and unused, and the harness would report a build error instead
# of an uncovered guard.
run_guard "a rewrite publishes under its own tier" compact_cleaner.go   '		segTier := seg.tier' '		segTier := defaultTierName'   '^TestNoManifestAPassPublishesEverDropsALiveSegment$'

echo
if [ "$failures" -ne 0 ]; then
  echo "guardcheck: $failures of $checked guard(s) are NOT covered by the test named for them."
  exit 1
fi
# A run that checked nothing prints the same green as one that checked
# everything, and this is the mode where that is likely: GUARDCHECK_SET=platform
# exists to check the guards no other runner can, so if it finds none, either
# they moved off this OS or the job is on the wrong runner. Either way its green
# would be covering for guards nobody is checking at all.
if [ "$set_sel" = "platform" ] && [ "$checked" -eq 0 ]; then
  echo "guardcheck: GUARDCHECK_SET=platform ran NO guards on $goos — this run proves nothing."
  exit 1
fi
# The same hole, reached the other way. The filter is a plain substring match,
# so a caller who reaches for a regex — "the old table|the local table" — selects
# nothing, and an empty selection printed the header, no summary, and exit 0.
# Indistinguishable from a run that checked every guard it was asked for. Count
# the deferred ones as selected: a filter naming only a windows guard on linux
# picked something out, it just isn't checkable here, and the deferral line
# already says so.
if [ -n "$filter" ] && [ $((checked + deferred)) -eq 0 ]; then
  echo "guardcheck: no guard name contains '$filter' — the filter is a substring, not a regex."
  exit 1
fi
# The same hole a third time, and the one that actually bit: a guard appended to
# the END of this file, after the summary block, never runs. It is not skipped
# loudly — it is not reached at all, so it is absent from $checked, absent from
# $failures, and the run exits green having proved nothing about it. Five join
# guards were added that way, and one of them was BROKEN; both this script and
# the CI job that runs it reported success.
#
# So the script counts its own declarations and requires that every one of them
# was reached. Self-maintaining on purpose: a count written down by hand is one
# more thing to forget to bump, and forgetting would fail exactly the runs that
# were fine.
#
# Counted across every form that declares one — run_guard, run_guard_windows and
# run_guard_pair — because counting only the plain one gets a number SMALLER than
# the guards that ran, which fails every healthy run instead of the broken one.
# Skipped for a filtered run, which selects a subset by design, and for
# GUARDCHECK_SET=platform, which skips every guard with no platform requirement
# and is already covered by its own check above.
declared=$(grep -cE '^run_guard(_windows|_pair)? ' "$0")
if [ -z "$filter" ] && [ "$set_sel" != "platform" ] && [ $((checked + deferred)) -ne "$declared" ]; then
  echo "guardcheck: $declared guards are declared but $((checked + deferred)) ran."
  echo "A run_guard line below the summary block never executes — move it up."
  exit 1
fi
if [ "$checked" -gt 0 ]; then
  echo "guardcheck: all $checked guards covered."
fi
if [ "$deferred" -ne 0 ]; then
  echo "guardcheck: $deferred guard(s) NOT covered here — deferred to another platform's run."
fi
