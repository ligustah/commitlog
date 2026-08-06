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

run_guard "frame-header CRC" reader.go   'if want, got := storedHeaderCrc(headersBuf), headerCrc(headersBuf); want != got {'   'if want, got := storedHeaderCrc(headersBuf), headerCrc(headersBuf); want != got && false {'   '^FuzzCorruptFrameHeaderIsNeverServedAsTruth$'

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
			return err
		}
		deleted++
	}'   '	deleted := 0
	l.mu.Lock()
	for i := idx + 1; i < len(snapshot); i++ {
		if err := snapshot[i].Delete(); err != nil {
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
run_guard "reader advances past its segment" util.go   '	next, _ := findSegment(segments, seg.NextOffset())'   '	next := findSegmentByBaseOffset(segments, seg.BaseOffset+1)'   '^TestASegmentAdvanceSkipsTheTrimOfTheSegmentJustRead$'

# A leader epoch arriving from the checkpoint file must be parsed as UNSIGNED.
# The neutralization restores the old parse-then-convert, which is the whole bug:
# "-1" is a valid int64, becomes 2^64-1 as a uint64, and that is a well-formed
# epoch higher than any a leader will ever assign -- so a corrupt file opens
# cleanly and pins latestEpoch() at the ceiling for good. Written as two lines so
# the package still COMPILES without the guard; a build failure would be reported
# as a harness error, not as coverage.
run_guard "leader epoch parsed unsigned" leader_epoch_cache.go   '		leaderEpoch, err := strconv.ParseUint(scanner.Text(), 10, 64)'   '		signedEpoch, err := strconv.ParseInt(scanner.Text(), 10, 64)
		leaderEpoch := uint64(signedEpoch)'   '^TestALeaderEpochCheckpointWithANegativeEpochIsRefused$'

# A store key names an object INSIDE the store. The manifest is the one place
# keys arrive from outside, and they become an action rather than a description:
# s.storeKey is handed to store.Delete. Neutralizing the boundary check lets a
# manifest name "../../x", which for FileSegmentStore is an os.Remove of a file
# the store never held -- filepath.Join CLEANS the traversal rather than refusing
# it, which is what made the escape silent.
run_guard "manifest refuses a foreign key" manifest.go   '		if err := validStoreKey(o.LogKey); err != nil {'   '		if err := error(nil); err != nil {'   '^TestATierManifestNamingAKeyOutsideTheStoreIsRefused$'

# The same rule one layer down, where the key becomes a path. This is the guard
# that makes the consequence concrete rather than the policy.
run_guard "store key stays inside the store" segment_store.go   '	if strings.ContainsAny(key, `/\`) {'   '	if false {'   '^TestAFileSegmentStoreKeyCannotReachOutsideItsDirectory$'

# Nothing REFERENCES the descriptor -- it is what the store says about itself,
# not part of what it holds -- so the reference-based garbage rule collects it
# unless told otherwise, and the log then refuses its own next open.
run_guard "the descriptor is never garbage" tier_state.go   '	referenced[descriptorKey] = true'   '	_ = descriptorKey'   '^TestTheDescriptorIsNeverGarbage$'

# A store-backed log's existence is a question for the store, not for whatever
# directory this process happens to be using. Neutralizing this falls back to the
# directory scan -- which is what shipped, and which calls a node adopting a tier
# a NEW log, so the one moment a process picks up someone else's log is the one
# moment its retention settings are never compared.
run_guard "newness comes from the store" descriptor.go   '	if opts.SegmentStore != nil {
		_, err := readStoreDescriptor(opts.SegmentStore)'   '	if false {
		_, err := readStoreDescriptor(opts.SegmentStore)'   '^TestAdoptingATierIsCheckedAgainstTheLogsOwnSettings$'

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
run_guard "a missing file is not retried" util.go   '		if err == nil || os.IsNotExist(err) || time.Now().After(deadline) {'   '		if err == nil || time.Now().After(deadline) {'   '^TestAReadOfAMissingFileDoesNotWaitOutTheRetryBound$'

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
run_guard "the read retry spends its whole budget" util.go   '	deadline := time.Now().Add(readRetryBudget)'   '	deadline := time.Now().Add(readRetryBudget / 10)'   '^TestTheReadRetryBoundIsATimeBudgetNotAnAttemptCount$'

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
run_guard "zero local retention never offloads" clean.go   '	if l.Options.LocalRetentionAge <= 0 || l.SegmentStore == nil {' '	if l.SegmentStore == nil {'   '^TestAZeroLocalRetentionAgeNeverOffloads$'

# Removing this returns the walk to adding cLen to pos unchecked, which is what
# let a block overrunning the file be listed as healthy while Records refused the
# same bytes.
run_guard "a block payload must be in the file" inspect.go   '		if end := start + int64(cLen); end > int64(len(s.raw)) {' '		if false {'   '^TestBlocksAndRecordsAgreeOnATruncatedPayload$'

# Neutralized to the laundering answer the check exists to refuse — reporting a
# version the file does not contain — rather than to a no-op, so the guard fails
# for the reason it is about.
run_guard "a magic with no version is refused" inspect.go   '		if hdr[0] == blockMagic {' '		if false {'   '^TestClassifySegmentRefusesAMagicWithNoVersion$'

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
if [ "$checked" -gt 0 ]; then
  echo "guardcheck: all $checked guards covered."
fi
if [ "$deferred" -ne 0 ]; then
  echo "guardcheck: $deferred guard(s) NOT covered here — deferred to another platform's run."
fi
