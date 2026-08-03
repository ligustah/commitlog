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
# Usage:  hack/guardcheck.sh          (all guards)
#         hack/guardcheck.sh crc      (only guards whose name matches)
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

# guard_start NAME — filter, count and print. Returns 1 if this guard is filtered out.
guard_start() {
  if [ -n "$filter" ] && [[ "$1" != *"$filter"* ]]; then
    return 1
  fi
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
  '	if l.segmentsClosed {
		return ErrCommitLogClosed
	}
	seg, idx := findSegment(l.segments, offset)' \
  '	if l.segmentsClosed && false {
		return ErrCommitLogClosed
	}
	seg, idx := findSegment(l.segments, offset)' \
  '^TestMaintenanceOnAClosedLogBuildsNothing$'

run_guard "roll refuses a closed log" commitlog.go \
  '	if l.segmentsClosed {
		// The log closed underneath this append. Do not publish into a set that' \
  '	if l.segmentsClosed && false {
		// The log closed underneath this append. Do not publish into a set that' \
  '^TestASegmentRolledWhileTheLogClosesIsStillClosed$'

# Copy-on-write, not a conditional: Segments() hands out the slice HEADER, so
# writing an element in place races every lock-free reader holding a snapshot.
# Needs -race — without it the neutralized version passes, which is the whole
# reason this runner learned the flag.
run_guard "TruncateBefore copy-on-write" commitlog.go \
  '	// whatever a reader is already holding immutable.
	segments := make([]*segment, len(l.segments))
	copy(segments, l.segments)' \
  '	// whatever a reader is already holding immutable.
	segments := l.segments' \
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

echo
if [ "$failures" -ne 0 ]; then
  echo "guardcheck: $failures of $checked guard(s) are NOT covered by the test named for them."
  exit 1
fi
echo "guardcheck: all $checked guards covered."
