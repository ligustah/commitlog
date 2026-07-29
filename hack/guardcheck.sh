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

# name | file | python-expression removing the guard | test regex
#
# The removal is a literal replacement rather than a patch, so it breaks loudly
# if the guarded code moves — which is the point: a guard that has been rewritten
# needs its claim re-checked, not silently skipped.
run_guard() {
  local name="$1" file="$2" old="$3" new="$4" test_re="$5"

  if [ -n "$filter" ] && [[ "$name" != *"$filter"* ]]; then
    return 0
  fi
  checked=$((checked + 1))
  printf '  %-34s ' "$name"

  TOUCHED+=("$file")
  if ! OLD="$old" NEW="$new" "$PY_BIN" -c '
import os, sys
p = sys.argv[1]
old, new = os.environ["OLD"], os.environ["NEW"]
s = open(p, encoding="utf-8", newline="").read()
if old not in s:
    sys.exit(3)
open(p, "w", encoding="utf-8", newline="").write(s.replace(old, new, 1))
' "$file"; then
    echo "SKIP (guard text not found — did it move?)"
    failures=$((failures + 1))
    git checkout -- "$file"
    return 0
  fi

  # A build failure must NOT count as coverage. The first version of this
  # script neutralized guards with `if false {`, which orphaned imports; the
  # package then failed to COMPILE, `go test` returned non-zero, and every
  # guard reported as covered — including one deliberately pointed at a test
  # that does not cover it. The replacements above keep each symbol used, and
  # this check makes the remaining case loud instead of green.
  if ! go build ./... >/dev/null 2>&1; then
    echo "HARNESS ERROR (package does not build with the guard neutralized)"
    failures=$((failures + 1))
    git checkout -- "$file"
    return 0
  fi
  if go test -run "$test_re" -count=1 -timeout 300s . >/dev/null 2>&1; then
    echo "NO COVERAGE — $test_re passed without it"
    failures=$((failures + 1))
  else
    echo "ok (test fails without it)"
  fi
  git checkout -- "$file"
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

run_guard "short-frame refusal" reader.go \
  'if len(m) < 4 {' \
  'if len(m) < 4 && false {' \
  '^FuzzTornLogServesOnlyAPrefix$'

run_guard "digest sidecar CRC" keydigest.go \
  'if crc32.ChecksumIEEE(body) != encoding.Uint32(crcBytes) {' \
  'if crc32.ChecksumIEEE(body) != encoding.Uint32(crcBytes) && false {' \
  '^FuzzCorruptDigestNeverChangesTheAnswer$'

run_guard "reclamation pin" commitlog.go \
  'if e.pin != nil && e.pin.referenced() {' \
  'if e.pin != nil && e.pin.referenced() && false {' \
  '^TestReclamationWaitsForTheReaderHoldingTheOldObject$'

echo
if [ "$failures" -ne 0 ]; then
  echo "guardcheck: $failures of $checked guard(s) are NOT covered by the test named for them."
  exit 1
fi
echo "guardcheck: all $checked guards covered."
