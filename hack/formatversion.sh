#!/usr/bin/env bash
# formatversion: a version constant that GATES a refusal must be held by a guard.
#
# Every on-disk format in this repo carries a version, and every reader refuses
# a version it does not recognise. That refusal is the only thing standing
# between an older file and a parse that misreads it — the block header did not
# merely gain a field in v0.89.0, it grew four bytes, so a v1 segment read as v2
# puts every block boundary after the first in the wrong place.
#
# Two things have to be true for that to keep working, and they are different
# claims:
#
#   1. the version LINE is checked — `if v != theVersion { return ... }`
#   2. the CONSTANT moved when the layout did
#
# A check that runs against the wrong number is still a check that runs. #300 is
# what this exists for: TierObject gained a Records field and manifestVersion
# stayed at 3, so every v0.88.0 manifest decoded with Records absent — which is
# zero — and a retention budget that sums record counts stopped reaching its
# ceiling at all. The check was there the whole time. The number was not.
#
# guardcheck holds both claims, one guard each, and that works. What did not
# work is REMEMBERING to add them: the descriptor, the block header, the block
# table and the digest sidecar each shipped a version refusal, and three of the
# four had no guard of either kind. A rule followed by memory is a rule that the
# next format skips, which is the same reasoning that produced storesize.sh.
#
# What it checks: for every version-like constant declared with a numeric value
# in a non-test file, if that constant is COMPARED anywhere in non-test code —
# which is what makes it a gate rather than a label — its name must appear in
# hack/guardcheck.sh.
#
# Deliberately about the NAME appearing, not about which guard or how many. What
# a specific format needs held is a judgement about that format; requiring that
# somebody made the judgement is the part that generalizes. guardcheck itself
# then proves each guard is real, since an anchor that no longer resolves is
# counted as a failure there rather than skipped.
#
# A constant that is declared and never compared is not a gate and is not
# required to have one. That is not a loophole: a version nothing checks refuses
# nothing, and this script is about refusals.
#
# Exits non-zero listing every offender.
set -u
cd "$(dirname "$0")/.." || exit 1

guards=hack/guardcheck.sh
if [ ! -f "$guards" ]; then
  echo "formatversion: HARNESS ERROR — $guards is missing." >&2
  exit 1
fi

files=$(git ls-files '*.go' ':!:*_test.go')
if [ -z "$files" ]; then
  echo "formatversion: HARNESS ERROR — selected no files to check." >&2
  exit 1
fi

tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT

# Two spellings, because this repo uses both and a check that knew only one
# would police half the formats: `somethingVersion = N` (manifestVersion,
# BlockFormatVersion, blockTableVersion, digestVersion) and `somethingFileVN = N`
# (descriptorFileV2, leaderEpochFileV0). The trailing-V form is why this cannot
# just grep for "version".
#
# The optional `const` is not padding. manifestVersion is declared on its own
# line rather than inside a `const (...)` block, and the first draft of this
# script omitted the keyword — so it enumerated five constants, reported all
# five held, and silently skipped the one format whose missed bump is the
# reason the script exists. That is #300 one layer up: the check ran, against
# the wrong set.
#
# The value must be a numeric literal. A constant assigned from another constant
# is an alias, and holding the alias would not hold the format.
#
# shellcheck disable=SC2086 # $files is a deliberately word-split path list
grep -nHE '^[[:space:]]*(const[[:space:]]+)?[A-Za-z_][A-Za-z0-9_]*([Vv]ersion[A-Za-z0-9_]*|FileV[0-9]+)[[:space:]]+(byte[[:space:]]+)?=[[:space:]]*[0-9]+[[:space:]]*$' $files \
  | sed -E 's/^([^:]+):([0-9]+):[[:space:]]*(const[[:space:]]+)?([A-Za-z_][A-Za-z0-9_]*).*/\1:\2:\4/' \
  | sort -u -t: -k3,3 > "$tmp"

seen=$(grep -c '' < "$tmp")
if [ "$seen" -eq 0 ]; then
  echo "formatversion: HARNESS ERROR — found no version constants." >&2
  echo "  Every on-disk format in this repo carries one, so zero means the" >&2
  echo "  declaration shape has changed. An empty selection is not a pass." >&2
  exit 1
fi

# Now close the set from the OTHER side, which is the part that survives the
# next declaration shape nobody predicted.
#
# The scan above recognises how a version is DECLARED, and that is a pattern
# written by hand against the spellings that exist today — exactly what missed
# manifestVersion. So instead of widening it again and hoping, ask the opposite
# question: which names are COMPARED as versions? A name compared like a version
# and absent from the declaration scan means the scan did not see its
# declaration, and every conclusion below it is about an incomplete set.
#
# This cannot be satisfied by adding a guard. It is a harness error on purpose:
# the answer is to teach the scan the new shape, and until then the script has
# no standing to report anything.
#
# shellcheck disable=SC2086 # deliberate word-split, as above
compared=$( { grep -ohE '(!=|==)[[:space:]]*[A-Za-z_][A-Za-z0-9_]*([Vv]ersion[A-Za-z0-9_]*|FileV[0-9]+)([^A-Za-z0-9_]|$)' $files \
                | sed -E 's/^(!=|==)[[:space:]]*//'
              grep -ohE '(^|[^A-Za-z0-9_.])[A-Za-z_][A-Za-z0-9_]*([Vv]ersion[A-Za-z0-9_]*|FileV[0-9]+)[[:space:]]*(!=|==)' $files \
                | sed -E 's/^[^A-Za-z_]*//'
            } | sed -E 's/[^A-Za-z0-9_].*$//' | sort -u )

# Declared names as a newline-delimited string rather than piping each lookup
# into `grep -q`: the CI comment beside the shellcheck step says neither script
# pipes into grep -q any more, and it is not a style rule. Under `set -o
# pipefail` grep exits early, the upstream command takes EPIPE, and the
# pipeline's status becomes the write failure instead of the match — which is
# how a guard that WAS covered once reported red.
decls=$(cut -d: -f3 < "$tmp")
missing=$(
  for name in $compared; do
    case "
$decls
" in
      *"
$name
"*) ;;
      *) echo "$name" ;;
    esac
  done
)
if [ -n "$missing" ]; then
  echo "formatversion: HARNESS ERROR — compared as a version, declaration not found:" >&2
  for name in $missing; do echo "  $name" >&2; done
  echo "  The declaration scan above knows a fixed set of spellings, and one of" >&2
  echo "  these is not among them — so the set it reports on is smaller than the" >&2
  echo "  set that exists. Adding a guard will NOT fix this. Teach the scan the" >&2
  echo "  new declaration shape. (manifestVersion is why: declared outside a" >&2
  echo "  const block, it was skipped in silence by the first draft.)" >&2
  exit 1
fi

bad=0
gated=0
while IFS=: read -r file line name; do
  # Compared somewhere in non-test code is what makes it a gate. Both
  # directions, because readers spell it `v != theVersion` and
  # `theVersion != v` about equally.
  #
  # shellcheck disable=SC2086 # deliberate word-split, as above
  if ! grep -qE "(!=|==)[[:space:]]*${name}([^A-Za-z0-9_]|\$)|${name}[[:space:]]*(!=|==)" $files; then
    continue
  fi
  gated=$((gated + 1))
  # Word-bounded, not a substring. `grep -q "$name"` reads as "is it named" and
  # is not: it accepts any LONGER identifier that contains this one, so renaming
  # manifestVersion to manifestVersionXX inside a guard anchor kept this check
  # green. That is how it was written, and the mutation that should have proved
  # this check works is what found it — the same substring-for-identity slip
  # that guardcheck's own --filter argument had.
  if grep -qE "(^|[^A-Za-z0-9_])$name([^A-Za-z0-9_]|\$)" "$guards"; then
    continue
  fi
  bad=1
  echo "$file:$line: '$name' gates a refusal and is named by no guard."
  echo "  Two claims need holding, and they are different: that the version line"
  echo "  is checked, and that the constant moved when the layout did. #300 had"
  echo "  the first and not the second, and a retention budget silently stopped"
  echo "  reaching its ceiling. See the manifestVersion pair in $guards."
# Read from a FILE, not a pipe: a `| while` runs its body in a subshell, so
# every assignment to $bad would be discarded and this would print each
# violation and still exit 0. That exact bug shipped in layercheck.sh.
done < "$tmp"

if [ "$gated" -eq 0 ]; then
  echo "formatversion: HARNESS ERROR — found $seen version constant(s), none compared." >&2
  echo "  Every reader in this repo refuses a version it does not recognise, so" >&2
  echo "  zero gates means the comparison shape has changed, not that the" >&2
  echo "  refusals are gone." >&2
  exit 1
fi

if [ "$bad" -ne 0 ]; then
  exit 1
fi
echo "formatversion: all $gated of $seen version constant(s) that gate a refusal are named by a guard."
