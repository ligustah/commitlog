#!/usr/bin/env bash
#
# Every exported sentinel in package commitlog is accounted for on the caller's
# side: it is either listed in the CommitLog remedy list in interface.go, or its
# declaration says why a caller never sorts on it.
#
# WHY THIS EXISTS. The interface doc used to state one rule with two exceptions
# ("a commitlog sentinel means PERMANENT"). That rule was false at the sentinels
# nobody re-read while writing it — ErrSegmentReplaced had carried "operations
# should be retried" on its own declaration for far longer than the rule had
# existed — and a follower applying it as written abandoned a healthy segment
# mid-compaction. The fix replaced the generalisation with an explicit list, on
# the reasoning that a claim about every sentinel is worth only as much as the
# last one someone checked it against.
#
# An explicit list has the same failure mode one layer down, and it had already
# happened by the time this script was written: six caller-facing sentinels were
# missing from it, including the one ReadMessageSet returns. A list is not a
# closed set just because it is a list. This closes it from the other side — the
# DECLARATIONS — so a sentinel added tomorrow cannot quietly stay unlisted.
#
# SCOPE, stated rather than guessed. Only package commitlog's own sentinels, in
# the package root. Subpackages (compress, and anything else with its own
# errors) answer to their own callers and are out. The scope is stated because a
# check whose scope is a guess is worse than one whose scope is written down:
# nobody can tell a deliberate omission from an oversight.
#
# HOW TO SATISFY IT for a new sentinel — pick one, deliberately:
#
#   1. Add it to the remedy list in interface.go, as a `//   - ErrX — what the
#      caller does now` bullet. This is the right answer whenever an exported
#      method can return it.
#   2. Write `not caller-sorted: <reason>` in its declaration's doc comment.
#      The reason has to say what a caller sees INSTEAD, since "internal" is not
#      a property of an exported identifier.
#
# Doing both is also an error: a sentinel cannot be one a caller sorts on and
# one it never sees.

set -euo pipefail
cd "$(dirname "$0")/.."

fail=0
note() { printf '  %s\n' "$1"; }

# Declared: every exported sentinel in the package root, with a flag for whether
# its own doc comment exempts it. The comment block is the contiguous run of
# comment lines immediately above the declaration, which is where a reader looks
# and so the only place the marker is worth honouring — a blank line ends it,
# exactly as it ends one entry of a `var (...)` block.
gofiles=()
for f in *.go; do
    case "$f" in
    *_test.go) continue ;;
    esac
    gofiles+=("$f")
done
if [ "${#gofiles[@]}" -eq 0 ]; then
    echo "HARNESS ERROR: no non-test .go files in the package root."
    exit 2
fi

declared=$(awk '
    FNR == 1 { block = "" }
    {
        t = $0
        sub(/^[[:space:]]+/, "", t)
        if (t ~ /^\/\//) { block = block "\n" t; next }
        if (t ~ /^(var )?Err[A-Za-z]+ = errors\.New/) {
            name = t
            sub(/^var /, "", name)
            sub(/ = errors\.New.*/, "", name)
            print name "\t" (block ~ /not caller-sorted:/ ? "exempt" : "listed?")
        }
        block = ""
    }
' "${gofiles[@]}")

if [ -z "$declared" ]; then
    echo "HARNESS ERROR: found no sentinel declarations at all."
    echo "  The scan pattern no longer matches how this package declares them,"
    echo "  which reports a clean run for a check that ran over nothing."
    exit 2
fi

# Documented: every sentinel named in a doc-comment bullet in interface.go. The
# anchor is the bullet STRUCTURE, not the prose around it, so rewording the
# section heading cannot silently empty this set. Two names on one bullet
# (ErrSegmentClosed, ErrSegmentReplaced — one condition, two spellings) both
# count, which is why this reads every match on the line.
#
# `|| true` is load-bearing, and its absence was caught by probing the branch
# below rather than by reading it. grep exits 1 on no match, so under `set -e`
# an empty result killed the script at the ASSIGNMENT — exit 1, no output, the
# harness-error message below unreachable. Fail-safe in CI and undiagnosable
# there, which is the same family as an empty test selection reporting a pass:
# the check's own failure has to be distinguishable from the thing it checks.
documented=$(grep -oE '^//[[:space:]]+- .*' interface.go | grep -oE 'Err[A-Za-z]+' | sort -u || true)

if [ -z "$documented" ]; then
    echo "HARNESS ERROR: interface.go names no sentinels in any doc bullet."
    echo "  Either the remedy list is gone or its bullets no longer look like"
    echo "  '//   - ErrX'. Both are a finding, not a pass."
    exit 2
fi

# A bullet naming two sentinels ASSERTS that one remedy fits both, and nothing
# checked that assertion. Membership of the list was closed from the declarations
# above; this closes the remedies from a third side.
#
# It found a real one on 2026-08-17, the day after the check was written:
# ErrCommitLogClosed and ErrCommitLogDeleted shared "this handle is finished.
# Open the log again if you still want it", which is true of closed and false of
# deleted — there is nothing to reopen, and opening that path creates a NEW empty
# log. Two downstream consumers mapped the deleted half two different wrong ways
# in the same week, one as permanent damage and one through a default arm into an
# internal error. Neither was reading carelessly: the list told them the two were
# interchangeable.
#
# Grouping is still right where the names really are one condition, so the
# allowlist is a set of pairs and every entry has to be argued rather than
# accumulated. Adding to it is a claim that the remedy sentence is true of each
# name on its own.
#
# Only the bullet's FIRST line is scanned, which is where the names being given a
# remedy sit. Continuation lines are prose and legitimately mention other
# sentinels to contrast with — including, now, the very pair this found.
# One entry, and it is the only grouping in the list that survived being argued.
# ErrSegmentClosed and ErrSegmentReplaced are the same event seen from two places
# — which one you get depends only on where you touched the segment — so the
# remedy sentence is true of each alone.
#
# ErrSegmentUnreadable and ErrCorruptRecord were the second entry here for about
# an hour, on the reasoning that both mean "damaged bytes, restore from a peer".
# Arguing it is what killed it: the damage is bounded by a SEGMENT in one and by
# one RECORD in the other, and ErrCorruptRecord's declaration says the caller may
# skip the record and carry on — a remedy the shared bullet never offered, having
# named only the heaviest of the three. Not false, like the bullet that prompted
# this check, but narrower than the sentinel it was describing, which is the same
# failure with the volume turned down. Two for two: of the two groupings in the
# list, neither survived being asked whether one sentence fit both members.
allowed_groups="ErrSegmentClosed,ErrSegmentReplaced"

grouped=$(grep -E '^//[[:space:]]+- ' interface.go | while IFS= read -r bullet; do
    names=$(printf '%s\n' "$bullet" | grep -oE 'Err[A-Za-z]+' | sort -u | paste -sd, -)
    case "$names" in
    *,*) printf '%s\n' "$names" ;;
    esac
done | sort -u || true)

# Deliberately NOT `| while ... fail=1`: a pipeline's while runs in a subshell
# and its assignment is lost, so the check would report a pass with its findings
# already printed. Same trap as the one noted for `|| true` above.
while IFS= read -r names; do
    [ -n "$names" ] || continue
    if printf '%s\n' "$allowed_groups" | grep -qx "$names"; then
        continue
    fi
    echo "GROUPED REMEDY: ${names}"
    note "one bullet in interface.go gives these names a single remedy, which"
    note "asserts the sentence is true of each of them separately. If it is, add"
    note "'${names}' to allowed_groups here and say why. If it is not, split the"
    note "bullet — a caller sorting on the weaker half acts on the stronger's"
    note "remedy, and cannot tell from the list that it was told two things."
    fail=1
done <<EOF
$grouped
EOF

while IFS=$'\t' read -r name state; do
    [ -n "$name" ] || continue
    in_list=no
    if printf '%s\n' "$documented" | grep -qx "$name"; then
        in_list=yes
    fi
    case "$in_list:$state" in
    yes:exempt)
        echo "CONTRADICTION: ${name}"
        note "listed in interface.go's remedy list AND marked not caller-sorted."
        note "It is one or the other. Decide which, and delete the other half."
        fail=1
        ;;
    no:listed?)
        echo "UNACCOUNTED: ${name}"
        note "not in interface.go's remedy list, and its declaration does not say"
        note "why a caller never sorts on it. A caller that receives it has to"
        note "guess whether to retry, restore, reopen or give up."
        note "Fix: add a '//   - ${name} — <remedy>' bullet to interface.go, or"
        note "write 'not caller-sorted: <what the caller sees instead>' in its doc."
        fail=1
        ;;
    esac
done <<EOF
$declared
EOF

if [ "$fail" -ne 0 ]; then
    exit 1
fi

echo "sentinels: every exported sentinel is listed or explicitly exempt"
