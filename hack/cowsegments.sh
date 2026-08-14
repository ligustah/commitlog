#!/usr/bin/env bash
# cowsegments: the segment slice is copy-on-write. Nothing may write into it.
#
# segmentsSnapshot() hands out the slice HEADER, not a copy, and every reader
# indexes what it got WITHOUT holding l.mu. So whoever changes the segment set
# publishes a NEW array; writing an element of the one readers are already
# holding is a data race against all of them, whatever lock is held while doing
# it. Assigning l.segments is fine. Appending is fine — append only touches
# indices at or past len, and a snapshot's length is fixed when it is taken.
#
# The rule has been broken twice, and neither break looked like a write:
#
#   - TruncateBefore assigned an element in place. Red under -race in a deletion
#     and a truncate chaos test, fixed in v0.44.2.
#   - adoptTierManifestLocked appended to l.segments and then sort.Slice'd it,
#     and sort.Slice SWAPS elements in place. That one was invisible: it runs
#     inside open(), before there is a log for anyone to hold a reader on, so it
#     was safe by the schedule rather than by the code. No test could have gone
#     red, because no reader exists to lose the race — which is exactly why a
#     grep is the right instrument and a guard is not.
#
# Tests are checked too, unlike docdrift and atomicwrite. Those two are about
# promises a test does not make; this is about a shared array, and an in-package
# test helper sorting it races the log's own readers just as well.
#
# Exits non-zero listing every offender.
set -u
cd "$(dirname "$0")/.." || exit 1

files=$(git ls-files '*.go')
if [ -z "$files" ]; then
  echo "cowsegments: HARNESS ERROR — selected no files to check." >&2
  exit 1
fi

# The rule only means anything while segmentsSnapshot still hands out the live
# header. If it is ever changed to return a copy, in-place writes stop racing
# readers and this check is enforcing a rule that no longer exists — which it
# would do silently, passing forever. Same vacuous green an empty file selection
# gives, one level up.
if ! grep -A 4 'func (l \*commitLog) segmentsSnapshot()' commitlog.go |
  grep -q 'return l\.segments$'; then
  echo "cowsegments: HARNESS ERROR — segmentsSnapshot no longer returns l.segments" >&2
  echo "  directly, so readers may no longer share the array this check protects." >&2
  echo "  Re-read its doc and either point this check at whatever replaced it, or" >&2
  echo "  delete it." >&2
  exit 1
fi

# Comment lines are stripped first: the prohibited forms are quoted verbatim in
# doc comments explaining the rule -- including in this repo, deliberately -- and
# a checker that cannot tell an example from a violation gets suppressed.
#
# Three shapes, all writes into the backing array:
#   x.segments[i] = ...   an element assignment (=, not ==)
#   sort.*(x.segments)    every sort swaps in place
#   copy(x.segments, ...) l.segments as the DESTINATION; as a source it is fine
bad='[a-z][a-zA-Z]*\.segments\[[^]]*\][[:space:]]*=[^=]'
bad="$bad|(sort\.(Slice|SliceStable|Sort|Stable)|slices\.Sort[A-Za-z]*)\([a-z][a-zA-Z]*\.segments\b"
bad="$bad|copy\([a-z][a-zA-Z]*\.segments\b"

offenders=""
# A `while read` over a pipe would set `offenders` in a subshell and lose every
# hit — this repo has already shipped that bug once, in layercheck, where it
# printed every violation and exited 0. The loop stays in this shell.
#
# shellcheck disable=SC2086 # $files is a deliberate word-split list of paths.
for f in $files; do
  hits=$(sed 's;//.*;;' "$f" | grep -nE "$bad" || true)
  if [ -n "$hits" ]; then
    offenders="$offenders$(printf '%s\n' "$hits" | sed "s;^;$f:;")
"
  fi
done

if [ -n "$offenders" ]; then
  echo "cowsegments: these write into the shared segment array:"
  printf '%s' "$offenders"
  echo
  echo "Build a new slice and assign it to l.segments instead. Readers hold the"
  echo "old header without l.mu, so no lock you take here makes an in-place"
  echo "write safe -- see segmentsSnapshot's doc."
  exit 1
fi
echo "cowsegments: nothing writes into the segment array ($(printf '%s\n' "$files" | wc -l | tr -d ' ') files checked)."
