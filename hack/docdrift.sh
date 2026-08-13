#!/usr/bin/env bash
# docdrift: a doc comment must not open with the name of a DIFFERENT function.
#
# Go's convention is that a function's doc comment begins with its own name, so
# an opener naming something else is not a style nit here — it means the comment
# and the code parted company, usually because the function was renamed or moved
# and its doc stayed behind. That is invisible to the compiler, to vet, and to
# staticcheck, and in this package the comments carry the contracts: the first
# sweep with this rule found six, including one doc block stranded on an
# unrelated function when its own moved to another file, and one whose opening
# paragraph described behaviour its own next paragraph denied.
#
# Non-test files only, deliberately. A test's doc legitimately opens by naming
# the function under test — "splitOffloadedPrefix cuts at the first local
# segment" above TestSplitOffloadedPrefix is correct, not drift — and a linter
# that needs a suppression list to stay quiet is a linter that gets ignored.
#
# EVERY package, not just the root one. This ran `for f in *.go` — the repo root
# — until 2026-08-13, so compress/ was exempt and nothing said so. The rule is
# plain Go doc convention and applies there identically; the exclusion was an
# artifact of the glob, not a decision. Note the contrast with layercheck.sh,
# which IS root-only and correctly so, because the thing it measures (direction
# of dependency on *commitLog) exists only in the one package — and which says
# that in its header rather than leaving it to be inferred from a glob.
#
# The count is printed, and an empty selection is an error. A checker that
# quietly narrows its own scope reports the same green as one that checked
# everything, which is the failure this repo has now had in four separate tools.
#
# Exits non-zero listing every offender.
set -u
cd "$(dirname "$0")/.." || exit 1

# git ls-files rather than a find: it is already the list of files that belong
# to the repo, so build artifacts and anything ignored cannot wander in.
files=$(git ls-files '*.go' ':!:*_test.go')
if [ -z "$files" ]; then
  echo "docdrift: HARNESS ERROR — selected no files to check." >&2
  exit 1
fi

found=$(
  for f in $files; do
    awk -v file="$f" '
      # Remember the most recent contiguous comment block: its first line and
      # where it started. A blank or code line ends the block.
      /^\/\// {
        if (!inblock) { inblock = 1; first = $0; firstline = NR }
        next
      }
      /^func / {
        if (inblock) {
          name = $0
          sub(/^func +\([^)]*\) +/, "", name)
          sub(/^func +/, "", name)
          # Cut at whichever comes first, the argument list or a type parameter
          # list: a generic function is "func retryWhileHeld[T any](...)", and
          # stopping only at "(" makes its name "retryWhileHeld[T any]", which
          # no doc comment can ever open with.
          sub(/[[(].*$/, "", name)
          opener = first
          sub(/^\/\/ */, "", opener)
          sub(/ .*$/, "", opener)
          sub(/[.,:]$/, "", opener)
          # Only lowercase identifiers: prose openers ("A", "The", "Returns")
          # are not function names, and neither is anything with punctuation.
          if (opener ~ /^[a-z][A-Za-z0-9_]*$/ && opener != name) {
            printf "%s:%d: doc opens with %s, but documents %s\n", file, firstline, opener, name
          }
        }
        inblock = 0
        next
      }
      { inblock = 0 }
    ' "$f"
  done
)

if [ -n "$found" ]; then
  echo "docdrift: a doc comment names a function it does not document:"
  echo "$found"
  echo
  echo "Fix the comment, or move it to the function it describes."
  exit 1
fi
echo "docdrift: every doc comment opens with the function it documents ($(printf '%s\n' "$files" | wc -l | tr -d ' ') files)."
