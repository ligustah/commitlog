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
# Exits non-zero listing every offender.
set -u
cd "$(dirname "$0")/.." || exit 1

found=$(
  for f in *.go; do
    case "$f" in *_test.go) continue ;; esac
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
          sub(/\(.*$/, "", name)
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
echo "docdrift: every doc comment opens with the function it documents."
