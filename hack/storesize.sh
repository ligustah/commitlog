#!/usr/bin/env bash
# storesize: a size a STORE reported must be bounded before it is allocated.
#
# A SegmentStore is supplied by the caller. Its Size() is the caller's code
# answering a question, and this package has no way to know it is honest: an
# object may have been replaced, a bucket listing may be stale, an
# implementation may return a sentinel it never meant to be believed.
#
# Everything the answer is used for downstream is checked. What is NOT checked
# is the moment before that, where the number becomes a length:
#
#     size, err := store.Size(key)
#     buf := make([]byte, size)     // <- here
#
# Both ends of that are damage, and only one of them is even an error. A
# NEGATIVE size is `makeslice: len out of range`: a panic, in the caller's
# process, thrown by a library the caller merely opened a tier through. A large
# one is taken quietly and in full, before a single byte is parsed, which is a
# remote store deciding how much of this process's memory it gets. In both cases
# the parser's own length checks — magic, version, an exact length, a CRC — run
# AFTER the buffer exists and so cannot protect the thing that made it. The
# general form, worth stating once: the checks that matter for an allocation are
# the ones that happen before it.
#
# The rule had been followed three times out of five and written down nowhere.
# readStoreDescriptor refused a non-positive size and bounded the rest by
# maxDescriptorBytes; readTierManifest refused a non-positive one; the remote
# index cache's fetch did too — and each of those was written separately, for
# the same reason, by someone who had just been bitten by it. fetchBlockTable,
# added later, was the one that was not, and nothing could notice: a hand-copied
# rule is invisible to the copy that never gets made. (copyObjectAs is the fifth
# reader and allocates nothing — it hands the size to a streaming Put — which is
# why this checks the allocation rather than the call.)
#
# What it checks, per function, in non-test files: if a variable is assigned
# from a `.Size(` call and later reaches `make(`, at least one comparison of
# that variable must stand between the two.
#
# Deliberately about the SHAPE and not about which bound is right. The correct
# ceiling differs per reader — maxDescriptorBytes for one, a value derived from
# the segment's physical extent for another — and a linter that tried to pick it
# would be wrong more often than the code. Requiring that a bound exists is the
# part that generalizes; guardcheck is what holds each specific bound in place.
#
# Non-test files only, matching docdrift and atomicwrite. A test allocating from
# a size it wrote itself is not trusting anybody.
#
# Exits non-zero listing every offender.
set -u
cd "$(dirname "$0")/.." || exit 1

# git ls-files rather than a find: already the list of files that belong to the
# repo, so build artifacts and anything ignored cannot wander in.
files=$(git ls-files '*.go' ':!:*_test.go')
if [ -z "$files" ]; then
  echo "storesize: HARNESS ERROR — selected no files to check." >&2
  exit 1
fi

# An empty selection reads as a pass, so the run is required to have SEEN the
# shape it polices. If a refactor renames Size() or moves every reader, this
# script must go red rather than quietly police nothing. The count is of
# size-to-make flows found, checked or not.
tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT

# shellcheck disable=SC2086 # $files is a deliberately word-split path list
awk '
  # Function boundary: a size never travels between functions, and pretending
  # otherwise would let a check in one cover an allocation in another.
  /^func / { delete from_size; delete checked; next }

  # A variable taking a store size. Both `:=` and `=`, and any receiver, so
  # s.store.Size(k), store.Size(k) and src.Size(k) all count.
  match($0, /^[[:space:]]*([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*,[[:space:]]*[A-Za-z_][A-Za-z0-9_]*[[:space:]]*:?=[[:space:]]*[^=]*\.Size\(/, m) {
    from_size[m[1]] = NR
    delete checked[m[1]]
    next
  }

  {
    # A comparison of a tracked variable arms it. Any relational operator: the
    # readers spell their bounds differently (`size <= 0`, `size < X`,
    # `size > max`) and all of them are a bound.
    for (v in from_size) {
      if ($0 ~ ("(^|[^A-Za-z0-9_.])" v "[[:space:]]*(<|>|<=|>=)")) checked[v] = 1
    }
    # An allocation from a tracked variable. make([]T, v) or make([]T, 0, v).
    for (v in from_size) {
      # Bracket classes, not backslash escapes: a backslash in a DYNAMIC regex
      # survives two levels of unquoting (awk string, then regex compiler) and
      # some awks strip it at the second, turning make\(\[\] into an unbalanced
      # paren. [(] [[] []] cannot be misread by either.
      if ($0 ~ ("make[(][[][]][A-Za-z0-9_.]*,[[:space:]]*(0,[[:space:]]*)?" v "[[:space:]]*[,)]")) {
        print FILENAME ":" NR ":" (v in checked ? "ok" : "UNBOUNDED") ":" v
      }
    }
  }
' $files > "$tmp"

seen=$(wc -l < "$tmp" | tr -d '[:space:]')
if [ "$seen" -eq 0 ]; then
  echo "storesize: HARNESS ERROR — found no store size reaching an allocation." >&2
  echo "  The shape this polices has moved or been renamed; an empty check is not a pass." >&2
  exit 1
fi

bad=0
while IFS=: read -r file line verdict var; do
  [ "$verdict" = "UNBOUNDED" ] || continue
  bad=1
  echo "$file:$line: '$var' came from a store's Size() and is allocated without a bound."
  echo "  A negative size panics in make(); a large one is taken in full before it is parsed."
  echo "  Refuse both before the allocation, as readStoreDescriptor and fetchBlockTable do."
# The loop reads from a FILE, not a pipe: a `| while` runs its body in a
# subshell, so every assignment to $bad here would be discarded and the script
# would print each violation and still exit 0. That exact bug shipped in
# layercheck.sh.
done < "$tmp"

if [ "$bad" -ne 0 ]; then
  exit 1
fi
echo "storesize: all $seen store size(s) reaching an allocation are bounded first."
