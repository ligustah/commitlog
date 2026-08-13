#!/usr/bin/env bash
# atomicwrite: only the wrapper's own file may reach the atomic-file library.
#
# An atomic write gives a file TORN-WRITE safety: the reader sees the whole old
# content or the whole new one, never a mixture. It does not give durability.
# The library finishes with a rename, and a rename that has returned is visible
# to every later reader in this boot while still being undoable by a power cut,
# so the directory holding the new name needs its own fsync — see syncDir.
#
# AtomicWriteFileWithRetry is that write plus the fsync, plus the Windows
# ReplaceFile retry a destination someone else has open needs. Every place in
# this package that finishes with a rename wants all three, so calling the
# library directly is never the right answer: it is a durability promise the
# caller believes it made and did not.
#
# It has been the wrong answer twice. writeDescriptor and leaderEpochCache.flush
# both wrote through the library, so the log's DESCRIPTOR — the file that says
# the log exists at all, and the one removeLogDir orders the whole of Delete
# around — could be reported as written and be absent after a power cut, leaving
# a directory of segments that readDescriptor refuses forever. Neither was a
# decision; both simply predated the wrapper, and nothing noticed for as long as
# the only record of the rule was a sentence in syncDir's doc listing the
# callers it applied to. A list written out by hand cannot see a caller that
# never arrives.
#
# Non-test files only, matching docdrift. A test writing a fixture is not
# claiming durability, and a linter that needs a suppression list is a linter
# that gets ignored.
#
# Exits non-zero listing every offender.
set -u
cd "$(dirname "$0")/.." || exit 1

# The one file allowed to import it: the wrapper is what it is for.
owner=util.go
lib=natefinch/atomic

# git ls-files rather than a find: already the list of files that belong to the
# repo, so build artifacts and anything ignored cannot wander in.
files=$(git ls-files '*.go' ':!:*_test.go')
if [ -z "$files" ]; then
  echo "atomicwrite: HARNESS ERROR — selected no files to check." >&2
  exit 1
fi

# The rule is only meaningful while the owner still holds the import. Without
# this, moving or renaming the wrapper leaves a check that passes because it is
# testing nothing — the same vacuous green an empty file selection gives, one
# level up.
if ! grep -q "$lib" "$owner"; then
  echo "atomicwrite: HARNESS ERROR — $owner no longer imports $lib," >&2
  echo "  so this check has nothing left to enforce. Point owner= at the file" >&2
  echo "  that wraps it now." >&2
  exit 1
fi

# shellcheck disable=SC2086 # $files is a deliberate word-split list of paths.
offenders=$(grep -l "$lib" $files | grep -v "^${owner}\$")
if [ -n "$offenders" ]; then
  echo "atomicwrite: these files write through $lib directly:"
  echo "$offenders"
  echo
  echo "Use AtomicWriteFileWithRetry instead. It is the same atomic write plus"
  echo "the directory fsync that makes the rename survive a power cut, and the"
  echo "Windows retry for a destination another handle has open."
  exit 1
fi
echo "atomicwrite: $owner is the only file reaching $lib ($(printf '%s\n' "$files" | wc -l | tr -d ' ') files checked)."
