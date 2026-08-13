#!/usr/bin/env bash
# layercheck: a file BELOW the log in the stack must not name the log.
#
# docs/layering.md describes a stack that the compiler cannot enforce, because
# everything outside compress/ is one Go package. The doc used to defend itself
# with a table of reference counts, and the headline row was "how many files
# name *commitLog". That number cannot detect a violation: nearly every hit is
# a `func (l *commitLog)` method DECLARATION, and a file full of commitLog
# methods is by definition part of the top layer. The count drifted from six to
# ten while the layering stayed perfectly intact — it was measuring how the
# methods are spread across files, not which way the dependencies point.
#
# This measures the direction instead. Files are split into two sets by hand,
# and the rule is one sentence: nothing in LOWER may name *commitLog, as a
# receiver, a field, or a parameter. A lower-layer file that needs the log has
# stopped being a lower layer, and that is exactly the change worth catching on
# the commit that makes it rather than in a later audit.
#
# UPPER is listed too, and a non-test .go file in NEITHER set is an error. That
# is the point of listing both: a new file otherwise escapes the rule by simply
# not being mentioned, which is the failure mode every check in this repo has
# had at least once. Adding a file forces one decision — does this know about
# the log? — and the answer is recorded where the next person reads it.
#
# Exits non-zero listing every offender.
set -u
cd "$(dirname "$0")/.." || exit 1

# LOWER: bytes, files, and one segment. Knows nothing about "the log".
LOWER="
block.go block_table.go block_table_local.go buf_reader.go encoder.go
index.go index_cache.go index_mmap_unix.go index_mmap_windows.go
keydigest.go message.go message_set.go
segment.go segment_store.go
descriptor.go dirlock.go dirlock_unix.go dirlock_windows.go
syncdir_unix.go syncdir_windows.go util.go
inspect.go leader_epoch_cache.go
"

# UPPER: is the log, or is written against it.
UPPER="
commitlog.go interface.go
clean.go clean_join.go compact_cleaner.go delete_cleaner.go
manifest.go tier.go tier_move.go tier_state.go copy_tier.go
reader.go prefix_read.go prefix_source.go read_options.go
sidecar.go
"

fail=0

# Collapse the lists to single-space form. They are written multi-line to stay
# readable, and the membership test below is a substring match on " $f " — which
# silently misses every file that happens to start a line if the newlines are
# left in. That failure is quiet in the harmless direction (nothing matches, so
# everything reports unclassified), which is the only reason it was noticed.
LOWER=$(echo $LOWER)
UPPER=$(echo $UPPER)

# 1. Every non-test .go must be classified.
for f in *.go; do
	case "$f" in *_test.go) continue ;; esac
	case " $LOWER $UPPER " in
	*" $f "*) ;;
	*)
		echo "layercheck: $f is in neither LOWER nor UPPER in $0"
		echo "  Decide where it sits: does it name *commitLog? Then UPPER."
		fail=1
		;;
	esac
done

# 2. A file listed in LOWER must not name the log.
#
# The pattern is the POINTER TYPE and not the bare identifier, deliberately.
# Prose mentions the type by name — dirlock.go's "another live commitLog holds
# this directory" and leader_epoch_cache.go's "commitLog.CleanWithSpec" are both
# correct comments — and a check that goes red on an accurate sentence gets
# suppressed rather than obeyed. Every way of actually DEPENDING on the log
# spells the pointer: a receiver, a struct field, a parameter.
for f in $LOWER; do
	[ -f "$f" ] || { echo "layercheck: $0 lists $f, which does not exist"; fail=1; continue; }
	hits=$(grep -nE '\*commitLog' "$f" || true)
	if [ -n "$hits" ]; then
		echo "layercheck: $f is below the log but names it:"
		echo "$hits" | sed 's/^/  /'
		fail=1
	fi
done

if [ "$fail" -ne 0 ]; then
	echo
	echo "See docs/layering.md. Either move the code up, or take the file out of"
	echo "LOWER and say in the doc why that layer now knows about the log."
	exit 1
fi

echo "layercheck: OK — $(echo $LOWER | wc -w) files below the log, none of them name it"
