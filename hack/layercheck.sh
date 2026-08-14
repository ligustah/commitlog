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

# 3. The stack's internal steps, not just the top one.
#
# docs/layering.md listed these three beside the *commitLog row as evidence for
# the same claim. Only that row got mechanized when the metric was replaced,
# which left three un-checked counts sitting in a table that now looked
# enforced — the precise state that let the first one rot for months. Each is a
# real direction claim (index.go naming *segment IS an upward reference, unlike
# the receiver-counting it sat next to), so each is worth a rule of its own.
#
# Format: file:pattern:description. The pattern is a pointer type for the same
# reason as rule 2 — prose names these types legitimately.
STEPS="
index.go:\*segment\b:the offset index must not know about segments
segment.go:\*Reader\b:a segment must not know about readers
segment.go:\*(compactCleaner|deleteCleaner)\b:a segment must not know about compaction policy
"
# Iterated LINE by line, not word by word: the descriptions contain spaces, and
# `for step in $STEPS` splits on them — which turned one rule into twenty
# nonsense ones named "must", "not", "know".
#
# Collected into a variable rather than looped over directly, because the
# obvious `printf ... | while read` puts the loop in a SUBSHELL, where setting
# fail=1 changes nothing the parent can see. That version reports every
# violation and then exits 0 — a check that prints its own failure and passes,
# which is worse than the word-splitting bug it replaced.
step_violations=$(
	printf '%s\n' "$STEPS" | while IFS= read -r step; do
		[ -n "$step" ] || continue
		f=${step%%:*}
		rest=${step#*:}
		pat=${rest%%:*}
		why=${rest#*:}
		if [ ! -f "$f" ]; then
			echo "layercheck: $0 checks $f, which does not exist"
			continue
		fi
		hits=$(grep -nE "$pat" "$f" || true)
		if [ -n "$hits" ]; then
			echo "layercheck: $why ($f):"
			echo "$hits" | sed 's/^/  /'
		fi
	done
)
if [ -n "$step_violations" ]; then
	echo "$step_violations"
	fail=1
fi

# 4. Every exported method on *commitLog must be ON the CommitLog interface.
#
# New returns the INTERFACE, so a method missing from it is not public in any
# useful sense: the only way to reach it is a structural type assertion, and
# that degrades SILENTLY when it misses — the caller gets the zero value or
# skips the call, with nothing to log. durable_streams was reaching RecoverTail
# and ActiveSegmentBase exactly that way, and RecoverTail at open is what makes
# their producer-id records survive a restart. Five methods had drifted off the
# interface before anyone noticed, which is what makes this worth a check
# rather than a habit.
#
# EXPORTED_EXCEPT lists the ones that are deliberately not on it, each of which
# needs a reason that is about the SIGNATURE and not about convenience:
#
# Empty, and worth keeping that way. Its one entry was `Segments`, excused on
# the grounds that it returns []*segment — an unexported type nothing outside
# this package can do anything with. That was a correct reason not to put it on
# the interface and an equally good reason not to EXPORT it, which is what it
# became: segmentsSnapshot. An exception list is where a question stops being
# asked, so prefer changing the code until one is genuinely unavoidable.
EXPORTED_EXCEPT=""

iface=$(awk '/^type CommitLog interface \{/,/^\}$/' interface.go)

# Matched with bash's own regex operator rather than `echo "$iface" | grep -q`.
# That pipeline is safe HERE and only by accident: grep -q exits at its first
# match, leaving echo writing into a pipe nothing reads, and this script has no
# `set -o pipefail` to turn the resulting EPIPE into a failure. guardcheck.sh
# does have one, had the identical construct, and reported a properly covered
# guard as a HARNESS ERROR the moment its output outgrew the pipe buffer — a
# false red that looks exactly like the real thing the check exists to catch.
#
# So the safety of this line was a property of a `set` line thirty lines above
# it, and adding pipefail to this script — an obvious hardening — would have
# reintroduced the same bug. No pipe, no EPIPE, no dependency on that.
#
# The regex keeps the anchoring the grep had: start of a line, indentation, the
# method name, an open paren. $nl is a literal newline; $m is the method name,
# matched by `[A-Z][A-Za-z0-9]*` above, so it holds no regex metacharacters.
#
# The receiver is matched as an IDENTIFIER, not as the literal `l`. Every one of
# the 95 methods spells it `l` today, so hard-coding it worked — and renaming the
# receiver would have emptied the selection, skipped the loop body, and printed
# the same "with no exceptions" green as a fully checked run. That is the failure
# this repo has now had in five separate tools, so the count is asserted and
# printed rather than assumed: an empty selection is a HARNESS ERROR here, not a
# pass. (Nothing else in this file needs the guard — rules 1-3 iterate lists that
# are written down in the script, and an entry naming a file that does not exist
# is already an error.)
nl=$'\n'
methods=$(grep -ohE '^func \([a-z][A-Za-z0-9]* \*commitLog\) [A-Z][A-Za-z0-9]*' *.go | sed 's/.*) //' | sort -u)
if [ -z "$methods" ]; then
	echo "layercheck: HARNESS ERROR — found no exported methods on *commitLog." >&2
	echo "  Rule 4 checked nothing. The receiver pattern in $0 no longer matches" >&2
	echo "  the code, so fix the pattern rather than trusting this run." >&2
	exit 1
fi
for m in $methods; do
	case " $EXPORTED_EXCEPT " in *" $m "*) continue ;; esac
	if ! [[ $iface =~ (^|$nl)[[:space:]]+$m\( ]]; then
		echo "layercheck: commitLog.$m is exported but not on the CommitLog interface."
		echo "  New returns the interface, so callers can only reach it through a"
		echo "  structural assertion that fails silently. Add it, or add it to"
		echo "  EXPORTED_EXCEPT in $0 with a reason about its signature."
		fail=1
	fi
done

if [ "$fail" -ne 0 ]; then
	echo
	echo "See docs/layering.md. Either move the code up, or take the file out of"
	echo "LOWER and say in the doc why that layer now knows about the log."
	exit 1
fi

echo "layercheck: OK — $(echo $LOWER | wc -w) files below the log, none of them name it;"
# The method count is what rule 4 actually looked at, not the length of a list
# written in this file. A green line quoting only the hand-written LOWER list
# says nothing about whether the other rule ran at all.
n_methods=$(echo $methods | wc -w | tr -d ' ')
if [ -n "$EXPORTED_EXCEPT" ]; then
	echo "  all $n_methods exported commitLog methods are on the interface but $EXPORTED_EXCEPT"
else
	echo "  all $n_methods exported commitLog methods are on the interface, no exceptions"
fi
