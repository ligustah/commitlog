#!/usr/bin/env bash
#
# Every refusal New makes about the caller's Options carries ErrInvalidOptions.
#
# Not for tidiness. New has callers that open on a retry loop, and the rule they
# sort on is: a commitlog sentinel from New means the condition is PERMANENT,
# with ErrLogLocked the sole exception, and anything else is an OS or store
# error that may be transient. Seven refusals used to carry no sentinel at all —
# an empty Path, an unknown codec, a negative option, and validateTiers' four —
# so they were indistinguishable from a disk that was briefly busy. A caller
# defaulting "unrecognised means transient" spins forever on an empty Path; one
# defaulting the other way gives up on a full disk. Both are correct callers.
#
# A test can only check the refusals someone remembered to write down, which is
# exactly the eighth one's problem. So this checks the SOURCE: no bare error
# constructor anywhere New decides about the caller's values.
#
# Scope is New's own body plus every validate* function in the package. The
# second half is what stops the obvious escape — moving a new check into a
# helper — and it is why a new one should be named validateX. A refusal added
# somewhere neither of those reaches is out of scope here, and the sweep note in
# docs/ says so rather than pretending otherwise.

set -euo pipefail
cd "$(dirname "$0")/.."

fail=0
scanned=0

# Print the body of a top-level func, from its signature to the closing brace in
# column 0. Go's gofmt guarantees that brace, which is why this can be a text
# scan rather than a parse.
body() {
	awk -v pat="$2" '
		$0 ~ pat { inside = 1 }
		inside { print FILENAME ":" FNR ":" $0 }
		inside && /^}$/ { inside = 0 }
	' "$1"
}

check() {
	local file=$1 pat=$2 what=$3 found
	found=$(body "$file" "$pat")
	if [ -z "$found" ]; then
		echo "openerrors: HARNESS ERROR: no $what in $file — this check is scanning nothing." >&2
		echo "  If it was renamed or moved, update the pattern. Deleting the scan is not the fix." >&2
		fail=1
		return
	fi
	scanned=$((scanned + 1))

	# errors.Wrap/Wrapf take the sentinel as their first argument, so they are
	# never a violation. errors.New/Errorf and fmt.Errorf construct one with no
	# sentinel at all — which is the whole defect.
	local bare
	bare=$(printf '%s\n' "$found" | grep -E 'errors\.New\(|errors\.Errorf\(|fmt\.Errorf\(' || true)
	if [ -n "$bare" ]; then
		echo "openerrors: a refusal in $what carries no sentinel:" >&2
		printf '%s\n' "$bare" >&2
		echo "" >&2
		echo "  A caller retrying New cannot tell this from a busy disk. Wrap it:" >&2
		echo "    errors.Wrapf(ErrInvalidOptions, \"...\")" >&2
		echo "  If it is NOT an Options refusal — an environment failure, or a" >&2
		echo "  condition that clears on its own like ErrLogLocked — it does not" >&2
		echo "  belong in this function; see New's doc comment for the boundary." >&2
		fail=1
	fi
}

check commitlog.go '^func New[(]opts Options[)]' "New's body"

# Every validate* function, found rather than listed. A hand-kept list is the
# thing that goes stale the one time it matters.
validators=$(grep -lE '^func validate[A-Za-z0-9_]*\(' -- *.go | grep -v '_test\.go' || true)
if [ -z "$validators" ]; then
	echo "openerrors: HARNESS ERROR: no validate* function found in the package." >&2
	echo "  validateTiers existed when this was written. If the convention changed," >&2
	echo "  this check now scans only New and silently covers less than it claims." >&2
	fail=1
fi
for f in $validators; do
	while read -r name; do
		check "$f" "^func $name[(]" "$name"
	done < <(grep -oE '^func validate[A-Za-z0-9_]*' "$f" | sed 's/^func //')
done

# The sentinel itself has to exist and be exported, or every wrap above is
# naming something a caller cannot compare against.
if ! grep -qE '^var ErrInvalidOptions = ' commitlog.go; then
	echo "openerrors: HARNESS ERROR: ErrInvalidOptions is not declared in commitlog.go." >&2
	fail=1
fi

if [ "$fail" -ne 0 ]; then
	exit 1
fi
echo "openerrors: $scanned function(s) that refuse the caller's Options; every refusal carries a sentinel."
