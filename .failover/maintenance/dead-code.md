---
name: Dead code detection
interval: 7d
after: 3
paths: *.go, compress/*.go
---
Find and remove unreachable or unused code — exported symbols nothing imports, unexported members never
referenced, permanently-dead branches, orphaned files.

Prove a symbol is unused before deleting it. In an exported package this means checking the test files and
the sibling repos that consume this one, not just the package itself: an exported identifier with no
in-repo caller may still be the whole point of its existence. When in doubt, leave it and note why.

Build and run the full suite after each removal, and commit in small steps so a mistaken deletion is one
revert rather than an unpicking job.

Skip anything reachable only from a fuzz target or a crash-recovery path — those exist to be exercised by
machinery that does not look like a normal call graph.
