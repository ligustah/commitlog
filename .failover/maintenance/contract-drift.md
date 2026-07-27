---
name: Contract drift
interval: 3d
after: 2
paths: interface.go, commitlog.go, CHANGELOG.md, README.md
---
Check that the documented contract still matches the code. `interface.go` is the published API surface —
its doc comments are what callers build against, and they are the only description of behaviour that
crosses the repo boundary.

Look for:

- a signature, error, or return value described in prose that no longer matches the declaration;
- behaviour asserted in a comment that the code no longer has, especially where a method's guarantees were
  narrowed or widened without the comment following;
- a doc comment that still describes a method as the way to do something after a better one was added
  beside it, so a caller reaching for the documented one gets the worse path;
- CHANGELOG entries describing a release differently from what the tag actually contains.

Correct what is stale. Do NOT write new documentation as part of this — an undocumented method is a
different task from a wrongly-documented one, and only the second is a correctness problem.

Worth knowing why this is scheduled: the durability method on this interface was added, then changed
shape, then had its coalescing behaviour reversed, across a handful of releases. Each change was correct;
the doc comment describing the *previous* one is what a caller would have read. Contract drift here is not
cosmetic — a consumer retired their own batching layer on the strength of what this file said.
