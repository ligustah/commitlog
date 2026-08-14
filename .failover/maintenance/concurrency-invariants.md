---
name: Concurrency invariant sweep
interval: 7d
after: 2
paths: commitlog.go, segment.go, index.go, compact_cleaner.go, delete_cleaner.go, reader.go
---
Pick ONE path that reads shared state and then mutates based on what it read, and either prove the two
steps are atomic with respect to every other writer or make them so. One path per run — this is a sweep,
not an audit of the whole package.

This is the shape behind the worst bugs this log has had, and each was silent:

- `Append` read the active segment's next offset and position, encoded a message set stamped with them,
  and only then took the segment's write lock. Two concurrent appends read the same tail and wrote over
  each other: 32 concurrent appends left 3 readable records, with no error to any caller.
- A segment roll ran on the cleaner's own ticker while an append was in flight. "Refuse if the file
  exists" and "create the file" are two steps, so two rollers built a segment over the same files and the
  loser's cleanup unlinked files the winner was using.

Both had passed the full suite for a long time. They stayed invisible because callers happened to
serialize their own writes, and surfaced the moment one stopped.

How to run it: pick a path, write the concurrent test FIRST and watch it fail, then fix. A test that
passes before the fix has not found anything. Prefer asserting an invariant that must hold across the
whole run — every offset handed out exactly once, every record readable, segments partitioning the offset
space — over asserting the absence of a specific interleaving, which you cannot schedule.

Note that `-race` does not find these. They are not data races; every individual access is correctly
locked. The bug is the gap between two correctly-locked operations.

Commit the fix with the failing test. If the path turns out to be safe, say why in a comment on it so the
next run does not re-derive the same argument.
