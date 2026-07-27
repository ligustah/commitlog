---
name: Assertion strength audit
interval: 14d
after: 4
paths: *_test.go, compress/*_test.go
---
Take the tests added or materially changed since this task last ran. For each, break the behaviour it
claims to check — delete the fix, revert the guard, disable the mechanism — and confirm the test actually
fails. Restore immediately either way.

A test that passes against the broken code is worse than no test: it reads as coverage and provides none.
This repo has produced several, and the pattern is always the same — the assertion checks the *direction*
of an effect rather than its *magnitude* or its *mechanism*:

- "fewer fsyncs than callers" passed against a barrier that barely coalesced, because 0.8 per caller is
  still fewer than one each. The bar had to become a ratio.
- "more writes than flushes" passed against a flush holding the lock writes needed, because 8 writes per
  flush still clears it. Same fix: assert the ratio, ~477 against ~8.
- A durability test passed with the whole sync replaced by `return nil`, because a reopen in the same
  process reads the page cache. That one cannot be strengthened — it was relabelled to say what it does
  and does not prove.

So: where an assertion survives the break, strengthen it to the ratio or the mechanism, or — if the
property genuinely is not observable from inside a test — say so plainly in the comment rather than
leaving a bar that implies more than it checks.

Bounded per run: audit the recent tests, not the whole suite. Fix what you find in the same run and commit
it; if a test cannot be made to fail without a large refactor, file that as an idea.
