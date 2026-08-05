---
name: Needless complexity
interval: 3d
after: 2
paths: *.go, compress/*.go, hack/*.sh
---
Find things that are obviously too complicated and make them simple. The test is the user's: if explaining
how something works would let someone immediately propose a MUCH simpler solution that covers the same
cases, the simpler one is the design and this one is debt.

Look for state re-derived when it could be kept and persisted; polling where a notification already exists;
two caches of one fact, kept in step by hand; a mechanism whose whole job a smaller one would do. A long
doc comment explaining why something is intricate is a lead, not a defence — the comment may be evidence
that the intricacy was never justified, only rationalised.

**Delete migrations and backwards compatibility outright.** This library is pre-v1 and has no deployments
to migrate, so compatibility shims, version-tolerant parsers, "older versions wrote X" fallbacks, dual-format
readers and converters are cost with no beneficiary. Take the correct design over the compatible one and
delete the bridge; do not write a converter for data no one has. Format *version fields* stay — they make a
future change detectable — but the branches that accept superseded formats go.

Look hard at *how* each compatibility branch is reached, because the first round found the same thing three
times: the branch was not dead, it was the one accepting corrupt input. A dual-format reader picks the old
format by a negative test — "the first byte is not `{`", "no version field" — and every truncated, garbled
or unrelated file satisfies a negative test. So the fallback ran for input that was not old, it was broken,
and it laundered that input into something that looked valid. When you delete one, check what the remaining
rule now refuses that it used to accept, and add a guard for it: that is usually the real find, and it is
worth a changelog entry of its own.

Two things this must not mistake for complexity. Some intricacy is a platform tax that the doc comment
records honestly: the Windows sharing-violation retries and the mmap handling exist because the OS behaves
that way, and simplifying them reintroduces a fixed bug. And a guard that looks redundant may be the second
half of a pair that only bites together — check `hack/guardcheck.sh` before removing anything it names.

Two branches on one condition are not automatically duplication either. A log keeps its descriptor in its
store when it has one and in its directory when it does not; that is two places for one fact, but never at
the same time, and collapsing it would mean a store-backed log whose identity lives somewhere a peer with
only the store cannot reach. The test is whether both can be live at once — if they can, that is the drift
worth removing; if the condition picks exactly one, the branch is the design.

Read doc comments first and take them seriously as evidence of intent, then judge anyway. Where something
is deliberately intricate and the comment does not say why, the fix may be the missing sentence rather than
a rewrite.

Prove each simplification: full suite green, `hack/guardcheck.sh` still fully covered, and a guard added
when the simpler mechanism has an invariant of its own. Commit in small steps so one bad call is one revert.
Where a simplification is too large to make unilaterally — it changes where a log's identity lives, or what
the store is authoritative for — write it up and ask rather than half-doing it.
