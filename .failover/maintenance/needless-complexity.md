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

Two things this must not mistake for complexity. Some intricacy is a platform tax that the doc comment
records honestly: the Windows sharing-violation retries and the mmap handling exist because the OS behaves
that way, and simplifying them reintroduces a fixed bug. And a guard that looks redundant may be the second
half of a pair that only bites together — check `hack/guardcheck.sh` before removing anything it names.

Read doc comments first and take them seriously as evidence of intent, then judge anyway. Where something
is deliberately intricate and the comment does not say why, the fix may be the missing sentence rather than
a rewrite.

Prove each simplification: full suite green, `hack/guardcheck.sh` still fully covered, and a guard added
when the simpler mechanism has an invariant of its own. Commit in small steps so one bad call is one revert.
Where a simplification is too large to make unilaterally — it changes where a log's identity lives, or what
the store is authoritative for — write it up and ask rather than half-doing it.
