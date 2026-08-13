# Standing complexity sweep — 2026-08-13

The standing task: *"scan your respective repos for things that are obviously too
complicated. We continuously have situations where you describe how something
works and I can immediately think of a MUCH simpler solution. Same for migrations
or backwards compatibility, we don't need either until we are v1."*

Two axes. Recording the negatives as well as the positives, because a sweep that
only writes down what it changed gets re-run against the same clean code.

## Axis 1 — backwards compatibility and migrations

### Descriptor v0 read support — a real one, decided, sequenced

`parseDescriptor` accepts `descriptorFileV0` or `descriptorFileV1`. That is
pre-v1 back-compat and goes.

The decision it forced first, though, was the more interesting half.
`renderDescriptor` stamps V1 **unconditionally**, while omitting the identity
line when unset — so a log that never uses `Options.Identity` gets a V1
descriptor whose bytes a v0.79.x reader could parse perfectly. It announces a
capability the file does not use, and that alone is what makes downgrade
one-way. It is why both downstream repos were holding a workaround: *"keep
Compression and MaxSegmentBytes fixed if you want a rollback path."*

Two coherent answers, in opposite directions:

- **A.** Emit V1 only when an identity is actually present, so the version states
  the minimum reader required. This is what a format that must maximise interop
  does — zip's version-needed-to-extract, PNG chunk criticality. It removes the
  downgrade caveat for every caller not using identity, and keeps two versions
  and a parse branch standing forever.
- **B.** Keep V1 unconditional and delete V0 reading once nobody needs a
  rollback. Ends with one version and no branch.

**Chose B.** A buys interop with old readers, which is exactly what the standing
instruction says we do not want before v1, and it is the more sophisticated of
the two — the sweep is supposed to remove that kind of cleverness, not add it.
B's end state is strictly simpler.

Sequenced rather than done: peers hold live v0.79.x dirs. sqlcdc cleared it
(*"every dir we own is recreated per soak/test run"*); durable_streams asked.
Tracked as #203.

### Checked and clean

- **`block_table_local.go`** — already correct, and instructively so. The
  "every segment sealed before this existed" justification had been struck from
  the comment while the branch stayed, because the failed-write case keeps it
  alive independently. That is the right treatment: a branch defended only by a
  claim about old data invites deletion the day someone checks whether old data
  exists.
- **`block.go` / `parseBlockHeader`** — clean cutover, pre-version segments
  refused outright, "there is deliberately no compatibility path". Nothing to do.
- **`manifest.go`** — "nothing to migrate; a store written by an older build is
  re-offloaded, not converted."
- **`Options.Compression`'s** "byte-for-byte compatible with logs written before
  compression existed" is a statement of fact about what `None` means, not a
  compatibility branch.

## Axis 2 — obvious over-complication

Length ranking was already sampled in an earlier pass (top 5 functions, clean).
This pass swept the surface ranking cannot reach, using the repo's own recorded
lenses: *a knob whose doc apologises*, and *default arms launder bad input*.

### `Options.PrefixReadConcurrency` — a real one, fixed

`concurrencyBudget` defaults on `v <= 0`, so a negative reached the arm that
exists to catch a **missing** value and the caller silently got 8 or 64.

What lifts this above a routine laundered default is that it is reachable **by
following the documentation**. The sibling `CoalesceBytes` knobs are described in
the same `Options` paragraph, and that paragraph teaches that a negative is
meaningful and powerful here — *"NEGATIVE means never coalesce... the
maximum-concurrency and maximum-request-count setting"*. A caller who reads that
and asks for the analogous extreme on the concurrency knob was quietly given the
default. The doc created the expectation the code then violated.

Refused rather than given a meaning: the analogous extreme is unbounded on one
reading and serial on the other, and picking either would be this package
deciding something the caller was trying to say.

The remaining asymmetry — four knobs in one paragraph, two refusing a negative
and two not — is now pinned by a test, because it reads as an oversight and is
not. Without that test, "finishing the job" by adding the two `CoalesceBytes`
fields to the refusal list would go red only in a cost test that never mentions
`New`.

### Not a finding: `RewriteBudget`'s two zero-semantics

`CleanSpec.RewriteBudget` documents `0 = unbounded`. `Options.CleanRewriteBudget`
documents `0` as "default to the cleaner tick" and **negative** as unbounded. The
same concept with opposite zero-semantics in two structs used together is exactly
the shape the sweep is looking for.

It survives inspection. `clean.go:409` reconciles them at the seam
(`if l.Options.CleanRewriteBudget > 0`), so a negative in `Options` is simply not
copied, the spec keeps its `0`, and both mean unbounded. The two structs
legitimately differ in what "unset" means — `Options` is configuration, where
unset should pick a sane default; `CleanSpec` is an explicit per-call spec, where
a field the caller did not fill in means no bound was asked for. The conversion
is deliberate, documented at both ends, and `clean_tier_budget_zero_test.go`
already reasons about it directly.

Recorded so the next sweep does not re-open it.
