# Duct-tape audit — 2026-08-13

Asked for by the user across all three repos: *"inspect whether the fixes you made
were purely duct taped and see if maybe a cleaner architecture or more sound
approach would be the better solution."*

Scope: commitlog, the 52 non-merge commits from v0.70.0 to v0.79.0, plus anything
older those point at. Findings are ordered by how much I would want to change,
not by how bad the original bug was. Nothing here has been acted on — this is the
inspection, and two of the three recommendations are breaking-ish enough to want
a yes first.

---

## 1. The working-copy disposal rule is a convention, and it has been forgotten
   three times

**Verdict: duct tape. Recommend rebuilding.**

Every rewrite path in this package builds a suffixed working copy
(`.cleaned`/`.truncated`/`.trimmed`/`.joined`), fills it, and then renames it
over its source. Until that rename nothing can reach it: it is not in
`l.segments` and the source does not link to it. So a failure between those two
points has to delete it, or the process holds a file handle and an index mapping
until it exits and leaves the suffixed files on disk.

That rule is written out, in prose, at four separate sites, in three different
shapes:

| site | shape |
|---|---|
| `compact_cleaner.go:843` (`cleanSegment`) | `defer` + a `disposed` flag + suffix check |
| `compact_cleaner.go:1111` (`consolidateOne`) | `defer` + suffix check |
| `clean_join.go:211` (`joinOne`) | `defer` + suffix check |
| `commitlog.go:1793` (`Truncate`) | a `dropReplacement()` closure called by hand at two returns |

The first three are near-identical code under near-identical paragraph-length
comments, each explaining that "the suffix is the discriminator". The fourth does
not check the suffix at all; it relies on the reader knowing `Replace` has not run
yet.

This is not a hypothetical fragility. Two commits one day apart fixed the same
defect from opposite sides:

- `0b73e7f` — a failed compaction pass abandoned rewrites it had already
  **installed** (they had to be published)
- `806f08c` — a failed truncate stranded a replacement it had **not** installed
  (it had to be dropped)

and its own commit message says so: *"What decides which is whether the rename has
happened, and nothing else."* That is a correct diagnosis and it did not lead to a
mechanism — each site kept its own copy of the reasoning. The next rewrite path
added to this package will be a fourth chance to forget.

**The sounder shape.** "Published or not" is a property of the working copy, not
of each caller. `seg.Cleaned()` / `seg.Joined()` already mint these; they should
hand back something that owns the rule:

```go
wc, err := seg.Cleaned()   // returns a workingCopy, not a bare *segment
defer wc.DropIfUnpublished()
...
wc.Publish()               // the rename; after this DropIfUnpublished is a no-op
```

One place holds the suffix check, one place holds the comment, and a new rewrite
path cannot get it wrong by omission — the worst it can do is fail to write the
`defer`, which is a visible missing line rather than an invisible missing
paragraph. `Truncate`'s hand-called closure disappears entirely.

Cost: it touches four call sites and the `Cleaned`/`Joined`/`Trimmed`
constructors. No file format changes, no behaviour change intended — the guards
that cover the four existing sites should stay green throughout, which is a good
falsification test for the refactor.

---

## 2. guardcheck anchors on literal source text, and pays for it every refactor

**Verdict: duct tape, but the cheap kind. Recommend a bounded change, not a
rewrite.**

`hack/guardcheck.sh` proves each of its 143 guards is real by textually replacing
a line of source with a neutralized version and requiring the named test to go
red. The anchor is a literal string. When a refactor rewrites that line, the
anchor stops matching and the guard reports SKIP.

That has cost **eight** dedicated commits so far, most recently today's `9c653ee`
— splitting `closeIndex(durable)` into `closeIndex(flush, trim)` silently
disarmed the guard on the line below it:

```
bf59418  e3784f2  86b2742  1969dc2  7f9ce08  af4295f  aaac8e9  bb84cc0
```

Two things are already right and should be left alone. SKIP is a hard failure —
the run exits non-zero and CI has a `guard coverage` job — so a disarmed guard
cannot rot silently. And the neutralization must still compile, which stops a
guard from "passing" against nonsense.

What is wrong is only the anchor. A literal line is the most volatile thing in
the file, and it is chosen for no reason other than being easy to `sed`.

**The sounder shape**, in increasing order of effort:

1. Anchor on a marker comment placed next to the guarded line
   (`// guard: close honours the index dirty mark`). A refactor that moves the
   line carries the comment with it, because it is attached to it. This is a
   one-line change per guard and removes most of the churn.
2. Anchor on `function name + occurrence`, resolved with `gofmt`-aware tooling
   rather than string matching.

I would do (1). (2) is a real program and the failure it prevents is already
loud.

---

## 3. Index recovery had four mechanisms and one uncovered case — now fixed at
   the root, and worth recording as a shape

**Verdict: was duct tape, is not any more. No further action.**

Recorded because it is the clearest example in this window of a fix that could
have been a patch and was not.

Before today there were four separate index-repair mechanisms:

| mechanism | covers |
|---|---|
| `reconcileIndexTail` | index BEHIND its log — active segment only |
| `rebuildOverIndex` + `rebuildIndexFromLog` | index AHEAD of its log |
| `scanBlocks` | block table lost |
| `RecoverTail` | log ahead of the watermark |

Each was added for the case in front of whoever added it, and between them they
left one state uncovered: an index behind its log on a **sealed** segment. Nobody
noticed, because the symptom is silence — `setupIndex` takes `lastOffset` from
the index's last entry, so the segment simply answers as if the missing records
are not there. Measured: one lost index entry cost 56 of 60 records.

The patch available was to fsync harder, and it was already in the tree —
`dirtyIndex` starting true for every reopened segment, one device-cache flush per
segment at every shutdown, justified as "we cannot know the predecessor's flush
state". That justification does not survive being written down: an fsync at CLOSE
cannot recover a predecessor's lost tail, it only writes back whatever survived.
It was paying a real cost for a protection it did not provide.

The fix was to make the invariant true instead — the index is derived, every
segment's is checked at open, a short one is filled in — which retired the fsync
as a side effect rather than as a goal. That is the shape finding 1 is asking
for: state the invariant once, in the place that can enforce it.

---

## Not in scope here

The standing complexity sweep — *"scan for things that are obviously too
complicated... same for migrations or backwards compatibility, we don't need
either until we are v1"* — is a separate pass and is not covered by this
document.
