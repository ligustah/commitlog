# Layers

Everything except `compress/` is ONE Go package. Nothing here is enforced by the
compiler: any file can reach any other, and the layering below survives on
discipline alone. That is the reason to write it down.

Checked rather than asserted, by `hack/layercheck.sh` in CI. The rule is one
sentence: **nothing below the log may name `*commitLog`** — not as a receiver,
not as a field, not as a parameter. The script carries the two halves of the
stack as explicit file lists, and a non-test `.go` file in neither half is an
error, so a new file cannot escape the rule by simply not being mentioned.

These still hold and are worth stating, though only the first is machine-checked:

| from → to | refs |
|---|---|
| lower half → `*commitLog` | 0 (enforced) |
| `index.go` → `*segment` | 0 |
| `segment.go` → `*Reader` | 0 |
| `segment.go` → cleaners | 0 |
| `compress/` → commitlog | 0 (compiler-enforced) |

The cleaners are written against `[]*segment` and never against the log, which
is why compaction policy can be tested without one.

### The metric this replaced, and why it could not fail

An earlier version of this doc defended itself with "`*commitLog` is named by
six files". That number is now ten, and **nothing was violated in between**.
Almost every hit is a `func (l *commitLog)` method DECLARATION, and a file full
of commitLog methods is by definition part of the top layer — so the count was
measuring how the log's 90-odd methods happen to be spread across files, which
is filing, not layering. It would have read exactly the same on a tree where
`index.go` had started calling into the log.

`segment.go` shows the same trap from the other side: it names `SegmentStore`
and `tier` in six places, which looks like the segment layer reaching up into
the tier layer. It is the opposite — `SegmentStore` is an INTERFACE the segment
is handed, so the arrow points down and this is the one place the repo does
dependency inversion on purpose.

The lesson generalises past this file: a count that a violation would not
change is not a measurement, and writing it in a table makes it look like one.

## The stack

```
compress/                     codecs. Knows nothing above it.
message.go message_set.go     record encoding, CRC, framing accessors
block.go                      block framing over compress/
index.go index_mmap_*.go      the offset index, mmap
segment.go segment_store.go   one segment. SegmentStore is an INTERFACE.
keydigest.go manifest.go      per-segment sidecars, tier manifest
tier_state.go                 tier ownership + object reclamation
*_cleaner.go                  compaction and retention POLICY over segments
clean.go                      the log's supervision of a pass
reader.go prefix_*.go         read paths, sequential and digest-planned
inspect.go                    read-only forensics over a file on disk
commitlog.go interface.go     the log. The only public surface that matters.
```

Read it top-down: each line may use the lines above it and must not know about
the lines below. When adding code, the question is not "where is this
convenient" but "what is the highest line that could not do its job without
this".

`SegmentStore` being an interface is the cleanest seam in the repo — the tier is
pluggable and fakeable, and every tier test uses that.

## Where things belong

Three concrete rules, each of which was violated at some point and cost
something:

**Generic helpers go in `util.go`.** `removeAllWithRetry` and
`atomicWriteWithRetry` sat in `commitlog.go` for a long time. They have nothing
to do with the log.

**A function belongs with the interface it takes, not the data it touches.**
`readMessage` and friends lived in `message_set.go` because they parse messages,
but they take a `contextReader` — so the encoding layer depended on the read
layer. They are read operations that use message primitives, and they now live in
`reader.go`.

**Watch the error import when moving anything.** `commitlog.go` and
`tier_state.go` import `pkg/errors` as `errors`; `util.go` and `reader.go` import
the STDLIB `errors` under that name and `pkg/errors` as `pkgErrors`. Both
packages export `New`, so moving an `errors.New` between those two groups
COMPILES and silently drops the stack trace. Only `Wrap` fails loudly, because
the stdlib has none. Check the aliases at both ends before moving code.

## The one thin boundary

`CleanSpec` is parameterized by concepts this package does not own:

```go
Ceiling      Bound                     // really the LSO; At(0) is a real bound
StripBelow   int64                     // ...below which TRANSACTIONAL headers go
StripHeaders []string                  // "pid", "epoch", "seq"
Aborted      func(offset int64) bool   // transaction abort decisions
```

The split itself is right, and deliberate: the log provides mechanism ("drop
these offsets, strip these headers"), the caller supplies policy ("which
offsets"). The callback is what keeps this package ignorant of transactions, and
it should stay that way.

What is thin is VALIDATION. `Ceiling` is the LSO by convention only — commitlog
cannot verify it against anything it owns, so a ceiling that is simply the WRONG
NUMBER is undetectable here and catchable only in the caller. The same holds for
`Aborted`. On the value itself this layer trusts its caller completely.

That is a trade, not an oversight, and it is recorded here so the next person
finds it stated rather than rediscovers it from a bug.

It is tempting to think the layer can at least catch the caller contradicting
ITSELF — two fields that must agree, checkable without knowing what either
means. v0.76.0 tried exactly that and shipped a bug: it refused
`StripBelow > Ceiling` on the reasoning that `Ceiling` says "records at or above
me may be undecided" while `StripBelow` says "records below me are decided", so
the range between them was described both ways at once.

`Ceiling` says no such thing. It bounds COMPACTION. A transactional caller passes
the LSO because undecided records must not be compacted, but that is one reason
to hold the bound down and not the only one — durable_streams builds both fields
equal at the LSO and then lowers `Ceiling` alone to pin records a lagging
consumer group has not read. And the pass could not have done damage with the
pairing anyway: `classify` returns `dispRetain` for `offset >= spec.ceiling`
before it looks at `StripBelow`, so the ceiling already wins and a `StripBelow`
above it simply stops applying. The refusal protected against something that
could not happen, while rejecting every pass on a stream that had a decided
transaction and a slow group at once. v0.77.0 took it back out.

The lesson is not "be more careful with invariants" — it is that an invariant
which reads as arithmetic between two fields is still a claim about their
MEANING, and the meaning lives in the caller. That is what "this layer does not
own these concepts" costs, stated concretely. If a check on `Ceiling` is ever
proposed again, the question to answer first is not whether it is cheap but
whether this package is entitled to hold the opinion it encodes.

## Reading the format from outside

`InspectSegment` exists so nobody writes a second decoder for this format. Two
consumers did, both were wrong, and reconciling their disagreement cost days
during a corruption hunt where nothing was corrupt.

If you need to look at bytes on disk, use it. If it cannot answer your question,
extend it — do not reimplement the framing somewhere else, and never copy the
magic byte or a format version into another repo.
