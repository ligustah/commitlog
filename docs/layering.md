# Layers

Everything except `compress/` is ONE Go package. Nothing here is enforced by the
compiler: any file can reach any other, and the layering below survives on
discipline alone. That is the reason to write it down.

Measured rather than asserted — these are reference counts over the tree, not
intentions:

| from → to | refs |
|---|---|
| `index.go` → `*segment` | 0 |
| `segment.go` → `*Reader` | 0 |
| `segment.go` → cleaners | 0 |
| `compact_cleaner.go` → `*commitLog` | 0 |
| `compress/` → commitlog | 0 |

`*commitLog` is named by six files. The cleaners are not among them: compaction
policy is written against `[]*segment`, never against the log.

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

What the layer CAN check is the caller contradicting itself, and one such check
now sits at the top of `CleanWithSpec`: **`StripBelow` must not exceed
`Ceiling`.** Both fields describe the same boundary — `Ceiling` says records at
or above it may be undecided and are retained verbatim, `StripBelow` says records
below it are decided and no longer need their transactional bookkeeping — so
`StripBelow > Ceiling` asserts both about the range in between. The pass acts on
the second: `mergeDigests` marks a record for stripping on
`r.offset < spec.StripBelow` before it consults the ceiling, so records the
ceiling was set to protect keep their offsets and lose the `pid`/`epoch`/`seq`
that would let anyone decide them later.

Note the shape of what makes it checkable, because it is the shape any future
check will have: nothing about the log is consulted. It compares two numbers the
caller wrote down together. The refusal deliberately does NOT fire when `Ceiling`
is unset — the bound is then the log's own high watermark, resolved inside the
pass and free to move, so a check against it would be a race rather than an
invariant.

## Reading the format from outside

`InspectSegment` exists so nobody writes a second decoder for this format. Two
consumers did, both were wrong, and reconciling their disagreement cost days
during a corruption hunt where nothing was corrupt.

If you need to look at bytes on disk, use it. If it cannot answer your question,
extend it — do not reimplement the framing somewhere else, and never copy the
magic byte or a format version into another repo.
