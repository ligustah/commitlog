# commitlog

[![Go Reference](https://pkg.go.dev/badge/github.com/ligustah/commitlog.svg)](https://pkg.go.dev/github.com/ligustah/commitlog)

A file-backed write-ahead log for Go: segmented, mmap-indexed, with
transparent block compression and transaction-aware compaction. The
storage engine under
[`durable_streams`](https://github.com/ligustah/durable_streams) (typed
exactly-once streams) and sqlcdc (incrementally maintained SQL views over
CDC), hardened by their adversarial kill -9 soaks.

Forked from the internal commitlog of
[liftbridge-io/liftbridge](https://github.com/liftbridge-io/liftbridge)
by [Tyler Treat](https://github.com/tylertreat) (everything unrelated to
the log removed); substantially extended since — see
[CHANGELOG.md](CHANGELOG.md).

## Features

- **Segmented log** with sparse offset indexes, high-watermark
  checkpointing, retention by bytes/messages/age, head truncation
  (`TruncateBefore`), and crash recovery that recovers the REAL tail
  (CRC forward-scan) instead of truncating to the last checkpoint.
- **Block compression** (snappy, s2, zstd): segments store compressed
  blocks while offsets, framing and indexes stay logical; raw and
  compressed segments coexist byte-compatibly. Cleans consolidate tiny
  blocks; sub-4KB appends skip the codec until then.
- **Transaction-aware compaction** (`CleanWithSpec`): latest-per-key with
  a caller-supplied decided ceiling, aborted-record removal, tombstone GC
  with retention, control-marker dropping and header stripping — driven by
  per-segment key-digest sidecars (streaming k-way merge, no global key
  map) with time-budgeted, drop-density-ordered rewrites. Returns a
  verified floor that callers persist to bound reopen scans.
- **Tiered storage** (`OffloadBefore`): sealed segments — log *and* index —
  move into a chain of `Tier`s (`Options.Tiers`, each a named `SegmentStore`,
  `FileSegmentStore` included) and are served read-through, so an offloaded
  segment is indistinguishable from a local one at the read API. Segments
  descend into the first tier and go further down on the caller's word
  (`CleanSpec.TierPlacement`); retention, ownership and rewrite budgets are
  per tier, so a record is gone only when the last tier runs out of room for
  it. Offloaded indexes are held in `RemoteIndexCache`, a process-wide LRU
  with pin counts, so an index cannot be evicted out from under a live seek.
- **Client sidecars**: atomic named metadata files in the log directory
  (`PutSidecar`/`GetSidecar`/`RemoveSidecar`) for checkpoints like
  recovery floors. Names carry the reserved `ClientSidecarPrefix`, which the
  log never writes and its directory scans skip — so neither side has to know
  what the other calls its files.
- Zero-allocation metadata scans, buffered sequential readers, on-demand
  full-durability barrier (`SyncAll`), leader-epoch tracking, and a
  non-durable close for directories that are about to be thrown away
  (`CloseDiscarding`, which then refuses to reopen them).

## Use

```go
import (
    "github.com/ligustah/commitlog"
    "github.com/ligustah/commitlog/compress"
)

log, err := commitlog.New(commitlog.Options{
    Path:        "orders-log",
    Compression: compress.Zstd,
    Compact:     true,
})
offsets, err := log.Append([]*commitlog.Message{{Key: k, Value: v}})
log.SetHighWatermark(offsets[len(offsets)-1])

r, err := log.NewReader() // committed, from the oldest surviving record
msg, offset, _, _, err := r.ReadMessage(ctx, make([]byte, commitlog.HeaderBufferLen))
```

This is an excerpt of `Example` in [example_test.go](example_test.go), which is
the copy that is compiled and output-checked. Read that one when the two
disagree: this snippet went stale once already, when the reader became
option-based and only the executed copy was updated.

The [package documentation](https://pkg.go.dev/github.com/ligustah/commitlog)
covers the full surface; [CHANGELOG.md](CHANGELOG.md) records the format
and API history.
