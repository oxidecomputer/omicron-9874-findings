# CockroachDB index backfill OOM analysis

This document analyzes the memory allocation behavior during CockroachDB's `CREATE INDEX` backfill operation, explaining why large tables trigger "memory budget exceeded" failures and how `bulkio.index_backfill.batch_size` affects the threshold.

All source references are against [`oxidecomputer/cockroach@367bca4`][commit].

[commit]: https://github.com/oxidecomputer/cockroach/tree/367bca413bc24e6213a45663fccd583cc726ba08

## Background

When CockroachDB creates a secondary index on an existing table, it performs an *index backfill*: a bulk read of every row followed by construction and ingestion of index entries. The backfill runs as a [DistSQL][distsql] processor ([`indexBackfiller`][indexbackfiller]) with two concurrent goroutines communicating over a buffered channel:

- The producer (`constructIndexEntries`) reads rows in chunks of [`bulkio.index_backfill.batch_size`][batch-size-setting] (default 50,000), encodes index entries, and sends batches over a [channel of capacity 10][channel-cap].
- The consumer (`ingestIndexEntries`) pulls batches from the channel and [adds entries one at a time][consumer-loop] to a [`BufferingAdder`][buffering-adder], which accumulates them in a [`kvBuf`][kvbuf-struct], sorts, and flushes to SSTables.

[distsql]: https://www.cockroachlabs.com/docs/stable/architecture/sql-layer#distsql
[indexbackfiller]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/rowexec/indexbackfiller.go
[batch-size-setting]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/backfill.go#L78-L84
[channel-cap]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/rowexec/indexbackfiller.go#L308
[consumer-loop]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/rowexec/indexbackfiller.go#L250-L268
[buffering-adder]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/buffering_adder.go#L38-L66
[kvbuf-struct]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L28-L31

## Memory monitor hierarchy

All memory allocations during the backfill are tracked by [`BytesMonitor`][bytes-monitor] instances arranged in a hierarchy rooted at [`rootSQLMemoryMonitor`][root-monitor], whose budget is `--max-sql-memory` (defaulting to 128 MiB on illumos):

```
rootSQLMemoryMonitor (128 MiB)                    [server_sql.go:354]
  └─ bulkMemoryMonitor (inherits limit)            [server_sql.go:493]
       ├─ backfillMemoryMonitor                    [server_sql.go:499]
       │    └─ IndexBackfiller.boundAccount        ← producer batch memory
       └─ bulk-adder-monitor                       [server_sql.go:633]
            └─ BufferingAdder.memAcc               ← kvBuf slab + entries
```

The producer's batch allocations (`GrowBoundAccount` in [`BuildIndexEntriesChunk`][build-chunk]) and the consumer's kvBuf growth (`acc.Grow` in [`kvBuf.fits`][kvbuf-fits]) draw from the same 128 MiB root budget. When the combined usage exceeds the budget, the next `Grow` call fails with a ["memory budget exceeded" error][budget-exceeded-error].

[bytes-monitor]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/util/mon/bytes_usage.go
[root-monitor]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/server/server_sql.go#L354-L363
[build-chunk]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/backfill/backfill.go#L773-L991
[kvbuf-fits]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L53-L135
[budget-exceeded-error]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/util/mon/resource.go#L25-L37

## The kvBuf: two allocations competing for budget

The [`kvBuf`][kvbuf-struct] stores index entries compactly using two parallel arrays:

1. **`slab []byte`** — raw key/value bytes packed contiguously ([`bpe`](#bpe) bytes per entry).
2. **`entries []kvBufEntry`** — [16 bytes per entry][kvbuf-entry] (two `uint64` packing offset and length into the slab).

Both arrays grow by doubling per step, clamped to `[minSlabGrow, maxSlabGrow]` and `[minEntryGrow, maxEntryGrow]` respectively. The [growth constants][growth-constants] are:

```go
const minSlabGrow = 512 << 10   // 512 KiB
const maxSlabGrow = 64 << 20    // 64 MiB
const minEntryGrow = 1 << 14    // 16K items = 256 KiB
const maxEntryGrow = (4 << 20) >> entrySizeShift  // 256K items = 4 MiB
```

A [balancing loop][balancing-loop] in `fits()` adjusts entries growth proportionally to slab growth, so the two arrays stay roughly in ratio.

> [!NOTE]
> The code contains a [reduction loop][slab-budget-reduction] that decrements the allocation by `minSlabGrow` when `remaining` (the gap between current usage and `maxBufferLimit`) is tight. However, for index backfills this loop is inert: `maxBufferLimit` is set to `max_buffer_size` (512 MiB), so `remaining` is always enormous relative to the growth request. Growth against the root budget is mediated solely by `acc.Grow()`. There are no partial-doubling steps.

Each growth step calls [`acc.Grow(needed)`][grow-call], which charges the root monitor. Crucially, the account is **monotonically non-decreasing**: [`Reset()`][kvbuf-reset] sets `len=0` but preserves `cap`, and the memory account is never shrunk (unless cumulative [underfill exceeds 1 GiB][underfill-check]).

[kvbuf-entry]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L37-L40
[growth-constants]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L42-L46
[balancing-loop]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L96-L107
[slab-budget-reduction]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L75-L77
[grow-call]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L119
[kvbuf-reset]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L205-L211
[underfill-check]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/buffering_adder.go#L187-L192

## Slab doubling and the growth accounting identity

Because each growth step charges the account by exactly the capacity added ([`acc.Grow(needed)`][grow-call] on line 119, then [`make(..., cap+slabGrow)`][slab-realloc] on line 131 and [`make(..., cap+entryGrow)`][entry-realloc] on line 126), there is an identity:

> **Cumulative account growth = final array capacity.**

* Growth is a pure doubling sequence: 512K → 1M → 2M → 4M → 8M → 16M → 32M → 64M. The cumulative charges are 512K + 512K + 1M + 2M + 4M + 8M + 16M + 32M = 64M, which equals the final slab capacity. The same holds for the entries array.
* The identity holds because each step charges `acc.Grow(needed)` by exactly the capacity added (line 119 vs lines 126/131).

[slab-realloc]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L131
[entry-realloc]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L126

This means we can reason about peak memory usage directly in terms of final capacities.

There's a [32 MiB initial reserve][reserve] (`schemachanger.backfiller.buffer_size`). `Reserve(R)` pre-charges `R` to the root monitor and stores it as a local pool. Subsequent `Grow` calls consume from the pool first; only excess beyond `R` charges the parent again. Once cumulative growth exceeds `R`, the reserve is fully consumed and the total root charge equals the cumulative growth:

```
total_kvBuf_root_charge = max(R, slab_cap + entries_cap)
                        = slab_cap + entries_cap     (for slab ≥ 32M)
```

[reserve]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/buffering_adder.go#L84-L127

## The kvBuf flush cycle

On the consumer side, when `fits()` returns false (because `acc.Grow` fails against the root monitor), the `BufferingAdder` handles it gracefully. The [`Add` method][add-flush] calls `doFlush()`, which sorts the accumulated entries, ingests an SSTable, and calls [`curBuf.Reset()`][do-flush-reset]. As mentioned above, `Reset()` sets `len=0` but preserves capacity and does not shrink the memory account (the only post-flush `Shrink` path requires accumulated [underfill > 1 GiB][underfill-check], which is effectively unreachable here since the buffer is nearly full when it flushes).

After the flush, `fits()` is called again with the empty buffer. Since `len=0` and existing capacity is ample, it succeeds via the [fast path][kvbuf-fits] (lines 54–56): the entry fits within current capacity, so no `Grow` is needed. The entry is appended, and the buffer begins refilling within existing capacity — no new `Grow` calls until capacity is exhausted again.

When the buffer fills up again, `fits()` tries to double again. If the root budget still cannot accommodate the growth, the cycle repeats: flush, refill, flush. The system can process unlimited entries by cycling flush/fill at whatever capacity it reached.

The upshot of this is that the consumer never causes a fatal error. Running out of memory triggers a flush instead. The fatal path is actually in the producer's `GrowBoundAccount`, which has no such graceful fallback.

[add-flush]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/buffering_adder.go#L167-L208
[do-flush-reset]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/buffering_adder.go#L331

## <a name="bpe"></a>Bytes per entry (bpe)

Each secondary index entry encodes as a key of the form:

```
/TableID/IndexID/IndexedCol₁/.../IndexedColₙ/PKCol₁/.../PKColₘ
```

The encoded size (`bpe`) depends on the schema. For the `console_session` table (UUID primary key, `TIMESTAMPTZ` index on `time_created`), empirical measurement gives `bpe ≈ 37` bytes.

With the entries array overhead of 16 bytes per entry, the effective per-entry cost to the memory budget is `bpe + 16` bytes.

## The critical slab doubling step

The slab doubles through a predictable sequence: 512K, 1M, 2M, ..., 32M, 64M. The step from 32M to 64M is the critical one because its outcome determines whether the system enters a safe steady state or a danger zone. At that point, the kvBuf account and the growth request both depend on bpe through the entries array, which the [balancing loop][balancing-loop] sizes proportionally to the slab.

At slab capacity `S`, the entries array holds `S / bpe` entries at 16 bytes each, so `entries_cap ≈ 16S / bpe`. When the 32 MiB slab fills up, `fits()` tries to double to 64 MiB. The growth request is `~(32 + 512/bpe)` MiB (slab + entries). This succeeds iff the root budget can accommodate the current kvBuf account plus the growth plus in-flight batches plus fetcher overhead.

**If the growth fails**: the adder flushes, and the slab stays at 32 MiB. The system continues safely — at slab=32M, the remaining budget (~72 MiB for bpe=37) comfortably holds the full batch pipeline. The buffer cycles flush/fill indefinitely at 32 MiB.

**If the growth succeeds**: the kvBuf account jumps to the post-doubling peak, leaving far less headroom. This is where the danger begins — the producer's subsequent `GrowBoundAccount` calls may fail in this reduced headroom.

Post-doubling state for concrete schemas:

| Schema | bpe | entries (16S/bpe) | Pre-doubling kvBuf | Growth request | Post-doubling kvBuf |
|---|---|---|---|---|---|
| INT8 PK, BOOL idx | ~29 | ~17.7 MiB | ~49.7 MiB | ~49.7 MiB | ~99.3 MiB |
| UUID PK, TIMESTAMPTZ idx | ~37 | ~13.8 MiB | ~45.8 MiB | ~45.8 MiB | ~91.7 MiB |
| STRING(40) PK, TIMESTAMPTZ idx | ~64 | ~8.0 MiB | ~40.0 MiB | ~40.0 MiB | ~80.0 MiB |

## About the producer-consumer channel

The producer and consumer run as concurrent goroutines communicating over a [channel of capacity 10][channel-cap]. The producer calls [`GrowBoundAccount`][grow-bound-account] to charge batch memory before sending, and the consumer calls [`ShrinkBoundAccount`][shrink-call] to release it after processing all entries in the batch:

```go
for indexBatch := range indexEntryCh {
    for _, indexEntry := range indexBatch.indexEntries {
        ib.adder.Add(...)  // kvBuf growth happens here; batch still live
    }
    indexBatch.indexEntries = nil
    ib.ShrinkBoundAccount(ctx, indexBatch.memUsedBuildingBatch)  // freed after
}
```

At any moment, there can be up to `k` batches in flight: one being consumed, plus up to 10 buffered in the channel (and possibly one being built by the producer). Each batch consumes approximately `batch_size × (bpe + E)` bytes, where `E ≈ 56` is `sizeof(rowenc.IndexEntry)` (the [Go struct overhead][sizeof-entry] tracked by `GrowBoundAccount` in [`BuildIndexEntriesChunk`][build-chunk]). All `k` batches are charged to the root monitor simultaneously.

While the channel size applies backpressure on the producer, memory use does not: when `GrowBoundAccount` fails, the error propagates up and fails the entire schema change job! There is no retry or flush mechanism within the producer. This is the fatal path. By contrast, as mentioned above, the consumer is graceful about this.

The OOM occurs when both of the following are true:

* The 32M → 64M doubling has already succeeded (i.e., the kvBuf is at `64 + 1024/bpe` MiB); and,
* The producer's subsequent batch allocation pushes total root usage over `B`.

The post-doubling headroom available for batches is:

```
headroom = B − (64 + 1024/bpe) − F    (MiB)
```

where `F` is overhead from the row fetcher's scan buffers ([`DefaultBatchBytesLimit`][batch-bytes-limit] = 10 MiB) and other root monitor charges. The producer's `GrowBoundAccount` fails when in-flight batches exceed this headroom:

```
k × batch_size × (bpe + E) / 2²⁰ > headroom    (MiB)
```

The doubling itself succeeds when pipeline depth is momentarily low. This requires the kvBuf account, the growth request, and in-flight batches to fit within `B`:

```
64 + 1024/bpe + k × batch_size × (bpe + E) / 2²⁰ ≤ B    (MiB)
```

The failure is therefore non-deterministic: the doubling succeeds during a transient low-`k` window, then the producer hits a transient high-`k` event in the reduced post-doubling headroom.

[batch-bytes-limit]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/rowinfra/base.go#L41-L43

[shrink-call]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/rowexec/indexbackfiller.go#L268
[sizeof-entry]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/backfill/backfill.go#L784
[grow-bound-account]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/backfill/backfill.go#L789

## Three regimes

The outcome of the 32M → 64M doubling attempt, combined with the post-doubling headroom, partitions the (bpe, batch_size, k) space into three regimes.

### Regime A (safe): doubling impossible (`bpe < bpe_min`)

Setting `k = 0` (no batches at all) gives a hard floor for the doubling to be possible:

```
64 + 1024/bpe ≤ B
bpe ≥ 1024 / (B − 64)
```

For `B = 128 MiB`: **`bpe_min = 16`**. When `bpe < 16`, the post-doubling kvBuf account alone exceeds the root budget, so the growth request always fails. In practice, the fetcher overhead (`F ≈ 10 MiB`) raises the effective floor to `1024 / 54 ≈ 19`.

This means that growth always fails, the adder flushes gracefully, and the kvBuf cycles flush/fill at 32 MiB indefinitely. At slab=32M, the remaining budget is ample: `128 − (32 + 512/bpe) − F`. Even at bpe=16 (kvBuf=64M), the remaining 54 MiB easily holds the full batch pipeline (k=12 × 3.4M = 41M). The system processes unlimited rows.

This boundary is essentially unreachable with real Omicron schemas (all UUID PKs, bpe ≥ 29).

| Schema | PK contribution | Indexed col | Overhead | bpe (est.) | Regime |
|---|---|---|---|---|---|
| INT8 PK, BOOL idx | ~9 | ~2 | ~8 | ~19 | B |
| INT8 PK, INT8 idx | ~9 | ~9 | ~8 | ~26 | B |
| UUID PK, BOOL idx | ~19 | ~2 | ~8 | ~29 | B |
| UUID PK, INT4 idx | ~19 | ~5 | ~8 | ~32 | B |
| UUID PK, TIMESTAMPTZ idx | ~19 | ~11 | ~7 | ~37 | B |

### Regime B (OOM): doubling succeeds, insufficient headroom (`bpe ≥ bpe_min`, default batch_size)

When `bpe ≥ bpe_min`, the 32M → 64M doubling can succeed when pipeline depth is momentarily low. The doubling succeeds when `k` is small enough that in-flight batches leave room for the growth request. For `console_session` (bpe ≈ 37), the doubling requires `k ≤ 5`.

Once the doubling succeeds, the kvBuf account consumes most of the budget. The post-doubling headroom available for batches is:

```
headroom = B − (64 + 1024/bpe) − F    (MiB)
```

For `console_session` (bpe ≈ 37, headroom ≈ 26 MiB), each batch costs `50,000 × 93 bytes ≈ 4.4 MiB` at the default batch size. The producer's `GrowBoundAccount` fails when `k ≥ 6` (6 × 4.4 = 26.4 MiB ≈ headroom).

The failure is non-deterministic: the doubling succeeds during a transient low-`k` window, then the producer hits a transient high-`k` event. More rows means more fill/flush cycles at 32 MiB before the doubling, which increases the probability that one cycle coincides with low `k` (this is why the failure appears to be deterministic in practice). Once the doubling succeeds, the system remains in the post-doubling danger zone permanently (the account never shrinks), and a high-`k` event eventually triggers the fatal `GrowBoundAccount` failure.

Post-doubling headroom versus regime at default `batch_size = 50,000`:

| bpe | Post-doubling headroom | Per-batch cost | Regime (k=3) | Regime (k=7) | N_max (approx.) |
|---|---|---|---|---|---|
| 16 | −10.0 MiB | 3.4 MiB | A | A | — |
| 29 | 18.7 MiB | 4.1 MiB | C (12.2 < 18.7) | B (28.5 > 18.7) | ~2.13M |
| 37 | 26.3 MiB | 4.4 MiB | C (13.3 < 26.3) | B (31.0 > 26.3) | ~1.81M |
| 50 | 33.5 MiB | 5.1 MiB | C (15.2 < 33.5) | B (35.5 > 33.5) | ~1.45M |
| 64 | 38.0 MiB | 5.7 MiB | C (17.2 < 38.0) | B (40.1 > 38.0) | ~1.20M |

Note that a smaller bpe means more entries per slab, which inflates the entries array overhead and leaves less post-doubling headroom, but also reduces per-batch cost. The exact pipeline depth at the critical moment varies with timing, making the B/C boundary non-deterministic.

The `N_max` column gives the approximate row count at which the doubling becomes likely to succeed during at least one fill/flush cycle. The formula `N_max ≈ (B − R) / (bpe + 16)` (where `R` = 32 MiB reserve) is a useful empirical approximation: it estimates the number of entries that fills the 32 MiB slab, multiplied by the number of flush cycles before the doubling is likely to succeed. The exact threshold is timing-dependent. See [empirical-validation.md](empirical-validation.md) for raw test data.

All tested schemas in `test-thresholds.sh` exhibited Regime B behavior at the default `batch_size = 50,000`.

### Regime C (safe): doubling succeeds, sufficient headroom (`bpe ≥ bpe_min`, small batch_size)

This regime has the doubling mechanics as Regime B: the 32M → 64M doubling eventually succeeds during a low-`k` window, and the kvBuf account jumps to `64 + 1024/bpe` MiB. The difference is that `batch_size` is small enough that even worst-case pipeline depth fits in the post-doubling headroom.

At `batch_size = 5,000`, each batch costs `5,000 × 93 bytes ≈ 0.44 MiB`. Even `k = 12` (i.e., a full pipeline) uses only 5.3 MiB — well within the ~26 MiB headroom for bpe=37. The producer's `GrowBoundAccount` always succeeds, and the system processes unlimited rows.

Reducing `batch_size` from 50,000 to 5,000 reliably shifts schemas from Regime B to C. Increasing `B` to 256 MiB achieves the same effect by widening the post-doubling headroom.

## Tunables analysis

Four settings govern memory usage during index backfill. Only one is effective via SQL at migration time.

### `schemachanger.backfiller.buffer_size` (default 32 MiB)

This is the [Reserve][ba-reserve] amount for the kvBuf's [`BoundAccount`][bound-account]. `Reserve` charges the full amount to the root monitor upfront; subsequent `Grow` calls consume from this pool first, only charging the parent for the excess. Since `acc.Used()` exceeds `buffer_size` well before the critical doubling step (at slab=32M, `acc.Used ≈ 46M > 32M`), the reserve covers only early growth. Increasing it pre-charges more upfront, leaving less headroom for batches.

This means that altering the buffer size is not helpful.

[bound-account]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/util/mon/bytes_usage.go#L642-L653
[ba-reserve]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/util/mon/bytes_usage.go#L712-L727

### `schemachanger.backfiller.max_buffer_size` (default 512 MiB)

The kvBuf's [own growth limit][kvbuf-max]. The root monitor rejects growth via `acc.Grow()` long before this limit is reached (128M root budget << 512M kvBuf limit), so changing it has no effect.

[kvbuf-max]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/rowexec/indexbackfiller.go#L65-L68

### `bulkio.index_backfill.batch_size` (default 50,000)

Reducing from 50,000 to 5,000 cuts per-batch cost by 10×, so the post-doubling headroom accommodates worst-case pipeline depth.

This is effective for all practical schemas (bpe > 16). Use a batch size of 5,000, since 10,000 appears to be paradoxically worse for compact schemas.

```sql
SET CLUSTER SETTING bulkio.index_backfill.batch_size = 5000;
```

### `--max-sql-memory` (default 128 MiB on illumos)

The root monitor budget. This is a process startup flag, not a cluster setting.

> [!NOTE]
> On Linux, `--max-sql-memory` defaults to 25% of system memory. The path to measure memory is stubbed out on illumos, and CockroachDB falls back to a 128 MiB default.

Peak memory at the critical doubling is U-shaped in bpe: compact entries inflate the entries array, while wide entries inflate the batch pipeline. At default `batch_size = 50,000` and worst-case `k = 12`:

| bpe | kvBuf peak | Batch pipeline (k=12) | F | Total |
|---|---|---|---|---|
| 19 (most compact real schema) | 117.9 MiB | 42.9 MiB | 10 MiB | **170.8 MiB** |
| 37 (console_session) | 91.7 MiB | 53.2 MiB | 10 MiB | **154.9 MiB** |
| 42 (minimum of the curve) | 88.4 MiB | 56.0 MiB | 10 MiB | **154.4 MiB** |
| 100 (wide compound index) | 74.2 MiB | 89.3 MiB | 10 MiB | **173.5 MiB** |
| 150 (very wide) | 70.8 MiB | 117.9 MiB | 10 MiB | **198.7 MiB** |

256 MiB covers all realistic schemas with 55+ MiB of headroom. The trade-off: `--max-sql-memory` governs all SQL memory, so doubling it means CockroachDB can consume 128 MiB more under peak SQL workload.

**Verdict:** effective for all regimes. **256 MiB** is the recommended value.

### Summary of tunables

| Setting | Scope | Effect |
|---|---|---|
| `buffer_size` | cluster | no effect |
| `max_buffer_size` | cluster | no effect |
| `batch_size` | cluster | **fixes** (use 5,000; avoid 10,000 for compact schemas) |
| `--max-sql-memory` | startup | **fixes** (use 256 MiB) |

## Recommended mitigation

Do both:

1. Increase `--max-sql-memory` to 256 MiB.
2. Set `bulkio.index_backfill.batch_size = 5000`.

Either fix alone is sufficient for most practical schemas. Together they make the OOM failure essentially unreachable.

### Why either alone is sufficient

**`batch_size = 5000` at 128 MiB.** The breakeven is at `bpe ≈ 26`; virtually every Omicron table uses UUID primary keys (`bpe ≥ 29`).

| bpe | kvBuf peak | Batches (k=12, bs=5000) | F | Total | Headroom @128 MiB |
|---|---|---|---|---|---|
| 19 | 117.9 MiB | 4.3 MiB | 10 MiB | 132.2 MiB | **−4.2 MiB** |
| 26 | 103.4 MiB | 4.7 MiB | 10 MiB | 118.1 MiB | 9.9 MiB |
| 29 | 99.3 MiB | 4.9 MiB | 10 MiB | 114.2 MiB | 13.8 MiB |
| 37 | 91.7 MiB | 5.3 MiB | 10 MiB | 107.0 MiB | 21.0 MiB |

**256 MiB without batch_size change.** The worst-case peak at default `batch_size = 50,000` is ~200 MiB, leaving 56+ MiB of headroom.

### Why both together

At 256 MiB + `batch_size = 5000`, even `bpe = 19` passes with 124 MiB to spare. The `batch_size` reduction is strictly redundant at this budget but costs nothing and provides insurance against concurrent SQL workload.
