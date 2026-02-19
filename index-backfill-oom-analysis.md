# CockroachDB index backfill OOM analysis

This document analyzes memory allocation during CockroachDB's `CREATE INDEX` backfill operation, explaining why large tables trigger "memory budget exceeded" failures and how `bulkio.index_backfill.batch_size` affects the threshold.

All source references are against [`oxidecomputer/cockroach@367bca4`][commit].

## Summary

Index backfills use a producer-consumer architecture sharing a 128 MiB memory budget.

* The consumer handles memory pressure gracefully by flushing its internal buffers.
* The producer has no such fallback: when its memory growth request fails, the entire schema change job fails.

Once the consumer's buffer claims enough of the budget, in-flight batches from the producer push total usage over the limit, triggering the fatal error.

Reducing `bulkio.index_backfill.batch_size` from 50,000 to 5,000 shrinks the batch pipeline 10x, providing enough headroom to avoid failure. Increasing `--max-sql-memory` to 256 MiB provides additional headroom (though it is not sufficient alone).

## Index backfill architecture

When CockroachDB creates a secondary index on an existing table, it *backfills* the index by bulk-reading every row and constructing index entries. The backfill runs as a [DistSQL][distsql] processor ([`indexBackfiller`][indexbackfiller]) with two concurrent goroutines communicating over a [buffered channel of capacity 10][channel-cap]:

- The **producer** (`constructIndexEntries`) reads rows in chunks of [`bulkio.index_backfill.batch_size`][batch-size-setting] (default 50,000), encodes index entries, and sends batches over the channel. Before sending, it calls [`GrowBoundAccount`][grow-bound-account] to charge the batch's memory to the root monitor.
- The **consumer** (`ingestIndexEntries`) pulls batches and [adds entries one at a time][consumer-loop] to a [`BufferingAdder`][buffering-adder], which accumulates them in a [`kvBuf`][kvbuf-struct], sorts, and flushes to SSTables. After each batch, it calls [`ShrinkBoundAccount`][shrink-call] to release the batch's memory.

```go
for indexBatch := range indexEntryCh {
    for _, indexEntry := range indexBatch.indexEntries {
        ib.adder.Add(...)  // kvBuf growth happens here; batch still live
    }
    indexBatch.indexEntries = nil
    ib.ShrinkBoundAccount(ctx, indexBatch.memUsedBuildingBatch)  // freed after
}
```

In this analysis, we describe the number of batches in flight at any moment as `k`: 1 being built by the producer, up to 10 buffered in the channel, and 1 being consumed. So the maximum possible `k` is 12. We use this value of 12 as part of our analysis below.

* Each batch consumes approximately `batch_size * (bpe + E)` bytes, where [`bpe`](#bpe) is the encoded entry size and `E ~ 56` is `sizeof(rowenc.IndexEntry)` (the [Go struct overhead][sizeof-entry] tracked by `GrowBoundAccount` in [`BuildIndexEntriesChunk`][build-chunk]).
* All `k` batches are charged to the root monitor.

The channel's capacity applies backpressure on the producer, but running out of memory does not: when `GrowBoundAccount` fails, the error propagates up and fails the entire schema change job. On the other hand, the consumer _does_ gracefully handle out of memory by flushing. The root cause of the OOM is this asymmetry.

## Memory accounting

### Monitor hierarchy

Memory allocations are tracked by [`BytesMonitor`][bytes-monitor] instances in a hierarchy rooted at [`rootSQLMemoryMonitor`][root-monitor], whose budget is `--max-sql-memory` (128 MiB on illumos):

```
rootSQLMemoryMonitor (128 MiB)                    [server_sql.go:354]
  +-- bulkMemoryMonitor (inherits limit)            [server_sql.go:493]
       +-- backfillMemoryMonitor                    [server_sql.go:499]
       |    +-- IndexBackfiller.boundAccount        <- producer batch memory
       +-- bulk-adder-monitor                       [server_sql.go:633]
            +-- BufferingAdder.memAcc               <- kvBuf slab + entries
```

Both draw from the same root budget; the next `Grow` call fails with ["memory budget exceeded"][budget-exceeded-error] when combined usage exceeds it.

### <a name="bpe"></a>Bytes per entry (bpe)

Each secondary index entry encodes as a key of the form:

```
/TableID/IndexID/IndexedCol_1/.../IndexedCol_n/PKCol_1/.../PKCol_m
```

The encoded size (`bpe`) depends on the schema: table/index prefix (~8 bytes), encoded indexed columns, and PK suffix. For `console_session` (UUID primary key, `TIMESTAMPTZ` index on `time_created`), `bpe ~ 37` bytes: ~8 bytes prefix + ~11 bytes `TIMESTAMPTZ` + ~19 bytes UUID.

All Omicron tables use UUID primary keys (~19 bytes), so even the smallest indexed column (BOOL, ~2 bytes) yields `bpe >= 29`.

Some schemas and their estimated bpe (we use these as representative examples below):

| Schema                              | PK contribution | Indexed col | Overhead | bpe (est.) |
|-------------------------------------|-----------------|-------------|----------|------------|
| UUID PK, BOOL idx                   | ~19             | ~2          | ~8       | ~29        |
| UUID PK, TIMESTAMPTZ idx            | ~19             | ~10         | ~8       | ~37        |
| UUID PK, (TIMESTAMPTZ, BOOL) idx    | ~19             | ~13         | ~8       | ~42        |
| UUID PK, (TIMESTAMPTZ, INT4) idx    | ~19             | ~16         | ~8       | ~50        |
| UUID PK, (TIMESTAMPTZ, STR(60)) idx | ~19             | ~73         | ~8       | ~100       |
| UUID PK, (TIMESTAMPTZ, STR(110)) idx| ~19             | ~123        | ~8       | ~150       |

## The consumer kvBuf

Let's first look at the consumer. On this side, a [`kvBuf`][kvbuf-struct] stores index entries compactly using two parallel arrays:

1. `slab []byte` contains raw key/value bytes packed contiguously ([`bpe`](#bpe) bytes per entry).
2. `entries []kvBufEntry` is [16 bytes per entry][kvbuf-entry] (two `uint64` packing offset and length into the slab).

So the effective per-entry cost is `bpe + 16` bytes.

Both arrays grow by doubling per step, clamped to `[minSlabGrow, maxSlabGrow]` and `[minEntryGrow, maxEntryGrow]` respectively. The [growth constants][growth-constants] are:

```go
const minSlabGrow = 512 << 10   // 512 KiB
const maxSlabGrow = 64 << 20    // 64 MiB
const minEntryGrow = 1 << 14    // 16K items = 256 KiB
const maxEntryGrow = (4 << 20) >> entrySizeShift  // 256K items = 4 MiB
```

A [balancing loop][balancing-loop] in `fits()` adjusts entries growth proportionally to slab growth.

> [!NOTE]
> A [reduction loop][slab-budget-reduction] decrements the allocation when the gap to `maxBufferLimit` is tight. For index backfills this loop is inert: `maxBufferLimit` is 512 MiB, far exceeding any growth request. Growth against the root budget is mediated solely by `acc.Grow()`.

Each growth step calls [`acc.Grow(needed)`][grow-call], claiming memory from the root monitor. The `kvBuf` capacity never goes down: [`Reset()`][kvbuf-reset] zeroes lengths but preserves capacity, and the account is never shrunk (unless cumulative [underfill exceeds 1 GiB][underfill-check]). So the final capacity is the same as the total growth over the course of the backfill.

(As is typical for systems of this nature, the capacity is doubled each time: 512K, 1M, 2M, 4M, 8M, 16M, 32M, 64M. The entries array is similar. Since there's effectively no shrinking, the final capacity is the same as the total cumulative growth.)

There's a [32 MiB reserve][reserve] (`schemachanger.backfiller.buffer_size`) that starts off as claimed. `Grow` calls draw from this pool first. Once cumulative growth exceeds the reserve, the `kvBuf` starts claiming more memory from the root monitor. This leads to:

```
total_kvBuf_root_charge = max(R, slab_cap + entries_cap)
                        = slab_cap + entries_cap     (for slab >= 32M)
```

### The flush cycle

When `Grow` fails, the [`Add` method][add-flush] calls `doFlush()`. This method sorts entries, ingests an SSTable, and calls [`Reset()`][do-flush-reset] on the buffer. As mentioned above, `Reset()` zeroes lengths, but it preserves capacity and does not shrink the account (the [underfill > 1 GiB][underfill-check] threshold is effectively unreachable).

After flushing, `fits()` succeeds via the [fast path][kvbuf-fits] (lines 54-56): the entry always fits within existing capacity. The buffer refills without new allocations until capacity is exhausted again.

What this means is that if there isn't space left over in `--max-sql-memory`, the consumer continues to perform flushes and fills indefinitely at the current capacity. So the consumer doesn't call the failure. The fatal path is exclusively in the producer's `GrowBoundAccount`.

### When does growth become problematic?

For `--max-sql-memory=128MiB`, the 32M -> 64M doubling step is the crucial one. If that succeeds, the system enters a danger zone where too many in-flight batches (too high a `k`) can lead to an OOM.

At slab capacity `S`, the entries array holds `S / bpe` entries at 16 bytes each, so `entries_cap ~ 16S / bpe`. So the total (and therefore peak) kvBuf allocation at slab capacity `S` is:

```
kvBuf_peak(S) = S + 16S / bpe = S * (1 + 16/bpe)
```

When the 32 MiB slab fills, `fits()` tries to double to 64 MiB. The growth request is `~(32 + 512/bpe)` MiB (slab + entries). This succeeds if (`B` is `--max-sql-memory`):

```
64 + 1024/bpe + k * batch_size * (bpe + E) / 2^20 <= B    (MiB)
```

In case this succeeds, the `kvBuf` consumes most of the budget, and subsequent allocations from the producer may fail in the reduced headroom.

The post-doubling state for the schemas mentioned above:

| bpe | entries (16S/bpe) | Pre-doubling kvBuf | Growth request | Post-doubling kvBuf |
|-----|-------------------|--------------------|----------------|---------------------|
| 29  | ~35.3 MiB         | ~49.7 MiB          | ~49.7 MiB      | ~99.3 MiB           |
| 37  | ~27.7 MiB         | ~45.8 MiB          | ~45.8 MiB      | ~91.7 MiB           |
| 42  | ~24.4 MiB         | ~44.2 MiB          | ~44.2 MiB      | ~88.4 MiB           |
| 50  | ~20.5 MiB         | ~42.2 MiB          | ~42.2 MiB      | ~84.5 MiB           |
| 100 | ~10.2 MiB         | ~37.1 MiB          | ~37.1 MiB      | ~74.2 MiB           |
| 150 | ~6.8 MiB          | ~35.4 MiB          | ~35.4 MiB      | ~70.8 MiB           |

The post-doubling headroom for the producer is:

```
headroom = B - (64 + 1024/bpe) - F    (MiB)
```

where `F` is miscellaneous overhead from the row fetcher's scan buffers ([`DefaultBatchBytesLimit`][batch-bytes-limit] = 10 MiB) and other root monitor charges.

> [!IMPORTANT]
> The table above describes the 32M -> 64M step, which is the problematic doubling for `B = 128 MiB`. At higher budgets, the slab can potentially continue growing. Each successful step consumes more budget and reduces headroom. At `B = 256 MiB`, the 64M -> 128M step normally succeeds, pushing the kvBuf account to around 183 MiB for bpe=37. This means that the second critical doubling reproduces the same OOM mechanism at a higher row count. See [`--max-sql-memory`](#--max-sql-memory-default-128-mib-on-illumos) for the full analysis.

## The producer

Now let's look at the producer. When it starts working on a batch, it calls `GrowBoundAccount`. Based on the headroom formula above, this process fails when:

```
k * batch_size * (bpe + E) / 2^20 > headroom    (MiB)
```

The outcome of the crucial doubling attempt, combined with the post-doubling headroom, partitions the (bpe, batch_size, k) space into three regimes.

### Regime A (safe): doubling impossible

Setting `k = 0` gives a hard floor for when doubling is allowed.

```
64 + 1024/bpe <= B
bpe >= 1024 / (B - 64)
```

* For `B = 128 MiB`, the smallest value of `bpe`, `bpe_min`, is 16.
* For `B = 256 MiB`, `bpe_min` is 5.33.

When `bpe < bpe_min`, the post-doubling `kvBuf` alone exceeds the root budget, so `kvBuf` growth always fails, and the consumer enters the flush/fill loop described above. The remaining budget easily holds the full batch pipeline.

For Omicron, this is mostly academic, since primary keys almost always include at least one UUID (which results in a `bpe` of at least 29).

### Regime B (OOM): doubling succeeds, insufficient headroom

When `bpe >= bpe_min`, the doubling can succeed when pipeline depth is momentarily low. As mentioned above, the post-doubling headroom for batches is:

```
headroom = B - (64 + 1024/bpe) - F    (MiB)
```

With the default batch size of 50,000, for `console_session` (bpe ~ 37, headroom ~ 26 MiB), each batch costs `50,000 * 93 bytes ~ 4.4 MiB`. So the producer fails when `k >= 6` (6 * 4.4 = 26.4 MiB). Since `k` can go up to 12, a high-`k` event eventually triggers the fatal `GrowBoundAccount` failure.

At the default batch size of 50,000, the *critical `k`* for each schema is the maximum pipeline depth that fits in post-doubling headroom (`floor(headroom / per_batch_cost)`). The producer fails when `k` is larger than this value.

| bpe | Post-doubling headroom | Per-batch cost | Critical k |
|-----|------------------------|----------------|------------|
| 29  | 18.7 MiB               | 4.1 MiB        | 4          |
| 37  | 26.3 MiB               | 4.4 MiB        | 5          |
| 42  | 29.6 MiB               | 4.7 MiB        | 6          |
| 50  | 33.5 MiB               | 5.1 MiB        | 6          |
| 100 | 43.8 MiB               | 7.4 MiB        | 5          |
| 150 | 47.2 MiB               | 9.8 MiB        | 4          |

Since the maximum possible `k` is 12, all practical Omicron schemas are in regime B: for a sufficiently large number of rows, the pipeline depth will eventually exceed the critical `k`, triggering the OOM.

It's worth noting that OOM risk is non-monotonic in bpe: smaller bpe inflates the entries array (less headroom) but reduces per-batch cost, while larger bpe increases headroom but the per-batch cost grows faster.

Even though the behavior described above is nondeterministic, in practice there's a row count above which OOMs consistently occur. See [empirical-validation.md](empirical-validation.md) for some data and a rough empirical model.

### Regime C (safe): doubling succeeds, sufficient headroom (`bpe >= bpe_min`, small batch_size)

This regime has the same doubling mechanics as Regime B, but the `batch_size` is small enough that even with the worst case `k = 12`, the total data fits in the post-doubling headroom.

For a bpe of 37, with a batch size of 5,000, each batch costs `5,000 * 93 bytes ~ 0.44 MiB`. Even `k = 12` uses only 5.3 MiB, which is well within the 26-ish MiB headroom available.

In other words, for the entire possible space of inputs, reducing the batch size from 50,000 to 5,000 reliably shifts backfill operations from regime B to C.

## Tunables and mitigation

Four settings govern memory usage during index backfill.

### `schemachanger.backfiller.buffer_size` (default 32 MiB)

The [Reserve][ba-reserve] amount for the kvBuf's [`BoundAccount`][bound-account]. Since `acc.Used()` exceeds `buffer_size` well before the critical doubling (at slab=32M, `acc.Used ~ 46M > 32M`), the reserve covers only early growth. Increasing it pre-charges more upfront, leaving less headroom for batches. So tweaking this tunable is not helpful.

### `schemachanger.backfiller.max_buffer_size` (default 512 MiB)

This is the kvBuf's [own growth limit][kvbuf-max]. The root monitor rejects growth via `acc.Grow()` long before this limit is reached (128M root budget << 512M kvBuf limit), so increasing it has no effect. One option that would probably work is to reduce this to a small amount like 32M, but that is a pretty large change with possibly unexpected effects.

### `bulkio.index_backfill.batch_size` (default 50,000)

As discussed above, reducing the batch size from 50,000 to 5,000 cuts per-batch cost by 10x, so the post-doubling headroom comfortably accommodates worst-case pipeline depth.

```sql
SET CLUSTER SETTING bulkio.index_backfill.batch_size = 5000;
```

(As the `SET CLUSTER SETTING` suggests, this is a cluster-wide persistent setting, and it applies to all nodes and all future backfills. So it only needs to be set once.)

At `B = 128 MiB`, assuming that the slab's growth to 64M is successful, this results in:

| bpe | kvBuf peak (slab=64M)  | Batches (k=12, bs=5000) | F      | Total     | Remaining  |
|-----|------------------------|-------------------------|--------|-----------|-----------|
| 29  | 99.3 MiB               | 4.9 MiB                 | 10 MiB | 114.2 MiB | 13.8 MiB  |
| 37  | 91.7 MiB               | 5.3 MiB                 | 10 MiB | 107.0 MiB | 21.0 MiB  |
| 42  | 88.4 MiB               | 5.6 MiB                 | 10 MiB | 104.0 MiB | 24.0 MiB  |
| 50  | 84.5 MiB               | 6.1 MiB                 | 10 MiB | 100.6 MiB | 27.4 MiB  |
| 100 | 74.2 MiB               | 8.9 MiB                 | 10 MiB | 93.1 MiB  | 34.9 MiB  |
| 150 | 70.8 MiB               | 11.8 MiB                | 10 MiB | 92.6 MiB  | 35.4 MiB  |

So this is an effective fix for all practical schemas. We choose 5,000 over 10,000 because testing showed 10,000 can still trigger failure with sufficiently compact schemas.

### `--max-sql-memory` (default 128 MiB on illumos)

This is the root monitor budget `B`. Note that this is a process startup flag, not a cluster setting.

> [!NOTE]
> On Linux, `--max-sql-memory` defaults to 25% of system memory. The path to determine system memory is stubbed out on illumos, so CockroachDB falls back to a 128 MiB default.

What happens if we increase this to, say, 256 MiB, while keeping the batch size at 50,000? As observed above, peak memory at the 32M -> 64M slab transition is U-shaped in bpe, since compact entries inflate the entries array while wide entries inflate the batch pipeline. At a 50,000 batch size, with the worst-case `k = 12`:

| bpe | kvBuf peak (slab=64M) | Batch pipeline (k=12) | F      | Total         |
|-----|------------------------|----------------------|--------|---------------|
| 29  | 99.3 MiB               | 48.6 MiB             | 10 MiB | 157.9 MiB     |
| 37  | 91.7 MiB               | 53.2 MiB             | 10 MiB | 154.9 MiB     |
| 42  | 88.4 MiB               | 56.0 MiB             | 10 MiB | 154.4 MiB     |
| 50  | 84.5 MiB               | 60.7 MiB             | 10 MiB | 155.2 MiB     |
| 100 | 74.2 MiB               | 89.3 MiB             | 10 MiB | 173.5 MiB     |
| 150 | 70.8 MiB               | 117.9 MiB            | 10 MiB | 198.7 MiB     |

But! If `--max-sql-memory` is 256 MiB, the slab has room to double again to 128 MiB. For a bpe of 37, the pre-doubling size and the growth request are both ~91.7 MiB (64M slab + ~28M entries), which easily fits into a maximum of 256 MiB.

After the second doubling, peak memory is now substantially higher with the worst case `k = 12`:

| bpe | kvBuf peak (slab=128M) | Batch pipeline (k=12) | F      | Total         | Remaining |
|-----|------------------------|-----------------------|--------|---------------|-----------|
| 29  | 198.6 MiB              | 48.6 MiB              | 10 MiB | 257.2 MiB     | -1.2 MiB  |
| 37  | 183.4 MiB              | 53.2 MiB              | 10 MiB | 246.6 MiB     | 9.4 MiB   |
| 42  | 176.8 MiB              | 56.0 MiB              | 10 MiB | 242.8 MiB     | 13.2 MiB  |
| 50  | 169.0 MiB              | 60.7 MiB              | 10 MiB | 239.7 MiB     | 16.3 MiB  |
| 100 | 148.5 MiB              | 89.3 MiB              | 10 MiB | 247.8 MiB     | 8.2 MiB   |
| 150 | 141.7 MiB              | 117.9 MiB             | 10 MiB | 269.6 MiB     | -13.6 MiB |

In practice, any remaining amount below 15 MiB can lead to OOMs. As with the 128 MiB limit, more rows increase the probability that the second doubling succeeds during a low-`k` window, then cause errors during a high-`k` window.

### Recommendation

Based on the analysis above, the recommendation is to do both:

1. Set the batch size to 5000.
2. Increase `--max-sql-memory` to 256 MiB.

Reducing the batch size is the primary fix: it is sufficient alone for all practical Omicron schemas. Increasing `--max-sql-memory` alone is not sufficient: it shifts the OOM threshold higher but enables a second doubling that recreates the same failure. But together, both fixes provide ample headroom.

Let's consider the bpe 37 situation, with a `--max-sql-memory` of 256 MiB, and with a batch size of 5000. In this case, even after the second doubling (slab=128M), the batch pipeline is only ~5.3 MiB instead of ~53 MiB.

For all our chosen representative schemas:

| bpe | kvBuf peak (slab=128M) | Batches (k=12, bs=5000) | F      | Total     | Remaining  |
|-----|------------------------|-------------------------|--------|-----------|-----------|
| 29  | 198.6 MiB              | 4.9 MiB                 | 10 MiB | 213.5 MiB | 42.5 MiB  |
| 37  | 183.4 MiB              | 5.3 MiB                 | 10 MiB | 198.7 MiB | 57.3 MiB  |
| 42  | 176.8 MiB              | 5.6 MiB                 | 10 MiB | 192.4 MiB | 63.6 MiB  |
| 50  | 169.0 MiB              | 6.1 MiB                 | 10 MiB | 185.1 MiB | 70.9 MiB  |
| 100 | 148.5 MiB              | 8.9 MiB                 | 10 MiB | 167.4 MiB | 88.6 MiB  |
| 150 | 141.7 MiB              | 11.8 MiB                | 10 MiB | 163.5 MiB | 92.5 MiB  |

So for all practical Omicron schemas, we have at least 42 MiB left over. More compact cases remain a bit tight, but no Omicron table has a schema that compact.

#### Performance implications

One would reasonably expect smaller batch sizes to lead to a loss in performance. That is indeed the case, but the loss is quite small (somewhere between 2-7%). This is acceptable for a one-time index backfill. See [the raw data](empirical-validation.md#index-creation-performance-across-bpe) for more.

[commit]: https://github.com/oxidecomputer/cockroach/tree/367bca413bc24e6213a45663fccd583cc726ba08
[distsql]: https://www.cockroachlabs.com/docs/stable/architecture/sql-layer#distsql
[indexbackfiller]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/rowexec/indexbackfiller.go
[batch-size-setting]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/backfill.go#L78-L84
[channel-cap]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/rowexec/indexbackfiller.go#L308
[consumer-loop]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/rowexec/indexbackfiller.go#L250-L268
[buffering-adder]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/buffering_adder.go#L38-L66
[kvbuf-struct]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L28-L31
[grow-bound-account]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/backfill/backfill.go#L789
[shrink-call]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/rowexec/indexbackfiller.go#L268
[sizeof-entry]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/backfill/backfill.go#L784
[build-chunk]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/backfill/backfill.go#L773-L991
[bytes-monitor]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/util/mon/bytes_usage.go
[root-monitor]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/server/server_sql.go#L354-L363
[kvbuf-fits]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L53-L135
[budget-exceeded-error]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/util/mon/resource.go#L25-L37
[kvbuf-entry]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L37-L40
[growth-constants]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L42-L46
[balancing-loop]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L96-L107
[slab-budget-reduction]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L75-L77
[grow-call]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L119
[kvbuf-reset]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L205-L211
[underfill-check]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/buffering_adder.go#L187-L192
[slab-realloc]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L131
[entry-realloc]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L126
[reserve]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/buffering_adder.go#L84-L127
[add-flush]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/buffering_adder.go#L167-L208
[do-flush-reset]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/buffering_adder.go#L331
[batch-bytes-limit]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/rowinfra/base.go#L41-L43
[bound-account]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/util/mon/bytes_usage.go#L642-L653
[ba-reserve]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/util/mon/bytes_usage.go#L712-L727
[kvbuf-max]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/rowexec/indexbackfiller.go#L65-L68
