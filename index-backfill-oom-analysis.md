# CockroachDB index backfill OOM analysis

This document analyzes the memory allocation behavior during CockroachDB's `CREATE INDEX` backfill operation, explaining why large tables trigger "memory budget exceeded" failures and how `bulkio.index_backfill.batch_size` affects the threshold.

All source references are against [`oxidecomputer/cockroach@367bca4`][commit].

**Summary.** The backfill uses a producer-consumer architecture sharing a 128 MiB memory budget. The consumer's buffer (kvBuf) doubles its capacity through a predictable sequence; the step from 32 MiB to 64 MiB consumes most of the budget. The consumer handles memory pressure gracefully by flushing, but the producer has no such fallback: when its `GrowBoundAccount` call fails, the entire schema change job fails. After the critical doubling, in-flight batches from the producer push total usage over the budget, triggering the fatal error. Reducing `bulkio.index_backfill.batch_size` from 50,000 to 5,000 shrinks the batch pipeline 10×, providing sufficient headroom. Increasing `--max-sql-memory` to 256 MiB provides additional margin but is [not sufficient alone](#--max-sql-memory-default-128-mib-on-illumos).

## Index backfill architecture

When CockroachDB creates a secondary index on an existing table, it performs an *index backfill*: a bulk read of every row followed by construction and ingestion of index entries. The backfill runs as a [DistSQL][distsql] processor ([`indexBackfiller`][indexbackfiller]) with two concurrent goroutines communicating over a [buffered channel of capacity 10][channel-cap]:

- The **producer** (`constructIndexEntries`) reads rows in chunks of [`bulkio.index_backfill.batch_size`][batch-size-setting] (default 50,000), encodes index entries, and sends batches over the channel. Before sending each batch, it calls [`GrowBoundAccount`][grow-bound-account] to charge the batch's memory to the root monitor.
- The **consumer** (`ingestIndexEntries`) pulls batches from the channel and [adds entries one at a time][consumer-loop] to a [`BufferingAdder`][buffering-adder], which accumulates them in a [`kvBuf`][kvbuf-struct], sorts, and flushes to SSTables. After processing all entries in a batch, it calls [`ShrinkBoundAccount`][shrink-call] to release the batch's memory.

```go
for indexBatch := range indexEntryCh {
    for _, indexEntry := range indexBatch.indexEntries {
        ib.adder.Add(...)  // kvBuf growth happens here; batch still live
    }
    indexBatch.indexEntries = nil
    ib.ShrinkBoundAccount(ctx, indexBatch.memUsedBuildingBatch)  // freed after
}
```

At any moment, there can be up to `k` batches in flight:

* 1 being built by the producer
* 10 buffered in the channel
* 1 being consumed

The maximum value of `k` is 12. Each batch consumes approximately `batch_size × (bpe + E)` bytes, where [`bpe`](#bpe) is the encoded entry size and `E ≈ 56` is `sizeof(rowenc.IndexEntry)` (the [Go struct overhead][sizeof-entry] tracked by `GrowBoundAccount` in [`BuildIndexEntriesChunk`][build-chunk]). All `k` batches are charged to the root monitor.

While the channel applies backpressure on the producer, memory use does not: when `GrowBoundAccount` fails, the error propagates up and **fails the entire schema change job**. There is no retry or flush mechanism within the producer. By contrast, when the consumer's kvBuf runs out of memory, it flushes gracefully — the memory pressure triggers a flush to SSTables, and the buffer begins refilling within existing capacity. This asymmetry is the root cause of the OOM.

## Memory accounting

### Monitor hierarchy

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

### <a name="bpe"></a>Bytes per entry (bpe)

Each secondary index entry encodes as a key of the form:

```
/TableID/IndexID/IndexedCol₁/.../IndexedColₙ/PKCol₁/.../PKColₘ
```

The encoded size (`bpe`) depends on the schema. The key components are the table/index prefix (~8 bytes), the encoded indexed columns, and the PK suffix. For the `console_session` table (UUID primary key, `TIMESTAMPTZ` index on `time_created`), empirical measurement gives `bpe ≈ 37` bytes: ~8 bytes prefix + ~11 bytes `TIMESTAMPTZ` + ~19 bytes UUID.

Since virtually every Omicron table uses UUID primary keys (~19 bytes), the PK suffix alone accounts for over half of bpe. Even the smallest possible indexed column (BOOL, ~2 bytes) yields `bpe ≥ 29` (prefix + column + UUID ≈ 8 + 2 + 19).

Some schemas and their estimated bpe (we use these bpes below):

| Schema                              | PK contribution | Indexed col | Overhead | bpe (est.) |
|-------------------------------------|-----------------|-------------|----------|------------|
| INT8 PK, BOOL idx                   | ~9              | ~2          | ~8       | ~19        |
| UUID PK, BOOL idx                   | ~19             | ~2          | ~8       | ~29        |
| UUID PK, TIMESTAMPTZ idx            | ~19             | ~10         | ~8       | ~37        |
| UUID PK, (TIMESTAMPTZ, BOOL) idx    | ~19             | ~13         | ~8       | ~42        |
| UUID PK, (TIMESTAMPTZ, INT4) idx    | ~19             | ~16         | ~8       | ~50        |
| UUID PK, (TIMESTAMPTZ, STR(60)) idx | ~19             | ~73         | ~8       | ~100       |
| UUID PK, (TIMESTAMPTZ, STR(110)) idx| ~19             | ~123        | ~8       | ~150       |

With the entries array overhead of 16 bytes per entry, the effective per-entry cost to the memory budget is `bpe + 16` bytes.

## The kvBuf: growth and flush

### Two arrays competing for budget

The [`kvBuf`][kvbuf-struct] stores index entries compactly using two parallel arrays:

1. `slab []byte` contains raw key/value bytes packed contiguously ([`bpe`](#bpe) bytes per entry).
2. `entries []kvBufEntry` is [16 bytes per entry][kvbuf-entry] (two `uint64` packing offset and length into the slab).

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

Each growth step calls [`acc.Grow(needed)`][grow-call], which charges the root monitor. Crucially, the account is monotonically non-decreasing: [`Reset()`][kvbuf-reset] sets `len=0` but preserves `cap`, and the memory account is never shrunk (unless cumulative [underfill exceeds 1 GiB][underfill-check]).

### The growth accounting identity

Because each growth step charges the account by exactly the capacity added ([`acc.Grow(needed)`][grow-call] on line 119, then [`make(..., cap+slabGrow)`][slab-realloc] on line 131 and [`make(..., cap+entryGrow)`][entry-realloc] on line 126), there is an identity:

> **Cumulative account growth = final array capacity.**

* Growth is a pure doubling sequence: 512K → 1M → 2M → 4M → 8M → 16M → 32M → 64M. The cumulative charges are 512K + 512K + 1M + 2M + 4M + 8M + 16M + 32M = 64M, which equals the final slab capacity. The same holds for the entries array.
* The identity holds because each step charges `acc.Grow(needed)` by exactly the capacity added (line 119 vs lines 126/131).

This means we can reason about peak memory usage directly in terms of final capacities.

There's a [32 MiB initial reserve][reserve] (`schemachanger.backfiller.buffer_size`). `Reserve(R)` pre-charges `R` to the root monitor and stores it as a local pool. Subsequent `Grow` calls consume from the pool first; only excess beyond `R` charges the parent again. Once cumulative growth exceeds `R`, the reserve is fully consumed and the total root charge equals the cumulative growth:

```
total_kvBuf_root_charge = max(R, slab_cap + entries_cap)
                        = slab_cap + entries_cap     (for slab ≥ 32M)
```

### The flush cycle

On the consumer side, when `fits()` returns false (because `acc.Grow` fails against the root monitor), the `BufferingAdder` handles it gracefully. The [`Add` method][add-flush] calls `doFlush()`, which sorts the accumulated entries, ingests an SSTable, and calls [`curBuf.Reset()`][do-flush-reset]. As mentioned above, `Reset()` sets `len=0` but preserves capacity and does not shrink the memory account (the only post-flush `Shrink` path requires accumulated [underfill > 1 GiB][underfill-check], which is effectively unreachable here since the buffer is nearly full when it flushes).

After the flush, `fits()` is called again with the empty buffer. Since `len=0` and existing capacity is ample, it succeeds via the [fast path][kvbuf-fits] (lines 54–56): the entry fits within current capacity, so no `Grow` is needed. The entry is appended, and the buffer begins refilling within existing capacity — no new `Grow` calls until capacity is exhausted again.

When the buffer fills up again, `fits()` tries to double again. If the root budget still cannot accommodate the growth, the cycle repeats: flush, refill, flush. The system can process unlimited entries by cycling flush/fill at whatever capacity it reached.

The consumer never causes a fatal error. Running out of memory triggers a flush instead. The fatal path is in the producer's `GrowBoundAccount`, which has no such graceful fallback.

## The failure mechanism

The slab doubles through a predictable sequence: 512K, 1M, 2M, ..., 32M, 64M. The step from 32M to 64M is the critical one because its outcome determines whether the system enters a safe steady state or a danger zone. At that point, the kvBuf account and the growth request both depend on bpe through the entries array (via the [balancing loop][balancing-loop]).

At slab capacity `S`, the entries array holds `S / bpe` entries at 16 bytes each, so `entries_cap ≈ 16S / bpe`. Combined with the [growth accounting identity](#the-growth-accounting-identity), the total kvBuf charge at slab capacity `S` is:

```
kvBuf_peak(S) = S + 16S / bpe = S × (1 + 16/bpe)
```

When the 32 MiB slab fills up, `fits()` tries to double to 64 MiB. The growth request is `~(32 + 512/bpe)` MiB (slab + entries). This succeeds iff the root budget can accommodate the current kvBuf account plus the growth plus in-flight batches plus fetcher overhead. The doubling succeeds when:

```
64 + 1024/bpe + k × batch_size × (bpe + E) / 2²⁰ ≤ B    (MiB)
```

If the growth fails, the adder flushes, and the slab stays at 32 MiB. The system continues safely — at slab=32M, the remaining budget (~72 MiB for bpe=37) comfortably holds the full batch pipeline. The buffer cycles flush/fill indefinitely at 32 MiB.

If the growth succeeds, the kvBuf account jumps to the post-doubling peak, leaving far less headroom. This is where the danger begins — the producer's subsequent `GrowBoundAccount` calls may fail in this reduced headroom.

Post-doubling state for the schemas mentioned above:

| bpe | entries (16S/bpe) | Pre-doubling kvBuf | Growth request | Post-doubling kvBuf |
|-----|-------------------|--------------------|----------------|---------------------|
| 19  | ~53.9 MiB         | ~58.9 MiB          | ~58.9 MiB      | ~117.9 MiB          |
| 29  | ~35.3 MiB         | ~49.7 MiB          | ~49.7 MiB      | ~99.3 MiB           |
| 37  | ~27.7 MiB         | ~45.8 MiB          | ~45.8 MiB      | ~91.7 MiB           |
| 42  | ~24.4 MiB         | ~44.2 MiB          | ~44.2 MiB      | ~88.4 MiB           |
| 50  | ~20.5 MiB         | ~42.2 MiB          | ~42.2 MiB      | ~84.5 MiB           |
| 100 | ~10.2 MiB         | ~37.1 MiB          | ~37.1 MiB      | ~74.2 MiB           |
| 150 | ~6.8 MiB          | ~35.4 MiB          | ~35.4 MiB      | ~70.8 MiB           |

The post-doubling headroom available for batches is:

```
headroom = B − (64 + 1024/bpe) − F    (MiB)
```

where `F` is overhead from the row fetcher's scan buffers ([`DefaultBatchBytesLimit`][batch-bytes-limit] = 10 MiB) and other root monitor charges. The producer's `GrowBoundAccount` fails when in-flight batches exceed this headroom:

```
k × batch_size × (bpe + E) / 2²⁰ > headroom    (MiB)
```

The doubling itself succeeds when pipeline depth is momentarily low. The failure is therefore non-deterministic: the doubling succeeds during a transient low-`k` window, then the producer hits a transient high-`k` event in the reduced post-doubling headroom.

> [!IMPORTANT]
> The table above describes the 32M → 64M step, which is the critical doubling at `B = 128 MiB`. At higher budgets, the slab continues growing in increments of `maxSlabGrow`: 128M, then 192M, etc. Each successful step consumes more budget and reduces headroom. At `B = 256 MiB`, the 64M → 128M step succeeds easily for typical schemas, pushing the kvBuf account to ~183 MiB for bpe=37. This second critical doubling reproduces the same OOM mechanism at a higher row count. See [`--max-sql-memory`](#--max-sql-memory-default-128-mib-on-illumos) for the full analysis.

## Three regimes

The outcome of the 32M → 64M doubling attempt, combined with the post-doubling headroom, partitions the (bpe, batch_size, k) space into three regimes.

### Regime A (safe): doubling impossible

Setting `k = 0` (no batches at all) gives a hard floor for the doubling to be possible:

```
64 + 1024/bpe ≤ B
bpe ≥ 1024 / (B − 64)
```

For `B = 128 MiB`, `bpe_min = 16`. When `bpe < 16`, the post-doubling kvBuf account alone exceeds the root budget, so the growth request always fails. In practice, the fetcher overhead (`F ≈ 10 MiB`) raises the effective floor to `1024 / 54 ≈ 19`.

This means that growth always fails, the adder flushes gracefully, and the kvBuf cycles flush/fill at 32 MiB indefinitely. At slab=32M, the remaining budget is ample: `128 − (32 + 512/bpe) − F`. Even at bpe=16 (kvBuf=64M), the remaining 54 MiB easily holds the full batch pipeline (k=12 × 3.4M = 41M). The system processes unlimited rows.

This boundary is essentially unreachable with real Omicron schemas (all UUID PKs, `bpe ≥ 29`; see the [representative schema table](#bpe) above).

### Regime B (OOM): doubling succeeds, insufficient headroom

When `bpe ≥ bpe_min`, the 32M → 64M doubling can succeed when pipeline depth is momentarily low. The doubling succeeds when `k` is small enough that in-flight batches leave room for the growth request. For example, for `console_session` (bpe ≈ 37), the doubling requires `k ≤ 5`.

Once the doubling succeeds, the kvBuf account consumes most of the budget. The post-doubling headroom available for batches is:

```
headroom = B − (64 + 1024/bpe) − F    (MiB)
```

For `console_session` (bpe ≈ 37, headroom ≈ 26 MiB), each batch costs `50,000 × 93 bytes ≈ 4.4 MiB` at the default batch size. The producer's `GrowBoundAccount` fails when `k ≥ 6` (6 × 4.4 = 26.4 MiB ≈ headroom).

Note that the failure is nondeterministic. The doubling succeeds during a transient low-`k` window, then the producer hits a transient high-`k` event. More rows means more fill/flush cycles at 32 MiB before the doubling, which increases the probability that one cycle coincides with low `k` (this is why the failure appears to be deterministic in practice). Once the doubling succeeds, the system remains in the post-doubling danger zone permanently, and a high-`k` event eventually triggers the fatal `GrowBoundAccount` failure.

At the default batch size, 50,000:

| bpe | Post-doubling headroom | Per-batch cost | Regime (k=3)     | Regime (k=7)     |
|-----|------------------------|----------------|------------------|------------------|
| 19  | 0.1 MiB                | 3.6 MiB        | A                | A                |
| 29  | 18.7 MiB               | 4.1 MiB        | C (12.2 < 18.7)  | B (28.4 > 18.7)  |
| 37  | 26.3 MiB               | 4.4 MiB        | C (13.3 < 26.3)  | B (31.0 > 26.3)  |
| 42  | 29.6 MiB               | 4.7 MiB        | C (14.0 < 29.6)  | B (32.7 > 29.6)  |
| 50  | 33.5 MiB               | 5.1 MiB        | C (15.2 < 33.5)  | B (35.4 > 33.5)  |
| 100 | 43.8 MiB               | 7.4 MiB        | C (22.3 < 43.8)  | B (52.0 > 43.8)  |
| 150 | 47.2 MiB               | 9.8 MiB        | C (29.5 < 47.2)  | B (68.8 > 47.2)  |

Note that the effect of bpe on OOM risk is non-monotonic.

* A smaller bpe inflates the entries array overhead and leaves less post-doubling headroom, but also reduces per-batch cost.
* Conversely, a larger bpe increases headroom, but the per-batch cost grows faster (`batch_size × (bpe + E)`), so the critical pipeline depth decreases.

In practice, even though the behavior is timing-dependent, the observed behavior is that there are a certain number of rows above which OOMs consistently occur. See [empirical-validation.md](empirical-validation.md) for raw test data and a rough empirical model.

### Regime C (safe): doubling succeeds, sufficient headroom (`bpe ≥ bpe_min`, small batch_size)

This regime has the same doubling mechanics as Regime B: the 32M → 64M doubling eventually succeeds during a low-`k` window, and the kvBuf account jumps to `64 + 1024/bpe` MiB. The difference is that `batch_size` is small enough that even worst-case pipeline depth fits in the post-doubling headroom.

At `batch_size = 5,000`, each batch costs `5,000 × 93 bytes ≈ 0.44 MiB`. Even `k = 12` (i.e., a full pipeline) uses only 5.3 MiB — well within the ~26 MiB headroom for bpe=37. The producer's `GrowBoundAccount` always succeeds, and the system can process as many rows as come in.

Reducing `batch_size` from 50,000 to 5,000 reliably shifts schemas from Regime B to C.

## Tunables and mitigation

Four settings govern memory usage during index backfill. The first two are ineffective; the latter two form the recommended fix.

### `schemachanger.backfiller.buffer_size` (default 32 MiB)

This is the [Reserve][ba-reserve] amount for the kvBuf's [`BoundAccount`][bound-account]. `Reserve` charges the full amount to the root monitor upfront; subsequent `Grow` calls consume from this pool first, only charging the parent for the excess. Since `acc.Used()` exceeds `buffer_size` well before the critical doubling step (at slab=32M, `acc.Used ≈ 46M > 32M`), the reserve covers only early growth. Increasing it pre-charges more upfront, leaving less headroom for batches.

Altering this setting is not helpful.

### `schemachanger.backfiller.max_buffer_size` (default 512 MiB)

The kvBuf's [own growth limit][kvbuf-max]. The root monitor rejects growth via `acc.Grow()` long before this limit is reached (128M root budget << 512M kvBuf limit), so increasing it has no effect.

### `bulkio.index_backfill.batch_size` (default 50,000)

Reducing from 50,000 to 5,000 cuts per-batch cost by 10×, so the post-doubling headroom accommodates worst-case pipeline depth. This is the primary fix.

```sql
SET CLUSTER SETTING bulkio.index_backfill.batch_size = 5000;
```

At `B = 128 MiB`, the breakeven is at `bpe ≈ 26`. All practical Omicron schemas have `bpe ≥ 29` (see [bytes per entry](#bpe)). The first critical doubling (32M → 64M) still occurs, but the batch pipeline is 10× smaller, so post-doubling headroom easily accommodates worst-case `k`:

| bpe | kvBuf peak (slab=64M) | Batches (k=12, bs=5000) | F      | Total     | Headroom  |
|-----|------------------------|-------------------------|--------|-----------|-----------|
| 19  | 117.9 MiB              | 4.3 MiB                 | 10 MiB | 132.2 MiB | −4.2 MiB  |
| 29  | 99.3 MiB               | 4.9 MiB                 | 10 MiB | 114.2 MiB | 13.8 MiB  |
| 37  | 91.7 MiB               | 5.3 MiB                 | 10 MiB | 107.0 MiB | 21.0 MiB  |
| 42  | 88.4 MiB               | 5.6 MiB                 | 10 MiB | 104.0 MiB | 24.0 MiB  |
| 50  | 84.5 MiB               | 6.1 MiB                 | 10 MiB | 100.6 MiB | 27.4 MiB  |
| 100 | 74.2 MiB               | 8.9 MiB                 | 10 MiB | 93.1 MiB  | 34.9 MiB  |
| 150 | 70.8 MiB               | 11.8 MiB                | 10 MiB | 92.6 MiB  | 35.4 MiB  |

At `B = 128 MiB`, the second doubling (64M → 128M) cannot succeed: the post-doubling kvBuf alone (~183 MiB for bpe=37) exceeds the budget. The slab stays at 64M and the system cycles flush/fill safely.

This is effective for all practical schemas. We choose a batch size of 5,000, since in testing we found that 10,000 can be worse for compact schemas.

### `--max-sql-memory` (default 128 MiB on illumos)

The root monitor budget. This is a process startup flag, not a cluster setting.

> [!NOTE]
> On Linux, `--max-sql-memory` defaults to 25% of system memory. The path to determine system memory is stubbed out on illumos, so CockroachDB falls back to a 128 MiB default.

Peak memory at the first critical doubling (32M → 64M) is U-shaped in bpe: compact entries inflate the entries array, while wide entries inflate the batch pipeline. At default `batch_size = 50,000` and worst-case `k = 12`:

| bpe | kvBuf peak (slab=64M) | Batch pipeline (k=12) | F      | Total         |
|-----|------------------------|----------------------|--------|---------------|
| 19  | 117.9 MiB              | 42.9 MiB             | 10 MiB | 170.8 MiB     |
| 29  | 99.3 MiB               | 48.6 MiB             | 10 MiB | 157.9 MiB     |
| 37  | 91.7 MiB               | 53.2 MiB             | 10 MiB | 154.9 MiB     |
| 42  | 88.4 MiB               | 56.0 MiB             | 10 MiB | 154.4 MiB     |
| 50  | 84.5 MiB               | 60.7 MiB             | 10 MiB | 155.2 MiB     |
| 100 | 74.2 MiB               | 89.3 MiB             | 10 MiB | 173.5 MiB     |
| 150 | 70.8 MiB               | 117.9 MiB            | 10 MiB | 198.7 MiB     |

At first glance, 256 MiB appears to be sufficient. However, this table only accounts for the first doubling. At `B = 256`, the slab has room to double again to 128MiB. The growth request for this step is ~92 MiB (64M slab + ~28M entries for bpe=37), and the pre-doubling kvBuf is ~92 MiB. For the step to succeed, the total must fit in the budget:

```
91.7 + 91.7 + k × 4.4 + F ≤ 256    →    k ≤ 14
```

This succeeds easily even at worst-case `k = 12`. After the second doubling, peak memory is substantially higher:

| bpe | kvBuf peak (slab=128M) | Batch pipeline (k=12) | F      | Total         | Headroom @256 MiB |
|-----|------------------------|-----------------------|--------|---------------|--------------------|
| 29  | 198.6 MiB              | 48.6 MiB              | 10 MiB | 257.2 MiB     | −1.2 MiB           |
| 37  | 183.4 MiB              | 53.2 MiB              | 10 MiB | 246.6 MiB     | 9.4 MiB            |
| 42  | 176.8 MiB              | 56.0 MiB              | 10 MiB | 242.8 MiB     | 13.2 MiB           |
| 50  | 169.0 MiB              | 60.7 MiB              | 10 MiB | 239.7 MiB     | 16.3 MiB           |
| 100 | 148.5 MiB              | 89.3 MiB              | 10 MiB | 247.8 MiB     | 8.2 MiB            |
| 150 | 141.7 MiB              | 117.9 MiB             | 10 MiB | 269.6 MiB     | −13.6 MiB          |

It turns out that headroom of <15 MiB is not survivable in practice. (This is the same issue as with the first doubling under a 128MiB limit: more rows mean more flush/fill cycles, and a higher probability that the second doubling succeeds during a low-`k` window. The post-second-doubling headroom is too tight for worst-case pipeline depth.)

### Recommendation

Do both:

1. Set `bulkio.index_backfill.batch_size = 5000`.
2. Increase `--max-sql-memory` to 256 MiB.

Reducing the batch size is the primary fix: it is sufficient alone for all practical Omicron schemas at any memory budget ≥ 128 MiB. Increasing `--max-sql-memory` alone is not sufficient for tables with many rows — it shifts the OOM threshold higher but enables a second slab doubling that recreates the same danger at a larger scale. But together, both fixes provide ample headroom.

At 256 MiB with `batch_size = 5000`, even after the second doubling (slab=128M), the batch pipeline is only ~5.3 MiB instead of ~53 MiB:

| bpe | kvBuf peak (slab=128M) | Batches (k=12, bs=5000) | F      | Total     | Headroom  |
|-----|------------------------|-------------------------|--------|-----------|-----------|
| 19  | 235.8 MiB              | 4.3 MiB                 | 10 MiB | 250.1 MiB | 5.9 MiB   |
| 29  | 198.6 MiB              | 4.9 MiB                 | 10 MiB | 213.5 MiB | 42.5 MiB  |
| 37  | 183.4 MiB              | 5.3 MiB                 | 10 MiB | 198.7 MiB | 57.3 MiB  |
| 42  | 176.8 MiB              | 5.6 MiB                 | 10 MiB | 192.4 MiB | 63.6 MiB  |
| 50  | 169.0 MiB              | 6.1 MiB                 | 10 MiB | 185.1 MiB | 70.9 MiB  |
| 100 | 148.5 MiB              | 8.9 MiB                 | 10 MiB | 167.4 MiB | 88.6 MiB  |
| 150 | 141.7 MiB              | 11.8 MiB                | 10 MiB | 163.5 MiB | 92.5 MiB  |

For all practical Omicron schemas (`bpe ≥ 29`), headroom is 42+ MiB. The `bpe = 19` case remains tight, but no Omicron table has a schema that compact.

Index creation time scales linearly with row count, and batch size plays only a minor role; `bs=5000` adds ~3–4% overhead versus `bs=50000` for tables where both succeed (see `bench-index-creation.sh`).

<!-- Link reference definitions -->

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
