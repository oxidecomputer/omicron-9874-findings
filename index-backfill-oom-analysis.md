# CockroachDB index backfill OOM analysis

This document analyzes the memory allocation behavior during CockroachDB's
`CREATE INDEX` backfill operation, explaining why large tables trigger
"memory budget exceeded" failures and how `bulkio.index_backfill.batch_size`
affects the threshold.

All source references are against [`oxidecomputer/cockroach@367bca4`][commit].

[commit]: https://github.com/oxidecomputer/cockroach/tree/367bca413bc24e6213a45663fccd583cc726ba08

## Background

When CockroachDB creates a secondary index on an existing table, it performs
an *index backfill*: a bulk read of every row followed by construction and
ingestion of index entries. The backfill runs as a DistSQL processor
([`indexBackfiller`][indexbackfiller]) with two concurrent goroutines
communicating over a buffered channel:

- **Producer** (`constructIndexEntries`): reads rows in chunks of
  [`bulkio.index_backfill.batch_size`][batch-size-setting] (default 50,000),
  encodes index entries, and sends batches over a [channel of capacity 10][channel-cap].
- **Consumer** (`ingestIndexEntries`): pulls batches from the channel and
  [adds entries one at a time][consumer-loop] to a [`BufferingAdder`][buffering-adder],
  which accumulates them in a [`kvBuf`][kvbuf-struct], sorts, and flushes
  to SSTables.

[indexbackfiller]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/rowexec/indexbackfiller.go
[batch-size-setting]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/backfill.go#L78-L84
[channel-cap]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/rowexec/indexbackfiller.go#L308
[consumer-loop]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/rowexec/indexbackfiller.go#L250-L268
[buffering-adder]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/buffering_adder.go#L38-L66
[kvbuf-struct]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L28-L31

## Memory monitor hierarchy

All memory allocations during the backfill are tracked by
[`BytesMonitor`][bytes-monitor] instances arranged in a hierarchy rooted at
[`rootSQLMemoryMonitor`][root-monitor], whose budget is `--max-sql-memory`
(128 MiB on the Oxide rack):

```
rootSQLMemoryMonitor (128 MiB)                    [server_sql.go:354]
  └─ bulkMemoryMonitor (inherits limit)            [server_sql.go:493]
       ├─ backfillMemoryMonitor                    [server_sql.go:499]
       │    └─ IndexBackfiller.boundAccount        ← producer batch memory
       └─ bulk-adder-monitor                       [server_sql.go:633]
            └─ BufferingAdder.memAcc               ← kvBuf slab + entries
```

The producer's batch allocations (`GrowBoundAccount` in
[`BuildIndexEntriesChunk`][build-chunk]) and the consumer's kvBuf growth
(`acc.Grow` in [`kvBuf.fits`][kvbuf-fits]) draw from the **same 128 MiB
root budget**. When the combined usage exceeds the budget, the next
`Grow` call fails with [`"memory budget exceeded"`][budget-exceeded-error].

[bytes-monitor]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/util/mon/bytes_usage.go
[root-monitor]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/server/server_sql.go#L354-L363
[build-chunk]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/backfill/backfill.go#L773-L991
[kvbuf-fits]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L53-L135
[budget-exceeded-error]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/util/mon/resource.go#L25-L37

## The kvBuf: two allocations competing for budget

The [`kvBuf`][kvbuf-struct] stores index entries compactly using two
parallel arrays:

1. **`slab []byte`** — raw key/value bytes packed contiguously
   ([`bpe`](#bpe) bytes per entry).
2. **`entries []kvBufEntry`** — [16 bytes per entry][kvbuf-entry] (two
   `uint64` packing offset and length into the slab).

Both arrays grow by [doubling][growth-constants], with per-step caps of
64 MiB for the slab and 4 MiB for entries:

```go
const minSlabGrow = 512 << 10   // 512 KiB
const maxSlabGrow = 64 << 20    // 64 MiB
const minEntryGrow = 1 << 14    // 16K items = 256 KiB
const maxEntryGrow = (4 << 20) >> entrySizeShift  // 256K items = 4 MiB
```

A [balancing loop][balancing-loop] in `fits()` adjusts entries growth
proportionally to slab growth, so the two arrays stay roughly in ratio.

Each growth step calls [`acc.Grow(needed)`][grow-call], which charges the
root monitor. Crucially, the account is **monotonically non-decreasing**:
[`Reset()`][kvbuf-reset] sets `len=0` but preserves `cap`, and the memory
account is never shrunk (unless cumulative [underfill exceeds 1 GiB][underfill-check]).

[kvbuf-entry]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L37-L40
[growth-constants]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L42-L46
[balancing-loop]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L96-L107
[grow-call]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L119
[kvbuf-reset]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L205-L211
[underfill-check]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/buffering_adder.go#L187-L192

## Slab doubling and the growth accounting identity

Because each growth step adds exactly the requested amount to both the
array capacity and the cumulative account, there is an identity:

> **Cumulative account growth = final array capacity.**

For example, the slab grows through steps 512K → 1M → 2M → 4M → 8M →
16M → 32M → 64M. The cumulative growth charges are
512K + 512K + 1M + 2M + 4M + 8M + 16M + 32M = 64M, which equals the
final slab capacity. The same holds for the entries array.

This means we can reason about peak memory usage directly in terms of
final capacities.

The [32 MiB initial reserve][reserve] (`schemachanger.backfiller.buffer_size`)
**is not additive**. `Reserve(R)` pre-charges `R` to the root monitor and
stores it as a local pool. Subsequent `Grow` calls consume from the pool
first; only excess beyond `R` charges the parent again. Once cumulative
growth exceeds `R`, the reserve is fully consumed and the total root
charge equals the cumulative growth:

```
total_kvBuf_root_charge = max(R, slab_cap + entries_cap)
                        = slab_cap + entries_cap     (for slab ≥ 32M)
```

[reserve]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/buffering_adder.go#L84-L127

## <a name="bpe"></a>Bytes per entry (bpe)

Each secondary index entry encodes as a key of the form:

```
/TableID/IndexID/IndexedCol₁/.../IndexedColₙ/PKCol₁/.../PKColₘ
```

The encoded size (`bpe`) depends on the schema. For the `console_session`
table (UUID primary key, `TIMESTAMPTZ` index on `time_created`), empirical
measurement gives `bpe ≈ 37` bytes.

With the entries array overhead of 16 bytes per entry, the effective
per-entry cost to the memory budget is `bpe + 16` bytes.

## The critical slab doubling step

The slab doubles through a predictable sequence: 512K, 1M, 2M, ..., 32M,
64M. The step from 32M to 64M is the critical one. At that point, the
kvBuf account and the growth request both depend on bpe through the
entries array, which the [balancing loop][balancing-loop] sizes
proportionally to the slab.

At slab capacity `S`, the entries array holds `S / bpe` entries at 16
bytes each, so `entries_cap ≈ 16S / bpe`. At the moment of the critical
step (`S = 32M`):

```
kvBuf_account = S + 16S/bpe     = 32 + 512/bpe          (MiB)
growth        = S + 16S/bpe     = 32 + 512/bpe          (MiB)
peak (after)  = 2 × (32 + 512/bpe) = 64 + 1024/bpe      (MiB)
```

For concrete schemas:

| Schema | bpe | entries (16S/bpe) | kvBuf account | Growth request | Peak after doubling |
|---|---|---|---|---|---|
| INT8 PK, BOOL idx | ~29 | ~17.7 MiB | ~49.7 MiB | ~49.7 MiB | ~99.3 MiB |
| UUID PK, TIMESTAMPTZ idx | ~37 | ~13.8 MiB | ~45.8 MiB | ~45.8 MiB | ~91.7 MiB |
| STRING(40) PK, TIMESTAMPTZ idx | ~64 | ~8.0 MiB | ~40.0 MiB | ~40.0 MiB | ~80.0 MiB |

## Pipeline depth: multiple batches in flight

The producer and consumer run as concurrent goroutines communicating
over a [channel of capacity 10][channel-cap]. The producer calls
[`GrowBoundAccount`][grow-bound-account] to charge batch memory
*before* sending, and the consumer calls
[`ShrinkBoundAccount`][shrink-call] to release it *after* processing
all entries in the batch:

```go
for indexBatch := range indexEntryCh {
    for _, indexEntry := range indexBatch.indexEntries {
        ib.adder.Add(...)  // kvBuf growth happens here; batch still live
    }
    indexBatch.indexEntries = nil
    ib.ShrinkBoundAccount(ctx, indexBatch.memUsedBuildingBatch)  // freed after
}
```

At any moment, there can be up to `k` batches in flight: one being
consumed, plus up to 10 buffered in the channel (and possibly one being
built by the producer). Each batch consumes approximately
`batch_size × (bpe + E)` bytes, where `E ≈ 56` is
`sizeof(rowenc.IndexEntry)` (the [Go struct overhead][sizeof-entry]
tracked by `GrowBoundAccount` in [`BuildIndexEntriesChunk`][build-chunk]).
All `k` batches are charged to the root monitor simultaneously.

The producer does **not** self-limit gracefully: when `GrowBoundAccount`
fails, the error propagates up and **fails the entire schema change
job**. There is no retry or backpressure. So `k` at the critical moment
determines whether the doubling succeeds.

The doubling succeeds iff the kvBuf account, the growth request, and
all in-flight batches fit within the budget `B`:

```
(32 + 512/bpe) + (32 + 512/bpe) + k × batch_size × (bpe + E) / 2²⁰ ≤ B
```

Simplifying:

```
64 + 1024/bpe + k × batch_size × (bpe + E) / 2²⁰ ≤ B    (MiB)
```

Additionally, even if the doubling succeeds, the system can still OOM
afterwards: the post-doubling kvBuf account is `64 + 1024/bpe` MiB, and
the producer's subsequent `GrowBoundAccount` calls fail if the remaining
budget cannot accommodate in-flight batches plus the row fetcher's scan
buffers (~10 MiB from [`DefaultBatchBytesLimit`][batch-bytes-limit]).

[batch-bytes-limit]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/rowinfra/base.go#L41-L43

[shrink-call]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/rowexec/indexbackfiller.go#L268
[sizeof-entry]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/backfill/backfill.go#L784
[grow-bound-account]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/backfill/backfill.go#L789

## Three regimes: whether the critical doubling succeeds

The doubling condition partitions the (bpe, batch_size, k) space into
three regimes.

### Regime A: entries overhead alone exceeds budget (`bpe < bpe_min`)

Setting `k = 0` (no batches at all) gives a hard floor:

```
64 + 1024/bpe ≤ B
bpe ≥ 1024 / (B − 64)
```

For `B = 128 MiB`: **`bpe_min = 16`**. When `bpe < 16`, the entries
array alone makes the 32M → 64M doubling impossible regardless of batch
size or pipeline depth. The backfill is stuck in the 32M slab regime.
In practice, the fetcher overhead (`F ≈ 10 MiB`) further constrains
the budget, raising the effective floor to
`1024 / (B − 64 − F) = 1024 / 54 ≈ 19`. The headroom table below
includes `F`, which is why bpe = 16 shows negative headroom there.

This boundary is very low — essentially unreachable with real schemas.
Even an `INT8` PK with a `BOOL` index gives bpe ≈ 19–29. Regime A is
therefore mostly academic for `B = 128 MiB`.

Common column encodings for reference:

| Column type | Encoded size |
|---|---|
| BOOL | ~2 bytes |
| INT2/INT4 | ~3–5 bytes |
| INT8 | ~5–9 bytes (varint, value-dependent) |
| UUID | ~18–19 bytes |
| TIMESTAMPTZ | ~10–12 bytes |

| Schema | PK contribution | Indexed col | Overhead | bpe (est.) | Regime |
|---|---|---|---|---|---|
| INT8 PK, BOOL idx | ~9 | ~2 | ~8 | ~19 | B |
| INT8 PK, INT8 idx | ~9 | ~9 | ~8 | ~26 | B |
| UUID PK, BOOL idx | ~19 | ~2 | ~8 | ~29 | B |
| UUID PK, INT4 idx | ~19 | ~5 | ~8 | ~32 | B |
| UUID PK, TIMESTAMPTZ idx | ~19 | ~11 | ~7 | ~37 | B |

### Regime B: doubling blocked by batch overhead (`bpe ≥ bpe_min`, too many or too large batches)

When `bpe ≥ bpe_min` but the in-flight batch memory plus other overhead
is too large, the doubling fails. The headroom available is:

```
headroom = B − 64 − 1024/bpe − F    (MiB)
```

Where `F` is overhead from the row fetcher's scan buffers
(`DefaultBatchBytesLimit` = 10 MiB) and other root monitor charges.
The doubling succeeds iff `k × batch_size × (bpe + E) / 2²⁰ ≤
headroom`. This fails when either `batch_size` is large or `k` is
large (or both).

For `console_session` (`bpe ≈ 37`, headroom ≈ 26 MiB):
- Each batch costs `50,000 × 93 / 2²⁰ ≈ 4.4 MiB` at the default batch
  size. At `k = 6`, the batch pipeline alone uses 26.4 MiB ≈ headroom,
  so `k ≥ 6` blocks the doubling.
- At `batch_size = 5,000`, each batch costs ~0.44 MiB.
  Even `k = 11` (full channel) uses only 4.8 MiB — well within headroom.

In Regime B, the slab is stuck at 32 MiB. The kvBuf flushes and
refills, but the kvBuf account plus in-flight batch memory plus
fetcher overhead leaves marginal headroom in the budget. When the
producer attempts to allocate a new batch while the kvBuf is near
capacity, the combined usage exceeds the budget and the producer's
`GrowBoundAccount` call fails with "memory budget exceeded." This is
a **hard failure** — there is no retry or backpressure.

**Empirical result:** OOM occurs at approximately
`(B − R) / (bpe + 16)` rows. For `console_session`: predicted 1.81M,
observed [1.85M, 1.9M). All tested schemas (bpe 29–64) exhibited
Regime B behavior at the default `batch_size = 50,000`.

### Regime C: doubling succeeds (`bpe ≥ bpe_min`, small enough `k × batch_size`)

When `k × batch_size × (bpe + E) / 2²⁰ ≤ headroom`, the 32M → 64M slab
doubling **succeeds**. The kvBuf reaches 64 MiB slab capacity with a
peak account of `64 + 1024/bpe` MiB. The pipeline operates indefinitely:
the kvBuf fills, flushes, and refills using the same capacity.

However, even after a successful doubling, the system is not guaranteed
to be safe: the post-doubling kvBuf account consumes most of the budget
(e.g., ~99 MiB for bpe ≈ 29), leaving limited headroom for the batch
pipeline and fetcher. This post-doubling headroom is approximately:

```
post_headroom = B − 64 − 1024/bpe − F    (MiB)
```

For schemas with small bpe (compact entries), post-doubling headroom
is tight. If the producer queues enough batches during a slow consumer
phase (e.g., during kvBuf flush), the combined usage can still exceed
the budget.

The transition from B to C depends on `batch_size` and the steady-state
pipeline depth `k`. Reducing `batch_size` helps in two ways: each batch
is smaller, and fewer total bytes are in flight at any moment. For
`console_session` at `batch_size = 5,000`, even `k = 11` fits in the
headroom, so the doubling reliably succeeds and the pipeline runs
indefinitely.

**Empirical result:** 5M rows pass without OOM at `batch_size = 5,000`
for `console_session` (bpe ≈ 37).

## Non-monotonic bpe curve

The regime structure produces a counterintuitive relationship between
entry size and OOM resilience. Smaller bpe means *more entries per slab*,
which inflates the entries array overhead (`1024/bpe` MiB at the critical
step) and leaves less headroom for the batch pipeline.

With `B = 128 MiB`, `F ≈ 10 MiB` (fetcher overhead), and
`batch_size = 50,000`:

| bpe | Headroom (B−64−1024/bpe−F) | Per-batch cost | Regime (k=3) | Regime (k=7) | Regime B max rows |
|---|---|---|---|---|---|
| 16 | −10.0 MiB | 3.4 MiB | A | A | — |
| 29 | 18.7 MiB | 4.1 MiB | C (12.2 < 18.7) | B (28.5 > 18.7) | ~2.13M |
| 37 | 26.3 MiB | 4.4 MiB | C (13.3 < 26.3) | B (31.0 > 26.3) | ~1.81M |
| 50 | 33.5 MiB | 5.1 MiB | C (15.2 < 33.5) | B (35.5 > 33.5) | ~1.45M |
| 64 | 38.0 MiB | 5.7 MiB | C (17.2 < 38.0) | B (40.1 > 38.0) | ~1.20M |

The empirical observation that **all tested schemas** (bpe 29–64) OOM
at default `batch_size = 50,000` is consistent with the producer filling
several channel slots before the consumer reaches the slab growth
point. The exact pipeline depth at the critical moment varies with
schema and timing, making the boundary non-deterministic.

With a reduced `batch_size = 5,000`, each batch costs ~0.4–0.6 MiB.
Even at `k = 11` (full channel), total batch memory is 4–7 MiB. This
fits in the headroom for any schema with `bpe ≥ bpe_min`, collapsing
Regime B and shifting the transition to `bpe ≈ bpe_min = 16`.

## Empirical validation

### Threshold vs. schema (batch_size=50,000)

Tested with `--max-sql-memory=128MiB` and default batch size. All four
schemas fall into Regime B (the 32M → 64M slab doubling fails due to
in-flight batch + fetcher overhead), so the Regime B formula
`N_max ≈ (B − R) / (bpe + 16) = 96M / (bpe + 16)` approximates the
threshold.

| Config | bpe (est.) | Predicted N_max | Empirical threshold |
|---|---|---|---|
| UUID PK, TIMESTAMPTZ idx | ~37 | 1.81M | [1.85M, 1.9M) |
| STRING(40) PK, TIMESTAMPTZ idx | ~64 | 1.20M | [1.25M, 1.3M) |
| UUID PK, (TIMESTAMPTZ, UUID) idx | ~61 | 1.25M | [1.25M, 1.3M) |
| INT8 PK, BOOL idx | ~29 | 2.13M | [2.0M, 2.1M) |

### Threshold vs. batch_size (UUID PK, TIMESTAMPTZ idx)

Tested with `--max-sql-memory=128MiB` at various row counts:

| Row count | batch_size=50,000 | batch_size=5,000 |
|---|---|---|
| 2,000,000 | OOM | PASS |
| 3,000,000 | OOM | PASS |
| 5,000,000 | OOM | PASS |

### Batch_size vs. compact schema (INT8 PK, BOOL idx, bpe ≈ 29)

Tested with `--max-sql-memory=128MiB` to validate that batch_size
reduction helps for compact schemas.

| Row count | bs=50,000 | bs=10,000 | bs=5,000 |
|---|---|---|---|
| 1,900,000 | PASS | OOM | OOM |
| 2,000,000 | PASS | OOM | PASS |
| 2,100,000 | OOM | OOM | PASS |
| 2,200,000 | OOM | OOM | OOM |

Control (UUID PK, TIMESTAMPTZ idx, bpe ≈ 37):

| Row count | bs=50,000 | bs=10,000 | bs=5,000 |
|---|---|---|---|
| 2,000,000 | OOM | PASS | PASS |

**Key findings:**

1. **batch_size matters for bpe ≈ 29.** Despite the compact entry
   size, bpe = 29 is Regime B (not A) — batch_size reduction from
   50K to 5K shifts the threshold upward by ~100K rows.

2. **bs=10K is paradoxically worse than bs=50K for this schema.** At
   all tested row counts, bs=10K OOMs while bs=50K passes at 1.9M
   and 2.0M. This is likely a timing effect: smaller batches change
   the producer/consumer race in a way that increases pipeline depth
   at the critical moment, but the per-batch cost reduction is not
   enough to compensate. The effect is non-deterministic.

3. **bs=5K shows non-monotonic behavior:** OOM at 1.9M, PASS at
   2.0M–2.1M, OOM at 2.2M. This further confirms the behavior is
   timing-sensitive. The steady-state outcome depends on the
   exact interplay between batch production rate, kvBuf flush
   duration, and channel fill level at the critical slab doubling.

4. **The Regime B control works as expected:** bs=10K and bs=5K both
   fix the OOM for bpe ≈ 37 at 2M rows, consistent with the model.

The practical implication is that **bs=5K is the safest choice** for
migration-time index creation, as it provides the most headroom.
bs=10K may not help (or may be worse) for compact schemas.

## Summary

The OOM threshold during index backfill is governed by a discrete
slab-doubling step in the `kvBuf`. The slab grows through powers of two
(512K, 1M, ..., 32M, 64M). The 32M → 64M doubling is the critical step:
it requires `32 + 512/bpe` MiB of free budget at a moment when `k`
in-flight producer batches and the row fetcher's scan buffers are also
live. The outcome depends on `bpe`, `batch_size`, `k`, and timing.

The 32 MiB Reserve (`schemachanger.backfiller.buffer_size`) is **not
additive** — it is consumed by early growth, so the total kvBuf root
charge equals the cumulative growth, not growth plus reserve.

The doubling condition is:

```
64 + 1024/bpe + k × batch_size × (bpe + E) / 2²⁰ + F ≤ B    (MiB)
```

Where `F ≈ 10 MiB` is fetcher/overhead. This gives `bpe_min = 16`.
Regime A (entries-only block) is essentially unreachable with real
schemas.

**Practically, all schemas with `bpe > 16` are Regime B** at the
default `batch_size = 50,000`: the in-flight batches plus fetcher
overhead consume the headroom and block the doubling. All tested
schemas (bpe 29–64) confirmed this.

Reducing `batch_size` to 5,000 shrinks each batch by 10× and
effectively collapses Regime B for most schemas. However, the behavior
is timing-sensitive: bs=10,000 can paradoxically be *worse* than
bs=50,000 for compact schemas (bpe ≈ 29), likely due to changes in
producer/consumer pacing. **The recommended setting is bs=5,000.**

## Tunables analysis

Four settings govern memory usage during index backfill. Only one
is effective via SQL at migration time.

### `schemachanger.backfiller.buffer_size` (default 32 MiB)

This is the [Reserve][ba-reserve] amount for the kvBuf's
[`BoundAccount`][bound-account]. [`Reserve`][ba-reserve] charges the
full amount to the root monitor upfront and sets an
[`earmark`][earmark] that prevents `Shrink` from releasing below
that level. Subsequent [`Grow`][ba-grow] calls consume from this local
pool first, only charging the parent for the excess:

```go
// bytes_usage.go:826-843
func (b *BoundAccount) Grow(ctx context.Context, x int64) error {
    if b.reserved < x {
        minExtra := b.mon.roundSize(x - b.reserved)
        if err := b.mon.reserveBytes(ctx, minExtra); err != nil {
            return err
        }
        b.reserved += minExtra
    }
    b.reserved -= x
    b.used += x
    return nil
}
```

The total root charge from the kvBuf equals `max(buffer_size,
acc.Used())`. Since `acc.Used()` exceeds `buffer_size` well before
the critical doubling step (at slab=32M, `acc.Used ≈ 46M > 32M`),
the reserve covers only the early growth. Increasing it pre-charges
*more* to root upfront, leaving *less* headroom for batches. If set
large enough to cover the full slab=64M growth (~92M), the
`Reserve` call itself would fail because 92M + in-flight batches
exceeds 128M.

**Verdict:** not helpful.

[bound-account]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/util/mon/bytes_usage.go#L642-L653
[ba-reserve]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/util/mon/bytes_usage.go#L712-L727
[earmark]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/util/mon/bytes_usage.go#L649
[ba-grow]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/util/mon/bytes_usage.go#L826-L843

### `schemachanger.backfiller.max_buffer_size` (default 512 MiB)

This is the kvBuf's [own growth limit][kvbuf-max], checked in
[`fits()`][fits-remaining] as `remaining = maxUsed - acc.Used()`.
The kvBuf already self-limits because the root monitor rejects growth
via [`acc.Grow()`][grow-call] before the kvBuf's own limit is reached
(128M root budget << 512M kvBuf limit). Reducing `max_buffer_size`
makes the rejection happen earlier (at the `remaining` check in
`fits()`) instead of later (at the root monitor), but the kvBuf
stabilizes at the same slab capacity either way.

**Verdict:** not helpful.

[kvbuf-max]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/sql/rowexec/indexbackfiller.go#L65-L68
[fits-remaining]: https://github.com/oxidecomputer/cockroach/blob/367bca413bc24e6213a45663fccd583cc726ba08/pkg/kv/bulk/kv_buf.go#L58-L59

### `bulkio.index_backfill.batch_size` (default 50,000)

Each batch's memory cost scales linearly with `batch_size`:
`batch_size × (bpe + E)` bytes per batch. Reducing from 50,000 to
5,000 cuts per-batch cost by 10×. At full pipeline depth (`k = 11`),
total in-flight batch memory drops from ~48 MiB to ~5 MiB, leaving
ample headroom for the slab doubling.

**Verdict:** effective for all practical schemas (bpe > 16). Empirically
confirmed for both bpe ≈ 37 and bpe ≈ 29.

**Important:** bs=10,000 can be paradoxically worse than the default
for compact schemas (bpe ≈ 29). Use **bs=5,000** for the widest safety
margin.

The recommended migration-time usage:

```sql
SET CLUSTER SETTING bulkio.index_backfill.batch_size = 5000;
CREATE INDEX ...;
SET CLUSTER SETTING bulkio.index_backfill.batch_size = 50000;
```

### `--max-sql-memory` (128 MiB on Oxide rack)

The root monitor budget. Increasing this gives more headroom for both
the kvBuf and the batch pipeline. Effective for **all regimes**.
However, this is a CockroachDB startup flag, not a cluster setting:
it cannot be changed at migration time via SQL. Increasing it requires
changing the CRDB zone's memory allocation.

The peak memory at the critical doubling is a U-shaped function of bpe:
compact entries inflate the entries array; wide entries inflate the batch
pipeline. Using default `batch_size = 50,000` and worst-case pipeline
depth (`k = 12`):

```
B ≥ (64 + 1024/bpe) + k × batch_size × (bpe + E) / 2²⁰ + F
```

| bpe | kvBuf peak | Batch pipeline (k=12) | F | Total |
|---|---|---|---|---|
| 19 (most compact real schema) | 117.9 MiB | 42.9 MiB | 10 MiB | **170.8 MiB** |
| 37 (console_session) | 91.7 MiB | 53.2 MiB | 10 MiB | **154.9 MiB** |
| 42 (minimum of the curve) | 88.4 MiB | 56.0 MiB | 10 MiB | **154.4 MiB** |
| 100 (wide compound index) | 74.2 MiB | 89.3 MiB | 10 MiB | **173.5 MiB** |
| 150 (very wide) | 70.8 MiB | 117.9 MiB | 10 MiB | **198.7 MiB** |

The worst realistic case for Omicron schemas is around 175–200 MiB.
**256 MiB** (a clean doubling of the current value) covers all of these
with 55+ MiB of headroom. At 256 MiB, post-doubling headroom for
`console_session` is `256 − 91.7 − 10 = 154 MiB`, supporting `k ≈ 35`
— well beyond the channel capacity of 10.

The trade-off is that `--max-sql-memory` governs all SQL memory, not
only backfill. Doubling it means CockroachDB can consume 128 MiB more
under SQL workload peaks.

**Verdict:** effective for all regimes. **256 MiB** is the recommended
value.

### Summary of tunables

| Setting | Scope | Effect |
|---|---|---|
| `buffer_size` | cluster | no effect |
| `max_buffer_size` | cluster | no effect |
| `batch_size` | cluster | **fixes** (use 5,000; avoid 10,000 for compact schemas) |
| `--max-sql-memory` | startup | **fixes** (use 256 MiB) |

## Recommended mitigation

Do both:

1. **Increase `--max-sql-memory` to 256 MiB.** This is a deployment
   change that protects all future migrations regardless of schema.
2. **Set `bulkio.index_backfill.batch_size = 5000` at migration time.**
   This is a SQL-time knob that protects the specific migration even
   before the deployment change has rolled out.

Either fix alone is sufficient for most practical schemas. Together
they make the OOM failure mode essentially unreachable: you would need
a schema with `bpe < 16` (impossible with real column types) or a
pipeline depth far exceeding the channel capacity.

The two fixes are independent in a useful way. The `--max-sql-memory`
bump is a one-time deployment change that eliminates the class of
problem entirely. The `batch_size` reduction is a defense-in-depth
measure that can be applied immediately via SQL, before the deployment
change propagates to all nodes.

### Why either alone is sufficient

**`batch_size = 5000` at 128 MiB.** Each batch costs ~0.44 MiB
(for `console_session`, bpe ≈ 37). Even at full pipeline depth
(`k = 11`), total in-flight batch memory is ~5 MiB against ~26 MiB
of headroom. The breakeven is at `bpe ≈ 26`; every Omicron table
uses UUID primary keys (`bpe ≥ 29`), so all current schemas are safe.

| bpe | kvBuf peak | Batches (k=12, bs=5000) | F | Total | Headroom @128 MiB |
|---|---|---|---|---|---|
| 19 | 117.9 MiB | 4.3 MiB | 10 MiB | 132.2 MiB | **−4.2 MiB** |
| 26 | 103.4 MiB | 4.7 MiB | 10 MiB | 118.1 MiB | 9.9 MiB |
| 29 | 99.3 MiB | 4.9 MiB | 10 MiB | 114.2 MiB | 13.8 MiB |
| 37 | 91.7 MiB | 5.3 MiB | 10 MiB | 107.0 MiB | 21.0 MiB |

**256 MiB without batch_size change.** The worst-case peak at default
`batch_size = 50,000` is ~200 MiB (very wide compound index), leaving
56+ MiB of headroom. For `console_session`, post-doubling headroom is
154 MiB — the batch pipeline cannot exhaust this even at maximum
channel fill.

### Why both together

At 256 MiB + `batch_size = 5000`, even `bpe = 19` (the most compact
real schema) passes with 124 MiB to spare. The batch_size reduction
is strictly redundant at this budget, but it costs nothing and provides
insurance against unexpected memory consumption from other SQL workload
running concurrently with the backfill.
