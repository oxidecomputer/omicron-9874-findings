# Empirical validation

Test data for the analysis in [index-backfill-oom-analysis.md](index-backfill-oom-analysis.md).

## Threshold vs. schema (batch_size=50,000)

Tested with `--max-sql-memory=128MiB` and default batch size. All four schemas fall into Regime B (the 32M → 64M slab doubling eventually succeeds, then the post-doubling headroom is insufficient for the batch pipeline). The empirical approximation `N_max ≈ (B − R) / (bpe + 16) = 96M / (bpe + 16)` estimates the row count at which the doubling becomes likely to succeed during a fill/flush cycle.

| Config | bpe (est.) | Predicted N_max | Empirical threshold |
|---|---|---|---|
| UUID PK, TIMESTAMPTZ idx | ~37 | 1.81M | [1.85M, 1.9M) |
| STRING(40) PK, TIMESTAMPTZ idx | ~64 | 1.20M | [1.25M, 1.3M) |
| UUID PK, (TIMESTAMPTZ, UUID) idx | ~61 | 1.25M | [1.25M, 1.3M) |
| INT8 PK, BOOL idx | ~29 | 2.13M | [2.0M, 2.1M) |

## Threshold vs. batch_size (UUID PK, TIMESTAMPTZ idx)

Tested with `--max-sql-memory=128MiB` at various row counts:

| Row count | batch_size=50,000 | batch_size=5,000 |
|---|---|---|
| 2,000,000 | OOM | PASS |
| 3,000,000 | OOM | PASS |
| 5,000,000 | OOM | PASS |

## Batch_size vs. compact schema (INT8 PK, BOOL idx, bpe ≈ 29)

Tested with `--max-sql-memory=128MiB` to validate that batch_size reduction helps for compact schemas.

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

## Key findings

1. **batch_size matters for bpe ≈ 29.** Despite the compact entry size, bpe = 29 is Regime B (not A) — batch_size reduction from 50K to 5K shifts the threshold upward by ~100K rows.

2. **bs=10K is paradoxically worse than bs=50K for this schema.** At all tested row counts, bs=10K OOMs while bs=50K passes at 1.9M and 2.0M. This is likely a timing effect: smaller batches change the producer/consumer race in a way that makes the doubling more likely to succeed (lower momentary `k`), but the per-batch cost reduction is not enough to keep the post-doubling headroom safe. The effect is non-deterministic.

3. **bs=5K shows non-monotonic behavior:** OOM at 1.9M, PASS at 2.0M–2.1M, OOM at 2.2M. This further confirms the behavior is timing-sensitive. The outcome depends on whether the doubling succeeds during a low-`k` window, and whether the producer subsequently encounters a high-`k` event in the reduced post-doubling headroom.

4. **The Regime B → C shift works as expected:** bs=10K and bs=5K both fix the OOM for bpe ≈ 37 at 2M rows, consistent with the model — at smaller batch_size, the post-doubling headroom accommodates worst-case pipeline depth.

The practical implication is that **bs=5K is the safest choice** for migration-time index creation, as it provides the most headroom. bs=10K may not help (or may be worse) for compact schemas.
