# Empirical validation

Test data for the analysis in [index-backfill-oom-analysis.md](index-backfill-oom-analysis.md).

## OOM threshold validation (`--max-sql-memory=128MiB`)

Tested with `test-thresholds.sh` (schema sweep) and `test-batch-size.sh` (batch size sweep) at `--max-sql-memory=128MiB`. All schemas fall into Regime B at the default `batch_size = 50,000`. The empirical approximation `N_max ≈ (B − R) / (bpe + 16) = 96M / (bpe + 16)` estimates the row count at which the 32M → 64M slab doubling becomes likely to succeed during a fill/flush cycle.

### N_max vs. schema (bs=50,000)

| Config                            | bpe (est.) | Predicted N_max | Empirical threshold |
|-----------------------------------|------------|-----------------|---------------------|
| UUID PK, TIMESTAMPTZ idx          | ~37        | 1.81M           | [1.85M, 1.9M)      |
| STRING(40) PK, TIMESTAMPTZ idx    | ~64        | 1.20M           | [1.25M, 1.3M)      |
| UUID PK, (TIMESTAMPTZ, UUID) idx  | ~61        | 1.25M           | [1.25M, 1.3M)      |
| INT8 PK, BOOL idx                 | ~29        | 2.13M           | [2.0M, 2.1M)       |

### bs=5,000 vs. bs=50,000 (UUID PK, TIMESTAMPTZ idx)

| Row count | bs=50,000 | bs=5,000 |
|-----------|-----------|----------|
| 2,000,000 | OOM       | PASS     |
| 3,000,000 | OOM       | PASS     |
| 5,000,000 | OOM       | PASS     |

Reducing `batch_size` to 5,000 eliminates OOM at all tested row counts, confirming the Regime B → C shift.

## Index creation performance across bpe

Benchmarked with `bench-index-creation.sh` at `--max-sql-memory=256MiB`, 3 runs per configuration. Times are mean ± σ. **X** indicates OOM ("memory budget exceeded"). The ratio column shows the overhead of `bs=5,000` relative to `bs=50,000` where both succeed.

### baseline (UUID PK, TIMESTAMPTZ idx, bpe ≈ 37)

| Row count | bs=50,000      | bs=5,000       | Ratio |
|-----------|----------------|----------------|-------|
| 1,000,000 | 2.01 ± 0.01s   | 2.09 ± 0.01s   | 1.04× |
| 2,000,000 | 3.98 ± 0.04s   | 4.16 ± 0.01s   | 1.04× |
| 3,000,000 | 5.76 ± 0.10s   | 6.01 ± 0.16s   | 1.04× |
| 5,000,000 | X              | 9.74 ± 0.03s   | —     |

OOM threshold for `bs=50,000` is between 3M and 5M rows. At 256 MiB, the first critical doubling (32M → 64M) is survivable; the OOM is caused by the second doubling (64M → 128M), which pushes kvBuf to ~183 MiB and leaves <10 MiB of headroom for worst-case pipeline depth (see the [`--max-sql-memory` analysis](index-backfill-oom-analysis.md#--max-sql-memory-default-128-mib-on-illumos)).

### wide (UUID PK, (TIMESTAMPTZ, STRING(60)) idx, bpe ≈ 100)

| Row count | bs=50,000      | bs=5,000       | Ratio |
|-----------|----------------|----------------|-------|
| 1,000,000 | 2.64 ± 0.04s   | 2.83 ± 0.00s   | 1.07× |
| 2,000,000 | X              | 5.44 ± 0.23s   | —     |
| 3,000,000 | X              | 8.02 ± 0.06s   | —     |
| 5,000,000 | X              | 14.26 ± 0.15s  | —     |

OOM threshold for `bs=50,000` is between 1M and 2M rows. The second doubling (64M → 128M) succeeds easily at this bpe (the growth request fits at any realistic `k`), and the post-doubling headroom is only ~8 MiB — tighter than baseline because wider entries inflate the batch pipeline faster than they shrink the entries array.

### very_wide (UUID PK, (TIMESTAMPTZ, STRING(110)) idx, bpe ≈ 150)

| Row count | bs=50,000      | bs=5,000           | Ratio |
|-----------|----------------|---------------------|-------|
| 1,000,000 | X              | 3.23 ± 0.05s       | —     |
| 2,000,000 | X              | 6.20 ± 0.02s       | —     |
| 3,000,000 | X              | 9.08 ± 0.07s       | —     |
| 5,000,000 | X              | 15.66 ± 0.19s       | —     |

OOM threshold for `bs=50,000` is below 1M rows. At bpe ≈ 150, the second doubling succeeds when `k ≤ 10`, and the post-doubling headroom (~104 MiB) is insufficient for worst-case pipeline depth (`k = 12` × 9.8 MiB ≈ 118 MiB). `bs=50,000` fails at every tested row count.

### Observations

1. **OOM threshold decreases with bpe as predicted.** baseline (bpe ≈ 37) survives to 3M rows, wide (bpe ≈ 100) fails at 2M, very_wide (bpe ≈ 150) fails at 1M. This confirms the analysis: wider entries increase per-batch cost (`batch_size × (bpe + E)`) faster than they reduce kvBuf overhead (`16S/bpe`).

2. **`bs=5,000` eliminates OOM for all tested configurations.** Every `bs=5,000` run succeeded, including very_wide at 3M rows (where `bs=50,000` OOMs in under 1.4s).

3. **Performance overhead of `bs=5,000` is small.** For configurations where both batch sizes succeed: ~4% for baseline, ~7% for wide. Index creation time scales linearly with row count regardless of batch size.
