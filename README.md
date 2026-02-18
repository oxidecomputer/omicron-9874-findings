# omicron#9874 findings

Investigation into CockroachDB index backfill OOM failures during schema migrations on the Oxide rack (`--max-sql-memory=128MiB`). See:

- [omicron#9866 Index creation is asynchronous and can lead to missing indexes](https://github.com/oxidecomputer/omicron/issues/9866)
- [omicron#9874 creating crdb indexes can fail on sufficiently large tables](https://github.com/oxidecomputer/omicron/issues/9874).

Note that this is currently a **draft**. I (Rain) still need to verify a number of details.

## tl;dr

Do both:

* Run CockroachDB nodes with `--max-sql-memory=256MiB`.
* Run `SET CLUSTER SETTING bulkio.index_backfill.batch_size = 5000;`.

Either one of these should be sufficient for most use cases. Both of them together all but eliminate this issue with a substantial cushion.

## Files

- **[index-backfill-oom-analysis.md](index-backfill-oom-analysis.md)** — detailed analysis of the kvBuf slab-doubling mechanism, memory monitor hierarchy, and the three failure regimes.
- **[empirical-validation.md](empirical-validation.md)** — raw test data validating the OOM model across schema configurations and batch sizes.
- **[repro-race.sh](repro-race.sh)** — reproduces the `CREATE INDEX IF NOT EXISTS` concurrency bug where OOM during backfill causes the index to silently not be created.
- **[test-thresholds.sh](test-thresholds.sh)** — empirical validation of OOM thresholds across four schema configurations at default batch size.
- **[test-batch-size.sh](test-batch-size.sh)** — measures the effect of reducing `bulkio.index_backfill.batch_size` on the baseline schema.

## Running the scripts

The scripts need a `cockroach` binary on `$PATH`. The easiest way is to run from the omicron checkout, where direnv provides one:

```bash
cd ~/dev/oxide/omicron
../omicron-9874-findings/test-thresholds.sh
```

You can also point `COCKROACH` at a specific binary:

```bash
COCKROACH=/path/to/cockroach ./test-thresholds.sh
```

Snapshots are cached in `$TMPDIR/crdb-threshold-snapshots` (defaulting to `/tmp`) so re-runs skip the (slow) data-insertion phase.
