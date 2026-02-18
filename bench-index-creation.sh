#!/usr/bin/env bash
#
# Benchmark index creation time: batch size 50000 vs 5000.
#
# Compares CREATE INDEX wall-clock time for
# bulkio.index_backfill.batch_size = 50000 (default) vs 5000, using
# the baseline schema (UUID PK, TIMESTAMPTZ index) at several row
# counts. Uses hyperfine for statistical measurement.
#
# Two-phase approach:
#   Phase 1: prepare CockroachDB snapshots with pre-inserted rows
#            (cached on disk, shared with other test scripts).
#   Phase 2: for each row count, use hyperfine to compare CREATE
#            INDEX time at each batch size.
#
# Note: at --max-sql-memory=128MiB, bs=50000 hits OOM around ~1.85M
# rows. Runs that OOM will show as failures in hyperfine output.
# Override MAX_SQL_MEMORY to raise the budget (e.g. 256MiB) if you
# want all runs to succeed.

set -euo pipefail

# -------------------------------------------------------------------
# Constants.
# -------------------------------------------------------------------
INSERT_BATCH_SIZE=10000
PORT=26399
HTTP_PORT=8199
# Export variables that must survive across hyperfine -> sh -> script
# re-invocations.
export COCKROACH="${COCKROACH:-cockroach}"
export MAX_SQL_MEMORY="${MAX_SQL_MEMORY:-128MiB}"
export BENCH_DIR="${TMPDIR:-/tmp}/crdb-bench-run"
export BENCH_LOG="${TMPDIR:-/tmp}/crdb-bench.log"
SNAPSHOT_DIR="${TMPDIR:-/tmp}/crdb-threshold-snapshots"
BENCH_RUNS="${BENCH_RUNS:-3}"

# -------------------------------------------------------------------
# Helpers.
# -------------------------------------------------------------------

sql() {
    "$COCKROACH" sql --insecure --host="localhost:$PORT" "$@"
}

wait_ready() {
    for _ in $(seq 1 30); do
        if sql -e "SELECT 1" &>/dev/null; then
            return 0
        fi
        sleep 1
    done
    echo "ERROR: cockroachdb did not become ready" >&2
    return 1
}

start_crdb() {
    local store_dir="$1"
    # Redirect both stdout and stderr to files so the backgrounded
    # cockroach process doesn't hold open the caller's pipe fds.
    # Without this, hyperfine's prepare command can hang or the
    # daemon can die from SIGPIPE when the pipe is closed.
    "$COCKROACH" start-single-node \
        --insecure \
        --store="$store_dir" \
        --listen-addr="localhost:$PORT" \
        --http-addr="localhost:$HTTP_PORT" \
        --max-sql-memory="$MAX_SQL_MEMORY" \
        --background \
        --pid-file="$store_dir/cockroach.pid" \
        >"$store_dir/cockroach.stdout" \
        2>"$store_dir/cockroach.stderr"
    wait_ready
}

stop_crdb() {
    local store_dir="$1"
    "$COCKROACH" quit --insecure --host="localhost:$PORT" 2>/dev/null || true
    local pid
    pid=$(cat "$store_dir/cockroach.pid" 2>/dev/null) || return 0
    while kill -0 "$pid" 2>/dev/null; do
        sleep 0.1
    done
}

prepare_snapshot() {
    local num_rows="$1"
    local snapshot_file="${SNAPSHOT_DIR}/baseline_uuid_pk_ts_idx_${num_rows}.tar"

    if [[ -f "$snapshot_file" ]]; then
        echo "  snapshot exists: $snapshot_file"
        return 0
    fi

    echo "  preparing snapshot with $num_rows rows ..."
    local prep_dir
    prep_dir=$(mktemp -d "${TMPDIR:-/tmp}/crdb-bench-prep.XXXXXX")

    start_crdb "$prep_dir"
    sql -e "CREATE DATABASE IF NOT EXISTS omicron;"
    sql -e "
CREATE TABLE IF NOT EXISTS omicron.public.test_table (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token STRING(40) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_last_used TIMESTAMPTZ NOT NULL,
    silo_user_id UUID NOT NULL
);"

    local inserted=0
    while (( inserted < num_rows )); do
        local remaining=$(( num_rows - inserted ))
        local batch=$(( remaining < INSERT_BATCH_SIZE ? remaining : INSERT_BATCH_SIZE ))
        sql -e "
INSERT INTO omicron.public.test_table (token, time_created, time_last_used, silo_user_id)
SELECT
    substr(gen_random_uuid()::STRING, 1, 40),
    now() - (random() * interval '30 days'),
    now() - (random() * interval '1 day'),
    gen_random_uuid()
FROM generate_series(1, $batch);" 2>/dev/null
        inserted=$(( inserted + batch ))
        if (( inserted % 100000 == 0 )); then
            echo "    inserted $inserted / $num_rows"
        fi
    done

    sql -e "SELECT count(*) AS row_count FROM omicron.public.test_table;"
    stop_crdb "$prep_dir"
    rm -f "$prep_dir/cockroach.pid" "$prep_dir/cockroach.stderr"
    mkdir -p "$SNAPSHOT_DIR"
    tar cf "$snapshot_file" -C "$prep_dir" .
    rm -rf "$prep_dir"
    echo "  snapshot saved: $snapshot_file"
}

# -------------------------------------------------------------------
# Subcommands invoked by hyperfine.
#
# hyperfine re-executes this script with an internal subcommand so
# that all helper functions are available in the subprocess.
#
#   __prepare <snapshot_file>   — untar snapshot, start CRDB.
#   __run <batch_size>          — set batch size, CREATE INDEX, poll.
#   __cleanup                   — stop CRDB, remove temp dir.
# -------------------------------------------------------------------

cmd_prepare() {
    local snapshot_file="$1"
    # Defensively kill any leftover cockroach holding our ports.
    "$COCKROACH" quit --insecure --host="localhost:$PORT" 2>/dev/null || true
    sleep 0.5
    rm -rf "$BENCH_DIR"
    mkdir -p "$BENCH_DIR"
    tar xf "$snapshot_file" -C "$BENCH_DIR"
    start_crdb "$BENCH_DIR"
}

cmd_run() {
    local batch_size="$1"
    sql -e "SET CLUSTER SETTING bulkio.index_backfill.batch_size = $batch_size;"

    local exit_code=0
    sql -e "CREATE INDEX IF NOT EXISTS test_idx ON omicron.public.test_table (time_created);" || exit_code=$?

    # Poll until the schema change job reaches a terminal state.
    # If CRDB crashed (OOM), the sql call fails and we break out.
    local polls=0
    while (( polls < 120 )); do
        local status
        if ! status=$(sql --format=csv -e "
WITH x AS (SHOW JOBS)
SELECT status FROM x
WHERE job_type = 'SCHEMA CHANGE'
ORDER BY created DESC
LIMIT 1;
" 2>/dev/null | tail -1); then
            # CRDB is unreachable (likely crashed from OOM).
            exit 1
        fi
        case "$status" in
            succeeded) exit 0 ;;
            failed|canceled) exit 1 ;;
            "") break ;;
            *) sleep 2 ;;
        esac
        (( polls++ )) || true
    done

    exit "$exit_code"
}

cmd_cleanup() {
    stop_crdb "$BENCH_DIR"
    rm -rf "$BENCH_DIR"
}

# -------------------------------------------------------------------
# Dispatch internal subcommands.
# -------------------------------------------------------------------

case "${1:-}" in
    __prepare)
        if cmd_prepare "$2" >> "$BENCH_LOG" 2>&1; then
            echo "prepare: ok" >> "$BENCH_LOG"
        else
            rc=$?
            echo "prepare: FAILED (exit=$rc)" >> "$BENCH_LOG"
            cat "$BENCH_DIR/cockroach.stderr" >> "$BENCH_LOG" 2>/dev/null || true
            exit "$rc"
        fi
        exit 0
        ;;
    __run)
        cmd_run "$2"
        ;;
    __cleanup)
        cmd_cleanup >> "$BENCH_LOG" 2>&1
        echo "cleanup: ok" >> "$BENCH_LOG"
        exit 0
        ;;
esac

# -------------------------------------------------------------------
# Main.
# -------------------------------------------------------------------

SCRIPT_PATH="$(realpath "$0")"
ROW_COUNTS=(1000000 2000000 3000000 5000000)
BATCH_SIZES=(50000 5000)

echo "========================================"
echo "Index creation time: batch size comparison"
echo "========================================"
echo ""
echo "Schema:          baseline (UUID PK, TIMESTAMPTZ index)"
echo "Max SQL memory:  $MAX_SQL_MEMORY"
echo "Batch sizes:     ${BATCH_SIZES[*]}"
echo "Row counts:      ${ROW_COUNTS[*]}"
echo "Runs per bench:  $BENCH_RUNS"
echo "Debug log:       $BENCH_LOG"
echo ""
: > "$BENCH_LOG"

# Phase 1: prepare snapshots.
echo "=== Phase 1: preparing snapshots ==="
for rc in "${ROW_COUNTS[@]}"; do
    prepare_snapshot "$rc"
done
echo ""

# Phase 2: benchmark with hyperfine.
echo "=== Phase 2: benchmarking with hyperfine ==="
echo ""

for rc in "${ROW_COUNTS[@]}"; do
    snapshot_file="${SNAPSHOT_DIR}/baseline_uuid_pk_ts_idx_${rc}.tar"
    printf -v formatted_rc "%'d" "$rc"
    echo "--- $formatted_rc rows ---"
    echo ""

    hyperfine_args=(
        --runs "$BENCH_RUNS"
        --prepare "$SCRIPT_PATH __prepare $snapshot_file"
        --cleanup "$SCRIPT_PATH __cleanup"
        --ignore-failure
    )

    for bs in "${BATCH_SIZES[@]}"; do
        hyperfine_args+=(
            -n "bs=$bs"
            "$SCRIPT_PATH __run $bs"
        )
    done

    hyperfine "${hyperfine_args[@]}"
    echo ""
done

echo "Done."
