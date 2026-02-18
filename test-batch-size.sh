#!/usr/bin/env bash
#
# Test the effect of bulkio.index_backfill.batch_size on OOM thresholds.
#
# Uses the baseline schema (UUID PK, TIMESTAMPTZ index) and tests
# row counts above the default-batch-size threshold (~1.85M) to see
# whether a smaller batch_size allows more rows to succeed.

set -euo pipefail

INSERT_BATCH_SIZE=10000
PORT=26399
HTTP_PORT=8199
COCKROACH="${COCKROACH:-cockroach}"
MAX_SQL_MEMORY="128MiB"
SNAPSHOT_DIR="${TMPDIR:-/tmp}/crdb-threshold-snapshots"

sql() {
    $COCKROACH sql --insecure --host="localhost:$PORT" "$@"
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
    $COCKROACH start-single-node \
        --insecure \
        --store="$store_dir" \
        --listen-addr="localhost:$PORT" \
        --http-addr="localhost:$HTTP_PORT" \
        --max-sql-memory="$MAX_SQL_MEMORY" \
        --background \
        --pid-file="$store_dir/cockroach.pid" \
        2>"$store_dir/cockroach.stderr"
    wait_ready
}

stop_crdb() {
    local store_dir="$1"
    $COCKROACH quit --insecure --host="localhost:$PORT" 2>/dev/null || true
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
    prep_dir=$(mktemp -d "${TMPDIR:-/tmp}/crdb-batch-prep.XXXXXX")

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

run_test() {
    local num_rows="$1"
    local backfill_batch_size="$2"
    local snapshot_file="${SNAPSHOT_DIR}/baseline_uuid_pk_ts_idx_${num_rows}.tar"

    echo "--- testing: ${num_rows} rows, batch_size=${backfill_batch_size} ---"

    local test_dir
    test_dir=$(mktemp -d "${TMPDIR:-/tmp}/crdb-batch-test.XXXXXX")
    tar xf "$snapshot_file" -C "$test_dir"
    start_crdb "$test_dir"

    # Set the backfill batch size.
    sql -e "SET CLUSTER SETTING bulkio.index_backfill.batch_size = $backfill_batch_size;" 2>&1

    # Run CREATE INDEX.
    local output exit_code=0
    output=$(sql -e "CREATE INDEX IF NOT EXISTS test_idx ON omicron.public.test_table (time_created);" 2>&1) || exit_code=$?

    # Poll job status.
    local job_status=""
    for _ in $(seq 1 120); do
        job_status=$(sql --format=csv -e "
WITH x AS (SHOW JOBS)
SELECT status FROM x
WHERE job_type = 'SCHEMA CHANGE'
ORDER BY created DESC
LIMIT 1;
" 2>/dev/null | tail -1)
        case "$job_status" in
            succeeded|failed|canceled|"") break ;;
            *) sleep 2 ;;
        esac
    done

    local index_count
    index_count=$(sql --format=csv -e "
SELECT count(*) FROM [SHOW INDEXES FROM omicron.public.test_table]
WHERE index_name = 'test_idx';
" 2>/dev/null | tail -1)

    local job_error=""
    if [[ "$job_status" == "failed" ]]; then
        job_error=$(sql --format=csv -e "
WITH x AS (SHOW JOBS)
SELECT error FROM x
WHERE job_type = 'SCHEMA CHANGE'
ORDER BY created DESC
LIMIT 1;
" 2>/dev/null | tail -1)
    fi

    stop_crdb "$test_dir"
    rm -rf "$test_dir"

    local result
    if [[ "$index_count" != "0" && "$index_count" != "" ]]; then
        result="PASS"
    elif echo "$output $job_error" | grep -qi "memory budget exceeded\|out of memory\|OOM\|budget exceeded"; then
        result="OOM"
    elif [[ $exit_code -ne 0 ]]; then
        result="FAIL(exit=$exit_code)"
    else
        result="FAIL(unknown)"
    fi

    echo "  result: $result (index_count=$index_count, exit=$exit_code, job=$job_status)"
    echo "$num_rows $backfill_batch_size $result"
}

# -------------------------------------------------------------------
# Main.
# -------------------------------------------------------------------

echo "========================================"
echo "Batch size effect on OOM threshold"
echo "========================================"
echo ""
echo "Schema: baseline (UUID PK, TIMESTAMPTZ index)"
echo "Max SQL memory: $MAX_SQL_MEMORY"
echo ""

# Row counts to test.
ROW_COUNTS=(2000000 3000000 5000000)

# Batch sizes to test.
BATCH_SIZES=(50000 5000)

# Phase 1: prepare snapshots.
echo "=== Phase 1: preparing snapshots ==="
for rc in "${ROW_COUNTS[@]}"; do
    prepare_snapshot "$rc"
done
echo ""

# Phase 2: run tests.
echo "=== Phase 2: running tests ==="
echo ""

declare -A RESULTS
for rc in "${ROW_COUNTS[@]}"; do
    for bs in "${BATCH_SIZES[@]}"; do
        result_line=$(run_test "$rc" "$bs" | tail -1)
        RESULTS["${rc}:${bs}"]=$(echo "$result_line" | awk '{print $3}')
        echo ""
    done
done

# Summary.
echo "========================================"
echo "Summary: rows x batch_size -> result"
echo "========================================"
echo ""
printf "  %12s" "rows"
for bs in "${BATCH_SIZES[@]}"; do
    printf "  bs=%-6s" "$bs"
done
echo ""
echo "  $(printf '%0.s-' {1..40})"
for rc in "${ROW_COUNTS[@]}"; do
    printf "  %'12d" "$rc"
    for bs in "${BATCH_SIZES[@]}"; do
        result="${RESULTS["${rc}:${bs}"]:-N/A}"
        printf "  %-10s" "$result"
    done
    echo ""
done
echo ""
echo "Done."
