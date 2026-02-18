#!/usr/bin/env bash
#
# Reproduction script for the CREATE INDEX IF NOT EXISTS race.
#
# The production bug: 3 Nexus instances concurrently ran
# CREATE INDEX IF NOT EXISTS on the same table. One started the
# backfill (which blocks), the others saw the in-progress index
# descriptor and IF NOT EXISTS returned immediately. Then the
# backfill failed with OOM ("memory budget exceeded"), the index
# descriptor was rolled back, and nobody ever created the index.
#
# Phase 1 (prepare): starts CockroachDB, creates a table with
# 5 million rows (no lookup_console_by_creation index), tars store.
#
# Phase 2 (iterate): untars snapshot, starts CockroachDB with
# constrained --max-sql-memory, launches N concurrent clients each
# running CREATE INDEX IF NOT EXISTS, checks the result.
#
# Environment variables:
#   MAX_SQL_MEMORY       — CockroachDB --max-sql-memory (default: 128MiB)
#   BACKFILL_BATCH_SIZE  — bulkio.index_backfill.batch_size cluster setting
#                          (default: unset, uses CockroachDB default)

set -euo pipefail

NUM_ROWS=2000000
BATCH_SIZE=10000
PORT=26399
HTTP_PORT=8199
COCKROACH="${COCKROACH:-cockroach}"
SNAPSHOT="${TMPDIR:-/tmp}/crdb-repro-snapshot-${NUM_ROWS}.tar"

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
        --max-sql-memory="${MAX_SQL_MEMORY:-128MiB}" \
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

# A single "nexus" client that runs CREATE INDEX IF NOT EXISTS.
run_nexus() {
    set +e
    local nexus_id="$1"

    echo "[nexus$nexus_id] CREATE INDEX + verify in single transaction"
    sql -e "
BEGIN;
CREATE INDEX IF NOT EXISTS lookup_console_by_creation ON omicron.public.console_session (time_created);
COMMIT;
BEGIN;
select cast(if((select true where exists (select index_name from omicron.crdb_internal.table_indexes where descriptor_name = 'console_session' AND index_name = 'lookup_console_by_creation')), 'true', 'kaboom') as bool);
COMMIT;
" 2>&1 | sed "s/^/[nexus$nexus_id] /"
    local exit_code=$?
    echo "[nexus$nexus_id] done (exit=$exit_code)"

   if [[ $exit_code -ne 0 ]]; then
       echo "[nexus$nexus_id] retrying..."
       sleep 1
       run_nexus "$nexus_id"
   fi
}

# ---------------------------------------------------------------
# Phase 1: prepare the snapshot (only if it doesn't exist yet)
# ---------------------------------------------------------------
if [[ ! -f "$SNAPSHOT" ]]; then
    PREP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/crdb-repro-prep.XXXXXX")
    trap 'stop_crdb "$PREP_DIR"; rm -rf "$PREP_DIR"' EXIT

    echo "=== phase 1: preparing snapshot ==="
    echo "--- starting cockroachdb (store=$PREP_DIR) ---"
    start_crdb "$PREP_DIR"

    echo "--- creating database and table (without lookup_console_by_creation) ---"
    sql -e "
CREATE DATABASE IF NOT EXISTS omicron;

CREATE TABLE IF NOT EXISTS omicron.public.console_session (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token STRING(40) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_last_used TIMESTAMPTZ NOT NULL,
    silo_user_id UUID NOT NULL
);
"

    echo "--- inserting $NUM_ROWS rows (batch size $BATCH_SIZE) ---"
    inserted=0
    while (( inserted < NUM_ROWS )); do
        remaining=$(( NUM_ROWS - inserted ))
        batch=$(( remaining < BATCH_SIZE ? remaining : BATCH_SIZE ))
        sql -e "
INSERT INTO omicron.public.console_session (token, time_created, time_last_used, silo_user_id)
SELECT
    substr(gen_random_uuid()::STRING, 1, 40),
    now() - (random() * interval '30 days'),
    now() - (random() * interval '1 day'),
    gen_random_uuid()
FROM generate_series(1, $batch);
" 2>/dev/null
        inserted=$(( inserted + batch ))
        if (( inserted % 100000 == 0 )); then
            echo "  inserted $inserted / $NUM_ROWS rows"
        fi
    done

    echo "--- verifying row count ---"
    sql -e "SELECT count(*) AS row_count FROM omicron.public.console_session;"

    echo "--- stopping cockroachdb and creating snapshot ---"
    stop_crdb "$PREP_DIR"

    rm -f "$PREP_DIR/cockroach.pid" "$PREP_DIR/cockroach.stderr"
    tar cf "$SNAPSHOT" -C "$PREP_DIR" .
    rm -rf "$PREP_DIR"
    trap - EXIT

    echo "=== snapshot saved to $SNAPSHOT ==="
    echo ""
fi

# ---------------------------------------------------------------
# Phase 2: iterate — untar, concurrent CREATE INDEX, check result
# ---------------------------------------------------------------
ITERATIONS=${1:-10}
NUM_CLIENTS=${2:-3}
echo "=== phase 2: running $ITERATIONS iterations with $NUM_CLIENTS concurrent clients ==="
echo "=== --max-sql-memory=${MAX_SQL_MEMORY:-128MiB} backfill_batch_size=${BACKFILL_BATCH_SIZE:-default} ==="

for i in $(seq 1 "$ITERATIONS"); do
    ITER_DIR=$(mktemp -d "${TMPDIR:-/tmp}/crdb-repro-iter.XXXXXX")

    tar xf "$SNAPSHOT" -C "$ITER_DIR"
    start_crdb "$ITER_DIR"

    # Optionally constrain the index backfill batch size to make OOM
    # more likely with low --max-sql-memory.
    if [[ -n "${BACKFILL_BATCH_SIZE:-}" ]]; then
        echo "--- iteration $i: setting bulkio.index_backfill.batch_size=$BACKFILL_BATCH_SIZE ---"
        sql -e "SET CLUSTER SETTING bulkio.index_backfill.batch_size = $BACKFILL_BATCH_SIZE;" 2>&1
    fi

    # Launch N concurrent clients, each running CREATE INDEX IF NOT EXISTS.
    pids=()
    for c in $(seq 1 "$NUM_CLIENTS"); do
        run_nexus "$c" &
        pids+=($!)
    done

    # Wait for all clients to finish.
    all_ok=true
    for pid in "${pids[@]}"; do
        if ! wait "$pid"; then
            all_ok=false
        fi
    done

    # Give schema change jobs time to complete (or fail).
    sleep 5

    # Check for failed schema change jobs.
    echo "--- iteration $i: schema change jobs ---"
    sql -e "WITH x AS (SHOW JOBS) SELECT job_id, job_type, status, description FROM x WHERE job_type = 'SCHEMA CHANGE' ORDER BY created;" 2>/dev/null

    # Check whether the index was created.
    echo "--- iteration $i: indexes ---"
    sql -e "SHOW INDEXES FROM omicron.public.console_session;" 2>/dev/null

    RESULT=$(sql --format=csv -e "
SELECT count(*) FROM [SHOW INDEXES FROM omicron.public.console_session]
WHERE index_name = 'lookup_console_by_creation';
" 2>/dev/null | tail -1)

    stop_crdb "$ITER_DIR"
    rm -rf "$ITER_DIR"

    if [[ "$RESULT" == "0" ]]; then
        echo "iteration $i: *** BUG REPRODUCED — index MISSING ***"
        exit 1
    else
        echo "iteration $i: ok (index present, clients_ok=$all_ok)"
    fi
done

echo ""
echo "=== $ITERATIONS iterations completed, race did not reproduce ==="
exit 0
