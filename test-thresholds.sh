#!/usr/bin/env bash
#
# Empirical validation of index backfill OOM thresholds.
#
# Theory: with --max-sql-memory=128MiB, the kvBuf in CockroachDB's
# BufferingAdder has two allocations competing for budget:
#
#   1. slab []byte   — raw key/value data, bpe bytes per entry
#   2. entries []kvBufEntry — 16 bytes per entry (offset/length metadata)
#
# The initial reserve is 32 MiB (MinBufferSize).  Due to doubling
# growth, cumulative account charges equal final allocation sizes.
# The threshold at which OOM occurs is:
#
#   threshold_rows ≈ (budget - reserve) / (bpe + 16)
#                  = 96 MiB / (bpe + 16)
#
# Where bpe is the encoded size of each secondary index key (table/
# index prefix + indexed columns + PK suffix).
#
# This script tests 4 schema configurations at row counts straddling
# the predicted threshold to find where the OOM boundary lies.
#
#   Config 1: baseline (UUID PK, TIMESTAMPTZ index)
#             bpe ~37 bytes → threshold ~1.81M rows
#
#   Config 2: larger PK (STRING(40) PK, TIMESTAMPTZ index)
#             bpe ~50 bytes → threshold ~1.45M rows
#
#   Config 3: compound index (UUID PK, TIMESTAMPTZ+UUID index)
#             bpe ~53 bytes → threshold ~1.39M rows
#
#   Config 4: small index (INT8 PK, BOOL index, padding columns)
#             bpe ~21 bytes → threshold ~2.59M rows
#
# For each (config, row_count) pair the script:
#   1. Prepares a snapshot with the required rows (cached on disk).
#   2. Untars the snapshot, starts CRDB with 128MiB memory budget.
#   3. Runs a single CREATE INDEX and checks pass/fail.
#   4. Records the result.
#
# At the end, a summary matrix is printed.

set -euo pipefail

# -------------------------------------------------------------------
# Constants.
# -------------------------------------------------------------------
BATCH_SIZE=10000
PORT=26399
HTTP_PORT=8199
COCKROACH="${COCKROACH:-cockroach}"
MAX_SQL_MEMORY="128MiB"
SNAPSHOT_DIR="${TMPDIR:-/tmp}/crdb-threshold-snapshots"

# -------------------------------------------------------------------
# Helpers (same pattern as repro-race.sh).
# -------------------------------------------------------------------

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

# -------------------------------------------------------------------
# Schema configurations.
#
# Each config is defined by a set of shell variables:
#   CONFIG_NAMES[i]        — human-readable label
#   CONFIG_SCHEMAS[i]      — CREATE TABLE DDL
#   CONFIG_INSERT_SQLS[i]  — INSERT template (BATCH_PLACEHOLDER is
#                            replaced with the batch size)
#   CONFIG_INDEX_SQLS[i]   — CREATE INDEX DDL to test
#   CONFIG_INDEX_NAMES[i]  — index name for verification
#   CONFIG_ROW_COUNTS[i]   — space-separated row counts to test
# -------------------------------------------------------------------

CONFIG_NAMES=()
CONFIG_SCHEMAS=()
CONFIG_INSERT_SQLS=()
CONFIG_INDEX_SQLS=()
CONFIG_INDEX_NAMES=()
CONFIG_ROW_COUNTS=()

# Config 1: baseline (original console_session schema).
CONFIG_NAMES+=("baseline_uuid_pk_ts_idx")
CONFIG_SCHEMAS+=("
CREATE TABLE IF NOT EXISTS omicron.public.test_table (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token STRING(40) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_last_used TIMESTAMPTZ NOT NULL,
    silo_user_id UUID NOT NULL
);
")
CONFIG_INSERT_SQLS+=("
INSERT INTO omicron.public.test_table (token, time_created, time_last_used, silo_user_id)
SELECT
    substr(gen_random_uuid()::STRING, 1, 40),
    now() - (random() * interval '30 days'),
    now() - (random() * interval '1 day'),
    gen_random_uuid()
FROM generate_series(1, BATCH_PLACEHOLDER);
")
CONFIG_INDEX_SQLS+=("CREATE INDEX IF NOT EXISTS test_idx ON omicron.public.test_table (time_created);")
CONFIG_INDEX_NAMES+=("test_idx")
CONFIG_ROW_COUNTS+=("1800000 1850000 1900000")

# Config 2: larger PK (STRING(40) primary key instead of UUID).
CONFIG_NAMES+=("string40_pk_ts_idx")
CONFIG_SCHEMAS+=("
CREATE TABLE IF NOT EXISTS omicron.public.test_table (
    token STRING(40) PRIMARY KEY,
    time_created TIMESTAMPTZ NOT NULL,
    time_last_used TIMESTAMPTZ NOT NULL,
    silo_user_id UUID NOT NULL
);
")
CONFIG_INSERT_SQLS+=("
INSERT INTO omicron.public.test_table (token, time_created, time_last_used, silo_user_id)
SELECT
    substr(gen_random_uuid()::STRING, 1, 40),
    now() - (random() * interval '30 days'),
    now() - (random() * interval '1 day'),
    gen_random_uuid()
FROM generate_series(1, BATCH_PLACEHOLDER);
")
CONFIG_INDEX_SQLS+=("CREATE INDEX IF NOT EXISTS test_idx ON omicron.public.test_table (time_created);")
CONFIG_INDEX_NAMES+=("test_idx")
CONFIG_ROW_COUNTS+=("1100000 1150000 1200000 1250000")

# Config 3: compound index (UUID PK, two-column index).
CONFIG_NAMES+=("uuid_pk_compound_idx")
CONFIG_SCHEMAS+=("
CREATE TABLE IF NOT EXISTS omicron.public.test_table (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token STRING(40) NOT NULL,
    time_created TIMESTAMPTZ NOT NULL,
    time_last_used TIMESTAMPTZ NOT NULL,
    silo_user_id UUID NOT NULL
);
")
CONFIG_INSERT_SQLS+=("
INSERT INTO omicron.public.test_table (token, time_created, time_last_used, silo_user_id)
SELECT
    substr(gen_random_uuid()::STRING, 1, 40),
    now() - (random() * interval '30 days'),
    now() - (random() * interval '1 day'),
    gen_random_uuid()
FROM generate_series(1, BATCH_PLACEHOLDER);
")
CONFIG_INDEX_SQLS+=("CREATE INDEX IF NOT EXISTS test_idx ON omicron.public.test_table (time_created, silo_user_id);")
CONFIG_INDEX_NAMES+=("test_idx")
CONFIG_ROW_COUNTS+=("1200000 1250000 1300000")

# Config 4: small index (INT8 PK, BOOL index, padding columns for
# similar source-table row width).
CONFIG_NAMES+=("int8_pk_bool_idx")
CONFIG_SCHEMAS+=("
CREATE TABLE IF NOT EXISTS omicron.public.test_table (
    id INT8 PRIMARY KEY DEFAULT unique_rowid(),
    is_active BOOL NOT NULL,
    padding1 TIMESTAMPTZ NOT NULL,
    padding2 UUID NOT NULL
);
")
CONFIG_INSERT_SQLS+=("
INSERT INTO omicron.public.test_table (is_active, padding1, padding2)
SELECT
    (random() > 0.5),
    now() - (random() * interval '30 days'),
    gen_random_uuid()
FROM generate_series(1, BATCH_PLACEHOLDER);
")
CONFIG_INDEX_SQLS+=("CREATE INDEX IF NOT EXISTS test_idx ON omicron.public.test_table (is_active);")
CONFIG_INDEX_NAMES+=("test_idx")
CONFIG_ROW_COUNTS+=("2000000 2100000 2150000 2200000")

NUM_CONFIGS=${#CONFIG_NAMES[@]}

# -------------------------------------------------------------------
# Result tracking.
#
# Results are stored in a flat associative array keyed by
# "config_index:row_count".
# -------------------------------------------------------------------

declare -A RESULTS

# -------------------------------------------------------------------
# Snapshot preparation.
#
# For each (config, row_count) pair, build a snapshot containing the
# table with the requested number of rows and NO index.  Snapshots
# are cached in SNAPSHOT_DIR so re-runs skip preparation.
# -------------------------------------------------------------------

prepare_snapshot() {
    local config_idx="$1"
    local num_rows="$2"
    local config_name="${CONFIG_NAMES[$config_idx]}"
    local snapshot_file="${SNAPSHOT_DIR}/${config_name}_${num_rows}.tar"

    if [[ -f "$snapshot_file" ]]; then
        echo "  snapshot already exists: $snapshot_file"
        return 0
    fi

    echo "  preparing snapshot: $config_name with $num_rows rows ..."

    local prep_dir
    prep_dir=$(mktemp -d "${TMPDIR:-/tmp}/crdb-threshold-prep.XXXXXX")

    start_crdb "$prep_dir"

    # Create the database and table.
    sql -e "CREATE DATABASE IF NOT EXISTS omicron;"
    sql -e "${CONFIG_SCHEMAS[$config_idx]}"

    # Insert rows in batches.
    local inserted=0
    local insert_template="${CONFIG_INSERT_SQLS[$config_idx]}"
    while (( inserted < num_rows )); do
        local remaining=$(( num_rows - inserted ))
        local batch=$(( remaining < BATCH_SIZE ? remaining : BATCH_SIZE ))
        local insert_sql="${insert_template//BATCH_PLACEHOLDER/$batch}"
        sql -e "$insert_sql" 2>/dev/null
        inserted=$(( inserted + batch ))
        if (( inserted % 100000 == 0 )); then
            echo "    inserted $inserted / $num_rows rows"
        fi
    done

    # Verify.
    echo "    verifying row count ..."
    sql -e "SELECT count(*) AS row_count FROM omicron.public.test_table;"

    # Stop, snapshot, clean up.
    stop_crdb "$prep_dir"
    rm -f "$prep_dir/cockroach.pid" "$prep_dir/cockroach.stderr"
    mkdir -p "$SNAPSHOT_DIR"
    tar cf "$snapshot_file" -C "$prep_dir" .
    rm -rf "$prep_dir"

    echo "  snapshot saved: $snapshot_file"
}

# -------------------------------------------------------------------
# Test runner.
#
# Untar snapshot, start CRDB with constrained memory, run a single
# CREATE INDEX, and check whether it succeeded or hit OOM.
# -------------------------------------------------------------------

run_test() {
    local config_idx="$1"
    local num_rows="$2"
    local config_name="${CONFIG_NAMES[$config_idx]}"
    local snapshot_file="${SNAPSHOT_DIR}/${config_name}_${num_rows}.tar"
    local index_sql="${CONFIG_INDEX_SQLS[$config_idx]}"
    local index_name="${CONFIG_INDEX_NAMES[$config_idx]}"

    echo "--- testing: $config_name @ $num_rows rows ---"

    local test_dir
    test_dir=$(mktemp -d "${TMPDIR:-/tmp}/crdb-threshold-test.XXXXXX")

    tar xf "$snapshot_file" -C "$test_dir"
    start_crdb "$test_dir"

    # Run CREATE INDEX.  Capture stdout+stderr; do not abort on failure.
    local output
    local exit_code=0
    output=$(sql -e "$index_sql" 2>&1) || exit_code=$?

    # Wait for the schema change job to reach a terminal state.
    # The CREATE INDEX command may return before the backfill completes
    # (or fails), so we poll the job status.
    local job_status=""
    for _ in $(seq 1 60); do
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

    # Check whether the index exists.
    local index_count
    index_count=$(sql --format=csv -e "
SELECT count(*) FROM [SHOW INDEXES FROM omicron.public.test_table]
WHERE index_name = '$index_name';
" 2>/dev/null | tail -1)

    # Fetch the error message if the job failed.
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

    # Determine result.
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
    RESULTS["${config_idx}:${num_rows}"]="$result"
}

# -------------------------------------------------------------------
# Main.
# -------------------------------------------------------------------

echo "========================================"
echo "Index backfill OOM threshold validation"
echo "========================================"
echo ""
echo "CockroachDB binary:  $COCKROACH"
echo "Max SQL memory:      $MAX_SQL_MEMORY"
echo "Snapshot directory:   $SNAPSHOT_DIR"
echo ""

# Phase 1: prepare all snapshots.
echo "=== Phase 1: preparing snapshots ==="
echo ""

for (( ci = 0; ci < NUM_CONFIGS; ci++ )); do
    config_name="${CONFIG_NAMES[$ci]}"
    row_counts_str="${CONFIG_ROW_COUNTS[$ci]}"
    read -ra row_counts <<< "$row_counts_str"

    echo "Config: $config_name"
    for rc in "${row_counts[@]}"; do
        prepare_snapshot "$ci" "$rc"
    done
    echo ""
done

echo "=== Phase 1 complete ==="
echo ""

# Phase 2: run tests.
echo "=== Phase 2: running OOM threshold tests ==="
echo ""

for (( ci = 0; ci < NUM_CONFIGS; ci++ )); do
    config_name="${CONFIG_NAMES[$ci]}"
    row_counts_str="${CONFIG_ROW_COUNTS[$ci]}"
    read -ra row_counts <<< "$row_counts_str"

    echo "Config: $config_name"
    for rc in "${row_counts[@]}"; do
        run_test "$ci" "$rc"
    done
    echo ""
done

echo "=== Phase 2 complete ==="
echo ""

# -------------------------------------------------------------------
# Summary.
# -------------------------------------------------------------------

echo "========================================"
echo "Summary: config x row_count -> result"
echo "========================================"
echo ""

# Print column headers.  First, collect all unique row counts across
# configs (sorted) for the header row, but since each config has its
# own row counts we print per-config rows instead.

for (( ci = 0; ci < NUM_CONFIGS; ci++ )); do
    config_name="${CONFIG_NAMES[$ci]}"
    row_counts_str="${CONFIG_ROW_COUNTS[$ci]}"
    read -ra row_counts <<< "$row_counts_str"

    echo "  $config_name:"
    for rc in "${row_counts[@]}"; do
        result="${RESULTS["${ci}:${rc}"]:-N/A}"
        printf "    %'10d rows -> %s\n" "$rc" "$result"
    done
    echo ""
done

# Print a compact table.
echo "----------------------------------------"
echo "Compact matrix (P=PASS, O=OOM, F=FAIL):"
echo "----------------------------------------"
echo ""

for (( ci = 0; ci < NUM_CONFIGS; ci++ )); do
    config_name="${CONFIG_NAMES[$ci]}"
    row_counts_str="${CONFIG_ROW_COUNTS[$ci]}"
    read -ra row_counts <<< "$row_counts_str"

    line="  $(printf '%-30s' "$config_name")"
    for rc in "${row_counts[@]}"; do
        result="${RESULTS["${ci}:${rc}"]:-N/A}"
        case "$result" in
            PASS)  symbol="P" ;;
            OOM)   symbol="O" ;;
            *)     symbol="F" ;;
        esac
        line+="  $(printf '%7s' "$rc")=$symbol"
    done
    echo "$line"
done

echo ""
echo "Done."
