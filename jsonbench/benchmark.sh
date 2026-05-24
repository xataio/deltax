#!/bin/bash
# Full EC2 benchmark setup: install deps, build extension, download data, transform, load+compress.
# Expects pg_deltax source already at ~/pg_deltax (synced via `make setup`).
# Expects this script to run from ~/jsonbench on the EC2 instance.
#
# Pipeline: file_NNNN.json.gz -> jq transform -> TSV (ts<TAB>data) -> COPY ... FORMAT deltax_compress
# Drops and recreates the DB so it can be re-run.
#
# Knobs:
#   SCALE=100     # number of files to load (1m=1, 10m=10, 100m=100, 1000m=1000)

set -euo pipefail

PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config
DB=bluesky
SCALE="${SCALE:-100}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

RAW_DIR=/tmp/bluesky_raw
TSV_DIR=/tmp/bluesky_tsv

# Install PostgreSQL 18
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -y
sudo apt-get install -y gnupg postgresql-common apt-transport-https lsb-release wget pigz jq parallel
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
sudo apt-get update -y
sudo apt-get install -y postgresql-18 postgresql-client-18

# Install Rust toolchain (skip if already present)
if [ ! -f "$HOME/.cargo/env" ]; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
fi
source "$HOME/.cargo/env"

# Install build dependencies
sudo apt-get install -y pkg-config libssl-dev libclang-dev clang postgresql-server-dev-18

# Install cargo-pgrx
if ! command -v cargo-pgrx >/dev/null 2>&1 || ! cargo pgrx --version 2>/dev/null | grep -q '0.17'; then
    cargo install cargo-pgrx --version 0.17.0 --locked
fi

# Build and install pg_deltax (source already synced by Makefile)
cd ~/pg_deltax
cargo pgrx init --pg18 "$PG_CONFIG"
sudo env "PATH=$PATH" "RUSTUP_HOME=${RUSTUP_HOME:-$HOME/.rustup}" "CARGO_HOME=${CARGO_HOME:-$HOME/.cargo}" "PGRX_HOME=$HOME/.pgrx" \
    cargo pgrx install --pg-config "$PG_CONFIG" --release
cd "$SCRIPT_DIR"

# Configure PostgreSQL (idempotent: only add if not already present)
if ! sudo grep -q "shared_preload_libraries.*pg_deltax" /etc/postgresql/18/main/postgresql.conf; then
    sudo bash -c "echo \"shared_preload_libraries = 'pg_deltax'\" >> /etc/postgresql/18/main/postgresql.conf"
fi
sudo systemctl restart postgresql

# Drop and recreate the database (allows re-running for recompression)
sudo -u postgres psql -c "DROP DATABASE IF EXISTS $DB"
sudo -u postgres psql -c "CREATE DATABASE $DB"
sudo -u postgres psql "$DB" -c "CREATE EXTENSION pg_deltax"
sudo -u postgres psql -c "ALTER DATABASE $DB SET work_mem TO '1GB'"
sudo -u postgres psql -c "ALTER DATABASE $DB SET min_parallel_table_scan_size TO '0'"
# Use the box: PG defaults cap at 2 workers per Gather (~3 cores total),
# which leaves a 32-vCPU m6i.8xlarge mostly idle during scans. Sized for
# m6i.8xlarge — bump if you swap instance class.
sudo -u postgres psql -c "ALTER DATABASE $DB SET max_parallel_workers_per_gather TO 16"
sudo -u postgres psql -c "ALTER SYSTEM SET max_parallel_workers TO 32"
sudo -u postgres psql -c "ALTER SYSTEM SET max_worker_processes TO 64"
# max_worker_processes requires a full restart, the others reload.
sudo -u postgres psql -c "SELECT pg_reload_conf()"
sudo systemctl restart postgresql

# Download data
mkdir -p "$RAW_DIR"
echo "Downloading $SCALE Bluesky files..."
seq -f "https://clickhouse-public-datasets.s3.amazonaws.com/bluesky/file_%04g.json.gz" 1 "$SCALE" \
    | xargs -P10 -I{} wget --continue --timestamping -q -P "$RAW_DIR" {}

# Transform ndjson.gz -> TSV (ts<TAB>jsonb_text). Cached: skips files already converted.
mkdir -p "$TSV_DIR"
sudo chmod 777 "$TSV_DIR"
echo "Transforming ndjson -> TSV (parallelism: $(nproc))..."
TRANSFORM_START=$(date +%s)
shopt -s nullglob
RAW_FILES=("$RAW_DIR"/file_*.json.gz)
shopt -u nullglob
if [ "${#RAW_FILES[@]}" -lt "$SCALE" ]; then
    echo "Error: expected $SCALE files in $RAW_DIR, found ${#RAW_FILES[@]}." >&2
    exit 1
fi
printf '%s\n' "${RAW_FILES[@]:0:$SCALE}" | \
    xargs -P"$(nproc)" -I{} bash -c '
        f="{}"
        out="'"$TSV_DIR"'/$(basename "$f" .json.gz).tsv"
        if [ -s "$out" ]; then exit 0; fi
        # Tolerant ndjson parse:
        #   `jq -R` reads each input line as a raw string, `fromjson?` yields
        #   the parsed JSON (or empty on parse error). Without this, a single
        #   record containing an unescaped raw control byte (Bluesky firehose
        #   data has these in user-text fields) makes jq abort and the rest
        #   of the file is silently dropped — observed loss of ~5.6M rows
        #   out of 100M before this fix.
        # Null-byte sanitisation:
        #   pre-jq sed: removes \\u0000 escapes already in the source ndjson
        #   post-jq sed: jq tojson can re-emit \\u0000 escapes for embedded NULs
        #   tr: belt-and-suspenders against any raw \0 byte that might survive
        # Postgres rejects \0 in jsonb_in, and CString::new fails on raw \0.
        pigz -dc "$f" \
            | sed "s/\\\\u0000//g" \
            | jq -rcR "fromjson? | select(.time_us != null) | [(.time_us|tonumber/1000000|todate), tojson] | @tsv" \
            | sed "s/\\\\u0000//g" \
            | tr -d "\000" \
            > "$out.tmp"
        mv "$out.tmp" "$out"
    '
TRANSFORM_END=$(date +%s)
echo "Transform time: $((TRANSFORM_END - TRANSFORM_START))s"

# Probe data time range from the first TSV. Take min of the first 10K rows so
# we don't depend on the actual first event being the earliest in time.
# Compute the min via awk rather than `sort | head -1` to avoid SIGPIPE under pipefail.
shopt -s nullglob
TSV_FILES=("$TSV_DIR"/file_*.tsv)
shopt -u nullglob
FIRST_TSV="${TSV_FILES[0]:-}"
if [ -z "$FIRST_TSV" ] || [ ! -s "$FIRST_TSV" ]; then
    echo "Error: no .tsv files found in $TSV_DIR after transform." >&2
    exit 1
fi
DATA_START=$(head -10000 "$FIRST_TSV" | cut -f1 | awk 'NR==1 {m=$0} $0<m {m=$0} END {print m}')
echo "Data starts around: $DATA_START"

# Create table
sudo -u postgres psql "$DB" < create.sql 2>&1 | tee load_out.txt
if grep 'ERROR' load_out.txt; then
    exit 1
fi

# Set up partitioning. mock_now is set to the observed data start; 365 daily
# partitions covers any plausible Bluesky collection window. Out-of-range data
# falls into the default partition and stays uncompressed — the partition_info
# query at the end will surface this if the heuristic is off.
sudo -u postgres psql "$DB" -t -c "SET pg_deltax.mock_now = '$DATA_START'; SELECT deltax.deltax_create_table('bluesky', 'ts', '1 day'::interval, 365)"

# Enable compression before loading (required for direct backfill).
# json_extract pre-extracts the JSON paths every JSONBench query touches into
# columnar synthetic columns; the planner_hook walker then rewrites
# `data->>'kind'`-style chains in upper plans to Var refs into those
# synthetic columns, sidestepping per-row jsonb_in / `->`/`->>` evaluation
# on the warm path. Requires pg_deltax.json_extract_mode=fields at query time.
sudo -u postgres psql "$DB" -t -c "SELECT deltax.deltax_enable_compression(
    'bluesky',
    order_by => ARRAY['ts'],
    segment_size => 30000,
    json_extract => '[
        {\"src\":\"data\",\"path\":[\"kind\"],\"name\":\"x_kind\",\"type\":\"text\"},
        {\"src\":\"data\",\"path\":[\"did\"],\"name\":\"x_did\",\"type\":\"text\"},
        {\"src\":\"data\",\"path\":[\"time_us\"],\"name\":\"x_time_us\",\"type\":\"bigint\"},
        {\"src\":\"data\",\"path\":[\"commit\",\"collection\"],\"name\":\"x_collection\",\"type\":\"text\"},
        {\"src\":\"data\",\"path\":[\"commit\",\"operation\"],\"name\":\"x_operation\",\"type\":\"text\"}
    ]'::jsonb
)"

# Enable the planner_hook walker by default for queries on this DB so that
# `data->>'kind'` etc. transparently use the pre-extracted columns.
sudo -u postgres psql -c "ALTER DATABASE $DB SET pg_deltax.json_extract_mode = 'fields'"

# Direct backfill: load and compress in a single pass via FORMAT deltax_compress.
# Single COPY with a glob — pg_deltax expands it and processes all matched
# files within one statement, so partitions are finalized only once at the
# end. (Per-file COPYs would compress the partition after file 1 and reject
# every subsequent file, since Bluesky data crosses daily partition lines.)
LOAD_START=$(date +%s)
TSV_COUNT=$(ls "$TSV_DIR"/file_*.tsv 2>/dev/null | wc -l)
if [ "$TSV_COUNT" -ne "$SCALE" ]; then
    echo "Error: $TSV_DIR has $TSV_COUNT .tsv files but SCALE=$SCALE." >&2
    echo "       Wipe $TSV_DIR (rm $TSV_DIR/file_*.tsv) and re-run setup." >&2
    exit 1
fi
echo "Loading $TSV_COUNT TSV files (FORMAT deltax_compress, single COPY via glob)..."
sudo -u postgres psql "$DB" -c \
    "COPY bluesky (ts, data) FROM '$TSV_DIR/file_*.tsv' WITH (FORMAT deltax_compress, DELIMITER E'\t')"
LOAD_END=$(date +%s)
echo "Load+compress time: $((LOAD_END - LOAD_START))s"

# Vacuum
echo -n "Vacuum start..."
VACUUM_START=$(date +%s)
sudo -u postgres psql "$DB" -q -t -c "VACUUM FREEZE ANALYZE bluesky"
VACUUM_END=$(date +%s)
echo " done in $((VACUUM_END - VACUUM_START))s"

# Capture data size (bytes) and row count for the JSONBench dashboard.
DATA_SIZE=$(sudo -u postgres psql "$DB" -t -A -c "SELECT deltax.deltax_table_size('bluesky')")
NUM_LOADED=$(sudo -u postgres psql "$DB" -t -A -c "SELECT count(*) FROM bluesky")
echo "Data size: $DATA_SIZE bytes ($(echo "$DATA_SIZE / 1024 / 1024 / 1024" | bc -l | xargs printf '%.2f') GB)"
echo "Loaded documents: $NUM_LOADED"

# Save load stats. LOAD_TIME includes transform + COPY + vacuum so it reflects the
# end-to-end time from raw .json.gz to a query-ready compressed table.
LOAD_TIME=$((TRANSFORM_END - TRANSFORM_START + LOAD_END - LOAD_START + VACUUM_END - VACUUM_START))
# JSONBench expects dataset_size as a NUMBER (target row count), not a label.
# 1 -> 1m -> 1_000_000, 10 -> 10_000_000, 100 -> 100_000_000, 1000 -> 1_000_000_000.
DATASET_SIZE_NUM=$((SCALE * 1000000))
cat > ~/jsonbench/load_stats.env <<STATS
LOAD_TIME=$LOAD_TIME
DATA_SIZE=$DATA_SIZE
NUM_LOADED=$NUM_LOADED
DATASET_SIZE=$DATASET_SIZE_NUM
STATS
echo "Saved load stats to ~/jsonbench/load_stats.env (load_time=${LOAD_TIME}s, data_size=${DATA_SIZE}, num_loaded=${NUM_LOADED}, dataset_size=${DATASET_SIZE_NUM})"

# Lower work_mem and disable JIT for the query phase
sudo -u postgres psql -c "ALTER DATABASE $DB SET work_mem TO '256MB'"
sudo -u postgres psql -c "ALTER DATABASE $DB SET jit TO off"

# Report partition and compression info
sudo -u postgres psql "$DB" -c "SELECT * FROM deltax.deltax_partition_info('bluesky')"
sudo -u postgres psql "$DB" -c "SELECT count(*) AS default_partition_rows FROM bluesky_default"

echo "Setup complete. Database '$DB' is ready (SCALE=$SCALE, dataset=${SCALE}m)."
echo "Run all queries with: ./run.sh"
