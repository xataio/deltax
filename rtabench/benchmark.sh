#!/bin/bash
# Full EC2 benchmark setup for RTABench: install deps, build extension, download CSVs,
# load + compress order_events via direct backfill, plain COPY for the dimension tables.
# Expects pg_deltax source already at ~/pg_deltax (synced via `make setup`/`make deploy`).
# Expects this script to run from ~/rtabench on the EC2 instance.
#
# Drops and recreates the DB so it can be re-run.

set -euo pipefail

PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config
DB=test
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_DIR=/tmp/rtabench_csv

# Install PostgreSQL 18
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -y
sudo apt-get install -y gnupg postgresql-common apt-transport-https lsb-release wget pigz
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
sudo apt-get update -y
sudo apt-get install -y postgresql-18 postgresql-client-18

# Install Rust toolchain
if [ ! -d "$HOME/.cargo" ]; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
fi
source "$HOME/.cargo/env"

# Install build dependencies
sudo apt-get install -y pkg-config libssl-dev libclang-dev clang postgresql-server-dev-18

# Install cargo-pgrx
cargo install cargo-pgrx --version 0.17.0 --locked

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

# Drop and recreate the database (allows re-running)
sudo -u postgres psql -c "DROP DATABASE IF EXISTS $DB"
sudo -u postgres psql -c "CREATE DATABASE $DB"
sudo -u postgres psql "$DB" -c "CREATE EXTENSION pg_deltax"
sudo -u postgres psql -c "ALTER DATABASE $DB SET work_mem TO '1GB'"
sudo -u postgres psql -c "ALTER DATABASE $DB SET min_parallel_table_scan_size TO '0'"

# Download data (5 CSVs)
sudo mkdir -p "$DATA_DIR"
download_one() {
    local name="$1"
    if [ ! -f "$DATA_DIR/$name.csv" ]; then
        sudo wget -q --continue -O "$DATA_DIR/$name.csv.gz" \
            "https://rtadatasets.timescale.com/$name.csv.gz"
        sudo pigz -d -f "$DATA_DIR/$name.csv.gz"
    fi
}
echo "Downloading 5 CSV files in parallel..."
for f in customers products orders order_items order_events; do
    download_one "$f" &
done
wait
sudo chmod 644 "$DATA_DIR"/*.csv

# Create schema
sudo -u postgres psql "$DB" < create.sql 2>&1 | tee load_out.txt
if grep 'ERROR' load_out.txt; then
    exit 1
fi

# Convert order_events to a pg_deltax time-series table.
# mock_now is set to the start of the dataset's range; 125 partitions of 3 days
# covers 2024-01-01 → 2025-01-15, comfortably spanning all data.
sudo -u postgres psql "$DB" -t -c \
    "SET pg_deltax.mock_now = '2024-01-01 00:00:00'; SELECT deltax.deltax_create_table('order_events', 'event_created', '3 days'::interval, 125)"

# Enable compression before loading (required for direct backfill).
# order_by mirrors the TimescaleDB baseline and matches the dominant query
# pattern (point lookups + short scans by order_id within a time window).
# SEGMENT_SIZE is overridable to allow sweeping (e.g. 1000, 10000, 30000).
SEGMENT_SIZE="${SEGMENT_SIZE:-30000}"
echo "Using segment_size=$SEGMENT_SIZE"
# `event_payload->>'terminal'` is the only chain RTABench queries touch
# (Q0/Q1/Q3/Q4/Q8/Q23). Pre-extract it so the planner_hook walker can
# rewrite chains to synthetic-Var refs and DeltaXAgg picks the queries
# up directly.
sudo -u postgres psql "$DB" -t -c \
    "SELECT deltax.deltax_enable_compression('order_events', order_by => ARRAY['order_id','event_created'], segment_size => $SEGMENT_SIZE, \
        json_extract => '[{\"src\":\"event_payload\",\"path\":[\"terminal\"],\"name\":\"x_terminal\",\"type\":\"text\"}]'::jsonb)"

# Activate the planner_hook walker by default so chain Exprs use the
# pre-extracted synthetic column.
sudo -u postgres psql -c "ALTER DATABASE $DB SET pg_deltax.json_extract_mode = 'fields'"

# Load dimension / small tables via plain COPY
LOAD_START=$(date +%s)
for t in customers products orders order_items; do
    echo "Loading $t..."
    sudo -u postgres psql "$DB" -c "COPY $t FROM '$DATA_DIR/$t.csv' WITH (FORMAT csv)"
done

# Load order_events via direct backfill (compress in-flight).
# FORMAT deltax_compress_csv uses PG's CSV parser under the hood so that
# the quoted jsonb event_payload column (embedded commas and quotes) is
# handled correctly.
echo "Loading order_events (FORMAT deltax_compress_csv)..."
sudo -u postgres psql "$DB" -c "COPY order_events FROM '$DATA_DIR/order_events.csv' WITH (FORMAT deltax_compress_csv)"
LOAD_END=$(date +%s)
echo "Load time: $((LOAD_END - LOAD_START))s"

# Indexes — match plain-PG rtabench baseline (no indexes on order_events).
sudo -u postgres psql "$DB" -c "CREATE INDEX ON orders (customer_id)"

# Vacuum
echo -n "Vacuum time: "
VACUUM_START=$(date +%s)
sudo -u postgres psql "$DB" -q -t -c "VACUUM FREEZE ANALYZE customers, products, orders, order_items, order_events"
VACUUM_END=$(date +%s)
echo "$((VACUUM_END - VACUUM_START))s"

# Capture data size = deltax(order_events) + plain(other 4 tables)
DELTAX_SIZE=$(sudo -u postgres psql "$DB" -t -A -c "SELECT deltax.deltax_table_size('order_events')")
PLAIN_SIZE=$(sudo -u postgres psql "$DB" -t -A -c "SELECT coalesce(sum(pg_total_relation_size(c.oid))::bigint, 0) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname IN ('customers','products','orders','order_items')")
DATA_SIZE=$((DELTAX_SIZE + PLAIN_SIZE))
echo "Data size: $DATA_SIZE bytes ($(echo "$DATA_SIZE / 1024 / 1024 / 1024" | bc -l | xargs printf '%.2f') GB)"

# Save load stats for the bench target to pick up later
LOAD_TIME=$((LOAD_END - LOAD_START + VACUUM_END - VACUUM_START))
cat > ~/rtabench/load_stats.env <<STATS
LOAD_TIME=$LOAD_TIME
DATA_SIZE=$DATA_SIZE
STATS
echo "Saved load stats to ~/rtabench/load_stats.env (load_time=${LOAD_TIME}s, data_size=${DATA_SIZE})"

# Tune PG for query phase (sized for c6a.4xlarge: 16 vCPU, 32 GB RAM).
# shared_buffers + max_worker_processes require a server restart; set them
# via ALTER SYSTEM and restart once before queries run.
sudo -u postgres psql -c "ALTER SYSTEM SET shared_buffers = '8GB'"
sudo -u postgres psql -c "ALTER SYSTEM SET effective_cache_size = '24GB'"
sudo -u postgres psql -c "ALTER SYSTEM SET max_worker_processes = 16"
sudo -u postgres psql -c "ALTER SYSTEM SET max_parallel_workers = 16"
sudo -u postgres psql -c "ALTER SYSTEM SET max_parallel_workers_per_gather = 8"
sudo -u postgres psql -c "ALTER DATABASE $DB SET work_mem TO '8GB'"
sudo -u postgres psql -c "ALTER DATABASE $DB SET jit TO off"
sudo systemctl restart postgresql

# Report partition / compression info and check for default-partition pollution
sudo -u postgres psql "$DB" -c "SELECT * FROM deltax.deltax_partition_info('order_events')"
sudo -u postgres psql "$DB" -c "SELECT count(*) AS default_partition_rows FROM order_events_default"

echo "Setup complete. Database '$DB' is ready."
echo "Run all queries with: ./run.sh"
