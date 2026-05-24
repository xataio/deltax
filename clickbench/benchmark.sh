#!/bin/bash
# Full EC2 benchmark setup: install deps, build extension, download data, load+compress.
# Expects pg_deltax source already at ~/pg_deltax (synced via `make deploy`).
# Expects this script to run from ~/clickbench on the EC2 instance.
#
# Uses direct backfill (FORMAT deltax_compress) to load and compress in a single pass.
# Drops and recreates the DB so it can be re-run.

set -euo pipefail

PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config
DB=test
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Install PostgreSQL 18
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -y
sudo apt-get install -y gnupg postgresql-common apt-transport-https lsb-release wget pigz
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
sudo apt-get update -y
sudo apt-get install -y postgresql-18 postgresql-client-18

# Install Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
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

# Drop and recreate the database (allows re-running for recompression)
sudo -u postgres psql -c "DROP DATABASE IF EXISTS $DB"
sudo -u postgres psql -c "CREATE DATABASE $DB"
sudo -u postgres psql "$DB" -c "CREATE EXTENSION pg_deltax"
sudo -u postgres psql -c "ALTER DATABASE $DB SET work_mem TO '1GB'"
sudo -u postgres psql -c "ALTER DATABASE $DB SET min_parallel_table_scan_size TO '0'"

# Download data
PARQUET="${PARQUET:-1}"
if [ "$PARQUET" = "1" ]; then
    PARQUET_DIR=/tmp/hits_parquet
    if [ ! -d "$PARQUET_DIR" ] || [ "$(ls "$PARQUET_DIR"/*.parquet 2>/dev/null | wc -l)" -lt 100 ]; then
        sudo mkdir -p "$PARQUET_DIR"
        echo "Downloading 100 parquet files..."
        seq 0 99 | xargs -P20 -I{} sudo wget -q -nc -P "$PARQUET_DIR" \
            "https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_{}.parquet"
        sudo chmod 644 "$PARQUET_DIR"/*.parquet
    fi
    TOTAL_LINES="(parquet)"
else
    if [ ! -f /tmp/hits.tsv ]; then
        wget --continue --progress=dot:giga 'https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz'
        pigz -d -f hits.tsv.gz
        sudo mv hits.tsv /tmp/hits.tsv
        sudo chmod 644 /tmp/hits.tsv
    fi
    TOTAL_LINES=$(wc -l < /tmp/hits.tsv)
fi

# Create table
sudo -u postgres psql "$DB" < create.sql 2>&1 | tee load_out.txt
if grep 'ERROR' load_out.txt; then
    exit 1
fi

# Set up partitioning — mock_now must be set before deltax_create_table
sudo -u postgres psql "$DB" -t -c "SET pg_deltax.mock_now = '2013-07-01 12:00:00'; SELECT deltax.deltax_create_table('hits', 'eventtime', '3 days'::interval, 15)"

# Enable compression before loading (required for direct backfill)
sudo -u postgres psql "$DB" -t -c "SELECT deltax.deltax_enable_compression('hits', order_by => ARRAY['counterid', 'userid', 'eventtime'], segment_size => 30000)"

# Direct backfill: load and compress in a single pass using FORMAT deltax_compress
LOAD_START=$(date +%s)
if [ "$PARQUET" = "1" ]; then
    echo "Loading data from parquet files (FORMAT deltax_compress)..."
    sudo -u postgres psql "$DB" -c "COPY hits FROM '/tmp/hits_parquet/hits_*.parquet' WITH (FORMAT deltax_compress)"
else
    echo "Loading data from TSV (FORMAT deltax_compress)..."
    sudo -u postgres psql "$DB" -c "COPY hits FROM '/tmp/hits.tsv' WITH (FORMAT deltax_compress, DELIMITER E'\t')"
fi
LOAD_END=$(date +%s)
echo "Load+compress time: $((LOAD_END - LOAD_START))s ($TOTAL_LINES rows, direct backfill)"

# Vacuum
echo -n "Vacuum time: "
VACUUM_START=$(date +%s)
sudo -u postgres psql "$DB" -q -t -c "VACUUM FREEZE ANALYZE hits"
VACUUM_END=$(date +%s)
echo "Vacuum time: $((VACUUM_END - VACUUM_START))s"

# Capture data size (bytes)
DATA_SIZE=$(sudo -u postgres psql "$DB" -t -A -c "SELECT deltax.deltax_table_size('hits')")
echo "Data size: $DATA_SIZE bytes ($(echo "$DATA_SIZE / 1024 / 1024 / 1024" | bc -l | xargs printf '%.2f') GB)"

# Save load stats for the bench target to pick up later
LOAD_TIME=$((LOAD_END - LOAD_START + VACUUM_END - VACUUM_START))
cat > ~/clickbench/load_stats.env <<STATS
LOAD_TIME=$LOAD_TIME
DATA_SIZE=$DATA_SIZE
STATS
echo "Saved load stats to ~/clickbench/load_stats.env (load_time=${LOAD_TIME}s, data_size=${DATA_SIZE})"

# Lower work_mem and disable JIT for the query phase
sudo -u postgres psql -c "ALTER DATABASE $DB SET work_mem TO '256MB'"
sudo -u postgres psql -c "ALTER DATABASE $DB SET jit TO off"

# Report partition and compression info
sudo -u postgres psql "$DB" -c "SELECT * FROM deltax.deltax_partition_info('hits')"
sudo -u postgres psql "$DB" -c "SELECT count(*) AS default_partition_rows FROM hits_default"

echo "Setup complete. Database '$DB' is ready."
echo "Run queries manually with: sudo -u postgres psql $DB -c '\timing' -c 'QUERY'"
echo "Or run all queries with: ./run.sh"
