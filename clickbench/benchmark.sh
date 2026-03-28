#!/bin/bash
# Full EC2 benchmark setup: install deps, build extension, download data, load, compress.
# Expects pg_deltax source already at ~/pg_deltax (synced via `make deploy`).
# Expects this script to run from ~/clickbench on the EC2 instance.

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

# Configure PostgreSQL
sudo bash -c "echo \"shared_preload_libraries = 'pg_deltax'\" >> /etc/postgresql/18/main/postgresql.conf"
sudo systemctl restart postgresql

# Tune database settings
sudo -u postgres psql -c "CREATE DATABASE $DB"
sudo -u postgres psql "$DB" -c "CREATE EXTENSION pg_deltax"
sudo -u postgres psql -c "ALTER DATABASE $DB SET work_mem TO '1GB'"
sudo -u postgres psql -c "ALTER DATABASE $DB SET min_parallel_table_scan_size TO '0'"

# Parallelism for loading and compression
LOAD_WORKERS=8
COMPRESS_WORKERS=8

# Download data
wget --continue --progress=dot:giga 'https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz'
pigz -d -f hits.tsv.gz
sudo mv hits.tsv /tmp/hits.tsv
sudo chmod 644 /tmp/hits.tsv

# Split TSV into chunks for parallel loading
echo "Splitting data into $LOAD_WORKERS chunks..."
SPLIT_DIR=/tmp/hits_chunks
sudo rm -rf "$SPLIT_DIR"
sudo mkdir -p "$SPLIT_DIR"
TOTAL_LINES=$(wc -l < /tmp/hits.tsv)
LINES_PER_CHUNK=$(( (TOTAL_LINES + LOAD_WORKERS - 1) / LOAD_WORKERS ))
sudo split -l "$LINES_PER_CHUNK" -d -a 2 /tmp/hits.tsv "$SPLIT_DIR/chunk_"
sudo chmod 644 "$SPLIT_DIR"/chunk_*
echo "Split into $(ls "$SPLIT_DIR" | wc -l) chunks of ~$LINES_PER_CHUNK lines each"

# Create table
sudo -u postgres psql "$DB" < create.sql 2>&1 | tee load_out.txt
if grep 'ERROR' load_out.txt; then
    exit 1
fi

# Set up partitioning — mock_now must be set before deltax_create_table
sudo -u postgres psql "$DB" -t -c "SET pg_deltax.mock_now = '2013-07-01 12:00:00'; SELECT deltax_create_table('hits', 'eventtime', '3 days'::interval, 15)"

# Parallel data loading
sudo apt-get install -y parallel
echo "Loading data with $LOAD_WORKERS parallel workers..."
echo -n "Load time: "
command time -f '%e' ls "$SPLIT_DIR"/chunk_* \
    | parallel -j "$LOAD_WORKERS" \
        "sudo -u postgres psql $DB -c \"\\copy hits FROM '{}'\""
echo "Loaded $TOTAL_LINES rows"

# Enable compression
sudo -u postgres psql "$DB" -t -c "SELECT deltax_enable_compression('hits', order_by => ARRAY['counterid', 'userid', 'eventtime'], segment_size => 30000)"

# Parallel compression
echo "Compressing partitions with $COMPRESS_WORKERS parallel workers..."
echo -n "Compress time: "
command time -f '%e' sudo -u postgres psql "$DB" -t -A -c \
    "SELECT partition_name FROM deltax_partition_info('hits') WHERE partition_name NOT LIKE '%default%'" \
    | grep -v '^$' \
    | parallel -j "$COMPRESS_WORKERS" \
        "sudo -u postgres psql $DB -q -c \"SELECT deltax_compress_partition('{}')\" && echo '  Compressed {}'"

# Vacuum
echo -n "Vacuum time: "
command time -f '%e' sudo -u postgres psql "$DB" -q -t -c "VACUUM FREEZE ANALYZE hits"

# Clean up chunks
sudo rm -rf "$SPLIT_DIR"

# Report data size
echo -n "Data size: "
sudo -u postgres psql "$DB" -t -c "SELECT pg_total_relation_size('hits')"

# Lower work_mem and disable JIT for the query phase
sudo -u postgres psql -c "ALTER DATABASE $DB SET work_mem TO '256MB'"
sudo -u postgres psql -c "ALTER DATABASE $DB SET jit TO off"

# Report partition and compression info
sudo -u postgres psql "$DB" -c "SELECT * FROM deltax_partition_info('hits')"
sudo -u postgres psql "$DB" -c "SELECT count(*) AS default_partition_rows FROM hits_default"

echo "Setup complete. Database '$DB' is ready."
echo "Run queries manually with: sudo -u postgres psql $DB -c '\timing' -c 'QUERY'"
echo "Or run all queries with: ./run.sh"
