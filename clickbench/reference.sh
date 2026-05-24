#!/bin/bash
# Vanilla-PostgreSQL bootstrap for ClickBench correctness reference.
#
# Runs on a *separate* EC2 instance — no pg_deltax build, no extension,
# no partitioning, no compression. Loads the same 100M-row ClickBench
# dataset via plain COPY so the query result sets match what the
# pg_deltax bench EC2 should produce.
#
# Expects this script to run from ~/clickbench on the EC2 instance.
# Once this finishes, run capture_results.sh to dump the result sets.
#
# Drops and recreates the DB so it can be re-run.

set -euo pipefail

PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config
DB=test
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Install PostgreSQL 18 (no Rust / pgrx / pg_deltax dependencies).
export DEBIAN_FRONTEND=noninteractive
sudo apt-get update -y
sudo apt-get install -y gnupg postgresql-common apt-transport-https lsb-release wget pigz
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
sudo apt-get update -y
sudo apt-get install -y postgresql-18 postgresql-client-18

# Drop and recreate the database so reruns are clean.
sudo -u postgres psql -c "DROP DATABASE IF EXISTS $DB"
sudo -u postgres psql -c "CREATE DATABASE $DB"
sudo -u postgres psql -c "ALTER DATABASE $DB SET work_mem TO '1GB'"
sudo -u postgres psql -c "ALTER DATABASE $DB SET min_parallel_table_scan_size TO '0'"

# Download the gzipped TSV — same source the bench EC2 uses with PARQUET=0.
# Single ~14 GB compressed file; ~70 GB unzipped.
if [ ! -f /tmp/hits.tsv ]; then
    cd /tmp
    sudo wget --continue --progress=dot:giga 'https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz'
    sudo pigz -d -f hits.tsv.gz
    sudo chmod 644 hits.tsv
    cd "$SCRIPT_DIR"
fi
TOTAL_LINES=$(wc -l < /tmp/hits.tsv)

# Create the plain hits table (same schema as create.sql, no extension calls).
sudo -u postgres psql "$DB" < create.sql 2>&1 | tee load_out.txt
if grep 'ERROR' load_out.txt; then
    exit 1
fi

# Plain COPY load — no pg_deltax involved.
# FORMAT text is COPY's default: tab delimiter, '\N' for NULL, empty fields
# stay empty strings (CSV would coerce them to NULL and break NOT NULL cols).
echo "Loading $TOTAL_LINES rows from /tmp/hits.tsv ..."
LOAD_START=$(date +%s)
sudo -u postgres psql "$DB" -c "COPY hits FROM '/tmp/hits.tsv'"
LOAD_END=$(date +%s)
echo "Load time: $((LOAD_END - LOAD_START))s"

# Freeze + analyze so subsequent reads are cheap and stable.
echo -n "Vacuum time: "
VACUUM_START=$(date +%s)
sudo -u postgres psql "$DB" -q -t -c "VACUUM FREEZE ANALYZE hits"
VACUUM_END=$(date +%s)
echo "$((VACUUM_END - VACUUM_START))s"

# Lower work_mem and disable JIT for the query phase, matching benchmark.sh.
sudo -u postgres psql -c "ALTER DATABASE $DB SET work_mem TO '256MB'"
sudo -u postgres psql -c "ALTER DATABASE $DB SET jit TO off"

# Sanity-check row count — this becomes the "row_count" field in reference metadata.
ROW_COUNT=$(sudo -u postgres psql "$DB" -t -A -c "SELECT count(*) FROM hits")
PG_VERSION=$(sudo -u postgres psql -t -A -c "SHOW server_version")

cat > ~/clickbench/reference_stats.env <<STATS
ROW_COUNT=$ROW_COUNT
PG_VERSION=$PG_VERSION
STATS

echo "Vanilla PG reference DB ready: $ROW_COUNT rows on PG $PG_VERSION"
echo "Run capture_results.sh next to dump per-query result sets."
