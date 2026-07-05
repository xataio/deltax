#!/bin/bash
# Run all RTABench queries 3x each, dropping OS caches between queries.
# Output is consumed by parse-results.sh — the "=== NAME ===" markers
# pair filenames with timings so partial-failure runs still produce
# well-formed JSON.

set -u

TRIES=3
DIR="$(cd "$(dirname "$0")" && pwd)"

for file in "$DIR/queries"/*.sql; do
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null

    name="$(basename "$file" .sql)"
    echo "=== $name ==="

    query="$(cat "$file")"
    for i in $(seq 1 $TRIES); do
        # `work_mem=50MB` matches the TimescaleDB RTABench harness for an
        # apples-to-apples comparison.
        sudo -u postgres psql test --no-psqlrc --tuples-only \
            --command "\timing on" \
            --command "SET work_mem = '50MB'" \
            --command "$query" 2>&1 | grep -P 'Time|psql: error' | tail -n1
    done
done
