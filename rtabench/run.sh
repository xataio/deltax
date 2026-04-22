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
        # `enable_nestloop=off` is a benchmark tuning: Q17-style queries
        # hit a NestLoop-over-Materialize plan with a 105M-row inner that
        # runs for 30+ minutes; disabling NL forces a pair of hash joins
        # and drops it to ~25 s. Point-lookup queries (Q07/Q10/Q11) still
        # finish in single-digit ms on hash paths.
        # `work_mem=8GB` keeps the 105M-row hash in a single batch (would
        # otherwise spill to disk at 2 GB, doubling runtime).
        sudo -u postgres psql test --no-psqlrc --tuples-only \
            --command "\timing on" \
            --command "SET enable_nestloop = off" \
            --command "SET work_mem = '8GB'" \
            --command "$query" 2>&1 | grep -P 'Time|psql: error' | tail -n1
    done
done
