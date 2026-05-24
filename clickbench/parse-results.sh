#!/bin/bash
# Parse run.sh output into ClickBench results JSON.
# Usage: ./parse-results.sh < run_output.txt > results/c6a.4xlarge.json

set -euo pipefail

MACHINE="${1:-c6a.4xlarge}"
LOAD_TIME="${LOAD_TIME:-0}"
DATA_SIZE="${DATA_SIZE:-0}"

# Collect timings: 3 per query, in milliseconds → seconds
timings=()
while IFS= read -r line; do
    if [[ "$line" =~ Time:\ ([0-9.]+)\ ms ]]; then
        # ms → seconds, 3 decimal places
        secs=$(echo "${BASH_REMATCH[1]} / 1000" | bc -l | xargs printf '%.3f')
        timings+=("$secs")
    elif [[ "$line" =~ ^(QUERY_ERROR|psql:\ error) ]]; then
        # This try failed — record one null. run.sh emits QUERY_ERROR
        # on PG ERROR (via ON_ERROR_STOP) or "psql: error" on psql crash.
        timings+=("null")
    fi
done

# Build JSON
cat <<EOF
{
    "system": "pg_deltax",
    "date": "$(date +%Y-%m-%d)",
    "machine": "${MACHINE}",
    "cluster_size": 1,
    "proprietary": "no",
    "hardware": "cpu",
    "tuned": "no",
    "tags": ["Rust", "PostgreSQL compatible", "column-oriented", "time-series", "lukewarm-cold-run"],
    "load_time": ${LOAD_TIME},
    "data_size": ${DATA_SIZE},
    "result": [
EOF

n=${#timings[@]}
queries=$((n / 3))
for ((q = 0; q < queries; q++)); do
    i=$((q * 3))
    comma=","
    [ $((q + 1)) -eq "$queries" ] && comma=""
    echo "        [${timings[$i]}, ${timings[$((i+1))]}, ${timings[$((i+2))]}]${comma}"
done

echo "    ]"
echo "}"
