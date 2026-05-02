#!/bin/bash
# Parse run.sh output into JSONBench-compatible results JSON.
# Usage: ./parse-results.sh < run_output.txt > results/pg_deltax.json

set -euo pipefail

MACHINE="${MACHINE:-m6i.8xlarge}"
LOAD_TIME="${LOAD_TIME:-0}"
DATA_SIZE="${DATA_SIZE:-0}"
DATASET_SIZE="${DATASET_SIZE:-unknown}"

# Collect timings: 3 per query, ms -> seconds
timings=()
while IFS= read -r line; do
    if [[ "$line" =~ Time:\ ([0-9.]+)\ ms ]]; then
        secs=$(echo "${BASH_REMATCH[1]} / 1000" | bc -l | xargs printf '%.3f')
        timings+=("$secs")
    elif [[ "$line" =~ psql:\ error ]]; then
        timings+=("null" "null" "null")
    fi
done

# Build JSON
cat <<EOF
{
    "system": "pg_deltax",
    "date": "$(date +%Y-%m-%d)",
    "machine": "${MACHINE}",
    "dataset_size": "${DATASET_SIZE}",
    "cluster_size": 1,
    "proprietary": "no",
    "hardware": "cpu",
    "tuned": "no",
    "tags": ["Rust", "PostgreSQL compatible", "column-oriented", "JSON", "lukewarm-cold-run"],
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
