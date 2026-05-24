#!/bin/bash
# Parse run.sh output into JSONBench-compatible results JSON.
# Usage: ./parse-results.sh < run_output.txt > results/pg_deltax.json

set -euo pipefail

# Schema matches the upstream JSONBench convention so the dashboard
# (`generate-results.sh` + `index.html`) recognizes our system. See e.g.
# the existing ClickHouse / PostgreSQL result files in the JSONBench repo.
MACHINE="${MACHINE:-m6i.8xlarge, 10000gib gp3}"
LOAD_TIME="${LOAD_TIME:-0}"
DATA_SIZE="${DATA_SIZE:-0}"
DATASET_SIZE="${DATASET_SIZE:-0}"   # NUMBER of target rows, not a label
NUM_LOADED="${NUM_LOADED:-0}"

# Collect timings: 3 per query, ms -> seconds
timings=()
while IFS= read -r line; do
    if [[ "$line" =~ Time:\ ([0-9.]+)\ ms ]]; then
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
    "version": "0.1.0",
    "os": "Ubuntu 24.04",
    "date": "$(date +%Y-%m-%d)",
    "machine": "${MACHINE}",
    "retains_structure": "yes",
    "tags": [],
    "dataset_size": ${DATASET_SIZE},
    "num_loaded_documents": ${NUM_LOADED},
    "total_size": ${DATA_SIZE},
    "data_size": ${DATA_SIZE},
    "load_time": ${LOAD_TIME},
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
