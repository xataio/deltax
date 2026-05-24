#!/bin/bash
# Run only the query phase + concurrent-QPS test of a ClickBench submission,
# against an already-loaded database. Skips install, download, and load.
#
# Methodology matches the upstream shared driver
# (lib/benchmark-common.sh): each cold try is preceded by a true cold cycle
# (./stop -> wait_stopped -> drop_caches -> ./start -> ./check), so timing
# numbers are directly comparable to submission-quality runs.
#
# Run from inside the per-system directory (e.g. ~/clickbench/pg-deltax/).
# Requires sibling ../lib/benchmark-common.sh.
#
# Output format mirrors `bash benchmark.sh` so the same parser handles both:
#   Load time: 0
#   [t1,t2,t3],            # one per query, 43 total
#   Data size: <bytes>
#   Concurrent QPS: <N.NNN>
#   Concurrent error ratio: <0.NNN>
set -e

if [ ! -f ../lib/benchmark-common.sh ]; then
    echo "queries-only: run from inside a per-system dir under ~/clickbench/" >&2
    exit 1
fi

export BENCH_DOWNLOAD_SCRIPT=""
export BENCH_DURABLE=yes
# shellcheck disable=SC1091
source ../lib/benchmark-common.sh

# Ensure the system is running and reachable. ./start is idempotent.
./start >/dev/null 2>&1 || true
bench_check_loop

# Load already happened in a previous full run; emit a placeholder so the
# parser still records a load_time field. The full bench target writes the
# real number.
echo "Load time: 0"

: > result.csv
query_num=1
while IFS= read -r query; do
    [ -z "$query" ] && continue
    bench_run_query "$query" "$query_num"
    query_num=$((query_num + 1))
done < "$BENCH_QUERIES_FILE"

echo -n "Data size: "
./data-size

bench_stop || true
