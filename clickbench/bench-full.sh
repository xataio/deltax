#!/bin/bash
# Full ClickBench flow EXCEPT the concurrent-QPS test. The lib's bench_main
# always runs the concurrent test (it has no skip flag), so we drive the
# phases ourselves: install -> start -> download -> load -> 43 queries ->
# data-size -> stop. The dashboard doesn't render concurrent metrics yet,
# so paying ~10 min for them on every full run isn't worth it — use
# `make bench-concurrent` separately to record them.
#
# Run from inside a per-system directory (e.g. ~/clickbench/pg-deltax/).
# Requires sibling ../lib/benchmark-common.sh.
set -e

if [ ! -f ../lib/benchmark-common.sh ]; then
    echo "bench-full: run from inside a per-system dir under ~/clickbench/" >&2
    exit 1
fi

export BENCH_DOWNLOAD_SCRIPT="${BENCH_DOWNLOAD_SCRIPT:-download-hits-parquet-partitioned}"
export BENCH_DURABLE="${BENCH_DURABLE:-yes}"
# shellcheck disable=SC1091
source ../lib/benchmark-common.sh

bench_install
bench_start

bench_download
bench_load

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

if [ "$BENCH_DURABLE" != "yes" ]; then
    rm -f hits.parquet hits_*.parquet hits.tsv hits.tsv.gz hits.json.gz
    sync
fi
