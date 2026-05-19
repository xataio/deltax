#!/bin/bash
# Run ONLY the concurrent-QPS sustained-throughput test against an already-
# loaded database. Skips install/load/cold-warm sweep entirely.
#
# Output (one line each):
#   Concurrent QPS: <N.NNN>
#   Concurrent error ratio: <0.NNN>
#
# `build-result.py --base <existing.json>` merges these into the result
# JSON produced by a prior `make bench-full`. Run from inside a per-system
# directory (e.g. ~/clickbench/pg-deltax/).
set -e

if [ ! -f ../lib/benchmark-common.sh ]; then
    echo "bench-concurrent: run from inside a per-system dir under ~/clickbench/" >&2
    exit 1
fi

export BENCH_DOWNLOAD_SCRIPT=""
export BENCH_DURABLE=yes
# shellcheck disable=SC1091
source ../lib/benchmark-common.sh

./start >/dev/null 2>&1 || true
bench_check_loop

bench_concurrent_qps

bench_stop || true
