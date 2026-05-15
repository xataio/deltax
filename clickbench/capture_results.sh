#!/bin/bash
# Capture ClickBench query results for correctness verification.
#
# Runs each query in queries.sql against the target DB, dumps the result
# rows to /tmp/results/qNN.csv (proper CSV with tab delimiter, "\N" for
# NULL, double-quoting for embedded newlines/quotes — required because
# the hits dataset contains URLs/titles with arbitrary whitespace).
# On query error, writes /tmp/results/qNN.err with psql stderr.
#
# Outputs /tmp/results.tar.gz containing the whole directory for easy SCP.
#
# Usage (on the EC2 host, from ~/clickbench):
#     DB=test bash capture_results.sh
#
# DB defaults to "test" — matches benchmark.sh and reference.sh.

set -euo pipefail

DB="${DB:-test}"
OUT_DIR="${OUT_DIR:-/tmp/results}"
TARBALL="${TARBALL:-/tmp/results.tar.gz}"

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"
sudo chown -R "$(whoami):" "$OUT_DIR" 2>/dev/null || true

n=0
nf=0
while IFS= read -r query; do
    # Match ClickBench's 0-indexed query numbering (Q0..Q42).
    qid=$(printf 'q%02d' "$n")
    out_file="$OUT_DIR/${qid}.csv"
    err_file="$OUT_DIR/${qid}.err"

    # Strip trailing semicolon — COPY (...) TO can't wrap a statement.
    trimmed_query="$(echo "$query" | sed -E 's/[[:space:]]*;[[:space:]]*$//')"

    # ON_ERROR_STOP=on so we detect PG errors via exit code.
    # COPY (...) TO STDOUT with FORMAT csv handles embedded tabs/newlines
    # via quoting; NULL '\N' makes nulls unambiguous on the consumer side.
    if ! sudo -u postgres psql -v ON_ERROR_STOP=on -d "$DB" \
            -c "COPY ($trimmed_query) TO STDOUT WITH (FORMAT csv, DELIMITER E'\t', NULL '\\N')" \
            > "$out_file" 2> "$err_file"; then
        mv "$out_file" "${out_file}.partial" 2>/dev/null || true
        echo "  $qid: QUERY_ERROR ($(head -n1 "$err_file" 2>/dev/null || echo '?'))"
        nf=$((nf + 1))
    else
        rm -f "$err_file"
        # Row count is approximate (lines, not CSV records) — only for logging.
        rows=$(wc -l < "$out_file" | tr -d ' ')
        echo "  $qid: $rows lines"
    fi
    n=$((n + 1))
done < queries.sql

tar -czf "$TARBALL" -C "$(dirname "$OUT_DIR")" "$(basename "$OUT_DIR")"
echo "Captured $n queries ($nf errored). Tarball: $TARBALL"
