#!/bin/bash

TRIES=3

cat queries.sql | while read -r query; do
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null

    echo "$query";
    for i in $(seq 1 $TRIES); do
        # ON_ERROR_STOP=on so psql exits non-zero on PG ERROR (e.g. OOM).
        # Without it, `\timing` still prints "Time: X.XX ms" (time-to-error)
        # and the parser would accept failed queries as fast successes.
        out=$(sudo -u postgres psql -v ON_ERROR_STOP=on test -t -c '\timing' -c "$query" 2>&1)
        if [ $? -ne 0 ]; then
            echo "QUERY_ERROR"
        else
            echo "$out" | grep -P 'Time' | tail -n1
        fi
    done;
done;
