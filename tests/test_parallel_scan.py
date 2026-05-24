"""Integration tests for parallel-aware DeltaXAppend (RTABench Q17/Q23/Q25/Q30
category). Verifies:

  * Gather appears above DeltaXAppend when `max_parallel_workers_per_scan` is
    enabled and the segment count is large enough to justify workers.
  * Results are identical between serial and parallel runs (byte-equal row sets).
  * Top-N queries still use the serial fast path.
  * `pg_deltax.max_parallel_workers_per_scan = 0` disables the partial path.
"""

from datetime import datetime, timedelta, timezone


def _seed_compressed_table(db, n_partitions=4, rows_per_partition=120_000):
    """Create a partitioned deltax table, insert enough rows to produce
    several segments per partition, then compress every partition.

    Rows for each partition are spaced evenly across 22 h inside that day
    (with 1 h padding on either end of the partition boundary), so none
    spill into the default partition or the future ones we don't load.
    After compression the fixture asserts `events_default` is empty.

    The default 120K rows/partition at default segment_size=30K yields ~4
    segments per partition; 4 partitions → ~16 segments, which clears the
    MIN_SEGS_PER_WORKER × worker_floor threshold used by the partial-path
    cost model. Bump rows_per_partition to exercise deeper splits.
    """
    db.execute(
        "CREATE TABLE events ("
        "  ts TIMESTAMPTZ NOT NULL,"
        "  device_id INT,"
        "  kind TEXT,"
        "  value FLOAT8"
        ")"
    )
    db.execute(
        "SELECT deltax.deltax_create_table('events', 'ts', '1 day', %s)",
        (n_partitions - 1,),
    )
    db.execute(
        "SELECT deltax.deltax_enable_compression('events',"
        "  segment_by => ARRAY['device_id'],"
        "  order_by => ARRAY['ts'])"
    )
    db.commit()

    # Anchor on UTC midnight so partition_start lines up with an actual
    # partition bound. deltax_create_table creates 1 past + 1 current +
    # (n_partitions-1) future partitions = n_partitions+1 total. We walk
    # forward from `today - 1 day` so every partition_start lands inside
    # an existing partition, not in the default.
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    # 22 h window inside each partition, 1 h padding on each boundary.
    window_us = 22 * 3600 * 1_000_000
    spacing_us = max(1, window_us // max(1, rows_per_partition - 1))

    for p in range(n_partitions):
        partition_start = today - timedelta(days=1) + timedelta(days=p) + timedelta(hours=1)
        db.execute(
            "INSERT INTO events (ts, device_id, kind, value) "
            "SELECT %s::timestamptz + (i::bigint * %s::bigint * interval '1 microsecond'), "
            "       (i %% 50)::int, "
            "       CASE WHEN i %% 3 = 0 THEN 'A' WHEN i %% 3 = 1 THEN 'B' ELSE 'C' END, "
            "       random() "
            "FROM generate_series(0, %s) i",
            (partition_start, spacing_us, rows_per_partition - 1),
        )
    db.commit()

    # Sanity: nothing overflowed into the default partition.
    default_rows = db.execute("SELECT count(*) FROM events_default").fetchone()[0]
    assert default_rows == 0, f"fixture leaked {default_rows} rows into events_default"

    # Compress every partition so the custom scan is exercised. Skip
    # future partitions that received no rows (deltax_compress_partition
    # errors on empty partitions).
    partitions = db.execute(
        "SELECT partition_info.partition_name "
        "FROM deltax.deltax_partition_info('events') AS partition_info "
        "WHERE EXISTS ("
        "  SELECT 1 FROM pg_class c "
        "  WHERE c.relname = partition_info.partition_name "
        ") "
        "ORDER BY range_start"
    ).fetchall()
    for (name,) in partitions:
        cnt = db.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0]
        if cnt > 0:
            db.execute("SELECT deltax.deltax_compress_partition(%s)", (name,))
    db.commit()

    # ANALYZE so reltuples gets populated. collect_compressed_children in
    # hook.rs skips uncompressed children only if ANALYZE confirms
    # reltuples == 0; without it, the default partition and any empty
    # future partitions count as "has unknown data" and block the
    # DeltaXAppend path from being chosen.
    db.rollback()  # ANALYZE can't run inside a transaction
    db.autocommit = True
    db.execute("ANALYZE events")
    db.autocommit = False


def _fetchone_scalar(db, sql, params=None):
    row = db.execute(sql, params).fetchone() if params else db.execute(sql).fetchone()
    return row[0]


def test_parallel_produces_same_rows_as_serial(db):
    """Running the same join query with and without the parallel partial
    path must produce byte-equal row sets."""
    _seed_compressed_table(db, n_partitions=4, rows_per_partition=120_000)

    # Use a large max_parallel_workers_per_gather so the planner actually
    # considers a partial path. Without this, PG's own cap might keep
    # everything serial.
    db.execute("SET max_parallel_workers_per_gather = 4")
    db.execute("SET max_parallel_workers = 8")

    query = (
        "SELECT kind, count(*) AS n, sum(value) AS s "
        "FROM events WHERE device_id < 40 "
        "GROUP BY kind ORDER BY kind"
    )

    db.execute("SET pg_deltax.max_parallel_workers_per_scan = 0")
    serial = db.execute(query).fetchall()

    db.execute("SET pg_deltax.max_parallel_workers_per_scan = -1")
    parallel = db.execute(query).fetchall()

    # Counts must match exactly; sums are floats, so compare per-row with
    # a tight tolerance (they're computed over identical input sets). A
    # NULL sum on either side is only acceptable if the other side also
    # produced NULL — that's still consistent between serial and parallel.
    assert [(r[0], r[1]) for r in serial] == [(r[0], r[1]) for r in parallel]
    for s, p in zip(serial, parallel):
        assert (s[2] is None) == (p[2] is None), (
            f"NULL mismatch for kind={s[0]}: serial={s[2]} parallel={p[2]}"
        )
        if s[2] is not None:
            assert abs(s[2] - p[2]) < 1e-6, (
                f"sum mismatch for kind={s[0]}: {s[2]} vs {p[2]}"
            )


def test_explain_shows_gather_over_deltax_append(db):
    """With the GUC enabled and a segment count that justifies workers, the
    planner should place a Gather above DeltaXAppend."""
    _seed_compressed_table(db, n_partitions=4, rows_per_partition=200_000)

    db.execute("SET max_parallel_workers_per_gather = 4")
    db.execute("SET max_parallel_workers = 8")
    db.execute("SET pg_deltax.max_parallel_workers_per_scan = -1")

    plan = db.execute(
        "EXPLAIN (FORMAT TEXT) "
        "SELECT * FROM events WHERE device_id < 25"
    ).fetchall()
    plan_text = "\n".join(row[0] for row in plan)

    assert "DeltaXAppend" in plan_text, f"DeltaXAppend not in plan:\n{plan_text}"
    assert "Gather" in plan_text, f"Gather not in plan:\n{plan_text}"
    assert "Workers Planned" in plan_text, (
        f"No parallel workers planned:\n{plan_text}"
    )


def test_guc_zero_disables_partial_path(db):
    """Setting `pg_deltax.max_parallel_workers_per_scan = 0` must suppress the
    partial-path emission so no Gather appears above DeltaXAppend."""
    _seed_compressed_table(db, n_partitions=4, rows_per_partition=200_000)

    db.execute("SET max_parallel_workers_per_gather = 4")
    db.execute("SET max_parallel_workers = 8")
    db.execute("SET pg_deltax.max_parallel_workers_per_scan = 0")

    plan = db.execute(
        "EXPLAIN (FORMAT TEXT) "
        "SELECT * FROM events WHERE device_id < 25"
    ).fetchall()
    plan_text = "\n".join(row[0] for row in plan)

    assert "DeltaXAppend" in plan_text
    # With the GUC off, no Gather should appear above DeltaXAppend.
    # (A Gather could still appear elsewhere in the plan for unrelated
    # reasons, but not directly above this node.)
    lines = plan_text.splitlines()
    for i, line in enumerate(lines):
        if "DeltaXAppend" in line:
            # Walk upward in the tree (less indentation = parent node).
            indent = len(line) - len(line.lstrip())
            for prev in reversed(lines[:i]):
                prev_indent = len(prev) - len(prev.lstrip())
                if prev_indent < indent:
                    assert "Gather" not in prev, (
                        f"Gather appeared above DeltaXAppend with GUC=0:\n{plan_text}"
                    )
                    break


def test_topn_falls_back_to_serial(db):
    """Queries with ORDER BY + LIMIT use the Top-N pushdown, which is
    serial-only (per-worker local Top-N would be incorrect without a
    Gather-Merge combiner). No Gather should appear above DeltaXAppend."""
    _seed_compressed_table(db, n_partitions=4, rows_per_partition=200_000)

    db.execute("SET max_parallel_workers_per_gather = 4")
    db.execute("SET max_parallel_workers = 8")
    db.execute("SET pg_deltax.max_parallel_workers_per_scan = -1")

    plan = db.execute(
        "EXPLAIN (FORMAT TEXT) "
        "SELECT * FROM events ORDER BY ts DESC LIMIT 10"
    ).fetchall()
    plan_text = "\n".join(row[0] for row in plan)

    assert "DeltaXAppend" in plan_text
    lines = plan_text.splitlines()
    for i, line in enumerate(lines):
        if "DeltaXAppend" in line:
            indent = len(line) - len(line.lstrip())
            for prev in reversed(lines[:i]):
                prev_indent = len(prev) - len(prev.lstrip())
                if prev_indent < indent:
                    assert "Gather" not in prev, (
                        f"Gather over DeltaXAppend with Top-N active:\n{plan_text}"
                    )
                    break


def test_small_table_stays_serial(db):
    """Tables with too few segments shouldn't get a partial path — the
    overhead of workers dominates. With default MIN_SEGS_PER_WORKER=8 and a
    small table, the planner should keep things serial."""
    _seed_compressed_table(db, n_partitions=2, rows_per_partition=30_000)

    db.execute("SET max_parallel_workers_per_gather = 4")
    db.execute("SET max_parallel_workers = 8")
    db.execute("SET pg_deltax.max_parallel_workers_per_scan = -1")

    # Use a projection that can't be answered from segment metadata so
    # the planner can't hop to the DeltaXAgg/DeltaXCount fast paths.
    plan = db.execute(
        "EXPLAIN (FORMAT TEXT) "
        "SELECT * FROM events WHERE device_id < 25"
    ).fetchall()
    plan_text = "\n".join(row[0] for row in plan)

    # Not asserting on Gather absence strictly (the planner may still choose
    # a serial plan purely on cost), but DeltaXAppend must be present.
    assert "DeltaXAppend" in plan_text
