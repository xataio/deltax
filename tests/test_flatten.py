"""Integration tests for pg_deltax.flatten_partitions.

When every child of a deltax parent that could hold data is compressed,
the planner hook sets `rte->inh = false` before standard_planner so
PostgreSQL never expands the partition hierarchy — DeltaXAppend is
installed directly on the un-expanded parent rel. Tables with
uncompressed data must never be flattened: they keep the regular
per-partition expansion (and its correct results).
"""

from datetime import datetime, timedelta, timezone


def _seed(db, n_partitions=3, rows_per_partition=5_000):
    db.execute(
        "CREATE TABLE fevents ("
        "  ts TIMESTAMPTZ NOT NULL,"
        "  device_id INT,"
        "  val INT"
        ")"
    )
    db.execute(
        "SELECT deltax.deltax_create_table('fevents', 'ts', '1 day', %s)",
        (n_partitions - 1,),
    )
    db.execute("SELECT deltax.deltax_enable_compression('fevents', order_by => ARRAY['ts'])")
    db.commit()

    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    for p in range(n_partitions):
        part_start = today - timedelta(days=1) + timedelta(days=p) + timedelta(hours=1)
        db.execute(
            "INSERT INTO fevents (ts, device_id, val) "
            "SELECT %s::timestamptz + (i * interval '1 second'), i %% 50, i "
            "FROM generate_series(0, %s) i",
            (part_start, rows_per_partition - 1),
        )
    db.commit()
    return today


def _compress_all(db):
    for (name,) in db.execute(
        "SELECT partition_name FROM deltax.deltax_partition_info('fevents') "
        "ORDER BY range_start"
    ).fetchall():
        cnt = db.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0]
        if cnt > 0:
            db.execute("SELECT deltax.deltax_compress_partition(%s)", (name,))
    db.commit()


def _plan(db, sql):
    rows = db.execute(f"EXPLAIN (FORMAT TEXT) {sql}").fetchall()
    return "\n".join(r[0] for r in rows)


def test_all_compressed_flattens_and_stays_exact(db):
    _seed(db)
    _compress_all(db)

    # Plan shape: a plain scan query must be a single DeltaXAppend on the
    # parent with no per-partition children expanded.
    scan_plan = _plan(db, "SELECT * FROM fevents WHERE device_id = 7 AND val < 3")
    assert "DeltaXAppend" in scan_plan, scan_plan
    assert "fevents_p" not in scan_plan, scan_plan

    sql = "SELECT count(*), sum(val), max(ts) FROM fevents WHERE device_id = 7"
    plan = _plan(db, sql)
    # Aggregates ride one of the pushdown paths on top of the flattened rel.
    assert "DeltaX" in plan, plan
    assert "fevents_p" not in plan, plan

    flat = db.execute(sql).fetchone()
    db.execute("SET pg_deltax.flatten_partitions = off")
    unflat = db.execute(sql).fetchone()
    db.execute("RESET pg_deltax.flatten_partitions")
    assert flat == unflat


def test_uncompressed_data_disables_flattening(db):
    today = _seed(db)
    _compress_all(db)

    # Insert a fresh row beyond the compressed days — it lands in an
    # empty pre-created (or default) partition, so the table now has
    # uncompressed data outside the companions.
    db.execute(
        "INSERT INTO fevents (ts, device_id, val) VALUES (%s, 7, 999999)",
        (today + timedelta(days=5, hours=2),),
    )
    db.commit()

    sql = "SELECT count(*), max(val) FROM fevents WHERE device_id = 7"
    plan = _plan(db, sql)
    # Must NOT be the flattened single-rel shape: the uncompressed row
    # has to be visible, which requires scanning the live partition.
    count, max_val = db.execute(sql).fetchone()
    assert max_val == 999999, (count, max_val, plan)

    # And the same result with flattening disabled entirely.
    db.execute("SET pg_deltax.flatten_partitions = off")
    assert db.execute(sql).fetchone() == (count, max_val)
    db.execute("RESET pg_deltax.flatten_partitions")


def test_only_keeps_empty_scan_semantics(db):
    _seed(db)
    _compress_all(db)
    # ONLY on a partitioned table scans no partitions — flattening must
    # not hijack the user's inh=false.
    assert db.execute("SELECT count(*) FROM ONLY fevents").fetchone()[0] == 0
    # A query mixing ONLY and a normal reference: the normal one still
    # returns data, the ONLY one stays empty.
    total, only_total = db.execute(
        "SELECT (SELECT count(*) FROM fevents), (SELECT count(*) FROM ONLY fevents)"
    ).fetchone()
    assert total > 0 and only_total == 0


def test_flattened_exists_subquery(db):
    _seed(db)
    _compress_all(db)
    # The walker must reach RTEs inside EXISTS sublinks (RTABench Q3/Q19/Q20 shape).
    row = db.execute(
        "SELECT EXISTS (SELECT FROM fevents WHERE device_id = 7 AND val > 0)"
    ).fetchone()
    assert row[0] is True


def test_flattened_join_and_prepared_statement(db):
    _seed(db)
    _compress_all(db)
    db.execute("CREATE TABLE devices (device_id INT PRIMARY KEY, name TEXT)")
    db.execute("INSERT INTO devices SELECT i, 'dev_' || i FROM generate_series(0, 49) i")
    db.commit()

    sql = (
        "SELECT d.name, count(*) FROM fevents f JOIN devices d USING (device_id) "
        "WHERE d.device_id = 7 GROUP BY d.name"
    )
    expected = db.execute(sql).fetchone()
    # Execute enough times to reach a generic cached plan.
    db.execute(f"PREPARE fq AS {sql}")
    for _ in range(7):
        assert db.execute("EXECUTE fq").fetchone() == expected
    db.execute("DEALLOCATE fq")
