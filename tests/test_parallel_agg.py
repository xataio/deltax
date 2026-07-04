"""Regression tests for the partial+Gather+FinalAgg DeltaXAgg path.

Hunts the plan shape that segfaulted the leader backend: an UNGROUPED
aggregate whose partial DeltaXAgg path wins the cost race (historically
only when statistics were skewed or parallel costs zeroed). Two distinct
failures lived there:

  - bare `count(*)` + matching filter: setrefs silently left the partial
    Aggref inside the Finalize Agg's transition expression, which executes
    as an EEOP_AGGREF step dereferencing NULL `ecxt_aggvalues` — leader
    SEGFAULT (signal 11, whole cluster restarts);
  - `sum(col)` + filter: "variable not found in subplan target list"
    planning error from the same tlist mismatch.

The fix gates `add_agg_partial_path` to grouped queries; these tests force
the cost conditions that made the broken path win and assert both plan
shape and results. A regression shows up either as a psycopg
OperationalError (connection killed by the segfault) or a wrong plan.
"""

from datetime import datetime, timedelta, timezone

import pytest


def _seed(db, n_partitions=3, rows_per_partition=30_000):
    db.execute(
        "CREATE TABLE pevents ("
        "  ts TIMESTAMPTZ NOT NULL,"
        "  device_id INT NOT NULL,"
        "  val INT,"
        "  note TEXT"
        ")"
    )
    db.execute(
        "SELECT deltax.deltax_create_table('pevents', 'ts', '1 day', %s)",
        (n_partitions - 1,),
    )
    # order_by leading with device_id mirrors the RTABench layout that
    # produced the original crash (`WHERE order_id = <const>` point filter).
    # segment_size 1000 matters: add_agg_partial_path only fires with >= 16
    # segments (MIN_SEGS_PER_WORKER floor) — with the default 30K segments
    # this table would have 3 segments, the partial path would never be
    # built, and the test would pass vacuously on a broken build. Verified:
    # this exact seed reproduces the leader segfault on the unfixed binary.
    db.execute(
        "SELECT deltax.deltax_enable_compression('pevents', "
        "order_by => ARRAY['device_id', 'ts'], segment_size => 1000)"
    )
    db.commit()

    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    for p in range(n_partitions):
        part_start = today - timedelta(days=1) + timedelta(days=p) + timedelta(hours=1)
        db.execute(
            "INSERT INTO pevents (ts, device_id, val, note) "
            "SELECT %s::timestamptz + (i * interval '2 second'), i %% 5000, i, 'n' || i "
            "FROM generate_series(0, %s) i",
            (part_start, rows_per_partition - 1),
        )
    db.commit()

    for (name,) in db.execute(
        "SELECT partition_name FROM deltax.deltax_partition_info('pevents') "
        "ORDER BY range_start"
    ).fetchall():
        cnt = db.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0]
        if cnt > 0:
            db.execute("SELECT deltax.deltax_compress_partition(%s)", (name,))
    db.commit()


def _force_parallel(db, leader="on", workers=2):
    """The GUC cocktail that made the broken partial path win the cost race."""
    db.execute("SET parallel_setup_cost = 0")
    db.execute("SET parallel_tuple_cost = 0")
    db.execute("SET min_parallel_table_scan_size = 0")
    db.execute(f"SET max_parallel_workers_per_gather = {workers}")
    db.execute(f"SET parallel_leader_participation = {leader}")


def _plan(db, sql):
    return "\n".join(r[0] for r in db.execute(f"EXPLAIN (FORMAT TEXT) {sql}").fetchall())


# The exact shapes that crashed or errored, plus neighbours. Each entry is
# (sql, description). Expected values are computed on the same connection
# with parallelism disabled before the forced-parallel run.
HUNT_QUERIES = [
    ("SELECT count(*) FROM pevents WHERE device_id = 333", "bare count, matching point filter (the segfault)"),
    ("SELECT count(*) FROM pevents WHERE device_id = 99999999", "bare count, empty result"),
    ("SELECT sum(val) FROM pevents WHERE device_id = 333", "bare sum (the setrefs error)"),
    ("SELECT count(*), sum(val) FROM pevents WHERE device_id = 333", "multi-agg ungrouped"),
    ("SELECT count(*) FROM pevents", "bare count, no filter"),
    ("SELECT max(val), min(val) FROM pevents WHERE device_id = 333", "min/max ungrouped"),
    ("SELECT device_id, count(*) FROM pevents WHERE device_id < 50 GROUP BY 1 ORDER BY 1", "grouped count"),
    ("SELECT device_id, count(*), sum(val) FROM pevents GROUP BY 1 ORDER BY 1 LIMIT 20", "grouped multi-agg"),
]


@pytest.mark.parametrize("leader", ["on", "off"])
def test_forced_parallel_agg_no_crash_and_correct(db, leader):
    """Run every hunt query under the cost conditions that historically
    promoted the broken partial path. A leader segfault surfaces as a lost
    connection; a planning regression as an ERROR; and any silent
    miscomputation as a result mismatch against the serial run."""
    _seed(db)

    expected = {}
    db.execute("SET max_parallel_workers_per_gather = 0")
    for sql, _desc in HUNT_QUERIES:
        expected[sql] = db.execute(sql).fetchall()

    _force_parallel(db, leader=leader)
    for sql, desc in HUNT_QUERIES:
        got = db.execute(sql).fetchall()
        assert got == expected[sql], f"{desc}: {sql}: got={got} expected={expected[sql]}"


def test_ungrouped_agg_never_takes_partial_gather_path(db):
    """The partial+Gather+FinalAgg DeltaXAgg model is only wired for grouped
    queries; ungrouped aggregates must not get a Parallel DeltaXAgg under a
    Gather even when parallelism is free."""
    _seed(db)
    _force_parallel(db)
    for sql in [
        "SELECT count(*) FROM pevents WHERE device_id = 333",
        "SELECT sum(val) FROM pevents WHERE device_id = 333",
        "SELECT count(*) FROM pevents",
    ]:
        plan = _plan(db, sql)
        assert not (
            "Gather" in plan and "Parallel Custom Scan (DeltaXAgg)" in plan
        ), f"ungrouped query took the partial DeltaXAgg path:\n{plan}"


def test_worker_starvation_zero_launched(db):
    """Workers planned but none launched (max_parallel_workers = 0) — the
    leader must produce the full, correct result alone."""
    _seed(db)
    db.execute("SET max_parallel_workers_per_gather = 0")
    expected = {sql: db.execute(sql).fetchall() for sql, _ in HUNT_QUERIES}

    _force_parallel(db)
    db.execute("SET max_parallel_workers = 0")
    for sql, desc in HUNT_QUERIES:
        got = db.execute(sql).fetchall()
        assert got == expected[sql], f"{desc} (0 workers launched): {sql}"
