"""Integration tests for the condition cache (see dev/docs/CONDITION_CACHE.md).

Rides the blob cache's shared memory, so the same fixture notes apply as
in test_blob_cache.py: the cache is cluster-wide and survives across
tests (assert on counter deltas, not absolutes). Each test's fresh
database yields fresh companion OIDs, so entries never alias across
tests.
"""

MOCK_NOW = "2025-01-15 12:00:00+00"
BASE_TS = "2025-01-15 00:00:00+00"


def _setup_compressed_table(conn, n_devices=20, n_points=200):
    """Partitioned + fully compressed table with numeric and text columns.

    `code` is a unique int per row (`d * 1000 + p`), so single-row DML with
    an integer-equality predicate is exactly evaluable (tombstone fast
    path); `value` is `d * 100 + p` (float8, non-unique).
    """
    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    conn.execute("""
        CREATE TABLE metrics (
            ts TIMESTAMPTZ NOT NULL,
            device_id TEXT NOT NULL,
            label TEXT NOT NULL,
            value DOUBLE PRECISION,
            code INT
        )
    """)
    conn.execute(
        "SELECT deltax.deltax_create_table('metrics', 'ts', '1 day'::interval)"
    )
    values = []
    for d in range(n_devices):
        for p in range(n_points):
            ts = f"'{BASE_TS}'::timestamptz + interval '{p} minutes'"
            values.append(
                f"({ts}, 'device-{d:03d}', "
                f"repeat(md5('{d}-{p}'), 4), {d * 100 + p}, {d * 1000 + p})"
            )
    conn.execute(f"INSERT INTO metrics VALUES {','.join(values)}")
    conn.execute(
        "SELECT deltax.deltax_enable_compression('metrics', "
        "segment_by => ARRAY[]::text[], order_by => ARRAY['ts'])"
    )
    conn.execute("""
        DO $$
        DECLARE p text;
        BEGIN
          FOR p IN SELECT partition_name
                   FROM deltax.deltax_partition_info('metrics')
                   WHERE NOT is_compressed
          LOOP
            PERFORM deltax.deltax_compress_partition(p);
          END LOOP;
        END $$;
    """)
    conn.commit()


def _cond_stats(conn):
    row = conn.execute(
        "SELECT cond_hits_total, cond_misses_total, cond_inserts_total "
        "FROM deltax.pg_deltax_blob_cache_stats()"
    ).fetchone()
    return {"hits": row[0], "misses": row[1], "inserts": row[2]}


def test_condition_cache_row_path_cold_then_warm(db):
    """A filtered plain SELECT (DeltaXAppend row path) misses cold,
    inserts, then hits warm — with identical results both times."""
    _setup_compressed_table(db)

    q = "SELECT ts, device_id, value FROM metrics WHERE value > 950"
    before = _cond_stats(db)
    first = sorted(db.execute(q).fetchall())
    after_cold = _cond_stats(db)
    assert after_cold["misses"] > before["misses"], (
        f"cold filtered scan should miss; before={before} after={after_cold}"
    )
    assert after_cold["inserts"] > before["inserts"], (
        "cold filtered scan should insert computed selections"
    )

    second = sorted(db.execute(q).fetchall())
    after_warm = _cond_stats(db)
    assert after_warm["hits"] > after_cold["hits"], (
        f"warm scan should hit; cold={after_cold} warm={after_warm}"
    )
    assert first == second
    assert len(first) > 0


def test_condition_cache_agg_text_filter_parity(db):
    """GROUP BY with a text filter (agg path): warm run hits and results
    match the cold run and a cache-off run exactly."""
    _setup_compressed_table(db)

    q = (
        "SELECT device_id, count(*), sum(value) FROM metrics "
        "WHERE device_id <> 'device-003' AND value > 100 "
        "GROUP BY device_id ORDER BY device_id"
    )
    before = _cond_stats(db)
    cold = db.execute(q).fetchall()
    after_cold = _cond_stats(db)
    warm = db.execute(q).fetchall()
    after_warm = _cond_stats(db)

    assert cold == warm
    assert after_cold["inserts"] > before["inserts"]
    assert after_warm["hits"] > after_cold["hits"]

    # Cache-off parity: identical results, counters frozen.
    db.execute("SET pg_deltax.condition_cache = off")
    off = db.execute(q).fetchall()
    after_off = _cond_stats(db)
    assert off == cold
    assert after_off["hits"] == after_warm["hits"]
    assert after_off["misses"] == after_warm["misses"]
    db.execute("RESET pg_deltax.condition_cache")


def test_condition_cache_nonepass_segments(db):
    """A filter matching no rows caches NonePass entries; the warm run
    hits them and still returns zero rows."""
    _setup_compressed_table(db)

    # High-cardinality label column defeats dict/valbitmap segment
    # pruning, so the per-row eq filter actually runs and produces
    # all-false selections.
    q = "SELECT count(*) FROM metrics WHERE label = 'no-such-label'"
    before = _cond_stats(db)
    assert db.execute(q).fetchone()[0] == 0
    after_cold = _cond_stats(db)
    assert after_cold["inserts"] > before["inserts"]

    assert db.execute(q).fetchone()[0] == 0
    after_warm = _cond_stats(db)
    assert after_warm["hits"] > after_cold["hits"]


def test_condition_cache_explain_line(db):
    """EXPLAIN ANALYZE on a warm filtered row scan surfaces the
    DeltaX Condition Cache counter line."""
    _setup_compressed_table(db)

    q = "SELECT ts, device_id, value FROM metrics WHERE value > 950"
    db.execute(q).fetchall()  # warm
    plan = "\n".join(
        r[0] for r in db.execute(f"EXPLAIN (ANALYZE, COSTS OFF) {q}").fetchall()
    )
    assert "DeltaX Condition Cache" in plan, plan


def test_condition_cache_after_delete_still_correct(db):
    """Compressed DML: a DELETE tombstones rows in-place; tombstoned
    segments bypass the cache, and results stay correct both against a
    pre-DELETE warm cache and on repeat runs."""
    _setup_compressed_table(db)

    q = "SELECT count(*), sum(value) FROM metrics WHERE value > 500"
    cold = db.execute(q).fetchone()
    warm = db.execute(q).fetchone()
    assert cold == warm

    db.execute("DELETE FROM metrics WHERE value > 1500")
    db.commit()

    expected = db.execute(
        "SELECT count(*), sum(value) FROM metrics WHERE value > 500"
    ).fetchone()
    again = db.execute(q).fetchone()
    assert again == expected
    # The deleted range is gone from the results.
    assert expected[0] < cold[0]


# ---------------------------------------------------------------------------
# DML interaction: the cache is never invalidated — every write path must
# either change the identity entries are keyed on (decompose → compaction
# recompresses under never-reused segment ids; recompress → new companion
# OID) or flip a per-segment bypass (tombstones). These tests warm the
# cache, apply each DML mechanism, and use a same-session cache-off run as
# the ground-truth oracle.
# ---------------------------------------------------------------------------


def _parity(conn, q):
    """Run `q` with the cache on, then off, in the same session; the two
    must agree exactly. Returns the (sorted) rows."""
    on = sorted(conn.execute(q).fetchall())
    conn.execute("SET pg_deltax.condition_cache = off")
    off = sorted(conn.execute(q).fetchall())
    conn.execute("RESET pg_deltax.condition_cache")
    assert on == off, f"cache-on vs cache-off divergence for: {q}"
    return on


def _compressed_partition(conn):
    return conn.execute(
        "SELECT partition_name FROM deltax.deltax_partition_info('metrics') "
        "WHERE is_compressed ORDER BY partition_name LIMIT 1"
    ).fetchone()[0]


def _dml_flags(conn, part):
    return conn.execute(
        "SELECT has_loose_rows, has_tombstones FROM deltax.deltax_partition "
        f"WHERE table_name = '{part.split('.')[-1]}'"
    ).fetchone()


def _warm(conn, q):
    """Cold run (populates) + warm run, asserting the warm run hits."""
    conn.execute(q).fetchall()
    before = _cond_stats(conn)
    conn.execute(q).fetchall()
    after = _cond_stats(conn)
    assert after["hits"] > before["hits"], (
        f"expected warm hits; before={before} after={after}"
    )


def test_condition_cache_tombstone_delete_bypasses(db):
    """Single-row DELETE takes the tombstone fast path: the segment stays
    physically intact under its cached identity, but its visible rows
    change — the per-segment tombstone bypass must keep results exact."""
    _setup_compressed_table(db)
    q = "SELECT device_id, code FROM metrics WHERE code > 9500"
    _warm(db, q)

    cur = db.execute("DELETE FROM metrics WHERE code = 10000")
    assert cur.rowcount == 1
    db.commit()
    part = _compressed_partition(db)
    loose, tombs = _dml_flags(db, part)
    assert tombs, "expected the tombstone fast path (has_tombstones)"
    assert not loose, "single-row delete must not decompose the segment"

    rows = _parity(db, q)
    assert ("device-010", 10000) not in rows


def test_condition_cache_update_decompose_then_compact(db):
    """A targeted UPDATE decomposes the touched segment (its meta row is
    deleted, rows go loose in the heap) — cached entries for it become
    unreachable. Compaction then recompresses under fresh segment ids.
    Results must stay exact at every step."""
    _setup_compressed_table(db)
    q = "SELECT device_id, code FROM metrics WHERE code > 9500"
    _warm(db, q)

    # code 9100 is outside the filter; +100000 moves it inside.
    cur = db.execute("UPDATE metrics SET code = code + 100000 WHERE code = 9100")
    assert cur.rowcount == 1
    db.commit()
    part = _compressed_partition(db)
    loose, _tombs = _dml_flags(db, part)
    assert loose, "expected decompose-on-write (has_loose_rows)"

    rows = _parity(db, q)
    assert ("device-009", 109100) in rows

    # Compaction recompresses the loose rows under never-reused segment
    # ids; the next cold run inserts fresh entries for them.
    result = db.execute(
        f"SELECT deltax.deltax_compact_partition('{part}')"
    ).fetchone()[0]
    db.commit()
    assert "Compacted" in result
    inserts_before = _cond_stats(db)["inserts"]
    rows = _parity(db, q)
    assert ("device-009", 109100) in rows
    assert _cond_stats(db)["inserts"] > inserts_before, (
        "recompacted segments should repopulate the cache under fresh ids"
    )


def test_condition_cache_insert_loose_rows(db):
    """INSERT into a compressed partition lands as loose heap rows;
    segments (and their cached bitmaps) are untouched, and the new rows
    must still appear in filtered results."""
    _setup_compressed_table(db)
    q = "SELECT device_id, code FROM metrics WHERE code > 9500"
    _warm(db, q)

    db.execute(
        f"INSERT INTO metrics VALUES ('{BASE_TS}'::timestamptz + interval "
        "'5 minutes', 'device-new', 'fresh', 1.5, 99999)"
    )
    db.commit()
    part = _compressed_partition(db)
    loose, _tombs = _dml_flags(db, part)
    assert loose, "expected the insert to land as loose heap rows"

    rows = _parity(db, q)
    assert ("device-new", 99999) in rows
