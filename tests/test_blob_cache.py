"""Integration tests for the blob cache.

Each test uses the standard `db` fixture (fresh database, default GUCs).
The cache is enabled with the auto-sized default, so these tests
exercise the live cache rather than a disabled stub.

Notes on what we don't test here:

- Cache-on vs cache-off result parity. `blob_cache_mb` is
  Postmaster-context, so toggling it per-test would require restarting
  the container — out of scope for the per-test fixture. We covered
  this manually on EC2 (37/43 ClickBench queries hash-identical, 6
  tie-breaking-different in ORDER BY ... LIMIT N queries with tied
  values). See `dev/docs/BLOB_CACHE.md#done`.
- Eviction under sustained load. Requires `blob_cache_mb` < working
  set, which again means a separate container.
"""

from datetime import datetime, timezone


MOCK_NOW = "2025-01-15 12:00:00+00"
BASE_TS = "2025-01-15 00:00:00+00"


def _setup_compressed_table(conn, n_devices=20, n_points=200):
    """Create a partitioned table with a TEXT column, populate it, and
    compress every partition. After this returns, scans of `metrics`
    will hit the deltax custom-scan path and (for the TEXT column)
    exercise the blob cache.
    """
    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    conn.execute("""
        CREATE TABLE metrics (
            ts TIMESTAMPTZ NOT NULL,
            device_id TEXT NOT NULL,
            label TEXT NOT NULL,
            value DOUBLE PRECISION
        )
    """)
    conn.execute(
        "SELECT deltax.deltax_create_table('metrics', 'ts', '1 day'::interval)"
    )

    # Two text columns ensure at least one blob per segment per partition.
    # The TEXT pattern is deliberately non-trivial so PG actually toasts it
    # (short literals stay inline and bypass the cache entirely).
    values = []
    for d in range(n_devices):
        for p in range(n_points):
            ts = f"'{BASE_TS}'::timestamptz + interval '{p} minutes'"
            values.append(
                f"({ts}, 'device-{d:03d}', "
                f"repeat(md5('{d}-{p}'), 4), {d * 100 + p})"
            )
    # Insert in one statement; the rows-per-partition split is handled by PG.
    conn.execute(f"INSERT INTO metrics VALUES {','.join(values)}")
    conn.execute(
        "SELECT deltax.deltax_enable_compression('metrics', "
        "segment_by => ARRAY[]::text[], order_by => ARRAY['ts'])"
    )
    # Compress every partition.
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


def _stats(conn):
    """Snapshot of deltax.pg_deltax_blob_cache_stats() as a dict."""
    row = conn.execute(
        "SELECT entries, bytes_used, bytes_max, hits_total, misses_total, "
        "evictions_total, insert_failures_total "
        "FROM deltax.pg_deltax_blob_cache_stats()"
    ).fetchone()
    return {
        "entries": row[0],
        "bytes_used": row[1],
        "bytes_max": row[2],
        "hits_total": row[3],
        "misses_total": row[4],
        "evictions_total": row[5],
        "insert_failures_total": row[6],
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_blob_cache_enabled_with_auto_default(db):
    """The auto-sized default (-1) resolves to a non-zero cap at
    postmaster start. bytes_max is the resolved value, not the GUC."""
    s = _stats(db)
    # 256 MiB floor at minimum; cap at 4 GiB. Either way > 0.
    assert s["bytes_max"] >= 256 * 1024 * 1024
    assert s["bytes_max"] <= 4096 * 1024 * 1024


def test_blob_cache_first_scan_populates_misses(db):
    """A cold scan of compressed data populates the cache: each blob
    accessed becomes a miss + insert. No hits expected on the first
    pass."""
    _setup_compressed_table(db)
    before = _stats(db)

    db.execute("SELECT count(*), sum(length(label)) FROM metrics").fetchone()
    db.commit()

    after = _stats(db)
    assert after["misses_total"] > before["misses_total"], (
        f"cold scan should produce misses; before={before} after={after}"
    )
    assert after["entries"] > before["entries"], (
        "cold scan should leave new entries in the cache"
    )


def test_blob_cache_warm_scan_produces_hits(db):
    """A second scan of the same data hits the cache: every blob the
    first scan inserted should be served from cache on the second."""
    _setup_compressed_table(db)

    # Warm the cache.
    db.execute("SELECT count(*), sum(length(label)) FROM metrics").fetchone()
    db.commit()
    after_first = _stats(db)

    # Second scan — every blob should now hit.
    db.execute("SELECT count(*), sum(length(label)) FROM metrics").fetchone()
    db.commit()
    after_second = _stats(db)

    hits_delta = after_second["hits_total"] - after_first["hits_total"]
    misses_delta = after_second["misses_total"] - after_first["misses_total"]
    assert hits_delta > 0, (
        f"second scan should produce hits; first={after_first} second={after_second}"
    )
    # Far more hits than misses — first scan filled in everything the
    # second scan needs.
    assert hits_delta > misses_delta, (
        f"warm scan should be mostly hits; hits={hits_delta} misses={misses_delta}"
    )


def test_blob_cache_returns_identical_results_across_scans(db):
    """Two consecutive scans return bytewise-identical results: the
    cache-hit path must return the same bytes as the freshly-detoasted
    path. Validates the BlobBytes::Cached borrow + pin lifecycle.
    """
    _setup_compressed_table(db)

    rows_first = db.execute(
        "SELECT ts, device_id, md5(label) AS lh, value "
        "FROM metrics ORDER BY ts, device_id"
    ).fetchall()
    db.commit()

    rows_second = db.execute(
        "SELECT ts, device_id, md5(label) AS lh, value "
        "FROM metrics ORDER BY ts, device_id"
    ).fetchall()
    db.commit()

    assert rows_first == rows_second, (
        "cache-hit path returned different bytes than cache-miss path"
    )
    # Sanity: we actually ran cached scans, not empty results.
    assert len(rows_first) > 0


def test_blob_cache_pins_release_between_queries(db):
    """After a query completes, no entry should be left pinned. Each
    backend's SegmentData drops at end-of-scan, releasing all the
    cached_blob_pins it accumulated.
    """
    _setup_compressed_table(db)

    # Run a scan to populate cache and acquire/release pins.
    db.execute("SELECT count(*), sum(length(label)) FROM metrics").fetchone()
    db.commit()

    # Walk all shards; every entry should have pin_count == 0.
    rows = db.execute(
        "SELECT shard_id, pinned_count, unpinned_count "
        "FROM deltax.pg_deltax_blob_cache_shard_stats() "
        "WHERE pinned_count > 0"
    ).fetchall()
    assert rows == [], (
        f"expected no pinned entries between queries; found {rows}"
    )


def test_blob_cache_explain_surfaces_hits(db):
    """EXPLAIN ANALYZE should emit the `DeltaX Blob Cache` line when
    the cache served anything. Validates the ScanTiming wiring +
    flush_timing_to_shmem aggregation.
    """
    _setup_compressed_table(db)
    # Warm the cache.
    db.execute("SELECT count(*), sum(length(label)) FROM metrics").fetchone()
    db.commit()

    # EXPLAIN ANALYZE the warm scan.
    plan_rows = db.execute(
        "EXPLAIN (ANALYZE, COSTS off, TIMING off) "
        "SELECT count(*), sum(length(label)) FROM metrics"
    ).fetchall()
    db.commit()

    plan = "\n".join(r[0] for r in plan_rows)
    assert "DeltaX Blob Cache" in plan, (
        f"expected 'DeltaX Blob Cache' line in EXPLAIN output; got:\n{plan}"
    )
    # Should report at least one hit (warm run on a previously-scanned table).
    assert "hits=0" not in plan, (
        f"warm scan reported zero hits; got:\n{plan}"
    )
