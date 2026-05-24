"""Regression test for the README quickstart.

Runs every SQL statement from the `### Quickstart` block in `README.md`
in order, using real `now()` (no `pg_deltax.mock_now`), and asserts each
step succeeds and produces sensible output. The point is to catch the
class of breakage Eric Ridge hit in xataio/deltax#13 — where a hardcoded
date in the docs only worked on one specific day — and to keep the
documented walkthrough honest on every PR.

Keep assertions loose enough that the test survives small wording or
shape changes in the quickstart; tight enough that an actual failure
(missing function, schema error, zero partitions compressed) trips it.
"""


class TestReadmeQuickstart:
    def test_quickstart_runs_end_to_end_on_any_day(self, db):
        # 1. Create the example table.
        db.execute(
            "CREATE TABLE metrics ("
            "ts TIMESTAMPTZ NOT NULL, device TEXT, value FLOAT8)"
        )
        db.execute("SELECT deltax.deltax_create_table('metrics', 'ts', '1 day')")
        db.commit()

        # 2. Insert ~100,000 rows spanning the last ~2.3 days across 5 devices,
        # matching the README. Past partition needs enough data that the
        # per-partition companion-table overhead is dwarfed by the compression
        # savings.
        db.execute("""
            INSERT INTO metrics (ts, device, value)
            SELECT
                now() - (i * interval '2 seconds'),
                'sensor-' || (i % 5),
                20.0 + sin(i::float / 100) * 5
            FROM generate_series(0, 100000) AS i
        """)
        db.commit()

        # 2b. Drain the default partition synchronously so all inserted rows
        # are routed to proper time-aligned partitions before compression.
        # Without this call, ~35K rows from >1 day ago would sit in
        # metrics_default (waiting on the background worker's 60s cycle)
        # and be invisible to deltax_compress_all_partitions.
        drain_msg = db.execute(
            "SELECT deltax.deltax_drain_default_partition('metrics')"
        ).fetchone()[0]
        db.commit()
        assert isinstance(drain_msg, str) and drain_msg, drain_msg
        # The 100K rows span ~2.3 days but only 1 past partition is premade,
        # so we expect a non-trivial drain on a typical wall-clock time.
        assert "Drained" in drain_msg, (
            f"expected the drain to move rows, got: {drain_msg!r}"
        )

        # Snapshot pg_database_size before compression so we can verify the
        # compression step is a net win (companion-table overhead < savings).
        db_size_before = db.execute(
            "SELECT pg_database_size(current_database())"
        ).fetchone()[0]

        # 3. Demo queries — every one must return rows.
        daily = db.execute(
            "SELECT deltax.time_bucket('1 day', ts) AS day, avg(value) "
            "FROM metrics GROUP BY 1 ORDER BY 1"
        ).fetchall()
        assert daily, "time_bucket aggregate returned no rows"

        first_last = db.execute(
            "SELECT deltax.first(value, ts), deltax.last(value, ts) FROM metrics"
        ).fetchone()
        assert first_last is not None and first_last[0] is not None
        assert first_last[1] is not None

        partitions = db.execute(
            "SELECT partition_name, is_compressed "
            "FROM deltax.deltax_partition_info('metrics') "
            "ORDER BY range_start"
        ).fetchall()
        # deltax_create_table defaults: 1 past + today + 3 future = 5 entries.
        assert len(partitions) >= 2, (
            f"expected at least past + today partition, got {partitions}"
        )
        assert all(not row[1] for row in partitions), (
            f"no partition should be compressed yet, got {partitions}"
        )

        # 4. Enable compression — message format may evolve; just require
        # the call returns a non-empty string and doesn't raise.
        enable_msg = db.execute(
            "SELECT deltax.deltax_enable_compression("
            "'metrics', order_by => ARRAY['device', 'ts'])"
        ).fetchone()[0]
        assert isinstance(enable_msg, str) and enable_msg
        db.commit()

        # 5. Compress every sealed partition. After the drain step the data
        # spans yesterday + at least one earlier past partition that the
        # drain just created, so we expect ≥ 2 result rows. Today's
        # still-open partition must not appear.
        compressed = db.execute(
            "SELECT partition_name, result "
            "FROM deltax.deltax_compress_all_partitions('metrics')"
        ).fetchall()
        db.commit()
        assert len(compressed) >= 2, (
            f"deltax_compress_all_partitions returned {len(compressed)} row(s); "
            f"expected ≥ 2 sealed partitions after drain; got {compressed}"
        )

        # The current-day partition has range_end > now() and so is not
        # eligible. Make sure none of the returned partitions overlap now().
        for part_name, _msg in compressed:
            row = db.execute(
                "SELECT range_end <= now() "
                "FROM deltax.deltax_partition_info('metrics') "
                "WHERE partition_name = %s",
                (part_name,),
            ).fetchone()
            assert row is not None and row[0] is True, (
                f"partition {part_name} should have range_end <= now()"
            )

        # 6. After compression, at least one partition must show up
        # compressed in the stats view with a real row count.
        stats = db.execute(
            "SELECT partition_name, is_compressed, row_count, "
            "       raw_size, compressed_size "
            "FROM deltax.deltax_compression_stats('metrics')"
        ).fetchall()
        compressed_rows = [r for r in stats if r[1] is True]
        assert compressed_rows, (
            f"no partition is compressed after the compress step; stats={stats}"
        )
        for _name, _is_c, row_count, raw, comp in compressed_rows:
            assert (row_count or 0) > 0, (
                f"compressed partition has zero rows: {_name}"
            )
            assert (raw or 0) > 0 and (comp or 0) > 0, (
                f"compressed partition has zero sizes: {_name}"
            )

        # 7. Size reporting — pg_size_pretty must accept and format the value.
        size_text = db.execute(
            "SELECT pg_size_pretty(deltax.deltax_table_size('metrics'))"
        ).fetchone()[0]
        assert isinstance(size_text, str) and size_text

        # 8. Database-size regression: at the quickstart's data scale,
        # compression must be a *net* win — companion-table overhead must
        # not exceed the savings. Catches the regression Tudor hit where
        # `pg_database_size` grew despite `deltax_table_size` showing a
        # small post-compression number.
        db_size_after = db.execute(
            "SELECT pg_database_size(current_database())"
        ).fetchone()[0]
        assert db_size_after < db_size_before, (
            f"compression grew the database: "
            f"before={db_size_before} bytes, after={db_size_after} bytes"
        )
