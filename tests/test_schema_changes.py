"""Integration tests for Session 1 of the schema-changes work:
descriptor catalog + scan-path generalization for ADD COLUMN on already-
compressed partitions. See `dev/docs/SCHEMA_CHANGES.md`.

Session 1 lands the read-side machinery. ALTER interception (Session 2) and
DROP COLUMN tombstones (Session 3) are not yet wired up, so these tests
only exercise pass-through ADD COLUMN behavior.
"""

import json
import pytest

MOCK_NOW = "2025-01-15 12:00:00+00"
BASE_TS = "2025-01-15 00:00:00+00"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def setup_and_compress(conn, n_devices=3, n_points=20):
    """Create a `metrics` deltatable, populate it, enable compression,
    compress a single partition that contains all the inserted rows.
    Returns (partition_name, row_count)."""
    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    conn.execute("""
        CREATE TABLE metrics (
            ts TIMESTAMPTZ NOT NULL,
            device_id TEXT NOT NULL,
            temperature DOUBLE PRECISION,
            pressure DOUBLE PRECISION
        )
    """)
    conn.execute("SELECT deltax_create_table('metrics', 'ts', '1 day'::interval)")

    rows = []
    for d in range(n_devices):
        for p in range(n_points):
            ts = f"'{BASE_TS}'::timestamptz + interval '{p} minutes'"
            rows.append(f"({ts}, 'd-{d}', {20.0 + d}, {1013.0 + p})")
    conn.execute(
        "INSERT INTO metrics (ts, device_id, temperature, pressure) VALUES "
        + ", ".join(rows)
    )

    conn.execute(
        "SELECT deltax_enable_compression('metrics', "
        "segment_by => ARRAY['device_id'], "
        "order_by => ARRAY['ts'])"
    )
    conn.commit()

    partitions = conn.execute(
        "SELECT partition_name FROM deltax_partition_info('metrics') "
        "WHERE range_start <= '2025-01-15'::timestamptz "
        "AND range_end > '2025-01-15'::timestamptz"
    ).fetchall()
    assert len(partitions) == 1, partitions
    part_name = partitions[0][0]

    conn.execute(f"SELECT deltax_compress_partition('{part_name}')")
    conn.commit()

    return part_name, n_devices * n_points


def get_descriptor(conn, part_name):
    """Return the `compressed_columns` descriptor (Python list) for a partition."""
    raw = conn.execute(
        "SELECT compressed_columns FROM deltax_partition WHERE table_name = %s",
        (part_name,),
    ).fetchone()[0]
    return raw  # psycopg returns JSONB as parsed Python


# ---------------------------------------------------------------------------
# Descriptor shape
# ---------------------------------------------------------------------------

class TestDescriptorShape:
    def test_descriptor_populated_on_compression(self, db):
        part_name, _ = setup_and_compress(db)

        desc = get_descriptor(db, part_name)
        assert isinstance(desc, list), desc
        # ts, device_id, temperature, pressure → 4 entries
        assert len(desc) == 4
        by_name = {e["name"]: e for e in desc}

        # Common shape invariants
        for entry in desc:
            assert set(entry.keys()) == {
                "attnum", "name", "type_oid", "typmod",
                "is_segment_by", "compressed_col_idx", "dropped",
            }
            assert entry["dropped"] is False

        # segment_by gets compressed_col_idx = null
        assert by_name["device_id"]["is_segment_by"] is True
        assert by_name["device_id"]["compressed_col_idx"] is None

        # Non-segment_by are numbered 0,1,2,... in attnum order.
        # ts attnum 1, temperature attnum 3, pressure attnum 4 → indices 0,1,2.
        assert by_name["ts"]["is_segment_by"] is False
        assert by_name["ts"]["compressed_col_idx"] == 0
        assert by_name["temperature"]["compressed_col_idx"] == 1
        assert by_name["pressure"]["compressed_col_idx"] == 2

        # type_oid is the numeric pg_type OID (timestamptz=1184, float8=701)
        assert by_name["ts"]["type_oid"] == 1184
        assert by_name["temperature"]["type_oid"] == 701


# ---------------------------------------------------------------------------
# ADD COLUMN — transparent reads on already-compressed partitions
# ---------------------------------------------------------------------------

class TestAddColumnTransparent:
    def test_add_column_nullable_returns_null(self, db):
        part_name, total = setup_and_compress(db)

        # ALTER passes straight through to PG today (no hook yet).
        db.execute("ALTER TABLE metrics ADD COLUMN note TEXT")
        db.commit()

        rows = db.execute(f'SELECT note FROM "{part_name}"').fetchall()
        assert len(rows) == total
        assert all(r[0] is None for r in rows)

    def test_add_column_default_integer(self, db):
        part_name, total = setup_and_compress(db)

        db.execute("ALTER TABLE metrics ADD COLUMN sensor_id INT DEFAULT 42")
        db.commit()

        rows = db.execute(f'SELECT sensor_id FROM "{part_name}"').fetchall()
        assert len(rows) == total
        # PG populates attmissingval; getmissingattr returns 42 for existing rows.
        assert all(r[0] == 42 for r in rows), {r[0] for r in rows}

    def test_add_column_not_null_default(self, db):
        part_name, total = setup_and_compress(db)

        db.execute(
            "ALTER TABLE metrics ADD COLUMN rev INT NOT NULL DEFAULT 7"
        )
        db.commit()

        rows = db.execute(f'SELECT rev FROM "{part_name}"').fetchall()
        assert len(rows) == total
        assert all(r[0] == 7 for r in rows)

    @pytest.mark.xfail(
        reason="Session 1 limitation: filter pushdown on a column added after "
        "compression routes through the aggregate compact/mixed paths which "
        "still build their blob_idx mapping positionally. Will be fixed once "
        "those paths are taught to consult MetadataInfo.blob_idx and "
        "synthesize missing values — Session 2 follow-up.",
        strict=True,
    )
    def test_filter_on_added_column(self, db):
        part_name, total = setup_and_compress(db)

        db.execute("ALTER TABLE metrics ADD COLUMN bucket INT DEFAULT 5")
        db.commit()

        # Equality matches all rows.
        matches = db.execute(
            f'SELECT count(*) FROM "{part_name}" WHERE bucket = 5'
        ).fetchone()[0]
        assert matches == total

        # Non-matching value returns no rows.
        miss = db.execute(
            f'SELECT count(*) FROM "{part_name}" WHERE bucket = 999'
        ).fetchone()[0]
        assert miss == 0

    def test_select_existing_columns_unchanged_after_add(self, db):
        """ADD COLUMN must not perturb reads of pre-existing columns."""
        part_name, total = setup_and_compress(db)

        before = db.execute(
            f'SELECT device_id, temperature, pressure FROM "{part_name}" '
            f"ORDER BY ts, device_id"
        ).fetchall()

        db.execute("ALTER TABLE metrics ADD COLUMN extra INT DEFAULT 1")
        db.commit()

        after = db.execute(
            f'SELECT device_id, temperature, pressure FROM "{part_name}" '
            f"ORDER BY ts, device_id"
        ).fetchall()

        assert before == after
        assert len(before) == total


# ---------------------------------------------------------------------------
# Legacy partition fallback
# ---------------------------------------------------------------------------

class TestLegacyPartitionFallback:
    def test_null_descriptor_uses_positional_mapping(self, db):
        """Simulate a partition compressed before `compressed_columns`
        existed by clearing the descriptor. Reads must still work via the
        legacy positional mapping that load_metadata falls back to."""
        part_name, total = setup_and_compress(db)

        db.execute(
            "UPDATE deltax_partition SET compressed_columns = NULL "
            "WHERE table_name = %s",
            (part_name,),
        )
        db.commit()

        # Existing columns still readable; row count unchanged.
        rows = db.execute(
            f'SELECT count(*), avg(temperature) FROM "{part_name}"'
        ).fetchone()
        assert rows[0] == total
        assert rows[1] is not None  # all rows have a temperature value
