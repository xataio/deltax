"""Integration tests for the schema-changes work.
See `dev/docs/SCHEMA_CHANGES.md`.
"""

import psycopg
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
    conn.execute(
        "SELECT deltax.deltax_create_table('metrics', 'ts', '1 day'::interval)"
    )

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
        "SELECT deltax.deltax_enable_compression('metrics', "
        "segment_by => ARRAY['device_id'], "
        "order_by => ARRAY['ts'])"
    )
    conn.commit()

    partitions = conn.execute(
        "SELECT partition_name FROM deltax.deltax_partition_info('metrics') "
        "WHERE range_start <= '2025-01-15'::timestamptz "
        "AND range_end > '2025-01-15'::timestamptz"
    ).fetchall()
    assert len(partitions) == 1, partitions
    part_name = partitions[0][0]

    conn.execute(f"SELECT deltax.deltax_compress_partition('{part_name}')")
    conn.commit()

    return part_name, n_devices * n_points


def get_descriptor(conn, part_name):
    """Return the `compressed_columns` descriptor (Python list) for a partition."""
    raw = conn.execute(
        "SELECT compressed_columns FROM deltax.deltax_partition WHERE table_name = %s",
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
                "attnum",
                "name",
                "type_oid",
                "typmod",
                "is_segment_by",
                "compressed_col_idx",
                "dropped",
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

        # Classified as Tier 1 pass-through by ddl::classify_add_column
        # (no identity / generated / NOT NULL / default), so the hook
        # chains to standard_ProcessUtility unchanged.
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

        db.execute("ALTER TABLE metrics ADD COLUMN rev INT NOT NULL DEFAULT 7")
        db.commit()

        rows = db.execute(f'SELECT rev FROM "{part_name}"').fetchall()
        assert len(rows) == total
        assert all(r[0] == 7 for r in rows)

    @pytest.mark.xfail(
        reason="Known limitation: filter pushdown on a column added after "
        "compression routes through the aggregate compact/mixed paths "
        "(scan/exec/agg/), which still derive `_col_idx` positionally "
        "from `col_names` + `segment_by` instead of consulting "
        "MetadataInfo.blob_idx. Fix: thread `&meta.blob_idx` and "
        "`&meta.missing_values` through Parallel*Config / Serial* — "
        "same shape as the basic decompress path. Tracked in "
        "dev/docs/SCHEMA_CHANGES.md Future Work.",
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
            "UPDATE deltax.deltax_partition SET compressed_columns = NULL "
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


# ---------------------------------------------------------------------------
# Tier 1 — transparent ALTERs (with optional catalog bookkeeping)
# ---------------------------------------------------------------------------


def get_deltatable_row(conn):
    """Return the single deltatable row's (id, schema_name, table_name,
    time_column, segment_by, order_by)."""
    return conn.execute(
        "SELECT id, schema_name, table_name, time_column, segment_by, order_by "
        "FROM deltax.deltax_deltatable WHERE table_name = 'metrics'"
    ).fetchone()


class TestTier1Transparent:
    def test_rename_column_non_key_updates_catalog(self, db):
        """RENAME COLUMN on a non-key column passes through, updates
        JSONB keys in column_ndistinct / column_valmap / compressed_columns,
        and SELECT under the new name returns expected rows."""
        part_name, total = setup_and_compress(db)

        # Force column_ndistinct / column_valmap to be populated by
        # picking a column the compression path tracks. `temperature` is
        # numeric → column_ndistinct gets an entry.
        db.execute("ALTER TABLE metrics RENAME COLUMN temperature TO temp_c")
        db.commit()

        # Catalog: column_ndistinct key was rewritten.
        ndistinct = db.execute(
            "SELECT column_ndistinct FROM deltax.deltax_partition "
            "WHERE table_name = %s",
            (part_name,),
        ).fetchone()[0]
        assert isinstance(ndistinct, dict)
        assert "temperature" not in ndistinct
        assert "temp_c" in ndistinct

        # Catalog: compressed_columns descriptor's `name` field was rewritten.
        desc = get_descriptor(db, part_name)
        names = {e["name"] for e in desc}
        assert "temperature" not in names
        assert "temp_c" in names

        # SELECT under the new name returns the expected row count and a
        # non-null average.
        row = db.execute(f'SELECT count(*), avg(temp_c) FROM "{part_name}"').fetchone()
        assert row[0] == total
        assert row[1] is not None

    def test_rename_column_segment_by_is_tier3(self, db):
        setup_and_compress(db)
        with pytest.raises(psycopg.errors.FeatureNotSupported):
            db.execute("ALTER TABLE metrics RENAME COLUMN device_id TO dev")
        db.rollback()

    def test_rename_column_time_column_is_tier3(self, db):
        setup_and_compress(db)
        with pytest.raises(psycopg.errors.FeatureNotSupported):
            db.execute("ALTER TABLE metrics RENAME COLUMN ts TO event_ts")
        db.rollback()

    def test_rename_table_updates_catalog(self, db):
        part_name, total = setup_and_compress(db)

        db.execute("ALTER TABLE metrics RENAME TO metrics2")
        db.commit()

        row = get_deltatable_row(db)
        assert row is None
        row = db.execute(
            "SELECT table_name FROM deltax.deltax_deltatable WHERE table_name = 'metrics2'"
        ).fetchone()
        assert row is not None

        # Partition is still queryable through the parent.
        count = db.execute("SELECT count(*) FROM metrics2").fetchone()[0]
        assert count == total

    def test_set_schema_updates_catalog(self, db):
        """SET SCHEMA on a partitioned parent moves only the parent —
        partitions stay in their original schema. Our catalog mirrors PG's
        behavior: deltatable.schema_name updates, partition.schema_name
        rows are left as-is."""
        part_name, total = setup_and_compress(db)
        db.execute("CREATE SCHEMA other")
        db.execute("ALTER TABLE metrics SET SCHEMA other")
        db.commit()

        ht_schema = db.execute(
            "SELECT schema_name FROM deltax.deltax_deltatable WHERE table_name = 'metrics'"
        ).fetchone()[0]
        assert ht_schema == "other"

        # Partition stayed put (PG behavior, mirrored in our catalog).
        part_schema = db.execute(
            "SELECT schema_name FROM deltax.deltax_partition WHERE table_name = %s",
            (part_name,),
        ).fetchone()[0]
        assert part_schema == "public"

        # Reads still work through the moved parent.
        count = db.execute("SELECT count(*) FROM other.metrics").fetchone()[0]
        assert count == total

    def test_drop_default_passes_through(self, db):
        setup_and_compress(db)
        db.execute("ALTER TABLE metrics ALTER COLUMN temperature DROP DEFAULT")
        db.commit()

    def test_drop_constraint_passes_through(self, db):
        setup_and_compress(db)
        # Add then drop a CHECK constraint with NOT VALID (so the ADD is
        # itself Tier 1 — see TestTier3Blocking for the validating form).
        db.execute(
            "ALTER TABLE metrics ADD CONSTRAINT pressure_positive "
            "CHECK (pressure > 0) NOT VALID"
        )
        db.execute("ALTER TABLE metrics DROP CONSTRAINT pressure_positive")
        db.commit()

    def test_add_constraint_not_valid_passes_through(self, db):
        setup_and_compress(db)
        db.execute(
            "ALTER TABLE metrics ADD CONSTRAINT pressure_positive "
            "CHECK (pressure > 0) NOT VALID"
        )
        db.commit()

    def test_disable_dml_trigger_rejected(self, db):
        """Block ALTER TABLE ... DISABLE TRIGGER deltax_reject_compressed_dml
        — that's the trigger that protects compressed partitions from
        rogue INSERT/UPDATE/DELETE."""
        part_name, _ = setup_and_compress(db)
        with pytest.raises(psycopg.errors.FeatureNotSupported):
            db.execute(
                f'ALTER TABLE "{part_name}" DISABLE TRIGGER '
                f"deltax_reject_compressed_dml"
            )
        db.rollback()

    def test_disable_trigger_all_rejected(self, db):
        part_name, _ = setup_and_compress(db)
        with pytest.raises(psycopg.errors.FeatureNotSupported):
            db.execute(f'ALTER TABLE "{part_name}" DISABLE TRIGGER ALL')
        db.rollback()


# ---------------------------------------------------------------------------
# Tier 3 — blocking + decompress→ALTER→recompress recipe
# ---------------------------------------------------------------------------


def run_recipe(conn, part_name, alter_sql):
    """Decompress → ALTER → recompress recipe. Returns nothing; raises on
    any step failing."""
    conn.execute(f"SELECT deltax.deltax_decompress_partition('{part_name}')")
    conn.execute(alter_sql)
    conn.execute(f"SELECT deltax.deltax_compress_partition('{part_name}')")
    conn.commit()


class TestTier3Blocking:
    def test_alter_column_type_blocked_and_recipe_works(self, db):
        part_name, _ = setup_and_compress(db)

        with pytest.raises(psycopg.errors.FeatureNotSupported) as exc:
            db.execute("ALTER TABLE metrics ALTER COLUMN pressure TYPE REAL")
        # The recipe HINT must mention the decompress→ALTER→recompress flow.
        assert "decompress" in (exc.value.diag.message_hint or "").lower()
        db.rollback()

        # Now run the recipe and verify it works.
        run_recipe(
            db,
            part_name,
            "ALTER TABLE metrics ALTER COLUMN pressure TYPE REAL",
        )

    def test_add_column_volatile_default_blocked(self, db):
        setup_and_compress(db)
        with pytest.raises(psycopg.errors.FeatureNotSupported):
            db.execute(
                "ALTER TABLE metrics ADD COLUMN rnd DOUBLE PRECISION DEFAULT random()"
            )
        db.rollback()

    def test_add_column_not_null_no_default_blocked(self, db):
        setup_and_compress(db)
        with pytest.raises(psycopg.errors.FeatureNotSupported):
            db.execute("ALTER TABLE metrics ADD COLUMN rev INT NOT NULL")
        db.rollback()

    def test_add_constraint_validating_blocked(self, db):
        setup_and_compress(db)
        with pytest.raises(psycopg.errors.FeatureNotSupported):
            db.execute(
                "ALTER TABLE metrics ADD CONSTRAINT pos_pressure CHECK (pressure > 0)"
            )
        db.rollback()

    def test_add_primary_key_blocked(self, db):
        setup_and_compress(db)
        with pytest.raises(psycopg.errors.FeatureNotSupported):
            db.execute("ALTER TABLE metrics ADD PRIMARY KEY (ts, device_id)")
        db.rollback()

    def test_set_storage_blocked(self, db):
        setup_and_compress(db)
        with pytest.raises(psycopg.errors.FeatureNotSupported):
            db.execute(
                "ALTER TABLE metrics ALTER COLUMN device_id SET STORAGE EXTERNAL"
            )
        db.rollback()

    def test_set_access_method_blocked(self, db):
        setup_and_compress(db)
        with pytest.raises(psycopg.errors.FeatureNotSupported):
            db.execute("ALTER TABLE metrics SET ACCESS METHOD heap")
        db.rollback()


# ---------------------------------------------------------------------------
# Mixed-subcommand statements are all-or-nothing
# ---------------------------------------------------------------------------


class TestMixedStatement:
    def test_mixed_statement_aborts_if_any_subcommand_is_tier3(self, db):
        """A single ALTER with [Tier 1, Tier 3] subcommands errors before
        PG executes anything; the Tier 1 part must NOT take effect."""
        setup_and_compress(db)
        with pytest.raises(psycopg.errors.FeatureNotSupported):
            db.execute(
                "ALTER TABLE metrics "
                "ADD COLUMN sensor_id INT, "
                "ALTER COLUMN pressure TYPE REAL"
            )
        db.rollback()

        # `sensor_id` must not exist — the statement was rejected before
        # PG ran the ADD COLUMN.
        cols = db.execute(
            "SELECT attname FROM pg_attribute "
            "WHERE attrelid = 'metrics'::regclass "
            "  AND attnum > 0 AND NOT attisdropped"
        ).fetchall()
        names = {r[0] for r in cols}
        assert "sensor_id" not in names


# ---------------------------------------------------------------------------
# Tier 2 — DROP COLUMN (tombstones the descriptor entry)
# ---------------------------------------------------------------------------


class TestTier2DropColumn:
    def test_drop_non_key_column_passes_through_and_tombstones(self, db):
        part_name, total = setup_and_compress(db)

        db.execute("ALTER TABLE metrics DROP COLUMN temperature")
        db.commit()

        # PG removed the column from pg_attribute.
        cols = db.execute(
            "SELECT attname FROM pg_attribute "
            "WHERE attrelid = 'metrics'::regclass "
            "  AND attnum > 0 AND NOT attisdropped"
        ).fetchall()
        assert {r[0] for r in cols} == {"ts", "device_id", "pressure"}

        # Descriptor entry for `temperature` was flipped to dropped: true
        # (other entries unchanged).
        desc = get_descriptor(db, part_name)
        by_name = {e["name"]: e for e in desc}
        assert by_name["temperature"]["dropped"] is True
        assert by_name["ts"]["dropped"] is False
        assert by_name["pressure"]["dropped"] is False

        # Reads still work — row count unchanged, remaining columns
        # accessible. Non-aggregating SELECT goes through the basic
        # decompress path which honors `MetadataInfo.blob_idx`; the
        # aggregation path's positional `_col_idx` lookup is the
        # `test_filter_on_added_column` xfail (see Future Work in
        # dev/docs/SCHEMA_CHANGES.md).
        rows = db.execute(
            f'SELECT ts, device_id, pressure FROM "{part_name}" LIMIT 5'
        ).fetchall()
        assert len(rows) == 5
        # All-rows count via the partition.
        cnt = db.execute(f'SELECT count(*) FROM "{part_name}"').fetchone()[0]
        assert cnt == total

    def test_drop_segment_by_column_is_tier3(self, db):
        setup_and_compress(db)
        with pytest.raises(psycopg.errors.FeatureNotSupported):
            db.execute("ALTER TABLE metrics DROP COLUMN device_id")
        db.rollback()

    def test_drop_time_column_is_tier3(self, db):
        setup_and_compress(db)
        with pytest.raises(psycopg.errors.FeatureNotSupported):
            db.execute("ALTER TABLE metrics DROP COLUMN ts")
        db.rollback()

    def test_drop_on_uncompressed_passes_through_with_no_tombstone(self, db):
        """Before any partition is compressed, DROP COLUMN should pass
        through with no tombstone (descriptor is NULL for uncompressed
        partitions, so there's nothing to flip)."""
        db.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
        db.execute("""
            CREATE TABLE metrics (
                ts TIMESTAMPTZ NOT NULL,
                device_id TEXT NOT NULL,
                temperature DOUBLE PRECISION,
                pressure DOUBLE PRECISION
            )
        """)
        db.execute(
            "SELECT deltax.deltax_create_table('metrics', 'ts', '1 day'::interval)"
        )
        db.execute(
            "SELECT deltax.deltax_enable_compression('metrics', "
            "segment_by => ARRAY['device_id'], order_by => ARRAY['ts'])"
        )
        db.commit()

        # No INSERT/compress yet — partitions exist but are empty +
        # uncompressed.
        db.execute("ALTER TABLE metrics DROP COLUMN temperature")
        db.commit()

        cols = db.execute(
            "SELECT attname FROM pg_attribute "
            "WHERE attrelid = 'metrics'::regclass "
            "  AND attnum > 0 AND NOT attisdropped"
        ).fetchall()
        assert "temperature" not in {r[0] for r in cols}

    def test_count_after_drop_matches_pre_drop(self, db):
        part_name, total = setup_and_compress(db)
        before = db.execute(f'SELECT count(*) FROM "{part_name}"').fetchone()[0]
        db.execute("ALTER TABLE metrics DROP COLUMN pressure")
        db.commit()
        after = db.execute(f'SELECT count(*) FROM "{part_name}"').fetchone()[0]
        assert before == after == total

    def test_recipe_round_trip_for_segment_by_drop(self, db):
        """Tier 3 recipe for `DROP COLUMN segment_by`: decompress all
        partitions, re-configure compression to drop the column from
        segment_by, recompress."""
        part_name, _ = setup_and_compress(db)

        # Recipe: decompress → reconfigure → drop column → recompress.
        db.execute(f"SELECT deltax.deltax_decompress_partition('{part_name}')")
        db.execute(
            "SELECT deltax.deltax_enable_compression('metrics', "
            "segment_by => ARRAY[]::text[], order_by => ARRAY['ts'])"
        )
        db.execute("ALTER TABLE metrics DROP COLUMN device_id")
        db.execute(f"SELECT deltax.deltax_compress_partition('{part_name}')")
        db.commit()

        cols = db.execute(
            "SELECT attname FROM pg_attribute "
            "WHERE attrelid = 'metrics'::regclass "
            "  AND attnum > 0 AND NOT attisdropped"
        ).fetchall()
        assert "device_id" not in {r[0] for r in cols}
