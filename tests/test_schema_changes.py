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
# Helper sanity check — everything below depends on `setup_and_compress`
# producing one compressed partition with the expected row count. If this
# fails, the downstream tier sweeps all fail in confusing ways; pin the
# invariants here so the failure points at the right thing.
# ---------------------------------------------------------------------------


class TestSetupAndCompress:
    def test_setup_produces_one_compressed_partition_with_expected_state(self, db):
        part_name, expected_rows = setup_and_compress(db, n_devices=3, n_points=20)

        # Expected row count matches what the helper claims.
        assert expected_rows == 3 * 20

        # The deltatable is registered with the configured shape.
        ht = db.execute(
            "SELECT schema_name, table_name, time_column, segment_by, order_by "
            "FROM deltax.deltax_deltatable WHERE table_name = 'metrics'"
        ).fetchone()
        assert ht == ("public", "metrics", "ts", ["device_id"], ["ts"]), ht

        # Exactly one partition row exists for the metrics deltatable, it's
        # the one the helper returned, and it's marked compressed.
        rows = db.execute(
            "SELECT p.table_name, p.is_compressed "
            "FROM deltax.deltax_partition p "
            "JOIN deltax.deltax_deltatable h ON h.id = p.deltatable_id "
            "WHERE h.table_name = 'metrics' AND p.is_compressed"
        ).fetchall()
        assert rows == [(part_name, True)], rows

        # SELECT count via the partition returns the expected rows; the
        # compressed scan path is exercised here so a regression that
        # corrupted decompression would surface immediately.
        partition_count = db.execute(
            f'SELECT count(*) FROM "{part_name}"'
        ).fetchone()[0]
        assert partition_count == expected_rows

        # Companion `_meta` table exists in `_deltax_compressed`.
        meta_exists = db.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_tables "
            "WHERE schemaname = '_deltax_compressed' AND tablename = %s)",
            (f"{part_name}_meta",),
        ).fetchone()[0]
        assert meta_exists

        # Descriptor was populated at compression time.
        desc = get_descriptor(db, part_name)
        assert isinstance(desc, list)
        assert len(desc) == 4  # ts, device_id, temperature, pressure
        assert {e["name"] for e in desc} == {
            "ts",
            "device_id",
            "temperature",
            "pressure",
        }


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

    def test_add_column_stable_default_now(self, db):
        """`DEFAULT now()` is stable: PG evaluates it once at ALTER time
        and stores the result in `attmissingval`. Every compressed row
        reads back that same captured timestamp."""
        part_name, total = setup_and_compress(db)

        db.execute("ALTER TABLE metrics ADD COLUMN ts_added TIMESTAMPTZ DEFAULT now()")
        db.commit()

        # Direct SELECT goes through the basic decompress path (already
        # blob_idx-aware from Session 1). Every row reads back the same
        # captured timestamp; the partition has `total` rows.
        rows = db.execute(
            f'SELECT ts_added FROM "{part_name}" LIMIT 5'
        ).fetchall()
        assert len(rows) == 5
        assert all(r[0] == rows[0][0] for r in rows), "timestamps should be equal"
        # Filter pushdown on the captured constant.
        matches = db.execute(
            f"SELECT count(*) FROM \"{part_name}\" WHERE ts_added = (SELECT ts_added FROM \"{part_name}\" LIMIT 1)"
        ).fetchone()[0]
        assert matches == total

    def test_add_column_stable_default_current_date(self, db):
        part_name, total = setup_and_compress(db)
        db.execute("ALTER TABLE metrics ADD COLUMN d DATE DEFAULT current_date")
        db.commit()

        # Read the synthesized date from a non-aggregating SELECT.
        first = db.execute(
            f'SELECT d FROM "{part_name}" LIMIT 1'
        ).fetchone()[0]
        assert first is not None
        cnt = db.execute(
            f"SELECT count(*) FROM \"{part_name}\" WHERE d = '{first}'::date"
        ).fetchone()[0]
        assert cnt == total

    def test_add_column_immutable_function_default(self, db):
        """Immutable functions (`abs(-5)`) are deterministic and PG
        evaluates them once at ALTER time."""
        part_name, total = setup_and_compress(db)
        db.execute("ALTER TABLE metrics ADD COLUMN x INT DEFAULT abs(-5)")
        db.commit()

        rows = db.execute(f'SELECT x FROM "{part_name}"').fetchall()
        assert len(rows) == total
        assert all(r[0] == 5 for r in rows)

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

    def test_aggregate_on_added_column(self, db):
        """sum/avg/min/max on a column added after compression should
        return the constant default repeated for every row — i.e. the
        agg path is consulting MetadataInfo.missing_values for the
        synthesized datum."""
        part_name, total = setup_and_compress(db)

        db.execute("ALTER TABLE metrics ADD COLUMN bucket INT DEFAULT 5")
        db.commit()

        row = db.execute(
            f'SELECT count(*), sum(bucket), avg(bucket), min(bucket), max(bucket) '
            f'FROM "{part_name}"'
        ).fetchone()
        assert row[0] == total
        assert row[1] == 5 * total
        assert float(row[2]) == 5.0
        assert row[3] == 5
        assert row[4] == 5

    def test_group_by_added_column(self, db):
        """GROUP BY on a column added after compression should produce a
        single group whose key is the synthesized default."""
        part_name, total = setup_and_compress(db)

        db.execute("ALTER TABLE metrics ADD COLUMN bucket INT DEFAULT 5")
        db.commit()

        groups = db.execute(
            f'SELECT bucket, count(*) FROM "{part_name}" '
            f'GROUP BY bucket ORDER BY bucket'
        ).fetchall()
        assert groups == [(5, total)]

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

    def test_add_column_clock_timestamp_default_blocked(self, db):
        """`clock_timestamp()` is volatile (changes per call within a
        transaction, unlike the stable `now()`)."""
        setup_and_compress(db)
        with pytest.raises(psycopg.errors.FeatureNotSupported):
            db.execute(
                "ALTER TABLE metrics ADD COLUMN tick TIMESTAMPTZ "
                "DEFAULT clock_timestamp()"
            )
        db.rollback()

    def test_add_column_nextval_default_blocked(self, db):
        setup_and_compress(db)
        db.execute("CREATE SEQUENCE deltax_test_seq")
        db.commit()
        with pytest.raises(psycopg.errors.FeatureNotSupported):
            db.execute(
                "ALTER TABLE metrics ADD COLUMN n BIGINT "
                "DEFAULT nextval('deltax_test_seq')"
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


# ---------------------------------------------------------------------------
# Per-subcommand sweep — one-line assertion per AlterTableType discriminant
#
# The sweeps verify the classifier in `src/ddl.rs` produces the expected
# disposition for each `AT_*` subcommand. Each parametrize entry has
# (subtype_label, setup_sql_or_None, alter_sql). All entries assume an
# already-compressed deltatable from `setup_and_compress` so the Tier 3
# `has_compressed_partitions` gate is active.
# ---------------------------------------------------------------------------


class TestTier1Sweep:
    """Subcommands that should pass through `standard_ProcessUtility`.

    Each entry verifies (a) the ALTER doesn't raise, and (b) PG actually
    applied the change — checked by a follow-up SELECT against the system
    catalogs whose expected value is the parametrize tuple's 4th element.
    """

    @pytest.mark.parametrize(
        "label,setup_sql,alter_sql,verify_sql,expected",
        [
            (
                "AT_DropNotNull",
                None,
                "ALTER TABLE metrics ALTER COLUMN device_id DROP NOT NULL",
                "SELECT attnotnull FROM pg_attribute "
                "WHERE attrelid = 'metrics'::regclass AND attname = 'device_id'",
                False,
            ),
            (
                "AT_SetStatistics",
                None,
                "ALTER TABLE metrics ALTER COLUMN temperature SET STATISTICS 100",
                "SELECT attstattarget FROM pg_attribute "
                "WHERE attrelid = 'metrics'::regclass AND attname = 'temperature'",
                100,
            ),
            (
                "AT_SetOptions",
                None,
                "ALTER TABLE metrics ALTER COLUMN temperature SET (n_distinct = 1000)",
                "SELECT 'n_distinct=1000' = ANY(attoptions) FROM pg_attribute "
                "WHERE attrelid = 'metrics'::regclass AND attname = 'temperature'",
                True,
            ),
            (
                "AT_ResetOptions",
                "ALTER TABLE metrics ALTER COLUMN temperature SET (n_distinct = 1000)",
                "ALTER TABLE metrics ALTER COLUMN temperature RESET (n_distinct)",
                "SELECT attoptions IS NULL FROM pg_attribute "
                "WHERE attrelid = 'metrics'::regclass AND attname = 'temperature'",
                True,
            ),
            # AT_SetRelOptions / AT_ResetRelOptions are pass-through in the
            # classifier but PG itself rejects them on partitioned parents
            # ("cannot specify storage parameters for a partitioned table").
            # Coverage instead falls to per-partition tests in test_compression.py
            # where compress.rs sets autovacuum_enabled=off on each leaf.
            (
                "AT_ReplicaIdentity",
                None,
                "ALTER TABLE metrics REPLICA IDENTITY FULL",
                # 'f' = FULL in pg_class.relreplident
                "SELECT relreplident::text FROM pg_class WHERE oid = 'metrics'::regclass",
                "f",
            ),
            (
                "AT_ChangeOwner",
                None,
                "ALTER TABLE metrics OWNER TO postgres",
                # Owner is `postgres` (already was, and stays). The fact
                # that the statement reached `pg_class` at all means our
                # hook chained through standard_ProcessUtility — a hook
                # that wrongly raised would skip the chain entirely.
                "SELECT pg_get_userbyid(relowner) FROM pg_class "
                "WHERE oid = 'metrics'::regclass",
                "postgres",
            ),
            (
                "AT_ColumnDefault_SET",
                None,
                "ALTER TABLE metrics ALTER COLUMN temperature SET DEFAULT 99.9",
                "SELECT atthasdef FROM pg_attribute "
                "WHERE attrelid = 'metrics'::regclass AND attname = 'temperature'",
                True,
            ),
            (
                "AT_ColumnDefault_DROP",
                "ALTER TABLE metrics ALTER COLUMN temperature SET DEFAULT 99.9",
                "ALTER TABLE metrics ALTER COLUMN temperature DROP DEFAULT",
                "SELECT atthasdef FROM pg_attribute "
                "WHERE attrelid = 'metrics'::regclass AND attname = 'temperature'",
                False,
            ),
            (
                "AT_AddConstraint_check_not_valid",
                None,
                "ALTER TABLE metrics ADD CONSTRAINT chk_p "
                "CHECK (pressure > 0) NOT VALID",
                "SELECT count(*)::int FROM pg_constraint "
                "WHERE conrelid = 'metrics'::regclass AND conname = 'chk_p'",
                1,
            ),
            # AT_AddConstraint FOREIGN KEY NOT VALID — pass-through in
            # the classifier (skip_validation=true), but PG itself rejects
            # NOT VALID FKs on partitioned parents pre-PG18. The CHECK
            # NOT VALID case above already exercises the same classifier
            # branch.
            (
                "AT_DropConstraint",
                "ALTER TABLE metrics ADD CONSTRAINT c CHECK (pressure > 0) NOT VALID",
                "ALTER TABLE metrics DROP CONSTRAINT c",
                "SELECT count(*)::int FROM pg_constraint "
                "WHERE conrelid = 'metrics'::regclass AND conname = 'c'",
                0,
            ),
            (
                "AT_EnableTrig_user_trigger",
                "CREATE FUNCTION u_trg_fn() RETURNS trigger LANGUAGE plpgsql AS "
                "$$ BEGIN RETURN NEW; END; $$; "
                "CREATE TRIGGER u_trg BEFORE INSERT ON metrics "
                "FOR EACH ROW EXECUTE FUNCTION u_trg_fn(); "
                "ALTER TABLE metrics DISABLE TRIGGER u_trg",
                "ALTER TABLE metrics ENABLE TRIGGER u_trg",
                # 'O' = enabled (Origin), 'D' = disabled.
                "SELECT tgenabled::text FROM pg_trigger "
                "WHERE tgrelid = 'metrics'::regclass AND tgname = 'u_trg'",
                "O",
            ),
            (
                "AT_DisableTrig_user_trigger",
                "CREATE FUNCTION u2_trg_fn() RETURNS trigger LANGUAGE plpgsql AS "
                "$$ BEGIN RETURN NEW; END; $$; "
                "CREATE TRIGGER u2_trg BEFORE INSERT ON metrics "
                "FOR EACH ROW EXECUTE FUNCTION u2_trg_fn()",
                "ALTER TABLE metrics DISABLE TRIGGER u2_trg",
                "SELECT tgenabled::text FROM pg_trigger "
                "WHERE tgrelid = 'metrics'::regclass AND tgname = 'u2_trg'",
                "D",
            ),
        ],
    )
    def test_passes_through(
        self, db, label, setup_sql, alter_sql, verify_sql, expected
    ):
        setup_and_compress(db)
        if setup_sql:
            db.execute(setup_sql)
            db.commit()
        # No exception → pass-through.
        db.execute(alter_sql)
        db.commit()
        # Verify the ALTER actually applied (not just that the hook
        # didn't error).
        actual = db.execute(verify_sql).fetchone()[0]
        assert actual == expected, (
            f"{label}: post-ALTER state mismatch — expected {expected!r}, got {actual!r}"
        )


class TestTier3Sweep:
    """Subcommands that should raise FeatureNotSupported with the recipe HINT."""

    @pytest.mark.parametrize(
        "label,setup_sql,alter_sql",
        [
            # -------- ALTER COLUMN --------
            (
                "AT_SetNotNull",
                None,
                "ALTER TABLE metrics ALTER COLUMN temperature SET NOT NULL",
            ),
            (
                "AT_SetCompression",
                None,
                "ALTER TABLE metrics ALTER COLUMN device_id SET COMPRESSION lz4",
            ),
            # -------- Constraints --------
            (
                "AT_AddConstraint_check_validating",
                None,
                "ALTER TABLE metrics ADD CONSTRAINT chk CHECK (pressure > 0)",
            ),
            (
                "AT_AddConstraint_unique",
                None,
                "ALTER TABLE metrics ADD CONSTRAINT u UNIQUE (ts, device_id)",
            ),
            (
                "AT_ValidateConstraint",
                "ALTER TABLE metrics ADD CONSTRAINT vc CHECK (pressure > 0) NOT VALID",
                "ALTER TABLE metrics VALIDATE CONSTRAINT vc",
            ),
            # -------- Identity / generated --------
            (
                "AT_AddIdentity",
                None,
                "ALTER TABLE metrics ALTER COLUMN temperature "
                "ADD GENERATED ALWAYS AS IDENTITY",
            ),
            (
                "AT_AddColumn_generated",
                None,
                "ALTER TABLE metrics ADD COLUMN gen INT "
                "GENERATED ALWAYS AS (1) STORED",
            ),
            # -------- Row-level security --------
            (
                "AT_EnableRowSecurity",
                None,
                "ALTER TABLE metrics ENABLE ROW LEVEL SECURITY",
            ),
            (
                "AT_DisableRowSecurity",
                None,
                "ALTER TABLE metrics DISABLE ROW LEVEL SECURITY",
            ),
            (
                "AT_ForceRowSecurity",
                None,
                "ALTER TABLE metrics FORCE ROW LEVEL SECURITY",
            ),
            (
                "AT_NoForceRowSecurity",
                None,
                "ALTER TABLE metrics NO FORCE ROW LEVEL SECURITY",
            ),
            # -------- Triggers (the ALL forms; specific-trigger DISABLE
            # on `deltax_reject_compressed_dml` is in TestTier1Transparent).
            (
                "AT_EnableTrigAll",
                None,
                "ALTER TABLE metrics ENABLE TRIGGER ALL",
            ),
            # -------- Cluster / storage location --------
            (
                "AT_DropCluster",
                None,
                "ALTER TABLE metrics SET WITHOUT CLUSTER",
            ),
            (
                "AT_SetTableSpace",
                None,
                "ALTER TABLE metrics SET TABLESPACE pg_default",
            ),
            # -------- Inheritance / OF type --------
            (
                "AT_AddInherit",
                "CREATE TABLE inherit_target ()",
                "ALTER TABLE metrics INHERIT inherit_target",
            ),
        ],
    )
    def test_blocks_with_feature_not_supported(self, db, label, setup_sql, alter_sql):
        setup_and_compress(db)
        if setup_sql:
            db.execute(setup_sql)
            db.commit()
        with pytest.raises(psycopg.errors.FeatureNotSupported) as exc:
            db.execute(alter_sql)
        # Recipe HINT must be present so users know what to do.
        assert "decompress" in (exc.value.diag.message_hint or "").lower(), (
            f"HINT missing recipe for {label}"
        )
        db.rollback()


# ---------------------------------------------------------------------------
# OWNER / GRANT / REVOKE cascade to companion tables
#
# `ALTER TABLE deltatable OWNER TO …` and `GRANT/REVOKE … ON TABLE
# deltatable …` apply only to the parent in PG. Our ALTER policy hook
# mirrors them onto every companion table in `_deltax_compressed.*` so
# admin-level operations on a deltatable behave the way users expect.
# ---------------------------------------------------------------------------


def companion_tables(conn, deltatable_name="metrics"):
    """Return the list of companion FQNs that exist for the given deltatable."""
    return [
        r[0]
        for r in conn.execute(
            "SELECT n.nspname || '.' || c.relname FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = '_deltax_compressed' "
            "  AND c.relkind = 'r' "
            "  AND EXISTS (SELECT 1 FROM deltax.deltax_partition p "
            "              JOIN deltax.deltax_deltatable h ON h.id = p.deltatable_id "
            "              WHERE h.table_name = %s "
            "                AND (c.relname LIKE p.table_name || '\\_%%'))",
            (deltatable_name,),
        ).fetchall()
    ]


class TestCascadeToCompanions:
    def test_change_owner_cascades_to_companions(self, db):
        part_name, _ = setup_and_compress(db)
        db.execute("CREATE ROLE deltax_test_owner")
        db.execute("ALTER TABLE metrics OWNER TO deltax_test_owner")
        db.commit()

        # Parent + every companion should now be owned by the new role.
        parent_owner = db.execute(
            "SELECT pg_get_userbyid(relowner) FROM pg_class "
            "WHERE oid = 'metrics'::regclass"
        ).fetchone()[0]
        assert parent_owner == "deltax_test_owner"

        comps = companion_tables(db)
        assert len(comps) > 0, "expected at least one companion table"
        for fqn in comps:
            owner = db.execute(
                f"SELECT pg_get_userbyid(relowner) FROM pg_class "
                f"WHERE oid = '{fqn}'::regclass"
            ).fetchone()[0]
            assert owner == "deltax_test_owner", (
                f"{fqn} owner is {owner}, expected deltax_test_owner"
            )

    def test_grant_select_cascades_to_companions(self, db):
        part_name, _ = setup_and_compress(db)
        db.execute("CREATE ROLE deltax_test_reader")
        db.execute("GRANT SELECT ON TABLE metrics TO deltax_test_reader")
        db.commit()

        # has_table_privilege returns true if the named role has the
        # specified privilege on the table.
        for fqn in companion_tables(db):
            has = db.execute(
                "SELECT has_table_privilege(%s, %s, 'SELECT')",
                ("deltax_test_reader", fqn),
            ).fetchone()[0]
            assert has, f"deltax_test_reader missing SELECT on {fqn}"

    def test_revoke_select_cascades_to_companions(self, db):
        setup_and_compress(db)
        db.execute("CREATE ROLE deltax_test_reader2")
        db.execute("GRANT SELECT ON TABLE metrics TO deltax_test_reader2")
        db.commit()
        # Sanity: companions have it.
        for fqn in companion_tables(db):
            assert db.execute(
                "SELECT has_table_privilege(%s, %s, 'SELECT')",
                ("deltax_test_reader2", fqn),
            ).fetchone()[0]

        db.execute("REVOKE SELECT ON TABLE metrics FROM deltax_test_reader2")
        db.commit()

        for fqn in companion_tables(db):
            has = db.execute(
                "SELECT has_table_privilege(%s, %s, 'SELECT')",
                ("deltax_test_reader2", fqn),
            ).fetchone()[0]
            assert not has, f"deltax_test_reader2 still has SELECT on {fqn}"

    def test_grant_all_cascades_to_companions(self, db):
        """`GRANT ALL` should cascade as ALL (not get translated to a
        specific privilege list)."""
        setup_and_compress(db)
        db.execute("CREATE ROLE deltax_test_admin")
        db.execute("GRANT ALL ON TABLE metrics TO deltax_test_admin")
        db.commit()

        for fqn in companion_tables(db):
            for priv in ("SELECT", "INSERT", "UPDATE", "DELETE"):
                has = db.execute(
                    "SELECT has_table_privilege(%s, %s, %s)",
                    ("deltax_test_admin", fqn, priv),
                ).fetchone()[0]
                assert has, f"deltax_test_admin missing {priv} on {fqn}"

    def test_grant_public_cascades_to_companions(self, db):
        """`GRANT … TO PUBLIC` must cascade — public is rendered with
        the PUBLIC keyword, not as a quoted role name."""
        setup_and_compress(db)
        db.execute("GRANT SELECT ON TABLE metrics TO PUBLIC")
        db.commit()

        for fqn in companion_tables(db):
            has = db.execute(
                f"SELECT has_table_privilege('public', '{fqn}', 'SELECT')"
            ).fetchone()[0]
            assert has, f"public missing SELECT on {fqn}"

    def test_column_level_grant_does_not_cascade(self, db):
        """Column-level grants reference user-facing column names that
        don't exist on companions; the hook should pass through with no
        cascade attempt (otherwise we'd error trying to grant on a
        nonexistent column)."""
        setup_and_compress(db)
        db.execute("CREATE ROLE deltax_test_colreader")
        # No exception: the hook leaves column-level grants alone.
        db.execute(
            "GRANT SELECT (temperature) ON TABLE metrics TO deltax_test_colreader"
        )
        db.commit()
