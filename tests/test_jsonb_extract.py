"""Integration tests for json_extract: planner-hook rewrite, executor synthetic
columns, correctness across `pg_deltax.json_extract_mode = 'fields' | 'none'`.

Each correctness test runs the same query under both modes and asserts equal
results. `'none'` is the slow-path control (no rewrite); `'fields'` exercises
the rewrite + synthetic-column path. Any divergence is a bug."""

import json
import random

import pytest

MOCK_NOW = "2025-01-15 12:00:00+00"
BASE_TS = "2025-01-15 00:00:00+00"


def _setup(conn, table_name="events"):
    conn.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
    conn.execute(f"""
        CREATE TABLE {table_name} (
            ts TIMESTAMPTZ NOT NULL,
            data JSONB NOT NULL
        )
    """)
    conn.execute(f"SELECT deltax_create_table('{table_name}', 'ts', '1 day'::interval)")
    conn.commit()


def _insert(conn, table_name="events", n=1500, seed=42):
    """Insert synthetic Bluesky-shaped JSONB rows. Independent distributions
    for kind / operation / collection / did so that JSONBench-shape filters
    (e.g. `kind='commit' AND op='create'`) return non-empty results.

    Distribution choices:
    - kind: weighted toward 'commit' (~80%) to mirror JSONBench's real shape
      and ensure filters that match the bulk of rows have plenty to operate on.
    - operation: uniform over 3 values.
    - collection: uniform over 5 (covers Q2's IN-clause and Q3/Q4's = filter).
    - did: 50 distinct, uniform — gives LIMIT 3 GROUP BY user_id queries
      multiple distinct users to choose from.
    - time_us: monotonically increases (deterministic for MIN/MAX assertions)
      with 1 ms spacing.
    """
    rng = random.Random(seed)
    kinds = ["account", "commit", "identity"]
    operations = ["create", "delete", "update"]
    collections = [
        "app.bsky.feed.post",
        "app.bsky.feed.repost",
        "app.bsky.feed.like",
        "app.bsky.graph.follow",
        "app.bsky.graph.block",
    ]
    n_dids = 50

    rows = []
    for i in range(n):
        ts = f"'{BASE_TS}'::timestamptz + interval '{i} seconds'"
        time_us = 1700000000000000 + i * 1000
        kind = rng.choices(kinds, weights=[1, 8, 1], k=1)[0]
        payload = {
            "kind": kind,
            "did": f"did:plc:{rng.randrange(n_dids):03d}",
            "time_us": time_us,
            "commit": {
                "collection": rng.choice(collections),
                "operation": rng.choice(operations),
                "rkey": f"rec{i}",
            },
        }
        rows.append(f"({ts}, '{json.dumps(payload)}'::jsonb)")
    batch = 200
    for i in range(0, len(rows), batch):
        conn.execute(
            f"INSERT INTO {table_name} (ts, data) VALUES " + ", ".join(rows[i:i+batch])
        )
    conn.commit()


def _enable_extracts(conn, table_name="events", segment_size=200):
    extract = json.dumps([
        {"src": "data", "path": ["kind"], "name": "x_kind", "type": "text"},
        {"src": "data", "path": ["did"], "name": "x_did", "type": "text"},
        {"src": "data", "path": ["time_us"], "name": "x_time_us", "type": "bigint"},
        {"src": "data", "path": ["commit", "collection"], "name": "x_collection", "type": "text"},
        {"src": "data", "path": ["commit", "operation"], "name": "x_operation", "type": "text"},
    ])
    conn.execute(f"""
        SELECT deltax_enable_compression(
            '{table_name}',
            order_by => ARRAY['ts'],
            segment_size => {segment_size},
            json_extract => %s::jsonb
        )
    """, (extract,))
    conn.commit()


def _ab(conn, query):
    """Run the same query under mode='none' and mode='fields'; return both results."""
    conn.execute("SET pg_deltax.json_extract_mode = 'none'")
    none_rows = conn.execute(query).fetchall()
    conn.execute("SET pg_deltax.json_extract_mode = 'fields'")
    fields_rows = conn.execute(query).fetchall()
    return none_rows, fields_rows


# ---------------------------------------------------------------------------
# Correctness: rewrite must produce the same results as the slow path.
# ---------------------------------------------------------------------------

class TestJsonExtractCorrectness:
    def test_groupby_kind(self, db):
        _setup(db); _enable_extracts(db); _insert(db)
        none, fields = _ab(db, """
            SELECT data ->> 'kind' AS kind, count(*)
            FROM events GROUP BY 1 ORDER BY 1
        """)
        assert none == fields
        # Sanity: slow path did produce non-empty groups.
        assert len(fields) == 3

    def test_filter_and_group(self, db):
        """Mirrors JSONBench Q1: filter on kind+operation, group on collection."""
        _setup(db); _enable_extracts(db); _insert(db)
        none, fields = _ab(db, """
            SELECT data -> 'commit' ->> 'collection' AS coll, count(*)
            FROM events
            WHERE data ->> 'kind' = 'commit'
              AND data -> 'commit' ->> 'operation' = 'create'
            GROUP BY 1 ORDER BY 1
        """)
        assert none == fields
        assert len(fields) > 0, "filter dropped all rows; data fixture or matcher is broken"

    def test_cast_to_bigint(self, db):
        """`(data ->> 'time_us')::bigint` exercises the cast-stripping in the
        chain matcher (terminal kind = bigint, not text)."""
        _setup(db); _enable_extracts(db); _insert(db)
        none, fields = _ab(db, """
            SELECT min((data ->> 'time_us')::bigint), max((data ->> 'time_us')::bigint)
            FROM events
            WHERE data ->> 'kind' = 'commit'
        """)
        assert none == fields
        assert fields[0][0] is not None and fields[0][1] is not None

    def test_raw_data_and_chain_together(self, db):
        """Regression: query reads BOTH raw `data` AND a chain expr. The
        prior unconditional Section::Cols prune dropped `data`'s col_idx
        from needed-cols, causing it to come back NULL. Ref-count walker
        must keep `data` when an upper plan still references it."""
        _setup(db); _enable_extracts(db); _insert(db)
        none, fields = _ab(db, """
            SELECT data ->> 'kind' AS kind,
                   jsonb_typeof(data) AS dt,
                   count(*)
            FROM events
            GROUP BY 1, 2 ORDER BY 1
        """)
        assert none == fields
        # Even with extracts active, raw `data` must still resolve to the
        # original JSONB object — `jsonb_typeof` would return NULL otherwise.
        for kind, dt, _ in fields:
            assert dt == "object", f"raw data dropped under mode='fields': kind={kind} dt={dt}"

    def test_select_star_with_chain(self, db):
        """`SELECT *` forces both ts and data into the upper plan. After
        rewrite the planner shouldn't accidentally NULL them out."""
        _setup(db); _enable_extracts(db); _insert(db)
        none, fields = _ab(db, "SELECT * FROM events ORDER BY ts, data ->> 'did' LIMIT 5")
        assert none == fields
        for ts, data in fields:
            assert ts is not None
            assert data is not None and "kind" in data

    def test_missing_path_returns_null(self, db):
        """A path that's absent from a row must produce NULL via both modes."""
        _setup(db); _enable_extracts(db)
        # Insert one row missing the `commit` sub-object.
        db.execute(
            f"INSERT INTO events (ts, data) VALUES "
            f"('{BASE_TS}'::timestamptz, '{{\"kind\":\"identity\",\"did\":\"x\"}}'::jsonb)"
        )
        db.commit()
        none, fields = _ab(db, """
            SELECT data -> 'commit' ->> 'collection'
            FROM events
        """)
        assert none == fields == [(None,)]

    def test_coalesce_with_chain(self, db):
        """`CoalesceExpr` nests chain Exprs as args. The ref-counter must
        descend into it to count the OUTER_VAR refs of the rewritten chain;
        otherwise the synthetic position would get pruned and the value
        would come back NULL."""
        _setup(db); _enable_extracts(db); _insert(db)
        none, fields = _ab(db, """
            SELECT COALESCE(data ->> 'kind', 'unknown') AS k, count(*)
            FROM events GROUP BY 1 ORDER BY 1
        """)
        assert none == fields
        assert len(fields) >= 1
        for k, _ in fields:
            assert k is not None  # COALESCE never returns NULL here.

    def test_chain_in_case_when(self, db):
        """`CaseExpr`/`CaseWhen` arms hold sub-expressions; chain must be
        matched/rewritten there too."""
        _setup(db); _enable_extracts(db); _insert(db)
        none, fields = _ab(db, """
            SELECT CASE WHEN data ->> 'kind' = 'commit' THEN 'c'
                        WHEN data ->> 'kind' = 'identity' THEN 'i'
                        ELSE 'other' END AS bucket, count(*)
            FROM events
            GROUP BY 1 ORDER BY 1
        """)
        assert none == fields
        assert len(fields) >= 2

    def test_type_mismatch_in_source_jsonb(self, db):
        """Spec declares `x_kind text`, but a row has `kind` as a number.
        PG's `->>` returns the text representation, so both modes should
        agree on the same string. The risk is COPY-time extraction
        diverging from runtime `->>` semantics — both must be consistent."""
        _setup(db)
        _enable_extracts(db)
        # Mix of well-formed and type-mismatched values for `kind`.
        db.execute(
            f"INSERT INTO events (ts, data) VALUES "
            f"('{BASE_TS}'::timestamptz, '{{\"kind\":\"commit\"}}'::jsonb),"
            f"('{BASE_TS}'::timestamptz + interval '1 second', '{{\"kind\":42}}'::jsonb),"
            f"('{BASE_TS}'::timestamptz + interval '2 seconds', '{{\"kind\":true}}'::jsonb),"
            f"('{BASE_TS}'::timestamptz + interval '3 seconds', '{{\"kind\":null}}'::jsonb)"
        )
        db.commit()
        none, fields = _ab(db, """
            SELECT data ->> 'kind' AS kind, count(*)
            FROM events GROUP BY 1 ORDER BY 1 NULLS LAST
        """)
        assert none == fields, (
            f"COPY-time extraction disagrees with runtime `->>`: "
            f"none={none}, fields={fields}"
        )

    def test_re_enable_with_added_path(self, db):
        """Spec evolves: enable with paths A, compress, then re-enable with
        paths A+B, compress more. The mixed-partition gate must fire because
        partition A's companion blobs don't have B's synthetic column."""
        _setup(db)
        # Enable with one path.
        db.execute("""
            SELECT deltax_enable_compression(
                'events',
                order_by => ARRAY['ts'],
                segment_size => 100,
                json_extract => '[
                    {"src":"data","path":["kind"],"name":"x_kind","type":"text"}
                ]'::jsonb
            )
        """)
        db.commit()
        # Insert + compress.
        rows = ", ".join(
            f"('{BASE_TS}'::timestamptz + interval '{i} seconds',"
            f"'{{\"kind\":\"commit\",\"did\":\"u{i % 5}\"}}'::jsonb)"
            for i in range(40)
        )
        db.execute(f"INSERT INTO events (ts, data) VALUES {rows}")
        db.commit()
        db.execute(
            "SELECT deltax_compress_partition(table_name) FROM deltax_partition "
            "WHERE deltatable_id = (SELECT id FROM deltax_deltatable WHERE table_name = 'events') "
            "AND row_count > 0"
        )
        db.commit()
        # Re-enable with an extra path — bumps json_extract_added_at.
        db.execute("""
            SELECT deltax_enable_compression(
                'events',
                order_by => ARRAY['ts'],
                segment_size => 100,
                json_extract => '[
                    {"src":"data","path":["kind"],"name":"x_kind","type":"text"},
                    {"src":"data","path":["did"],"name":"x_did","type":"text"}
                ]'::jsonb
            )
        """)
        db.commit()
        # Query a path the OLD partition's companion blobs don't carry yet.
        # Without the gate, `data->>'did'` would be rewritten to read the
        # x_did synthetic — which is missing for the old partition → NULLs.
        none, fields = _ab(db, """
            SELECT data ->> 'did' AS did, count(*)
            FROM events GROUP BY 1 ORDER BY 1
        """)
        assert none == fields
        # Every row's `did` resolves correctly even though x_did didn't
        # exist when the partition was compressed.
        assert all(d is not None for d, _ in fields), (
            f"x_did missing for old partition: {fields}"
        )

    def test_synthetic_columns_not_exposed_to_users(self, db):
        """Synthetic columns live in companion tables only — they must NOT
        leak into the user-facing relation's `pg_attribute` /
        `information_schema.columns`. `SELECT *` and `\\d events` should
        show only the original columns (ts, data)."""
        _setup(db); _enable_extracts(db); _insert(db, n=50)

        # User-facing schema: only ts + data.
        cols = db.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'events'
            ORDER BY ordinal_position
        """).fetchall()
        assert [c[0] for c in cols] == ["ts", "data"], (
            f"synthetic columns leaked into user-facing schema: {cols}"
        )

        # SELECT * returns the same shape — no extra trailing columns.
        db.execute("SET pg_deltax.json_extract_mode = 'fields'")
        row = db.execute("SELECT * FROM events LIMIT 1").fetchone()
        assert len(row) == 2, f"SELECT * returned {len(row)} columns, expected 2"

    def test_clearing_json_extract_falls_through(self, db):
        """Re-enable_compression with `'[]'::jsonb` clears the extract list.
        Walker sees no specs and falls through; queries on the table
        (including segments compressed when extracts existed) must still
        return correct results."""
        _setup(db); _enable_extracts(db); _insert(db, n=200)
        # Force a compression so we have segments populated under the
        # initial spec list. Then clear the spec.
        db.execute(
            "SELECT deltax_compress_partition(table_name) FROM deltax_partition "
            "WHERE deltatable_id = (SELECT id FROM deltax_deltatable WHERE table_name = 'events') "
            "AND row_count > 0"
        )
        db.commit()
        db.execute("""
            SELECT deltax_enable_compression(
                'events', order_by => ARRAY['ts'],
                json_extract => '[]'::jsonb
            )
        """)
        db.commit()
        # Both modes must agree and return non-empty results.
        none, fields = _ab(db, """
            SELECT data ->> 'kind' AS kind, count(*)
            FROM events GROUP BY 1 ORDER BY 1
        """)
        assert none == fields
        assert len(fields) == 3

    def test_prepared_plan_after_enable_compression_change(self, db):
        """A PREPAREd query is plan-cached. If we change `json_extract`
        between PREPARE and EXECUTE, the cached plan may carry a stale
        `custom_scan_tlist`. The result must remain correct (or the plan
        must be invalidated and replanned) — never silent corruption."""
        _setup(db); _enable_extracts(db); _insert(db, n=200)
        db.execute("SET pg_deltax.json_extract_mode = 'fields'")
        db.execute("""
            PREPARE q AS
            SELECT data ->> 'kind' AS kind, count(*)
            FROM events GROUP BY 1 ORDER BY 1
        """)
        first = db.execute("EXECUTE q").fetchall()
        db.commit()

        # Bump json_extract: add a path the prepared plan didn't see.
        db.execute("""
            SELECT deltax_enable_compression(
                'events', order_by => ARRAY['ts'],
                json_extract => '[
                    {"src":"data","path":["kind"],"name":"x_kind","type":"text"},
                    {"src":"data","path":["did"],"name":"x_did","type":"text"},
                    {"src":"data","path":["time_us"],"name":"x_time_us","type":"bigint"},
                    {"src":"data","path":["commit","collection"],"name":"x_collection","type":"text"},
                    {"src":"data","path":["commit","operation"],"name":"x_operation","type":"text"},
                    {"src":"data","path":["commit","rkey"],"name":"x_rkey","type":"text"}
                ]'::jsonb
            )
        """)
        db.commit()

        # Re-execute the same prepared statement. Result must match the
        # control under the current spec.
        second = db.execute("EXECUTE q").fetchall()
        db.execute("SET pg_deltax.json_extract_mode = 'none'")
        control = db.execute("""
            SELECT data ->> 'kind' AS kind, count(*)
            FROM events GROUP BY 1 ORDER BY 1
        """).fetchall()

        assert first == control, f"first execute differs: {first} vs {control}"
        assert second == control, (
            f"prepared plan returned stale results after enable_compression change: "
            f"{second} vs control {control}"
        )

    def test_groupby_physical_ts_extract_with_synthetic(self, db):
        """Regression for issue #4: mixing a physical-column EXTRACT and a
        JSONB chain in GROUP BY crashed planning with `cache lookup
        failed for attribute N of relation X`. The Extract shape (a) over
        the physical `ts` Var sets `group_by_relid` to the parent rel;
        the chain synthetic group spec carries `col_idx` past the
        parent's pg_attribute count, so resolving its name via
        `get_attname(group_by_relid, col_idx + 1)` errored out.

        The bug fires only when the planner's ndistinct heuristic block
        runs, which is gated on `companion_oids` being non-empty (i.e.
        the partition is compressed). Use the direct-backfill COPY so
        the compression happens deterministically without depending on
        the background worker."""
        db.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
        db.execute("""
            CREATE TABLE events (
                ts TIMESTAMPTZ NOT NULL,
                data JSONB NOT NULL
            )
        """)
        db.execute(
            "SELECT deltax_create_table('events', 'ts', '1 day'::interval, 5)"
        )
        _enable_extracts(db, segment_size=50)
        db.execute("SET pg_deltax.json_extract_mode = 'fields'")
        db.commit()

        # Direct-backfill into the first-day partition. Generates 200
        # rows across two hours so EXTRACT(HOUR FROM ts) yields >1 group.
        collections = [
            "app.bsky.feed.post",
            "app.bsky.feed.repost",
            "app.bsky.feed.like",
        ]
        rows = []
        for i in range(200):
            ts = f"2025-01-15 {(i // 100):02d}:{(i % 60):02d}:00+00"
            payload = {
                "kind": "commit",
                "did": f"did:plc:{i % 10:03d}",
                "time_us": 1700000000000000 + i * 1000,
                "commit": {
                    "collection": collections[i % 3],
                    "operation": "create",
                    "rkey": f"rec{i}",
                },
            }
            rows.append((ts, json.dumps(payload)))
        text = "\n".join(f"{ts}\t{payload}" for ts, payload in rows) + "\n"
        with db.cursor() as cur:
            with cur.copy(
                "COPY events FROM STDIN WITH (FORMAT deltax_compress)"
            ) as cp:
                cp.write(text)
        db.commit()

        none, fields = _ab(db, """
            SELECT data->'commit'->>'collection' AS event,
                   EXTRACT(HOUR FROM ts) AS hour_of_day,
                   COUNT(*) AS count
            FROM events
            WHERE data->>'kind' = 'commit'
              AND data->'commit'->>'operation' = 'create'
              AND data->'commit'->>'collection' IN
                  ('app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like')
            GROUP BY event, hour_of_day
            ORDER BY hour_of_day, event
        """)
        assert none == fields
        assert len(fields) >= 1

    def test_chain_in_in_clause(self, db):
        """`ScalarArrayOpExpr` is `chain IN ('a', 'b', 'c')` after PG's
        normalization. Walker must descend into it on both the rewrite
        side (substitute) and the ref-count side (pull_var_clause covers
        it for free)."""
        _setup(db); _enable_extracts(db); _insert(db)
        none, fields = _ab(db, """
            SELECT data -> 'commit' ->> 'collection' AS coll, count(*)
            FROM events
            WHERE data ->> 'kind' = 'commit'
              AND data -> 'commit' ->> 'collection'
                  IN ('app.bsky.feed.post', 'app.bsky.feed.like')
            GROUP BY 1 ORDER BY 1
        """)
        assert none == fields
        assert len(fields) >= 1


# ---------------------------------------------------------------------------
# JSONBench query suite — Q0..Q4 transcribed from `jsonbench/queries.sql`.
# Each test runs both `mode='fields'` (rewrite active) and `mode='none'`
# (slow-path control) and asserts result equality. The synthetic data
# fixture is shaped to ensure each filter returns a non-empty result so an
# empty-set match isn't mistaken for correctness.
# ---------------------------------------------------------------------------

JSONBENCH_QUERIES = [
    (
        "q0_groupby_kind",
        """
        SELECT data ->> 'kind' AS event, COUNT(*) AS count
        FROM events
        GROUP BY event
        ORDER BY count DESC, event
        """,
        # Expected: 3 groups (one per kind).
        3,
    ),
    (
        "q1_collection_count_distinct_did",
        """
        SELECT data -> 'commit' ->> 'collection' AS event,
               COUNT(*) AS count,
               COUNT(DISTINCT data ->> 'did') AS users
        FROM events
        WHERE data ->> 'kind' = 'commit'
          AND data -> 'commit' ->> 'operation' = 'create'
        GROUP BY event
        ORDER BY count DESC, event
        """,
        # Expected: up to 5 collections (uniformly chosen).
        5,
    ),
    (
        "q2_collection_hour_count",
        """
        SELECT data->'commit'->>'collection' AS event,
               EXTRACT(HOUR FROM TO_TIMESTAMP((data->>'time_us')::BIGINT / 1000000)) AS hour_of_day,
               COUNT(*) AS count
        FROM events
        WHERE data->>'kind' = 'commit'
          AND data->'commit'->>'operation' = 'create'
          AND data->'commit'->>'collection' IN ('app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like')
        GROUP BY event, hour_of_day
        ORDER BY hour_of_day, event
        """,
        # Expected: 3 collections × however many hours the data spans (>= 1).
        3,
    ),
    (
        "q3_first_post_per_user",
        """
        SELECT data->>'did' AS user_id,
               MIN(TIMESTAMP WITH TIME ZONE 'epoch'
                   + INTERVAL '1 microsecond' * (data->>'time_us')::BIGINT) AS first_post_ts
        FROM events
        WHERE data->>'kind' = 'commit'
          AND data->'commit'->>'operation' = 'create'
          AND data->'commit'->>'collection' = 'app.bsky.feed.post'
        GROUP BY user_id
        ORDER BY first_post_ts ASC
        LIMIT 3
        """,
        3,
    ),
    (
        "q4_activity_span_per_user",
        """
        SELECT data->>'did' AS user_id,
               EXTRACT(EPOCH FROM (
                   MAX(TIMESTAMP WITH TIME ZONE 'epoch'
                       + INTERVAL '1 microsecond' * (data->>'time_us')::BIGINT)
                 - MIN(TIMESTAMP WITH TIME ZONE 'epoch'
                       + INTERVAL '1 microsecond' * (data->>'time_us')::BIGINT)
               )) * 1000 AS activity_span
        FROM events
        WHERE data->>'kind' = 'commit'
          AND data->'commit'->>'operation' = 'create'
          AND data->'commit'->>'collection' = 'app.bsky.feed.post'
        GROUP BY user_id
        ORDER BY activity_span DESC, user_id
        LIMIT 3
        """,
        3,
    ),
]


class TestWalkerForwarderGate:
    """The post-`standard_planner` walker rebuilds each touched cscan's
    `custom_scan_tlist` from the deltatable catalog (physical columns +
    one synthetic per `json_extract` spec) and extends
    `scan.plan.targetlist` with synthetic forwarder TargetEntries so
    upper plans' `Var(OUTER_VAR, k)` refs resolve. Without a gate, the
    extension fires for every synthetic in cstlist regardless of
    whether any plan node references it — which silently widens cscan
    output. That's harmless for non-Append plans (PG projection
    narrows it) but for `Append`/`MergeAppend` over a mix of
    compressed (cscan, gets +1 col) and uncompressed (SeqScan, no
    forwarder) partitions, child output widths disagree and PG hits
    `unexpected field count in 'D' message`."""

    def test_chain_unreferenced_query_over_mixed_partitions(self, db):
        """Regression for the protocol-level "unexpected field count"
        bug: query touches only physical columns + spans an Append over
        compressed and uncompressed partitions. With the gate, every
        Append child has the same output width and PG accepts the
        result; without, the cscan child emits an extra synthetic
        column and the protocol decoder errors out."""
        db.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
        db.execute("""
            CREATE TABLE events (
                ts TIMESTAMPTZ NOT NULL,
                kind TEXT NOT NULL,
                data JSONB NOT NULL
            )
        """)
        db.execute("SELECT deltax_create_table('events', 'ts', '1 day'::interval, 5)")
        db.execute(
            "SELECT deltax_enable_compression('events', "
            "order_by => ARRAY['ts'], segment_size => 50, "
            "json_extract => '[{\"src\":\"data\","
            "\"path\":[\"terminal\"],\"name\":\"x_terminal\",\"type\":\"text\"}]'::jsonb)"
        )
        db.execute("SET pg_deltax.json_extract_mode = 'fields'")
        db.commit()

        # Load via direct-backfill into the first day's partition. The
        # subsequent (premake'd) partitions stay empty + uncompressed,
        # which puts both flavours in the same Append at query time.
        rows = []
        for i in range(50):
            rows.append((f"2025-01-15 {i // 60:02d}:{i % 60:02d}:00+00",
                         "Created" if i % 2 else "Delivered",
                         json.dumps({"terminal": "Berlin"})))
        text = "\n".join("\t".join(c for c in r) for r in rows) + "\n"
        with db.cursor() as cur:
            with cur.copy("COPY events FROM STDIN WITH (FORMAT deltax_compress)") as cp:
                cp.write(text)
        db.commit()

        # Query references only physical columns. With the gate broken,
        # `psycopg.DatabaseError: unexpected field count in "D" message`
        # fires here.
        rows = db.execute(
            "SELECT ts, kind FROM events WHERE kind = 'Delivered' "
            "ORDER BY ts LIMIT 5"
        ).fetchall()
        assert len(rows) == 5
        assert all(r[1] == "Delivered" for r in rows)

    def test_chain_unreferenced_direct_feed_join(self, db):
        """Regression for the direct-feed JOIN returning 0 rows: cscan
        output feeds straight into a Hash/NestLoop join (no Materialize,
        no CTE in between) on a query that doesn't reference any chain
        Expr. Without the gate, the cscan's slot tuple descriptor is
        widened by `rebuild_custom_scan_tlist_from_catalog` adding
        synthetics, but PG's `set_customscan_references` resolved
        `Var(OUTER_VAR, k)` against the original (un-widened) shape —
        the join's probe side reads the wrong physical slot position
        and every comparison fails."""
        db.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
        db.execute("""
            CREATE TABLE oe (
                order_id integer NOT NULL,
                event_created timestamptz NOT NULL,
                event_type text NOT NULL,
                data jsonb
            )
        """)
        db.execute("CREATE TABLE oi (order_id integer NOT NULL, amount integer NOT NULL)")
        db.execute("SELECT deltax_create_table('oe', 'event_created', '1 day'::interval, 5)")
        db.execute(
            "SELECT deltax_enable_compression('oe', "
            "order_by => ARRAY['order_id','event_created'], segment_size => 50, "
            "json_extract => '[{\"src\":\"data\","
            "\"path\":[\"terminal\"],\"name\":\"x_terminal\",\"type\":\"text\"}]'::jsonb)"
        )
        db.execute("SET pg_deltax.json_extract_mode = 'fields'")
        db.commit()

        rows = []
        for oid in range(1, 11):
            for i in range(3):
                rows.append((str(oid), f"2025-01-{15 + i % 3:02d} 10:00:00+00",
                             "Delivered", json.dumps({"terminal": "Berlin"})))
        text = "\n".join("\t".join(r) for r in rows) + "\n"
        with db.cursor() as cur:
            with cur.copy("COPY oe FROM STDIN WITH (FORMAT deltax_compress)") as cp:
                cp.write(text)
            i_text = "\n".join(f"{i}\t1" for i in range(1, 11)) + "\n"
            with cur.copy("COPY oi FROM STDIN") as cp:
                cp.write(i_text)
        db.commit()

        # Direct-feed Hash Join — no Materialize, no CTE.
        n = db.execute(
            "SELECT count(*) FROM oe JOIN oi USING (order_id) "
            "WHERE event_type='Delivered'"
        ).fetchone()[0]
        # 30 events × 1 matching item each = 30.
        assert n == 30, f"direct-feed join: expected 30 rows, got {n}"

    def test_grouping_sets_with_walker_active(self, db):
        """Regression for the `unrecognized node type` error on Agg.chain:
        a query with GROUPING SETS that has no chain Expr, no synthetic
        column in its plan, but runs while the walker is active. Without
        the fix, the walker passed `Agg.chain` (a List of *plan* nodes —
        the GROUPING SETS rollup) to `pull_var_clause` (an
        expression-tree walker), which errored on T_Agg = node 361 in
        PG17."""
        # Set up a table with json_extract configured (to activate the
        # walker) — but the test query won't touch the chain.
        db.execute(f"SET pg_deltax.mock_now = '{MOCK_NOW}'")
        db.execute("""
            CREATE TABLE events_gs (
                ts TIMESTAMPTZ NOT NULL,
                country TEXT,
                state TEXT,
                amount INTEGER,
                data JSONB
            )
        """)
        db.execute(
            "SELECT deltax_create_table('events_gs', 'ts', '1 day'::interval, 5)"
        )
        db.execute(
            "SELECT deltax_enable_compression('events_gs', "
            "order_by => ARRAY['ts'], segment_size => 50, "
            "json_extract => '[{\"src\":\"data\","
            "\"path\":[\"terminal\"],\"name\":\"x_terminal\",\"type\":\"text\"}]'::jsonb)"
        )
        db.execute("SET pg_deltax.json_extract_mode = 'fields'")
        db.commit()

        rows = []
        for i in range(20):
            rows.append((f"2025-01-15 {i:02d}:00:00+00",
                         "DE" if i % 2 else "US",
                         "Berlin" if i % 3 == 0 else "Hamburg",
                         str(i + 1),
                         json.dumps({"terminal": "T"})))
        text = "\n".join("\t".join(r) for r in rows) + "\n"
        with db.cursor() as cur:
            with cur.copy("COPY events_gs FROM STDIN WITH (FORMAT deltax_compress)") as cp:
                cp.write(text)
        db.commit()

        # GROUPING SETS query — without the fix, this errored with
        # `unrecognized node type: 361` during planning.
        rows = db.execute(
            "SELECT country, state, sum(amount) FROM events_gs "
            "GROUP BY GROUPING SETS ((country), (country, state), ()) "
            "ORDER BY country NULLS LAST, state NULLS LAST"
        ).fetchall()
        # Always > 0 since GROUPING SETS includes the empty-group row.
        assert len(rows) > 0


class TestMixedPartitionGate:
    """Partitions compressed before `json_extract` was added don't have
    synthetic columns in their companion blobs. The walker must detect
    this and skip the rewrite — otherwise old partitions would emit NULLs
    at synthetic positions, silently corrupting query results.
    """

    def test_old_partition_still_returns_correct_results(self, db):
        # Step 1: enable_compression WITHOUT json_extract.
        # Step 2: insert into "old" partition + force compression.
        # Step 3: enable_compression with json_extract added.
        # Step 4: insert into "new" partition + force compression.
        # Step 5: query touching both partitions — must produce correct
        #         results for both. The chain Exprs in the query MUST
        #         resolve correctly even on the old partition.
        old_ts = "2024-12-01 00:00:00+00"
        new_ts = "2025-01-15 00:00:00+00"
        # mock_now sits between the two partitions so both fall within
        # the 1-day partition window of their respective dates.
        mock_now = "2025-01-16 12:00:00+00"

        db.execute(f"SET pg_deltax.mock_now = '{mock_now}'")
        db.execute("""
            CREATE TABLE events (
                ts TIMESTAMPTZ NOT NULL,
                data JSONB NOT NULL
            )
        """)
        db.execute(
            "SELECT deltax_create_table('events', 'ts', '1 day'::interval, 365)"
        )
        db.commit()

        # 1) compression enabled without json_extract.
        db.execute("""
            SELECT deltax_enable_compression(
                'events', order_by => ARRAY['ts'], segment_size => 100
            )
        """)
        db.commit()

        # 2) load + compress the OLD partition.
        old_rows = ", ".join(
            f"('{old_ts}'::timestamptz + interval '{i} seconds', "
            f"'{{\"kind\":\"commit\",\"did\":\"u{i % 10}\","
            f"\"commit\":{{\"collection\":\"app.bsky.feed.post\",\"operation\":\"create\"}}}}'::jsonb)"
            for i in range(50)
        )
        db.execute(f"INSERT INTO events (ts, data) VALUES {old_rows}")
        db.commit()
        # Compress all partitions that contain rows so far.
        db.execute(
            "SELECT deltax_compress_partition(table_name) FROM deltax_partition "
            "WHERE deltatable_id = (SELECT id FROM deltax_deltatable WHERE table_name = 'events') "
            "AND row_count > 0"
        )
        db.commit()

        # 3) re-enable_compression with json_extract added.
        db.execute("""
            SELECT deltax_enable_compression(
                'events',
                order_by => ARRAY['ts'],
                segment_size => 100,
                json_extract => '[
                    {"src":"data","path":["kind"],"name":"x_kind","type":"text"},
                    {"src":"data","path":["did"],"name":"x_did","type":"text"},
                    {"src":"data","path":["commit","collection"],"name":"x_collection","type":"text"}
                ]'::jsonb
            )
        """)
        db.commit()

        # 4) load + compress the NEW partition.
        new_rows = ", ".join(
            f"('{new_ts}'::timestamptz + interval '{i} seconds', "
            f"'{{\"kind\":\"commit\",\"did\":\"u{i % 10}\","
            f"\"commit\":{{\"collection\":\"app.bsky.feed.post\",\"operation\":\"create\"}}}}'::jsonb)"
            for i in range(50)
        )
        db.execute(f"INSERT INTO events (ts, data) VALUES {new_rows}")
        db.commit()
        db.execute(
            "SELECT deltax_compress_partition(table_name) FROM deltax_partition "
            "WHERE deltatable_id = (SELECT id FROM deltax_deltatable WHERE table_name = 'events') "
            "AND row_count > 0 AND NOT is_compressed"
        )
        db.commit()

        # 5) query touching both partitions; mode='fields' must produce
        #    same results as mode='none'. With the gate engaged, the walker
        #    falls through to the slow path for the parent baserel because
        #    one of the partitions predates json_extract_added_at.
        db.execute("SET pg_deltax.json_extract_mode = 'none'")
        none = db.execute("""
            SELECT data ->> 'kind' AS kind, count(*)
            FROM events
            GROUP BY 1 ORDER BY 1
        """).fetchall()

        db.execute("SET pg_deltax.json_extract_mode = 'fields'")
        fields = db.execute("""
            SELECT data ->> 'kind' AS kind, count(*)
            FROM events
            GROUP BY 1 ORDER BY 1
        """).fetchall()

        assert none == fields
        assert len(fields) >= 1
        # Both partitions contributed rows.
        assert dict(fields).get("commit") == 100, (
            f"expected 100 commit rows across both partitions; got {fields}"
        )


class TestJsonBenchQueryCorrectness:
    """Each JSONBench query produces identical results under
    `json_extract_mode='fields'` (rewrite path) and `'none'` (slow path).
    Catches regressions like the prior unconditional-prune walker, which
    silently produced empty results because it dropped `data` from
    needed-cols and the chain-Expr filter at scan level evaluated to NULL.
    """

    @pytest.mark.parametrize(
        "name, query, expected_min_rows",
        JSONBENCH_QUERIES,
        ids=[name for name, _, _ in JSONBENCH_QUERIES],
    )
    def test_jsonbench(self, db, name, query, expected_min_rows):
        _setup(db); _enable_extracts(db); _insert(db)
        none, fields = _ab(db, query)
        assert none == fields, f"{name}: results differ between mode='fields' and mode='none'"
        assert len(fields) >= expected_min_rows, (
            f"{name}: only {len(fields)} rows; data fixture or filter pushdown is wrong"
        )


class TestJsonBenchQ2AggPushdown:
    """JSONBench Q2 groups by `EXTRACT(HOUR FROM TO_TIMESTAMP(<bigint
    chain> / 1_000_000))`. The recognizer in `hook.rs::extract` peeks
    through the `to_timestamp(... / Const)` shape so this lands in
    DeltaXAgg instead of the fallback DeltaXAppend → Sort → GroupAgg
    plan that disk-spills 416 MB / worker on the full bench."""

    def test_extract_hour_from_to_timestamp_correctness(self, db):
        """End-to-end: the bigint-scaled extract produces the same hour
        values whether the walker is active (mode='fields') or not
        (mode='none'). The full Q2 shape is also covered by
        `TestJsonBenchQueryCorrectness::test_jsonbench[q2_...]`; this
        narrower variant pins the specific recognizer pattern in
        isolation."""
        _setup(db); _enable_extracts(db); _insert(db)
        none, fields = _ab(db, """
            SELECT EXTRACT(HOUR FROM TO_TIMESTAMP((data->>'time_us')::BIGINT / 1000000)) AS hour,
                   COUNT(*)
            FROM events
            WHERE data->>'kind' = 'commit'
            GROUP BY hour
            ORDER BY hour
        """)
        assert none == fields, (
            f"Hour-of-day group counts differ between mode='none' "
            f"and mode='fields':\n  none={none}\n  fields={fields}"
        )
        assert len(fields) > 0, "no hour groups; fixture or pushdown broken"
