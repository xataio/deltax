-- Test multi-aggregate MIN/MAX pushdown on non-time columns
CREATE EXTENSION IF NOT EXISTS pg_seaturtle;
SET pg_seaturtle.mock_now = '2025-01-15 12:00:00+00';

-- Create table with multiple numeric/date columns
CREATE TABLE test_minmax (
    ts TIMESTAMPTZ NOT NULL,
    event_date DATE NOT NULL,
    counter_id INTEGER NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    small_val SMALLINT NOT NULL
);

SELECT seaturtle_create_table('test_minmax', 'ts');
SELECT seaturtle_enable_compression('test_minmax', order_by => ARRAY['ts']);

-- Insert data into a single partition (2025-01-14)
INSERT INTO test_minmax (ts, event_date, counter_id, value, small_val)
SELECT
    '2025-01-14 00:00:00+00'::timestamptz + (i || ' seconds')::interval,
    '2025-01-10'::date + (i % 7),
    (i % 100) + 1,
    (i % 1000)::double precision + 0.5,
    (i % 32000)::smallint
FROM generate_series(1, 10000) AS i;

-- Show partition info
SELECT table_name FROM seaturtle_partition WHERE table_name LIKE 'test_minmax%' ORDER BY range_start;

-- Compress the partition that has data
SELECT seaturtle_compress_partition('test_minmax_p20250114');

-- Test 1: Single MIN on non-time column
EXPLAIN (COSTS OFF) SELECT MIN(event_date) FROM test_minmax;
SELECT MIN(event_date) FROM test_minmax;

-- Test 2: Single MAX on non-time column
EXPLAIN (COSTS OFF) SELECT MAX(counter_id) FROM test_minmax;
SELECT MAX(counter_id) FROM test_minmax;

-- Test 3: Multi-aggregate - MIN and MAX of same column (like Q7)
EXPLAIN (COSTS OFF) SELECT MIN(event_date), MAX(event_date) FROM test_minmax;
SELECT MIN(event_date), MAX(event_date) FROM test_minmax;

-- Test 4: Multi-aggregate - different columns
EXPLAIN (COSTS OFF) SELECT MIN(counter_id), MAX(counter_id) FROM test_minmax;
SELECT MIN(counter_id), MAX(counter_id) FROM test_minmax;

-- Test 5: MIN/MAX on float column
EXPLAIN (COSTS OFF) SELECT MIN(value), MAX(value) FROM test_minmax;
SELECT MIN(value), MAX(value) FROM test_minmax;

-- Cleanup
DROP TABLE test_minmax CASCADE;
