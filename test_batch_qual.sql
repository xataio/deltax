CREATE EXTENSION pg_seaturtle;
SET client_min_messages = log;

-- Mimics clickbench schema with many columns, AdvEngineID at column ~42
CREATE TABLE hits_mini (
    WatchID BIGINT NOT NULL,
    JavaEnable SMALLINT NOT NULL,
    Title TEXT NOT NULL,
    GoodEvent SMALLINT NOT NULL,
    EventTime TIMESTAMPTZ NOT NULL,
    EventDate DATE NOT NULL,
    CounterID INTEGER NOT NULL,
    ClientIP INTEGER NOT NULL,
    RegionID INTEGER NOT NULL,
    UserID BIGINT NOT NULL,
    CounterClass SMALLINT NOT NULL,
    OS SMALLINT NOT NULL,
    UserAgent SMALLINT NOT NULL,
    URL TEXT NOT NULL,
    Referer TEXT NOT NULL,
    IsRefresh SMALLINT NOT NULL,
    RefererCategoryID SMALLINT NOT NULL,
    RefererRegionID INTEGER NOT NULL,
    URLCategoryID SMALLINT NOT NULL,
    URLRegionID INTEGER NOT NULL,
    ResolutionWidth SMALLINT NOT NULL,
    ResolutionHeight SMALLINT NOT NULL,
    ResolutionDepth SMALLINT NOT NULL,
    FlashMajor SMALLINT NOT NULL,
    FlashMinor SMALLINT NOT NULL,
    FlashMinor2 TEXT NOT NULL,
    NetMajor SMALLINT NOT NULL,
    NetMinor SMALLINT NOT NULL,
    UserAgentMajor SMALLINT NOT NULL,
    UserAgentMinor VARCHAR(255) NOT NULL,
    CookieEnable SMALLINT NOT NULL,
    JavascriptEnable SMALLINT NOT NULL,
    IsMobile SMALLINT NOT NULL,
    MobilePhone SMALLINT NOT NULL,
    MobilePhoneModel TEXT NOT NULL,
    Params TEXT NOT NULL,
    IPNetworkID INTEGER NOT NULL,
    TraficSourceID SMALLINT NOT NULL,
    SearchEngineID SMALLINT NOT NULL,
    SearchPhrase TEXT NOT NULL,
    AdvEngineID SMALLINT NOT NULL,
    IsArtifical SMALLINT NOT NULL,
    WindowClientWidth SMALLINT NOT NULL,
    WindowClientHeight SMALLINT NOT NULL,
    ClientTimeZone SMALLINT NOT NULL,
    ClientEventTime TIMESTAMPTZ NOT NULL
);

SELECT seaturtle_create_table('hits_mini', 'eventtime', interval '1 day', premake => 2);

-- Insert 1000 rows: 99% have AdvEngineID=0, 1% have AdvEngineID=2
INSERT INTO hits_mini
SELECT
    i::bigint,             -- WatchID
    1::smallint,           -- JavaEnable
    'title'::text,         -- Title
    1::smallint,           -- GoodEvent
    '2026-03-03 00:00:00+00'::timestamptz + (i * interval '1 second'),  -- EventTime
    '2026-03-03'::date,    -- EventDate
    62,                    -- CounterID
    0,                     -- ClientIP
    0,                     -- RegionID
    i::bigint,             -- UserID
    0::smallint,           -- CounterClass
    0::smallint,           -- OS
    0::smallint,           -- UserAgent
    ''::text,              -- URL
    ''::text,              -- Referer
    0::smallint,           -- IsRefresh
    0::smallint,           -- RefererCategoryID
    0,                     -- RefererRegionID
    0::smallint,           -- URLCategoryID
    0::smallint,           -- URLRegionID
    0::smallint,           -- ResolutionWidth
    0::smallint,           -- ResolutionHeight
    0::smallint,           -- ResolutionDepth
    0::smallint,           -- FlashMajor
    0::smallint,           -- FlashMinor
    ''::text,              -- FlashMinor2
    0::smallint,           -- NetMajor
    0::smallint,           -- NetMinor
    0::smallint,           -- UserAgentMajor
    ''::varchar(255),      -- UserAgentMinor
    0::smallint,           -- CookieEnable
    0::smallint,           -- JavascriptEnable
    0::smallint,           -- IsMobile
    0::smallint,           -- MobilePhone
    ''::text,              -- MobilePhoneModel
    ''::text,              -- Params
    0,                     -- IPNetworkID
    0::smallint,           -- TraficSourceID
    0::smallint,           -- SearchEngineID
    ''::text,              -- SearchPhrase
    CASE WHEN i % 100 = 0 THEN 2 ELSE 0 END::smallint,  -- AdvEngineID (99% = 0)
    0::smallint,           -- IsArtifical
    0::smallint,           -- WindowClientWidth
    0::smallint,           -- WindowClientHeight
    0::smallint,           -- ClientTimeZone
    '2026-03-03 00:00:00+00'::timestamptz + (i * interval '1 second')  -- ClientEventTime
FROM generate_series(1, 1000) AS i;

-- Compress
SELECT seaturtle_enable_compression('hits_mini', segment_by => ARRAY['counterid'], order_by => ARRAY['eventtime']);

DO $$
DECLARE
    pname TEXT;
BEGIN
    FOR pname IN
        SELECT partition_name FROM seaturtle_partition_info('hits_mini')
        WHERE NOT is_compressed
    LOOP
        PERFORM seaturtle_compress_partition(pname);
    END LOOP;
END $$;

-- Q2 pattern: COUNT(*) WHERE AdvEngineID <> 0
EXPLAIN (ANALYZE, COSTS OFF) SELECT COUNT(*) FROM hits_mini WHERE AdvEngineID <> 0;
