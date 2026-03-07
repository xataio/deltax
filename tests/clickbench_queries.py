"""Selected ClickBench queries for pg_seaturtle benchmark.

15 queries chosen from the 43 ClickBench queries to cover different access patterns:
- Full scans (Q1, Q3, Q7)
- Filtered scans (Q2, Q8)
- Heavy aggregations (Q5, Q9)
- Text-heavy (Q13, Q21, Q34)
- CounterID-filtered / segment_by pruning (Q37, Q38, Q43)
- Point lookup (Q20)
- Time-ordered (Q25)

Note: EventDate references are changed to use EventTime where needed for
TIMESTAMPTZ compatibility, and TIMESTAMP columns are treated as TIMESTAMPTZ.
"""

QUERIES = [
    # Q1: COUNT(*) — full scan baseline
    (
        "Q1",
        "COUNT(*)",
        "SELECT COUNT(*) FROM hits",
    ),
    # Q2: Filtered count — WHERE AdvEngineID <> 0
    (
        "Q2",
        "COUNT WHERE AdvEngineID",
        "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0",
    ),
    # Q3: SUM/AVG aggregation — full scan
    (
        "Q3",
        "SUM/AVG full scan",
        "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits",
    ),
    # Q5: COUNT DISTINCT — heavy aggregation
    (
        "Q5",
        "COUNT DISTINCT UserID",
        "SELECT COUNT(DISTINCT UserID) FROM hits",
    ),
    # Q7: MIN/MAX dates
    (
        "Q7",
        "MIN/MAX EventDate",
        "SELECT MIN(EventDate), MAX(EventDate) FROM hits",
    ),
    # Q8: GROUP BY AdvEngineID
    (
        "Q8",
        "GROUP BY AdvEngineID",
        "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 "
        "GROUP BY AdvEngineID ORDER BY COUNT(*) DESC",
    ),
    # Q9: GROUP BY RegionID with DISTINCT
    (
        "Q9",
        "GROUP BY RegionID",
        "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits "
        "GROUP BY RegionID ORDER BY u DESC LIMIT 10",
    ),
    # Q13: SearchPhrase top — text-heavy
    (
        "Q13",
        "Top SearchPhrase",
        "SELECT SearchPhrase, COUNT(*) AS c FROM hits "
        "WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10",
    ),
    # Q20: Point lookup by UserID
    (
        "Q20",
        "Point lookup UserID",
        "SELECT UserID FROM hits WHERE UserID = 435090932899640449",
    ),
    # Q21: URL LIKE — text scan
    (
        "Q21",
        "URL LIKE google",
        "SELECT COUNT(*) FROM hits WHERE URL LIKE '%google%'",
    ),
    # Q25: Time-ordered scan
    (
        "Q25",
        "ORDER BY EventTime",
        "SELECT SearchPhrase FROM hits WHERE SearchPhrase <> '' "
        "ORDER BY EventTime, SearchPhrase LIMIT 10",
    ),
    # Q34: Top URLs — heavy GROUP BY text
    (
        "Q34",
        "Top URLs",
        "SELECT URL, COUNT(*) AS c FROM hits GROUP BY URL ORDER BY c DESC LIMIT 10",
    ),
    # Q37: CounterID-filtered (segment_by pruning)
    (
        "Q37",
        "CounterID=62 URLs",
        "SELECT URL, COUNT(*) AS PageViews FROM hits "
        "WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' "
        "AND DontCountHits = 0 AND IsRefresh = 0 AND URL <> '' "
        "GROUP BY URL ORDER BY PageViews DESC LIMIT 10",
    ),
    # Q38: CounterID-filtered titles
    (
        "Q38",
        "CounterID=62 Titles",
        "SELECT Title, COUNT(*) AS PageViews FROM hits "
        "WHERE CounterID = 62 AND EventDate >= '2013-07-01' AND EventDate <= '2013-07-31' "
        "AND DontCountHits = 0 AND IsRefresh = 0 AND Title <> '' "
        "GROUP BY Title ORDER BY PageViews DESC LIMIT 10",
    ),
    # Q43: CounterID-filtered time aggregation
    (
        "Q43",
        "CounterID=62 by minute",
        "SELECT DATE_TRUNC('minute', EventTime) AS M, COUNT(*) AS PageViews FROM hits "
        "WHERE CounterID = 62 AND EventDate >= '2013-07-14' AND EventDate <= '2013-07-15' "
        "AND IsRefresh = 0 AND DontCountHits = 0 "
        "GROUP BY DATE_TRUNC('minute', EventTime) "
        "ORDER BY DATE_TRUNC('minute', EventTime) LIMIT 10 OFFSET 1000",
    ),
]
