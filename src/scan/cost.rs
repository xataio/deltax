use pgrx::pg_sys;
use pgrx::prelude::*;
use std::cell::RefCell;
use std::collections::HashMap;

thread_local! {
    /// Cache of companion_oid → (row_count, segment_count) from deltax.deltax_partition.
    /// Only populated on successful lookups; misses are not cached because
    /// companion lookups can race with partition creation.
    static PARTITION_STATS_CACHE: RefCell<HashMap<pg_sys::Oid, (i64, i64)>> =
        RefCell::new(HashMap::new());

    /// Cache of companion_oid → per-column ndistinct counts from companion table.
    /// An empty map is a valid cached value (stable schema shape with no
    /// `_ndistinct_*` columns).
    static NDISTINCT_CACHE: RefCell<HashMap<pg_sys::Oid, HashMap<String, i64>>> =
        RefCell::new(HashMap::new());

    /// Cache of companion_oid → per-column value→bit_idx maps for the segment
    /// value-presence bitmap. Empty map = no eligible columns for this
    /// partition (no low-card text columns).
    static VALMAP_CACHE: RefCell<HashMap<pg_sys::Oid, HashMap<String, Vec<String>>>> =
        RefCell::new(HashMap::new());
}

/// Clear all cost-related caches. Called from `hook::invalidate_compressed_cache`.
pub(super) fn invalidate_caches() {
    PARTITION_STATS_CACHE.with(|cache| cache.borrow_mut().clear());
    NDISTINCT_CACHE.with(|cache| cache.borrow_mut().clear());
    VALMAP_CACHE.with(|cache| cache.borrow_mut().clear());
}

/// Estimate the cost and row count for scanning a compressed partition.
/// Returns (startup_cost, total_cost, estimated_rows).
///
/// When `workers > 0`, applies PG's parallel divisor to non-startup cost and
/// row count so callers building a partial path see per-worker values.
pub unsafe fn estimate_cost(companion_oid: pg_sys::Oid, workers: usize) -> (f64, f64, f64) {
    let (total_rows, segment_count) = get_partition_stats(companion_oid);

    let rows = if total_rows > 0 {
        total_rows as f64
    } else {
        let rel_tuples = unsafe { get_reltuples(companion_oid) };
        let segments = if rel_tuples > 0.0 { rel_tuples } else { 1.0 };
        segments * 10000.0
    };

    let startup = 10.0;
    let segs = if segment_count > 0 {
        segment_count as f64
    } else {
        (rows / 10000.0).max(1.0)
    };
    let per_segment = 100.0;
    let per_row = 0.1;
    let total = startup + segs * per_segment + rows * per_row;

    if workers > 0 {
        let div = parallel_divisor(workers);
        let non_startup = total - startup;
        return (startup, startup + non_startup / div, rows / div);
    }

    (startup, total, rows)
}

/// Mirror PG's `get_parallel_divisor` in `costsize.c`: workers contribute
/// fully, leader contribution decays at 0.3/worker, clamped to ≥ 0.
pub(crate) fn parallel_divisor(workers: usize) -> f64 {
    let w = workers as f64;
    let leader = (1.0 - 0.3 * w).max(0.0);
    w + leader
}

/// Get partition stats from deltax.deltax_partition catalog.
fn get_partition_stats(companion_oid: pg_sys::Oid) -> (i64, i64) {
    if let Some(cached) =
        PARTITION_STATS_CACHE.with(|cache| cache.borrow().get(&companion_oid).copied())
    {
        return cached;
    }

    let name = unsafe {
        let name_ptr = pg_sys::get_rel_name(companion_oid);
        if name_ptr.is_null() {
            return (0, 0);
        }
        std::ffi::CStr::from_ptr(name_ptr)
            .to_string_lossy()
            .into_owned()
    };
    // Strip _meta suffix to get the partition name for catalog lookup
    let partition_name = name.strip_suffix("_meta").unwrap_or(&name);

    let result = Spi::get_one_with_args::<i64>(
        "SELECT row_count FROM deltax.deltax_partition WHERE table_name = $1 AND is_compressed = true",
        &[partition_name.into()],
    );

    match result {
        Ok(Some(row_count)) => {
            let segments = (row_count / 100_000).max(1);
            let stats = (row_count, segments);
            PARTITION_STATS_CACHE.with(|cache| cache.borrow_mut().insert(companion_oid, stats));
            stats
        }
        // Do not cache misses: companion lookups can race with partition creation.
        _ => (0, 0),
    }
}

/// Get relpages from pg_class for a relation OID.
#[allow(dead_code)]
pub(super) unsafe fn get_relpages(rel_oid: pg_sys::Oid) -> i32 {
    unsafe {
        let tuple = pg_sys::SearchSysCache1(
            pg_sys::SysCacheIdentifier::RELOID as i32,
            pg_sys::ObjectIdGetDatum(rel_oid),
        );
        if tuple.is_null() {
            return 0;
        }
        let rel_form = pg_sys::GETSTRUCT(tuple) as pg_sys::Form_pg_class;
        let pages = (*rel_form).relpages;
        pg_sys::ReleaseSysCache(tuple);
        pages
    }
}

/// Get the uncompressed row count for a companion OID from deltax.deltax_partition catalog.
/// Returns Some(row_count) if positive, None otherwise.
pub(super) fn get_row_count(companion_oid: pg_sys::Oid) -> Option<i64> {
    let (row_count, _) = get_partition_stats(companion_oid);
    if row_count > 0 { Some(row_count) } else { None }
}

/// Realistic cost estimate for `DeltaXAgg` (see §5.8-b in
/// `dev/docs/RTABENCH_QUERY_ANALYSIS.md`).
///
/// Returns `(startup_cost, total_cost)` with standard PG semantics:
/// `startup_cost` is the cost to produce the first output row (= scan + per-row
/// aggregate evaluation, because GROUP BY can't emit until every row is
/// consumed), and `total_cost` adds the per-group output-emit cost.
///
/// Replaces the historic `(10.0, 20.0)` hack. The constants are calibrated
/// so that on every RTABench query shape, DeltaXAgg remains cheaper than the
/// alternative of `DeltaXAppend → Aggregate` — i.e. the planner still picks
/// the fused path — while the absolute numbers scale meaningfully with row
/// count so future parallel-partial paths can sit above/below serial based on
/// `parallel_setup_cost`.
///
/// Per-row/per-agg-expr coefficients are deliberately far below PG's
/// `cpu_tuple_cost (0.01)` / `cpu_operator_cost (0.0025)` because the
/// `DeltaXAgg` executor:
///   1. Parallelises scan + aggregate across Rust threads within the leader
///      process (`get_parallel_workers()` threads).
///   2. Avoids per-row PG heap-tuple materialisation — aggregates consume
///      decompressed columnar batches directly.
///   3. Has metadata / catalog fast paths (`DeltaXMinMax`, `DeltaXCount`)
///      for simple shapes; the formula here applies only when those don't
///      fire.
pub(super) fn estimate_agg_cost(
    companion_oids: &[pg_sys::Oid],
    num_agg_exprs: usize,
    estimated_groups: f64,
    num_having_filters: usize,
    workers: usize,
) -> (f64, f64) {
    // Calibrated against RTABench suite (Apr 2026). Adjusting any of these
    // risks regressing planner selection on a subset of queries; re-run
    // `make bench-rtabench` + the EC2 suite after tuning.
    const PER_PARTITION: f64 = 50.0; // metadata SPI + heap-scan startup
    const PER_ROW: f64 = 0.0005; // 20× below pg cpu_tuple_cost
    const PER_AGG_EXPR: f64 = 0.00005; // 50× below pg cpu_operator_cost
    const PER_GROUP: f64 = 0.01; // matches cpu_tuple_cost for output
    const PER_HAVING: f64 = 0.00005; // per-group HAVING eval

    let total_rows: f64 = companion_oids
        .iter()
        .map(|&oid| get_row_count(oid).unwrap_or(0) as f64)
        .sum();

    let num_partitions = companion_oids.len() as f64;
    let num_aggs = num_agg_exprs.max(1) as f64;
    let groups = estimated_groups.max(1.0);

    let mut scan_work = num_partitions * PER_PARTITION + total_rows * PER_ROW;
    let mut agg_work = total_rows * num_aggs * PER_AGG_EXPR;
    let having_work = groups * num_having_filters as f64 * PER_HAVING;
    let group_emit = groups * PER_GROUP;

    // Phase C.2.f — when workers > 0, the parallel-aware DeltaXAgg path
    // splits scan + per-row aggregate work across leader + workers via
    // `next_segment.fetch_add`; group emit and HAVING stay leader-side.
    if workers > 0 {
        let div = parallel_divisor(workers);
        scan_work /= div;
        agg_work /= div;
    }

    let startup = 10.0 + scan_work + agg_work + having_work;
    let total = startup + group_emit;

    (startup, total)
}

/// Phase C.2.f — recommend a worker count for the parallel-aware DeltaXAgg
/// path. Returns 0 when the table is too small to amortise parallel setup.
///
/// Heuristic: workers ≈ total_segments / 8 (mirrors DeltaXAppend's
/// MIN_SEGS_PER_WORKER), clamped to PG's `max_parallel_workers_per_gather`
/// and to `MAX_AGG_WORKER_SLOTS - 1` (DSM region accounts for one leader +
/// N workers). Below 16 segments we keep the path serial — overhead of
/// DSM setup + worker fork dominates.
pub(super) fn recommend_agg_workers(companion_oids: &[pg_sys::Oid]) -> i32 {
    let total_segments: i64 = companion_oids
        .iter()
        .map(|&oid| get_segment_count(oid))
        .sum();
    let pg_cap = unsafe { pg_sys::max_parallel_workers_per_gather };
    recommend_agg_workers_inner(total_segments, pg_cap)
}

/// Pure-Rust core of `recommend_agg_workers` so the threshold + clamp logic
/// can be unit-tested without a live PG instance.
fn recommend_agg_workers_inner(total_segments: i64, max_per_gather: i32) -> i32 {
    if total_segments < 16 {
        return 0;
    }
    const MIN_SEGS_PER_WORKER: i64 = 8;
    const MAX_AGG_WORKER_SLOTS_MINUS_ONE: i32 = (super::exec::MAX_AGG_WORKER_SLOTS as i32) - 1;
    let seg_floor = (total_segments / MIN_SEGS_PER_WORKER) as i32;
    seg_floor
        .min(max_per_gather)
        .clamp(0, MAX_AGG_WORKER_SLOTS_MINUS_ONE)
}

/// Get the estimated segment count for a companion OID (0 if unknown).
pub(super) fn get_segment_count(companion_oid: pg_sys::Oid) -> i64 {
    let (_, segments) = get_partition_stats(companion_oid);
    segments
}

/// Get per-column ndistinct for a companion OID from the catalog column
/// `deltax.deltax_partition.column_ndistinct` (populated at compression time).
/// Returns a map from column name to max-across-segments ndistinct count,
/// or an empty map if the partition has no stored ndistinct info.
///
/// This used to scan the whole meta table via `MAX(_ndistinct_*)`, which
/// was cheap warm but forced ~9 MB of cold reads on the meta table during
/// planning on every fresh backend. Now the info is persisted once at
/// compression time and read via a small catalog lookup.
pub(super) fn get_column_ndistinct(
    companion_oid: pg_sys::Oid,
) -> std::collections::HashMap<String, i64> {
    if let Some(cached) = NDISTINCT_CACHE.with(|cache| cache.borrow().get(&companion_oid).cloned())
    {
        return cached;
    }

    let companion_name = unsafe {
        let name_ptr = pg_sys::get_rel_name(companion_oid);
        if name_ptr.is_null() {
            return std::collections::HashMap::new();
        }
        std::ffi::CStr::from_ptr(name_ptr)
            .to_string_lossy()
            .into_owned()
    };
    // Strip _meta suffix to get the partition name for catalog lookup
    let partition_name = companion_name
        .strip_suffix("_meta")
        .unwrap_or(&companion_name);

    let mut result_map: std::collections::HashMap<String, i64> = std::collections::HashMap::new();

    // Retrieve the JSONB column as text and parse manually. This avoids
    // pulling in a JSON dependency just for a trivial `{string: int}` map.
    let json_text = Spi::get_one_with_args::<String>(
        "SELECT column_ndistinct::text FROM deltax.deltax_partition
         WHERE table_name = $1 AND is_compressed = true",
        &[partition_name.into()],
    );

    if let Ok(Some(text)) = json_text {
        parse_ndistinct_json(&text, &mut result_map);
    }

    NDISTINCT_CACHE.with(|cache| cache.borrow_mut().insert(companion_oid, result_map.clone()));
    result_map
}

/// Get per-column value-list for the segment value-presence bitmap from
/// `deltax.deltax_partition.column_valmap` (populated at compression time). Returns
/// a map of column-name → sorted distinct values; the array index is the bit
/// position in each segment's bitmap. Empty map ⇒ no eligible columns.
pub(crate) fn get_column_valmap(
    companion_oid: pg_sys::Oid,
) -> std::collections::HashMap<String, Vec<String>> {
    if let Some(cached) = VALMAP_CACHE.with(|cache| cache.borrow().get(&companion_oid).cloned()) {
        return cached;
    }

    let companion_name = unsafe {
        let name_ptr = pg_sys::get_rel_name(companion_oid);
        if name_ptr.is_null() {
            return std::collections::HashMap::new();
        }
        std::ffi::CStr::from_ptr(name_ptr)
            .to_string_lossy()
            .into_owned()
    };
    let partition_name = companion_name
        .strip_suffix("_meta")
        .unwrap_or(&companion_name);

    let mut result_map: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();

    let json_text = Spi::get_one_with_args::<String>(
        "SELECT column_valmap::text FROM deltax.deltax_partition
         WHERE table_name = $1 AND is_compressed = true",
        &[partition_name.into()],
    );

    if let Ok(Some(text)) = json_text {
        parse_valmap_json(&text, &mut result_map);
    }

    VALMAP_CACHE.with(|cache| cache.borrow_mut().insert(companion_oid, result_map.clone()));
    result_map
}

/// Parse a `{"col": ["v0","v1",...], ...}` JSON object (as emitted by
/// `catalog::update_partition_column_valmap`). Trivial hand-rolled parser —
/// values are quoted strings, keys are column names with `\\` and `\"`
/// escapes. Lenient: malformed input → leave `out` partially populated.
fn parse_valmap_json(text: &str, out: &mut std::collections::HashMap<String, Vec<String>>) {
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if i >= bytes.len() || bytes[i] != b'{' {
        return;
    }
    i += 1;

    loop {
        while i < bytes.len() && (bytes[i].is_ascii_whitespace() || bytes[i] == b',') {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] == b'}' {
            return;
        }
        if bytes[i] != b'"' {
            return;
        }
        i += 1;

        // Key.
        let mut key = String::new();
        while i < bytes.len() && bytes[i] != b'"' {
            if bytes[i] == b'\\' && i + 1 < bytes.len() {
                key.push(bytes[i + 1] as char);
                i += 2;
            } else {
                key.push(bytes[i] as char);
                i += 1;
            }
        }
        if i >= bytes.len() {
            return;
        }
        i += 1;

        while i < bytes.len() && (bytes[i].is_ascii_whitespace() || bytes[i] == b':') {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b'[' {
            return;
        }
        i += 1;

        let mut vals: Vec<String> = Vec::new();
        loop {
            while i < bytes.len() && (bytes[i].is_ascii_whitespace() || bytes[i] == b',') {
                i += 1;
            }
            if i >= bytes.len() || bytes[i] == b']' {
                break;
            }
            if bytes[i] != b'"' {
                return;
            }
            i += 1;
            let mut v = String::new();
            while i < bytes.len() && bytes[i] != b'"' {
                if bytes[i] == b'\\' && i + 1 < bytes.len() {
                    v.push(bytes[i + 1] as char);
                    i += 2;
                } else {
                    v.push(bytes[i] as char);
                    i += 1;
                }
            }
            if i >= bytes.len() {
                return;
            }
            i += 1;
            vals.push(v);
        }
        if i < bytes.len() && bytes[i] == b']' {
            i += 1;
        }
        out.insert(key, vals);
    }
}

/// Parse a `{"col": int, ...}` JSON object (as emitted by
/// `catalog::update_partition_column_ndistinct`) into the result map.
/// Trivial hand-rolled parser — values are always integers, keys are
/// always column names with limited escaping (backslash and quote).
fn parse_ndistinct_json(text: &str, out: &mut std::collections::HashMap<String, i64>) {
    let bytes = text.as_bytes();
    let mut i = 0;
    // Skip leading whitespace and opening brace
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if i >= bytes.len() || bytes[i] != b'{' {
        return;
    }
    i += 1;

    loop {
        while i < bytes.len() && (bytes[i].is_ascii_whitespace() || bytes[i] == b',') {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] == b'}' {
            return;
        }
        if bytes[i] != b'"' {
            return;
        }
        i += 1;

        // Parse key (with \" and \\ escapes).
        let mut key = String::new();
        while i < bytes.len() && bytes[i] != b'"' {
            if bytes[i] == b'\\' && i + 1 < bytes.len() {
                key.push(bytes[i + 1] as char);
                i += 2;
            } else {
                key.push(bytes[i] as char);
                i += 1;
            }
        }
        if i >= bytes.len() {
            return;
        }
        i += 1; // closing quote

        while i < bytes.len() && (bytes[i].is_ascii_whitespace() || bytes[i] == b':') {
            i += 1;
        }

        // Parse integer value (may be negative in principle).
        let start = i;
        if i < bytes.len() && (bytes[i] == b'-' || bytes[i] == b'+') {
            i += 1;
        }
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if let Ok(s) = std::str::from_utf8(&bytes[start..i])
            && let Ok(v) = s.parse::<i64>()
        {
            out.insert(key, v);
        }
    }
}

/// Get reltuples from pg_class for a relation OID.
pub(super) unsafe fn get_reltuples(rel_oid: pg_sys::Oid) -> f64 {
    unsafe {
        let tuple = pg_sys::SearchSysCache1(
            pg_sys::SysCacheIdentifier::RELOID as i32,
            pg_sys::ObjectIdGetDatum(rel_oid),
        );
        if tuple.is_null() {
            return 0.0;
        }
        let rel_form = pg_sys::GETSTRUCT(tuple) as pg_sys::Form_pg_class;
        let tuples = (*rel_form).reltuples;
        pg_sys::ReleaseSysCache(tuple);
        tuples as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recommend_below_threshold_returns_zero() {
        for segs in [0i64, 1, 8, 15] {
            assert_eq!(recommend_agg_workers_inner(segs, 8), 0, "segs={}", segs);
        }
    }

    #[test]
    fn recommend_clamps_to_max_per_gather() {
        // 1000 segs / 8 = 125 worker-slots, but PG caps at 4.
        assert_eq!(recommend_agg_workers_inner(1000, 4), 4);
        // Exactly at the floor: 16 segs / 8 = 2 workers, PG cap=4 → 2.
        assert_eq!(recommend_agg_workers_inner(16, 4), 2);
        // 64 / 8 = 8, PG cap=2 → 2.
        assert_eq!(recommend_agg_workers_inner(64, 2), 2);
    }

    #[test]
    fn recommend_negative_pg_cap_clamps_to_zero() {
        // Defensive: if max_parallel_workers_per_gather is somehow negative
        // (PG default is 2, but some configs disable parallelism by setting
        // 0), clamp to 0.
        assert_eq!(recommend_agg_workers_inner(1000, 0), 0);
    }

    #[test]
    fn parallel_divisor_matches_pg_costsize_formula() {
        // From PG costsize.c get_parallel_divisor: leader contribution decays
        // at 0.3 per worker. 1 worker → leader=0.7 → div=1.7; 2 → 0.4 → 2.4;
        // 3 → 0.1 → 3.1. Past 3 workers the leader pins at 0.0.
        assert!(
            (parallel_divisor(0) - 1.0).abs() < 1e-9,
            "got {}",
            parallel_divisor(0)
        );
        assert!(
            (parallel_divisor(1) - 1.7).abs() < 1e-9,
            "got {}",
            parallel_divisor(1)
        );
        assert!(
            (parallel_divisor(2) - 2.4).abs() < 1e-9,
            "got {}",
            parallel_divisor(2)
        );
        assert!(
            (parallel_divisor(3) - 3.1).abs() < 1e-9,
            "got {}",
            parallel_divisor(3)
        );
        // 4 workers: 1 - 1.2 = -0.2 → clamp to 0, div = 4.0.
        assert!(
            (parallel_divisor(4) - 4.0).abs() < 1e-9,
            "got {}",
            parallel_divisor(4)
        );
        assert!(
            (parallel_divisor(10) - 10.0).abs() < 1e-9,
            "got {}",
            parallel_divisor(10)
        );
    }

    #[test]
    fn parse_ndistinct_json_handles_basic_shapes() {
        let mut out = HashMap::new();
        parse_ndistinct_json(r#"{"a":1,"b":42,"c":1234}"#, &mut out);
        assert_eq!(out.get("a").copied(), Some(1));
        assert_eq!(out.get("b").copied(), Some(42));
        assert_eq!(out.get("c").copied(), Some(1234));

        // Whitespace + negative + leading/trailing spaces.
        let mut out2 = HashMap::new();
        parse_ndistinct_json(r#"  { "x" : -7,  "y": +0 }  "#, &mut out2);
        assert_eq!(out2.get("x").copied(), Some(-7));
        assert_eq!(out2.get("y").copied(), Some(0));
    }

    #[test]
    fn parse_ndistinct_json_unescapes_keys() {
        // The writer (`catalog::json_escape`) emits `\"` and `\\` — round-trip
        // them through the parser.
        let mut out = HashMap::new();
        parse_ndistinct_json(r#"{"col\"with\\quotes":5}"#, &mut out);
        assert_eq!(out.get("col\"with\\quotes").copied(), Some(5));
    }

    #[test]
    fn parse_ndistinct_json_tolerates_garbage() {
        // Malformed input should never panic — caller treats an empty map as
        // "no info, fall through to default selectivity".
        let mut out = HashMap::new();
        parse_ndistinct_json("", &mut out);
        parse_ndistinct_json("null", &mut out);
        parse_ndistinct_json("{", &mut out);
        parse_ndistinct_json(r#"{"a":xyz}"#, &mut out);
        // Doesn't insert "a" because the value didn't parse, and may stop at
        // the bad token — what matters is "no crash".
        assert_eq!(out.get("a").copied(), None);
    }

    #[test]
    fn parse_valmap_json_basic_array() {
        let mut out = HashMap::new();
        parse_valmap_json(r#"{"col":["a","b","c"]}"#, &mut out);
        assert_eq!(
            out.get("col").cloned(),
            Some(vec!["a".to_string(), "b".to_string(), "c".to_string()])
        );

        let mut out2 = HashMap::new();
        parse_valmap_json(r#"{"a":[],"b":["x"]}"#, &mut out2);
        assert_eq!(out2.get("a").cloned(), Some(Vec::<String>::new()));
        assert_eq!(out2.get("b").cloned(), Some(vec!["x".to_string()]));
    }

    #[test]
    fn parse_valmap_json_unescapes_quoted_values() {
        // Writer escapes `"` and `\` in both keys and values — confirm the
        // parser undoes both.
        let mut out = HashMap::new();
        parse_valmap_json(r#"{"event":["click\"ed","back\\slash"]}"#, &mut out);
        let v = out.get("event").cloned().unwrap();
        assert_eq!(v, vec!["click\"ed".to_string(), "back\\slash".to_string()]);
    }
}
