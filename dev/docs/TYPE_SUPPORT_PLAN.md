# Type Support Improvement Plan

Status: Phases 0-2 implemented (2026-07-20, PR #53). Follows community PR #51
/ issue #50 (non-text fallthrough columns read back wrong or crashed the
backend).

Implementation notes vs. the original plan below:

- time uses the existing integer tags (no new tag needed — legacy time blobs
  have text-family tags, so the tag disambiguates generations naturally).
- uuid/bytea/inet/cidr use three new tags (BinaryDictionary=10,
  BinaryDictionaryLz4=11, BinaryLz4Blocked=12) that remap the byte-pipeline
  output; jsonb keeps the legacy tags (disambiguated by oid) for on-disk
  compatibility.
- numeric kept `ColumnKind::Text` entirely: only `compress_typed_column`
  gained a gate that tries `CompressionType::NumericScaled` (=13, scaled-i64
  mantissas + uniform dscale) per blob, with text fallback for
  NaN/Infinity/mixed-dscale/>i64 segments. Reads build long-format
  NumericData varlenas directly (`make_numeric_datum`).
- Mixed-generation testing runs with current code via the
  `pg_deltax.force_text_fallback` GUC (tests/test_native_type_codecs.py),
  which forces legacy text-era blobs at compression time.
- Found and fixed along the way: the single-`count(*)` fast path ignored
  FILTER clauses (returned total row count for `count(*) FILTER (...)` on
  any compressed table).

Remaining: macaddr (would ride the fixed-length path like uuid), numeric
minmax pruning (needs a cross-segment order-preserving encoding), bloom
probes for time/uuid equality, and the Phase 3 benchmark validation runs.

## Background

`classify_column` (src/compress.rs) maps each column to a `ColumnKind` with
explicit arms for smallint/integer/bigint, real/double precision, boolean,
timestamp/timestamptz, date, and jsonb. Everything else — including
text/varchar/bpchar — lands in `ColumnKind::Text`. Text-kind columns are
stored as their lossless `::text` rendering; at read time the emit path
dispatches on the attribute's type oid:

- `text`/`varchar`: raw-varlena arena fast path (zero-copy-ish).
- Everything else (bpchar, jsonb safety net, and all fallthrough types):
  reconstructed via the type's input function (`getTypeInputInfo` +
  `OidInputFunctionCall`) — this is what PR #51 fixed.

Fallthrough types today: `numeric`, `uuid`, `bytea`, `inet`/`cidr`/`macaddr`,
`time`/`timetz`, `interval`, plain `json`, `xml`, every array type, enums,
domains, composites, ranges, and extension types (citext, postgis, …). They
are now **correct** but pay per-value input-function parsing on read and store
an inflated text rendering (e.g. uuid: 36 text bytes vs 16 binary; bytea: 2x+
hex text).

## Phase 0 — Merge PR #51

Merge as-is. It is the correctness baseline everything below builds on, and it
reads existing compressed partitions correctly without recompression.

## Phase 1 — Review follow-ups (small, independent PRs)

1. **Hoist the input-function lookup out of per-value loops.** In
   `str_to_text_datum` the `getTypeInputInfo` + `CString` work runs per value;
   `str_slices_to_text_datums_arena`, `text_ranges_to_datums`, and
   `matched_text_ranges_to_datums` (src/scan/exec/datum_utils.rs) repeat it per
   slice/range. Resolve `typinput`/`typioparam` once per column call, `fmgr_info`
   into a stack `FmgrInfo`, and use `InputFunctionCall` instead of
   `OidInputFunctionCall` (which re-runs `fmgr_info` internally each call).
   Same hoist for `getTypeOutputInfo` in `jsonb_binary_to_text`
   (src/compress.rs), called per value from `decompress_column_values`.
2. **Pin GUC-dependent renderings.** The text round-trip is only guaranteed
   when the reader's parsing GUCs match the writer's rendering GUCs. Concrete
   hazard: `interval_out` under `IntervalStyle=sql_standard` can re-parse to a
   different value under another style (the pg_dump caveat; pg_dump pins
   `IntervalStyle=postgres`). Pin `IntervalStyle` (and audit `DateStyle`,
   `extra_float_digits` for any affected fallthrough type) around the
   write-side `::text` rendering in the compress path, and document the
   constraint in COLUMNAR_STORAGE.md.
3. **Tests:** add an `interval` column and a fallback-typed `segment_by`
   column (already safe via `string_to_datum`, but untested) to
   tests/test_nontext_columns.py.
4. **Doc nit:** the `str_slices_to_text_datums_arena` doc comment still leads
   with "single contiguous allocation"; the non-text branch per-value
   allocates.
5. Optional follow-up from the PR discussion: a per-batch memory-context reset
   inside the reconstruction loop to cap peak per-segment memory for wide
   fallback columns (correctness does not depend on it; `segment_mcxt` is
   already reset per segment).

## Phase 2 — Native codecs for the common types

Ordered by value/effort. Each type graduates from the text fallback to a
proper `ColumnKind`, so it gets binary storage, cheap reads, and (where
applicable) minmax pruning.

### Design invariant: on-disk compatibility

Old partitions compressed before a type graduates still hold text-rendered
blobs. Dispatch must therefore be **per blob, not per column type**: the
`CompressedColumn` `type_tag` already distinguishes codecs, so the read side
(`decode_compressed_datums` and friends in src/scan/exec/datum_utils.rs, and
`decompress_column_values` in src/compress.rs) keys on the tag it finds —
Dictionary/Lz4* blobs on a uuid column decode as text + input function
(the PR #51 path, which stays forever as the legacy/compat path), new-tag
blobs decode natively. No recompression required; `deltax_decompress_partition`
must handle both generations of blobs for the same column. This mirrors how
jsonb already works (binary payload through the byte-level codec entry points,
text safety net retained).

Every graduated type must be wired through all of:

- `classify_column`, `new_typed_column`, `TypedColumn` + `push_from`,
  `compress_typed_column` (src/compress.rs)
- the COPY ingest path (`parse_and_append` in src/copyparse.rs)
- the read dispatch (`decode_compressed_datums`, truncated/filtered variants)
- `decompress_column_values` for `deltax_decompress_partition`
- colstats/minmax (`encode_datum_to_i64` in src/scan/exec/segments.rs)
  where an order-preserving i64 encoding exists
- correctness tests: extend tests/correctness/test_codecs_extended.py and
  tests/test_nontext_columns.py with the graduated type across all codecs,
  NULL mixes, and old-blob/new-blob mixed partitions

`segment_by` columns intentionally stay text (`classify_column` short-circuits
`is_segment_by` — segment values are used as SQL literals and read via
`string_to_datum`, which already uses the input function).

### 2a. `time` — trivial

Microseconds since midnight as i64. Map to a new `ColumnKind::Time` backed by
`TypedColumn::Int64`; reuse the existing integer codecs
(Constant/ForBitpacked/DeltaVarint) and the i64 minmax encoding. Read side
emits the i64 datum directly, like timestamp minus the epoch offset. `timetz`
stays on fallback (carries a zone offset; rare).

### 2b. `uuid` — fixed 16 bytes, high value

Ids in event tables are the main real-world hit. Store the 16 raw bytes via a
fixed-width byte column (either reuse `TypedColumn::Bytes` + the byte-blob
pipeline `compress_byte_values`, or a dedicated fixed-stride encoding that
skips per-value length prefixes). Read side wraps bytes in a varlena like
`byte_slices_to_jsonb_datums_arena` does. Dictionary coding still applies for
low-cardinality uuid columns (byte-level dictionary already exists for jsonb).
Bloom filters on uuid equality quals are a natural follow-up (hash the 16
bytes), but not required for graduation.

### 2c. `bytea` — near-free storage win

Same `TypedColumn::Bytes` treatment as jsonb, minus the jsonb output/input
conversion: store raw bytes, emit varlena-wrapped bytes. Kills the 2x hex-text
inflation. `decompress_column_values` needs a bytea branch mirroring the new
jsonb one (render via `byteaout` or emit binary through the insert path).

### 2d. `inet` / `cidr` — small fixed-ish binary

4 or 16 address bytes + family/prefix; store the raw datum payload through the
Bytes pipeline. Common in log/pingback tables (the incident table had one).
`macaddr`/`macaddr8` can ride along with the same treatment if trivial.

### 2e. `numeric` — highest value, most design work

Most common fallback type in practice (money, metrics). Variable-length, so
the plan is a **scaled-int64 encoding with escape hatch**:

- At compress time, scan the segment's values; if every non-null value fits
  `int64` at some common scale `s` (digits × 10^s, s bounded to avoid
  overflow), store as i64 column + scale byte, reusing the integer codecs and
  gaining i64 minmax pruning.
- Otherwise (NaN, huge precision, mixed extreme scales) fall back to the text
  rendering for that blob — the per-blob tag dispatch makes mixing free.
- Read side reconstructs numeric datums from (i64, scale) directly
  (`int64_to_numeric` + `numeric_mul`-free scaling, or via a small digit
  builder) instead of `numeric_in`.

Aggregate pushdown (`sum`/`min`/`max` on numeric) over the scaled-i64 form is
a follow-up with real benchmark upside, but graduation only requires correct
scan emit.

### Explicitly staying on the text fallback

Arrays (including `text[]`), `interval` (month/day/usec triple; Phase 1 GUC
pinning makes it safe), plain `json` (text *is* its native form), `xml`,
enums, domains, composites, ranges, extension types. The fallback is correct
after PR #51; these are rare or hard enough that native codecs aren't worth
the format surface for now.

## Phase 3 — Validation

- Unit + integration + correctness suites green on PG 17 and 18 per phase.
- Mixed-generation test: compress a partition pre-graduation (checkout or
  fixture blob), upgrade, verify reads and `deltax_decompress_partition` on a
  partition containing both blob generations of the same column.
- Full benchmark protocol (local + EC2 ClickBench, RTABench) after Phase 1
  and after each Phase 2 codec — none of the benchmark schemas use these
  types, so the expectation is "no movement"; the run is a regression guard
  for the shared dispatch paths.

## Sequencing

| Step | Contents | Size |
|---|---|---|
| 0 | Merge PR #51 | — |
| 1 | Input-function hoisting + GUC pinning + tests/doc nits | S |
| 2a | `time` as i64 | S |
| 2b/2c | `uuid` + `bytea` via Bytes pipeline | M |
| 2d | `inet`/`cidr` (+`macaddr` if trivial) | S–M |
| 2e | `numeric` scaled-i64 with per-blob fallback | L |
| 3 | Mixed-generation tests + bench validation | ongoing |
