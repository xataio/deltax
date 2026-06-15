//! Immutable per-partition segment files (`.dxs`) — STORAGE_V2 P1.
//!
//! In `pg_deltax.blob_storage = 'dual'` mode the SPI compress path writes,
//! in addition to the TOAST-backed `<partition>_blobs` companion table, one
//! immutable file per partition under
//! `$PGDATA/pg_deltax/<db_oid>/<partition_id>_<generation>.dxs` containing
//! the same compressed column blobs, column-major. The read path mmaps the
//! file and serves blob slices with zero detoast/copy; on any file problem
//! it silently falls back to the TOAST blobs table (which dual mode
//! guarantees is populated). See `dev/docs/STORAGE_V2.md`.
//!
//! On-disk layout (all integers little-endian):
//!
//! ```text
//! Header (64 bytes):  magic "DXSEG\0" | version u16 | flags u32
//!                     | partition_id u32 | n_columns u32 | n_segments u32
//!                     | index_offset u64 | index_len u64 | header_crc u32
//!                     | zero pad to 64
//! Blob data:          column-major, each blob 64-byte aligned; the bytes
//!                     are exactly what `CompressedColumn::from_bytes`
//!                     consumes (no re-framing).
//! Blob index:         per (col_idx, segment_id), sorted:
//!                     col_idx u16 | segment_id u32 | offset u64
//!                     | length u32 | crc32c u32          (22 B/entry)
//! Footer (16 bytes):  index_crc u32 | file_len u64 | magic "DXSE"
//! ```
//!
//! TODO(P2): background-worker GC sweep for orphan files (crashed
//! compressions, raw `DROP TABLE` of a partition while the worker was
//! down). P1 only unlinks on decompress and retention drops.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use memmap2::Mmap;
use pgrx::pg_sys;

use crate::scan::exec::datum_utils::tupdesc_get_attr;

const MAGIC: [u8; 6] = *b"DXSEG\0";
const FOOTER_MAGIC: [u8; 4] = *b"DXSE";
const VERSION: u16 = 1;
const HEADER_LEN: usize = 64;
/// Header bytes covered by `header_crc` (everything before the crc field).
const HEADER_CRC_COVERAGE: usize = 40;
const ENTRY_LEN: usize = 22;
const FOOTER_LEN: usize = 16;
const BLOB_ALIGN: u64 = 64;

// ============================================================================
// CRC32C (Castagnoli, reflected). Software table implementation — only run
// at compress time and (optionally) on first read, so throughput is not
// load-bearing for query latency.
// ============================================================================

const fn build_crc32c_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

static CRC32C_TABLE: [u32; 256] = build_crc32c_table();

pub(crate) fn crc32c(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &b in data {
        crc = (crc >> 8) ^ CRC32C_TABLE[((crc ^ b as u32) & 0xFF) as usize];
    }
    !crc
}

// ============================================================================
// Encoding (pure — unit-testable without a server)
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IndexEntry {
    pub(crate) col_idx: u16,
    pub(crate) segment_id: i32,
    pub(crate) offset: u64,
    pub(crate) len: u32,
    pub(crate) crc: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SegmentFileHeader {
    pub(crate) version: u16,
    pub(crate) partition_id: u32,
    pub(crate) n_columns: u32,
    pub(crate) n_segments: u32,
    pub(crate) index_offset: u64,
    pub(crate) index_len: u64,
}

fn round_up(v: u64, align: u64) -> u64 {
    v.div_ceil(align) * align
}

/// Serialize the 64-byte header. Shared by `SegmentFileWriter` and the
/// test-only one-shot encoder so the two can never drift.
fn encode_header(
    partition_id: u32,
    n_columns: u32,
    n_segments: u32,
    index_offset: u64,
    index_len: u64,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN);
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes()); // flags
    out.extend_from_slice(&partition_id.to_le_bytes());
    out.extend_from_slice(&n_columns.to_le_bytes());
    out.extend_from_slice(&n_segments.to_le_bytes());
    out.extend_from_slice(&index_offset.to_le_bytes());
    out.extend_from_slice(&index_len.to_le_bytes());
    debug_assert_eq!(out.len(), HEADER_CRC_COVERAGE);
    let header_crc = crc32c(&out);
    out.extend_from_slice(&header_crc.to_le_bytes());
    out.resize(HEADER_LEN, 0);
    out
}

/// Serialize the blob index (no checksum — the footer carries it).
fn encode_index(entries: &[IndexEntry]) -> Vec<u8> {
    let mut index_bytes = Vec::with_capacity(entries.len() * ENTRY_LEN);
    for e in entries {
        index_bytes.extend_from_slice(&e.col_idx.to_le_bytes());
        index_bytes.extend_from_slice(&(e.segment_id as u32).to_le_bytes());
        index_bytes.extend_from_slice(&e.offset.to_le_bytes());
        index_bytes.extend_from_slice(&e.len.to_le_bytes());
        index_bytes.extend_from_slice(&e.crc.to_le_bytes());
    }
    index_bytes
}

/// Serialize the 16-byte footer.
fn encode_footer(index_crc: u32, file_len: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(FOOTER_LEN);
    out.extend_from_slice(&index_crc.to_le_bytes());
    out.extend_from_slice(&file_len.to_le_bytes());
    out.extend_from_slice(&FOOTER_MAGIC);
    out
}

/// Distinct (column, segment) counts for the header. `entries` must be
/// sorted by `(col_idx, segment_id)` (columns counted by run-length).
fn count_distinct(entries: &[IndexEntry]) -> (u32, u32) {
    let mut n_columns = 0u32;
    let mut last: Option<u16> = None;
    for e in entries {
        if last != Some(e.col_idx) {
            n_columns += 1;
            last = Some(e.col_idx);
        }
    }
    let n_segments = entries
        .iter()
        .map(|e| e.segment_id)
        .collect::<std::collections::HashSet<_>>()
        .len() as u32;
    (n_columns, n_segments)
}

/// Serialize the full `.dxs` image for one partition in one shot. Test-only
/// oracle: `incremental_writer_matches_one_shot_encoder` pins the
/// `SegmentFileWriter` (the single production write path) to this pure
/// encoding. `blobs` must be sorted by `(col_idx, segment_id)` and segment
/// ids must be non-negative.
#[cfg(test)]
pub(crate) fn encode_segment_file(partition_id: u32, blobs: &[(u16, i32, Vec<u8>)]) -> Vec<u8> {
    debug_assert!(
        blobs
            .windows(2)
            .all(|w| (w[0].0, w[0].1) < (w[1].0, w[1].1))
    );

    // Lay out blob offsets first so the header can be written in one pass.
    let mut entries: Vec<IndexEntry> = Vec::with_capacity(blobs.len());
    let mut off = HEADER_LEN as u64;
    for (col_idx, segment_id, blob) in blobs {
        off = round_up(off, BLOB_ALIGN);
        entries.push(IndexEntry {
            col_idx: *col_idx,
            segment_id: *segment_id,
            offset: off,
            len: blob.len() as u32,
            crc: crc32c(blob),
        });
        off += blob.len() as u64;
    }
    let index_offset = off;
    let index_len = (entries.len() * ENTRY_LEN) as u64;
    let file_len = index_offset + index_len + FOOTER_LEN as u64;
    let (n_columns, n_segments) = count_distinct(&entries);

    let mut out = Vec::with_capacity(file_len as usize);

    // Header.
    out.extend_from_slice(&encode_header(
        partition_id,
        n_columns,
        n_segments,
        index_offset,
        index_len,
    ));

    // Blob data, 64-byte aligned.
    for (entry, (_, _, blob)) in entries.iter().zip(blobs) {
        out.resize(entry.offset as usize, 0);
        out.extend_from_slice(blob);
    }

    // Index.
    debug_assert_eq!(out.len() as u64, index_offset);
    let index_bytes = encode_index(&entries);
    let index_crc = crc32c(&index_bytes);
    out.extend_from_slice(&index_bytes);

    // Footer.
    out.extend_from_slice(&encode_footer(index_crc, file_len));
    debug_assert_eq!(out.len() as u64, file_len);

    out
}

// ============================================================================
// Decoding / validation (pure)
// ============================================================================

fn le_u16(b: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(b[at..at + 2].try_into().unwrap())
}
fn le_u32(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap())
}
fn le_u64(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().unwrap())
}

/// Validate header, footer and index of a complete file image and parse the
/// blob index. O(header + index); does NOT checksum blob data (per-blob CRC
/// is verified lazily on lookup when enabled).
pub(crate) fn parse_and_validate(
    bytes: &[u8],
) -> Result<(SegmentFileHeader, Vec<IndexEntry>), String> {
    if bytes.len() < HEADER_LEN + FOOTER_LEN {
        return Err(format!("file too short ({} bytes)", bytes.len()));
    }
    if bytes[..MAGIC.len()] != MAGIC {
        return Err("bad header magic".into());
    }
    let version = le_u16(bytes, 6);
    if version != VERSION {
        return Err(format!(
            "unsupported segment-file version {} (expected {}); recompress the partition",
            version, VERSION
        ));
    }
    let header_crc = le_u32(bytes, HEADER_CRC_COVERAGE);
    if crc32c(&bytes[..HEADER_CRC_COVERAGE]) != header_crc {
        return Err("header checksum mismatch".into());
    }
    let header = SegmentFileHeader {
        version,
        partition_id: le_u32(bytes, 12),
        n_columns: le_u32(bytes, 16),
        n_segments: le_u32(bytes, 20),
        index_offset: le_u64(bytes, 24),
        index_len: le_u64(bytes, 32),
    };

    let footer_at = bytes.len() - FOOTER_LEN;
    if bytes[footer_at + 12..] != FOOTER_MAGIC {
        return Err("bad footer magic".into());
    }
    let file_len = le_u64(bytes, footer_at + 4);
    if file_len != bytes.len() as u64 {
        return Err(format!(
            "footer file_len {} != actual length {} (truncated?)",
            file_len,
            bytes.len()
        ));
    }

    let idx_start = header.index_offset;
    let idx_end = idx_start.checked_add(header.index_len);
    if idx_end != Some(footer_at as u64) || !header.index_len.is_multiple_of(ENTRY_LEN as u64) {
        return Err("index bounds inconsistent with file length".into());
    }
    let index_bytes = &bytes[idx_start as usize..footer_at];
    let index_crc = le_u32(bytes, footer_at);
    if crc32c(index_bytes) != index_crc {
        return Err("index checksum mismatch".into());
    }

    let n_entries = index_bytes.len() / ENTRY_LEN;
    let mut entries = Vec::with_capacity(n_entries);
    for i in 0..n_entries {
        let at = i * ENTRY_LEN;
        let e = IndexEntry {
            col_idx: le_u16(index_bytes, at),
            segment_id: le_u32(index_bytes, at + 2) as i32,
            offset: le_u64(index_bytes, at + 6),
            len: le_u32(index_bytes, at + 14),
            crc: le_u32(index_bytes, at + 18),
        };
        // Bounds-check every entry up front so lookups can hand out slices
        // without re-validating (mitigates SIGBUS-past-EOF on corrupt files).
        if e.offset < HEADER_LEN as u64 || e.offset + e.len as u64 > idx_start {
            return Err(format!(
                "index entry (col {}, seg {}) out of bounds",
                e.col_idx, e.segment_id
            ));
        }
        entries.push(e);
    }
    // Written sorted; sort defensively so binary search is always valid.
    entries.sort_unstable_by_key(|e| (e.col_idx, e.segment_id));

    Ok((header, entries))
}

// ============================================================================
// Writer (durable: tmp + fsync + rename + dir fsync)
// ============================================================================

/// Streaming `.dxs` writer — the single production write path, used one-shot
/// by the SPI compress path (`write_partition_blob_file`) and incrementally
/// by the COPY direct-backfill path, where blob batches arrive across
/// multiple `flush_partition_blobs` drains (`BLOB_BUFFER_THRESHOLD` early
/// flushes) and the total may exceed what we want to hold in memory. Blobs
/// are appended as they drain (column-major within each batch; global order
/// across batches is not required — the format only needs index entries to
/// carry correct offsets, and `finish` sorts the index). The index, footer,
/// and final header are written once in `finish()`, followed by fsync +
/// rename + dir fsync — so `finish()` must complete before the surrounding
/// transaction commits a `blob_file` reference (STORAGE_V2.md §3): a
/// committed catalog row never references a missing/torn file.
///
/// Until `finish()` succeeds only the `.tmp` sibling exists; an errored or
/// abandoned writer leaves at most a `.tmp` orphan (removed best-effort by
/// `abandon`, otherwise collected by the planned P2 GC sweep).
pub(crate) struct SegmentFileWriter {
    file: File,
    dir: PathBuf,
    file_name: String,
    tmp_path: PathBuf,
    partition_id: u32,
    /// End-of-data offset == bytes written so far (header included).
    offset: u64,
    entries: Vec<IndexEntry>,
}

impl SegmentFileWriter {
    /// Create the `.tmp` file and reserve the header (zero-filled;
    /// rewritten in `finish()` once the index offset is known).
    pub(crate) fn create_at(
        dir: &Path,
        file_name: &str,
        partition_id: u32,
    ) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let tmp_path = dir.join(format!("{file_name}.tmp"));
        let mut file = File::create(&tmp_path)?;
        file.write_all(&[0u8; HEADER_LEN])?;
        Ok(Self {
            file,
            dir: dir.to_path_buf(),
            file_name: file_name.to_string(),
            tmp_path,
            partition_id,
            offset: HEADER_LEN as u64,
            entries: Vec::new(),
        })
    }

    /// Create an incremental writer for one partition's dual-mode file under
    /// `$PGDATA/pg_deltax/<db_oid>/`. Returns the writer plus the
    /// data-directory-relative path to record in
    /// `deltax.deltax_partition.blob_file` once `finish()` has succeeded.
    pub(crate) fn create_for_partition(partition_id: i32) -> std::io::Result<(Self, String)> {
        let (dir, file_name, rel_path) = partition_file_location(partition_id);
        let writer = Self::create_at(&dir, &file_name, partition_id as u32)?;
        Ok((writer, rel_path))
    }

    /// Append one drain batch. Each `(col_idx, segment_id)` pair must be
    /// unique across the file's lifetime (every segment is flushed exactly
    /// once); batches should be sorted by `(col_idx, segment_id)` within
    /// themselves for the column-major read locality the format aims for.
    pub(crate) fn append_blobs(&mut self, blobs: &[(u16, i32, Vec<u8>)]) -> std::io::Result<()> {
        const ZEROS: [u8; BLOB_ALIGN as usize] = [0u8; BLOB_ALIGN as usize];
        for (col_idx, segment_id, blob) in blobs {
            let aligned = round_up(self.offset, BLOB_ALIGN);
            if aligned > self.offset {
                self.file
                    .write_all(&ZEROS[..(aligned - self.offset) as usize])?;
            }
            self.file.write_all(blob)?;
            self.entries.push(IndexEntry {
                col_idx: *col_idx,
                segment_id: *segment_id,
                offset: aligned,
                len: blob.len() as u32,
                crc: crc32c(blob),
            });
            self.offset = aligned + blob.len() as u64;
        }
        Ok(())
    }

    /// Write index + footer, rewrite the header in place, fsync, rename to
    /// the final name, and fsync the directory (and its parent). After this
    /// returns `Ok` the file is durable and complete.
    pub(crate) fn finish(mut self) -> std::io::Result<()> {
        use std::io::Seek;
        self.entries
            .sort_unstable_by_key(|e| (e.col_idx, e.segment_id));
        let index_offset = self.offset;
        let index_bytes = encode_index(&self.entries);
        let index_len = index_bytes.len() as u64;
        let file_len = index_offset + index_len + FOOTER_LEN as u64;
        self.file.write_all(&index_bytes)?;
        self.file
            .write_all(&encode_footer(crc32c(&index_bytes), file_len))?;

        let (n_columns, n_segments) = count_distinct(&self.entries);
        self.file.seek(std::io::SeekFrom::Start(0))?;
        self.file.write_all(&encode_header(
            self.partition_id,
            n_columns,
            n_segments,
            index_offset,
            index_len,
        ))?;
        self.file.sync_all()?;

        std::fs::rename(&self.tmp_path, self.dir.join(&self.file_name))?;
        File::open(&self.dir)?.sync_all()?;
        if let Some(parent) = self.dir.parent() {
            // Best-effort: makes the <db_oid> dir entry itself durable.
            let _ = File::open(parent).and_then(|d| d.sync_all());
        }
        Ok(())
    }

    /// Best-effort removal of the `.tmp` file after a write error.
    pub(crate) fn abandon(self) {
        drop(self.file);
        let _ = std::fs::remove_file(&self.tmp_path);
    }
}

// ============================================================================
// mmap reader
// ============================================================================

pub(crate) struct MappedSegmentFile {
    mmap: Mmap,
    entries: Vec<IndexEntry>,
}

/// Result of a blob lookup. `Absent` mirrors a missing blobs-table row
/// (all-null column / column added after compression) and is NOT an error;
/// `Corrupt` means the caller must abandon the file and fall back to TOAST.
pub(crate) enum BlobLookup<'a> {
    Found(&'a [u8]),
    Absent,
    Corrupt,
}

impl MappedSegmentFile {
    pub(crate) fn open(path: &Path) -> Result<Self, String> {
        let file = File::open(path).map_err(|e| format!("open: {e}"))?;
        // SAFETY: the file is written once and never modified in place
        // (recompression writes a new generation under a new name). External
        // truncation is the same risk class as deleting a relation segment
        // file under a running server.
        let mmap = unsafe { Mmap::map(&file) }.map_err(|e| format!("mmap: {e}"))?;
        let (_, entries) = parse_and_validate(&mmap)?;
        Ok(Self { mmap, entries })
    }

    /// Look up the blob for `(col_idx, segment_id)`. The returned slice
    /// borrows from the mmap — callers that stash raw pointers must keep
    /// the owning `Arc<MappedSegmentFile>` alive for as long as the
    /// pointers are dereferenced (see `SegmentData::blob_file_backing`).
    pub(crate) fn get(&self, col_idx: u16, segment_id: i32, verify_crc: bool) -> BlobLookup<'_> {
        let Ok(pos) = self
            .entries
            .binary_search_by_key(&(col_idx, segment_id), |e| (e.col_idx, e.segment_id))
        else {
            return BlobLookup::Absent;
        };
        let e = &self.entries[pos];
        let start = e.offset as usize;
        let end = start + e.len as usize;
        if end > self.mmap.len() {
            return BlobLookup::Corrupt; // unreachable after open-time validation
        }
        let slice = &self.mmap[start..end];
        if verify_crc && crc32c(slice) != e.crc {
            return BlobLookup::Corrupt;
        }
        BlobLookup::Found(slice)
    }
}

// ============================================================================
// PostgreSQL glue
// ============================================================================

fn data_dir() -> PathBuf {
    // SAFETY: DataDir is set once at postmaster start and never changes.
    unsafe {
        PathBuf::from(
            std::ffi::CStr::from_ptr(pg_sys::DataDir)
                .to_string_lossy()
                .into_owned(),
        )
    }
}

/// Whether to verify per-blob CRC32C on every lookup. Always on in debug
/// builds; opt-in via `pg_deltax.verify_file_checksums` in release builds.
pub(crate) fn verify_checksums() -> bool {
    cfg!(debug_assertions) || crate::VERIFY_FILE_CHECKSUMS.get()
}

/// Directory, file name, and data-directory-relative catalog path for a new
/// segment file of `partition_id`. Generation = epoch micros: recompression
/// never reuses a name, so a stale mmap in another backend keeps reading the
/// old (deleted) file instead of torn new bytes.
fn partition_file_location(partition_id: i32) -> (PathBuf, String, String) {
    let db_oid = unsafe { pg_sys::MyDatabaseId }.to_u32();
    let generation = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0);
    let file_name = format!("{partition_id}_{generation}.dxs");
    let dir = data_dir().join("pg_deltax").join(db_oid.to_string());
    let rel_path = format!("pg_deltax/{db_oid}/{file_name}");
    (dir, file_name, rel_path)
}

/// Write the dual-mode segment file for one partition and return its path
/// relative to the data directory (the value stored in
/// `deltax.deltax_partition.blob_file`). `blobs` must already be sorted by
/// `(col_idx, segment_id)`.
pub(crate) fn write_partition_blob_file(
    partition_id: i32,
    blobs: &[(u16, i32, Vec<u8>)],
) -> std::io::Result<String> {
    let (mut writer, rel_path) = SegmentFileWriter::create_for_partition(partition_id)?;
    if let Err(e) = writer.append_blobs(blobs) {
        writer.abandon();
        return Err(e);
    }
    writer.finish()?;
    Ok(rel_path)
}

/// Best-effort unlink of a segment file by its catalog-relative path.
/// Logs (does not raise) on failure — the worst case is an orphan file,
/// which the planned P2 GC sweep will collect.
pub(crate) fn unlink_blob_file(rel_path: &str) {
    let path = data_dir().join(rel_path);
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => pgrx::warning!(
            "pg_deltax: failed to remove segment file {}: {}",
            path.display(),
            e
        ),
    }
}

thread_local! {
    // Backend-local cache: companion meta-table OID → mmap'd segment file
    // (None = partition has no usable file; read via TOAST). Companion
    // tables are dropped and recreated on every (de)compression cycle, so
    // a meta OID never maps to two different blob files — entries never
    // need invalidation within a backend's lifetime.
    static MAPPED_FILE_CACHE: RefCell<HashMap<pg_sys::Oid, Option<Arc<MappedSegmentFile>>>> =
        RefCell::new(HashMap::new());
}

/// Resolve the mmap'd segment file for a partition identified by its meta
/// companion-table OID. Returns `None` (and logs once per backend) when the
/// catalog has no `blob_file` or the file fails to open/validate — callers
/// fall back to the TOAST blobs table.
pub(crate) fn mapped_file_for_companion(meta_oid: pg_sys::Oid) -> Option<Arc<MappedSegmentFile>> {
    if let Some(cached) = MAPPED_FILE_CACHE.with(|c| c.borrow().get(&meta_oid).cloned()) {
        return cached;
    }
    let resolved = resolve_and_open(meta_oid);
    MAPPED_FILE_CACHE.with(|c| c.borrow_mut().insert(meta_oid, resolved.clone()));
    resolved
}

fn resolve_and_open(meta_oid: pg_sys::Oid) -> Option<Arc<MappedSegmentFile>> {
    let companion_name = unsafe {
        let name_ptr = pg_sys::get_rel_name(meta_oid);
        if name_ptr.is_null() {
            return None;
        }
        std::ffi::CStr::from_ptr(name_ptr)
            .to_string_lossy()
            .into_owned()
    };
    let partition_name = companion_name
        .strip_suffix("_meta")
        .unwrap_or(&companion_name);

    let rel_path = unsafe { lookup_blob_file_catalog(partition_name) }?;

    match MappedSegmentFile::open(&data_dir().join(&rel_path)) {
        Ok(f) => Some(Arc::new(f)),
        Err(e) => {
            // Logged once per backend per partition: the negative result is
            // cached, so we don't repeat this on every scan.
            pgrx::log!(
                "pg_deltax: segment file {} unusable ({}); falling back to TOAST blobs",
                rel_path,
                e
            );
            None
        }
    }
}

/// Detoast a text datum and copy its body into a `Vec<u8>`.
unsafe fn text_datum_to_vec(datum: pg_sys::Datum) -> Vec<u8> {
    unsafe {
        let varlena = datum.cast_mut_ptr::<pg_sys::varlena>();
        let detoasted = pg_sys::pg_detoast_datum(varlena);
        let len = pgrx::varsize_any_exhdr(detoasted);
        let data = pgrx::vardata_any(detoasted);
        #[allow(clippy::unnecessary_cast)]
        let bytes = std::slice::from_raw_parts(data as *const u8, len).to_vec();
        if detoasted != varlena {
            pg_sys::pfree(detoasted as *mut _);
        }
        bytes
    }
}

/// Read `deltax.deltax_partition.blob_file` for a compressed partition via
/// a direct heap scan under the active snapshot — deliberately NOT SPI.
/// This runs inside scan execution, including under parallel mode (leader
/// and parallel workers), where pgrx's read-write SPI attempts
/// command-counter/xid operations and errors with "cannot assign
/// transaction IDs during a parallel operation".
unsafe fn lookup_blob_file_catalog(partition_name: &str) -> Option<String> {
    unsafe {
        if !pg_sys::ActiveSnapshotSet() {
            return None;
        }
        let ns_oid = pg_sys::get_namespace_oid(c"deltax".as_ptr(), true);
        if ns_oid == pg_sys::InvalidOid {
            return None;
        }
        let rel_oid = pg_sys::get_relname_relid(c"deltax_partition".as_ptr(), ns_oid);
        if rel_oid == pg_sys::InvalidOid {
            return None;
        }
        let rel = pg_sys::table_open(rel_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
        let tupdesc = (*rel).rd_att;
        let natts = (*tupdesc).natts as usize;

        let mut name_att: Option<usize> = None;
        let mut compressed_att: Option<usize> = None;
        let mut file_att: Option<usize> = None;
        for i in 0..natts {
            let att = &*tupdesc_get_attr(tupdesc, i);
            if att.attisdropped {
                continue;
            }
            match std::ffi::CStr::from_ptr(att.attname.data.as_ptr()).to_bytes() {
                b"table_name" => name_att = Some(i),
                b"is_compressed" => compressed_att = Some(i),
                b"blob_file" => file_att = Some(i),
                _ => {}
            }
        }
        let (Some(name_att), Some(compressed_att), Some(file_att)) =
            (name_att, compressed_att, file_att)
        else {
            pg_sys::table_close(rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
            return None;
        };

        let snapshot = pg_sys::GetActiveSnapshot();
        let flags: u32 = pg_sys::ScanOptions::SO_TYPE_SEQSCAN
            | pg_sys::ScanOptions::SO_ALLOW_STRAT
            | pg_sys::ScanOptions::SO_ALLOW_SYNC
            | pg_sys::ScanOptions::SO_ALLOW_PAGEMODE;
        let scan = (*(*rel).rd_tableam).scan_begin.unwrap()(
            rel,
            snapshot,
            0,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            flags,
        );

        let mut values = vec![pg_sys::Datum::from(0); natts];
        let mut nulls = vec![true; natts];
        let mut result: Option<String> = None;
        loop {
            let tuple = pg_sys::heap_getnext(scan, pg_sys::ScanDirection::ForwardScanDirection);
            if tuple.is_null() {
                break;
            }
            pg_sys::heap_deform_tuple(tuple, tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr());
            if nulls[name_att] || nulls[compressed_att] || nulls[file_att] {
                continue;
            }
            if values[compressed_att].value() == 0 {
                continue; // not compressed
            }
            if text_datum_to_vec(values[name_att]) != partition_name.as_bytes() {
                continue;
            }
            result = String::from_utf8(text_datum_to_vec(values[file_att])).ok();
            break;
        }
        (*(*rel).rd_tableam).scan_end.unwrap()(scan);
        pg_sys::table_close(rel, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
        result
    }
}

// ============================================================================
// Unit tests (file format only — no server needed)
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_blobs() -> Vec<(u16, i32, Vec<u8>)> {
        vec![
            (0, 1, vec![1u8; 10]),
            (0, 2, vec![2u8; 100]),
            (1, 1, (0..255u8).collect()),
            (2, 1, vec![]),
            (2, 7, vec![0xAB; 65]),
        ]
    }

    fn tmp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "dxs_test_{}_{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn crc32c_known_vectors() {
        // RFC 3720 test vector: 32 bytes of zeros.
        assert_eq!(crc32c(&[0u8; 32]), 0x8A91_36AA);
        // "123456789" → 0xE3069283 (standard CRC-32C check value).
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b""), 0);
    }

    #[test]
    fn encode_parse_roundtrip() {
        let blobs = sample_blobs();
        let image = encode_segment_file(42, &blobs);
        let (header, entries) = parse_and_validate(&image).unwrap();
        assert_eq!(header.version, VERSION);
        assert_eq!(header.partition_id, 42);
        assert_eq!(header.n_columns, 3);
        assert_eq!(header.n_segments, 3); // segment ids {1, 2, 7}
        assert_eq!(entries.len(), blobs.len());
        for ((ci, sid, blob), e) in blobs.iter().zip(&entries) {
            assert_eq!((e.col_idx, e.segment_id), (*ci, *sid));
            assert_eq!(e.len as usize, blob.len());
            assert_eq!(e.offset % BLOB_ALIGN, 0, "blob not 64-byte aligned");
            assert_eq!(
                &image[e.offset as usize..e.offset as usize + e.len as usize],
                blob.as_slice()
            );
            assert_eq!(e.crc, crc32c(blob));
        }
    }

    #[test]
    fn mmap_lookup_roundtrip() {
        let blobs = sample_blobs();
        let image = encode_segment_file(7, &blobs);
        let dir = tmp_dir();
        std::fs::write(dir.join("7_1.dxs"), &image).unwrap();
        let f = MappedSegmentFile::open(&dir.join("7_1.dxs")).unwrap();
        for (ci, sid, blob) in &blobs {
            match f.get(*ci, *sid, true) {
                BlobLookup::Found(slice) => assert_eq!(slice, blob.as_slice()),
                _ => panic!("blob (col {ci}, seg {sid}) not found"),
            }
        }
        // Absent entries are Absent, not Corrupt.
        assert!(matches!(f.get(0, 99, true), BlobLookup::Absent));
        assert!(matches!(f.get(99, 1, true), BlobLookup::Absent));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn corrupt_blob_byte_detected_by_crc() {
        let blobs = sample_blobs();
        let mut image = encode_segment_file(7, &blobs);
        let (_, entries) = parse_and_validate(&image).unwrap();
        // Flip one byte inside the second blob's data.
        let victim = entries[1];
        image[victim.offset as usize + 3] ^= 0xFF;
        let dir = tmp_dir();
        std::fs::write(dir.join("7_2.dxs"), &image).unwrap();
        let f = MappedSegmentFile::open(&dir.join("7_2.dxs")).unwrap();
        assert!(matches!(
            f.get(victim.col_idx, victim.segment_id, true),
            BlobLookup::Corrupt
        ));
        // Without verification the (corrupt) bytes are still served.
        assert!(matches!(
            f.get(victim.col_idx, victim.segment_id, false),
            BlobLookup::Found(_)
        ));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn truncation_detected() {
        let image = encode_segment_file(1, &sample_blobs());
        let truncated = &image[..image.len() - 1];
        assert!(parse_and_validate(truncated).is_err());
        assert!(parse_and_validate(&image[..10]).is_err());
        assert!(parse_and_validate(&[]).is_err());
    }

    #[test]
    fn corrupt_header_and_index_detected() {
        let blobs = sample_blobs();
        let good = encode_segment_file(1, &blobs);

        let mut bad_magic = good.clone();
        bad_magic[0] = b'X';
        assert!(parse_and_validate(&bad_magic).is_err());

        let mut bad_version = good.clone();
        bad_version[6] = 99;
        assert!(parse_and_validate(&bad_version).is_err());

        // Flip a byte in the header payload → header crc mismatch.
        let mut bad_header = good.clone();
        bad_header[12] ^= 0xFF;
        assert!(parse_and_validate(&bad_header).is_err());

        // Flip a byte inside the index → index crc mismatch.
        let (header, _) = parse_and_validate(&good).unwrap();
        let mut bad_index = good.clone();
        bad_index[header.index_offset as usize + 1] ^= 0xFF;
        assert!(parse_and_validate(&bad_index).is_err());
    }

    #[test]
    fn incremental_writer_matches_one_shot_encoder() {
        // A single sorted batch through the incremental writer must produce
        // a byte-identical file to `encode_segment_file`.
        let blobs = sample_blobs();
        let image = encode_segment_file(42, &blobs);
        let dir = tmp_dir();
        let mut w = SegmentFileWriter::create_at(&dir, "42_w.dxs", 42).unwrap();
        w.append_blobs(&blobs).unwrap();
        w.finish().unwrap();
        let written = std::fs::read(dir.join("42_w.dxs")).unwrap();
        assert_eq!(written, image);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn incremental_writer_multi_batch_roundtrip() {
        // Batches are column-major within themselves but interleave across
        // batches (col 0 of batch 2 lands after col 2 of batch 1, as happens
        // with BLOB_BUFFER_THRESHOLD early flushes). The footer index must
        // cover all entries and every lookup must succeed.
        let batch1 = vec![(0u16, 1i32, vec![1u8; 10]), (1, 1, vec![9u8; 33])];
        let batch2 = vec![
            (0u16, 2i32, vec![2u8; 100]),
            (1, 2, (0..255u8).collect::<Vec<_>>()),
            (2, 2, vec![0xCD; 7]),
        ];
        let dir = tmp_dir();
        let mut w = SegmentFileWriter::create_at(&dir, "9_w.dxs", 9).unwrap();
        w.append_blobs(&batch1).unwrap();
        w.append_blobs(&batch2).unwrap();
        w.finish().unwrap();

        let written = std::fs::read(dir.join("9_w.dxs")).unwrap();
        let (header, entries) = parse_and_validate(&written).unwrap();
        assert_eq!(header.partition_id, 9);
        assert_eq!(header.n_columns, 3);
        assert_eq!(header.n_segments, 2);
        assert_eq!(entries.len(), batch1.len() + batch2.len());
        for e in &entries {
            assert_eq!(e.offset % BLOB_ALIGN, 0, "blob not 64-byte aligned");
        }

        let f = MappedSegmentFile::open(&dir.join("9_w.dxs")).unwrap();
        for (ci, sid, blob) in batch1.iter().chain(&batch2) {
            match f.get(*ci, *sid, true) {
                BlobLookup::Found(slice) => assert_eq!(slice, blob.as_slice()),
                _ => panic!("blob (col {ci}, seg {sid}) not found"),
            }
        }
        assert!(matches!(f.get(0, 99, true), BlobLookup::Absent));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn writer_abandon_removes_tmp() {
        let dir = tmp_dir();
        let mut w = SegmentFileWriter::create_at(&dir, "5_a.dxs", 5).unwrap();
        w.append_blobs(&[(0, 1, vec![7u8; 16])]).unwrap();
        let tmp = dir.join("5_a.dxs.tmp");
        assert!(tmp.exists());
        w.abandon();
        assert!(!tmp.exists());
        assert!(!dir.join("5_a.dxs").exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn empty_blob_list_roundtrip() {
        let image = encode_segment_file(3, &[]);
        let (header, entries) = parse_and_validate(&image).unwrap();
        assert_eq!(header.n_columns, 0);
        assert_eq!(header.n_segments, 0);
        assert!(entries.is_empty());
    }
}
