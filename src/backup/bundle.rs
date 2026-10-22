//! Batch F bundle format: a versioned ZIP with `manifest.json`, typed
//! table entries, and both media pools.
//!
//! Layout:
//! * `manifest.json` — format/app/commit/redb/schema versions, timestamps,
//!   per-table counts, per-component checksums+sizes, media inventory,
//!   referenced-but-missing blob gaps, and the overall checksum.
//! * `tables/<name>.tbl` — binary rows: magic `PBT1`, then per row u32-LE
//!   key len, u32-LE value len, key bytes, value bytes. Keys/values are the
//!   exact redb key/value bytes for the table's declared types (see
//!   `RedbStore::export_backup_snapshot`). The authoritative row count lives
//!   in the manifest; the file carries no count header (a ZipWriter entry
//!   cannot be patched after the fact, and buffering whole tables in RAM to
//!   pre-count them would break the streaming guarantee).
//! * `media/durable/...` and `media/cache/...` — content-addressed objects
//!   plus `.len` sidecars, mirroring the pool `objects/` trees. Transient
//!   `tmp/` staging is never archived; unexpected shapes are skipped and
//!   counted, never trusted.
//!
//! Streaming: rows and blob bytes are written incrementally; only the
//! manifest (counts + checksums) accumulates in memory. Output goes to a
//! `.partial-<jobid>.zip` staging file and is atomically renamed only after
//! sync + full re-verification.

use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{BackupContext, BackupError, now_ms};
use crate::media::storage::media_gate_snapshot;

/// Bundle format version (bump only with an explicit tested migration).
pub const BACKUP_FORMAT_VERSION: u32 = 1;
/// Persistence schema generation this binary writes/reads. Any other value
/// refuses restore by default (explicit tested migrations only — none yet).
pub const BACKUP_SCHEMA_VERSION: u32 = 1;
/// Pinned redb version the bytes were exported with.
pub const BACKUP_REDB_VERSION: &str = "4.4.0";
pub const MANIFEST_NAME: &str = "manifest.json";
pub const TABLES_PREFIX: &str = "tables/";
pub const TABLE_SUFFIX: &str = ".tbl";
pub const MEDIA_DURABLE_PREFIX: &str = "media/durable/";
pub const MEDIA_CACHE_PREFIX: &str = "media/cache/";
/// Per-row byte cap (key+value). Rows are small metadata/JSON; anything
/// larger is a corrupt bundle, never a real row.
pub const MAX_TABLE_ROW_BYTES: u64 = 64 * 1024 * 1024;
/// Per-table file cap.
pub const MAX_TABLE_FILE_BYTES: u64 = 8 * 1024 * 1024 * 1024;
/// Whole-bundle cap (also bounds restore reads).
pub const MAX_BUNDLE_BYTES: u64 = 64 * 1024 * 1024 * 1024;
/// Manifest cap (counts + checksums only; a larger manifest is corrupt).
pub const MAX_MANIFEST_BYTES: u64 = 64 * 1024 * 1024;
/// Table-entry file magic.
pub(crate) const TABLE_MAGIC: &[u8; 4] = b"PBT1";

pub fn table_entry_name(table: &str) -> String {
    format!("{TABLES_PREFIX}{table}{TABLE_SUFFIX}")
}

pub fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// One table's manifest line.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableManifest {
    pub name: String,
    pub key_sig: String,
    pub value_sig: String,
    pub rows: u64,
    pub bytes: u64,
    pub sha256: String,
}

/// One archived blob's manifest line.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaFileManifest {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}

/// Bundle manifest (serialized as `manifest.json`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: u32,
    pub app_version: String,
    pub app_commit: String,
    pub redb_version: String,
    pub schema_version: u32,
    pub created_ms: u64,
    pub snapshot_id: String,
    pub tables: Vec<TableManifest>,
    pub media_durable_files: u64,
    pub media_durable_bytes: u64,
    pub media_cache_files: u64,
    pub media_cache_bytes: u64,
    pub media_files: Vec<MediaFileManifest>,
    /// Pool files skipped as unexpected shapes (never trusted, just
    /// counted so the operator can reconcile).
    pub media_skipped_files: u64,
    /// Blob keys referenced by the snapshot whose bytes were absent from
    /// both pools at copy time (pre-existing tear; deletions are barred
    /// during backup so these predate the run). Restore refuses bundles
    /// with non-empty gaps.
    pub media_gaps: Vec<String>,
    /// Overall verification: sha256 over the canonical component-inventory
    /// lines (see [`compute_overall`]).
    pub overall_sha256: String,
}

/// Canonical overall checksum: deterministic over the sorted component
/// inventory, so any tampered size/count/checksum fails verification.
pub fn compute_overall(
    snapshot_id: &str,
    tables: &[TableManifest],
    media_files: &[MediaFileManifest],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(
        format!(
            "pinetree-backup-v1:{BACKUP_FORMAT_VERSION}:{BACKUP_SCHEMA_VERSION}:{snapshot_id}\n"
        )
        .as_bytes(),
    );
    let mut names: Vec<&TableManifest> = tables.iter().collect();
    names.sort_by(|a, b| a.name.cmp(&b.name));
    for table in names {
        hasher.update(
            format!(
                "table:{}:{}:{}:{}\n",
                table.name, table.rows, table.bytes, table.sha256
            )
            .as_bytes(),
        );
    }
    let mut files: Vec<&MediaFileManifest> = media_files.iter().collect();
    files.sort_by(|a, b| a.path.cmp(&b.path));
    for file in files {
        hasher.update(format!("media:{}:{}:{}\n", file.path, file.bytes, file.sha256).as_bytes());
    }
    hex::encode(hasher.finalize())
}

/// Outcome of [`create_bundle`] (safe to log: names/counts only).
pub struct BundleOutcome {
    pub file_name: String,
    pub bytes_written: u64,
    pub tables_exported: u64,
    pub media_files: u64,
    pub gate_wait_ms: u64,
}

/// Blob filename rules mirror `FsBlobStore` (alphanumeric hash, optional
/// `.len` sidecar). Returns whether the file is a sidecar; `None` means an
/// unexpected shape (skip + count, never trust).
fn classify_blob_file_name(name: &str) -> Option<bool> {
    let bare = name.strip_suffix(".len").unwrap_or(name);
    if bare.len() < 4
        || bare.len() > 128
        || !bare.bytes().all(|b| b.is_ascii_alphanumeric())
        || name.len() > 140
    {
        return None;
    }
    Some(name.ends_with(".len"))
}

fn is_staging(name: &str) -> bool {
    name.ends_with(".tmp")
}

/// Deterministic pool walk: sorted `(archive-relative path, fs path)`.
/// Skips `tmp/` staging; unexpected shapes are counted in `skipped`.
fn walk_pool_objects(
    pool: &Path,
    skipped: &mut u64,
) -> Result<Vec<(String, PathBuf)>, BackupError> {
    let mut out = Vec::new();
    let objects = pool.join("objects");
    if !objects.is_dir() {
        return Ok(out);
    }
    let mut shards: Vec<PathBuf> = std::fs::read_dir(&objects)
        .map_err(|e| BackupError::Storage(format!("read pool objects: {e}")))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.is_dir())
        .collect();
    shards.sort();
    for shard in shards {
        let shard_name = shard
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        let mut files: Vec<PathBuf> = std::fs::read_dir(&shard)
            .map_err(|e| BackupError::Storage(format!("read pool shard: {e}")))?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.is_file())
            .collect();
        files.sort();
        for file in files {
            let name = file
                .file_name()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default();
            if is_staging(&name) {
                continue;
            }
            if classify_blob_file_name(&name).is_none() {
                *skipped += 1;
                continue;
            }
            out.push((format!("{shard_name}/{name}"), file));
        }
    }
    Ok(out)
}

fn zip_options_blobs() -> zip::write::SimpleFileOptions {
    zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored)
}

fn zip_options_deflated() -> zip::write::SimpleFileOptions {
    // `zip` is vendored with `default-features = false, features =
    // ["deflate"]`, so Deflated is available; tables/manifest compress well
    // while blobs stay Stored (already-compressed media).
    zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated)
}

/// Estimate source bytes (db file + pool trees) for the pre-flight disk
/// guard. This bounds the staging output against [`MAX_BUNDLE_BYTES`]; true
/// free-space probing is the operator's job (see the cutover snippet).
fn estimate_source_bytes(ctx: &BackupContext) -> Result<u64, BackupError> {
    let mut total = std::fs::metadata(ctx.store.path())
        .map(|m| m.len())
        .unwrap_or(0);
    let mut skipped = 0u64;
    for pool in [&ctx.durable_pool, &ctx.cache_pool] {
        for (_, path) in walk_pool_objects(pool, &mut skipped)? {
            total = total.saturating_add(std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0));
        }
    }
    Ok(total)
}

/// Incremental per-table sink: one open zip entry per table plus a running
/// hasher, so memory stays flat regardless of table size. Tables must arrive
/// in registry order without interleaving (guaranteed by
/// `export_backup_snapshot`, enforced here).
struct TableSink<'w> {
    writer: &'w mut zip::ZipWriter<std::fs::File>,
    current_name: Option<String>,
    current_hasher: Sha256,
    current_bytes: u64,
    current_rows: u64,
    seen: BTreeSet<String>,
    manifests: Vec<TableManifest>,
}

impl<'w> TableSink<'w> {
    fn new(writer: &'w mut zip::ZipWriter<std::fs::File>) -> Self {
        Self {
            writer,
            current_name: None,
            current_hasher: Sha256::new(),
            current_bytes: 0,
            current_rows: 0,
            seen: BTreeSet::new(),
            manifests: Vec::new(),
        }
    }

    fn emit(
        &mut self,
        table: &str,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<(), crate::application::ports::PortError> {
        use crate::application::ports::PortError;
        if key.len() as u64 + value.len() as u64 > MAX_TABLE_ROW_BYTES {
            return Err(PortError::StorageUnavailable(format!(
                "backup row exceeds cap in table {table}"
            )));
        }
        let switching = self.current_name.as_deref() != Some(table);
        if switching {
            self.finish_current()?;
            if !self.seen.insert(table.to_string()) {
                return Err(PortError::StorageUnavailable(format!(
                    "backup table stream interleaved at {table}"
                )));
            }
            self.writer
                .start_file(table_entry_name(table), zip_options_deflated())
                .map_err(|e| PortError::StorageUnavailable(format!("stage table {table}: {e}")))?;
            self.writer
                .write_all(TABLE_MAGIC)
                .map_err(|e| PortError::StorageUnavailable(format!("write table magic: {e}")))?;
            self.current_name = Some(table.to_string());
            self.current_hasher = Sha256::new();
            self.current_hasher.update(TABLE_MAGIC);
            self.current_bytes = TABLE_MAGIC.len() as u64;
            self.current_rows = 0;
        }
        let mut rec = Vec::with_capacity(8 + key.len() + value.len());
        rec.extend_from_slice(&(key.len() as u32).to_le_bytes());
        rec.extend_from_slice(&(value.len() as u32).to_le_bytes());
        rec.extend_from_slice(&key);
        rec.extend_from_slice(&value);
        self.writer
            .write_all(&rec)
            .map_err(|e| PortError::StorageUnavailable(format!("write table row: {e}")))?;
        self.current_hasher.update(&rec);
        self.current_bytes += rec.len() as u64;
        self.current_rows += 1;
        if self.current_bytes > MAX_TABLE_FILE_BYTES {
            return Err(PortError::StorageUnavailable(format!(
                "backup table {table} exceeds file cap"
            )));
        }
        Ok(())
    }

    fn finish_current(&mut self) -> Result<(), crate::application::ports::PortError> {
        use crate::application::ports::PortError;
        let Some(name) = self.current_name.take() else {
            return Ok(());
        };
        let (key_sig, value_sig) = crate::persistence::redb::backup_table_signature(&name)
            .ok_or_else(|| {
                PortError::StorageUnavailable(format!("backup exported unknown table {name}"))
            })?;
        // `Sha256` is not `Clone`; replace the hasher with a fresh one and
        // finalize the old value via `finalize_reset` on a swap.
        let mut hasher = Sha256::new();
        std::mem::swap(&mut hasher, &mut self.current_hasher);
        self.manifests.push(TableManifest {
            name,
            key_sig: key_sig.to_string(),
            value_sig: value_sig.to_string(),
            rows: self.current_rows,
            bytes: self.current_bytes,
            sha256: hex::encode(hasher.finalize()),
        });
        self.current_bytes = 0;
        self.current_rows = 0;
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<TableManifest>, BackupError> {
        self.finish_current()
            .map_err(|e| BackupError::Storage(format!("finish table stream: {e}")))?;
        // Materialize every registry table, even empty ones: restore
        // validates the full table set, and an absent file is
        // indistinguishable from a missing component. Empty tables get a
        // magic-only file with rows: 0.
        for name in crate::persistence::redb::backup_table_names() {
            if self.seen.contains(name) {
                continue;
            }
            let (key_sig, value_sig) =
                crate::persistence::redb::backup_table_signature(name).ok_or_else(|| {
                    BackupError::Storage(format!("backup registry table without signature {name}"))
                })?;
            self.writer
                .start_file(table_entry_name(name), zip_options_deflated())
                .map_err(|e| BackupError::Storage(format!("stage empty table {name}: {e}")))?;
            self.writer
                .write_all(TABLE_MAGIC)
                .map_err(|e| BackupError::Storage(format!("write table magic: {e}")))?;
            let mut hasher = Sha256::new();
            hasher.update(TABLE_MAGIC);
            self.manifests.push(TableManifest {
                name: name.to_string(),
                key_sig: key_sig.to_string(),
                value_sig: value_sig.to_string(),
                rows: 0,
                bytes: TABLE_MAGIC.len() as u64,
                sha256: hex::encode(hasher.finalize()),
            });
        }
        Ok(self.manifests)
    }
}

/// Build the bundle: exclusive gate → one consistent redb snapshot → pool
/// copies → manifest → sync+verify → atomic publish. Any failure leaves the
/// staging file (never the final name) for startup cleanup.
pub fn create_bundle(ctx: &BackupContext, job_id: &str) -> Result<BundleOutcome, BackupError> {
    let wait_before = media_gate_snapshot().wait_ms_total;
    let _gate = crate::media::storage::acquire_backup_gate(job_id).map_err(|busy| {
        BackupError::Busy(format!(
            "backup already running ({})",
            busy.active_backup_id
        ))
    })?;
    let estimated = estimate_source_bytes(ctx)?;
    if estimated > MAX_BUNDLE_BYTES {
        return Err(BackupError::Invalid(
            "backup source estimate exceeds bundle cap".to_string(),
        ));
    }
    let staging = ctx.backup_dir.join(format!(
        "{}{job_id}{}",
        super::BACKUP_PARTIAL_PREFIX,
        super::BACKUP_FILE_SUFFIX
    ));
    let file = std::fs::File::create(&staging)
        .map_err(|e| BackupError::Storage(format!("create staging bundle: {e}")))?;
    let mut writer = zip::ZipWriter::new(file);

    // --- Tables: one consistent read txn, streamed entry by entry. ---
    let mut table_manifests = {
        let mut sink = TableSink::new(&mut writer);
        ctx.store
            .export_backup_snapshot(&mut |table, key, value| sink.emit(table, key, value))
            .map_err(|e| BackupError::Storage(format!("export snapshot: {e}")))?;
        sink.finish()?
    };

    // Snapshot-referenced blob keys: every non-evicted `blob_key` across
    // the media table (small projection walk, not full-row retention).
    let referenced_blobs = collect_referenced_blobs(ctx)?;

    // --- Media pools: full copy under the same deletion barrier. ---
    // A full-pool copy may carry harmless orphan bytes; it must never miss
    // a blob the snapshot references (deletions are barred by the gate, so
    // any gap predates the run and is recorded, not hidden).
    let mut media_files: Vec<MediaFileManifest> = Vec::new();
    let mut durable_files = 0u64;
    let mut durable_bytes = 0u64;
    let mut cache_files = 0u64;
    let mut cache_bytes = 0u64;
    let mut skipped = 0u64;
    let mut present: BTreeSet<String> = BTreeSet::new();
    let pools: [(&Path, &str); 2] = [
        (&ctx.durable_pool, MEDIA_DURABLE_PREFIX),
        (&ctx.cache_pool, MEDIA_CACHE_PREFIX),
    ];
    for (pool, prefix) in pools {
        for (rel, fs_path) in walk_pool_objects(pool, &mut skipped)? {
            let archive_path = format!("{prefix}objects/{rel}");
            let (size, sha) = stream_file_into_zip(&mut writer, &archive_path, &fs_path)?;
            media_files.push(MediaFileManifest {
                path: archive_path,
                bytes: size,
                sha256: sha,
            });
            if prefix == MEDIA_DURABLE_PREFIX {
                durable_files += 1;
                durable_bytes = durable_bytes.saturating_add(size);
            } else {
                cache_files += 1;
                cache_bytes = cache_bytes.saturating_add(size);
            }
            let stem = rel
                .rsplit('/')
                .next()
                .unwrap_or(&rel)
                .strip_suffix(".len")
                .unwrap_or(rel.rsplit('/').next().unwrap_or(&rel))
                .to_string();
            present.insert(stem);
        }
    }
    // Keep table lines in registry order (deterministic manifest).
    table_manifests.sort_by(|a, b| a.name.cmp(&b.name));
    let media_gaps: Vec<String> = referenced_blobs.difference(&present).cloned().collect();

    // --- Manifest (written last; counts + checksums only). ---
    let created_ms = now_ms();
    let manifest = Manifest {
        format_version: BACKUP_FORMAT_VERSION,
        app_version: env!("CARGO_PKG_VERSION").to_string(),
        app_commit: option_env!("PINETREE_COMMIT")
            .unwrap_or("unknown")
            .to_string(),
        redb_version: BACKUP_REDB_VERSION.to_string(),
        schema_version: BACKUP_SCHEMA_VERSION,
        created_ms,
        snapshot_id: job_id.to_string(),
        tables: table_manifests,
        media_durable_files: durable_files,
        media_durable_bytes: durable_bytes,
        media_cache_files: cache_files,
        media_cache_bytes: cache_bytes,
        media_files: media_files.clone(),
        media_skipped_files: skipped,
        media_gaps,
        overall_sha256: String::new(),
    };
    let overall = compute_overall(job_id, &manifest.tables, &manifest.media_files);
    let manifest = Manifest {
        overall_sha256: overall,
        ..manifest
    };
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)
        .map_err(|e| BackupError::Storage(format!("encode manifest: {e}")))?;
    writer
        .start_file(MANIFEST_NAME, zip_options_deflated())
        .map_err(|e| BackupError::Storage(format!("stage manifest: {e}")))?;
    writer
        .write_all(&manifest_bytes)
        .map_err(|e| BackupError::Storage(format!("write manifest: {e}")))?;
    let staging_file = writer
        .finish()
        .map_err(|e| BackupError::Storage(format!("finish bundle: {e}")))?;
    staging_file
        .sync_all()
        .map_err(|e| BackupError::Storage(format!("sync bundle: {e}")))?;
    drop(staging_file);

    // --- Verify the staging bundle before it may look final. ---
    verify_staging_bundle(&staging, &manifest)?;

    let bytes_written = std::fs::metadata(&staging).map(|m| m.len()).unwrap_or(0);
    if bytes_written > MAX_BUNDLE_BYTES {
        let _ = std::fs::remove_file(&staging);
        return Err(BackupError::Invalid("bundle exceeds size cap".to_string()));
    }
    let file_name = format!(
        "pinetree-backup-{created_ms}-{job_id}{}",
        super::BACKUP_FILE_SUFFIX
    );
    let final_path = ctx.backup_dir.join(&file_name);
    if final_path.exists() {
        let _ = std::fs::remove_file(&staging);
        return Err(BackupError::Invalid("backup output collision".to_string()));
    }
    std::fs::rename(&staging, &final_path)
        .map_err(|e| BackupError::Storage(format!("publish bundle: {e}")))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&final_path, std::fs::Permissions::from_mode(0o600));
    }
    let gate_wait_ms = media_gate_snapshot()
        .wait_ms_total
        .saturating_sub(wait_before);
    Ok(BundleOutcome {
        file_name,
        bytes_written,
        tables_exported: manifest.tables.len() as u64,
        media_files: manifest.media_files.len() as u64,
        gate_wait_ms,
    })
}

/// Snapshot-referenced blob keys: every non-evicted `blob_key` in the media
/// table, walked newest-first with exclusive `until_id` cursors (empty page
/// terminates — never a short page, per the Batch C paging contract).
fn collect_referenced_blobs(ctx: &BackupContext) -> Result<BTreeSet<String>, BackupError> {
    use crate::application::ports::{MediaStore, TimelineCursor};
    use crate::persistence::redb::MAX_MEDIA_PAGE_ROWS;
    let mut out = BTreeSet::new();
    let mut cursor = TimelineCursor::with_limit(MAX_MEDIA_PAGE_ROWS);
    loop {
        let page = ctx
            .store
            .list_all_media(&cursor)
            .map_err(|e| BackupError::Storage(format!("read media for blob refs: {e}")))?;
        if page.is_empty() {
            break;
        }
        for meta in &page {
            if !meta.evicted && !meta.blob_key.is_empty() {
                out.insert(meta.blob_key.clone());
            }
        }
        if page.len() < cursor.limit {
            break;
        }
        let last = page.last().map(|m| m.id.clone()).unwrap_or_default();
        cursor = TimelineCursor::with_limit(MAX_MEDIA_PAGE_ROWS);
        cursor.until_id = Some(last);
    }
    Ok(out)
}

fn stream_file_into_zip(
    writer: &mut zip::ZipWriter<std::fs::File>,
    archive_path: &str,
    fs_path: &Path,
) -> Result<(u64, String), BackupError> {
    let mut file = std::fs::File::open(fs_path)
        .map_err(|e| BackupError::Storage(format!("open pool file: {e}")))?;
    writer
        .start_file(archive_path, zip_options_blobs())
        .map_err(|e| BackupError::Storage(format!("stage blob: {e}")))?;
    let mut hasher = Sha256::new();
    let mut bytes = 0u64;
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = file
            .read(&mut buf)
            .map_err(|e| BackupError::Storage(format!("read pool file: {e}")))?;
        if n == 0 {
            break;
        }
        writer
            .write_all(&buf[..n])
            .map_err(|e| BackupError::Storage(format!("write blob: {e}")))?;
        hasher.update(&buf[..n]);
        bytes = bytes.saturating_add(n as u64);
        if bytes > MAX_BUNDLE_BYTES {
            return Err(BackupError::Invalid("pool file exceeds cap".to_string()));
        }
    }
    Ok((bytes, hex::encode(hasher.finalize())))
}

/// Re-open the staging bundle and verify every component checksum plus the
/// overall checksum before the file may take its final name.
fn verify_staging_bundle(staging: &Path, manifest: &Manifest) -> Result<(), BackupError> {
    let file = std::fs::File::open(staging)
        .map_err(|e| BackupError::Storage(format!("reopen staging: {e}")))?;
    let mut archive = zip::ZipArchive::new(file)
        .map_err(|e| BackupError::Corrupt(format!("staging zip: {e}")))?;
    if compute_overall(
        &manifest.snapshot_id,
        &manifest.tables,
        &manifest.media_files,
    ) != manifest.overall_sha256
    {
        return Err(BackupError::Corrupt("staging overall checksum".to_string()));
    }
    for table in &manifest.tables {
        let mut entry = archive
            .by_name(&table_entry_name(&table.name))
            .map_err(|e| BackupError::Corrupt(format!("staging table {}: {e}", table.name)))?;
        verify_stream(&mut entry, table.bytes, &table.sha256)?;
    }
    for media in &manifest.media_files {
        let mut entry = archive
            .by_name(&media.path)
            .map_err(|e| BackupError::Corrupt(format!("staging media {}: {e}", media.path)))?;
        verify_stream(&mut entry, media.bytes, &media.sha256)?;
    }
    Ok(())
}

fn verify_stream(
    reader: &mut impl Read,
    expected_bytes: u64,
    expected_sha: &str,
) -> Result<(), BackupError> {
    let mut hasher = Sha256::new();
    let mut bytes = 0u64;
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = reader
            .read(&mut buf)
            .map_err(|e| BackupError::Corrupt(format!("read staged entry: {e}")))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        bytes = bytes.saturating_add(n as u64);
        if bytes > expected_bytes.saturating_add(16 * 1024 * 1024) {
            return Err(BackupError::Corrupt("staged entry overflows".to_string()));
        }
    }
    if bytes != expected_bytes || hex::encode(hasher.finalize()) != expected_sha {
        return Err(BackupError::Corrupt("staged entry checksum".to_string()));
    }
    Ok(())
}

/// Decode one `tables/<name>.tbl` payload into rows. The manifest count is
/// authoritative: header magic must match and the decoded row count must
/// equal `expected_rows` exactly.
pub fn decode_table_file(
    bytes: &[u8],
    expected_rows: u64,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, BackupError> {
    if bytes.len() < TABLE_MAGIC.len() || &bytes[..TABLE_MAGIC.len()] != TABLE_MAGIC {
        return Err(BackupError::Corrupt("bad table magic".to_string()));
    }
    if expected_rows > MAX_TABLE_ROW_BYTES * 1024 {
        return Err(BackupError::Corrupt(
            "table row count exceeds cap".to_string(),
        ));
    }
    let mut rows = Vec::new();
    let mut pos = TABLE_MAGIC.len();
    while pos < bytes.len() {
        if bytes.len() - pos < 8 {
            return Err(BackupError::Corrupt("truncated table row".to_string()));
        }
        let klen = u32::from_le_bytes(
            bytes[pos..pos + 4]
                .try_into()
                .map_err(|_| BackupError::Corrupt("bad row header".to_string()))?,
        ) as usize;
        let vlen = u32::from_le_bytes(
            bytes[pos + 4..pos + 8]
                .try_into()
                .map_err(|_| BackupError::Corrupt("bad row header".to_string()))?,
        ) as usize;
        pos += 8;
        if klen as u64 + vlen as u64 > MAX_TABLE_ROW_BYTES {
            return Err(BackupError::Corrupt("table row exceeds cap".to_string()));
        }
        if bytes.len() - pos < klen + vlen {
            return Err(BackupError::Corrupt("truncated table payload".to_string()));
        }
        rows.push((
            bytes[pos..pos + klen].to_vec(),
            bytes[pos + klen..pos + klen + vlen].to_vec(),
        ));
        pos += klen + vlen;
        if rows.len() as u64 > expected_rows {
            return Err(BackupError::Corrupt("table holds more rows than the manifest declares".to_string()));
        }
    }
    if rows.len() as u64 != expected_rows {
        return Err(BackupError::Corrupt(format!(
            "table row count mismatch (manifest {expected_rows}, actual {})",
            rows.len()
        )));
    }
    Ok(rows)
}
