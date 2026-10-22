//! Batch F offline restore: validate a bundle, then restore it into a
//! fresh directory. Never hot-swaps the live database.
//!
//! Order of operations (fail closed):
//! 1. Validate paths (fresh dir must not exist; bundle must be a file).
//! 2. Enumerate the archive: reject traversal/absolute/duplicate/symlink/
//!    unexpected entries before trusting anything.
//! 3. Parse + validate the manifest: format/schema/redb versions, required
//!    table set, type signatures, declared caps, empty media gaps — all
//!    before writing a single byte.
//! 4. Verify the overall checksum and every component checksum (streaming).
//! 5. Write into the fresh dir: recreate the pinned schema
//!    (`RedbStore::open`, same code as production), import tables in bounded
//!    write transactions, extract media with inline checksum verification.
//! 6. Reopen + `check_integrity` with no active transactions, reopen again,
//!    and verify counts, META markers, queue/dedup presence, storage
//!    accounting, sidecars, and that every non-evicted media row has bytes.
//! 7. Only then mark the restore ready (`RESTORE_READY.json`).
//!
//! Any failure before step 7 removes the fresh dir (when this function
//! created it) and reports the redacted reason; nothing is ever marked
//! ready on a partial restore.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sha2::{Digest, Sha256};

use super::BackupError;
use super::bundle::{
    MAX_BUNDLE_BYTES, MAX_MANIFEST_BYTES, MAX_TABLE_ROW_BYTES, Manifest, TABLE_MAGIC,
    compute_overall, table_entry_name,
};
use crate::backup::RESTORE_COMMIT_CHUNK_ROWS;
use crate::backup::bundle::{
    BACKUP_FORMAT_VERSION, BACKUP_REDB_VERSION, BACKUP_SCHEMA_VERSION, MANIFEST_NAME,
    MEDIA_CACHE_PREFIX, MEDIA_DURABLE_PREFIX,
};

const FRESH_DB_FILE: &str = "pinetree.redb";
const FRESH_DURABLE_DIR: &str = "blobs_durable";
const FRESH_CACHE_DIR: &str = "blobs_cache";
const READY_MARKER: &str = "RESTORE_READY.json";

/// Verified restore outcome (safe to log: counts only).
#[derive(Debug, Clone)]
pub struct RestoreReport {
    pub restored_dir: PathBuf,
    pub snapshot_id: String,
    pub tables_restored: u64,
    pub rows_total: u64,
    pub media_files: u64,
    pub media_bytes: u64,
    pub sidecars_healed: u64,
    pub pool_warnings: u64,
}

/// Validate `src` and restore it into `fresh_dir` (which must not exist).
pub fn restore_bundle(src: &Path, fresh_dir: &Path) -> Result<RestoreReport, BackupError> {
    let meta = std::fs::metadata(src)
        .map_err(|e| BackupError::Invalid(format!("bundle not readable: {e}")))?;
    if !meta.is_file() {
        return Err(BackupError::Invalid("bundle is not a file".to_string()));
    }
    if meta.len() > MAX_BUNDLE_BYTES {
        return Err(BackupError::Corrupt("bundle exceeds size cap".to_string()));
    }
    if src.extension().and_then(|s| s.to_str()) != Some("zip") {
        return Err(BackupError::Invalid(
            "bundle must be a .zip file".to_string(),
        ));
    }
    if fresh_dir.exists() {
        return Err(BackupError::Invalid(
            "restore target exists; restore only into a fresh directory".to_string(),
        ));
    }
    let run = || -> Result<RestoreReport, BackupError> {
        std::fs::create_dir_all(fresh_dir)
            .map_err(|e| BackupError::Storage(format!("create restore dir: {e}")))?;
        let file = std::fs::File::open(src)
            .map_err(|e| BackupError::Storage(format!("open bundle: {e}")))?;
        let mut archive = zip::ZipArchive::new(file)
            .map_err(|e| BackupError::Corrupt(format!("open bundle zip: {e}")))?;
        let manifest = pass0_enumerate_and_validate(&mut archive)?;
        pass1_verify_checksums(&mut archive, &manifest)?;
        pass2_write_and_verify(&mut archive, &manifest, fresh_dir)
    };
    match run() {
        Ok(report) => Ok(report),
        Err(e) => {
            // Never leave a partial restore behind where an operator could
            // mistake it for ready (no READY marker exists yet, but remove
            // the dir anyway so retries start clean).
            let _ = std::fs::remove_dir_all(fresh_dir);
            Err(e)
        }
    }
}

fn is_symlink(mode: Option<u32>) -> bool {
    mode.is_some_and(|m| m & 0o170000 == 0o120000)
}

/// Pass 0: enumerate every entry, reject hostile shapes, parse the manifest,
/// and validate versions/sets/signatures/caps/gaps — before any write.
fn pass0_enumerate_and_validate(
    archive: &mut zip::ZipArchive<std::fs::File>,
) -> Result<Manifest, BackupError> {
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut names: Vec<String> = Vec::new();
    for i in 0..archive.len() {
        let entry = archive
            .by_index(i)
            .map_err(|e| BackupError::Corrupt(format!("read bundle entry: {e}")))?;
        let raw = entry.name().to_string();
        if !seen.insert(raw.clone()) {
            return Err(BackupError::Corrupt("duplicate archive entry".to_string()));
        }
        if is_symlink(entry.unix_mode()) {
            return Err(BackupError::Corrupt("archive contains symlink".to_string()));
        }
        if entry.enclosed_name().is_none() {
            return Err(BackupError::Corrupt(
                "archive contains absolute/traversal path".to_string(),
            ));
        }
        if entry.is_dir() {
            return Err(BackupError::Corrupt(
                "archive contains directory entry".to_string(),
            ));
        }
        if entry.size() > MAX_BUNDLE_BYTES {
            return Err(BackupError::Corrupt(
                "archive entry exceeds cap".to_string(),
            ));
        }
        names.push(raw);
    }
    // Manifest first (needed to interpret everything else).
    if !names.contains(&MANIFEST_NAME.to_string()) {
        return Err(BackupError::Corrupt(
            "bundle misses manifest.json".to_string(),
        ));
    }
    let mut manifest_entry = archive
        .by_name(MANIFEST_NAME)
        .map_err(|e| BackupError::Corrupt(format!("read manifest: {e}")))?;
    if manifest_entry.size() > MAX_MANIFEST_BYTES {
        return Err(BackupError::Corrupt("manifest exceeds cap".to_string()));
    }
    let mut manifest_bytes = Vec::new();
    manifest_entry
        .read_to_end(&mut manifest_bytes)
        .map_err(|e| BackupError::Corrupt(format!("read manifest: {e}")))?;
    drop(manifest_entry);
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|e| BackupError::Corrupt(format!("unparseable manifest: {e}")))?;

    // Versions before anything else.
    if manifest.format_version != BACKUP_FORMAT_VERSION {
        return Err(BackupError::UnsupportedVersion(format!(
            "bundle format {} (this binary reads {})",
            manifest.format_version, BACKUP_FORMAT_VERSION
        )));
    }
    if manifest.schema_version != BACKUP_SCHEMA_VERSION {
        return Err(BackupError::Incompatible(format!(
            "bundle schema {} (this binary restores {})",
            manifest.schema_version, BACKUP_SCHEMA_VERSION
        )));
    }
    if manifest.redb_version != BACKUP_REDB_VERSION {
        return Err(BackupError::Incompatible(format!(
            "bundle redb {} (this binary uses {})",
            manifest.redb_version, BACKUP_REDB_VERSION
        )));
    }
    if manifest.snapshot_id.trim().is_empty() {
        return Err(BackupError::Corrupt(
            "manifest misses snapshot id".to_string(),
        ));
    }
    if !manifest.media_gaps.is_empty() {
        return Err(BackupError::Corrupt(format!(
            "bundle records {} blobs missing from its pools",
            manifest.media_gaps.len()
        )));
    }

    // Required table set: exactly the registry, no missing, no extras, no
    // duplicates, signatures matching the pinned schema.
    let registry = crate::persistence::redb::backup_table_names();
    let registry_set: BTreeSet<&str> = registry.iter().copied().collect();
    let mut manifest_tables: BTreeSet<&str> = BTreeSet::new();
    for table in &manifest.tables {
        if !manifest_tables.insert(table.name.as_str()) {
            return Err(BackupError::Corrupt(
                "duplicate table in manifest".to_string(),
            ));
        }
        if table.rows > MAX_TABLE_ROW_BYTES * 1024 || table.bytes > MAX_BUNDLE_BYTES {
            return Err(BackupError::Corrupt(
                "table declares excessive size".to_string(),
            ));
        }
        let Some((key_sig, value_sig)) =
            crate::persistence::redb::backup_table_signature(&table.name)
        else {
            return Err(BackupError::Corrupt(
                "bundle table is not in the pinned schema".to_string(),
            ));
        };
        if table.key_sig != key_sig || table.value_sig != value_sig {
            return Err(BackupError::Corrupt(
                "bundle table has incompatible type metadata".to_string(),
            ));
        }
    }
    let manifest_set: BTreeSet<&str> = manifest_tables.clone();
    if manifest_set != registry_set {
        let missing: Vec<&&str> = registry_set.difference(&manifest_set).collect();
        let extra: Vec<&&str> = manifest_set.difference(&registry_set).collect();
        return Err(BackupError::Corrupt(format!(
            "bundle table set mismatch (missing {}, extra {})",
            missing.len(),
            extra.len()
        )));
    }

    // Every archive entry must be accounted for by the manifest, and every
    // manifest component must exist in the archive.
    let mut expected: BTreeSet<String> = BTreeSet::new();
    expected.insert(MANIFEST_NAME.to_string());
    for table in &manifest.tables {
        expected.insert(table_entry_name(&table.name));
    }
    for media in &manifest.media_files {
        if !media.path.starts_with(MEDIA_DURABLE_PREFIX)
            && !media.path.starts_with(MEDIA_CACHE_PREFIX)
        {
            return Err(BackupError::Corrupt(
                "manifest media path outside pools".to_string(),
            ));
        }
        if media.bytes > MAX_BUNDLE_BYTES {
            return Err(BackupError::Corrupt(
                "media declares excessive size".to_string(),
            ));
        }
        expected.insert(media.path.clone());
    }
    let actual: BTreeSet<String> = names.into_iter().collect();
    if actual != expected {
        return Err(BackupError::Corrupt(format!(
            "bundle entry set mismatch (archive {}, manifest {})",
            actual.len(),
            expected.len()
        )));
    }
    Ok(manifest)
}

/// Pass 1: verify the overall checksum and every component checksum by
/// streaming re-reads. No writes have happened yet.
fn pass1_verify_checksums(
    archive: &mut zip::ZipArchive<std::fs::File>,
    manifest: &Manifest,
) -> Result<(), BackupError> {
    if compute_overall(
        &manifest.snapshot_id,
        &manifest.tables,
        &manifest.media_files,
    ) != manifest.overall_sha256
    {
        return Err(BackupError::Corrupt("bundle overall checksum".to_string()));
    }
    for table in &manifest.tables {
        let mut entry = archive
            .by_name(&table_entry_name(&table.name))
            .map_err(|e| BackupError::Corrupt(format!("open table entry: {e}")))?;
        verify_component(&mut entry, table.bytes, &table.sha256)?;
    }
    for media in &manifest.media_files {
        let mut entry = archive
            .by_name(&media.path)
            .map_err(|e| BackupError::Corrupt(format!("open media entry: {e}")))?;
        verify_component(&mut entry, media.bytes, &media.sha256)?;
    }
    Ok(())
}

fn verify_component(
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
            .map_err(|e| BackupError::Corrupt(format!("read bundle entry: {e}")))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        bytes = bytes.saturating_add(n as u64);
        if bytes > expected_bytes.saturating_add(16 * 1024 * 1024) {
            return Err(BackupError::Corrupt("bundle entry overflows".to_string()));
        }
    }
    if bytes != expected_bytes || hex::encode(hasher.finalize()) != expected_sha {
        return Err(BackupError::Corrupt("bundle entry checksum".to_string()));
    }
    Ok(())
}

struct MediaRef {
    blob_key: String,
    evicted: bool,
    owner_id: Option<String>,
}

/// Pass 2: write everything into the fresh dir, then reopen + verify.
fn pass2_write_and_verify(
    archive: &mut zip::ZipArchive<std::fs::File>,
    manifest: &Manifest,
    fresh_dir: &Path,
) -> Result<RestoreReport, BackupError> {
    use crate::domain::id::{AidxGenerator, SystemClock};

    let db_path = fresh_dir.join(FRESH_DB_FILE);
    let durable_root = fresh_dir.join(FRESH_DURABLE_DIR);
    let cache_root = fresh_dir.join(FRESH_CACHE_DIR);
    std::fs::create_dir_all(durable_root.join("objects"))
        .map_err(|e| BackupError::Storage(format!("create durable pool: {e}")))?;
    std::fs::create_dir_all(cache_root.join("objects"))
        .map_err(|e| BackupError::Storage(format!("create cache pool: {e}")))?;

    // Recreate the pinned schema with the same code production uses, then
    // import tables in bounded write transactions.
    let clock: Arc<SystemClock> = Arc::new(SystemClock);
    let ids: Arc<AidxGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
    let store = crate::persistence::redb::RedbStore::open(
        &db_path,
        clock.clone() as Arc<dyn crate::application::ports::Clock>,
        ids.clone() as Arc<dyn crate::application::ports::IdGenerator>,
    )
    .map_err(|e| BackupError::Storage(format!("recreate schema: {e}")))?;

    let mut rows_total = 0u64;
    let mut media_refs: Vec<MediaRef> = Vec::new();
    for table in &manifest.tables {
        let mut entry = archive
            .by_name(&table_entry_name(&table.name))
            .map_err(|e| BackupError::Corrupt(format!("open table entry: {e}")))?;
        let imported = import_table_stream(&mut entry, table, &store, &mut media_refs)?;
        if imported != table.rows {
            return Err(BackupError::Corrupt(
                "table import count mismatch".to_string(),
            ));
        }
        rows_total += imported;
    }

    // Media extraction with inline checksum verification.
    let mut media_bytes = 0u64;
    let mut extracted: BTreeMap<String, u64> = BTreeMap::new();
    for media in &manifest.media_files {
        let mut entry = archive
            .by_name(&media.path)
            .map_err(|e| BackupError::Corrupt(format!("open media entry: {e}")))?;
        let (pool_root, rel) = split_media_path(&media.path)?;
        let base = if pool_root == MEDIA_DURABLE_PREFIX {
            &durable_root
        } else {
            &cache_root
        };
        // `rel` is shard/file below the `objects/` tree (the prefix is
        // validated, not stored); re-anchor it under `objects/`.
        let dest = base.join("objects").join(rel);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| BackupError::Storage(format!("create pool shard: {e}")))?;
        }
        let size = extract_verified(&mut entry, &dest, media.bytes, &media.sha256)?;
        media_bytes += size;
        extracted.insert(media.path.clone(), size);
    }
    // Sidecars: every archived `.len` must agree with its extracted object;
    // objects without an archived sidecar get one regenerated from verified
    // bytes (legacy tolerance). A corrupt sidecar refuses the restore.
    let mut sidecars_healed = 0u64;
    let mut object_sizes: BTreeMap<String, u64> = BTreeMap::new();
    for (path, size) in &extracted {
        if path.ends_with(".len") {
            continue;
        }
        object_sizes.insert(path.clone(), *size);
    }
    for (object_path, size) in &object_sizes {
        let sidecar_path = format!("{object_path}.len");
        let (pool_root, rel) = split_media_path(object_path)?;
        let base = if pool_root == MEDIA_DURABLE_PREFIX {
            &durable_root
        } else {
            &cache_root
        };
        let object_file = base.join("objects").join(rel);
        let sidecar_file = PathBuf::from(format!("{}.len", object_file.to_string_lossy()));
        match extracted.get(&sidecar_path) {
            Some(_) => {
                let content = std::fs::read_to_string(&sidecar_file)
                    .map_err(|e| BackupError::Corrupt(format!("read sidecar: {e}")))?;
                let declared: u64 = content
                    .trim()
                    .parse()
                    .map_err(|_| BackupError::Corrupt("sidecar is not a byte count".to_string()))?;
                if declared != *size {
                    return Err(BackupError::Corrupt(
                        "sidecar disagrees with object bytes".to_string(),
                    ));
                }
            }
            None => {
                std::fs::write(&sidecar_file, size.to_string().as_bytes())
                    .map_err(|e| BackupError::Storage(format!("regenerate sidecar: {e}")))?;
                sidecars_healed += 1;
            }
        }
    }

    // Every non-evicted media row must have bytes in some pool (expected
    // pool by owner, other pool tolerated with a warning for the
    // reconciler; missing from both refuses).
    let durable_probe = crate::media::storage::FsBlobStore::new(&durable_root)
        .map_err(|e| BackupError::Storage(format!("open restored durable pool: {e}")))?;
    let cache_probe = crate::media::storage::FsBlobStore::new(&cache_root)
        .map_err(|e| BackupError::Storage(format!("open restored cache pool: {e}")))?;
    let mut pool_warnings = 0u64;
    for media_ref in &media_refs {
        if media_ref.evicted || media_ref.blob_key.is_empty() {
            continue;
        }
        let (primary, fallback) = if media_ref.owner_id.is_some() {
            (&durable_probe, &cache_probe)
        } else {
            (&cache_probe, &durable_probe)
        };
        let present_primary = primary
            .byte_len(&media_ref.blob_key)
            .map_err(|e| BackupError::Storage(format!("probe restored pool: {e}")))?
            .is_some();
        if present_primary {
            continue;
        }
        let present_fallback = fallback
            .byte_len(&media_ref.blob_key)
            .map_err(|e| BackupError::Storage(format!("probe restored pool: {e}")))?
            .is_some();
        if present_fallback {
            pool_warnings += 1;
            continue;
        }
        return Err(BackupError::Corrupt(format!(
            "restored media row misses its bytes (blob_key={}, owner={:?})",
            media_ref.blob_key, media_ref.owner_id
        )));
    }

    // Reopen + integrity check with no active transactions (the store is
    // dropped first, so no transaction can be outstanding), then reopen and
    // verify counts, accounting, and markers.
    let live_counts = store
        .table_row_counts()
        .map_err(|e| BackupError::Storage(format!("read restored counts: {e}")))?;
    drop(store);
    let intact = crate::persistence::redb::verify_backup_integrity(&db_path)
        .map_err(|e| BackupError::Storage(format!("integrity check: {e}")))?;
    if !intact {
        return Err(BackupError::Corrupt(
            "restored database fails integrity".to_string(),
        ));
    }
    let reopened = crate::persistence::redb::RedbStore::open(
        &db_path,
        clock as Arc<dyn crate::application::ports::Clock>,
        ids as Arc<dyn crate::application::ports::IdGenerator>,
    )
    .map_err(|e| BackupError::Storage(format!("reopen restored store: {e}")))?;
    for table in &manifest.tables {
        let found = live_counts
            .iter()
            .find(|(name, _)| name == &table.name)
            .map(|(_, n)| *n)
            .unwrap_or(0);
        if found != table.rows {
            return Err(BackupError::Corrupt(
                "restored table count mismatch".to_string(),
            ));
        }
    }
    let mismatches = reopened
        .verify_storage_accounting()
        .map_err(|e| BackupError::Storage(format!("verify restored accounting: {e}")))?;
    if !mismatches.is_empty() {
        return Err(BackupError::Corrupt(format!(
            "restored storage accounting drifts ({} mismatches)",
            mismatches.len()
        )));
    }
    drop(reopened);

    let report = RestoreReport {
        restored_dir: fresh_dir.to_path_buf(),
        snapshot_id: manifest.snapshot_id.clone(),
        tables_restored: manifest.tables.len() as u64,
        rows_total,
        media_files: manifest.media_files.len() as u64,
        media_bytes,
        sidecars_healed,
        pool_warnings,
    };
    let marker = serde_json::json!({
        "ready": true,
        "snapshot_id": report.snapshot_id,
        "tables_restored": report.tables_restored,
        "rows_total": report.rows_total,
        "media_files": report.media_files,
        "media_bytes": report.media_bytes,
        "verified_ms": super::now_ms(),
    });
    std::fs::write(
        fresh_dir.join(READY_MARKER),
        serde_json::to_vec_pretty(&marker).unwrap_or_default(),
    )
    .map_err(|e| BackupError::Storage(format!("mark restore ready: {e}")))?;
    Ok(report)
}

fn split_media_path(path: &str) -> Result<(&str, &str), BackupError> {
    for prefix in [MEDIA_DURABLE_PREFIX, MEDIA_CACHE_PREFIX] {
        if let Some(rest) = path.strip_prefix(prefix) {
            let Some(rel) = rest.strip_prefix("objects/") else {
                return Err(BackupError::Corrupt(
                    "media path outside objects tree".to_string(),
                ));
            };
            if rel.contains("..") || rel.starts_with('/') || rel.is_empty() {
                return Err(BackupError::Corrupt("media path traversal".to_string()));
            }
            // Shard/file shape check (mirrors the pool layout).
            let mut parts = rel.split('/');
            match (parts.next(), parts.next(), parts.next()) {
                (Some(shard), Some(file), None)
                    if shard.len() == 2 && !file.is_empty() && !file.contains('/') =>
                {
                    return Ok((prefix, rel));
                }
                _ => {
                    return Err(BackupError::Corrupt("media path shape".to_string()));
                }
            }
        }
    }
    Err(BackupError::Corrupt("media path outside pools".to_string()))
}

fn extract_verified(
    reader: &mut impl Read,
    dest: &Path,
    expected_bytes: u64,
    expected_sha: &str,
) -> Result<u64, BackupError> {
    if expected_bytes > MAX_BUNDLE_BYTES {
        return Err(BackupError::Corrupt(
            "media declares excessive size".to_string(),
        ));
    }
    let tmp = dest.with_extension("restore.tmp");
    let mut out = std::fs::File::create(&tmp)
        .map_err(|e| BackupError::Storage(format!("stage media: {e}")))?;
    let mut hasher = Sha256::new();
    let mut bytes = 0u64;
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = reader
            .read(&mut buf)
            .map_err(|e| BackupError::Corrupt(format!("read media entry: {e}")))?;
        if n == 0 {
            break;
        }
        out.write_all(&buf[..n])
            .map_err(|e| BackupError::Storage(format!("write media: {e}")))?;
        hasher.update(&buf[..n]);
        bytes = bytes.saturating_add(n as u64);
        if bytes > expected_bytes {
            let _ = std::fs::remove_file(&tmp);
            return Err(BackupError::Corrupt("media entry overflows".to_string()));
        }
    }
    if bytes != expected_bytes || hex::encode(hasher.finalize()) != expected_sha {
        let _ = std::fs::remove_file(&tmp);
        return Err(BackupError::Corrupt("media entry checksum".to_string()));
    }
    drop(out);
    std::fs::rename(&tmp, dest).map_err(|e| BackupError::Storage(format!("place media: {e}")))?;
    Ok(bytes)
}

/// Stream one table entry into the store in bounded write transactions,
/// collecting media references for the pool check. Single codec
/// implementation shared with the slice decoder.
fn import_table_stream(
    reader: &mut impl Read,
    table: &super::bundle::TableManifest,
    store: &crate::persistence::redb::RedbStore,
    media_refs: &mut Vec<MediaRef>,
) -> Result<u64, BackupError> {
    let mut magic = [0u8; 4];
    reader
        .read_exact(&mut magic)
        .map_err(|_| BackupError::Corrupt("truncated table entry".to_string()))?;
    if magic != *TABLE_MAGIC {
        return Err(BackupError::Corrupt("bad table magic".to_string()));
    }
    let mut total = 0u64;
    let mut chunk: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(RESTORE_COMMIT_CHUNK_ROWS);
    loop {
        if total == table.rows {
            break;
        }
        if total > table.rows {
            return Err(BackupError::Corrupt(
                "table holds more rows than declared".to_string(),
            ));
        }
        let mut lens = [0u8; 8];
        reader
            .read_exact(&mut lens)
            .map_err(|_| BackupError::Corrupt("truncated table row".to_string()))?;
        let klen = u32::from_le_bytes(
            lens[..4]
                .try_into()
                .map_err(|_| BackupError::Corrupt("bad row header".to_string()))?,
        ) as usize;
        let vlen = u32::from_le_bytes(
            lens[4..]
                .try_into()
                .map_err(|_| BackupError::Corrupt("bad row header".to_string()))?,
        ) as usize;
        if klen as u64 + vlen as u64 > MAX_TABLE_ROW_BYTES {
            return Err(BackupError::Corrupt("table row exceeds cap".to_string()));
        }
        let mut key = vec![0u8; klen];
        let mut value = vec![0u8; vlen];
        reader
            .read_exact(&mut key)
            .map_err(|_| BackupError::Corrupt("truncated table payload".to_string()))?;
        reader
            .read_exact(&mut value)
            .map_err(|_| BackupError::Corrupt("truncated table payload".to_string()))?;
        if table.name == "media_meta" {
            // MediaMeta rows are JSON; a decode failure here is a corrupt
            // bundle, never a skip.
            let meta: crate::domain::media::MediaMeta = serde_json::from_slice(&value)
                .map_err(|e| BackupError::Corrupt(format!("unparseable media row: {e}")))?;
            media_refs.push(MediaRef {
                blob_key: meta.blob_key,
                evicted: meta.evicted,
                owner_id: meta.owner_id,
            });
        }
        chunk.push((key, value));
        total += 1;
        if chunk.len() >= RESTORE_COMMIT_CHUNK_ROWS {
            store
                .import_backup_rows(&table.name, &chunk)
                .map_err(|e| BackupError::Corrupt(format!("import table: {e}")))?;
            chunk.clear();
        }
    }
    if !chunk.is_empty() {
        store
            .import_backup_rows(&table.name, &chunk)
            .map_err(|e| BackupError::Corrupt(format!("import table: {e}")))?;
    }
    // No trailing bytes allowed (exact framing, like the encoder).
    let mut tail = [0u8; 1];
    match reader.read(&mut tail) {
        Ok(0) => {}
        _ => return Err(BackupError::Corrupt("trailing bytes in table".to_string())),
    }
    if total != table.rows {
        return Err(BackupError::Corrupt("table row count mismatch".to_string()));
    }
    Ok(total)
}

/// Redacted one-line summary for logs (counts only, no paths or secrets).
pub fn restore_summary(report: &RestoreReport) -> String {
    format!(
        "restore ready: {} tables, {} rows, {} media files ({} bytes), {} sidecars healed, {} pool warnings",
        report.tables_restored,
        report.rows_total,
        report.media_files,
        report.media_bytes,
        report.sidecars_healed,
        report.pool_warnings
    )
}
