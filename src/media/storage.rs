use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::application::ports::{BlobStore, PortError};

#[derive(Debug)]
pub struct FsBlobStore {
    root: PathBuf,
    temp_seq: AtomicU64,
}

impl FsBlobStore {
    pub fn new(root: impl AsRef<Path>) -> Result<Self, PortError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("objects"))
            .map_err(|e| PortError::StorageUnavailable(format!("create blob objects dir: {e}")))?;
        fs::create_dir_all(root.join("tmp"))
            .map_err(|e| PortError::StorageUnavailable(format!("create blob tmp dir: {e}")))?;
        Ok(Self {
            root,
            temp_seq: AtomicU64::new(0),
        })
    }

    fn validate_hash(hash: &str) -> Result<(), PortError> {
        if hash.len() < 4 || hash.len() > 128 || !hash.bytes().all(|b| b.is_ascii_alphanumeric()) {
            return Err(PortError::Validation(format!("invalid blob hash: {hash}")));
        }
        Ok(())
    }

    pub fn object_path(&self, hash: &str) -> Result<PathBuf, PortError> {
        Self::validate_hash(hash)?;
        let prefix = &hash[0..2];
        Ok(self.root.join("objects").join(prefix).join(hash))
    }

    pub fn temp_path(&self, hash: &str) -> Result<PathBuf, PortError> {
        Self::validate_hash(hash)?;
        let seq = self.temp_seq.fetch_add(1, Ordering::SeqCst);
        let pid = std::process::id();
        Ok(self
            .root
            .join("tmp")
            .join(format!("{hash}.{pid}.{seq}.tmp")))
    }

    /// Length sidecar for an object: `{object_path}.len`, holding the
    /// decimal byte length (Phase 2.6 slice 8). Written via tmp+rename
    /// alongside the bytes; `get` treats a missing/mismatched sidecar as
    /// corrupt (`Ok(None)`) so a torn write can never serve short bytes.
    /// The `BlobStore` trait is unchanged — the sidecar is internal.
    fn sidecar_path(&self, hash: &str) -> Result<PathBuf, PortError> {
        let path = self.object_path(hash)?;
        let mut name = path
            .file_name()
            .map(|s| s.to_os_string())
            .ok_or_else(|| PortError::Validation(format!("invalid blob hash: {hash}")))?;
        name.push(".len");
        let mut sidecar = path;
        sidecar.set_file_name(name);
        Ok(sidecar)
    }

    fn write_atomic(&self, hash: &str, final_path: &Path, bytes: &[u8]) -> Result<(), PortError> {
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                PortError::StorageUnavailable(format!("create blob shard dir: {e}"))
            })?;
        }
        let tmp_path = self.temp_path(hash)?;
        {
            let mut file = fs::File::create(&tmp_path).map_err(|e| {
                PortError::StorageUnavailable(format!("create temp blob file: {e}"))
            })?;
            file.write_all(bytes).map_err(|e| {
                let _ = fs::remove_file(&tmp_path);
                PortError::StorageUnavailable(format!("write temp blob file: {e}"))
            })?;
            file.sync_all().map_err(|e| {
                let _ = fs::remove_file(&tmp_path);
                PortError::StorageUnavailable(format!("sync temp blob file: {e}"))
            })?;
        }
        fs::rename(&tmp_path, final_path).map_err(|e| {
            let _ = fs::remove_file(&tmp_path);
            PortError::StorageUnavailable(format!("rename temp blob to final path: {e}"))
        })?;
        Ok(())
    }

    /// Object-file presence without reading any content. `true` means a
    /// file exists at the content-addressed path; use [`Self::byte_len`]
    /// when the length sidecar must also agree (torn-write detection).
    pub fn exists(&self, hash: &str) -> Result<bool, PortError> {
        let final_path = self.object_path(hash)?;
        match fs::metadata(&final_path) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(PortError::StorageUnavailable(format!(
                "stat blob file: {e}"
            ))),
        }
    }

    /// Byte length without reading content: the object-file size must agree
    /// with the `.len` sidecar, otherwise the row is treated as corrupt
    /// (`Ok(None)`, same rule as `get`). `Ok(None)` also covers absent
    /// objects and pre-sidecar blobs.
    pub fn byte_len(&self, hash: &str) -> Result<Option<u64>, PortError> {
        let final_path = self.object_path(hash)?;
        let sidecar = self.sidecar_path(hash)?;
        let expected: u64 = match fs::read_to_string(&sidecar) {
            Ok(s) => match s.trim().parse() {
                Ok(n) => n,
                Err(_) => return Ok(None),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(PortError::StorageUnavailable(format!(
                    "read blob sidecar: {e}"
                )));
            }
        };
        match fs::metadata(&final_path) {
            Ok(m) => {
                let len = m.len();
                if len == expected {
                    Ok(Some(len))
                } else {
                    Ok(None)
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(PortError::StorageUnavailable(format!(
                "stat blob file: {e}"
            ))),
        }
    }

    /// Direct stat + write-sidecar heal for sidecar-only tears. `get` and
    /// `byte_len` both require the sidecar, so a present-but-unsidecarred
    /// object reads as `None` through the trait and the generic reconciler
    /// cannot regenerate the sidecar from `get` bytes. This path stats the
    /// object file and rewrites the `.len` sidecar without reading content.
    /// Returns `true` when a sidecar was (re)written, `false` when the row
    /// was already healthy or the object is absent. Best-effort: callers
    /// that only have `&dyn BlobStore` (ports frozen) cannot reach this and
    /// keep the documented limitation (see the reconciler scan heal).
    pub fn heal_sidecar(&self, hash: &str) -> Result<bool, PortError> {
        let _gate = acquire_media_op("heal_sidecar")?;
        if self.byte_len(hash)?.is_some() {
            return Ok(false);
        }
        let final_path = self.object_path(hash)?;
        let len = match fs::metadata(&final_path) {
            Ok(m) => m.len(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => {
                return Err(PortError::StorageUnavailable(format!(
                    "stat blob file: {e}"
                )));
            }
        };
        let sidecar = self.sidecar_path(hash)?;
        self.write_atomic(hash, &sidecar, len.to_string().as_bytes())?;
        Ok(true)
    }

    /// Efficient server-side import of a staged temp file (stream-to-disk
    /// target): rename-or-copy into the content-addressed path, verify the
    /// length, then write the `.len` sidecar. Mirrors the `put` skip: when
    /// identical bytes are already stored the staged file is just dropped.
    /// The staged file is consumed (renamed or removed) on success.
    pub fn import_file(&self, hash: &str, src: &Path) -> Result<(), PortError> {
        // Batch F backup gate: pool mutations wait behind an active backup
        // (see the gate region at the end of this file).
        let _gate = acquire_media_op("import_file")?;
        let src_len = fs::metadata(src)
            .map(|m| m.len())
            .map_err(|e| PortError::StorageUnavailable(format!("stat staged blob file: {e}")))?;
        let final_path = self.object_path(hash)?;
        let sidecar = self.sidecar_path(hash)?;
        // SHA-256 keys make the skip safe (same key ⇒ same bytes): skip
        // only when the object length and the sidecar both record exactly
        // this length, otherwise (re)write so a torn object heals.
        let object_len_ok = fs::metadata(&final_path).map(|m| m.len()).ok() == Some(src_len);
        let sidecar_ok = fs::read_to_string(&sidecar)
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            == Some(src_len);
        if object_len_ok && sidecar_ok {
            let _ = fs::remove_file(src);
            return Ok(());
        }
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                PortError::StorageUnavailable(format!("create blob shard dir: {e}"))
            })?;
        }
        if fs::rename(src, &final_path).is_err() {
            // Cross-device move (or Windows rename-onto-existing): fall
            // back to copy, removing a stale target first.
            if final_path.exists() {
                fs::remove_file(&final_path).map_err(|e| {
                    PortError::StorageUnavailable(format!("replace stale blob file: {e}"))
                })?;
            }
            if fs::rename(src, &final_path).is_err() {
                fs::copy(src, &final_path).map_err(|e| {
                    PortError::StorageUnavailable(format!("copy staged blob file: {e}"))
                })?;
                let _ = fs::remove_file(src);
            }
        }
        let actual = fs::metadata(&final_path)
            .map(|m| m.len())
            .map_err(|e| PortError::StorageUnavailable(format!("stat imported blob file: {e}")))?;
        if actual != src_len {
            return Err(PortError::StorageUnavailable(format!(
                "blob import length mismatch: staged {src_len}, stored {actual}"
            )));
        }
        self.write_atomic(hash, &sidecar, src_len.to_string().as_bytes())?;
        Ok(())
    }
}

impl BlobStore for FsBlobStore {
    fn put(&self, hash: &str, bytes: &[u8]) -> Result<(), PortError> {
        // Batch F backup gate: pool mutations wait behind an active backup.
        let _gate = acquire_media_op("put")?;
        let final_path = self.object_path(hash)?;
        let sidecar = self.sidecar_path(hash)?;
        // SHA-256 keys make the skip safe (same key ⇒ same bytes): skip only
        // when the object length (via stat, no content read) and the sidecar
        // both record exactly this length, otherwise (re)write both files so
        // a torn object or a missing/stale sidecar heals instead of lingering.
        let object_len_ok =
            fs::metadata(&final_path).map(|m| m.len()).ok() == Some(bytes.len() as u64);
        let sidecar_ok = fs::read_to_string(&sidecar)
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            == Some(bytes.len() as u64);
        if object_len_ok && sidecar_ok {
            return Ok(());
        }
        self.write_atomic(hash, &final_path, bytes)?;
        self.write_atomic(hash, &sidecar, bytes.len().to_string().as_bytes())?;
        Ok(())
    }

    fn get(&self, hash: &str) -> Result<Option<Vec<u8>>, PortError> {
        let final_path = self.object_path(hash)?;
        let sidecar = self.sidecar_path(hash)?;
        let expected: u64 = match fs::read_to_string(&sidecar) {
            // Missing or unparsable sidecar (torn `put`, or a pre-slice-8
            // MD5-keyed blob) is corruption: `None`, never raw bytes.
            // Callers already map `None` to 500/404.
            Ok(s) => match s.trim().parse() {
                Ok(n) => n,
                Err(_) => return Ok(None),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(PortError::StorageUnavailable(format!(
                    "read blob sidecar: {e}"
                )));
            }
        };
        match fs::read(&final_path) {
            Ok(bytes) => {
                if bytes.len() as u64 != expected {
                    return Ok(None);
                }
                Ok(Some(bytes))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(PortError::StorageUnavailable(format!(
                "read blob file: {e}"
            ))),
        }
    }

    fn delete(&self, hash: &str) -> Result<(), PortError> {
        // Batch F backup gate: pool deletions wait behind an active backup
        // so a snapshot can never reference bytes that vanish mid-copy.
        let _gate = acquire_media_op("delete")?;
        let final_path = self.object_path(hash)?;
        match fs::remove_file(&final_path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => {
                return Err(PortError::StorageUnavailable(format!(
                    "delete blob file: {e}"
                )));
            }
        }?;
        let sidecar = self.sidecar_path(hash)?;
        match fs::remove_file(&sidecar) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(PortError::StorageUnavailable(format!(
                "delete blob sidecar: {e}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_store_put_get_delete_and_partial_write_invisible() {
        let dir = std::env::temp_dir().join(format!("pinetree-blob-test-{}", uuid::Uuid::new_v4()));
        let store = FsBlobStore::new(&dir).unwrap();
        let hash = "d41d8cd98f00b204e9800998ecf8427e";
        let payload = b"hello content-addressed world";

        assert_eq!(store.get(hash).unwrap(), None);

        // Simulate an un-renamed partial temp write in tmp/
        let orphan_tmp = store.temp_path(hash).unwrap();
        std::fs::write(&orphan_tmp, b"partial-corrupt").unwrap();
        assert_eq!(
            store.get(hash).unwrap(),
            None,
            "partial temp write must never be visible via get()"
        );

        store.put(hash, payload).unwrap();
        assert_eq!(store.get(hash).unwrap().as_deref(), Some(&payload[..]));

        // Idempotent second put
        store.put(hash, payload).unwrap();
        assert_eq!(store.get(hash).unwrap().as_deref(), Some(&payload[..]));

        store.delete(hash).unwrap();
        assert_eq!(store.get(hash).unwrap(), None);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn blob_store_get_verifies_length_sidecar() {
        // Slice 8 (D6, written before the fix): `get` must never serve
        // short bytes from a torn write. A length mismatch — or a
        // missing/corrupt sidecar — reads as `None`, never as truncated
        // content (callers map `None` to 500/404; the bytes are re-fetched
        // or re-uploaded).
        let dir = std::env::temp_dir().join(format!("pinetree-blob-test-{}", uuid::Uuid::new_v4()));
        let store = FsBlobStore::new(&dir).unwrap();
        let hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let payload = b"length-checked blob bytes";

        store.put(hash, payload).unwrap();
        assert_eq!(store.get(hash).unwrap().as_deref(), Some(&payload[..]));

        // Truncate the object file in place: must not serve short bytes.
        let object = store.object_path(hash).unwrap();
        std::fs::write(&object, &payload[..4]).unwrap();
        assert_eq!(
            store.get(hash).unwrap(),
            None,
            "truncated blob must read as corrupt, not short"
        );

        // A fresh put heals the torn write.
        store.put(hash, payload).unwrap();
        assert_eq!(store.get(hash).unwrap().as_deref(), Some(&payload[..]));

        // A corrupt sidecar is corruption too, not a fallback to raw bytes.
        let sidecar = store.sidecar_path(hash).unwrap();
        std::fs::write(&sidecar, b"not-a-number").unwrap();
        assert_eq!(store.get(hash).unwrap(), None);

        // A missing sidecar (e.g. a pre-slice-8 blob) is also `None`.
        std::fs::remove_file(&sidecar).unwrap();
        assert_eq!(store.get(hash).unwrap(), None);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn blob_store_rejects_traversal_hashes() {
        let dir = std::env::temp_dir().join(format!("pinetree-blob-test-{}", uuid::Uuid::new_v4()));
        let store = FsBlobStore::new(&dir).unwrap();
        assert!(store.put("../etc/passwd", b"x").is_err());
        assert!(store.put("ab", b"x").is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn test_store(tag: &str) -> (FsBlobStore, PathBuf) {
        let dir =
            std::env::temp_dir().join(format!("pinetree-blob-test-{tag}-{}", uuid::Uuid::new_v4()));
        let store = FsBlobStore::new(&dir).unwrap();
        (store, dir)
    }

    #[test]
    fn blob_store_exists_and_byte_len_without_reading() {
        let (store, dir) = test_store("exists-len");
        let hash = "d41d8cd98f00b204e9800998ecf8427e";
        assert_eq!(store.exists(hash).unwrap(), false);
        assert_eq!(store.byte_len(hash).unwrap(), None);

        let payload = b"length-checked blob bytes";
        store.put(hash, payload).unwrap();
        assert_eq!(store.exists(hash).unwrap(), true);
        assert_eq!(store.byte_len(hash).unwrap(), Some(payload.len() as u64));

        // Torn object: length mismatch reads as corrupt, never short.
        let object = store.object_path(hash).unwrap();
        std::fs::write(&object, &payload[..4]).unwrap();
        assert_eq!(store.byte_len(hash).unwrap(), None);

        // Missing sidecar (pre-sidecar blob) is also `None`, but the file
        // still exists.
        store.put(hash, payload).unwrap();
        std::fs::remove_file(store.sidecar_path(hash).unwrap()).unwrap();
        assert_eq!(store.byte_len(hash).unwrap(), None);
        assert_eq!(store.exists(hash).unwrap(), true);

        // Invalid hashes are rejected, not probed on disk.
        assert!(store.exists("../etc/passwd").is_err());
        assert!(store.byte_len("ab").is_err());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn blob_store_import_file_moves_staged_bytes() {
        let (store, dir) = test_store("import");
        let hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let payload = b"staged stream-to-disk bytes";

        let staged = dir.join("staged.tmp");
        std::fs::write(&staged, payload).unwrap();
        store.import_file(hash, &staged).unwrap();
        // Staged file consumed (same-device rename), bytes addressable.
        assert!(!staged.exists());
        assert_eq!(store.get(hash).unwrap().as_deref(), Some(&payload[..]));
        assert_eq!(store.byte_len(hash).unwrap(), Some(payload.len() as u64));

        // Idempotent second import of identical bytes: staged file dropped,
        // stored bytes untouched.
        std::fs::write(&staged, payload).unwrap();
        store.import_file(hash, &staged).unwrap();
        assert!(!staged.exists());
        assert_eq!(store.get(hash).unwrap().as_deref(), Some(&payload[..]));

        // Import heals a torn object instead of leaving short bytes.
        let object = store.object_path(hash).unwrap();
        std::fs::write(&object, &payload[..4]).unwrap();
        assert_eq!(store.get(hash).unwrap(), None);
        std::fs::write(&staged, payload).unwrap();
        store.import_file(hash, &staged).unwrap();
        assert_eq!(store.get(hash).unwrap().as_deref(), Some(&payload[..]));

        // Missing staged file and bad hashes surface errors.
        assert!(store.import_file(hash, &dir.join("nope.tmp")).is_err());
        std::fs::write(&staged, payload).unwrap();
        assert!(store.import_file("../etc/passwd", &staged).is_err());

        let _ = std::fs::remove_dir_all(&dir);
    }
}

// ===== Batch F media-storage lifecycle gate (append-only region) ==========
// One shared gate for BOTH pools (durable + cache). The online backup holds
// the exclusive (write) side for the whole snapshot+copy; every pool-byte
// mutation path — `put`, `import_file`, `delete` (hence local upload,
// transcode import, remote fetch, cache eviction via the `BlobStore` trait,
// reconciler trait-path cleanup, and account/media deletion) — holds the
// shared (read) side briefly and may wait behind an active backup. Reads
// (`get`/`exists`/`byte_len`) and all non-media requests never touch the
// gate and keep serving.
//
// Lock order (documented, no inversions): gate → db-txn → fs. Mutation
// paths never acquire the gate while holding a DB transaction (bytes land
// before the metadata txn on write, after the metadata commit on delete),
// and the backup never calls a mutating pool op while holding the exclusive
// side (it copies via plain fs reads). Batch C's at-open backfill runs
// inside `RedbStore::open` with write txns only — no gate, no pool
// mutation — so it composes trivially (no backup exists before `open`).
//
// Orphan sweep (`reconciler::sweep_orphan_blobs_checked`) holds
// `acquire_media_op("sweep")` for the whole walk, so filesystem deletes
// wait behind an active backup like every other pool mutation (the sweep
// uses direct `remove_file` on orphan keys + sidecars — key-shape-gated —
// rather than `FsBlobStore::delete`, but under the same shared hold).

// Batch F single-backup invariant: the exclusive gate below enforces
// `MAX_CONCURRENT_BACKUPS == 1` (a second backup fails fast with
// `BackupBusy` via `WouldBlock`, it never queues behind the lock).
// Compile-time pin so a future const bump fails the build until the gate
// is reworked.
const _: () = assert!(crate::backup::MAX_CONCURRENT_BACKUPS == 1);

use std::sync::atomic::AtomicUsize;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError};

fn gate_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

struct MediaGateState {
    active: bool,
    started_ms: u64,
}

fn media_gate() -> &'static RwLock<MediaGateState> {
    static GATE: std::sync::OnceLock<RwLock<MediaGateState>> = std::sync::OnceLock::new();
    GATE.get_or_init(|| {
        RwLock::new(MediaGateState {
            active: false,
            started_ms: 0,
        })
    })
}

/// Active backup id, mirrored outside the `RwLock` so a rejected second
/// backup can name the holder without blocking on the write guard.
/// Lock order: write-guard → id-mutex; readers take the id-mutex alone.
fn active_backup_id_cell() -> &'static std::sync::Mutex<String> {
    static CELL: std::sync::OnceLock<std::sync::Mutex<String>> = std::sync::OnceLock::new();
    CELL.get_or_init(|| std::sync::Mutex::new(String::new()))
}

fn read_active_backup_id() -> String {
    active_backup_id_cell()
        .try_lock()
        .map(|id| id.clone())
        .unwrap_or_default()
}

static MEDIA_GATE_WAITERS: AtomicUsize = AtomicUsize::new(0);
static MEDIA_GATE_WAITS_TOTAL: AtomicU64 = AtomicU64::new(0);
static MEDIA_GATE_WAIT_MS_TOTAL: AtomicU64 = AtomicU64::new(0);
static MEDIA_GATE_OPS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// RAII shared hold for one pool mutation. Drop releases the gate.
pub struct MediaOpGuard {
    _guard: RwLockReadGuard<'static, MediaGateState>,
}

impl std::fmt::Debug for MediaOpGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MediaOpGuard(..)")
    }
}

/// Acquire the shared side for a pool mutation (`put`/`import_file`/
/// `delete`). Blocks while a backup holds the exclusive side; the wait is
/// counted and timed for diagnostics (see [`media_gate_snapshot`]).
/// A poisoned lock is recovered (never a panic): poison only means a
/// previous holder panicked, and this leaf gate carries no data that a
/// panic could leave inconsistent.
pub fn acquire_media_op(_op: &'static str) -> Result<MediaOpGuard, PortError> {
    MEDIA_GATE_WAITERS.fetch_add(1, Ordering::SeqCst);
    let started = std::time::Instant::now();
    let guard = media_gate()
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let waited_ms = started.elapsed().as_millis() as u64;
    MEDIA_GATE_WAITERS.fetch_sub(1, Ordering::SeqCst);
    MEDIA_GATE_WAITS_TOTAL.fetch_add(1, Ordering::SeqCst);
    MEDIA_GATE_WAIT_MS_TOTAL.fetch_add(waited_ms, Ordering::SeqCst);
    MEDIA_GATE_OPS_TOTAL.fetch_add(1, Ordering::SeqCst);
    Ok(MediaOpGuard { _guard: guard })
}

/// RAII exclusive hold for one backup run. Drop releases the gate and
/// clears the holder flag.
pub struct BackupGateGuard {
    _guard: RwLockWriteGuard<'static, MediaGateState>,
}

impl std::fmt::Debug for BackupGateGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BackupGateGuard(..)")
    }
}

/// The backup that currently holds the gate.
#[derive(Debug)]
pub struct BackupBusy {
    pub active_backup_id: String,
}

/// Acquire the exclusive side for `backup_id`. At most one backup runs at a
/// time (`MAX_CONCURRENT_BACKUPS == 1`); a second request gets [`BackupBusy`]
/// and must surface "already running" (or retry later) — never block behind
/// the lock, so admin status stays truthful about what is actually running.
/// A poisoned lock is recovered; a stale `active` flag observed under an
/// exclusively held guard is repaired (exclusive ownership proves no live
/// holder exists).
///
/// Momentary contention with in-flight media ops (shared holders) is NOT a
/// second backup: acquisition waits for readers to drain (bounded), failing
/// fast with [`BackupBusy`] only when a live backup holds the write side or
/// the drain times out.
pub fn acquire_backup_gate(backup_id: &str) -> Result<BackupGateGuard, BackupBusy> {
    /// Upper bound for waiting out in-flight media ops at backup start.
    const BACKUP_GATE_DRAIN_TIMEOUT_MS: u64 = 30_000;
    /// Poll interval while draining readers.
    const BACKUP_GATE_DRAIN_POLL_MS: u64 = 5;
    debug_assert_eq!(
        crate::backup::MAX_CONCURRENT_BACKUPS,
        1,
        "exclusive media gate enforces a single concurrent backup"
    );
    let deadline = gate_now_ms().saturating_add(BACKUP_GATE_DRAIN_TIMEOUT_MS);
    loop {
        let mut guard = match media_gate().try_write() {
            Ok(guard) => guard,
            Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(TryLockError::WouldBlock) => {
                // Write side is held. If a *reader* can still be acquired,
                // no writer exists, so this is only in-flight media ops:
                // wait for them to drain. If even a read lock is refused, a
                // live backup holds the write side: fail fast.
                let writer_held = media_gate().try_read().is_err();
                if writer_held {
                    return Err(BackupBusy {
                        active_backup_id: read_active_backup_id(),
                    });
                }
                if gate_now_ms() >= deadline {
                    return Err(BackupBusy {
                        active_backup_id: read_active_backup_id(),
                    });
                }
                std::thread::sleep(std::time::Duration::from_millis(BACKUP_GATE_DRAIN_POLL_MS));
                continue;
            }
        };
        guard.active = true;
        guard.started_ms = gate_now_ms();
        if let Ok(mut cell) = active_backup_id_cell().try_lock() {
            *cell = backup_id.to_string();
        }
        return Ok(BackupGateGuard { _guard: guard });
    }
}

impl Drop for BackupGateGuard {
    fn drop(&mut self) {
        // The write guard releases right after this runs; clear the flag
        // through the guard we already hold (no re-acquisition, so no
        // recursion and no inversion).
        self._guard.active = false;
        self._guard.started_ms = 0;
        if let Ok(mut cell) = active_backup_id_cell().try_lock() {
            cell.clear();
        }
    }
}

/// Point-in-time gate diagnostics for status surfaces (counts only).
#[derive(Debug, Clone)]
pub struct MediaGateSnapshot {
    pub backup_active: bool,
    pub active_backup_id: String,
    pub backup_started_ms: u64,
    pub waiters: usize,
    pub ops_total: u64,
    pub waits_total: u64,
    pub wait_ms_total: u64,
}

pub fn media_gate_snapshot() -> MediaGateSnapshot {
    let (active, started_ms) = match media_gate().try_read() {
        Ok(state) => (state.active, state.started_ms),
        Err(TryLockError::Poisoned(poisoned)) => {
            let state = poisoned.into_inner();
            (state.active, state.started_ms)
        }
        // A backup holds the exclusive side right now: report active without
        // blocking (the id cell is lock-free for readers).
        Err(TryLockError::WouldBlock) => (true, 0),
    };
    MediaGateSnapshot {
        backup_active: active,
        active_backup_id: read_active_backup_id(),
        backup_started_ms: started_ms,
        waiters: MEDIA_GATE_WAITERS.load(Ordering::SeqCst),
        ops_total: MEDIA_GATE_OPS_TOTAL.load(Ordering::SeqCst),
        waits_total: MEDIA_GATE_WAITS_TOTAL.load(Ordering::SeqCst),
        wait_ms_total: MEDIA_GATE_WAIT_MS_TOTAL.load(Ordering::SeqCst),
    }
}
