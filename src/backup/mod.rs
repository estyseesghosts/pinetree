//! Batch F (`docs/prod-ready.md` §7): live database + media backup and restore.
//!
//! The live process owns the redb handle (a second CLI process generally
//! cannot open the same file), so backup is an in-process operation:
//!
//! * [`request_backup`] records a durable queued status and returns a job id
//!   immediately (accepted-fast).
//! * [`run_next_backup`] drains the oldest queued request (the worker drain
//!   arm in `crate::jobs::worker` calls this); [`run_backup_blocking`]
//!   runs one request synchronously.
//! * [`backup_status`] reports queued/running/completed/failed with times,
//!   output id, bytes, gate-wait time, and a redacted failure reason.
//! * [`cleanup_abandoned_partials`] removes `.partial-*` staging files on
//!   startup and fails stale `running` records (wire it from `main.rs`).
//! * [`restore_bundle`] (offline) validates a bundle and restores it into a
//!   fresh directory; it never hot-swaps the live database.
//!
//! Consistency model: one shared media-storage lifecycle gate (see
//! `crate::media::storage`) is held for the whole bundle creation while a
//! single redb read transaction streams every table (see
//! `RedbStore::export_backup_snapshot`). Non-media requests keep serving;
//! pool mutations (put/import/delete, hence upload/eviction/reconciler and
//! fetch paths) wait behind the gate. Lock order is always
//! gate → db-txn → fs; media paths never acquire the gate while holding a
//! DB transaction (bytes are written before the metadata txn on `put`, and
//! after the metadata commit on `delete`), so no lock-order inversion with
//! Batch C's at-open backfill (which runs inside `RedbStore::open` with
//! write txns only, no gate, no pool mutation — backup cannot exist before
//! `open` returns a handle).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use serde::{Deserialize, Serialize};

use crate::application::ports::PortError;
use crate::persistence::redb::RedbStore;

pub mod bundle;
pub mod restore;

pub use bundle::{
    BACKUP_FORMAT_VERSION, BACKUP_REDB_VERSION, BACKUP_SCHEMA_VERSION, BackupPolicy,
    HARD_BUNDLE_CEILING_BYTES, MAX_BUNDLE_BYTES, create_bundle_with_policy, policy_for_context,
};
pub use restore::{RestoreReport, restore_bundle, restore_bundle_with_policy};

/// Default backup directory name under the data dir (a const default — no
/// config-schema change in this batch; see the parent snippets for the
/// config/route/CLI wiring that passes an explicit dir instead).
pub const DEFAULT_BACKUP_DIR_NAME: &str = "backups";
/// Staging prefix for incomplete bundles. A failed/incomplete backup never
/// carries the final name.
pub const BACKUP_PARTIAL_PREFIX: &str = ".partial-";
/// Final bundle suffix.
pub const BACKUP_FILE_SUFFIX: &str = ".zip";
/// Durable per-job status suffix.
pub const BACKUP_STATUS_SUFFIX: &str = ".status.json";
/// Normally exactly one concurrent backup (exclusive gate).
pub const MAX_CONCURRENT_BACKUPS: usize = 1;
/// Bounded retries for a transient backup failure before it is failed.
pub const MAX_BACKUP_ATTEMPTS: u32 = 5;

/// Hard bound on in-memory status records. The durable status files in
/// `backup_dir` are the source of truth; the registry is only a fast path.
/// Terminal records (completed/failed) beyond this cap are dropped oldest
/// first — queued/running records are never evicted, so a live job is
/// always queryable. `list_backup_statuses` re-reads from disk and is
/// unaffected by the bound.
pub const MAX_STATUS_REGISTRY_ENTRIES: usize = 1024;
/// Chunk size for restore write transactions (bounds one txn).
pub const RESTORE_COMMIT_CHUNK_ROWS: usize = 500;

/// What the backup needs. Built by the caller (admin route / CLI / tests)
/// from the live store and pool roots; outputs stay confined to
/// `backup_dir`. `max_bundle_bytes` carries the configured
/// `backup.max_bundle_bytes` cap (`None` = hard ceiling); the retention
/// fields carry the configured `backup.retention_*` bounds (`None` = keep).
#[derive(Clone)]
pub struct BackupContext {
    pub store: Arc<RedbStore>,
    pub durable_pool: PathBuf,
    pub cache_pool: PathBuf,
    pub backup_dir: PathBuf,
    pub max_bundle_bytes: Option<u64>,
    pub retention_max_count: Option<usize>,
    pub retention_max_bytes: Option<u64>,
}

/// Backup lifecycle state (serialized into the durable status file).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackupState {
    Queued,
    Running,
    Completed,
    Failed,
}

/// Durable + in-memory status record for one backup job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupStatus {
    pub job_id: String,
    pub state: BackupState,
    pub requested_ms: u64,
    pub started_ms: Option<u64>,
    pub finished_ms: Option<u64>,
    pub attempts: u32,
    pub output_file: Option<String>,
    pub bytes_written: Option<u64>,
    pub tables_exported: u64,
    pub media_files: u64,
    /// Time media ops spent waiting behind the backup gate during this run
    /// (delta of the gate's wait counter), for diagnostics.
    pub gate_wait_ms: u64,
    /// Redacted failure reason (no secrets; see [`redact_failure`]).
    pub failure_redacted: Option<String>,
}

/// Backup/restore failure. Never carries key material, tokens, or passwords:
/// messages name components and byte counts only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackupError {
    /// Another backup holds the exclusive gate.
    Busy(String),
    /// Caller error (bad dir, bad job id, traversal output).
    Invalid(String),
    /// Storage I/O failure; retryable.
    Storage(String),
    /// Transient failure (e.g. gate busy at drain time); retryable.
    Transient(String),
    /// Bundle fails validation (bad checksum, missing component, traversal
    /// entry, oversize, bad type). Fail closed.
    Corrupt(String),
    /// Bundle version/schema the running binary cannot restore. Fail closed.
    UnsupportedVersion(String),
    /// Compatible bundle but wrong schema generation. Fail closed.
    Incompatible(String),
}

impl std::fmt::Display for BackupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackupError::Busy(m)
            | BackupError::Invalid(m)
            | BackupError::Storage(m)
            | BackupError::Transient(m)
            | BackupError::Corrupt(m)
            | BackupError::UnsupportedVersion(m)
            | BackupError::Incompatible(m) => write!(f, "{m}"),
        }
    }
}

impl BackupError {
    /// Map onto the worker's error type: transient/storage faults retry
    /// with backoff; validation-class failures quarantine.
    pub fn to_port_error(&self) -> PortError {
        match self {
            BackupError::Transient(m) | BackupError::Storage(m) | BackupError::Busy(m) => {
                PortError::StorageUnavailable(format!("backup: {m}"))
            }
            BackupError::Invalid(m)
            | BackupError::Corrupt(m)
            | BackupError::UnsupportedVersion(m)
            | BackupError::Incompatible(m) => PortError::Validation(format!("backup: {m}")),
        }
    }
}

/// Strip anything secret-looking and truncate: status records and logs must
/// never expose key material even if an inner error echoes input.
pub fn redact_failure(message: &str) -> String {
    let mut out = message.to_string();
    for key in [
        "private_key",
        "private-key",
        "password",
        "token",
        "secret",
        "credential",
    ] {
        let mut start = 0;
        while let Some(pos) = out[start..].find(key) {
            let from = start + pos;
            let to = (from + key.len() + 64).min(out.len());
            out.replace_range(from..to, "[redacted]");
            start = from + "[redacted]".len();
        }
    }
    out.chars().take(500).collect::<String>()
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

static JOB_SEQ: AtomicU64 = AtomicU64::new(0);

fn new_job_id() -> String {
    let seq = JOB_SEQ.fetch_add(1, Ordering::SeqCst);
    format!("bk-{}-{}-{seq}", now_ms(), std::process::id())
}

fn status_registry() -> &'static Mutex<HashMap<String, BackupStatus>> {
    static REGISTRY: std::sync::OnceLock<Mutex<HashMap<String, BackupStatus>>> =
        std::sync::OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn runner_lock() -> &'static Mutex<()> {
    static RUNNER: std::sync::OnceLock<Mutex<()>> = std::sync::OnceLock::new();
    RUNNER.get_or_init(|| Mutex::new(()))
}

fn lock_registry() -> MutexGuard<'static, HashMap<String, BackupStatus>> {
    status_registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn status_path(backup_dir: &Path, job_id: &str) -> PathBuf {
    backup_dir.join(format!("{job_id}{BACKUP_STATUS_SUFFIX}"))
}

fn validate_job_id(job_id: &str) -> Result<(), BackupError> {
    if job_id.len() < 4
        || job_id.len() > 128
        || !job_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(BackupError::Invalid("bad backup job id".to_string()));
    }
    Ok(())
}

fn persist_status(backup_dir: &Path, status: &BackupStatus) -> Result<(), BackupError> {
    let bytes = serde_json::to_vec_pretty(status)
        .map_err(|e| BackupError::Storage(format!("encode backup status: {e}")))?;
    let path = status_path(backup_dir, &status.job_id);
    let tmp = path.with_extension("status.json.tmp");
    std::fs::write(&tmp, &bytes)
        .map_err(|e| BackupError::Storage(format!("write backup status: {e}")))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600));
    }
    std::fs::rename(&tmp, &path)
        .map_err(|e| BackupError::Storage(format!("publish backup status: {e}")))?;
    Ok(())
}

fn load_status(backup_dir: &Path, job_id: &str) -> Result<Option<BackupStatus>, BackupError> {
    validate_job_id(job_id)?;
    let path = status_path(backup_dir, job_id);
    match std::fs::read(&path) {
        Ok(bytes) => {
            let status: BackupStatus = serde_json::from_slice(&bytes).map_err(|e| {
                BackupError::Corrupt(format!("unparseable backup status file: {e}"))
            })?;
            Ok(Some(status))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(BackupError::Storage(format!("read backup status: {e}"))),
    }
}

fn store_status(status: &BackupStatus) {
    let mut registry = lock_registry();
    registry.insert(status.job_id.clone(), status.clone());
    if registry.len() > MAX_STATUS_REGISTRY_ENTRIES {
        // Drop oldest terminal records first; queued/running are never
        // evicted so live jobs stay queryable. The durable files remain
        // the source of truth (`backup_status` re-loads on miss).
        let excess = registry.len() - MAX_STATUS_REGISTRY_ENTRIES;
        let mut terminal: Vec<(u64, String)> = registry
            .values()
            .filter(|s| matches!(s.state, BackupState::Completed | BackupState::Failed))
            .map(|s| (s.requested_ms, s.job_id.clone()))
            .collect();
        terminal.sort();
        for (_, job_id) in terminal.into_iter().take(excess) {
            registry.remove(&job_id);
        }
    }
}

/// Accepted-fast trigger: records a durable queued status and returns the
/// job id. The bundle is built by [`run_next_backup`] (worker drain) or
/// [`run_backup_blocking`]. Outputs stay inside `ctx.backup_dir`; the job
/// id charset is filename-safe so no traversal is possible.
pub fn request_backup(ctx: &BackupContext) -> Result<String, BackupError> {
    std::fs::create_dir_all(&ctx.backup_dir)
        .map_err(|e| BackupError::Storage(format!("create backup dir: {e}")))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&ctx.backup_dir, std::fs::Permissions::from_mode(0o700));
    }
    if !ctx.durable_pool.is_dir() {
        return Err(BackupError::Invalid("durable pool dir missing".to_string()));
    }
    if !ctx.cache_pool.is_dir() {
        return Err(BackupError::Invalid("cache pool dir missing".to_string()));
    }
    let job_id = new_job_id();
    let status = BackupStatus {
        job_id: job_id.clone(),
        state: BackupState::Queued,
        requested_ms: now_ms(),
        started_ms: None,
        finished_ms: None,
        attempts: 0,
        output_file: None,
        bytes_written: None,
        tables_exported: 0,
        media_files: 0,
        gate_wait_ms: 0,
        failure_redacted: None,
    };
    persist_status(&ctx.backup_dir, &status)?;
    store_status(&status);
    Ok(job_id)
}

/// Status view: queued/running/completed/failed with times, output id,
/// bytes, gate-wait time, and a redacted failure. Falls back to the durable
/// status file when the record is not in memory (e.g. after a restart).
pub fn backup_status(ctx: &BackupContext, job_id: &str) -> Option<BackupStatus> {
    if let Some(status) = lock_registry().get(job_id).cloned() {
        return Some(status);
    }
    match load_status(&ctx.backup_dir, job_id) {
        Ok(Some(status)) => {
            store_status(&status);
            Some(status)
        }
        _ => None,
    }
}

/// Every known status in `backup_dir` (for the admin status surface).
pub fn list_backup_statuses(backup_dir: &Path) -> Vec<BackupStatus> {
    let mut out = Vec::new();
    let entries =
        std::fs::read_dir(backup_dir).map(|r| r.filter_map(|e| e.ok()).collect::<Vec<_>>());
    let entries = match entries {
        Ok(entries) => entries,
        Err(_) => return out,
    };
    for entry in entries {
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(job_id) = name.strip_suffix(BACKUP_STATUS_SUFFIX) else {
            continue;
        };
        if validate_job_id(job_id).is_err() {
            continue;
        }
        if let Ok(Some(status)) = load_status(backup_dir, job_id) {
            out.push(status);
        }
    }
    out.sort_by_key(|s| s.requested_ms);
    out
}

/// Retention pruning outcome (best-effort counts for logs/diagnostics).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetentionReport {
    pub bundles_removed: u64,
    pub bytes_reclaimed: u64,
}

/// Enforce the context's configured retention (`retention_max_count` /
/// `retention_max_bytes`; `None` = keep). Only older *completed* bundles
/// are ever removed, newest first-kept: the newest completed bundle is
/// never deleted even when it alone exceeds the budget, and failed/queued/
/// running records are never touched. Each pruned bundle removes its `.zip`
/// and its `.status.json`; the in-memory record is dropped too. I/O errors
/// on individual files stop that file's removal only — pruning never fails
/// the backup that triggered it (callers ignore the report).
pub fn prune_retained_backups(ctx: &BackupContext) -> RetentionReport {
    let mut report = RetentionReport::default();
    let keep_count = ctx.retention_max_count.unwrap_or(usize::MAX);
    let keep_bytes = ctx.retention_max_bytes.unwrap_or(u64::MAX);
    if keep_count == usize::MAX && keep_bytes == u64::MAX {
        return report;
    }
    // Invalid direct constructions fail safe: a zero bound would delete
    // everything but the newest, so treat it as unset.
    let keep_count = if keep_count == 0 {
        usize::MAX
    } else {
        keep_count
    };
    let keep_bytes = if keep_bytes == 0 {
        u64::MAX
    } else {
        keep_bytes
    };
    let mut completed: Vec<BackupStatus> = list_backup_statuses(&ctx.backup_dir)
        .into_iter()
        .filter(|s| {
            s.state == BackupState::Completed
                && s.output_file.as_deref().is_some_and(|f| !f.is_empty())
        })
        .collect();
    // Newest first; ties keep the larger job id (monotonic suffix).
    completed.sort_by(|a, b| {
        b.finished_ms
            .cmp(&a.finished_ms)
            .then_with(|| b.job_id.cmp(&a.job_id))
    });
    let mut kept_bytes: u64 = 0;
    let mut kept: usize = 0;
    for status in &completed {
        if kept == 0 {
            // Never delete the newest completed bundle.
            kept = 1;
            kept_bytes = kept_bytes.saturating_add(bundle_bytes(&ctx.backup_dir, status));
            continue;
        }
        let size = bundle_bytes(&ctx.backup_dir, status);
        let over_count = kept >= keep_count;
        let over_bytes = kept_bytes.saturating_add(size) > keep_bytes;
        if !over_count && !over_bytes {
            kept += 1;
            kept_bytes = kept_bytes.saturating_add(size);
            continue;
        }
        let Some(name) = status.output_file.clone() else {
            continue;
        };
        let zip = ctx.backup_dir.join(&name);
        let bytes = std::fs::metadata(&zip).map(|m| m.len()).unwrap_or(size);
        if std::fs::remove_file(&zip).is_ok() {
            report.bundles_removed += 1;
            report.bytes_reclaimed = report.bytes_reclaimed.saturating_add(bytes);
        }
        let status_file = status_path(&ctx.backup_dir, &status.job_id);
        if std::fs::remove_file(&status_file).is_ok() {
            lock_registry().remove(&status.job_id);
        }
    }
    report
}

/// Best-effort byte size of a completed bundle (recorded value, else stat).
fn bundle_bytes(backup_dir: &Path, status: &BackupStatus) -> u64 {
    if let Some(bytes) = status.bytes_written {
        return bytes;
    }
    status
        .output_file
        .as_deref()
        .and_then(|name| std::fs::metadata(backup_dir.join(name)).ok())
        .map(|m| m.len())
        .unwrap_or(0)
}

/// Drain one queued backup (oldest first). Called by the worker drain arm.
/// Returns the completed job id, or `None` when nothing is queued. A
/// transient failure re-queues with bounded attempts; anything else fails
/// the record with a redacted reason.
pub fn run_next_backup(ctx: &BackupContext) -> Result<Option<String>, BackupError> {
    let _runner = runner_lock()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut queued = list_backup_statuses(&ctx.backup_dir);
    queued.retain(|s| s.state == BackupState::Queued);
    let Some(mut status) = queued.into_iter().next() else {
        return Ok(None);
    };
    status.state = BackupState::Running;
    status.started_ms = Some(now_ms());
    status.attempts = status.attempts.saturating_add(1);
    status.failure_redacted = None;
    persist_status(&ctx.backup_dir, &status)?;
    store_status(&status);
    match bundle::create_bundle(ctx, &status.job_id) {
        Ok(outcome) => {
            status.state = BackupState::Completed;
            status.finished_ms = Some(now_ms());
            status.output_file = Some(outcome.file_name);
            status.bytes_written = Some(outcome.bytes_written);
            status.tables_exported = outcome.tables_exported;
            status.media_files = outcome.media_files;
            status.gate_wait_ms = outcome.gate_wait_ms;
            persist_status(&ctx.backup_dir, &status)?;
            store_status(&status);
            // Best-effort rotation: pruning never fails a good backup.
            let _ = prune_retained_backups(ctx);
            Ok(Some(status.job_id))
        }
        Err(e @ BackupError::Transient(_)) | Err(e @ BackupError::Busy(_)) => {
            if status.attempts >= MAX_BACKUP_ATTEMPTS {
                status.state = BackupState::Failed;
                status.finished_ms = Some(now_ms());
                status.failure_redacted = Some(redact_failure(&e.to_string()));
                persist_status(&ctx.backup_dir, &status)?;
                store_status(&status);
                return Ok(Some(status.job_id));
            }
            status.state = BackupState::Queued;
            status.failure_redacted = Some(redact_failure(&e.to_string()));
            persist_status(&ctx.backup_dir, &status)?;
            store_status(&status);
            Err(e)
        }
        Err(e) => {
            status.state = BackupState::Failed;
            status.finished_ms = Some(now_ms());
            status.failure_redacted = Some(redact_failure(&e.to_string()));
            persist_status(&ctx.backup_dir, &status)?;
            store_status(&status);
            Err(e)
        }
    }
}

/// Run one request synchronously (tests / maintenance CLI). The request
/// must exist (see [`request_backup`); a finished record is left as-is and
/// its bundle path returned.
pub fn run_backup_blocking(ctx: &BackupContext, job_id: &str) -> Result<PathBuf, BackupError> {
    let _runner = runner_lock()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let Some(mut status) = load_status(&ctx.backup_dir, job_id)? else {
        return Err(BackupError::Invalid("unknown backup job".to_string()));
    };
    if status.state == BackupState::Completed {
        let name = status.output_file.clone().unwrap_or_default();
        return Ok(ctx.backup_dir.join(name));
    }
    status.state = BackupState::Running;
    status.started_ms = Some(now_ms());
    status.attempts = status.attempts.saturating_add(1);
    status.failure_redacted = None;
    persist_status(&ctx.backup_dir, &status)?;
    store_status(&status);
    match bundle::create_bundle(ctx, &status.job_id) {
        Ok(outcome) => {
            status.state = BackupState::Completed;
            status.finished_ms = Some(now_ms());
            status.output_file = Some(outcome.file_name.clone());
            status.bytes_written = Some(outcome.bytes_written);
            status.tables_exported = outcome.tables_exported;
            status.media_files = outcome.media_files;
            status.gate_wait_ms = outcome.gate_wait_ms;
            persist_status(&ctx.backup_dir, &status)?;
            store_status(&status);
            // Best-effort rotation: pruning never fails a good backup.
            let _ = prune_retained_backups(ctx);
            Ok(ctx.backup_dir.join(outcome.file_name))
        }
        Err(e) => {
            status.state = BackupState::Failed;
            status.finished_ms = Some(now_ms());
            status.failure_redacted = Some(redact_failure(&e.to_string()));
            persist_status(&ctx.backup_dir, &status)?;
            store_status(&status);
            Err(e)
        }
    }
}

/// Startup cleanup: remove abandoned `.partial-*` staging files and fail
/// stale `running` records (their process is gone — a failed backup must
/// never look final). Queued records are left for the drain. Wire from
/// `main.rs` before serving (see the parent snippet).
pub fn cleanup_abandoned_partials(backup_dir: &Path) -> Result<CleanupReport, BackupError> {
    let mut report = CleanupReport::default();
    let entries = match std::fs::read_dir(backup_dir) {
        Ok(entries) => entries.filter_map(|e| e.ok()).collect::<Vec<_>>(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(report),
        Err(e) => return Err(BackupError::Storage(format!("scan backup dir: {e}"))),
    };
    for entry in entries {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(BACKUP_PARTIAL_PREFIX) {
            match std::fs::remove_file(entry.path()) {
                Ok(()) => report.partials_removed += 1,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(BackupError::Storage(format!("remove partial backup: {e}")));
                }
            }
        }
    }
    for mut status in list_backup_statuses(backup_dir) {
        if status.state == BackupState::Running {
            status.state = BackupState::Failed;
            status.finished_ms = Some(now_ms());
            status.failure_redacted = Some("abandoned-restart".to_string());
            persist_status(backup_dir, &status)?;
            store_status(&status);
            report.stale_running_failed += 1;
        }
    }
    Ok(report)
}

/// Startup-cleanup outcome (safe to log: counts only, no paths or secrets).
#[derive(Debug, Clone, Default)]
pub struct CleanupReport {
    pub partials_removed: u64,
    pub stale_running_failed: u64,
}
