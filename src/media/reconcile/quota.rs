use crate::application::commands::AppServices;
use crate::application::ports::{JobKind, JobQueue, NewJob, PortError, TimelineCursor};
use crate::persistence::redb::MAX_MEDIA_PAGE_ROWS;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Rows per page for [`backfill_durable_accounting`]. Every page is one
/// bounded `list_all_media` read (FDB-safe); the in-memory sums stay
/// small (one entry per owner/blob key). Capped by
/// [`MAX_MEDIA_PAGE_ROWS`], the adapter contract maximum.
pub const BACKFILL_PAGE_SIZE: usize = 500;

/// Pages scanned by one [`backfill_durable_step`] job run before it yields
/// a durable continuation. Bounds one job's work to
/// `BACKFILL_PAGES_PER_RUN * BACKFILL_PAGE_SIZE` rows (2000 by default)
/// while the staged sums still fit a job payload (see
/// [`BACKFILL_STAGED_BUDGET_BYTES`]).
pub const BACKFILL_PAGES_PER_RUN: usize = 4;

/// Serialized-size budget for staged backfill sums carried in a
/// continuation job payload. Well under the 64 KiB job cap (leaves room
/// for the cursor + envelope). When a run's staged sums exceed this, the
/// run extends in memory instead of yielding — payloads stay small, and
/// only genuinely large tables run longer per job.
pub const BACKFILL_STAGED_BUDGET_BYTES: usize = 32 * 1024;

/// Outcome of one [`backfill_durable_accounting`] run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BackfillReport {
    /// Media rows scanned.
    pub media_scanned: u64,
    /// Owner byte-counter rows overwritten.
    pub owners_updated: u64,
    /// Blob-refcount rows overwritten.
    pub refcounts_updated: u64,
}

/// Staged backfill accumulation carried across durable continuations: exact
/// per-owner durable sums and per-key refcounts for the walked prefix, plus
/// the walked row count. Serialized into continuation job payloads
/// ([`BackfillDurableJobPayload`]); deterministic key order keeps retries
/// byte-identical.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillStaged {
    /// Exact durable bytes per owner for rows walked so far.
    #[serde(default)]
    pub durable_sums: BTreeMap<String, u64>,
    /// Exact live-reference count per blob key for rows walked so far
    /// (durable AND cache rows — every row references its bytes).
    #[serde(default)]
    pub ref_sums: BTreeMap<String, u64>,
    /// Rows walked so far (observability only).
    #[serde(default)]
    pub scanned: u64,
}

/// One bounded backfill step: either more pages remain (durable
/// continuation state) or the walk ended (exact final sums to persist).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackfillStep {
    /// More rows may remain: resume by passing `staged` + `next_after` to
    /// the next [`backfill_durable_step`]. Counters are NOT written yet —
    /// the accumulation covers only a prefix, and overwriting from a
    /// partial scan is never allowed.
    Continue {
        /// Accumulated prefix sums (durable across jobs via the
        /// continuation payload).
        staged: BackfillStaged,
        /// Exclusive resume cursor (last walked id).
        next_after: String,
    },
    /// The walk reached the oldest row: `staged` covers every walked row
    /// and is exact. Persist it with [`apply_backfill_sums`].
    Complete {
        /// Final report inputs (filled from `staged`).
        report: BackfillReport,
        /// Exact final sums.
        staged: BackfillStaged,
    },
}

/// Walk at most `max_pages` media pages from `after` (`None` = newest),
/// accumulating exact sums into `staged`. Only an empty page ends the walk
/// — short pages (concurrent deletes) and exact-multiple datasets both
/// continue to the trailing empty read, so a truncated adapter page can
/// never masquerade as end-of-scan.
///
/// When the page budget is spent mid-table, the step yields `Continue`
/// provided the staged sums still fit [`BACKFILL_STAGED_BUDGET_BYTES`];
/// otherwise the run extends in memory (payloads stay small, large tables
/// just run longer per job). A crashed run writes nothing (the worker
/// persists only on `Complete`), so any interruption restarts safely from
/// the last durable continuation — or from scratch, converging to the same
/// sums.
pub fn backfill_durable_step(
    services: &AppServices,
    mut staged: BackfillStaged,
    after: Option<String>,
    page_size: usize,
    max_pages: usize,
) -> Result<BackfillStep, PortError> {
    let page = page_size
        .clamp(1, BACKFILL_PAGE_SIZE)
        .min(MAX_MEDIA_PAGE_ROWS);
    let mut cursor_after = after;
    let mut pages: usize = 0;
    loop {
        let mut cursor = TimelineCursor::with_limit(page);
        cursor.until_id = cursor_after.clone();
        let rows = services.media.list_all_media(&cursor)?;
        if rows.is_empty() {
            let mut report = BackfillReport::default();
            report.media_scanned = staged.scanned;
            report.owners_updated = staged.durable_sums.len() as u64;
            report.refcounts_updated = staged.ref_sums.len() as u64;
            return Ok(BackfillStep::Complete { report, staged });
        }
        for meta in &rows {
            let entry = staged.ref_sums.entry(meta.blob_key.clone()).or_insert(0);
            *entry = entry.saturating_add(1);
            if let Some(owner) = meta.owner_id.as_deref() {
                let entry = staged.durable_sums.entry(owner.to_string()).or_insert(0);
                *entry = entry.saturating_add(meta.size_bytes);
            }
        }
        staged.scanned += rows.len() as u64;
        pages += 1;
        cursor_after = rows.last().map(|m| m.id.clone());
        if pages >= max_pages.max(1) {
            let staged_bytes = serde_json::to_vec(&staged)
                .map(|v| v.len())
                .unwrap_or(usize::MAX);
            if staged_bytes <= BACKFILL_STAGED_BUDGET_BYTES {
                // Non-empty page ⇒ `cursor_after` is `Some` (the last id).
                let next_after = cursor_after.expect("non-empty page has a last row id");
                return Ok(BackfillStep::Continue { staged, next_after });
            }
            // Staged sums no longer fit a durable continuation payload:
            // extend this run in memory instead of yielding.
        }
    }
}

/// Persist exact backfill sums: overwrite one owner counter + one refcount
/// row per accumulated entry. Call ONLY with a `Complete` step's staged
/// state (full-table accumulation). Idempotent: re-applying the same sums
/// writes the same rows.
pub fn apply_backfill_sums(
    services: &AppServices,
    staged: &BackfillStaged,
) -> Result<(), PortError> {
    for (owner, sum) in &staged.durable_sums {
        services.media.set_durable_bytes(owner, *sum)?;
    }
    for (key, count) in &staged.ref_sums {
        services.media.set_blob_refcount(key, *count)?;
    }
    Ok(())
}

/// Recompute `DURABLE_BYTES_BY_OWNER` (and `BLOB_REFCOUNT`) from the
/// metadata rows and overwrite the stored counters. Idempotent: a re-run
/// over unchanged rows writes the same sums. `page_size` is clamped to
/// `1..=BACKFILL_PAGE_SIZE`. The walk runs to the oldest row (only an empty
/// page ends it) and persists the sums once; the returned cursor is always
/// `None` (done). Sums accumulate in memory (one entry per owner/blob key)
/// and are persisted only after the final page, so an interrupted run
/// writes nothing and simply re-runs from the start.
///
/// `after_media_id` must be `None` (global rebuild). A mid-table start
/// would accumulate suffix-only sums that must never overwrite global
/// counters, so it is rejected loudly — resumed walks go through
/// [`backfill_durable_step`] with staged state instead.
pub fn backfill_durable_accounting(
    services: &AppServices,
    after_media_id: Option<String>,
    page_size: usize,
) -> Result<(BackfillReport, Option<String>), PortError> {
    if after_media_id.is_some() {
        return Err(PortError::Validation(
            "backfill_durable_accounting takes after=None (global rebuild); \
             use backfill_durable_step for resumed walks"
                .to_string(),
        ));
    }
    let mut staged = BackfillStaged::default();
    let mut after: Option<String> = None;
    loop {
        match backfill_durable_step(services, staged, after, page_size, usize::MAX)? {
            BackfillStep::Continue {
                staged: next,
                next_after,
            } => {
                staged = next;
                after = Some(next_after);
            }
            BackfillStep::Complete {
                report,
                staged: sums,
            } => {
                apply_backfill_sums(services, &sums)?;
                return Ok((report, None));
            }
        }
    }
}

/// Forward-compatible worker payload for `JobKind::BackfillDurableAccounting`:
/// the contract shape (`after`, `batch_size`) plus staged prefix sums for
/// durable continuations. Old payloads decode (staged defaults empty);
/// payloads carrying staged state are still valid JSON for old readers
/// (unknown fields ignored), but old handlers would walk the suffix and
/// overwrite partial sums — operators must not downgrade mid-chain. The
/// formal contract addition is proposed in the Batch C report.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackfillDurableJobPayload {
    /// Exclusive resume cursor (`None` = newest). The initial job uses
    /// `None`; continuations carry the last walked id.
    #[serde(default)]
    pub after: Option<String>,
    /// Adapter page size (clamped to `1..=BACKFILL_PAGE_SIZE`).
    #[serde(default)]
    pub batch_size: Option<usize>,
    /// Prefix sums accumulated by previous runs in this chain.
    #[serde(default)]
    pub staged: BackfillStaged,
}

/// Forward-compatible worker payload for `JobKind::ReconcileStorage`: the
/// contract shape (`scan_cap`) plus the resume cursor that turns repeated
/// bounded runs into one monotonic walk to the oldest row. Same
/// compatibility notes as [`BackfillDurableJobPayload`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconcileStorageJobPayload {
    /// Rows examined per run (clamped to `1..=RECONCILE_SCAN_CAP`).
    #[serde(default)]
    pub scan_cap: Option<usize>,
    /// Exclusive resume cursor (`None` = newest).
    #[serde(default)]
    pub after: Option<String>,
}

/// Build the repeatable bounded reconcile job (boot path + operator path).
/// Idempotent and always safe to run: one run examines at most
/// `RECONCILE_SCAN_CAP` rows, repairs only from complete walks, and chains
/// a durable continuation otherwise.
pub fn new_reconcile_job(now_ms: i64) -> Result<NewJob, PortError> {
    NewJob::new_json(
        JobKind::ReconcileStorage,
        &ReconcileStorageJobPayload {
            scan_cap: None,
            after: None,
        },
        format!("storage-reconcile:{now_ms}"),
        now_ms,
    )
}

/// Build the explicit backfill repair job (operator path only — NOT
/// boot-enqueued: it rewrites counters and belongs in a quiet maintenance
/// window; see the module safety model).
pub fn new_backfill_job(now_ms: i64) -> Result<NewJob, PortError> {
    NewJob::new_json(
        JobKind::BackfillDurableAccounting,
        &BackfillDurableJobPayload::default(),
        format!("storage-backfill:{now_ms}"),
        now_ms,
    )
}

/// Production trigger, repeatable side: enqueue one bounded reconcile job.
/// Intended call site: server startup after services are built (before
/// serving), so every boot heals what the previous run left behind; the
/// worker drains it within its normal polling. Safe to call repeatedly
/// (idempotent runs, timestamped idempotency key).
pub fn enqueue_storage_reconcile(jobs: &dyn JobQueue, now_ms: i64) -> Result<(), PortError> {
    jobs.enqueue_jobs(vec![new_reconcile_job(now_ms)?])
}

/// Production trigger, explicit side: enqueue one backfill repair job.
/// Intended call site: an explicit operator maintenance operation (CLI;
/// patch proposed in the Batch C report) — never automatic, per the module
/// safety model.
pub fn enqueue_storage_backfill(jobs: &dyn JobQueue, now_ms: i64) -> Result<(), PortError> {
    jobs.enqueue_jobs(vec![new_backfill_job(now_ms)?])
}
