//! Idempotent media reconciler (remote-media cache plan §5.4).
//!
//! Heals what the database/filesystem split cannot make atomic: orphan
//! bytes from crashes between blob writes and metadata commits, stale
//! cache bytes, drifted accounting, pre-slice-8 MD5-keyed rows, and `.len`
//! sidecar mismatches.
//!
//! Batch C (`prod-ready.md` §4) paging contract:
//!
//! * Every metadata walk pages newest-first through `list_all_media` with
//!   exclusive `until_id` cursors bounded by
//!   [`crate::persistence::redb::MAX_MEDIA_PAGE_ROWS`]. Pages advance
//!   monotonically (strictly older ids), so repeated runs make forward
//!   progress and never revisit the newest page. Completion means an empty
//!   page — never a short page.
//! * One [`reconcile_media`] run examines at most [`RECONCILE_SCAN_CAP`]
//!   rows and returns a continuation cursor when the table is larger; the
//!   worker enqueues it durably (`ReconcileStorageJobPayload.after`), so a
//!   multi-page reconcile reaches the oldest row across jobs.
//! * [`backfill_durable_accounting`] walks the whole table (empty-page
//!   termination) and persists sums once; [`backfill_durable_step`] bounds
//!   one job run to [`BACKFILL_PAGES_PER_RUN`] pages and carries staged sums
//!   in the durable continuation payload, so counters are overwritten only
//!   from a complete accumulation — never from a partial scan.
//! * Detection vs repair are separate counters: `*_drift_detected` counts
//!   compared-but-untouched rows (truncated scans and chained suffix
//!   continuations), `*_repaired` counts rows actually overwritten via the
//!   setters (from-newest complete walks only — suffix sums must never
//!   repair, they under-count).
//!
//! Safety model: the exact one-time rebuild is the at-open
//! `run_storage_accounting_backfill` migration (runs before requests are
//! served and before the worker starts, so nothing races it). The job paths
//! here are convergent repair for quiet periods: a run that observes a
//! concurrent mutation may write slightly stale sums, and the next
//! detection pass flags the residual drift. Operators should trigger the
//! backfill during low traffic (see [`enqueue_storage_backfill`]); the boot
//! reconcile job ([`enqueue_storage_reconcile`]) is bounded and always safe
//! to run.
//!
//! Split of labour for orphan bytes (task §5.4, item b):
//!
//! * Dangling-index orphans (a remote-cache index row whose media row is
//!   gone) are swept inside [`reconcile_media`] via traits only and counted
//!   in `orphans_removed`.
//! * Filesystem-level orphans (bytes no metadata references — e.g. a crash
//!   between `blobs.put` and the metadata commit) cannot be enumerated
//!   through the `BlobStore` trait, which has no list operation. They are
//!   swept by [`sweep_orphan_blobs`], which walks an `objects/` pool
//!   directory; the server startup wiring (which owns the `FsBlobStore`
//!   roots, `blobs_durable/` + `blobs_cache/` under the data dir) calls it
//!   with the live key sets from [`collect_live_blob_keys`].
//!
//! No-snapshot disclosure: separate pages are separate reads. Rows inserted
//! after a walk starts (larger ids, already passed) are not revisited, and
//! rows deleted mid-walk are skipped. Destructive operations must never run
//! on a partial key set: [`sweep_orphan_blobs_checked`] refuses an
//! explicitly incomplete set, and production wiring must only sweep the
//! fully walked sets (durable/cache pools strictly separate — a key is
//! swept only from the pool its rows reference).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::application::commands::AppServices;
use crate::application::ports::{
    BlobStore, JobKind, JobQueue, MediaClass, NewJob, PortError, TimelineCursor,
};
use crate::persistence::redb::MAX_MEDIA_PAGE_ROWS;

/// Rows examined per [`reconcile_media`] run. The run pages newest-first
/// with `until_id` cursors; larger instances continue across jobs via the
/// returned cursor (monotonic forward progress, never re-scanning the
/// newest page).
pub const RECONCILE_SCAN_CAP: usize = 5000;

/// Outcome of one [`reconcile_media`] run. All counters saturate at
/// `u64::MAX` in principle; in practice they are small.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    /// Media rows examined this run.
    pub examined: u64,
    /// True when the walk reached the oldest row (empty-page termination).
    /// False means more rows remain past the returned continuation cursor.
    pub complete: bool,
    /// Dangling-index cache bytes deleted (index row without a media row
    /// and `refcount == 0`). Filesystem-level orphans are swept by
    /// [`sweep_orphan_blobs`] and counted by its caller.
    pub orphans_removed: u64,
    /// Owner-less (cache) rows whose bytes were absent: the row is marked
    /// evicted (metadata + re-fetch index kept) and accounting adjusted
    /// down exactly once per row. Re-runs skip already-evicted rows, so the
    /// adjustment is idempotent and no phantom bytes are charged.
    pub missing_marked_evicted: u64,
    /// Durable rows whose bytes are unreadable in the durable pool.
    /// Metadata is NEVER deleted: the rows stay visible and are reported
    /// here (plus a bounded id sample) for operator repair.
    pub durable_missing: u64,
    /// Ids (at most [`DURABLE_MISSING_SAMPLE_CAP`]) backing
    /// `durable_missing`, oldest-sampled first, for operator triage.
    pub durable_missing_sample: Vec<String>,
    /// Blob-refcount drifts seen on a truncated run (compared only — the
    /// scan did not cover the whole table, so nothing was overwritten).
    pub refcount_drift_detected: u64,
    /// Durable-bytes drifts seen on a truncated run (compared only).
    pub durable_drift_detected: u64,
    /// Blob-refcount rows actually overwritten via `set_blob_refcount`
    /// after a complete scan (expected sums covered every row).
    pub refcounts_repaired: u64,
    /// Owner byte-counter rows actually overwritten via
    /// `set_durable_bytes` after a complete scan.
    pub durable_repaired: u64,
    /// Missing/corrupt `.len` sidecars regenerated by re-putting bytes
    /// that were still readable.
    pub sidecars_healed: u64,
    /// Pre-slice-8 MD5-keyed rows seen (flagged only — metadata and bytes
    /// are never deleted by the reconciler).
    pub md5_rows_purged_or_flagged: u64,
}

/// Cap on [`ReconcileReport::durable_missing_sample`] (operator triage
/// sample, not a paging bound).
pub const DURABLE_MISSING_SAMPLE_CAP: usize = 32;

/// Run one bounded reconciliation scan. Idempotent: re-running converges
/// (every mutation moves state toward accounted, indexed, present bytes).
pub fn reconcile_media(services: &AppServices) -> Result<ReconcileReport, PortError> {
    reconcile_media_with_cap(services, RECONCILE_SCAN_CAP)
}

/// [`reconcile_media`] with an explicit scan cap (storage job payload).
/// Caps above [`RECONCILE_SCAN_CAP`] are clamped — one run never exceeds
/// the bound the reconciler was designed around. Single-page entry point
/// kept for callers that want one run: pages newest-first from the start
/// and discards the continuation cursor (use [`reconcile_media_paged`]
/// when the table may exceed the cap).
pub fn reconcile_media_with_cap(
    services: &AppServices,
    scan_cap: usize,
) -> Result<ReconcileReport, PortError> {
    Ok(reconcile_media_paged(services, None, scan_cap)?.0)
}

/// [`reconcile_media`] starting after `after` (`None` = newest), examining
/// at most `scan_cap` rows (clamped to [`RECONCILE_SCAN_CAP`]).
///
/// Returns the report plus the continuation cursor: `Some(last id)` when
/// more rows may remain (the run hit the cap without an empty page),
/// `None` when the walk reached the oldest row (`report.complete` is the
/// same signal as a bool). Cursors are exclusive and monotonic — a resume
/// passes the returned id as the next `after` and never re-examines a row.
/// Only an empty page terminates the walk: short pages (concurrent deletes)
/// and exact-multiple datasets both continue to the trailing empty read.
/// Counter repair runs only on from-newest complete walks (see below);
/// chained continuations detect drift but never overwrite.
///
/// Per-row semantics: durable rows with missing bytes are counted in
/// `durable_missing` (+ sample) and otherwise untouched; cache rows with
/// missing bytes are charged off once (index-anchored, idempotent);
/// readable bytes with a missing/corrupt sidecar are re-put. Drift repair
/// runs only on from-newest complete walks; truncated walks and chained
/// suffix continuations count detections.
pub fn reconcile_media_paged(
    services: &AppServices,
    after: Option<&str>,
    scan_cap: usize,
) -> Result<(ReconcileReport, Option<String>), PortError> {
    let cap = scan_cap.clamp(1, RECONCILE_SCAN_CAP);
    // One adapter page per read: never exceed the run cap per page, and
    // never exceed the adapter contract maximum.
    let page = cap.min(MAX_MEDIA_PAGE_ROWS);
    let mut report = ReconcileReport::default();
    let mut cursor_after: Option<String> = after.map(ToOwned::to_owned);
    // Blob keys whose missing-bytes adjustment was already applied this
    // run (shared blobs adjust once, not once per row).
    let mut adjusted_blobs: HashSet<String> = HashSet::new();

    // Expected state recomputed from the walk: per-key reference counts
    // and per-owner durable sums, compared against the stored counters.
    let mut expected_ref: HashMap<String, u64> = HashMap::new();
    let mut expected_durable: HashMap<String, u64> = HashMap::new();

    loop {
        let mut cursor = TimelineCursor::with_limit(page);
        cursor.until_id = cursor_after.clone();
        let rows = services.media.list_all_media(&cursor)?;
        if rows.is_empty() {
            report.complete = true;
            break;
        }
        for meta in &rows {
            *expected_ref.entry(meta.blob_key.clone()).or_insert(0) += 1;
            let legacy = is_legacy_md5_row(&meta.blob_key);
            if legacy {
                // Flag only: never delete metadata or bytes for legacy rows
                // (the key may be shared; §5.4 says purge-or-migrate, and the
                // safe half is the flag + this count).
                report.md5_rows_purged_or_flagged += 1;
            }
            // Durable pool for owned rows, cache pool for owner-less rows.
            // Never touch the wrong pool (local uploads must survive any
            // amount of cache pressure, and cache sweeps must never eat
            // durable bytes).
            let pool: &dyn BlobStore = match &meta.owner_id {
                Some(owner) => {
                    let entry = expected_durable.entry(owner.clone()).or_insert(0);
                    *entry = entry.saturating_add(meta.size_bytes);
                    services.blobs.as_ref()
                }
                None => services.cache_blobs.as_ref(),
            };
            match pool.byte_len(&meta.blob_key)? {
                Some(_) => {}
                None => {
                    // Bytes unreadable through this pool: try the sidecar heal
                    // (a backend whose `get` still serves bytes while the
                    // sidecar is missing/corrupt gets a regenerated sidecar),
                    // else pool-appropriate missing handling.
                    match pool.get(&meta.blob_key)? {
                        Some(bytes) => {
                            pool.put(&meta.blob_key, &bytes)?;
                            report.sidecars_healed += 1;
                        }
                        None if legacy => {
                            // Flagged above; never account or delete legacy rows.
                        }
                        None if meta.owner_id.is_some() => {
                            // Durable bytes missing: NEVER auto-delete
                            // metadata. Count it and sample the id so the
                            // row stays visible to operators for repair.
                            report.durable_missing += 1;
                            if report.durable_missing_sample.len() < DURABLE_MISSING_SAMPLE_CAP {
                                report.durable_missing_sample.push(meta.id.clone());
                            }
                        }
                        None => {
                            // Cache bytes missing: adjust accounting down
                            // exactly once per blob per run (several rows
                            // can share one blob key), anchored on the
                            // `(source_url, variant)` index row so re-runs
                            // stay idempotent. The trait has no persisted
                            // evicted flag (`InsertMediaInput` carries none
                            // and the backend derives the field), so the
                            // index record doubles as the adjustment
                            // record: its `size_bytes` is zeroed when the
                            // missing bytes are charged off (a zero size
                            // adjusts nothing on re-runs), while the index
                            // row itself is KEPT for re-fetch bookkeeping.
                            // A later re-fetch overwrites the record with
                            // fresh bytes + size, self-healing the anchor.
                            // The metadata row always survives: dropping it
                            // would destroy the re-fetch path.
                            if meta.evicted {
                                continue;
                            }
                            if !adjusted_blobs.insert(meta.blob_key.clone()) {
                                continue;
                            }
                            let Some(url) = meta.source_url.as_deref() else {
                                // No index anchor exists to record the
                                // adjustment: leave the charge in place
                                // (bounded, non-ratcheting) rather than
                                // double-subtracting on every run.
                                continue;
                            };
                            let Some(mut rec) =
                                services.media.remote_cache_lookup(url, &meta.variant)?
                            else {
                                // Already adjusted on an earlier run (anchor
                                // zeroed) — skip so re-runs never
                                // double-subtract.
                                continue;
                            };
                            if rec.size_bytes == 0 {
                                continue;
                            }
                            let class = MediaClass::classify_mime(&meta.mime);
                            services.media.cache_accounting_apply(
                                -(meta.size_bytes as i64),
                                class,
                                -(meta.size_bytes as i64),
                                0,
                                0,
                                0,
                                0,
                                0,
                            )?;
                            rec.size_bytes = 0;
                            services.media.remote_cache_insert(&rec)?;
                            report.missing_marked_evicted += 1;
                        }
                    }
                }
            }
        }
        report.examined += rows.len() as u64;
        cursor_after = rows.last().map(|m| m.id.clone());
        if report.examined >= cap as u64 {
            break;
        }
    }

    // (b, trait-level half): dangling remote-cache index rows — indexed but
    // the media row is gone. Drop the bytes only when nothing references
    // the key, always drop the stale accounting, and always remove the
    // index row so the next lookup misses and re-fetches cleanly.
    let mut accounted_blobs: HashSet<String> = HashSet::new();
    let candidates = services.media.remote_cache_evict_candidates(cap)?;
    for rec in &candidates {
        if services.media.get(&rec.media_id)?.is_some() {
            continue;
        }
        if services.media.blob_refcount(&rec.blob_key)? == 0 {
            services.cache_blobs.delete(&rec.blob_key)?;
            report.orphans_removed += 1;
        }
        // One accounting adjustment per blob key even if several dangling
        // index rows share it.
        if accounted_blobs.insert(rec.blob_key.clone()) {
            services.media.cache_accounting_apply(
                -(rec.size_bytes as i64),
                rec.class,
                -(rec.size_bytes as i64),
                0,
                0,
                0,
                1,
                0,
            )?;
        }
        services
            .media
            .remote_cache_remove(&rec.source_url, &rec.variant)?;
    }

    // (c-d) refcount + durable-bytes drift. Repaired only when THIS run
    // walked the whole table from the newest row (`after == None` plus an
    // empty page): only then are the expected sums exact. Chained suffix
    // runs and truncated runs count detections, so a partial view can
    // never overwrite live counters. (Suffix sums must not repair: they
    // under-count shared keys and multi-page owners. Global rewrites go
    // through the backfill chain, which accumulates staged sums across
    // continuations.)
    if report.complete && after.is_none() {
        for (key, expected) in &expected_ref {
            if services.media.blob_refcount(key)? != *expected {
                services.media.set_blob_refcount(key, *expected)?;
                report.refcounts_repaired += 1;
            }
        }
        for (owner, expected) in &expected_durable {
            if services.media.durable_bytes(owner)? != *expected {
                services.media.set_durable_bytes(owner, *expected)?;
                report.durable_repaired += 1;
            }
        }
    } else {
        for (key, expected) in &expected_ref {
            if services.media.blob_refcount(key)? != *expected {
                report.refcount_drift_detected += 1;
            }
        }
        for (owner, expected) in &expected_durable {
            if services.media.durable_bytes(owner)? != *expected {
                report.durable_drift_detected += 1;
            }
        }
    }

    let next = if report.complete { None } else { cursor_after };
    Ok((report, next))
}

/// Live blob keys partitioned by pool, for [`sweep_orphan_blobs`].
/// `(durable_keys, cache_keys)`. Walks the whole table newest-first with
/// exclusive `until_id` cursors to an empty page, so the returned sets are
/// complete by construction (see the no-snapshot disclosure in the module
/// docs).
pub fn collect_live_blob_keys(
    services: &AppServices,
) -> Result<(HashSet<String>, HashSet<String>), PortError> {
    let mut durable = HashSet::new();
    let mut cache = HashSet::new();
    let mut after: Option<String> = None;
    loop {
        let mut cursor = TimelineCursor::with_limit(MAX_MEDIA_PAGE_ROWS);
        cursor.until_id = after;
        let rows = services.media.list_all_media(&cursor)?;
        if rows.is_empty() {
            break;
        }
        for meta in &rows {
            if meta.owner_id.is_some() {
                durable.insert(meta.blob_key.clone());
            } else {
                cache.insert(meta.blob_key.clone());
            }
        }
        after = rows.last().map(|m| m.id.clone());
    }
    Ok((durable, cache))
}

/// Reset a leaked in-flight reservation (crash-heal). Call at startup:
/// with no live fetches running, any `in_flight_bytes` is a leak from a
/// crash between `reserve_cache_bytes` and its matching release. The
/// release is saturating on correct backends, so an already-zero counter
/// is a no-op.
pub fn reset_in_flight(services: &AppServices) -> Result<(), PortError> {
    let stats = services.media.cache_stats()?;
    if stats.in_flight_bytes > 0 {
        services
            .media
            .release_cache_reservation(stats.in_flight_bytes)?;
    }
    Ok(())
}

/// Sweep one `objects/` pool directory (`<pool>/objects/xx/<key>` shards
/// with `<key>.len` sidecars): delete every key file — plus its sidecar —
/// not in `live_keys`. Returns the number of object keys removed.
///
/// Safety: only alphanumeric keys of length 4–128 are eligible (the
/// `FsBlobStore` key shape), `.len` sidecars are never matched directly
/// (they die with their object), non-directories and unreadable entries
/// are skipped, a missing pool directory sweeps nothing, and `NotFound`
/// races on delete are ignored. Metadata is never touched here.
///
/// The caller vouches that `live_keys` is COMPLETE (a fully walked set
/// from [`collect_live_blob_keys`): this function cannot tell a partial
/// set from a full one. Callers that cannot vouch completeness must use
/// [`sweep_orphan_blobs_checked`] and pass their completion signal.
/// Prefer leaking an orphan over deleting live media.
pub fn sweep_orphan_blobs(
    objects_dir: &Path,
    live_keys: &HashSet<String>,
) -> Result<u64, PortError> {
    sweep_orphan_blobs_checked(objects_dir, live_keys, true)
}

/// [`sweep_orphan_blobs`] with an explicit completeness gate: `complete`
/// must be true — the result of a fully walked live-key set — or the sweep
/// refuses with `Validation` and deletes nothing. This is the destructive
/// operation's guard against truncated scans: a short page, a capped walk,
/// or any interrupted collection must never feed a delete sweep.
pub fn sweep_orphan_blobs_checked(
    objects_dir: &Path,
    live_keys: &HashSet<String>,
    complete: bool,
) -> Result<u64, PortError> {
    if !complete {
        return Err(PortError::Validation(
            "refusing orphan-blob sweep on an incomplete live-key set; \
             finish the paged collection to an empty page first"
                .to_string(),
        ));
    }
    // Batch F gate (Batch H one-snippet): pool deletions wait behind an
    // active backup so a concurrent snapshot/copy can never lose bytes the
    // bundle's metadata references. Held for the whole sweep.
    let _media_gate = crate::media::storage::acquire_media_op("sweep")?;
    let shards = match std::fs::read_dir(objects_dir) {
        Ok(shards) => shards,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => {
            return Err(PortError::StorageUnavailable(format!(
                "list orphan-sweep pool dir: {e}"
            )))
        }
    };
    let mut removed: u64 = 0;
    for shard in shards {
        let shard = match shard {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        if !shard.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let entries = match std::fs::read_dir(shard.path()) {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => continue,
            };
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".len") {
                continue;
            }
            if live_keys.contains(&name) {
                continue;
            }
            if !(4..=128).contains(&name.len())
                || !name.bytes().all(|b| b.is_ascii_alphanumeric())
            {
                continue;
            }
            match std::fs::remove_file(entry.path()) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => {
                    return Err(PortError::StorageUnavailable(format!(
                        "delete orphan blob {name}: {e}"
                    )))
                }
            }
            let mut sidecar = entry.path().into_os_string();
            sidecar.push(".len");
            let _ = std::fs::remove_file(&sidecar);
            removed += 1;
        }
    }
    Ok(removed)
}

/// Pre-slice-8 rows keyed their blob by MD5 (32 hex chars) instead of
/// SHA-256 (64 hex chars). Anything that is not a 64-char hex key is
/// legacy: flagged, never purged.
fn is_legacy_md5_row(blob_key: &str) -> bool {
    blob_key.len() != 64 || !blob_key.bytes().all(|b| b.is_ascii_hexdigit())
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::ports::{
        ActivityDedup, BlobStore, CacheStats, InsertMediaInput, MediaStore, NoopStreamingSink,
        RemoteCacheRecord, TimelineCursor,
    };
    use crate::domain::id::{AidxGenerator, Clock, IdGenerator, ManualClock};
    use crate::domain::media::{MediaMeta, MediaProperties};
    use crate::federation::delivery::{SafeHttpClient, SafeHttpConfig};
    use crate::media::storage::FsBlobStore;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};
    use std::sync::atomic::{AtomicU64, Ordering};

    // --- Minimal in-memory MediaStore fake (trait-complete) ---

    #[derive(Debug, Default)]
    struct FakeMedia {
        rows: Mutex<HashMap<String, MediaMeta>>,
        index: Mutex<HashMap<(String, String), RemoteCacheRecord>>,
        stats: Mutex<CacheStats>,
    }

    impl MediaStore for FakeMedia {
        fn insert_metadata(&self, input: InsertMediaInput) -> Result<MediaMeta, PortError> {
            let meta = MediaMeta {
                id: input.id.unwrap_or_else(|| "mid00000000000001".to_string()),
                owner_id: input.owner_id,
                name: input.name,
                mime: input.mime,
                size_bytes: input.size_bytes,
                hash: input.hash,
                blob_key: input.blob_key,
                blurhash: input.blurhash,
                properties: input.properties,
                is_sensitive: input.is_sensitive,
                is_link: input.is_link,
                url: input.url,
                thumbnail_url: input.thumbnail_url,
                comment: input.comment,
                source_url: input.source_url,
                // `InsertMediaInput` carries no variant/class/evicted
                // fields: the fake stores the documented defaults
                // (agent A follow-up decides the real backend mapping).
                variant: "original".to_string(),
                media_class: MediaClass::Preview.as_str().to_string(),
                evicted: false,
            };
            self.rows.lock().unwrap().insert(meta.id.clone(), meta.clone());
            Ok(meta)
        }
        fn get(&self, id: &str) -> Result<Option<MediaMeta>, PortError> {
            Ok(self.rows.lock().unwrap().get(id).cloned())
        }
        fn find_by_owner_and_hash(
            &self,
            owner_id: &str,
            hash: &str,
        ) -> Result<Option<MediaMeta>, PortError> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .values()
                .find(|m| m.owner_id.as_deref() == Some(owner_id) && m.hash == hash)
                .cloned())
        }
        fn find_by_source_url(&self, source_url: &str) -> Result<Vec<MediaMeta>, PortError> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .values()
                .filter(|m| m.source_url.as_deref() == Some(source_url))
                .cloned()
                .collect())
        }
        fn delete_metadata(&self, id: &str) -> Result<(), PortError> {
            self.rows.lock().unwrap().remove(id);
            Ok(())
        }
        fn list_by_owner(&self, owner_id: &str) -> Result<Vec<MediaMeta>, PortError> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .values()
                .filter(|m| m.owner_id.as_deref() == Some(owner_id))
                .cloned()
                .collect())
        }
        fn list_all_media(&self, cursor: &TimelineCursor) -> Result<Vec<MediaMeta>, PortError> {
            let mut out: Vec<MediaMeta> = self.rows.lock().unwrap().values().cloned().collect();
            out.sort_by(|a, b| b.id.cmp(&a.id));
            // Exclusive cursor bounds, mirroring the production adapter
            // contract (newest-first, `until_id` walks to an empty page).
            out.retain(|m| {
                if let Some(since) = cursor.since_id.as_deref() {
                    if m.id.as_str() <= since {
                        return false;
                    }
                }
                if let Some(until) = cursor.until_id.as_deref() {
                    if m.id.as_str() >= until {
                        return false;
                    }
                }
                true
            });
            let limit = if cursor.limit == 0 { 10 } else { cursor.limit };
            out.truncate(limit);
            Ok(out)
        }
        fn durable_bytes(&self, owner_id: &str) -> Result<u64, PortError> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .values()
                .filter(|m| m.owner_id.as_deref() == Some(owner_id))
                .map(|m| m.size_bytes)
                .sum())
        }
        fn blob_refcount(&self, blob_key: &str) -> Result<u64, PortError> {
            Ok(self
                .rows
                .lock()
                .unwrap()
                .values()
                .filter(|m| m.blob_key == blob_key)
                .count() as u64)
        }
        fn remote_cache_lookup(
            &self,
            source_url: &str,
            variant: &str,
        ) -> Result<Option<RemoteCacheRecord>, PortError> {
            Ok(self
                .index
                .lock()
                .unwrap()
                .get(&(source_url.to_string(), variant.to_string()))
                .cloned())
        }
        fn remote_cache_insert(&self, rec: &RemoteCacheRecord) -> Result<(), PortError> {
            self.index.lock().unwrap().insert(
                (rec.source_url.clone(), rec.variant.clone()),
                rec.clone(),
            );
            Ok(())
        }
        fn remote_cache_touch(
            &self,
            source_url: &str,
            variant: &str,
            now_ms: i64,
        ) -> Result<(), PortError> {
            if let Some(rec) = self
                .index
                .lock()
                .unwrap()
                .get_mut(&(source_url.to_string(), variant.to_string()))
            {
                rec.last_access_ms = now_ms;
                rec.access_count += 1;
            }
            Ok(())
        }
        fn remote_cache_evict_candidates(
            &self,
            limit: usize,
        ) -> Result<Vec<RemoteCacheRecord>, PortError> {
            let mut out: Vec<RemoteCacheRecord> =
                self.index.lock().unwrap().values().cloned().collect();
            out.sort_by_key(|r| r.last_access_ms);
            out.truncate(limit);
            Ok(out)
        }
        fn remote_cache_remove(
            &self,
            source_url: &str,
            variant: &str,
        ) -> Result<(), PortError> {
            self.index
                .lock()
                .unwrap()
                .remove(&(source_url.to_string(), variant.to_string()));
            Ok(())
        }
        fn cache_stats(&self) -> Result<CacheStats, PortError> {
            Ok(self.stats.lock().unwrap().clone())
        }
        fn cache_accounting_apply(
            &self,
            total_delta: i64,
            class: MediaClass,
            class_delta: i64,
            hits: u64,
            misses: u64,
            bytes_fetched: u64,
            evictions: u64,
            failed_origins: u64,
        ) -> Result<(), PortError> {
            let mut s = self.stats.lock().unwrap();
            let apply = |v: u64, d: i64| -> u64 {
                if d >= 0 {
                    v.saturating_add(d as u64)
                } else {
                    v.saturating_sub((-d) as u64)
                }
            };
            s.total_bytes = apply(s.total_bytes, total_delta);
            match class {
                MediaClass::Avatar => s.avatar_bytes = apply(s.avatar_bytes, class_delta),
                MediaClass::Preview => s.preview_bytes = apply(s.preview_bytes, class_delta),
                MediaClass::Full => s.full_bytes = apply(s.full_bytes, class_delta),
                // AudioVideo has no dedicated counter: fold any residual
                // class delta into the total (callers pass equal deltas).
                MediaClass::AudioVideo => {
                    s.total_bytes =
                        apply(s.total_bytes, class_delta.saturating_sub(total_delta));
                }
            }
            s.hits += hits;
            s.misses += misses;
            s.bytes_fetched += bytes_fetched;
            s.evictions += evictions;
            s.failed_origins += failed_origins;
            Ok(())
        }
        fn reserve_cache_bytes(&self, bytes: u64) -> Result<bool, PortError> {
            let mut s = self.stats.lock().unwrap();
            // Fake cap: 1 MiB, to mirror a real global-cap check.
            if s.total_bytes.saturating_add(s.in_flight_bytes).saturating_add(bytes) > (1 << 20) {
                return Ok(false);
            }
            s.in_flight_bytes = s.in_flight_bytes.saturating_add(bytes);
            Ok(true)
        }
        fn release_cache_reservation(&self, bytes: u64) -> Result<(), PortError> {
            let mut s = self.stats.lock().unwrap();
            s.in_flight_bytes = s.in_flight_bytes.saturating_sub(bytes);
            Ok(())
        }
    }

    /// Scratch temp dir (no `tempfile` dev-dependency; mirrors the other
    /// test modules' `std::env::temp_dir` pattern).
    fn scratch_dir(tag: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinetree-reconcile-{}-{tag}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// `AppServices` with a fake media store and real filesystem blob pools.
    /// Every other store is the production redb backend, which the
    /// reconciler never touches — this keeps `AppServices` fully populated
    /// without hand-rolled stubs for 20+ stores.
    fn fake_services() -> (AppServices, Arc<FakeMedia>, std::path::PathBuf) {
        let dir = scratch_dir("svc");
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(1_760_400_000_000));
        let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
        let fake = Arc::new(FakeMedia::default());
        // Reuse the production redb backend for the stores the reconciler
        // never touches, so `AppServices` stays fully populated.
        let store = crate::persistence::redb::RedbStore::open(
            dir.join("test.redb"),
            clock.clone(),
            ids.clone(),
        )
        .unwrap();
        let services = AppServices {
            clock,
            ids,
            accounts: store.clone(),
            notes: store.clone(),
            follows: store.clone(),
            blocks: store.clone(),
            mutes: store.clone(),
            reactions: store.clone(),
            favorites: store.clone(),
            emojis: store.clone(),
            antennas: store.clone(),
            antenna_notes: store.clone(),
            polls: store.clone(),
            jobs: store.clone(),
            media: fake.clone(),
            blobs: Arc::new(FsBlobStore::new(dir.join("blobs_durable")).unwrap()),
            cache_blobs: Arc::new(FsBlobStore::new(dir.join("blobs_cache")).unwrap()),
            notifications: store.clone(),
            auth: store.clone(),
            streaming: Arc::new(NoopStreamingSink),
            dedup: store.clone() as Arc<dyn ActivityDedup>,
            host_caps: store.clone(),
            moderation: store.clone(),
            instance_settings: store.clone(),
            invites: store.clone(),
            abuse_reports: store.clone(),
            lists: store.clone(),
            signin: store.clone(),
            password_resets: store.clone(),
            registry: store.clone(),
            clips: store.clone(),
            pages: store.clone(),
            folders: store.clone(),
            system_webhooks: store.clone(),
            user_webhooks: store.clone(),
            abuse_recipients: store.clone(),
            memos: store.clone(),
            thread_mutes: store.clone(),
            federation_http: SafeHttpClient::new(SafeHttpConfig {
                allow_private_ips: true,
                ..Default::default()
            }),
            public_url: "https://pinetree.local".to_string(),
            max_file_size: 4096,
            preserved_usernames: Vec::new(),
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
            note_draft_limit: crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
            scheduled_note_limit: crate::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
            pin_limit: crate::domain::pinning::DEFAULT_PIN_LIMIT,
            default_like: None,
            drive_capacity_per_user_bytes: 1 << 30,
            remote_cache_max_bytes: 512 << 20,
            remote_cache_avatar_bytes: 128 << 20,
            remote_cache_preview_bytes: 256 << 20,
            remote_cache_full_bytes: 128 << 20,
            remote_cache_mode: crate::config::RemoteCacheMode::OnDemand,
            remote_cache_avatar_ttl_days: 30,
            remote_cache_preview_ttl_days: 7,
            remote_cache_full_ttl_days: 3,
            remote_cache_warming_enabled: false,
        };
        (services, fake, dir)
    }

    fn insert_row(
        services: &AppServices,
        id: &str,
        owner: Option<&str>,
        blob_key: &str,
        size: u64,
        source_url: Option<&str>,
    ) -> MediaMeta {
        services
            .media
            .insert_metadata(InsertMediaInput {
                id: Some(id.to_string()),
                owner_id: owner.map(ToOwned::to_owned),
                name: "f.bin".to_string(),
                mime: "application/octet-stream".to_string(),
                size_bytes: size,
                hash: "0cc175b9c0f1b6a831c399e269772661".to_string(),
                blob_key: blob_key.to_string(),
                blurhash: None,
                properties: MediaProperties::default(),
                is_sensitive: false,
                is_link: owner.is_none(),
                url: format!("https://pinetree.local/files/{id}"),
                thumbnail_url: None,
                comment: None,
                source_url: source_url.map(ToOwned::to_owned),
                max_file_size: 4096,
            })
            .unwrap()
    }

    #[test]
    fn reconcile_heals_missing_cache_bytes_and_counts_legacy_rows() {
        let (services, _, _dir) = fake_services();
        // Durable row with present bytes: untouched.
        let durable_key = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        services.blobs.put(durable_key, b"durable").unwrap();
        insert_row(
            &services,
            "mid00000000000001",
            Some("aid00000000000001"),
            durable_key,
            7,
            None,
        );
        // Cache row with present bytes: untouched.
        let live_key = "abababababababababababababababababababababababababababababababab";
        services.cache_blobs.put(live_key, b"live").unwrap();
        insert_row(
            &services,
            "mid00000000000002",
            None,
            live_key,
            4,
            Some("https://r.example/a"),
        );
        // Cache row with MISSING bytes: accounting adjusted, row kept. The
        // production fetch path always writes the `(source_url, variant)`
        // index row alongside the metadata row, so the fixture does too —
        // the reconciler anchors its exactly-once adjustment on it.
        let gone_key = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
        insert_row(
            &services,
            "mid00000000000003",
            None,
            gone_key,
            9,
            Some("https://r.example/b"),
        );
        services
            .media
            .remote_cache_insert(&RemoteCacheRecord {
                source_url: "https://r.example/b".to_string(),
                variant: "original".to_string(),
                media_id: "mid00000000000003".to_string(),
                blob_key: gone_key.to_string(),
                size_bytes: 9,
                class: MediaClass::Preview,
                fetched_at_ms: 1,
                last_access_ms: 1,
                access_count: 1,
            })
            .unwrap();
        // Legacy MD5-keyed row: flagged, never deleted.
        insert_row(
            &services,
            "mid00000000000004",
            None,
            "d41d8cd98f00b204e9800998ecf8427e",
            0,
            None,
        );

        let report = reconcile_media(&services).unwrap();
        assert_eq!(report.examined, 4);
        assert!(report.complete);
        assert_eq!(report.missing_marked_evicted, 1);
        assert_eq!(report.md5_rows_purged_or_flagged, 1);
        // Missing cache bytes were accounted down (fake starts at 0, so the
        // signed delta saturates — the call itself must succeed).
        assert!(services.media.get("mid00000000000003").unwrap().is_some());
        assert!(services.media.get("mid00000000000004").unwrap().is_some());
        // Re-running is idempotent: the zeroed index anchor suppresses a
        // second adjustment, and neither metadata row is ever deleted.
        let again = reconcile_media(&services).unwrap();
        assert_eq!(again.missing_marked_evicted, 0);
        assert!(services.media.get("mid00000000000003").unwrap().is_some());
        assert!(services.media.get("mid00000000000004").unwrap().is_some());
    }

    #[test]
    fn reconcile_sweeps_dangling_index_rows() {
        let (services, _, _dir) = fake_services();
        let key = "abababababababababababababababababababababababababababababababab";
        services.cache_blobs.put(key, b"orphan").unwrap();
        services
            .media
            .remote_cache_insert(&RemoteCacheRecord {
                source_url: "https://r.example/gone".to_string(),
                variant: "original".to_string(),
                media_id: "mid00000000000009".to_string(),
                blob_key: key.to_string(),
                size_bytes: 6,
                class: MediaClass::Preview,
                fetched_at_ms: 1,
                last_access_ms: 1,
                access_count: 1,
            })
            .unwrap();
        let report = reconcile_media(&services).unwrap();
        assert_eq!(report.orphans_removed, 1);
        assert_eq!(services.cache_blobs.byte_len(key).unwrap(), None);
        assert!(services
            .media
            .remote_cache_lookup("https://r.example/gone", "original")
            .unwrap()
            .is_none());
    }

    #[test]
    fn sweep_orphan_blobs_removes_only_unlisted_keys() {
        let mut live_keys = HashSet::new();
        let live = "abababababababababababababababababababababababababababababababab";
        let dead = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
        live_keys.insert(live.to_string());
        // Sweep a scratch pool (same `objects/xx/<key>` layout as the
        // production pools).
        let scratch = scratch_dir("sweep");
        let pool = FsBlobStore::new(&scratch).unwrap();
        pool.put(live, b"keep").unwrap();
        pool.put(dead, b"drop").unwrap();
        let objects = scratch.join("objects");
        let removed = sweep_orphan_blobs(&objects, &live_keys).unwrap();
        assert_eq!(removed, 1);
        assert!(pool.get(dead).unwrap().is_none());
        assert_eq!(pool.get(live).unwrap().unwrap(), b"keep");
        // Missing dir sweeps nothing.
        assert_eq!(
            sweep_orphan_blobs(&scratch.join("nope"), &live_keys).unwrap(),
            0
        );
        // Non-key junk and sidecar-only names are never eligible.
        let _ = std::fs::create_dir_all(objects.join("zz"));
        std::fs::write(objects.join("zz").join("!!!not-a-key!!!"), b"x").unwrap();
        std::fs::write(objects.join("zz").join("orphan.len"), b"3").unwrap();
        assert_eq!(sweep_orphan_blobs(&objects, &live_keys).unwrap(), 0);
    }

    #[test]
    fn reset_in_flight_releases_leaked_reservation() {
        let (services, fake, _dir) = fake_services();
        assert!(fake.reserve_cache_bytes(100).unwrap());
        assert_eq!(fake.cache_stats().unwrap().in_flight_bytes, 100);
        reset_in_flight(&services).unwrap();
        assert_eq!(fake.cache_stats().unwrap().in_flight_bytes, 0);
    }
}
