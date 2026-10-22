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
//!   swept by [`sweep_orphan_blobs_checked`], which walks an `objects/` pool
//!   directory; the server startup wiring (which owns the `FsBlobStore`
//!   roots, `blobs_durable/` + `blobs_cache/` under the data dir) calls it
//!   with the live key sets from [`collect_live_blob_keys`] plus the
//!   completion signal from that full walk.
//!
//! No-snapshot disclosure: separate pages are separate reads. Rows inserted
//! after a walk starts (larger ids, already passed) are not revisited, and
//! rows deleted mid-walk are skipped. Destructive operations must never run
//! on a partial key set: [`sweep_orphan_blobs_checked`] refuses an
//! explicitly incomplete set, and production wiring must only sweep the
//! fully walked sets (durable/cache pools strictly separate — a key is
//! swept only from the pool its rows reference).

mod heal;
mod quota;
mod scan;
#[cfg(test)]
mod support;

pub use heal::{reset_in_flight, sweep_orphan_blobs, sweep_orphan_blobs_checked};
pub use quota::{
    BACKFILL_PAGE_SIZE, BACKFILL_PAGES_PER_RUN, BACKFILL_STAGED_BUDGET_BYTES,
    BackfillDurableJobPayload, BackfillReport, BackfillStaged, BackfillStep,
    ReconcileStorageJobPayload, apply_backfill_sums, backfill_durable_accounting,
    backfill_durable_step, enqueue_storage_backfill, enqueue_storage_reconcile, new_backfill_job,
    new_reconcile_job,
};
pub use scan::{
    DURABLE_MISSING_SAMPLE_CAP, RECONCILE_SCAN_CAP, ReconcileReport, collect_live_blob_keys,
    reconcile_media, reconcile_media_paged, reconcile_media_with_cap,
};
