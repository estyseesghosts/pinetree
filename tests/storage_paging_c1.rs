//! Batch C (`docs/prod-ready.md` §4) paging/backfill/reconcile regressions
//! against the REAL `RedbStore`.
//!
//! Covers C2 (pagination over 0/1/99/100/101/499/500/501 + several thousand
//! rows: exact-once, no skip/repeat at boundaries, partial-final-page and
//! exact-multiple termination, exclusive cursors, insert/delete-during-scan
//! semantics) and C5 (backfill exactness past 1,000 rows, multi-page
//! reconcile reaching the oldest row, sweep refusal on incomplete key sets,
//! crash windows with restart+reconcile invariants).
//!
//! Allowed scan semantics (documented, asserted in
//! `paging_insert_delete_during_scan`): pages are separate reads with no
//! snapshot. Rows inserted after the scan starts carry larger ids and are
//! never revisited by an in-flight newest-first walk; rows deleted
//! mid-walk are skipped. Destructive operations must therefore never run on
//! a partial walk — the orphan sweep refuses incomplete key sets
//! (`sweep_refuses_incomplete_key_list`).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    AccountStore, ActivityDedup, BackfillDurablePayload, BlobStore, CacheStats, CreateAccountInput,
    InsertMediaInput, JobKind, JobQueue, MediaClass, MediaStore, NewJob, NoopStreamingSink,
    PortError, RemoteCacheRecord, TimelineCursor,
};
use pinetree::config::RemoteCacheMode;
use pinetree::domain::id::{Clock, ManualClock};
use pinetree::domain::media::{MediaMeta, MediaProperties};
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::federation::retry::DestinationTracker;
use pinetree::jobs::worker::JobWorker;
use pinetree::media::ingest::{md5_hex, sha256_hex};
use pinetree::media::reconciler;
use pinetree::media::storage::FsBlobStore;
use pinetree::media::transcode::TranscodeConfig;
use pinetree::persistence::redb::{MAX_MEDIA_PAGE_ROWS, RedbStore};
use pinetree::services::email::{SmtpConfig, SmtpEmailSender};

const BASE_MS: i64 = 1_760_400_000_000;
const MAX_FILE: u64 = 10_000_000;
static C1_SEQ: AtomicU64 = AtomicU64::new(0);

struct Harness {
    store: Arc<RedbStore>,
    services: Arc<AppServices>,
    durable: Arc<FsBlobStore>,
    cache: Arc<FsBlobStore>,
    dir: std::path::PathBuf,
    worker: JobWorker,
}

fn harness(tag: &str) -> Harness {
    let n = C1_SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!(
        "pinetree-paging-c1-{tag}-{}-{n}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let clock: Arc<ManualClock> = Arc::new(ManualClock::new(BASE_MS));
    let clock_port: Arc<dyn Clock> = clock.clone();
    let store =
        RedbStore::open_with_default_ids(dir.join("store.redb"), clock_port.clone()).unwrap();
    let (services, durable, cache, worker) = build_services(&store, &clock_port, &dir);
    Harness {
        store,
        services,
        durable,
        cache,
        dir,
        worker,
    }
}

fn build_services(
    store: &Arc<RedbStore>,
    clock_port: &Arc<dyn Clock>,
    dir: &std::path::Path,
) -> (
    Arc<AppServices>,
    Arc<FsBlobStore>,
    Arc<FsBlobStore>,
    JobWorker,
) {
    let durable = Arc::new(FsBlobStore::new(dir.join("blobs_durable")).unwrap());
    let cache = Arc::new(FsBlobStore::new(dir.join("blobs_cache")).unwrap());
    let services = Arc::new(AppServices {
        clock: clock_port.clone(),
        ids: store.ids().clone(),
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
        media: store.clone(),
        blobs: durable.clone(),
        cache_blobs: cache.clone(),
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
        max_file_size: MAX_FILE,
        preserved_usernames: Vec::new(),
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        note_draft_limit: pinetree::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
        scheduled_note_limit: pinetree::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
        pin_limit: pinetree::domain::pinning::DEFAULT_PIN_LIMIT,
        default_like: None,
        drive_capacity_per_user_bytes: 1 << 30,
        remote_cache_max_bytes: 512 << 20,
        remote_cache_avatar_bytes: 128 << 20,
        remote_cache_preview_bytes: 256 << 20,
        remote_cache_full_bytes: 128 << 20,
        remote_cache_mode: RemoteCacheMode::OnDemand,
        remote_cache_avatar_ttl_days: 30,
        remote_cache_preview_ttl_days: 7,
        remote_cache_full_ttl_days: 3,
        remote_cache_warming_enabled: false,
    });
    let worker = JobWorker {
        clock: clock_port.clone(),
        ids: store.ids().clone(),
        jobs: store.clone(),
        notifications: store.clone(),
        streaming: Arc::new(NoopStreamingSink),
        services: services.clone(),
        delivery_tracker: Arc::new(DestinationTracker::new()),
        lease_duration_ms: 30_000,
        base_backoff_ms: 1_000,
        transcode_config: TranscodeConfig::default(),
        email_sender: Arc::new(SmtpEmailSender::new(SmtpConfig::default())),
    };
    (services, durable, cache, worker)
}

/// Insert one durable (owned) row. `blob_key` is caller-chosen so tests can
/// force sharing; ids are store-generated (aidx, time-ordered).
fn insert_owned(
    store: &RedbStore,
    owner: &str,
    seq: usize,
    size: u64,
    blob_key: &str,
) -> MediaMeta {
    store
        .insert_metadata(InsertMediaInput {
            id: None,
            owner_id: Some(owner.to_string()),
            name: format!("f{seq}.bin"),
            mime: "image/png".to_string(),
            size_bytes: size,
            hash: format!("{seq:032x}"),
            blob_key: blob_key.to_string(),
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive: false,
            is_link: false,
            url: format!("https://pinetree.local/files/{seq}"),
            thumbnail_url: None,
            comment: None,
            source_url: None,
            max_file_size: MAX_FILE,
        })
        .unwrap()
}

/// Insert one cache (owner-less) row with its `(source_url, variant)` index
/// row and accounting, mirroring the production fetch path.
fn insert_cached(h: &Harness, seq: usize, size: u64, with_bytes: bool) -> MediaMeta {
    let bytes = vec![(seq % 251) as u8; size as usize];
    let key = sha256_hex(&bytes);
    if with_bytes {
        h.cache.put(&key, &bytes).unwrap();
    }
    let source_url = format!("https://cdn.example/{seq}.png");
    let meta = h
        .store
        .insert_metadata(InsertMediaInput {
            id: None,
            owner_id: None,
            name: format!("r{seq}.bin"),
            mime: "image/png".to_string(),
            size_bytes: size,
            hash: md5_hex(&bytes),
            blob_key: key.clone(),
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive: false,
            is_link: true,
            url: format!("https://pinetree.local/files/c{seq}"),
            thumbnail_url: None,
            comment: None,
            source_url: Some(source_url.clone()),
            max_file_size: MAX_FILE,
        })
        .unwrap();
    h.store
        .remote_cache_insert(&RemoteCacheRecord {
            source_url,
            variant: "original".to_string(),
            media_id: meta.id.clone(),
            blob_key: key,
            size_bytes: size,
            class: MediaClass::Preview,
            fetched_at_ms: BASE_MS,
            last_access_ms: BASE_MS,
            access_count: 1,
            bytes_absent: false,
        })
        .unwrap();
    h.store
        .cache_accounting_apply(
            size as i64,
            MediaClass::Preview,
            size as i64,
            0,
            1,
            size,
            0,
            0,
        )
        .unwrap();
    meta
}

/// Full newest-first cursor walk at `page_size`: asserts per-page ordering
/// (strictly descending ids), exclusive bounds (every row older than the
/// previous page's last id), and empty-page termination. Returns all rows.
fn walk_all(store: &RedbStore, page_size: usize) -> Vec<MediaMeta> {
    let mut out = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let mut cursor = TimelineCursor::with_limit(page_size);
        cursor.until_id = after.clone();
        let rows: Vec<MediaMeta> = store.list_all_media(&cursor).unwrap();
        if rows.is_empty() {
            break;
        }
        for w in rows.windows(2) {
            assert!(
                w[0].id > w[1].id,
                "page not newest-first: {} !> {}",
                w[0].id,
                w[1].id
            );
        }
        if let Some(a) = &after {
            for m in &rows {
                assert!(
                    m.id.as_str() < a.as_str(),
                    "cursor not exclusive: row {} >= after {a}",
                    m.id
                );
            }
        }
        after = rows.last().map(|m| m.id.clone());
        out.extend(rows);
    }
    out
}

fn id_set(rows: &[MediaMeta]) -> HashSet<String> {
    rows.iter().map(|m| m.id.clone()).collect()
}

fn drain(worker: &JobWorker, max: usize) -> usize {
    let mut n = 0;
    for _ in 0..max {
        if !worker.process_one().unwrap() {
            break;
        }
        n += 1;
    }
    n
}

// --- C2: pagination boundaries over the real adapter -----------------------

#[test]
fn paging_exact_once_across_boundary_sizes() {
    // 0/1/99/100/101/499/500/501 rows at page 100: every row exactly once,
    // newest-first, partial-final-page termination, no skip/repeat at the
    // 100-row (old clamp) boundary.
    for n in [0usize, 1, 99, 100, 101, 499, 500, 501] {
        let h = harness(&format!("bounds-{n}"));
        let mut expected = HashSet::new();
        for i in 0..n {
            let m = insert_owned(
                &h.store,
                "owner",
                i,
                (i % 997 + 1) as u64,
                &format!("{i:064x}"),
            );
            expected.insert(m.id.clone());
        }
        let walked = walk_all(&h.store, 100);
        assert_eq!(walked.len(), n, "size {n}");
        assert_eq!(id_set(&walked), expected, "size {n}: exact-once");
    }
}

#[test]
fn paging_exact_multiple_terminates_and_clamp_is_named() {
    // 1000 rows at page 500 (an exact multiple): the walk needs the
    // trailing empty read — a short page must never end the scan.
    let h = harness("exact-multiple");
    let mut expected = HashSet::new();
    for i in 0..1000 {
        let m = insert_owned(&h.store, "owner", i, 10, &format!("{i:064x}"));
        expected.insert(m.id.clone());
    }
    let walked = walk_all(&h.store, 500);
    assert_eq!(walked.len(), 1000);
    assert_eq!(id_set(&walked), expected);
    // The named maximum is honored: a huge request returns MAX rows, not
    // the old hidden 100-row clamp.
    assert_eq!(MAX_MEDIA_PAGE_ROWS, 500);
    let first: Vec<MediaMeta> = h
        .store
        .list_all_media(&TimelineCursor {
            limit: 10_000,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(first.len(), MAX_MEDIA_PAGE_ROWS);
    // limit == 0 keeps the documented backend default (10).
    let zero = h
        .store
        .list_all_media(&TimelineCursor::with_limit(0))
        .unwrap();
    assert_eq!(zero.len(), 10);
}

#[test]
fn paging_large_mixed_walk_several_thousand() {
    // 3000 rows (durable + cache + shared blob keys) at an odd page size:
    // exact-once across many pages, shared-key rows all present.
    let h = harness("large-walk");
    let mut expected = HashSet::new();
    let shared = "f".repeat(64);
    for i in 0..3000 {
        let m = if i % 10 == 9 {
            insert_owned(&h.store, "owner-b", i, 5, &shared)
        } else if i % 3 == 2 {
            insert_cached(&h, i, 7, false)
        } else {
            insert_owned(&h.store, "owner-a", i, 11, &format!("{i:064x}"))
        };
        expected.insert(m.id.clone());
    }
    let walked = walk_all(&h.store, 137);
    assert_eq!(walked.len(), 3000);
    assert_eq!(id_set(&walked), expected);
    assert_eq!(walked.iter().filter(|m| m.blob_key == shared).count(), 300);
}

#[test]
fn paging_insert_delete_during_scan() {
    // No-snapshot disclosure: appends land ahead of the walk (never
    // revisited); deletes of unvisited rows are skipped without disturbing
    // the rest. Destructive ops must not rely on partial walks.
    let h = harness("churn");
    let mut original = HashSet::new();
    for i in 0..200 {
        let m = insert_owned(&h.store, "owner", i, 3, &format!("{i:064x}"));
        original.insert(m.id.clone());
    }
    let p1: Vec<MediaMeta> = h
        .store
        .list_all_media(&TimelineCursor::with_limit(50))
        .unwrap();
    assert_eq!(p1.len(), 50);
    let after1 = p1.last().unwrap().id.clone();
    // Append 3 newest rows mid-scan.
    let mut appended = HashSet::new();
    for i in 200..203 {
        let m = insert_owned(&h.store, "owner", i, 3, &format!("{i:064x}"));
        appended.insert(m.id.clone());
    }
    // Continue from the cursor: the appended rows are ahead of the walk and
    // must not appear (no revisit, no duplication).
    let mut rest = Vec::new();
    let mut after = Some(after1.clone());
    loop {
        let mut cursor = TimelineCursor::with_limit(50);
        cursor.until_id = after.clone();
        let rows: Vec<MediaMeta> = h.store.list_all_media(&cursor).unwrap();
        if rows.is_empty() {
            break;
        }
        after = rows.last().map(|m| m.id.clone());
        rest.extend(rows);
    }
    assert!(rest.iter().all(|m| !appended.contains(&m.id)));
    assert_eq!(rest.len(), 150, "appends are never revisited");
    // Delete 2 rows from the not-yet-visited suffix, then re-walk it from
    // the same cursor: the walk skips exactly the deleted rows.
    let suffix_ids: HashSet<String> = original
        .iter()
        .filter(|id| id.as_str() < after1.as_str())
        .cloned()
        .collect();
    assert_eq!(suffix_ids.len(), 150);
    let to_delete: Vec<String> = suffix_ids.iter().take(2).cloned().collect();
    for id in &to_delete {
        h.store.delete_metadata(id).unwrap();
    }
    let mut suffix = Vec::new();
    let mut after = Some(after1.clone());
    loop {
        let mut cursor = TimelineCursor::with_limit(50);
        cursor.until_id = after.clone();
        let rows: Vec<MediaMeta> = h.store.list_all_media(&cursor).unwrap();
        if rows.is_empty() {
            break;
        }
        after = rows.last().map(|m| m.id.clone());
        suffix.extend(rows);
    }
    let mut want = suffix_ids;
    for id in &to_delete {
        assert!(want.remove(id));
    }
    assert_eq!(id_set(&suffix), want);
    assert_eq!(suffix.len(), want.len(), "no duplicates after churn");
}

// --- C3/C5: backfill exactness, continuations, crash windows ---------------

/// Zero every counter the backfill owns (simulates a pre-upgrade DB).
fn zero_counters(store: &RedbStore, owners: &[&str], keys: &[String]) {
    for owner in owners {
        store.set_durable_bytes(owner, 0).unwrap();
    }
    for key in keys {
        store.set_blob_refcount(key, 0).unwrap();
    }
}

#[test]
fn backfill_exact_over_thousand_rows_with_shared_keys() {
    // >1000 rows: per-owner durable sums exact, blob refcounts exact
    // including keys shared across owners and cache references.
    let h = harness("backfill-1200");
    let mut keys = Vec::new();
    for i in 0..400 {
        let m = insert_owned(&h.store, "owner-a", i, (i + 1) as u64, &format!("{i:064x}"));
        keys.push(m.blob_key);
    }
    for i in 0..400 {
        let m = insert_owned(
            &h.store,
            "owner-b",
            1000 + i,
            10,
            &format!("{:064x}", 100_000 + i / 4),
        );
        keys.push(m.blob_key);
    }
    let shared_c = "c".repeat(64);
    for i in 0..200 {
        let m = insert_owned(&h.store, "owner-c", 2000 + i, 7, &shared_c);
        keys.push(m.blob_key);
    }
    for i in 0..200 {
        let m = insert_cached(&h, 5000 + i, 9, false);
        keys.push(m.blob_key);
    }
    zero_counters(&h.store, &["owner-a", "owner-b", "owner-c"], &keys);
    let (report, done) = reconciler::backfill_durable_accounting(&h.services, None, 500).unwrap();
    assert!(done.is_none());
    assert_eq!(report.media_scanned, 1200, "{report:?}");
    assert_eq!(h.store.durable_bytes("owner-a").unwrap(), 80_200);
    assert_eq!(h.store.durable_bytes("owner-b").unwrap(), 4_000);
    assert_eq!(h.store.durable_bytes("owner-c").unwrap(), 1_400);
    assert_eq!(h.store.blob_refcount(&shared_c).unwrap(), 200);
    assert_eq!(
        h.store.blob_refcount(&format!("{:064x}", 100_000)).unwrap(),
        4
    );
    // Cache rows count toward refcounts but never toward durable bytes.
    assert_eq!(report.refcounts_updated, 400 + 100 + 1 + 200);
    // Idempotent re-run writes the same sums.
    let (again, _) = reconciler::backfill_durable_accounting(&h.services, None, 500).unwrap();
    assert_eq!(again.media_scanned, 1200);
    assert_eq!(h.store.durable_bytes("owner-a").unwrap(), 80_200);
}

#[test]
fn backfill_worker_chain_never_completes_from_partial_scan() {
    // 650 rows at page 100 with 4 pages/run: the first job run must NOT
    // write counters (it holds a prefix); the chain completes on run 2.
    let h = harness("backfill-chain");
    let owners = ["o0", "o1", "o2", "o3", "o4"];
    let mut keys = Vec::new();
    for i in 0..650 {
        let m = insert_owned(
            &h.store,
            owners[i % 5],
            i,
            ((i % 50) + 1) as u64,
            &format!("{i:064x}"),
        );
        keys.push(m.blob_key);
    }
    zero_counters(&h.store, &owners, &keys);
    h.store
        .enqueue_jobs(vec![
            NewJob::new_json(
                JobKind::BackfillDurableAccounting,
                &BackfillDurablePayload {
                    after: None,
                    batch_size: Some(100),
                },
                "chain-backfill-1",
                BASE_MS,
            )
            .unwrap(),
        ])
        .unwrap();
    // Run 1 scans 400 rows and enqueues a durable continuation.
    assert!(h.worker.process_one().unwrap());
    assert_eq!(h.store.pending_count().unwrap(), 1);
    for owner in owners {
        assert_eq!(
            h.store.durable_bytes(owner).unwrap(),
            0,
            "no write from a partial scan"
        );
    }
    // Run 2 walks the rest, persists exact sums, and drains the chain.
    assert!(h.worker.process_one().unwrap());
    assert_eq!(h.store.pending_count().unwrap(), 0);
    assert!(!h.worker.process_one().unwrap());
    let mut want: HashMap<&str, u64> = HashMap::new();
    for i in 0..650 {
        *want.entry(owners[i % 5]).or_insert(0) += ((i % 50) + 1) as u64;
    }
    for (owner, sum) in want {
        assert_eq!(h.store.durable_bytes(owner).unwrap(), sum);
    }
    assert_eq!(h.store.blob_refcount(&format!("{:064x}", 42)).unwrap(), 1);
}

#[test]
fn backfill_interrupted_run_writes_nothing_then_converges() {
    // Crash window scan→backfill-state: a run that never reaches the final
    // page must leave every counter untouched; resuming converges exactly.
    let h = harness("backfill-crash");
    let mut keys = Vec::new();
    for i in 0..300 {
        let m = insert_owned(&h.store, "owner", i, 8, &format!("{i:064x}"));
        keys.push(m.blob_key);
    }
    zero_counters(&h.store, &["owner"], &keys);
    // One bounded step (1 page of 100): yields a continuation, writes nil.
    let step = reconciler::backfill_durable_step(
        &h.services,
        reconciler::BackfillStaged::default(),
        None,
        100,
        1,
    )
    .unwrap();
    let (mut staged, mut after) = match step {
        reconciler::BackfillStep::Continue { staged, next_after } => (staged, Some(next_after)),
        reconciler::BackfillStep::Complete { .. } => panic!("300 rows cannot complete in 1 page"),
    };
    assert_eq!(staged.scanned, 100);
    assert_eq!(h.store.durable_bytes("owner").unwrap(), 0);
    // "Crash": drop staged state and counters stay zero (nothing partial).
    // Resume from the durable cursor to completion; still nothing written
    // until the final page.
    loop {
        match reconciler::backfill_durable_step(&h.services, staged, after, 100, 10).unwrap() {
            reconciler::BackfillStep::Continue {
                staged: next,
                next_after,
            } => {
                assert_eq!(h.store.durable_bytes("owner").unwrap(), 0);
                staged = next;
                after = Some(next_after);
            }
            reconciler::BackfillStep::Complete {
                report,
                staged: sums,
            } => {
                assert_eq!(report.media_scanned, 300);
                reconciler::apply_backfill_sums(&h.services, &sums).unwrap();
                break;
            }
        }
    }
    assert_eq!(h.store.durable_bytes("owner").unwrap(), 2_400);
    assert_eq!(h.store.blob_refcount(&format!("{:064x}", 7)).unwrap(), 1);
}

#[test]
fn backfill_rejects_mid_table_start() {
    // Suffix-only sums must never overwrite global counters: a mid-table
    // start without staged state is a loud error, not a partial write.
    let h = harness("backfill-guard");
    insert_owned(&h.store, "owner", 0, 1, &format!("{:064x}", 0));
    let err = reconciler::backfill_durable_accounting(
        &h.services,
        Some("mid00000000000001".to_string()),
        500,
    )
    .unwrap_err();
    assert!(matches!(err, PortError::Validation(_)), "{err:?}");
}

// --- C4/C5: reconcile paging, repair vs detection, sweep gate --------------

#[test]
fn reconcile_repairs_only_from_newest_complete_walk() {
    // 60 rows, zeroed counters: a single from-newest run is complete and
    // repairs exactly; nothing is reported "healed" without being written.
    let h = harness("reconcile-repair");
    for i in 0..60 {
        let bytes = vec![i as u8; 16];
        let key = sha256_hex(&bytes);
        h.durable.put(&key, &bytes).unwrap();
        h.store
            .insert_metadata(InsertMediaInput {
                id: None,
                owner_id: Some(if i % 2 == 0 { "alice" } else { "bob" }.to_string()),
                name: format!("d{i}.bin"),
                mime: "image/png".to_string(),
                size_bytes: 16,
                hash: md5_hex(&bytes),
                blob_key: key,
                blurhash: None,
                properties: MediaProperties::default(),
                is_sensitive: false,
                is_link: false,
                url: format!("https://pinetree.local/files/d{i}"),
                thumbnail_url: None,
                comment: None,
                source_url: None,
                max_file_size: MAX_FILE,
            })
            .unwrap();
    }
    let keys: Vec<String> = walk_all(&h.store, 500)
        .iter()
        .map(|m| m.blob_key.clone())
        .collect();
    zero_counters(&h.store, &["alice", "bob"], &keys);
    let report = reconciler::reconcile_media(&h.services).unwrap();
    assert!(report.complete);
    assert_eq!(report.examined, 60);
    assert_eq!(report.durable_missing, 0, "{report:?}");
    assert!(report.refcounts_repaired > 0);
    assert!(report.durable_repaired > 0);
    assert_eq!(report.refcount_drift_detected, 0);
    assert_eq!(report.durable_drift_detected, 0);
    assert_eq!(h.store.durable_bytes("alice").unwrap(), 30 * 16);
    assert_eq!(h.store.durable_bytes("bob").unwrap(), 30 * 16);
}

#[test]
fn reconcile_chain_reaches_oldest_without_partial_repair() {
    // 60 rows at cap 7: every run makes monotonic forward progress to the
    // oldest row; truncated runs (and the chained suffix runs) detect drift
    // but never overwrite counters.
    let h = harness("reconcile-chain");
    let mut expected_ids = HashSet::new();
    for i in 0..60 {
        let m = insert_owned(&h.store, "owner", i, 4, &format!("{i:064x}"));
        expected_ids.insert(m.id.clone());
    }
    let keys: Vec<String> = expected_ids.iter().cloned().collect();
    zero_counters(&h.store, &["owner"], &keys);
    let mut after: Option<String> = None;
    let mut seen = HashSet::new();
    let mut runs = 0;
    loop {
        let (report, next) =
            reconciler::reconcile_media_paged(&h.services, after.as_deref(), 7).unwrap();
        runs += 1;
        assert!(report.examined <= 7, "{report:?}");
        assert_eq!(report.refcounts_repaired, 0, "no partial repair");
        assert_eq!(report.durable_repaired, 0, "no partial repair");
        assert_eq!(
            h.store.durable_bytes("owner").unwrap(),
            0,
            "no partial write"
        );
        match next {
            Some(cursor) => {
                assert!(!report.complete);
                after = Some(cursor);
            }
            None => {
                assert!(report.complete);
                break;
            }
        }
        assert!(runs < 30, "must converge");
    }
    // The chain above only asserted progress; re-walk to collect coverage.
    let mut after: Option<String> = None;
    loop {
        let mut cursor = TimelineCursor::with_limit(7);
        cursor.until_id = after.clone();
        let rows: Vec<MediaMeta> = h.store.list_all_media(&cursor).unwrap();
        if rows.is_empty() {
            break;
        }
        for m in &rows {
            seen.insert(m.id.clone());
        }
        after = rows.last().map(|m| m.id.clone());
    }
    assert_eq!(seen, expected_ids);
    assert!(
        runs >= 9,
        "7-row caps over 60 rows need many runs, got {runs}"
    );
    // First chained run saw drift (counters were zero) without repairing.
    let (first, _) = reconciler::reconcile_media_paged(&h.services, None, 7).unwrap();
    assert!(!first.complete);
    assert!(
        first.refcount_drift_detected > 0 || first.durable_drift_detected > 0,
        "{first:?}"
    );
}

#[test]
fn sweep_refuses_incomplete_key_list() {
    // A destructive sweep must never run on a partial live-key set: the
    // checked entry point errors and deletes nothing; the complete set
    // sweeps exactly the orphans. Pools stay separate (durable live keys
    // never authorize cache deletes and vice versa).
    let h = harness("sweep-gate");
    let live_bytes = b"live-payload";
    let live_key = sha256_hex(live_bytes);
    h.cache.put(&live_key, live_bytes).unwrap();
    let orphan_bytes = b"orphan-payload";
    let orphan_key = sha256_hex(orphan_bytes);
    h.cache.put(&orphan_key, orphan_bytes).unwrap();
    let objects = h.dir.join("blobs_cache").join("objects");
    let mut partial = HashSet::new();
    partial.insert(live_key.clone());
    // Refusal first: nothing may be deleted on an incomplete set.
    let err = reconciler::sweep_orphan_blobs_checked(&objects, &partial, false).unwrap_err();
    assert!(matches!(err, PortError::Validation(_)), "{err:?}");
    assert!(h.cache.get(&live_key).unwrap().is_some());
    assert!(h.cache.get(&orphan_key).unwrap().is_some());
    // Complete set (as produced by a full collect walk) sweeps orphans.
    let (durable_keys, cache_keys) = reconciler::collect_live_blob_keys(&h.services).unwrap();
    assert!(durable_keys.is_empty());
    assert!(cache_keys.is_empty());
    let mut live = HashSet::new();
    live.insert(live_key.clone());
    let removed = reconciler::sweep_orphan_blobs_checked(&objects, &live, true).unwrap();
    assert_eq!(removed, 1);
    assert_eq!(h.cache.get(&live_key).unwrap().unwrap(), live_bytes);
    assert!(h.cache.get(&orphan_key).unwrap().is_none());
}

// --- C5: crash windows ------------------------------------------------------

fn remove_len_sidecar(objects_dir: &std::path::Path, key: &str) {
    let mut found = false;
    for shard in std::fs::read_dir(objects_dir).unwrap() {
        let target = shard.unwrap().path().join(format!("{key}.len"));
        if target.exists() {
            std::fs::remove_file(&target).unwrap();
            found = true;
        }
    }
    assert!(found, "sidecar must exist before removal");
}

#[test]
fn crash_torn_sidecar_durable_is_reported_never_deleted() {
    // Crash between blob write and sidecar write: `byte_len` is gone and
    // this backend's `get` also refuses the bytes. The durable row must
    // stay visible and be explicitly reported — never silently deleted.
    let h = harness("torn-sidecar");
    let bytes = b"durable-torn-bytes";
    let key = sha256_hex(bytes);
    h.durable.put(&key, bytes).unwrap();
    let meta = insert_owned(&h.store, "owner", 0, bytes.len() as u64, &key);
    assert_eq!(h.durable.byte_len(&key).unwrap(), Some(bytes.len() as u64));
    remove_len_sidecar(&h.dir.join("blobs_durable").join("objects"), &key);
    assert_eq!(h.durable.byte_len(&key).unwrap(), None);
    let report = reconciler::reconcile_media(&h.services).unwrap();
    assert_eq!(report.durable_missing, 1, "{report:?}");
    assert_eq!(report.durable_missing_sample, vec![meta.id.clone()]);
    // Invariant: present-or-reported. The row and the underlying bytes file
    // both survive for operator repair.
    assert!(h.store.get(&meta.id).unwrap().is_some());
    assert!(h.dir.join("blobs_durable").join("objects").exists());
}

#[test]
fn crash_bytes_without_metadata_are_swept_after_full_collect() {
    // Crash between pool write and metadata insert: unreferenced bytes are
    // orphans once a COMPLETE key set proves no row references them.
    let h = harness("orphan-bytes");
    let live_bytes = b"live-row-bytes";
    let live_key = sha256_hex(live_bytes);
    h.durable.put(&live_key, live_bytes).unwrap();
    insert_owned(&h.store, "owner", 0, live_bytes.len() as u64, &live_key);
    let orphan_bytes = b"staged-never-committed";
    let orphan_key = sha256_hex(orphan_bytes);
    h.durable.put(&orphan_key, orphan_bytes).unwrap();
    let (durable_keys, _) = reconciler::collect_live_blob_keys(&h.services).unwrap();
    assert!(durable_keys.contains(&live_key));
    assert!(!durable_keys.contains(&orphan_key));
    let removed = reconciler::sweep_orphan_blobs_checked(
        &h.dir.join("blobs_durable").join("objects"),
        &durable_keys,
        true,
    )
    .unwrap();
    assert_eq!(removed, 1);
    assert_eq!(h.durable.get(&live_key).unwrap().unwrap(), live_bytes);
    assert!(h.durable.get(&orphan_key).unwrap().is_none());
}

#[test]
fn crash_evict_leaves_cache_refetchable_without_phantom_bytes() {
    // Crash between cache-evict metadata change and physical delete (bytes
    // gone, row + index present): the row stays with its `source_url` for
    // re-fetch, bytes are charged off exactly once, and re-runs never
    // double-subtract.
    let h = harness("evict-torn");
    let meta = insert_cached(&h, 1, 64, true);
    assert_eq!(
        h.store.cache_stats().unwrap().total_bytes,
        64,
        "fixture charges the bytes"
    );
    h.cache.delete(&meta.blob_key).unwrap();
    let before = h.store.cache_stats().unwrap().total_bytes;
    let report = reconciler::reconcile_media(&h.services).unwrap();
    assert_eq!(report.missing_marked_evicted, 1, "{report:?}");
    // Row + re-fetch index survive; bytes are gone from accounting.
    let again_meta = h.store.get(&meta.id).unwrap().expect("row kept");
    assert_eq!(again_meta.source_url, meta.source_url);
    assert!(
        h.store
            .remote_cache_lookup(meta.source_url.as_deref().unwrap(), "original")
            .unwrap()
            .is_some(),
        "index kept for re-fetch"
    );
    assert_eq!(h.store.cache_stats().unwrap().total_bytes, before - 64);
    // Second run: no double-subtract, nothing newly "healed".
    let second = reconciler::reconcile_media(&h.services).unwrap();
    assert_eq!(second.missing_marked_evicted, 0, "{second:?}");
    assert_eq!(
        h.store.cache_stats().unwrap().total_bytes,
        before - 64,
        "no phantom double-subtract"
    );
    assert!(h.store.get(&meta.id).unwrap().is_some());
}

#[test]
fn crash_durable_missing_is_reported_with_sample() {
    // Crash between metadata/refcount commit and blob delete (or a lost
    // blob file): durable media is either present and accounted, or
    // explicitly reported missing — never silently dropped.
    let h = harness("durable-missing");
    let bytes = b"precious-upload";
    let key = sha256_hex(bytes);
    h.durable.put(&key, bytes).unwrap();
    let meta = insert_owned(&h.store, "owner", 0, bytes.len() as u64, &key);
    h.durable.delete(&key).unwrap();
    let report = reconciler::reconcile_media(&h.services).unwrap();
    assert_eq!(report.durable_missing, 1, "{report:?}");
    assert_eq!(report.durable_missing_sample, vec![meta.id.clone()]);
    assert!(h.store.get(&meta.id).unwrap().is_some(), "never deleted");
}

#[test]
fn reopen_skips_completed_migration_and_backfill_repairs_drift() {
    // Crash window scan→marker: once the at-open migration marker is set,
    // reopening never rebuilds (post-migration drift needs the explicit
    // repair job, which then converges exactly).
    let dir = std::env::temp_dir().join(format!(
        "pinetree-paging-c1-reopen-{}-{}",
        std::process::id(),
        C1_SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(BASE_MS));
    {
        let store =
            RedbStore::open_with_default_ids(dir.join("store.redb"), clock.clone()).unwrap();
        for i in 0..50 {
            insert_owned(&store, "owner", i, 6, &format!("{i:064x}"));
        }
        assert_eq!(store.durable_bytes("owner").unwrap(), 300);
        // Post-migration drift (crashed delete, bit-rot, whatever).
        store.set_durable_bytes("owner", 7).unwrap();
    }
    // Reopen = restart: the completed migration does not touch counters.
    let store = RedbStore::open_with_default_ids(dir.join("store.redb"), clock.clone()).unwrap();
    assert_eq!(
        store.durable_bytes("owner").unwrap(),
        7,
        "marker-gated open preserves drift for the repair job"
    );
    // The explicit repair job converges exactly on the reopened DB.
    let (services, _, _, _) = build_services(&store, &clock, &dir);
    let (report, done) = reconciler::backfill_durable_accounting(&services, None, 500).unwrap();
    assert!(done.is_none());
    assert_eq!(report.media_scanned, 50, "{report:?}");
    assert_eq!(store.durable_bytes("owner").unwrap(), 300);
}

#[test]
fn maintenance_triggers_enqueue_drainable_jobs() {
    // Production triggers: the boot reconcile job and the explicit backfill
    // job enqueue through the helpers and drain through the real worker.
    let h = harness("triggers");
    for i in 0..10 {
        insert_owned(&h.store, "owner", i, 2, &format!("{i:064x}"));
    }
    reconciler::enqueue_storage_reconcile(h.services.jobs.as_ref(), BASE_MS).unwrap();
    reconciler::enqueue_storage_backfill(h.services.jobs.as_ref(), BASE_MS).unwrap();
    assert_eq!(h.store.pending_count().unwrap(), 2);
    // Keys embed the timestamp (no silent dedup of repeat boots); every
    // enqueued job is idempotent, so stacking is safe. (`pending_count`
    // counts due jobs under the frozen test clock, so the repeat uses the
    // same timestamp.)
    reconciler::enqueue_storage_reconcile(h.services.jobs.as_ref(), BASE_MS).unwrap();
    assert_eq!(h.store.pending_count().unwrap(), 3);
    assert_eq!(drain(&h.worker, 8), 3);
    assert_eq!(h.store.pending_count().unwrap(), 0);
    assert_eq!(h.store.durable_bytes("owner").unwrap(), 20);
}

#[test]
fn backfill_worker_uses_legacy_contract_payloads() {
    // Payloads written by the frozen contract shape (no staged state) still
    // drive full, exact rebuilds through the worker.
    let h = harness("legacy-payload");
    for i in 0..30 {
        insert_owned(&h.store, "owner", i, 3, &format!("{i:064x}"));
    }
    let keys: Vec<String> = walk_all(&h.store, 500)
        .iter()
        .map(|m| m.blob_key.clone())
        .collect();
    zero_counters(&h.store, &["owner"], &keys);
    h.store
        .enqueue_jobs(vec![
            NewJob::new_json(
                JobKind::BackfillDurableAccounting,
                &BackfillDurablePayload {
                    after: None,
                    batch_size: None,
                },
                "legacy-backfill-1",
                BASE_MS,
            )
            .unwrap(),
        ])
        .unwrap();
    assert_eq!(drain(&h.worker, 4), 1);
    assert_eq!(h.store.durable_bytes("owner").unwrap(), 90);
}

#[test]
fn reconcile_account_surfaces_owner_setup() {
    // Smoke: the harness account path used by other suites still works
    // alongside media paging (guards fixture drift).
    let h = harness("account-smoke");
    let account = h
        .store
        .create_account(CreateAccountInput::local("c1_owner"), None)
        .unwrap();
    insert_owned(&h.store, &account.id, 0, 5, &format!("{:064x}", 1));
    assert_eq!(h.store.durable_bytes(&account.id).unwrap(), 5);
    let walked = walk_all(&h.store, 100);
    assert_eq!(walked.len(), 1);
    let stats = h.store.cache_stats().unwrap();
    assert_eq!(
        stats,
        CacheStats::default(),
        "durable-only traffic leaves cache accounting at zero"
    );
}
