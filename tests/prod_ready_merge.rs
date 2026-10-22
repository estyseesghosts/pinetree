//! Parent merge tests for `docs/prod-ready-2.md` shared-file wiring.
//!
//! Covers what the workstream test files cannot: the configured
//! `backup.max_bundle_bytes` cap reaching the bundle/restore paths through
//! [`BackupContext`], retention rotation keeping the newest completed bundle
//! while pruning older ones, and the DNS pool configuration entry point.
//! Hermetic (temp dirs only).

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use pinetree::application::ports::{AccountStore, CreateAccountInput};
use pinetree::backup::{
    BackupContext, BackupState, backup_status, list_backup_statuses, policy_for_context,
    prune_retained_backups, request_backup, run_backup_blocking,
};
use pinetree::domain::id::ManualClock;
use pinetree::persistence::redb::RedbStore;

static SEQ: AtomicU64 = AtomicU64::new(0);

fn harness(
    tag: &str,
    ctx: impl FnOnce(BackupContext) -> BackupContext,
) -> (Arc<RedbStore>, BackupContext, PathBuf) {
    let n = SEQ.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("pinetree-merge-{tag}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let clock = Arc::new(ManualClock::new(1_760_400_000_000i64 + n as i64));
    let store = RedbStore::open_with_default_ids(dir.join("store.redb"), clock).unwrap();
    let durable_pool = dir.join("blobs_durable");
    let cache_pool = dir.join("blobs_cache");
    std::fs::create_dir_all(durable_pool.join("objects")).unwrap();
    std::fs::create_dir_all(cache_pool.join("objects")).unwrap();
    let backup_dir = dir.join("backups");
    let base = BackupContext {
        store: store.clone(),
        durable_pool,
        cache_pool,
        backup_dir,
        max_bundle_bytes: None,
        retention_max_count: None,
        retention_max_bytes: None,
    };
    let full = ctx(base);
    (store, full, dir)
}

fn seed_account(store: &Arc<RedbStore>, name: &str) {
    store
        .create_account(CreateAccountInput::local(name), None)
        .expect("seed account");
}

fn complete_backup(ctx: &BackupContext, store: &Arc<RedbStore>, name: &str) -> String {
    seed_account(store, name);
    let job = request_backup(ctx).expect("request");
    run_backup_blocking(ctx, &job).expect("run");
    let status = backup_status(ctx, &job).expect("status");
    assert_eq!(
        status.state,
        BackupState::Completed,
        "backup {job} must complete"
    );
    job
}

fn zip_names(ctx: &BackupContext) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(&ctx.backup_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".zip"))
        .collect();
    names.sort();
    names
}

#[test]
fn merge_policy_for_context_honors_configured_cap() {
    let (_, ctx, _dir) = harness("policy", |c| c);
    // Unset = hard ceiling.
    assert_eq!(
        policy_for_context(&ctx).cap(),
        pinetree::backup::HARD_BUNDLE_CEILING_BYTES
    );
    let (_, tight, _dir) = harness("policy-tight", |mut c| {
        c.max_bundle_bytes = Some(1 << 20);
        c
    });
    assert_eq!(policy_for_context(&tight).cap(), 1 << 20);
    // Invalid direct constructions fail closed to the ceiling, never open.
    let (_, zero, _dir) = harness("policy-zero", |mut c| {
        c.max_bundle_bytes = Some(0);
        c
    });
    assert_eq!(
        policy_for_context(&zero).cap(),
        pinetree::backup::HARD_BUNDLE_CEILING_BYTES
    );
    let (_, over, _dir) = harness("policy-over", |mut c| {
        c.max_bundle_bytes = Some(pinetree::backup::HARD_BUNDLE_CEILING_BYTES + 1);
        c
    });
    assert_eq!(
        policy_for_context(&over).cap(),
        pinetree::backup::HARD_BUNDLE_CEILING_BYTES
    );
}

#[test]
fn merge_tiny_configured_cap_fails_backup_instead_of_publishing() {
    let (store, ctx, _dir) = harness("tiny-cap", |mut c| {
        c.max_bundle_bytes = Some(1);
        c
    });
    seed_account(&store, "tiny");
    let job = request_backup(&ctx).expect("request");
    let err = run_backup_blocking(&ctx, &job).expect_err("1-byte cap must fail");
    assert!(
        err.to_string().contains("exceeds") || err.to_string().contains("cap"),
        "unexpected error: {err}"
    );
    let status = backup_status(&ctx, &job).expect("status");
    assert_eq!(status.state, BackupState::Failed);
    assert!(
        zip_names(&ctx).is_empty(),
        "failed backup publishes nothing"
    );
}

#[test]
fn merge_retention_count_keeps_newest_two() {
    let (store, ctx, _dir) = harness("retention-count", |mut c| {
        c.retention_max_count = Some(2);
        c
    });
    let first = complete_backup(&ctx, &store, "alice");
    let _second = complete_backup(&ctx, &store, "bob");
    let third = complete_backup(&ctx, &store, "carol");
    // Three completions with keep=2: oldest pruned, newest two kept.
    let zips = zip_names(&ctx);
    assert_eq!(zips.len(), 2, "keep=2 must leave two bundles: {zips:?}");
    let statuses = list_backup_statuses(&ctx.backup_dir);
    let completed: Vec<_> = statuses
        .iter()
        .filter(|s| s.state == BackupState::Completed)
        .collect();
    assert_eq!(completed.len(), 2);
    assert!(
        completed.iter().any(|s| s.job_id == third),
        "newest completed bundle is never pruned"
    );
    assert!(
        completed.iter().all(|s| s.job_id != first),
        "oldest completed bundle pruned past keep=2"
    );
}

#[test]
fn merge_retention_never_deletes_newest_over_budget() {
    let (store, ctx, _dir) = harness("retention-single", |mut c| {
        c.retention_max_count = Some(1);
        c.retention_max_bytes = Some(1);
        c
    });
    let only = complete_backup(&ctx, &store, "solo");
    // A single completed bundle survives even a 1-byte budget.
    assert_eq!(zip_names(&ctx).len(), 1);
    let statuses = list_backup_statuses(&ctx.backup_dir);
    assert!(statuses.iter().any(|s| s.job_id == only));
    // Explicit prune with no bounds configured keeps everything.
    let (store2, ctx2, _dir2) = harness("retention-off", |c| c);
    complete_backup(&ctx2, &store2, "keepa");
    complete_backup(&ctx2, &store2, "keepb");
    let report = prune_retained_backups(&ctx2);
    assert_eq!(report.bundles_removed, 0);
    assert_eq!(zip_names(&ctx2).len(), 2);
}

#[test]
fn merge_dns_pool_configure_entry_point() {
    // The binary calls this once at startup from the validated config; a
    // second pre-init call still applies (last wins) without panicking.
    // Behavior (thread bound, fail-fast saturation) is pinned by
    // `tests/dns_bounded_p1.rs`.
    let _ = pinetree::federation::retry::configure_dns_pool(32);
}
