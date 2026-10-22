use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pinetree::api::misskey::auth::hash_password;
use pinetree::api::misskey::response::{RateLimiter, UploadSlots};
use pinetree::api::misskey::{ApiState, build_router};
use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    AccountStore, ActivityDedup, AuthStore, BlobStore, BlockStore, FollowStore, JobQueue,
    MediaStore, MuteStore, NoteStore, NotificationStore, PollStore, ReactionStore, StreamingSink,
};
use pinetree::application::ports::{Clock, IdGenerator};
use pinetree::config::{Config, DeploymentMode, RemoteCacheMode};
use pinetree::domain::id::{AidxGenerator, SystemClock};
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::federation::retry::DestinationTracker;
use pinetree::jobs::worker::JobWorker;
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;
use pinetree::services::email::{EmailSender, SmtpEmailSender};
use pinetree::streaming::hub::hub_and_sink;

pub(crate) fn build_services(
    cfg: &Config,
) -> Result<
    (
        Arc<AppServices>,
        pinetree::streaming::hub::Hub,
        Arc<RedbStore>,
    ),
    String,
> {
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
    let store = RedbStore::open(
        cfg.data_dir.join("pinetree.redb"),
        clock.clone(),
        ids.clone(),
    )
    .map_err(|err| format!("open redb store: {err}"))?;
    store
        .set_cache_cap(cfg.remote_cache_max_bytes)
        .map_err(|err| format!("init cache cap: {err}"))?;
    // Storage overhaul: durable local uploads live in `blobs_durable/`
    // (never evicted), remote fetches in `blobs_cache/` (LRU+TTL, capped).
    // Both roots are config-overridable (`durable_dir` / `cache_dir`) and
    // must be distinct (enforced by `Config::validate`).
    // A pre-overhaul `blobs/` directory is migrated once via rename.
    let legacy_blobs = cfg.data_dir.join("blobs");
    let durable_dir = cfg.durable_pool_dir();
    if !durable_dir.exists() && legacy_blobs.exists() {
        std::fs::rename(&legacy_blobs, &durable_dir)
            .map_err(|err| format!("migrate blobs/ to {}: {err}", durable_dir.display()))?;
    }
    let cache_dir = cfg.cache_pool_dir();
    let blobs: Arc<dyn BlobStore> =
        Arc::new(FsBlobStore::new(&durable_dir).map_err(|err| format!("init blob store: {err}"))?);
    let cache_blobs: Arc<dyn BlobStore> = Arc::new(
        FsBlobStore::new(&cache_dir).map_err(|err| format!("init cache blob store: {err}"))?,
    );
    let streaming: Arc<dyn StreamingSink>;
    let hub: pinetree::streaming::hub::Hub;
    (hub, streaming) = hub_and_sink(1024);
    let services = Arc::new(AppServices {
        clock: clock.clone(),
        ids: ids.clone(),
        accounts: store.clone() as Arc<dyn AccountStore>,
        notes: store.clone() as Arc<dyn NoteStore>,
        follows: store.clone() as Arc<dyn FollowStore>,
        blocks: store.clone() as Arc<dyn BlockStore>,
        mutes: store.clone() as Arc<dyn MuteStore>,
        reactions: store.clone() as Arc<dyn ReactionStore>,
        favorites: store.clone(),
        emojis: store.clone(),
        antennas: store.clone(),
        antenna_notes: store.clone(),
        polls: store.clone() as Arc<dyn PollStore>,
        jobs: store.clone() as Arc<dyn JobQueue>,
        media: store.clone() as Arc<dyn MediaStore>,
        blobs,
        cache_blobs,
        notifications: store.clone() as Arc<dyn NotificationStore>,
        auth: store.clone() as Arc<dyn AuthStore>,
        streaming,
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
        federation_http: SafeHttpClient::new(SafeHttpConfig::default()),
        public_url: cfg.normalized_public_url(),
        max_file_size: cfg.max_file_size,
        preserved_usernames: cfg.preserved_usernames.clone(),
        prohibited_words: cfg.prohibited_words.clone(),
        sensitive_words: cfg.sensitive_words.clone(),
        note_draft_limit: cfg.note_draft_limit,
        scheduled_note_limit: cfg.scheduled_note_limit,
        pin_limit: cfg.pin_limit,
        default_like: cfg.default_like.clone(),
        drive_capacity_per_user_bytes: cfg.drive_capacity_per_user_bytes,
        remote_cache_max_bytes: cfg.remote_cache_max_bytes,
        remote_cache_avatar_bytes: cfg.remote_cache_avatar_bytes,
        remote_cache_preview_bytes: cfg.remote_cache_preview_bytes,
        remote_cache_full_bytes: cfg.remote_cache_full_bytes,
        remote_cache_mode: cfg.remote_cache_mode,
        remote_cache_avatar_ttl_days: cfg.remote_cache_avatar_ttl_days,
        remote_cache_preview_ttl_days: cfg.remote_cache_preview_ttl_days,
        remote_cache_full_ttl_days: cfg.remote_cache_full_ttl_days,
        remote_cache_warming_enabled: cfg.remote_cache_warming_enabled,
    });
    // Crash-heal: a previous process may have died mid-fetch holding an
    // in-flight reservation. Clear it before serving (idempotent).
    pinetree::media::reconciler::reset_in_flight(&services)
        .map_err(|err| format!("reset cache in-flight: {err}"))?;
    Ok((services, hub, store))
}

/// Batch H/F/C maintenance CLI ops. These diverge (never return): they run
/// one store-backed one-shot instead of serving. The store is already open
/// (a running server holds the redb lock, so the open above fails first
/// with the actionable error); the worker and listener never start here.
pub(crate) fn run_maintenance_op(
    op: &str,
    args: &[String],
    services: &Arc<AppServices>,
    backup_ctx: &pinetree::backup::BackupContext,
) -> ! {
    match op {
        // Batch F: queue one backup and build it synchronously, then print
        // the final bundle path. Same engine as the admin route + drain.
        "backup" => {
            if !args.is_empty() {
                eprintln!("usage: backup (no arguments)");
                std::process::exit(2);
            }
            let job_id = pinetree::backup::request_backup(backup_ctx).unwrap_or_else(|err| {
                eprintln!("backup request failed: {err}");
                std::process::exit(1);
            });
            match pinetree::backup::run_backup_blocking(backup_ctx, &job_id) {
                Ok(path) => {
                    println!("backup {} complete: {}", job_id, path.display());
                    std::process::exit(0);
                }
                Err(err) => {
                    eprintln!("backup {job_id} failed: {err}");
                    std::process::exit(1);
                }
            }
        }
        // Batch F: read durable backup statuses (`--id=<job>` for one).
        // Offline-safe: status files only, no store access.
        "backup-status" => {
            let mut id: Option<String> = None;
            for arg in args {
                if let Some(value) = arg.strip_prefix("--id=") {
                    id = Some(value.to_string());
                } else {
                    eprintln!("usage: backup-status [--id=<backup-job-id>]");
                    std::process::exit(2);
                }
            }
            match id {
                Some(job_id) => match pinetree::backup::backup_status(backup_ctx, &job_id) {
                    Some(status) => {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&status)
                                .unwrap_or_else(|_| format!("{status:?}"))
                        );
                        std::process::exit(0);
                    }
                    None => {
                        eprintln!("unknown backup job: {job_id}");
                        std::process::exit(1);
                    }
                },
                None => {
                    let statuses = pinetree::backup::list_backup_statuses(&backup_ctx.backup_dir);
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&statuses)
                            .unwrap_or_else(|_| "[]".to_string())
                    );
                    std::process::exit(0);
                }
            }
        }
        // Batch F: validate a bundle and restore it into a FRESH directory
        // (never the live data dir). Operator cutover stays manual; see
        // `docs/prod-ready.md` §7 F5.
        "restore" => {
            let mut bundle: Option<String> = None;
            let mut target_dir: Option<String> = None;
            for arg in args {
                if let Some(value) = arg.strip_prefix("--bundle=") {
                    bundle = Some(value.to_string());
                } else if let Some(value) = arg.strip_prefix("--target-dir=") {
                    target_dir = Some(value.to_string());
                } else {
                    eprintln!("usage: restore --bundle=<file.zip> --target-dir=<fresh-dir>");
                    std::process::exit(2);
                }
            }
            let (Some(bundle), Some(target_dir)) = (bundle, target_dir) else {
                eprintln!("usage: restore --bundle=<file.zip> --target-dir=<fresh-dir>");
                std::process::exit(2);
            };
            match pinetree::backup::restore_bundle_with_policy(
                std::path::Path::new(&bundle),
                std::path::Path::new(&target_dir),
                pinetree::backup::policy_for_context(backup_ctx),
            ) {
                Ok(report) => {
                    println!(
                        "restore complete: dir={} tables={} rows={} media_files={} media_bytes={}",
                        report.restored_dir.display(),
                        report.tables_restored,
                        report.rows_total,
                        report.media_files,
                        report.media_bytes,
                    );
                    std::process::exit(0);
                }
                Err(err) => {
                    eprintln!("restore failed: {err}");
                    std::process::exit(1);
                }
            }
        }
        // Batch C: enqueue one backfill repair job (the worker drains it at
        // next boot/serve). Explicit operator trigger, never automatic.
        // Operator must run this in a QUIET maintenance window (no
        // uploads/deletes/evictions): the at-open migration is the source
        // of truth; this job path is best-effort convergent repair.
        "storage-backfill" => {
            if !args.is_empty() {
                eprintln!("usage: storage-backfill (no arguments)");
                std::process::exit(2);
            }
            if let Err(err) = pinetree::media::reconciler::enqueue_storage_backfill(
                services.jobs.as_ref(),
                services.clock.now_ms(),
            ) {
                eprintln!("storage-backfill enqueue failed: {err}");
                std::process::exit(1);
            }
            println!(
                "storage backfill enqueued (drains on next serve; run only in a quiet maintenance window)"
            );
            std::process::exit(0);
        }
        _ => {
            eprintln!("unknown subcommand: {op}");
            std::process::exit(2);
        }
    }
}
