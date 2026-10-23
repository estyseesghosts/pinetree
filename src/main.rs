use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pinetree::api::misskey::auth::hash_password;
use pinetree::api::misskey::response::{
    RateLimitBounds, RateLimiter, UploadSlotBounds, UploadSlots,
};
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

mod cli;
mod runtime;

fn main() {
    // Storage-overhaul fallout (2026-10-21): the sync `/api` dispatch
    // match needs just over 2 MiB of stack in dev builds, overflowing
    // tokio's default 2 MiB worker/blocking threads on EVERY `/api` call
    // (`meta`, `username/available`, … — `healthz` was unaffected).
    // Diagnosed via `RUST_MIN_STACK` gradient (2 MiB aborts, 4 MiB serves)
    // after `drive/cache/stats` + quota work tipped the match over.
    // 8 MiB leaves 2x headroom; `dispatch_fits_bounded_stack` guards the
    // 4 MiB half. This covers both pools: the blocking pool inherits the
    // same builder stack size.
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(8 << 20)
        .build()
        .expect("build tokio runtime")
        .block_on(async_main());
}

#[allow(clippy::too_many_lines)]
async fn async_main() {
    pinetree::observability::init();

    // Base configuration: the `--config=PATH` JSON file over the compiled
    // defaults (unknown keys, unreadable files and malformed JSON are hard
    // errors naming the path). Every key below is optional in the file;
    // absent keys take the defaults. This loop's flags override the file.
    let mut cfg = cli::config_base();
    // Wave 5: deprecated env-only overrides. They land in the merged config
    // BEFORE `validate()` (same as file keys) and lose to explicit CLI
    // flags below. Prefer the JSON keys named in each message.
    if let Some(v) = std::env::var("PINETREE_ALLOW_PRIVATE_IPS")
        .ok()
        .filter(|v| !v.trim().is_empty())
    {
        eprintln!(
            "deprecated PINETREE_ALLOW_PRIVATE_IPS is set: use JSON federation.allow_private_ips instead"
        );
        cfg.federation.allow_private_ips = v.trim() == "1" || v.trim().eq_ignore_ascii_case("true");
    }
    if std::env::var("PINETREE_DEBUG_FEDERATION")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .is_some()
    {
        eprintln!(
            "deprecated PINETREE_DEBUG_FEDERATION is set: use JSON federation.debug_federation or --debug-federation instead"
        );
        cfg.federation.debug_federation = true;
    }
    if let Some(limit_raw) = std::env::var("PINETREE_REPLY_REPAIR_LIMIT")
        .ok()
        .filter(|v| !v.trim().is_empty())
    {
        eprintln!(
            "deprecated PINETREE_REPLY_REPAIR_LIMIT is set: use JSON federation.reply_repair_limit instead"
        );
        match limit_raw.trim().parse::<usize>() {
            Ok(limit) => cfg.federation.reply_repair_limit = Some(limit),
            Err(_) => {
                eprintln!(
                    "invalid PINETREE_REPLY_REPAIR_LIMIT={limit_raw:?} (want 1..=5000); ignoring"
                );
            }
        }
    }
    let mut positional: Vec<String> = Vec::new();
    let mut debug_federation_flag = false;
    let mut debug_weak_rate_limits_flag = false;
    for arg in std::env::args().skip(1) {
        if arg == "--debug-federation" {
            debug_federation_flag = true;
            cfg.federation.debug_federation = true;
        } else if arg == "--debug-weak-rate-limits" {
            debug_weak_rate_limits_flag = true;
        } else if let Some(path) = arg.strip_prefix("--federation-extra-ca=") {
            // Wave 5: CLI override of the `federation.federation_extra_ca`
            // file value (same merged-then-validated flow as every setting).
            cfg.federation.federation_extra_ca = Some(path.into());
        } else if let Some(flag) = arg.strip_prefix("--allow-private-ips") {
            // Per-invocation override of the JSON
            // `federation.allow_private_ips` (throwaway federation labs
            // only): bare flag means true, `=true|false|1|0` explicit.
            // The webhook delivery path ignores this (always strict).
            cfg.federation.allow_private_ips = match flag {
                "" | "=true" | "=1" => true,
                "=false" | "=0" => false,
                other => {
                    eprintln!(
                        "invalid --allow-private-ips{other} (want bare flag or =true|=false)"
                    );
                    std::process::exit(2);
                }
            };
        } else if let Some(limit) = arg.strip_prefix("--reply-repair-limit=") {
            cfg.federation.reply_repair_limit = Some(limit.parse().ok().unwrap_or_else(|| {
                eprintln!("invalid --reply-repair-limit (want an integer 1..=5000): {limit}");
                std::process::exit(2);
            }));
        } else if arg == "--storage-integrity-check-at-open" {
            cfg.storage_integrity_check_at_open = true;
        } else if arg.starts_with("--config=") {
            // Consumed by `config_base()` (sets the base this loop
            // overrides); ignored here so it never falls through to
            // `positional`.
        } else if let Some(mode) = arg.strip_prefix("--mode=") {
            cfg.mode = match mode {
                "single" => DeploymentMode::Single,
                "cluster" => DeploymentMode::Cluster,
                other => {
                    eprintln!("unknown deployment mode: {other}");
                    std::process::exit(1);
                }
            };
        } else if let Some(url) = arg.strip_prefix("--public-url=") {
            cfg.public_url = url.to_string();
        } else if let Some(dir) = arg.strip_prefix("--data-dir=") {
            cfg.data_dir = dir.into();
        } else if let Some(listen) = arg.strip_prefix("--listen=") {
            cfg.listen = listen.parse().unwrap_or_else(|_| {
                eprintln!("invalid --listen address: {listen}");
                std::process::exit(1);
            });
        } else if let Some(list) = arg.strip_prefix("--trusted-proxies=") {
            cfg.trusted_proxies = match cli::parse_trusted_proxies(list) {
                Ok(proxies) => proxies,
                Err(err) => {
                    eprintln!("{err}");
                    std::process::exit(1);
                }
            };
        } else if let Some(bytes) = arg.strip_prefix("--max-file-size=") {
            cfg.max_file_size = bytes.parse().ok().filter(|n| *n > 0).unwrap_or_else(|| {
                eprintln!("invalid --max-file-size (want an integer > 0): {bytes}");
                std::process::exit(2);
            });
        } else if let Some(list) = arg.strip_prefix("--preserved-usernames=") {
            cfg.preserved_usernames = cli::parse_csv_list(list);
        } else if let Some(list) = arg.strip_prefix("--prohibited-words=") {
            cfg.prohibited_words = cli::parse_csv_list(list);
        } else if let Some(list) = arg.strip_prefix("--sensitive-words=") {
            cfg.sensitive_words = cli::parse_csv_list(list);
        } else if let Some(limit) = arg.strip_prefix("--note-draft-limit=") {
            cfg.note_draft_limit = limit.parse().ok().filter(|n| *n > 0).unwrap_or_else(|| {
                eprintln!("invalid --note-draft-limit (want an integer > 0): {limit}");
                std::process::exit(2);
            });
        } else if let Some(limit) = arg.strip_prefix("--scheduled-note-limit=") {
            cfg.scheduled_note_limit = limit.parse().ok().filter(|n| *n > 0).unwrap_or_else(|| {
                eprintln!("invalid --scheduled-note-limit (want an integer > 0): {limit}");
                std::process::exit(2);
            });
        } else if let Some(limit) = arg.strip_prefix("--pin-limit=") {
            cfg.pin_limit = limit.parse().ok().filter(|n| *n > 0).unwrap_or_else(|| {
                eprintln!("invalid --pin-limit (want an integer > 0): {limit}");
                std::process::exit(2);
            });
        } else if let Some(like) = arg.strip_prefix("--default-like=") {
            if like.chars().count() == 0 || like.chars().count() > 260 {
                eprintln!("invalid --default-like (want 1..=260 chars)");
                std::process::exit(2);
            }
            cfg.default_like = Some(like.to_string());
        } else if let Some(bytes) = arg.strip_prefix("--drive-capacity-per-user-bytes=") {
            cfg.drive_capacity_per_user_bytes =
                bytes.parse().ok().filter(|n| *n > 0).unwrap_or_else(|| {
                    eprintln!(
                        "invalid --drive-capacity-per-user-bytes (want an integer > 0): {bytes}"
                    );
                    std::process::exit(2);
                });
        } else if let Some(bytes) = arg.strip_prefix("--remote-cache-max-bytes=") {
            cfg.remote_cache_max_bytes =
                bytes.parse().ok().filter(|n| *n > 0).unwrap_or_else(|| {
                    eprintln!("invalid --remote-cache-max-bytes (want an integer > 0): {bytes}");
                    std::process::exit(2);
                });
        } else if let Some(bytes) = arg.strip_prefix("--remote-cache-avatar-bytes=") {
            cfg.remote_cache_avatar_bytes =
                bytes.parse().ok().filter(|n| *n > 0).unwrap_or_else(|| {
                    eprintln!("invalid --remote-cache-avatar-bytes (want an integer > 0): {bytes}");
                    std::process::exit(2);
                });
        } else if let Some(bytes) = arg.strip_prefix("--remote-cache-preview-bytes=") {
            cfg.remote_cache_preview_bytes =
                bytes.parse().ok().filter(|n| *n > 0).unwrap_or_else(|| {
                    eprintln!(
                        "invalid --remote-cache-preview-bytes (want an integer > 0): {bytes}"
                    );
                    std::process::exit(2);
                });
        } else if let Some(bytes) = arg.strip_prefix("--remote-cache-full-bytes=") {
            cfg.remote_cache_full_bytes =
                bytes.parse().ok().filter(|n| *n > 0).unwrap_or_else(|| {
                    eprintln!("invalid --remote-cache-full-bytes (want an integer > 0): {bytes}");
                    std::process::exit(2);
                });
        } else if let Some(mode) = arg.strip_prefix("--remote-cache-mode=") {
            cfg.remote_cache_mode = match mode {
                "disabled" => RemoteCacheMode::Disabled,
                "on_demand" => RemoteCacheMode::OnDemand,
                "warm" => RemoteCacheMode::Warm,
                other => {
                    eprintln!("unknown remote cache mode (want disabled|on_demand|warm): {other}");
                    std::process::exit(1);
                }
            };
        } else if let Some(days) = arg.strip_prefix("--remote-cache-avatar-ttl-days=") {
            cfg.remote_cache_avatar_ttl_days =
                days.parse().ok().filter(|n| *n > 0).unwrap_or_else(|| {
                    eprintln!(
                        "invalid --remote-cache-avatar-ttl-days (want an integer > 0): {days}"
                    );
                    std::process::exit(2);
                });
        } else if let Some(days) = arg.strip_prefix("--remote-cache-preview-ttl-days=") {
            cfg.remote_cache_preview_ttl_days =
                days.parse().ok().filter(|n| *n > 0).unwrap_or_else(|| {
                    eprintln!(
                        "invalid --remote-cache-preview-ttl-days (want an integer > 0): {days}"
                    );
                    std::process::exit(2);
                });
        } else if let Some(days) = arg.strip_prefix("--remote-cache-full-ttl-days=") {
            cfg.remote_cache_full_ttl_days =
                days.parse().ok().filter(|n| *n > 0).unwrap_or_else(|| {
                    eprintln!("invalid --remote-cache-full-ttl-days (want an integer > 0): {days}");
                    std::process::exit(2);
                });
        } else if let Some(flag) = arg.strip_prefix("--remote-cache-warming-enabled=") {
            cfg.remote_cache_warming_enabled = match flag {
                "true" | "1" => true,
                "false" | "0" => false,
                other => {
                    eprintln!("invalid --remote-cache-warming-enabled (want true|false): {other}");
                    std::process::exit(2);
                }
            };
        } else if let Some(dir) = arg.strip_prefix("--durable-dir=") {
            cfg.durable_dir = Some(dir.into());
        } else if let Some(dir) = arg.strip_prefix("--cache-dir=") {
            cfg.cache_dir = Some(dir.into());
        } else if let Some(dir) = arg.strip_prefix("--backup-dir=") {
            // Batch H: CLI override of the `backup.dir` file value (same
            // merged-then-validated flow as every other setting).
            cfg.backup.dir = Some(dir.into());
        } else if arg == "--no-transcode" {
            cfg.transcode.enabled = false;
        } else if let Some(rest) = arg.strip_prefix("--allow-local-http-for-testing") {
            // Per-invocation override of the JSON `unsafe` section (throwaway
            // smoke rigs only): bare flag means true, `=true|false|1|0`
            // explicit. `validate()` still restricts http:// to local/test
            // host classes.
            cfg.r#unsafe.allow_local_http_for_testing = match rest {
                "" | "=true" | "=1" => true,
                "=false" | "=0" => false,
                other => {
                    eprintln!(
                        "invalid --allow-local-http-for-testing{other} (want bare flag or =true|=false)"
                    );
                    std::process::exit(2);
                }
            };
        } else {
            positional.push(arg);
        }
    }

    if let Err(err) = cfg.validate() {
        eprintln!("{err}");
        std::process::exit(1);
    }

    // Batch H: publish the configured log shape process-wide before any
    // structured line is emitted.
    pinetree::observability::set_structured_logging(cfg.diagnostics.structured_logging);

    // Batch H/F/C maintenance ops: store-backed one-shots that run instead
    // of serving (`backup`, `backup-status`, `restore`, `storage-backfill`).
    // Named early so typos fail fast; executed after `build_services`
    // below (they need the store but not the worker or listener).
    const MAINTENANCE_OPS: &[&str] = &["backup", "backup-status", "restore", "storage-backfill"];
    let maintenance_op: Option<String> = positional
        .first()
        .filter(|cmd| MAINTENANCE_OPS.contains(&cmd.as_str()))
        .cloned();

    // Batch E wiring: publish the configured RFC 9421 mode process-wide so
    // the inbox/fetch signature gates enforce it instead of silently
    // ignoring `federation.rfc9421_mode` (default `permissive`).
    pinetree::activitypub::signatures::set_rfc9421_mode(cfg.federation.rfc9421_mode);

    // Phase 4 P1 wiring: publish the configured DNS resolver pool size
    // process-wide before serving (no-op once the first lookup initialized
    // the pool; startup always wins that race).
    if !pinetree::federation::delivery::configure_dns_pool(cfg.federation.dns_max_in_flight) {
        eprintln!(
            "DNS resolver pool already initialized; configured dns_max_in_flight takes effect on restart"
        );
    }

    // Wave 5: extra CA bundle (JSON `federation.federation_extra_ca` or
    // `--federation-extra-ca=PATH`). Appended as an additional trust anchor
    // — the platform default store is never replaced. A missing/unreadable
    // file is a permanent startup error, never a transient retry. The
    // file's SHA-256 fingerprint is logged so operators can confirm which
    // bundle is in force.
    if let Some(ca_path) = cfg.federation.federation_extra_ca.as_deref() {
        let bytes = std::fs::read(ca_path).unwrap_or_else(|err| {
            eprintln!(
                "federation_extra_ca unreadable ({}): {err}; refusing to boot",
                ca_path.display()
            );
            std::process::exit(1);
        });
        use sha2::Digest as _;
        let fingerprint = hex::encode(sha2::Sha256::digest(&bytes));
        eprintln!(
            "federation extra CA in force ({} bytes, sha256={}): not for production",
            bytes.len(),
            fingerprint
        );
        pinetree::federation::delivery::set_extra_ca(ca_path.to_path_buf());
    }

    // Batch B: conspicuous warning whenever the unsafe local-HTTP exception
    // is active. Host class only (loopback/localhost/private-lan/test-domain
    // /public) — never URLs, keys or other secrets.
    if cfg.r#unsafe.allow_local_http_for_testing {
        eprintln!(
            "WARNING: unsafe.allow_local_http_for_testing=true with an http:// public_url \
             (host class: {}): local/test rigs only, never enable in production",
            cfg.public_host_class().as_str()
        );
    }

    // Federation debug lines (accept/reject, delivery). On when the JSON
    // `federation.debug_federation`, the `--debug-federation` flag, or the
    // deprecated `PINETREE_DEBUG_FEDERATION` env override (all merged into
    // the config above) requests it. Non-production toggle: `validate()`
    // rejects `true` in release builds. See `pinetree::observability`.
    if cfg.federation.debug_federation {
        pinetree::observability::set_federation_debug(true);
        let source = if debug_federation_flag {
            "--debug-federation"
        } else if std::env::var(pinetree::observability::DEBUG_FEDERATION_ENV)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .is_some()
        {
            "PINETREE_DEBUG_FEDERATION"
        } else {
            "federation.debug_federation"
        };
        eprintln!("federation debug logging on (source: {source})");
    }

    if positional
        .first()
        .is_some_and(|cmd| cmd == "create-account")
    {
        cli::run_create_account(&positional[1..], &cfg);
    } else if positional.first().is_some_and(|cmd| cmd == "set-role") {
        cli::run_set_role(&positional[1..], &cfg);
    } else if let Some(cmd) = positional.first() {
        if maintenance_op.is_none() {
            eprintln!("unknown subcommand: {cmd}");
            std::process::exit(2);
        }
    }

    // Transcode startup gate (gaps WP1): when enabled, the ffmpeg
    // toolchain must probe cleanly — a missing/broken binary is a
    // startup error, never a silent passthrough of untranscoded bytes
    // (the worker also refuses stray `TranscodeMedia` jobs when
    // disabled). `--no-transcode` (or `transcode.enabled=false` in the
    // file) routes around ffmpeg entirely. Only the serve path reaches
    // here (`create-account`/`set-role` return above); `validate()`
    // checks static ranges, this probe checks the live toolchain and
    // prints the running version for the logs.
    let transcode_config = cfg.transcode.to_runtime();
    // The validated toolchain line feeds the admin status surface; disabled
    // installs report `"disabled"`.
    let mut ffmpeg_status = "disabled".to_string();
    if transcode_config.enabled {
        match transcode_config.validate() {
            Ok(info) => {
                ffmpeg_status = info.version.to_string();
                eprintln!("transcode toolchain: {}", info.version);
            }
            Err(err) => {
                // Fail closed: never enter serve mode with an explicitly
                // enabled but unusable toolchain (no silent untranscoded
                // passthrough). The probe below names the binary and the
                // checks run against it.
                eprintln!(
                    "transcode toolchain unavailable (probe: `{0} -version` plus \
                     `-encoders`/`-decoders` for {1}; set transcode.enabled=false \
                     or pass --no-transcode to start without FFmpeg): {err}",
                    transcode_config.ffmpeg_path,
                    pinetree::media::transcode::REQUIRED_ENCODERS.join(", ")
                );
                std::process::exit(1);
            }
        }
    } else {
        eprintln!("transcode disabled (--no-transcode or transcode.enabled=false)");
    }
    // Crash-heal: a SIGKILL between ffmpeg spawn and temp-guard drop
    // orphans `pinetree-transcode-*` files, so sweep hour-old ones at
    // boot (bounded; never touches a live job's fresh files).
    let swept = pinetree::media::transcode::sweep_tmp_files(
        &std::env::temp_dir(),
        pinetree::media::transcode::TMP_SWEEP_GRACE_MS,
        pinetree::media::transcode::TMP_SWEEP_CAP,
    );
    if swept > 0 {
        eprintln!("transcode temp sweep: removed {swept} stale files");
    }

    let (services, hub, store) = runtime::build_services(&cfg).unwrap_or_else(|err| {
        eprintln!("{err}");
        std::process::exit(1);
    });
    // Batch F/H: the live backup context (store handle + pool roots +
    // confined backup dir). Published for the admin backup routes; the
    // worker drain below and the maintenance CLI ops share it.
    let backup_ctx = pinetree::backup::BackupContext {
        store: store.clone(),
        durable_pool: cfg.durable_pool_dir(),
        cache_pool: cfg.cache_pool_dir(),
        backup_dir: cfg.backup_dir(),
        max_bundle_bytes: Some(cfg.backup.max_bundle_bytes),
        retention_max_count: cfg.backup.retention_max_count,
        retention_max_bytes: cfg.backup.retention_max_bytes,
    };
    // Batch F startup cleanup: abandoned `.partial-*` bundles never look
    // final, and stale `running` records (their process is gone) fail with
    // `abandoned-restart`. Queued records stay for the drain.
    match pinetree::backup::cleanup_abandoned_partials(&backup_ctx.backup_dir) {
        Ok(report) if report.partials_removed > 0 || report.stale_running_failed > 0 => {
            eprintln!(
                "backup cleanup: removed {} partial files, failed {} stale running records",
                report.partials_removed, report.stale_running_failed
            );
        }
        Ok(_) => {}
        Err(err) => eprintln!("backup cleanup failed (continuing to serve): {err}"),
    }
    // Batch H maintenance CLI ops (run instead of serving; the store is
    // already open — if the server holds it, the open above fails with the
    // lock error, which is the actionable message).
    if let Some(op) = maintenance_op.as_deref() {
        runtime::run_maintenance_op(op, &positional[1..], &services, &backup_ctx);
    }
    // Batch H ops surface: probes + admin status read this (never raw
    // paths, secrets, or key material — see `OpsConfig`).
    pinetree::api::misskey::endpoints::set_ops_config(
        pinetree::api::misskey::endpoints::OpsConfig {
            detailed_admin_status: cfg.diagnostics.detailed_admin_status,
            durable_dir: cfg.durable_pool_dir(),
            cache_dir: cfg.cache_pool_dir(),
            backup_dir: cfg.backup_dir(),
            transcode_enabled: transcode_config.enabled,
            ffmpeg_status,
            config_fingerprint: cfg.config_summary(),
            started_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
        },
    );
    pinetree::api::misskey::endpoints::set_backup_context(backup_ctx.clone());
    // Batch C boot trigger: one bounded reconcile job at startup (the
    // worker drains it within its normal polling). Safe to re-run; a
    // failure here is loud but not fatal (readiness still gates traffic).
    if let Err(err) = pinetree::media::reconciler::enqueue_storage_reconcile(
        services.jobs.as_ref(),
        services.clock.now_ms(),
    ) {
        eprintln!("boot storage reconcile enqueue failed (continuing to serve): {err}");
    }
    // Opt-in bounded repair for pre-Slice-B historical remote notes whose
    // original `inReplyTo` was discarded (`docs/replies.md` §6.1). Off by
    // default (no network I/O at startup); set JSON
    // `federation.reply_repair_limit` (1..=5000, `--reply-repair-limit=`,
    // or deprecated `PINETREE_REPLY_REPAIR_LIMIT`) to re-fetch at most that
    // many of the newest stored notes' origin documents and mark the ones
    // that still declare a parent. Sequential, restartable, safe to re-run.
    if let Some(limit) = cfg.federation.reply_repair_limit {
        eprintln!("reply repair: scanning up to {limit} stored notes");
        match pinetree::application::commands::inbound::replies::repair_unresolved_reply_parents(
            &services, limit,
        ) {
            Ok(report) => eprintln!(
                "reply repair: scanned={} marked={} (resolver jobs enqueued)",
                report.scanned, report.marked
            ),
            Err(err) => eprintln!("reply repair failed (continuing to serve): {err}"),
        }
    }
    // Outbound email transport (gaps WP3): the worker holds the sender
    // as `Arc<dyn EmailSender>` and runs sends on blocking threads. An
    // unconfigured transport stays dormant here — sends fail loudly and
    // the job quarantines (endpoints map the same state to
    // `EMAIL_NOT_CONFIGURED`).
    let email_sender: Arc<dyn EmailSender> = Arc::new(SmtpEmailSender::new(cfg.smtp_config()));
    // Publish the sender to the dispatcher slot so the email-identity arms
    // (`i/update-email`, `request-reset-password`) send live when SMTP is
    // configured; unconfigured stays dormant (`EMAIL_NOT_CONFIGURED`).
    if cfg.smtp_config().is_configured() {
        pinetree::api::misskey::endpoints::set_email_sender(email_sender.clone());
    }
    let worker = Arc::new(JobWorker {
        clock: services.clock.clone(),
        ids: services.ids.clone(),
        jobs: services.jobs.clone(),
        notifications: services.notifications.clone(),
        streaming: services.streaming.clone(),
        services: services.clone(),
        delivery_tracker: Arc::new(DestinationTracker::new()),
        lease_duration_ms: 30_000,
        base_backoff_ms: 1_000,
        transcode_config,
        email_sender,
    });
    // The worker stops claiming new jobs once `stop` is set (see the
    // shutdown arm below). A job claimed but not settled keeps its lease;
    // the lease expires and the next process to start reclaims it, so no
    // work is lost across a restart.
    let stop = Arc::new(AtomicBool::new(false));
    let _worker_handle = worker
        .clone()
        .spawn_until(Duration::from_millis(50), stop.clone());
    // Batch F backup drain: the worker tick owns durable jobs; backup
    // requests are durable status files, so a slow-interval task claims the
    // oldest queued request and builds its bundle (exclusive gate, bounded
    // retries inside `run_next_backup`). Stops with the worker.
    {
        let worker = worker.clone();
        let backup_ctx = backup_ctx.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                // Wave 4 liveness: the drain runs a redb scan, zip, and
                // sha256 under the backup `runner_lock` — all blocking. It
                // must never run on a Tokio worker, so each step goes
                // through `spawn_blocking` (blocking pool, 8 MiB stacks like
                // the workers). The lock lives only inside the blocking
                // thread, never across `.await`.
                let worker = worker.clone();
                let backup_ctx = backup_ctx.clone();
                match tokio::task::spawn_blocking(move || {
                    worker.drain_pending_backups(&backup_ctx, 1)
                })
                .await
                {
                    Ok(Ok(0)) => {}
                    Ok(Ok(done)) => pinetree::observability::log_event(
                        pinetree::observability::LogLevel::Info,
                        "backup.drained",
                        &[("completed", done.to_string().as_str())],
                    ),
                    Ok(Err(err)) => pinetree::observability::log_event(
                        pinetree::observability::LogLevel::Warn,
                        "backup.drain_failed",
                        &[(
                            "error_class",
                            pinetree::observability::error_class(&err.to_string()),
                        )],
                    ),
                    Err(join_err) => pinetree::observability::log_event(
                        pinetree::observability::LogLevel::Warn,
                        "backup.drain_join_failed",
                        &[(
                            "error_class",
                            pinetree::observability::error_class(&join_err.to_string()),
                        )],
                    ),
                }
            }
        });
    }
    let rate_bounds = RateLimitBounds::new(
        cfg.rate_limit.max_buckets,
        cfg.rate_limit.max_auth_failures,
        cfg.rate_limit.prune_interval_ms,
    );
    let mut rate_limiter =
        RateLimiter::with_misskey_defaults(services.clock.clone()).with_config(rate_bounds);
    if debug_weak_rate_limits_flag {
        // Load tests only: 10x the endpoint bucket counts (same factor as birchtree).
        rate_limiter = rate_limiter.with_weakened_limits(10);
        eprintln!(
            "debug rate-limit weakening is in force (x10, --debug-weak-rate-limits): not for production"
        );
    }
    let rate_limiter = Arc::new(rate_limiter);
    let upload_bounds =
        UploadSlotBounds::new(cfg.uploads.per_ip, cfg.uploads.global, cfg.uploads.max_ips);
    let api_state = ApiState {
        services,
        worker,
        rate_limiter,
        upload_slots: Arc::new(UploadSlots::from_bounds(upload_bounds)),
        hub,
        trusted_proxies: Arc::new(cfg.trusted_proxies.clone()),
        featured_cache: Arc::new(pinetree::application::commands::pins::FeaturedCache::default()),
    };
    let app = build_router(api_state);
    let listener = tokio::net::TcpListener::bind(cfg.listen)
        .await
        .expect("bind listener");
    // Client IPs come from the TCP peer (`ConnectInfo`), not from headers.
    //
    // On `SIGTERM` (how a container runtime stops a container) the worker
    // stops claiming jobs and the HTTP server stops accepting connections
    // while the in-flight ones finish. Without this the default `SIGTERM`
    // disposition kills the process mid-request and cuts streaming sockets
    // without a close frame.
    let shutdown = async move {
        pinetree::shutdown::signal().await;
        stop.store(true, Ordering::Relaxed);
        eprintln!("shutdown signal received; draining in-flight requests");
    };
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown)
    .await
    .expect("serve axum");
}
