use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use pinetree::api::misskey::auth::hash_password;
use pinetree::api::misskey::response::{RateLimiter, UploadSlots};
use pinetree::api::misskey::{ApiState, build_router};
use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    AccountStore, ActivityDedup, AuthStore, BlobStore, BlockStore, FollowStore, JobQueue, MediaStore,
    MuteStore, NoteStore, NotificationStore, PollStore, ReactionStore, StreamingSink,
};
use pinetree::streaming::hub::hub_and_sink;
use pinetree::application::ports::{Clock, IdGenerator};
use pinetree::config::{Config, DeploymentMode, RemoteCacheMode};
use pinetree::domain::id::{AidxGenerator, SystemClock};
use pinetree::federation::delivery::{SafeHttpClient, SafeHttpConfig};
use pinetree::federation::retry::DestinationTracker;
use pinetree::jobs::worker::JobWorker;
use pinetree::media::storage::FsBlobStore;
use pinetree::persistence::redb::RedbStore;

fn build_services(cfg: &Config) -> Result<(Arc<AppServices>, pinetree::streaming::hub::Hub), String> {
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
    let store = RedbStore::open(cfg.data_dir.join("pinetree.redb"), clock.clone(), ids.clone())
        .map_err(|err| format!("open redb store: {err}"))?;
    store
        .set_cache_cap(cfg.remote_cache_max_bytes)
        .map_err(|err| format!("init cache cap: {err}"))?;
    // Storage overhaul: durable local uploads live in `blobs_durable/`
    // (never evicted), remote fetches in `blobs_cache/` (LRU+TTL, capped).
    // A pre-overhaul `blobs/` directory is migrated once via rename.
    let legacy_blobs = cfg.data_dir.join("blobs");
    let durable_dir = cfg.data_dir.join("blobs_durable");
    if !durable_dir.exists() && legacy_blobs.exists() {
        std::fs::rename(&legacy_blobs, &durable_dir)
            .map_err(|err| format!("migrate blobs/ to blobs_durable/: {err}"))?;
    }
    let blobs: Arc<dyn BlobStore> = Arc::new(
        FsBlobStore::new(&durable_dir).map_err(|err| format!("init blob store: {err}"))?,
    );
    let cache_blobs: Arc<dyn BlobStore> = Arc::new(
        FsBlobStore::new(cfg.data_dir.join("blobs_cache"))
            .map_err(|err| format!("init cache blob store: {err}"))?,
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
    Ok((services, hub))
}

/// `create-account --username=<name> --password=<pw>`: create a local
/// account (with password) from the CLI for operators and scripts
/// (`docs/phase-1-plan.md` gap #2).
fn run_create_account(args: &[String], cfg: &Config) -> ! {
    let mut username: Option<String> = None;
    let mut password: Option<String> = None;
    for arg in args {
        if let Some(value) = arg.strip_prefix("--username=") {
            username = Some(value.to_string());
        } else if let Some(value) = arg.strip_prefix("--password=") {
            password = Some(value.to_string());
        } else {
            eprintln!("unknown create-account argument: {arg}");
            std::process::exit(2);
        }
    }
    let (username, password) = match (username, password) {
        (Some(username), Some(password)) if !username.is_empty() && !password.is_empty() => {
            (username, password)
        }
        _ => {
            eprintln!("usage: create-account --username=<name> --password=<pw>");
            std::process::exit(2);
        }
    };
    let services = build_services(cfg).unwrap_or_else(|err| {
        eprintln!("{err}");
        std::process::exit(1);
    }).0;
    match services.create_account(&username, Some(&hash_password(&password))) {
        Ok(account) => {
            println!("created account {} id={}", account.username, account.id);
            std::process::exit(0);
        }
        Err(err) => {
            eprintln!("create account {username}: {err}");
            std::process::exit(1);
        }
    }
}

/// `set-role --username=<name> (--admin|--moderator)[=false]`: set the
/// fixed built-in role flags on a local account from the CLI (Batch M
/// §3.1). This is how the first admin is created (§9.9). Flags not named
/// keep their current values. Writes a `setRole` moderation-log row with
/// no actor account (operator action).
fn run_set_role(args: &[String], cfg: &Config) -> ! {
    fn parse_flag(name: &str, rest: &str) -> bool {
        match rest {
            "" | "=true" => true,
            "=false" => false,
            _ => {
                eprintln!("usage: set-role --username=<name> (--admin|--moderator)[=false]");
                eprintln!("bad flag value: {name}{rest}");
                std::process::exit(2);
            }
        }
    }
    let mut username: Option<String> = None;
    let mut is_admin: Option<bool> = None;
    let mut is_moderator: Option<bool> = None;
    for arg in args {
        if let Some(value) = arg.strip_prefix("--username=") {
            username = Some(value.to_string());
        } else if let Some(rest) = arg.strip_prefix("--admin") {
            is_admin = Some(parse_flag("--admin", rest));
        } else if let Some(rest) = arg.strip_prefix("--moderator") {
            is_moderator = Some(parse_flag("--moderator", rest));
        } else {
            eprintln!("unknown set-role argument: {arg}");
            std::process::exit(2);
        }
    }
    let (Some(username), true) = (username, is_admin.is_some() || is_moderator.is_some()) else {
        eprintln!("usage: set-role --username=<name> (--admin|--moderator)[=false]");
        std::process::exit(2);
    };
    let services = build_services(cfg).unwrap_or_else(|err| {
        eprintln!("{err}");
        std::process::exit(1);
    }).0;
    let account = match services
        .accounts
        .find_by_username_and_host(&username, None)
    {
        Ok(Some(account)) => account,
        Ok(None) => {
            eprintln!("no local account {username}");
            std::process::exit(1);
        }
        Err(err) => {
            eprintln!("lookup {username}: {err}");
            std::process::exit(1);
        }
    };
    let new_is_admin = is_admin.unwrap_or(account.is_admin);
    let new_is_moderator = is_moderator.unwrap_or(account.is_moderator);
    match services.set_role(None, &account.id, new_is_admin, new_is_moderator) {
        Ok(account) => {
            println!(
                "account {} id={} isAdmin={} isModerator={}",
                account.username, account.id, account.is_admin, account.is_moderator
            );
            std::process::exit(0);
        }
        Err(err) => {
            eprintln!("set role {username}: {err}");
            std::process::exit(1);
        }
    }
}

#[tokio::main]
async fn main() {
    pinetree::observability::init();

    // Base configuration: the `--config=PATH` JSON file over the compiled
    // defaults (unknown keys, unreadable files and malformed JSON are hard
    // errors naming the path). Every key below is optional in the file;
    // absent keys take the defaults. This loop's flags override the file.
    let mut cfg = config_base();
    let mut positional: Vec<String> = Vec::new();
    let mut debug_federation_flag = false;
    let mut debug_weak_rate_limits_flag = false;
    for arg in std::env::args().skip(1) {
        if arg == "--debug-federation" {
            debug_federation_flag = true;
        } else if arg == "--debug-weak-rate-limits" {
            debug_weak_rate_limits_flag = true;
        } else if let Some(path) = arg.strip_prefix("--federation-extra-ca=") {
            pinetree::federation::delivery::set_extra_ca(path.into());
            eprintln!("federation extra CA in force (--federation-extra-ca={path}): not for production");
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
            cfg.trusted_proxies = match parse_trusted_proxies(list) {
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
            cfg.preserved_usernames = parse_csv_list(list);
        } else if let Some(list) = arg.strip_prefix("--prohibited-words=") {
            cfg.prohibited_words = parse_csv_list(list);
        } else if let Some(list) = arg.strip_prefix("--sensitive-words=") {
            cfg.sensitive_words = parse_csv_list(list);
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
            cfg.drive_capacity_per_user_bytes = bytes.parse().ok().filter(|n| *n > 0).unwrap_or_else(|| {
                eprintln!("invalid --drive-capacity-per-user-bytes (want an integer > 0): {bytes}");
                std::process::exit(2);
            });
        } else if let Some(bytes) = arg.strip_prefix("--remote-cache-max-bytes=") {
            cfg.remote_cache_max_bytes = bytes.parse().ok().filter(|n| *n > 0).unwrap_or_else(|| {
                eprintln!("invalid --remote-cache-max-bytes (want an integer > 0): {bytes}");
                std::process::exit(2);
            });
        } else if let Some(bytes) = arg.strip_prefix("--remote-cache-avatar-bytes=") {
            cfg.remote_cache_avatar_bytes = bytes.parse().ok().filter(|n| *n > 0).unwrap_or_else(|| {
                eprintln!("invalid --remote-cache-avatar-bytes (want an integer > 0): {bytes}");
                std::process::exit(2);
            });
        } else if let Some(bytes) = arg.strip_prefix("--remote-cache-preview-bytes=") {
            cfg.remote_cache_preview_bytes = bytes.parse().ok().filter(|n| *n > 0).unwrap_or_else(|| {
                eprintln!("invalid --remote-cache-preview-bytes (want an integer > 0): {bytes}");
                std::process::exit(2);
            });
        } else if let Some(bytes) = arg.strip_prefix("--remote-cache-full-bytes=") {
            cfg.remote_cache_full_bytes = bytes.parse().ok().filter(|n| *n > 0).unwrap_or_else(|| {
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
            cfg.remote_cache_avatar_ttl_days = days.parse().ok().filter(|n| *n > 0).unwrap_or_else(|| {
                eprintln!("invalid --remote-cache-avatar-ttl-days (want an integer > 0): {days}");
                std::process::exit(2);
            });
        } else if let Some(days) = arg.strip_prefix("--remote-cache-preview-ttl-days=") {
            cfg.remote_cache_preview_ttl_days = days.parse().ok().filter(|n| *n > 0).unwrap_or_else(|| {
                eprintln!("invalid --remote-cache-preview-ttl-days (want an integer > 0): {days}");
                std::process::exit(2);
            });
        } else if let Some(days) = arg.strip_prefix("--remote-cache-full-ttl-days=") {
            cfg.remote_cache_full_ttl_days = days.parse().ok().filter(|n| *n > 0).unwrap_or_else(|| {
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
        } else {
            positional.push(arg);
        }
    }

    if let Err(err) = cfg.validate() {
        eprintln!("{err}");
        std::process::exit(1);
    }

    // Federation debug lines (accept/reject, delivery). Off unless the flag or
    // PINETREE_DEBUG_FEDERATION is set. See `pinetree::observability`.
    let env_debug = pinetree::observability::env_enables_federation_debug(
        std::env::var(pinetree::observability::DEBUG_FEDERATION_ENV).ok().as_deref(),
    );
    if debug_federation_flag || env_debug {
        pinetree::observability::set_federation_debug(true);
        let source = if debug_federation_flag { "--debug-federation" } else { "PINETREE_DEBUG_FEDERATION" };
        eprintln!("federation debug logging on (source: {source})");
    }

    if positional.first().is_some_and(|cmd| cmd == "create-account") {
        run_create_account(&positional[1..], &cfg);
    } else if positional.first().is_some_and(|cmd| cmd == "set-role") {
        run_set_role(&positional[1..], &cfg);
    } else if let Some(cmd) = positional.first() {
        eprintln!("unknown subcommand: {cmd}");
        std::process::exit(2);
    }

    let (services, hub) = build_services(&cfg).unwrap_or_else(|err| {
        eprintln!("{err}");
        std::process::exit(1);
    });
    // Opt-in bounded repair for pre-Slice-B historical remote notes whose
    // original `inReplyTo` was discarded (`docs/replies.md` §6.1). Off by
    // default (no network I/O at startup); set
    // `PINETREE_REPLY_REPAIR_LIMIT=<1..=5000>` to re-fetch at most that many
    // of the newest stored notes' origin documents and mark the ones that
    // still declare a parent. Sequential, restartable, and safe to re-run.
    if let Some(limit_raw) = std::env::var("PINETREE_REPLY_REPAIR_LIMIT").ok().filter(|v| !v.trim().is_empty()) {
        match limit_raw.trim().parse::<usize>() {
            Ok(limit) if limit > 0 => {
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
            _ => eprintln!(
                "invalid PINETREE_REPLY_REPAIR_LIMIT={limit_raw:?} (want 1..=5000); skipping reply repair"
            ),
        }
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
    });
    // The worker stops claiming new jobs once `stop` is set (see the
    // shutdown arm below). A job claimed but not settled keeps its lease;
    // the lease expires and the next process to start reclaims it, so no
    // work is lost across a restart.
    let stop = Arc::new(AtomicBool::new(false));
    let _worker_handle = worker.clone().spawn_until(Duration::from_millis(50), stop.clone());
    let mut rate_limiter = RateLimiter::with_misskey_defaults(services.clock.clone());
    if debug_weak_rate_limits_flag {
        // Load tests only: 10x the endpoint bucket counts (same factor as birchtree).
        rate_limiter = rate_limiter.with_weakened_limits(10);
        eprintln!("debug rate-limit weakening is in force (x10, --debug-weak-rate-limits): not for production");
    }
    let rate_limiter = Arc::new(rate_limiter);
    let api_state = ApiState {
        services,
        worker,
        rate_limiter,
        upload_slots: Arc::new(UploadSlots::with_defaults()),
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

/// `--trusted-proxies=ip[,ip...]`: reverse proxies whose `X-Forwarded-For`
/// is honored for client IPs (slice 9, H4).
fn parse_trusted_proxies(list: &str) -> Result<Vec<IpAddr>, String> {
    list.split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| {
            part.parse::<IpAddr>()
                .map_err(|err| format!("invalid --trusted-proxies address {part}: {err}"))
        })
        .collect()
}

/// Comma-separated string lists (`--preserved-usernames`,
/// `--prohibited-words`, `--sensitive-words`). Empty items are dropped;
/// pass an empty value to clear the file/default list.
fn parse_csv_list(list: &str) -> Vec<String> {
    list.split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_string)
        .collect()
}

/// Base configuration: the `--config=PATH` JSON file over the compiled
/// defaults (unknown keys, unreadable files and malformed JSON are hard
/// errors naming the path). Every other CLI flag overrides the file.
fn config_base() -> Config {
    let path = std::env::args()
        .skip(1)
        .filter_map(|arg| arg.strip_prefix("--config=").map(str::to_string))
        .next();
    match path {
        None => Config::default(),
        Some(path) => Config::load_from_file(std::path::Path::new(&path)).unwrap_or_else(|err| {
            eprintln!("configuration error: {err}");
            std::process::exit(1);
        }),
    }
}
