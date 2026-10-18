use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
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
use pinetree::config::{Config, DeploymentMode};
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
    let blobs: Arc<dyn BlobStore> = Arc::new(
        FsBlobStore::new(cfg.data_dir.join("blobs")).map_err(|err| format!("init blob store: {err}"))?,
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
        polls: store.clone() as Arc<dyn PollStore>,
        jobs: store.clone() as Arc<dyn JobQueue>,
        media: store.clone() as Arc<dyn MediaStore>,
        blobs,
        notifications: store.clone() as Arc<dyn NotificationStore>,
        auth: store.clone() as Arc<dyn AuthStore>,
        streaming,
        dedup: store.clone() as Arc<dyn ActivityDedup>,
        host_caps: store.clone(),
        moderation: store.clone(),
        instance_settings: store.clone(),
        invites: store.clone(),
        federation_http: SafeHttpClient::new(SafeHttpConfig::default()),
        public_url: cfg.normalized_public_url(),
        max_file_size: cfg.max_file_size,
        preserved_usernames: cfg.preserved_usernames.clone(),
        prohibited_words: cfg.prohibited_words.clone(),
        sensitive_words: cfg.sensitive_words.clone(),
        note_draft_limit: cfg.note_draft_limit,
        scheduled_note_limit: cfg.scheduled_note_limit,
        pin_limit: cfg.pin_limit,
    });
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

    let mut cfg = Config::default();
    let mut positional: Vec<String> = Vec::new();
    let mut debug_federation_flag = false;
    for arg in std::env::args().skip(1) {
        if arg == "--debug-federation" {
            debug_federation_flag = true;
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
            cfg.listen = listen.parse().expect("valid --listen addr");
        } else if let Some(list) = arg.strip_prefix("--trusted-proxies=") {
            cfg.trusted_proxies = match parse_trusted_proxies(list) {
                Ok(proxies) => proxies,
                Err(err) => {
                    eprintln!("{err}");
                    std::process::exit(1);
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
    let _worker_handle = worker.clone().spawn(Duration::from_millis(50));
    let rate_limiter = Arc::new(RateLimiter::with_misskey_defaults(services.clock.clone()));
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
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
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
