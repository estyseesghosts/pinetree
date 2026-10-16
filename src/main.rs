use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use pinetree::api::misskey::auth::hash_password;
use pinetree::api::misskey::response::{RateLimiter, UploadSlots};
use pinetree::api::misskey::{ApiState, build_router};
use pinetree::application::commands::AppServices;
use pinetree::application::ports::{
    AccountStore, ActivityDedup, AuthStore, BlobStore, FollowStore, JobQueue, MediaStore,
    NoteStore, NotificationStore, ReactionStore, StreamingSink,
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
        reactions: store.clone() as Arc<dyn ReactionStore>,
        jobs: store.clone() as Arc<dyn JobQueue>,
        media: store.clone() as Arc<dyn MediaStore>,
        blobs,
        notifications: store.clone() as Arc<dyn NotificationStore>,
        auth: store.clone() as Arc<dyn AuthStore>,
        streaming,
        dedup: store.clone() as Arc<dyn ActivityDedup>,
        federation_http: SafeHttpClient::new(SafeHttpConfig::default()),
        public_url: cfg.normalized_public_url(),
        max_file_size: cfg.max_file_size,
        preserved_usernames: cfg.preserved_usernames.clone(),
        prohibited_words: cfg.prohibited_words.clone(),
        sensitive_words: cfg.sensitive_words.clone(),
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

#[tokio::main]
async fn main() {
    pinetree::observability::init();

    let mut cfg = Config::default();
    let mut positional: Vec<String> = Vec::new();
    for arg in std::env::args().skip(1) {
        if let Some(mode) = arg.strip_prefix("--mode=") {
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

    if positional.first().is_some_and(|cmd| cmd == "create-account") {
        run_create_account(&positional[1..], &cfg);
    } else if let Some(cmd) = positional.first() {
        eprintln!("unknown subcommand: {cmd}");
        std::process::exit(2);
    }

    let (services, hub) = build_services(&cfg).unwrap_or_else(|err| {
        eprintln!("{err}");
        std::process::exit(1);
    });
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
