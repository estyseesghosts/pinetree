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

use crate::runtime::build_services;

/// `create-account --username=<name> --password=<pw>`: create a local
/// account (with password) from the CLI for operators and scripts
/// (`docs/phase-1-plan.md` gap #2).
pub(crate) fn run_create_account(args: &[String], cfg: &Config) -> ! {
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
    let services = build_services(cfg)
        .unwrap_or_else(|err| {
            eprintln!("{err}");
            std::process::exit(1);
        })
        .0;
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

/// fixed built-in role flags on a local account from the CLI (Batch M
/// §3.1). This is how the first admin is created (§9.9). Flags not named
/// keep their current values. Writes a `setRole` moderation-log row with
/// no actor account (operator action).
pub(crate) fn run_set_role(args: &[String], cfg: &Config) -> ! {
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
    let services = build_services(cfg)
        .unwrap_or_else(|err| {
            eprintln!("{err}");
            std::process::exit(1);
        })
        .0;
    let account = match services.accounts.find_by_username_and_host(&username, None) {
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

/// `--trusted-proxies=ip[,ip...]`: reverse proxies whose `X-Forwarded-For`
/// is honored for client IPs (slice 9, H4).
pub(crate) fn parse_trusted_proxies(list: &str) -> Result<Vec<IpAddr>, String> {
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
pub(crate) fn parse_csv_list(list: &str) -> Vec<String> {
    list.split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_string)
        .collect()
}

/// Base configuration: the `--config=PATH` JSON file over the compiled
/// defaults (unknown keys, unreadable files and malformed JSON are hard
/// errors naming the path). Every other CLI flag overrides the file.
pub(crate) fn config_base() -> Config {
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
