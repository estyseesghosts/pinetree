#![allow(unused_imports)]
#![allow(dead_code)]

use crate::application::commands::AppServices;
use crate::application::commands::tests::UnavailableStores;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::application::ports::{
    AccountStore, ActivityDedup, ApiTokenRecord, AuthStore, BlobStore, BlockStore,
    CreateAccountInput, CreateNoteInput, FollowStore, HostCapsStore, HostSoftware,
    InsertMediaInput, JobQueue, LegacyAuthSessionRecord, MediaStore, MiAuthSessionRecord,
    MuteStore, NewJob, NoteStore, NotificationStore, PollChoiceCount, PollStore, PortError,
    ReactionStore, StreamingSink, UpdateAccountPatch,
};
use crate::domain::account::Account;
use crate::domain::blocking::Blocking;
use crate::domain::follow::Follow;
use crate::domain::id::{AidxGenerator, ManualClock};
use crate::domain::media::MediaMeta;
use crate::domain::muting::{Muting, RenoteMuting};
use crate::domain::note::{Note, Visibility};
use crate::domain::notification::Notification;
use crate::domain::poll::{Poll, PollVote};
use crate::domain::reaction::Reaction;
use serde_json::Value;

use super::support::Mem;

pub(crate) fn test_services(clock_ms: i64) -> (AppServices, Arc<Mem>, Arc<ManualClock>) {
    let clock = Arc::new(ManualClock::new(clock_ms));
    let mem = Arc::new(Mem::empty());
    let services = AppServices {
        clock: clock.clone(),
        ids: Arc::new(AidxGenerator::with_node_id(clock.clone(), "test", 0)),
        accounts: mem.clone(),
        notes: mem.clone(),
        follows: mem.clone(),
        blocks: mem.clone(),
        mutes: mem.clone(),
        reactions: mem.clone(),
        favorites: mem.clone(),
        emojis: mem.clone(),
        antennas: mem.clone(),
        antenna_notes: mem.clone(),
        polls: mem.clone(),
        jobs: mem.clone(),
        media: mem.clone(),
        blobs: mem.clone(),
        notifications: mem.clone(),
        auth: mem.clone(),
        streaming: mem.clone(),
        dedup: mem.clone() as Arc<dyn ActivityDedup>,
        host_caps: mem.clone(),
        moderation: mem.clone(),
        instance_settings: mem.clone(),
        invites: mem.clone(),
        abuse_reports: mem.clone(),
        lists: mem.clone(),
        signin: mem.clone(),
        password_resets: mem.clone(),
        registry: mem.clone(),
        clips: mem.clone(),
        pages: mem.clone(),
        folders: Arc::new(UnavailableStores),
        system_webhooks: Arc::new(UnavailableStores),
        user_webhooks: Arc::new(UnavailableStores),
        abuse_recipients: Arc::new(UnavailableStores),
        memos: Arc::new(UnavailableStores),
        thread_mutes: Arc::new(UnavailableStores),
        federation_http: crate::federation::delivery::SafeHttpClient::new(
            crate::federation::delivery::SafeHttpConfig {
                allow_private_ips: true,
                ..Default::default()
            },
        ),
        public_url: "https://example.local".to_string(),
        max_file_size: 10 * 1024 * 1024,
        preserved_usernames: Vec::new(),
        prohibited_words: Vec::new(),
        sensitive_words: Vec::new(),
        note_draft_limit: crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
        scheduled_note_limit: crate::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
        pin_limit: crate::domain::pinning::DEFAULT_PIN_LIMIT,
        default_like: None,
        cache_blobs: mem.clone(),
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
    (services, mem, clock)
}

pub(crate) fn test_account(id: &str, username: &str) -> Account {
    let mut account = Account::new_local(id.to_string(), username.to_string()).unwrap();
    account.followers_count = 3;
    account.following_count = 5;
    account.notes_count = 7;
    account
}

pub(crate) fn test_note(id: &str, author_id: &str) -> Note {
    Note {
        id: id.to_string(),
        author_id: author_id.to_string(),
        text: Some("hello world".to_string()),
        cw: None,
        visibility: Visibility::Public,
        visible_user_ids: Vec::new(),
        local_only: false,
        reply_id: None,
        renote_id: None,
        reply_user_id: None,
        file_ids: Vec::new(),
        mentions: Vec::new(),
        mentioned_remote_users: "[]".to_string(),
        tags: vec!["rust".to_string()],
        emojis: Vec::new(),
        reactions: BTreeMap::from([("❤️".to_string(), 2), ("👍".to_string(), 1)]),
        renote_count: 1,
        replies_count: 2,
        reaction_acceptance: None,
        uri: None,
        url: None,
        user_host: None,
        is_deleted: false,
        reply_parent_uri: None,
        quote: None,
        updated_at_ms: None,
        process_errors: None,
    }
}

pub(crate) fn test_media(id: &str, owner_id: &str) -> MediaMeta {
    MediaMeta {
        id: id.to_string(),
        owner_id: Some(owner_id.to_string()),
        name: "photo.png".to_string(),
        mime: "image/png".to_string(),
        size_bytes: 512,
        hash: "d41d8cd98f00b204e9800998ecf8427e".to_string(),
        blob_key: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string(),
        blurhash: None,
        properties: crate::domain::media::MediaProperties {
            width: Some(1280),
            height: Some(720),
            orientation: None,
            avg_color: None,
        },
        is_sensitive: false,
        is_link: false,
        url: format!("https://example.local/files/{id}"),
        thumbnail_url: None,
        comment: None,
        source_url: None,
        variant: "original".to_string(),
        media_class: "preview".to_string(),
        evicted: false,
    }
}

// --- Checkout schema parsing -------------------------------------------

pub(crate) fn schema_path(file: &str) -> String {
    format!(
        "{}/misskey/packages/backend/src/models/json-schema/{file}",
        env!("CARGO_MANIFEST_DIR")
    )
}

pub(crate) fn is_ident(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
}

pub(crate) fn parse_str_attr(line: &str, attr: &str) -> Option<String> {
    let needle = format!("{attr}:");
    for part in line.split(',') {
        let trimmed = part.trim();
        if let Some(rest) = trimmed.strip_prefix(&needle) {
            let rest = rest.trim().strip_prefix('\'')?;
            return Some(rest.split('\'').next().unwrap_or("").to_string());
        }
    }
    None
}

pub(crate) fn parse_bool_attr(line: &str, attr: &str) -> Option<bool> {
    let needle = format!("{attr}:");
    for part in line.split(',') {
        let trimmed = part.trim();
        if let Some(rest) = trimmed.strip_prefix(&needle) {
            let token: String = rest
                .trim()
                .chars()
                .take_while(|c| c.is_ascii_alphabetic())
                .collect();
            return match token.as_str() {
                "true" => Some(true),
                "false" => Some(false),
                _ => None,
            };
        }
    }
    None
}

/// Parse the top-level `properties` of `export const {schema} = {...}`:
/// `(name, type, nullable)` for every entry. Entries without
/// `optional:` are required (Misskey treats a missing `optional` as
/// required); entries with `optional: true` are returned too so tests
/// can decide.
pub(crate) fn parse_schema_props(src: &str, schema: &str) -> Vec<(String, String, bool, bool)> {
    let marker = format!("export const {schema} = {{");
    let start = src
        .find(&marker)
        .unwrap_or_else(|| panic!("schema {schema} not found"));
    let body = &src[start + marker.len()..];
    let mut depth: i32 = 1;
    let mut in_properties = false;
    let mut out = Vec::new();
    let mut current: Option<(String, Option<String>, Option<bool>, Option<bool>)> = None;
    for line in body.lines().skip(1) {
        let trimmed = line.trim();
        if !in_properties {
            if depth == 1 && trimmed.starts_with("properties:") {
                in_properties = true;
            }
        } else if depth == 2 {
            if let Some(header) = trimmed.strip_suffix('{') {
                if let Some(name) = header.trim().strip_suffix(':').map(str::trim) {
                    if is_ident(name) {
                        current = Some((name.to_string(), None, None, None));
                    }
                }
            }
        } else if depth == 3 {
            if let Some((_, ty, opt, nul)) = current.as_mut() {
                if let Some(v) = parse_str_attr(trimmed, "type") {
                    *ty = Some(v);
                }
                if let Some(v) = parse_bool_attr(trimmed, "optional") {
                    *opt = Some(v);
                }
                if let Some(v) = parse_bool_attr(trimmed, "nullable") {
                    *nul = Some(v);
                }
            }
        }
        depth += line.chars().filter(|&c| c == '{').count() as i32;
        depth -= line.chars().filter(|&c| c == '}').count() as i32;
        if in_properties && depth == 2 {
            if let Some((name, ty, opt, nul)) = current.take() {
                out.push((
                    name,
                    ty.unwrap_or_default(),
                    opt.unwrap_or(false),
                    nul.unwrap_or(false),
                ));
            }
        }
        if in_properties && depth < 2 {
            break;
        }
        if depth < 1 {
            break;
        }
    }
    assert!(!out.is_empty(), "no props parsed for {schema}");
    out
}

pub(crate) fn load_props(file: &str, schema: &str) -> Option<Vec<(String, String, bool, bool)>> {
    let path = schema_path(file);
    let src = match std::fs::read_to_string(&path) {
        Ok(src) => src,
        // The `./misskey` reference checkout is git-ignored and absent in
        // clean clones; skip (don't fail) so `cargo test` stays green.
        // Run with the checkout present to actually verify the snapshots.
        Err(e) => {
            eprintln!("SKIP snapshot: cannot read Misskey schema {path}: {e}");
            return None;
        }
    };
    Some(parse_schema_props(&src, schema))
}

/// Assert `packed` contains every **required** (`optional: false`)
/// schema key with a matching JSON type and nullability.
pub(crate) fn assert_shape(packed: &Value, props: &[(String, String, bool, bool)], ctx: &str) {
    let obj = packed
        .as_object()
        .unwrap_or_else(|| panic!("{ctx} is not an object: {packed}"));
    for (name, ty, optional, nullable) in props {
        if *optional {
            continue;
        }
        let value = obj
            .get(name)
            .unwrap_or_else(|| panic!("{ctx} missing required key `{name}`"));
        if !nullable {
            assert!(
                !value.is_null(),
                "{ctx}.{name} must be non-null (nullable: false)"
            );
        }
        if value.is_null() || ty.is_empty() {
            continue;
        }
        let ok = match ty.as_str() {
            "string" => value.is_string(),
            "number" => value.is_number(),
            "integer" => value.is_number(),
            "boolean" => value.is_boolean(),
            "array" => value.is_array(),
            "object" => value.is_object(),
            _ => true,
        };
        assert!(ok, "{ctx}.{name} must be {ty}, got {value}");
    }
}
