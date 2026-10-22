//! Pack emoji (split from `packers.rs`). Pure move.
//!
#![allow(unused_imports)]
//! Misskey response packers (Slice D4).
//!
//! Render domain types into Misskey 2026.10 wire shapes
//! (`misskey/packages/backend/src/models/json-schema/`:
//! `note.ts`, `user.ts`, `drive-file.ts`, `meta.ts`). Field sets are
//! pinned by snapshot tests at the bottom of this file that read the
//! actual checkout schemas from disk. The tests skip (pass vacuously) when
//! the git-ignored `./misskey` checkout is absent, e.g. in clean clones.

use serde_json::{Map, Value};

use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::domain::account::{Account, FollowVisibility};
use crate::domain::blocking::Blocking;
use crate::domain::draft::NoteDraft;
use crate::domain::drive_folder::DriveFolder;
use crate::domain::follow::Follow;
use crate::domain::instance::InviteCode;
use crate::domain::media::MediaMeta;
use crate::domain::moderation::{AbuseReport, ModerationLogEntry};
use crate::domain::muting::{Muting, RenoteMuting};
use crate::domain::note::{Note, Visibility};
use crate::domain::notification::{Notification, NotificationKind};
use crate::domain::reaction::Reaction;
use crate::domain::visibility::{ViewerContext, is_visible};
use crate::domain::webhook::SystemWebhook;
use crate::domain::webhook::UserWebhook;

/// Misskey server version we claim compatibility with.

/// Millisecond timestamps as ISO-8601 UTC strings
/// (`"YYYY-MM-DDTHH:MM:SS.sssZ"`).

/// Custom emoji packs (Batch N WS1; Misskey `EmojiEntityService` parity).
/// `EmojiSimple {aliases, name, category, url, localOnly?, isSensitive?,
/// roleIds…?}` omits falsy `localOnly`/`isSensitive` and empty `roleIds`
/// (upstream `omit` parity); `url` prefers the served drive URL. `EmojiDetailed`
/// adds `id/host/license` (+ stored media flags verbatim).
pub fn pack_emoji_simple(
    emoji: &crate::domain::emoji::CustomEmoji,
    services: &AppServices,
) -> Value {
    let mut obj = Map::new();
    obj.insert("name".to_string(), Value::String(emoji.shortcode.clone()));
    if let Some(category) = emoji.category.as_deref() {
        obj.insert("category".to_string(), Value::String(category.to_string()));
    }
    obj.insert(
        "aliases".to_string(),
        Value::Array(emoji.aliases.iter().cloned().map(Value::String).collect()),
    );
    obj.insert("url".to_string(), emoji_url(emoji, services));
    if emoji.local_only {
        obj.insert("localOnly".to_string(), Value::Bool(true));
    }
    if emoji.is_sensitive {
        obj.insert("isSensitive".to_string(), Value::Bool(true));
    }
    if !emoji.role_ids.is_empty() {
        obj.insert(
            "roleIdsThatCanBeUsedThisEmojiAsReaction".to_string(),
            Value::Array(emoji.role_ids.iter().cloned().map(Value::String).collect()),
        );
    }
    Value::Object(obj)
}
pub fn pack_emoji_detailed(
    emoji: &crate::domain::emoji::CustomEmoji,
    services: &AppServices,
) -> Value {
    let mut obj = match pack_emoji_simple(emoji, services) {
        Value::Object(map) => map,
        other => return other,
    };
    obj.insert("id".to_string(), Value::String(emoji.id.clone()));
    obj.insert("host".to_string(), Value::Null);
    obj.insert(
        "license".to_string(),
        emoji
            .license
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    // Stored (never enforced — pinetree has no custom-role subsystem).
    obj.insert("localOnly".to_string(), Value::Bool(emoji.local_only));
    obj.insert("isSensitive".to_string(), Value::Bool(emoji.is_sensitive));
    obj.insert(
        "roleIdsThatCanBeUsedThisEmojiAsReaction".to_string(),
        Value::Array(emoji.role_ids.iter().cloned().map(Value::String).collect()),
    );
    Value::Object(obj)
}
/// Remote custom-emoji pack (emoji completion; `admin/emoji/list-remote` +
/// upstream `EmojiDetailed` parity): mirrors `pack_emoji_detailed`
/// field-for-field, with `host` set (never null) and `url` preferring the
/// lazily cached drive copy when `media_id` resolves, else the remote URL
/// (the same rule as `emoji_maps_for_viewer`).
///
/// DIVERGENCE: remote rows have no global ids, so `id` is the composite
/// `{host}:{shortcode}`. Remote rows carry no category/aliases/license/
/// flags, so `category` stays absent and those slots pack as
/// empty/null/false.
pub fn pack_remote_emoji_detailed(
    remote: &crate::domain::emoji::RemoteEmoji,
    services: &AppServices,
) -> Value {
    let url = match remote.media_id.as_deref() {
        Some(media_id) => match services.media.get(media_id) {
            Ok(Some(meta)) => Value::String(meta.url.clone()),
            _ => Value::String(remote.remote_url.clone()),
        },
        None => Value::String(remote.remote_url.clone()),
    };
    let mut obj = Map::new();
    obj.insert("name".to_string(), Value::String(remote.shortcode.clone()));
    obj.insert("aliases".to_string(), Value::Array(Vec::new()));
    obj.insert("url".to_string(), url);
    obj.insert(
        "id".to_string(),
        Value::String(format!("{}:{}", remote.host, remote.shortcode)),
    );
    obj.insert("host".to_string(), Value::String(remote.host.clone()));
    obj.insert("license".to_string(), Value::Null);
    obj.insert("localOnly".to_string(), Value::Bool(false));
    obj.insert("isSensitive".to_string(), Value::Bool(false));
    obj.insert(
        "roleIdsThatCanBeUsedThisEmojiAsReaction".to_string(),
        Value::Array(Vec::new()),
    );
    Value::Object(obj)
}
/// Served URL for one local emoji: the drive file's URL, falling back to
/// the canonical `/emojis/:name` path when the file row is gone (upstream
/// renders `publicUrl || originalUrl`; pinetree has one served URL).
pub(crate) fn emoji_url(
    emoji: &crate::domain::emoji::CustomEmoji,
    services: &AppServices,
) -> Value {
    if let Some(media_id) = emoji.media_id.as_deref() {
        if let Ok(Some(meta)) = services.media.get(media_id) {
            return Value::String(meta.url.clone());
        }
    }
    Value::String(format!(
        "{}/emojis/{}",
        services.public_url.trim_end_matches('/'),
        emoji.shortcode
    ))
}
/// Registry maps for note packs (Batch N WS1 + WP8b): bare shortcode → URL
/// for local emoji, plus `name@host` → URL for remote emoji when the viewer
/// opted in (`showRemoteEmojis`). Both the `emojis` and `reactionEmojis`
/// slots carry the same registry (pinetree does not track per-note emoji
/// use; clients look up by name — documented simplification).
/// With the setting off, remote custom reactions render as their
/// `:name@host:` text and no remote URL is packed.
pub fn emoji_maps_for_viewer(services: &AppServices, show_remote: bool) -> (Value, Value) {
    let mut map = Map::new();
    if let Ok(rows) = services.emojis.all_local_emojis() {
        let mut rows = rows;
        rows.sort_by(|a, b| a.shortcode.cmp(&b.shortcode));
        for emoji in &rows {
            map.insert(emoji.shortcode.clone(), emoji_url(emoji, services));
        }
    }
    if show_remote {
        if let Ok(rows) = services.emojis.all_remote_emojis() {
            let mut rows = rows;
            rows.sort_by(|a, b| (&a.host, &a.shortcode).cmp(&(&b.host, &b.shortcode)));
            for remote in &rows {
                let url = match remote.media_id.as_deref() {
                    // Lazily cached copy: the drive file's served URL.
                    Some(media_id) => match services.media.get(media_id) {
                        Ok(Some(meta)) => meta.url.clone(),
                        _ => remote.remote_url.clone(),
                    },
                    // Until cached, the packed URL points at the remote host
                    // (the documented privacy trade-off).
                    None => remote.remote_url.clone(),
                };
                map.insert(
                    format!("{}@{}", remote.shortcode, remote.host),
                    Value::String(url),
                );
            }
        }
    }
    let value = Value::Object(map);
    (value.clone(), value)
}
