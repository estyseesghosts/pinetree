//! Shared request parsing, validation and packing helpers (rss).
//!
//! Split from `endpoints/parse.rs` (decomposition). Pure move, no behavior change.

#![allow(unused_imports)]
use super::super::ApiState;
use super::super::files_http::identicon;
use crate::activitypub::federated_collections as ap_collections;
use crate::activitypub::{actor as ap_actor, objects as ap_objects, webfinger as ap_webfinger};
use crate::api::misskey::auth::{
    AuthenticatedCaller, SESSION_COOKIE_NAME, authenticate, extract_raw_token,
    generate_random_token, hash_password_async, hash_token, is_safe_callback_url, needs_rehash,
    render_miauth_page, require_admin, require_moderator, verify_password_async,
};
use crate::api::misskey::errors::ApiError;
use crate::api::misskey::packers::{
    pack_abuse_report, pack_antenna, pack_blocking, pack_clip, pack_drive_file, pack_drive_folder,
    pack_emoji_detailed, pack_emoji_simple, pack_following, pack_invite_code, pack_me_detailed,
    pack_meta, pack_moderation_log, pack_muting, pack_note, pack_note_draft,
    pack_note_reaction_with_note, pack_notification, pack_page, pack_remote_emoji_detailed,
    pack_renote_muting, pack_scheduled_note, pack_system_webhook, pack_user_detailed_not_me,
    pack_user_list, pack_user_lite, pack_user_relation, pack_user_webhook,
};
use crate::api::misskey::request::check_allowed_fields;
use crate::api::misskey::response::{RateLimiter, UploadSlots};
use crate::application::commands::AppServices;
use crate::application::commands::announcements::MemAnnouncementStore;
use crate::application::commands::gallery::FlashVisibility;
use crate::application::commands::gallery::IdCursor;
use crate::application::commands::gallery::MemGalleryStore;
use crate::application::commands::invites::SignupRejection;
use crate::application::commands::misc_endpoints::EmojisV2Page;
use crate::application::commands::misc_endpoints::ping;
use crate::application::commands::pins::FeaturedCache;
use crate::application::commands::webhooks::CreateSystemWebhookInput;
use crate::application::commands::webhooks::CreateUserWebhookInput;
use crate::application::commands::webhooks::UpdateSystemWebhookInput;
use crate::application::commands::webhooks::UpdateUserWebhookInput;
use crate::application::commands::webhooks::create_system_webhook;
use crate::application::commands::webhooks::create_user_webhook;
use crate::application::commands::webhooks::delete_system_webhook;
use crate::application::commands::webhooks::delete_user_webhook;
use crate::application::commands::webhooks::show_system_webhook;
use crate::application::commands::webhooks::show_user_webhook;
use crate::application::commands::webhooks::update_system_webhook;
use crate::application::commands::webhooks::update_user_webhook;
use crate::application::ports::{
    AbuseReportFilter, ApiTokenRecord, BlobStore, CreateDraftInput, CreateNoteInput, EditNoteInput,
    ExportEntity, ImportEntity, InviteListFilter, InviteSort, InviteTypeFilter, JobKind,
    LegacyAuthSessionRecord, MiAuthSessionRecord, ModerationLogFilter, NewJob,
    PasswordResetRequest, PortError, RegistryItem, SigninRecord, TimelineCursor,
    UpdateAccountPatch, UpdateDraftPatch,
};
use crate::domain::account::{
    Account, AvatarDecoration, ChatScope, FollowVisibility, MutedWord, NotificationRecieveConfig,
    ProfileField,
};
use crate::domain::follow::FollowNotify;
use crate::domain::instance::{InviteCode, UgcVisibilityForVisitor};
use crate::domain::moderation::{AbuseReportResolution, ModerationAction};
use crate::domain::note::{MAX_CW_API_CHARS, MAX_NOTE_TEXT_CHARS, ReactionAcceptance, Visibility};
use crate::domain::notification::NotificationKind;
use crate::federation::delivery::{
    fetch_and_upsert_remote_actor, fetch_remote_note, webfinger_discover_actor_uri,
};
use crate::federation::inbox as fed_inbox;
use crate::jobs::worker::JobWorker;
use crate::media::transcode::TranscodeConfig;
use crate::services::email::EmailError;
use crate::services::email::EmailMessage;
use crate::services::email::EmailSendPayload;
use crate::services::email::EmailSender;
use crate::services::email::SmtpConfig;
use crate::services::email::SmtpEmailSender;
use crate::services::email::password_reset_email;
use crate::services::email::verification_email;
use crate::streaming::hub::{Hub, ONLINE_THRESHOLD_MS};
use crate::streaming::server::streaming_handler;
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{ConnectInfo, FromRequest, Multipart, Path, Query, Request, State},
    http::{
        HeaderMap, HeaderName, HeaderValue, StatusCode,
        header::{AUTHORIZATION, CONTENT_DISPOSITION, CONTENT_TYPE, COOKIE, LOCATION, SET_COOKIE},
    },
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use http_body_util::{BodyExt, Limited};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// True for absolute `http(s)` URLs (Batch P `fetch-rss`
/// `INVALID_URL` parity: only http(s) may be fetched, like the SSRF-guarded
/// federation client).
pub(crate) fn is_fetchable_http_url(url: &str) -> bool {
    let lower = url.trim().to_lowercase();
    (lower.starts_with("http://") || lower.starts_with("https://"))
        && !url.trim().contains([' ', '\t', '\n'])
}

/// Minimal RSS 2.0 + Atom fetch-and-parse (Batch P `fetch-rss`; upstream
/// uses `rss-parser`). Shape-compatible subset: feed `title`/`link`/
/// `description`/`feedUrl` + per-item `title`/`link`/`guid`/`pubDate`/
/// `creator`/`summary`/`content`/`isoDate`/`categories`. Omitted (recorded):
/// `image`, `paginationLinks`, `itunes`, per-item `contentSnippet` and
/// `enclosure`. SSRF-guarded fetch, 1 MiB cap; failures answer
/// `FETCH_RSS_FAILED` (422), bad URLs `INVALID_URL` (400).
pub(crate) fn fetch_rss_feed(services: &AppServices, url: &str) -> Result<Value, ApiError> {
    let resp = services
        .federation_http
        .send("GET", url, &[], None, 1_048_576)
        .map_err(|_| ApiError::fetch_rss_failed())?;
    if !(200..300).contains(&resp.status) {
        return Err(ApiError::fetch_rss_failed());
    }
    let text = String::from_utf8(resp.body).map_err(|_| ApiError::fetch_rss_failed())?;
    parse_rss_feed(&text, url).ok_or_else(ApiError::fetch_rss_failed)
}

/// Scan one `<tag ...>body</tag>` occurrence (first) out of `xml`.
/// Attribute-aware open-tag match, CDATA unwrapped, entities decoded.
pub(crate) fn xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}");
    let close = format!("</{tag}>");
    let start = xml.find(open.as_str())?;
    let after_open = xml[start..].find('>')? + start + 1;
    let end = xml[after_open..].find(close.as_str())? + after_open;
    Some(xml_unescape(xml_cdata(&xml[after_open..end]).trim()))
}

/// All `<tag ...>body</tag>` occurrences in order.
pub(crate) fn xml_tags(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find(open.as_str()) {
        let Some(gt) = rest[start..].find('>') else {
            break;
        };
        let body_start = start + gt + 1;
        let Some(end) = rest[body_start..].find(close.as_str()) else {
            break;
        };
        out.push(xml_unescape(
            xml_cdata(&rest[body_start..body_start + end]).trim(),
        ));
        rest = &rest[body_start + end + close.len()..];
    }
    out
}

pub(crate) fn xml_cdata(s: &str) -> String {
    let trimmed = s.trim();
    if trimmed.starts_with("<![CDATA[") && trimmed.ends_with("]]>") {
        trimmed[9..trimmed.len() - 3].to_string()
    } else {
        s.to_string()
    }
}

pub(crate) fn xml_unescape(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&#39;", "'")
}

/// Attribute value `attr="..."` (or `'...'`) on the first `<tag ...>` open
/// tag in `xml`.
pub(crate) fn xml_attr(xml: &str, tag: &str, attr: &str) -> Option<String> {
    let open = format!("<{tag}");
    let start = xml.find(open.as_str())?;
    let gt = xml[start..].find('>')? + start;
    let head = &xml[start..gt];
    for quote in ['"', '\''] {
        let needle = format!("{attr}={quote}");
        if let Some(pos) = head.find(needle.as_str()) {
            let value_start = pos + needle.len();
            if let Some(end) = head[value_start..].find(quote) {
                return Some(head[value_start..value_start + end].to_string());
            }
        }
    }
    None
}

pub(crate) fn parse_rss_feed(text: &str, feed_url: &str) -> Option<Value> {
    // Atom (`<feed>` + `<entry>`).
    if text.contains("<feed") {
        let title = xml_tag(text, "title");
        let link = xml_attr(text, "link", "href");
        let items: Vec<Value> = xml_tags(text, "entry")
            .iter()
            .map(|entry| {
                let mut item = serde_json::Map::new();
                if let Some(title) = xml_tag(entry, "title") {
                    item.insert("title".to_string(), Value::String(title));
                }
                if let Some(href) = xml_attr(entry, "link", "href") {
                    item.insert("link".to_string(), Value::String(href));
                } else if let Some(link) = xml_tag(entry, "link") {
                    item.insert("link".to_string(), Value::String(link));
                }
                if let Some(id) = xml_tag(entry, "id") {
                    item.insert("guid".to_string(), Value::String(id));
                }
                if let Some(updated) = xml_tag(entry, "updated") {
                    item.insert("pubDate".to_string(), Value::String(updated.clone()));
                    item.insert("isoDate".to_string(), Value::String(updated));
                }
                if let Some(author) = xml_tag(entry, "author") {
                    let name = xml_tag(&author, "name").unwrap_or(author);
                    item.insert("creator".to_string(), Value::String(name));
                }
                if let Some(summary) = xml_tag(entry, "summary") {
                    item.insert("summary".to_string(), Value::String(summary));
                }
                if let Some(content) = xml_tag(entry, "content") {
                    item.insert("content".to_string(), Value::String(content));
                }
                if !xml_tags(entry, "category").is_empty() {
                    item.insert(
                        "categories".to_string(),
                        Value::Array(
                            xml_tags(entry, "category")
                                .into_iter()
                                .map(Value::String)
                                .collect(),
                        ),
                    );
                }
                Value::Object(item)
            })
            .collect();
        let mut feed = serde_json::Map::new();
        if let Some(title) = title {
            feed.insert("title".to_string(), Value::String(title));
        }
        if let Some(link) = link {
            feed.insert("link".to_string(), Value::String(link));
        }
        feed.insert("feedUrl".to_string(), Value::String(feed_url.to_string()));
        feed.insert("items".to_string(), Value::Array(items));
        return Some(Value::Object(feed));
    }
    // RSS 2.0 (`<channel>` + `<item>`).
    if text.contains("<channel") || text.contains("<rss") {
        let channel = xml_tag(text, "channel").unwrap_or_else(|| text.to_string());
        let mut feed = serde_json::Map::new();
        if let Some(title) = xml_tag(&channel, "title") {
            feed.insert("title".to_string(), Value::String(title));
        }
        if let Some(link) = xml_tag(&channel, "link") {
            feed.insert("link".to_string(), Value::String(link));
        }
        if let Some(description) = xml_tag(&channel, "description") {
            feed.insert("description".to_string(), Value::String(description));
        }
        feed.insert("feedUrl".to_string(), Value::String(feed_url.to_string()));
        let items: Vec<Value> = xml_tags(&channel, "item")
            .iter()
            .map(|item_xml| {
                let mut item = serde_json::Map::new();
                for key in [
                    "title",
                    "link",
                    "guid",
                    "pubDate",
                    "description",
                    "content:encoded",
                ] {
                    if let Some(value) = xml_tag(item_xml, key) {
                        let out_key = if key == "description" {
                            "summary"
                        } else if key == "content:encoded" {
                            "content"
                        } else {
                            key
                        };
                        item.insert(out_key.to_string(), Value::String(value));
                    }
                }
                if let Some(creator) =
                    xml_tag(item_xml, "dc:creator").or_else(|| xml_tag(item_xml, "author"))
                {
                    item.insert("creator".to_string(), Value::String(creator));
                }
                if let Some(pub_date) = item.get("pubDate").cloned() {
                    item.insert("isoDate".to_string(), pub_date);
                }
                if !xml_tags(item_xml, "category").is_empty() {
                    item.insert(
                        "categories".to_string(),
                        Value::Array(
                            xml_tags(item_xml, "category")
                                .into_iter()
                                .map(Value::String)
                                .collect(),
                        ),
                    );
                }
                Value::Object(item)
            })
            .collect();
        feed.insert("items".to_string(), Value::Array(items));
        return Some(Value::Object(feed));
    }
    None
}
