//! ActivityPub Actor documents, key objects, and actor-scoped collection
//! endpoints (Phase 2 Slices A2 & B1).
//!
//! Canonical actor URLs take the `{public_url}/users/{account_id}` form and
//! never expose node hostnames.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::activitypub::format_millis;
use crate::activitypub::objects::{
    AP_CONTENT_TYPE, ap_context, is_ap_accept, render_create_or_announce_for_note,
};
use crate::activitypub::webfinger::extract_host;
use crate::application::commands::AppServices;
use crate::application::ports::{PortError, TimelineCursor};
use crate::domain::account::{Account, FollowVisibility};
use crate::domain::note::Visibility;
use crate::domain::visibility::{ViewerContext, is_visible};

const CACHE_ACTOR: &str = "public, max-age=180";

/// Parsed representation of an incoming or fetched remote ActivityPub Actor
/// (`Person`, `Service`, `Application`, `Group`, `Organization`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedRemoteActor {
    pub uri: String,
    pub actor_type: String,
    pub username: String,
    pub host: String,
    pub display_name: Option<String>,
    pub summary_html: Option<String>,
    pub summary_mfm: Option<String>,
    pub inbox: String,
    pub shared_inbox: Option<String>,
    pub outbox: Option<String>,
    pub followers_url: Option<String>,
    pub following_url: Option<String>,
    pub featured_url: Option<String>,
    pub public_key_id: String,
    pub public_key_pem: String,
    pub manually_approves_followers: bool,
    pub discoverable: bool,
    pub is_cat: bool,
    pub birthday: Option<String>,
    pub location: Option<String>,
    pub icon_url: Option<String>,
    pub image_url: Option<String>,
    /// Pre-parsed AP `Emoji` tags (`(shortcode, icon_url)`), capped at 32.
    /// WP8b ingest source for actor documents (metadata only).
    pub emojis: Vec<(String, String)>,
    /// Batch P: `alsoKnownAs` actor URIs (capped at 10, Misskey
    /// `ApPersonService` parity — drives the `i/move` alias handshake).
    pub also_known_as: Option<Vec<String>>,
    /// Batch P: `movedTo` actor URI (Misskey `movedToUri` parity).
    pub moved_to_uri: Option<String>,
}

impl ParsedRemoteActor {
    pub fn is_bot(&self) -> bool {
        matches!(self.actor_type.as_str(), "Service" | "Application")
    }
}

/// Extract `(scheme_and_host, host_only)` from an HTTP(S) URL.
pub fn parse_url_host(url: &str) -> Option<(String, String)> {
    let trimmed = url.trim();
    let (scheme, rest) = trimmed.split_once("://")?;
    let scheme_lower = scheme.to_ascii_lowercase();
    if scheme_lower != "http" && scheme_lower != "https" {
        return None;
    }
    let authority = rest.split(['/', '?', '#']).next()?;
    if authority.is_empty() {
        return None;
    }
    let host_port = authority.to_ascii_lowercase();
    Some((format!("{scheme_lower}://{host_port}"), host_port))
}

/// Check that `key_id` is owned by the actor at `actor_uri`: either equal
/// to it, or `{actor}#fragment` / `{actor}/sub-path`, in all cases on the
/// same origin (scheme + host) as the actor. Anything else (notably another
/// actor's id) is rejected so a malicious document cannot advertise a key
/// id it does not own and poison the public-key cache (Slice 2, C1).
fn key_id_belongs_to_actor(actor_uri: &str, key_id: &str) -> bool {
    let actor = actor_uri.trim();
    let key = key_id.trim();
    if actor.is_empty() || key.is_empty() {
        return false;
    }
    let (Some((actor_origin, _)), Some((key_origin, _))) =
        (parse_url_host(actor), parse_url_host(key))
    else {
        return false;
    };
    if actor_origin != key_origin {
        return false;
    }
    if key == actor {
        return true;
    }
    let Some(rest) = key.strip_prefix(actor) else {
        return false;
    };
    match rest.as_bytes().first() {
        // `{actor}#fragment` with a non-empty fragment.
        Some(b'#') => rest.len() > 1,
        // `{actor}/sub-path` with a non-empty path remainder (a bare `/`,
        // `/?query` or `/#fragment` is not a key under the actor).
        Some(b'/') => rest.len() > 1 && !matches!(rest.as_bytes()[1], b'?' | b'#'),
        _ => false,
    }
}

/// Parse an ActivityPub Actor JSON document into [`ParsedRemoteActor`].
pub fn parse_remote_actor(doc: &Value) -> Result<ParsedRemoteActor, String> {
    let obj = doc
        .as_object()
        .ok_or_else(|| "actor document must be a JSON object".to_string())?;
    let uri = obj
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| "actor missing id".to_string())?
        .trim()
        .to_string();
    let (actor_origin, host) =
        parse_url_host(&uri).ok_or_else(|| format!("invalid actor id URL: {uri}"))?;
    let actor_scheme = actor_origin.split("://").next().unwrap_or("");

    let actor_type = match obj.get("type") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Array(arr)) => arr
            .iter()
            .filter_map(Value::as_str)
            .find(|t| {
                matches!(
                    *t,
                    "Person" | "Service" | "Application" | "Group" | "Organization"
                )
            })
            .unwrap_or("Person")
            .to_string(),
        _ => return Err("actor missing type".to_string()),
    };
    if !matches!(
        actor_type.as_str(),
        "Person" | "Service" | "Application" | "Group" | "Organization"
    ) {
        return Err(format!("unsupported actor type: {actor_type}"));
    }

    let username = obj
        .get("preferredUsername")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "actor missing preferredUsername".to_string())?
        .to_string();

    let inbox = obj
        .get("inbox")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "actor missing inbox".to_string())?
        .to_string();
    let (inbox_origin, inbox_host) =
        parse_url_host(&inbox).ok_or_else(|| format!("invalid inbox URL: {inbox}"))?;
    if inbox_host != host {
        return Err(format!(
            "actor inbox host {inbox_host} does not match actor host {host}"
        ));
    }
    // Slice 3 (H5): the inbox is required, so violations reject the actor.
    // Length fails fast at parse, matching the `Account::validate` 1..=512
    // bound (re-checked at upsert); an https actor must not name an http
    // inbox (downgrade refused). Plain-http actors (local test rigs) still
    // pass.
    if inbox.chars().count() > 512 {
        return Err(format!("invalid inbox URL (over 512 chars): {inbox}"));
    }
    let inbox_scheme = inbox_origin.split("://").next().unwrap_or("");
    if actor_scheme == "https" && inbox_scheme == "http" {
        return Err(format!(
            "actor inbox {inbox} downgrades https actor {uri} to http"
        ));
    }

    let shared_inbox = obj
        .get("endpoints")
        .and_then(Value::as_object)
        .and_then(|ep| ep.get("sharedInbox"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
        // Slice 3 (H5): same parser and same rules as the personal inbox,
        // but never reject the actor — drop an invalid sharedInbox (None)
        // and fall back to the personal inbox, so actors without a usable
        // sharedInbox still federate.
        .filter(|s| {
            if s.chars().count() > 512 {
                return false;
            }
            let Some((shared_origin, shared_host)) = parse_url_host(s) else {
                return false;
            };
            if shared_host != host {
                return false;
            }
            let shared_scheme = shared_origin.split("://").next().unwrap_or("");
            !(actor_scheme == "https" && shared_scheme == "http")
        });

    let public_key = obj
        .get("publicKey")
        .and_then(Value::as_object)
        .ok_or_else(|| "actor missing publicKey object".to_string())?;
    let public_key_id = public_key
        .get("id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "actor publicKey missing id".to_string())?
        .to_string();
    if let Some(owner) = public_key.get("owner").and_then(Value::as_str) {
        if owner.trim() != uri {
            return Err(format!(
                "publicKey owner {} does not match actor id {}",
                owner.trim(),
                uri
            ));
        }
    }
    // Slice 2 (C1): the key id must belong to this actor (exact id,
    // fragment, or sub-path on the same origin). `owner` alone is not
    // enough: an attacker could omit it, or set it to its own id, while
    // advertising a victim's key id.
    if !key_id_belongs_to_actor(&uri, &public_key_id) {
        return Err(format!(
            "publicKey id {public_key_id} is not owned by actor id {uri}"
        ));
    }
    let public_key_pem = public_key
        .get("publicKeyPem")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "actor publicKey missing publicKeyPem".to_string())?
        .to_string();

    let display_name = obj
        .get("name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.chars().take(128).collect());
    let summary_html = obj
        .get("summary")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned);
    let summary_mfm = obj
        .get("_misskey_summary")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned);

    let outbox = obj
        .get("outbox")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let followers_url = obj
        .get("followers")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let following_url = obj
        .get("following")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let featured_url = obj
        .get("featured")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);

    let manually_approves_followers = obj
        .get("manuallyApprovesFollowers")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let discoverable = obj
        .get("discoverable")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let is_cat = obj.get("isCat").and_then(Value::as_bool).unwrap_or(false);
    // Batch P `i/move` handshake inputs (untrusted: trimmed, capped at 10
    // entries × 512 chars, non-URI strings kept verbatim — the handshake
    // compares substrings, like upstream `includes`).
    let also_known_as = match obj.get("alsoKnownAs") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => {
            let trimmed = s.trim();
            if trimmed.is_empty() || trimmed.chars().count() > 512 {
                None
            } else {
                Some(vec![trimmed.to_string()])
            }
        }
        Some(Value::Array(arr)) => {
            let mut uris = Vec::new();
            for entry in arr.iter().take(10) {
                if let Some(s) = entry.as_str().map(str::trim).filter(|s| {
                    !s.is_empty() && s.chars().count() <= 512
                }) {
                    uris.push(s.to_string());
                }
            }
            if uris.is_empty() { None } else { Some(uris) }
        }
        Some(_) => None,
    };
    let moved_to_uri = obj
        .get("movedTo")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty() && s.chars().count() <= 512)
        .map(ToOwned::to_owned);
    let birthday = obj
        .get("vcard:bday")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned);
    let location = obj
        .get("vcard:Address")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.chars().take(50).collect());

    let extract_image_url = |val: Option<&Value>| -> Option<String> {
        match val? {
            Value::String(s) if !s.trim().is_empty() => Some(s.trim().to_string()),
            Value::Object(m) => m
                .get("url")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(ToOwned::to_owned),
            Value::Array(arr) => arr.first().and_then(|first| {
                first
                    .get("url")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(ToOwned::to_owned)
            }),
            _ => None,
        }
    };
    let icon_url = extract_image_url(obj.get("icon"));
    let image_url = extract_image_url(obj.get("image"));

    // WP8b: actor-document `Emoji` tags (profile emoji), metadata only.
    let mut emojis: Vec<(String, String)> = Vec::new();
    if let Some(tag_val) = obj.get("tag") {
        let items: Vec<&serde_json::Value> = match tag_val {
            serde_json::Value::Array(arr) => arr.iter().collect(),
            other => vec![other],
        };
        for item in items {
            if emojis.len() >= 32 {
                break;
            }
            if item.get("type").and_then(|v| v.as_str()) != Some("Emoji") {
                continue;
            }
            if let Some((shortcode, url)) = crate::domain::emoji::parse_emoji_tag(item) {
                emojis.push((shortcode, url));
            }
        }
    }

    Ok(ParsedRemoteActor {
        uri,
        actor_type,
        username,
        host,
        display_name,
        summary_html,
        summary_mfm,
        inbox,
        shared_inbox,
        outbox,
        followers_url,
        following_url,
        featured_url,
        public_key_id,
        public_key_pem,
        manually_approves_followers,
        discoverable,
        is_cat,
        birthday,
        location,
        icon_url,
        image_url,
        emojis,
        also_known_as,
        moved_to_uri,
    })
}

/// Render a local account as an ActivityPub Actor (`Person` or `Service`)
/// JSON-LD document matching Misskey/Sharkey `renderPerson`.
pub fn render_actor_document(
    services: &AppServices,
    account: &Account,
) -> Result<Value, PortError> {
    let base = services.public_url.trim_end_matches('/');
    let actor_url = account.actor_url(base);
    let keypair = services.ensure_actor_keypair(&account.id)?;
    let created_ms = services
        .ids
        .parse_time(&account.id)
        .unwrap_or_else(|_| services.clock.now_ms());
    let published = format_millis(created_ms);

    let preferred_username = if account.username_lower == "system_actor" {
        "system.actor".to_string()
    } else {
        account.username.clone()
    };

    let actor_type = if account.username_lower == "system_actor" {
        "Application"
    } else if account.is_bot {
        "Service"
    } else {
        "Person"
    };

    let icon = match &account.avatar_id {
        Some(fid) => {
            let meta = services.media.get(fid)?;
            let url = meta
                .as_ref()
                .map(|m| m.url.clone())
                .unwrap_or_else(|| format!("{base}/files/{fid}"));
            let mime = meta
                .as_ref()
                .map(|m| m.mime.clone())
                .unwrap_or_else(|| "image/png".to_string());
            json!({
                "type": "Image",
                "url": url,
                "mediaType": mime,
                "sensitive": false,
            })
        }
        None => json!({
            "type": "Image",
            "url": format!("{base}/identicon/{}", account.id),
            "mediaType": "image/png",
            "sensitive": false,
        }),
    };

    let image = match &account.banner_id {
        Some(fid) => {
            let meta = services.media.get(fid)?;
            let url = meta
                .as_ref()
                .map(|m| m.url.clone())
                .unwrap_or_else(|| format!("{base}/files/{fid}"));
            let mime = meta
                .as_ref()
                .map(|m| m.mime.clone())
                .unwrap_or_else(|| "image/png".to_string());
            Value::Object(
                serde_json::from_value(json!({
                    "type": "Image",
                    "url": url,
                    "mediaType": mime,
                    "sensitive": false,
                }))
                .unwrap_or_default(),
            )
        }
        None => Value::Null,
    };

    let summary_html = account
        .description
        .as_deref()
        .map(|d| crate::activitypub::objects::mfm_to_html(d, base));

    let mut doc = json!({
        "@context": ap_context(),
        "type": actor_type,
        "id": actor_url,
        "inbox": format!("{actor_url}/inbox"),
        "outbox": format!("{actor_url}/outbox"),
        "followers": format!("{actor_url}/followers"),
        "following": format!("{actor_url}/following"),
        "featured": format!("{actor_url}/collections/featured"),
        "sharedInbox": format!("{base}/inbox"),
        "endpoints": {
            "sharedInbox": format!("{base}/inbox"),
        },
        "url": format!("{base}/@{}", account.username),
        "preferredUsername": preferred_username,
        "name": account.display_name.clone().unwrap_or_else(|| account.username.clone()),
        "summary": summary_html,
        "_misskey_summary": account.description,
        "icon": icon,
        "image": image,
        "tag": [],
        "attachment": [],
        "manuallyApprovesFollowers": account.is_locked,
        "discoverable": account.is_explorable,
        "indexable": account.is_explorable,
        "published": published,
        "publicKey": {
            "id": format!("{actor_url}#main-key"),
            "type": "Key",
            "owner": actor_url,
            "publicKeyPem": keypair.public_key_pem,
        },
        "isCat": account.is_cat,
        "vcard:bday": account.birthday,
        "vcard:Address": account.location,
    });

    // Batch P `i/move` handshake outputs (upstream `renderPerson`
    // `alsoKnownAs`/`movedTo` parity): omitted when unset, so old
    // documents keep their shape.
    if let Some(map) = doc.as_object_mut() {
        // Federated clips advertisement (pinetree-first DIVERGENCE):
        // listed clips surface through `{actor}/clips`; the reference is
        // omitted while the global switch is off (zero protocol
        // footprint) and never advertises unlisted clips (the index
        // itself filters to listed + federate-enabled).
        let federate_clips = services
            .instance_settings
            .get()
            .map(|s| s.federate_clips)
            .unwrap_or(false);
        if federate_clips {
            map.insert(
                "clips".to_string(),
                Value::String(format!("{actor_url}/clips")),
            );
        }
        if let Some(uris) = account.also_known_as.as_deref().filter(|uris| !uris.is_empty()) {
            map.insert(
                "alsoKnownAs".to_string(),
                Value::Array(uris.iter().map(|u| Value::String(u.clone())).collect()),
            );
        }
        if let Some(moved) = account.moved_to_uri.as_deref() {
            map.insert("movedTo".to_string(), Value::String(moved.to_string()));
        }
    }

    if image.is_null() {
        if let Some(map) = doc.as_object_mut() {
            map.remove("image");
        }
    }

    Ok(doc)
}

/// Render the standalone `/users/{id}/publickey` CryptographicKey object.
pub fn render_public_key_document(
    services: &AppServices,
    account: &Account,
) -> Result<Value, PortError> {
    let base = services.public_url.trim_end_matches('/');
    let actor_url = account.actor_url(base);
    let keypair = services.ensure_actor_keypair(&account.id)?;
    Ok(json!({
        "@context": ap_context(),
        "id": format!("{actor_url}#main-key"),
        "type": "Key",
        "owner": actor_url,
        "publicKeyPem": keypair.public_key_pem,
    }))
}

fn ap_json_response(status: StatusCode, body: &Value, cache: Option<&'static str>) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(AP_CONTENT_TYPE),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    if let Some(c) = cache {
        headers.insert(header::CACHE_CONTROL, HeaderValue::from_static(c));
    }
    let payload = serde_json::to_string(body).unwrap_or_else(|_| "{}".to_string());
    (status, headers, payload).into_response()
}

fn find_visible_local_user(services: &AppServices, id: &str) -> Result<Account, StatusCode> {
    let acc = services
        .accounts
        .find_by_id(id)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    if !acc.is_local() || acc.is_deleted {
        return Err(StatusCode::GONE);
    }
    if acc.is_suspended {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(acc)
}

/// `GET /users/{id}` — serves the ActivityPub Actor JSON-LD document (or a
/// `302 Found` redirect to `/@{username}` when a browser requests HTML).
pub async fn get_actor(
    State(services): State<Arc<AppServices>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let account = match find_visible_local_user(&services, &id) {
        Ok(a) => a,
        Err(code) => return code.into_response(),
    };
    if !is_ap_accept(&headers) {
        let base = services.public_url.trim_end_matches('/');
        let location = format!("{base}/@{}", account.username);
        let mut resp_headers = HeaderMap::new();
        if let Ok(hv) = HeaderValue::from_str(&location) {
            resp_headers.insert(header::LOCATION, hv);
        }
        return (StatusCode::FOUND, resp_headers, String::new()).into_response();
    }
    match render_actor_document(&services, &account) {
        Ok(doc) => ap_json_response(StatusCode::OK, &doc, Some(CACHE_ACTOR)),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// `GET /@{acct}` — serves the ActivityPub Actor document when requested with
/// an ActivityPub `Accept` header, or `404`/`200` profile placeholder otherwise.
pub async fn get_at_profile(
    State(services): State<Arc<AppServices>>,
    Path(acct): Path<String>,
    headers: HeaderMap,
) -> Response {
    let raw = acct.trim().trim_start_matches('@');
    let local_host = extract_host(&services.public_url);
    let username = if let Some((u, h)) = raw.split_once('@') {
        if !h.eq_ignore_ascii_case(&local_host) {
            return StatusCode::NOT_FOUND.into_response();
        }
        u
    } else {
        raw
    };
    let lookup_name = if username.eq_ignore_ascii_case("system.actor")
        || username.eq_ignore_ascii_case("instance.actor")
    {
        "system_actor".to_string()
    } else {
        username.to_ascii_lowercase()
    };
    let account = match services
        .accounts
        .find_by_username_and_host(&lookup_name, None)
    {
        Ok(Some(a)) if a.is_local() && !a.is_deleted && !a.is_suspended => a,
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    if is_ap_accept(&headers) {
        match render_actor_document(&services, &account) {
            Ok(doc) => ap_json_response(StatusCode::OK, &doc, Some(CACHE_ACTOR)),
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    } else {
        match render_actor_document(&services, &account) {
            Ok(doc) => ap_json_response(StatusCode::OK, &doc, Some(CACHE_ACTOR)),
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    }
}

/// `GET /users/{id}/publickey` — serves the actor's public key object.
pub async fn get_actor_public_key(
    State(services): State<Arc<AppServices>>,
    Path(id): Path<String>,
) -> Response {
    let account = match find_visible_local_user(&services, &id) {
        Ok(a) => a,
        Err(code) => return code.into_response(),
    };
    match render_public_key_document(&services, &account) {
        Ok(doc) => ap_json_response(StatusCode::OK, &doc, Some(CACHE_ACTOR)),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// Items per page for the outbox, followers and following (Slice 11;
/// `docs/activitypub-collections.md` §3).
const COLLECTION_PAGE_SIZE: usize = 40;
/// Raw notes read per store call while building an outbox page (§3.1).
const OUTBOX_SCAN_BATCH: usize = 40;
/// Upper bound on outbox store reads per request (§3.1).
const OUTBOX_MAX_SCAN_BATCHES: usize = 25;

/// Encodes an internal position as an opaque cursor: URL-safe base64, no padding.
fn encode_collection_cursor(position: &str) -> String {
    URL_SAFE_NO_PAD.encode(position.as_bytes())
}

/// Decodes a `cursor` query value to its internal position. Anything that is
/// not URL-safe unpadded base64 of non-empty UTF-8 is a 400.
fn decode_collection_cursor(raw: &str) -> Result<String, StatusCode> {
    let bytes = URL_SAFE_NO_PAD
        .decode(raw)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let position = String::from_utf8(bytes).map_err(|_| StatusCode::BAD_REQUEST)?;
    if position.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(position)
}

/// Reads the optional `cursor` query value. `Ok(None)` when absent.
fn collection_cursor(params: &HashMap<String, String>) -> Result<Option<String>, StatusCode> {
    params
        .get("cursor")
        .map(|raw| decode_collection_cursor(raw))
        .transpose()
}

/// `?page=true` selects a page; anything else is the index.
fn wants_collection_page(params: &HashMap<String, String>) -> bool {
    params.get("page").map(|v| v == "true").unwrap_or(false)
}

/// URL of one page. `cursor` is the internal position the page starts after.
fn collection_page_url(coll_url: &str, cursor: Option<&str>) -> String {
    match cursor {
        None => format!("{coll_url}?page=true"),
        Some(position) => format!(
            "{coll_url}?page=true&cursor={}",
            encode_collection_cursor(position)
        ),
    }
}

/// `OrderedCollection` index. `total_items` is `None` when the count is not
/// cheaply accurate (outbox); the field is then omitted.
fn ordered_collection_index(coll_url: &str, total_items: Option<u64>) -> Response {
    let mut collection = json!({
        "@context": ap_context(),
        "id": coll_url,
        "type": "OrderedCollection",
        "first": collection_page_url(coll_url, None),
    });
    if let Some(total) = total_items {
        collection["totalItems"] = json!(total);
    }
    ap_json_response(StatusCode::OK, &collection, None)
}

/// `OrderedCollectionPage`. `next_position` is `Some` only when more items exist.
fn ordered_collection_page(
    coll_url: &str,
    cursor: Option<&str>,
    next_position: Option<&str>,
    total_items: Option<u64>,
    items: Vec<Value>,
) -> Response {
    let mut page = json!({
        "@context": ap_context(),
        "id": collection_page_url(coll_url, cursor),
        "type": "OrderedCollectionPage",
        "partOf": coll_url,
        "orderedItems": items,
    });
    if let Some(total) = total_items {
        page["totalItems"] = json!(total);
    }
    if let Some(position) = next_position {
        page["next"] = Value::String(collection_page_url(coll_url, Some(position)));
    }
    ap_json_response(StatusCode::OK, &page, None)
}

/// `GET /users/{id}/outbox` — ActivityPub `OrderedCollection` / `OrderedCollectionPage`
/// of the actor's public/home non-local-only notes wrapped in `Create` or `Announce`.
/// Paging contract: `docs/activitypub-collections.md`.
pub async fn get_actor_outbox(
    State(services): State<Arc<AppServices>>,
    Path(id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let account = match find_visible_local_user(&services, &id) {
        Ok(a) => a,
        Err(code) => return code.into_response(),
    };
    let base = services.public_url.trim_end_matches('/');
    let outbox_url = format!("{base}/users/{}/outbox", account.id);
    if !wants_collection_page(&params) {
        // Outbox totalItems is omitted (docs/activitypub-collections.md §2).
        return ordered_collection_index(&outbox_url, None);
    }
    let cursor = match collection_cursor(&params) {
        Ok(c) => c,
        Err(code) => return code.into_response(),
    };

    let viewer = ViewerContext::anonymous();
    let mut items: Vec<Value> = Vec::new();
    // Note IDs of `items`, in the same order; the last kept one is the cursor.
    let mut item_note_ids: Vec<String> = Vec::new();
    // Last raw note read from the store; the next scan starts before it.
    let mut scan_position: Option<String> = cursor.clone();
    let mut exhausted = false;
    let mut batches: usize = 0;
    'scan: while !exhausted && batches < OUTBOX_MAX_SCAN_BATCHES {
        batches += 1;
        let batch_cursor = TimelineCursor {
            until_id: scan_position.clone(),
            ..TimelineCursor::with_limit(OUTBOX_SCAN_BATCH)
        };
        let notes = match services.notes.notes_by_author(&account.id, &batch_cursor) {
            Ok(notes) => notes,
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        };
        exhausted = notes.len() < OUTBOX_SCAN_BATCH;
        for note in &notes {
            scan_position = Some(note.id.clone());
            if note.local_only
                || !matches!(note.visibility, Visibility::Public | Visibility::Home)
                || !is_visible(note, &viewer)
            {
                continue;
            }
            match render_create_or_announce_for_note(&services, note) {
                Ok(activity) => {
                    items.push(activity);
                    item_note_ids.push(note.id.clone());
                }
                // A storage error is never a silent skip.
                Err(PortError::StorageUnavailable(_)) => {
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
                Err(err) => eprintln!(
                    "outbox: account {} note {} skipped: {err}",
                    account.id, note.id
                ),
            }
            // One visible item past the page size proves a `next` page exists.
            if items.len() > COLLECTION_PAGE_SIZE {
                break 'scan;
            }
        }
    }

    let next_position = if items.len() > COLLECTION_PAGE_SIZE {
        items.truncate(COLLECTION_PAGE_SIZE);
        item_note_ids.truncate(COLLECTION_PAGE_SIZE);
        item_note_ids.last().cloned()
    } else if !exhausted {
        // Scan budget spent: continue from the last raw note read so nothing is
        // skipped. The page may then be short or empty while `next` is present.
        scan_position
    } else {
        None
    };
    ordered_collection_page(
        &outbox_url,
        cursor.as_deref(),
        next_position.as_deref(),
        None,
        items,
    )
}

/// `403` for a followers/following collection whose owner restricted its
/// visibility (`ActivityPubServerService` ff-visibility parity: any
/// non-`public` visibility denies every requester, with no per-requester
/// exception). Carries upstream's `Cache-Control: public, max-age=30`.
fn forbidden_collection() -> Response {
    let mut resp = StatusCode::FORBIDDEN.into_response();
    resp.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=30"),
    );
    resp
}
/// `GET /users/{id}/followers` — ActivityPub `OrderedCollection` of follower actor URIs.
/// Paging contract: `docs/activitypub-collections.md`.
pub async fn get_actor_followers(
    State(services): State<Arc<AppServices>>,
    Path(id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let account = match find_visible_local_user(&services, &id) {
        Ok(a) => a,
        Err(code) => return code.into_response(),
    };
    if account.followers_visibility != FollowVisibility::Public {
        return forbidden_collection();
    }
    let base = services.public_url.trim_end_matches('/');
    let coll_url = format!("{base}/users/{}/followers", account.id);
    if !wants_collection_page(&params) {
        return ordered_collection_index(&coll_url, Some(account.followers_count));
    }
    let cursor = match collection_cursor(&params) {
        Ok(c) => c,
        Err(code) => return code.into_response(),
    };
    // One extra row tells us whether a `next` page exists.
    let fetched = match services.follows.followers_of(
        &account.id,
        cursor.as_deref(),
        COLLECTION_PAGE_SIZE + 1,
    ) {
        Ok(rows) => rows,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let has_more = fetched.len() > COLLECTION_PAGE_SIZE;
    let rows = &fetched[..fetched.len().min(COLLECTION_PAGE_SIZE)];
    let mut uris = Vec::with_capacity(rows.len());
    for row in rows {
        match services.accounts.find_by_id(&row.follower_id) {
            Ok(Some(acc)) => uris.push(Value::String(acc.actor_url(base))),
            Ok(None) => eprintln!(
                "followers: account {} follows {} but has no account row; skipped",
                row.follower_id, account.id
            ),
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    }
    let next = if has_more {
        rows.last().map(|r| r.follower_id.clone())
    } else {
        None
    };
    ordered_collection_page(
        &coll_url,
        cursor.as_deref(),
        next.as_deref(),
        Some(account.followers_count),
        uris,
    )
}

/// `GET /users/{id}/following` — ActivityPub `OrderedCollection` of followee actor URIs.
/// Paging contract: `docs/activitypub-collections.md`.
pub async fn get_actor_following(
    State(services): State<Arc<AppServices>>,
    Path(id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let account = match find_visible_local_user(&services, &id) {
        Ok(a) => a,
        Err(code) => return code.into_response(),
    };
    if account.following_visibility != FollowVisibility::Public {
        return forbidden_collection();
    }
    let base = services.public_url.trim_end_matches('/');
    let coll_url = format!("{base}/users/{}/following", account.id);
    if !wants_collection_page(&params) {
        return ordered_collection_index(&coll_url, Some(account.following_count));
    }
    let cursor = match collection_cursor(&params) {
        Ok(c) => c,
        Err(code) => return code.into_response(),
    };
    // One extra row tells us whether a `next` page exists.
    let fetched = match services.follows.following_of(
        &account.id,
        cursor.as_deref(),
        COLLECTION_PAGE_SIZE + 1,
    ) {
        Ok(rows) => rows,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let has_more = fetched.len() > COLLECTION_PAGE_SIZE;
    let rows = &fetched[..fetched.len().min(COLLECTION_PAGE_SIZE)];
    let mut uris = Vec::with_capacity(rows.len());
    for row in rows {
        match services.accounts.find_by_id(&row.followee_id) {
            Ok(Some(acc)) => uris.push(Value::String(acc.actor_url(base))),
            Ok(None) => eprintln!(
                "following: account {} follows {} but has no account row; skipped",
                account.id, row.followee_id
            ),
            Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    }
    let next = if has_more {
        rows.last().map(|r| r.followee_id.clone())
    } else {
        None
    };
    ordered_collection_page(
        &coll_url,
        cursor.as_deref(),
        next.as_deref(),
        Some(account.following_count),
        uris,
    )
}

/// `GET /users/{id}/collections/featured` — ActivityPub featured notes `OrderedCollection`.
pub async fn get_actor_featured(
    State(services): State<Arc<AppServices>>,
    Path(id): Path<String>,
) -> Response {
    let account = match find_visible_local_user(&services, &id) {
        Ok(a) => a,
        Err(code) => return code.into_response(),
    };
    let base = services.public_url.trim_end_matches('/');
    let coll_url = format!("{base}/users/{}/collections/featured", account.id);
    let collection = json!({
        "@context": ap_context(),
        "id": coll_url,
        "type": "OrderedCollection",
        "totalItems": 0,
        "orderedItems": [],
    });
    ap_json_response(StatusCode::OK, &collection, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    use serde_json::json;

    use crate::application::ports::{
        AccountStore, ActivityDedup, BlobStore, Clock, IdGenerator, NoopStreamingSink,
        UpdateAccountPatch,
    };
    use crate::domain::account::FollowVisibility;
    use crate::domain::id::{AidxGenerator, ManualClock};
    use crate::federation::delivery::{
        SafeHttpClient, SafeHttpConfig, upsert_remote_actor_from_doc,
    };
    use crate::media::storage::FsBlobStore;
    use crate::persistence::redb::RedbStore;

    /// Fixture PEM: `parse_remote_actor` and the key cache store the string
    /// opaquely, so signature validity is irrelevant here.
    const FIXTURE_PEM: &str =
        "-----BEGIN PUBLIC KEY-----\nZmFrZXJlbW90ZXN0a2V5\n-----END PUBLIC KEY-----";

    fn actor_doc(id: &str, username: &str, inbox: &str, shared_inbox: Option<&str>) -> Value {
        let mut doc = json!({
            "id": id,
            "type": "Person",
            "preferredUsername": username,
            "inbox": inbox,
            "publicKey": {
                "id": format!("{id}#main-key"),
                "type": "Key",
                "owner": id,
                "publicKeyPem": FIXTURE_PEM,
            },
        });
        if let Some(shared) = shared_inbox {
            doc["endpoints"] = json!({ "sharedInbox": shared });
        }
        doc
    }

    fn test_services(prefix: &str) -> AppServices {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinetree-inbox-url-{}-{prefix}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(1_760_400_000_000));
        let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
        let store = RedbStore::open(dir.join("test.redb"), clock.clone(), ids.clone()).unwrap();
        let blobs: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir.join("blobs")).unwrap());
        AppServices {
            clock,
            ids,
            accounts: store.clone(),
            notes: store.clone(),
            follows: store.clone(),
            blocks: store.clone(),
            mutes: store.clone(),
            reactions: store.clone(),
            favorites: store.clone(),
            emojis: store.clone(),
            antennas: store.clone(),
            antenna_notes: store.clone(),
            polls: store.clone(),
            jobs: store.clone(),
            media: store.clone(),
            blobs: blobs.clone(),
            notifications: store.clone(),
            auth: store.clone(),
            streaming: Arc::new(NoopStreamingSink),
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
            federation_http: SafeHttpClient::new(SafeHttpConfig {
                allow_private_ips: true,
                ..Default::default()
            }),
            public_url: "https://pinetree.local".to_string(),
            max_file_size: 10 * 1024 * 1024,
            preserved_usernames: Vec::new(),
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
            note_draft_limit: crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
            scheduled_note_limit: crate::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
            pin_limit: crate::domain::pinning::DEFAULT_PIN_LIMIT,
            default_like: None,
        cache_blobs: blobs.clone(),
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
        }
    }

    #[test]
    fn batch_p_alias_and_moved_round_trip() {
        let doc = actor_doc(
            "https://remote.example/users/1",
            "bob",
            "https://remote.example/users/1/inbox",
            None,
        );
        let mut doc = doc;
        doc["alsoKnownAs"] = json!(["https://local.example/users/9", "https://x.example/u"]);
        doc["movedTo"] = json!("https://remote.example/users/2");
        let parsed = parse_remote_actor(&doc).expect("parses");
        assert_eq!(
            parsed.also_known_as,
            Some(vec![
                "https://local.example/users/9".to_string(),
                "https://x.example/u".to_string()
            ])
        );
        assert_eq!(
            parsed.moved_to_uri,
            Some("https://remote.example/users/2".to_string())
        );
    }

    // Slice 3 (H5): a sharedInbox on another host is dropped (None) so
    // delivery falls back to the personal inbox; the actor still parses.
    #[test]
    fn shared_inbox_on_other_host_falls_back_to_personal_inbox() {
        let doc = actor_doc(
            "https://remote.example/users/alice",
            "alice",
            "https://remote.example/users/alice/inbox",
            Some("https://relay.other.example/inbox"),
        );
        let parsed = parse_remote_actor(&doc)
            .expect("actor with third-party sharedInbox must still parse");
        assert_eq!(parsed.inbox, "https://remote.example/users/alice/inbox");
        assert_eq!(parsed.shared_inbox, None);
    }

    // Slice 3 (H5): an https actor must not name an http sharedInbox
    // (downgrade refused); drop it, don't reject the actor.
    #[test]
    fn https_actor_http_shared_inbox_downgrade_dropped() {
        let doc = actor_doc(
            "https://remote.example/users/alice",
            "alice",
            "https://remote.example/users/alice/inbox",
            Some("http://remote.example/inbox"),
        );
        let parsed = parse_remote_actor(&doc)
            .expect("actor with downgraded sharedInbox must still parse");
        assert_eq!(parsed.shared_inbox, None);
    }

    // Slice 3 (H5): an unparseable sharedInbox is dropped, not fatal.
    #[test]
    fn unparseable_shared_inbox_dropped() {
        let doc = actor_doc(
            "https://remote.example/users/alice",
            "alice",
            "https://remote.example/users/alice/inbox",
            Some("not a url"),
        );
        let parsed = parse_remote_actor(&doc)
            .expect("actor with garbage sharedInbox must still parse");
        assert_eq!(parsed.shared_inbox, None);
    }

    // Slice 3 (H5): the personal inbox is required, so an https→http
    // downgrade there rejects the whole actor.
    #[test]
    fn https_actor_http_personal_inbox_rejected() {
        let doc = actor_doc(
            "https://remote.example/users/alice",
            "alice",
            "http://remote.example/users/alice/inbox",
            Some("https://remote.example/inbox"),
        );
        assert!(
            parse_remote_actor(&doc).is_err(),
            "https actor with http inbox must be rejected"
        );
    }

    // Slice 3 (H5): overlong URLs — sharedInbox (>512 chars) drops to None,
    // personal inbox (>512) rejects the actor (fail fast at parse, matching
    // the `Account::validate` 512-char bound).
    #[test]
    fn overlong_inbox_urls_rejected_or_dropped() {
        let long_shared = format!("https://remote.example/{}", "x".repeat(600));
        assert!(
            long_shared.chars().count() > 512,
            "fixture must exceed the 512-char bound"
        );
        let doc = actor_doc(
            "https://remote.example/users/alice",
            "alice",
            "https://remote.example/users/alice/inbox",
            Some(long_shared.as_str()),
        );
        let parsed = parse_remote_actor(&doc)
            .expect("actor with overlong sharedInbox must still parse");
        assert_eq!(parsed.shared_inbox, None);

        let long_inbox = format!(
            "https://remote.example/users/alice/inbox/{}",
            "x".repeat(600)
        );
        assert!(
            long_inbox.chars().count() > 512,
            "fixture must exceed the 512-char bound"
        );
        let doc = actor_doc(
            "https://remote.example/users/alice",
            "alice",
            &long_inbox,
            Some("https://remote.example/inbox"),
        );
        assert!(
            parse_remote_actor(&doc).is_err(),
            "overlong personal inbox must be rejected"
        );
    }

    // Slice 3 (H5): plain-http actors (local test rigs use http) keep working.
    #[test]
    fn http_actor_with_http_inboxes_accepted() {
        let doc = actor_doc(
            "http://127.0.0.1:3000/users/alice",
            "alice",
            "http://127.0.0.1:3000/users/alice/inbox",
            Some("http://127.0.0.1:3000/inbox"),
        );
        let parsed = parse_remote_actor(&doc).expect("http actor must still parse");
        assert_eq!(parsed.inbox, "http://127.0.0.1:3000/users/alice/inbox");
        assert_eq!(
            parsed.shared_inbox.as_deref(),
            Some("http://127.0.0.1:3000/inbox")
        );
    }

    // Slice 3 (H5) upsert level: sharedInbox fallback flows into
    // `account.shared_inbox == None` with the personal inbox kept, and
    // `Account::validate` is enforced on the upsert path.
    #[test]
    fn upsert_applies_inbox_validation_and_shared_inbox_fallback() {
        let services = test_services("upsert");

        // Third-party sharedInbox: upsert succeeds, falls back to personal.
        let id = "https://remote.example/users/alice";
        let doc = actor_doc(
            id,
            "alice",
            "https://remote.example/users/alice/inbox",
            Some("https://relay.other.example/inbox"),
        );
        let (account, parsed) =
            upsert_remote_actor_from_doc(&services, id, &doc)
                .expect("third-party sharedInbox must fall back");
        assert_eq!(parsed.shared_inbox, None);
        assert_eq!(account.shared_inbox, None);
        assert_eq!(
            account.inbox.as_deref(),
            Some("https://remote.example/users/alice/inbox")
        );

        // https→http sharedInbox downgrade: upsert succeeds with None.
        let id = "https://remote.example/users/bob";
        let doc = actor_doc(
            id,
            "bob",
            "https://remote.example/users/bob/inbox",
            Some("http://remote.example/inbox"),
        );
        let (account, _) =
            upsert_remote_actor_from_doc(&services, id, &doc)
                .expect("downgraded sharedInbox must fall back");
        assert_eq!(account.shared_inbox, None);

        // Overlong sharedInbox: upsert succeeds with None (not a store error).
        let long_shared = format!("https://remote.example/{}", "x".repeat(600));
        let id = "https://remote.example/users/carol";
        let doc = actor_doc(
            id,
            "carol",
            "https://remote.example/users/carol/inbox",
            Some(long_shared.as_str()),
        );
        let (account, _) =
            upsert_remote_actor_from_doc(&services, id, &doc)
                .expect("overlong sharedInbox must fall back");
        assert_eq!(account.shared_inbox, None);

        // https actor + http personal inbox: upsert rejected.
        let id = "https://remote.example/users/dave";
        let doc = actor_doc(
            id,
            "dave",
            "http://remote.example/users/dave/inbox",
            Some("https://remote.example/inbox"),
        );
        assert!(
            upsert_remote_actor_from_doc(&services, id, &doc).is_err(),
            "https actor with http inbox must not upsert"
        );

        // `Account::validate` on the upsert path: a hostile remote
        // preferredUsername that fails domain validation must not upsert.
        let id = "https://remote.example/users/erin";
        let doc = actor_doc(
            id,
            "not a valid name!",
            "https://remote.example/users/erin/inbox",
            Some("https://remote.example/inbox"),
        );
        // Sanity: the username really is domain-invalid.
        assert!(
            crate::domain::account::validate_remote_username("not a valid name!").is_err()
        );
        assert!(
            upsert_remote_actor_from_doc(&services, id, &doc).is_err(),
            "domain-invalid username must not upsert"
        );
        // The failed upsert must not have cached anything.
        assert!(
            services.accounts.find_by_uri(id).expect("lookup").is_none(),
            "rejected actor must leave no account row"
        );
    }

    // ---- Slice 11 (M7): AP collection paging. Tests call the real handlers
    // and read the JSON body back. Contract: docs/activitypub-collections.md.

    use crate::application::ports::{CreateAccountInput, CreateNoteInput, FollowStore, NewJob, NoteStore};
    use crate::domain::follow::{Follow, FollowNotify};
    use std::collections::HashSet;

    const SLICE11_BASE: &str = "https://pinetree.local";

    /// Calls one of the three paged collection handlers and returns the
    /// status plus the parsed JSON body (`Value::Null` when there is none).
    async fn slice11_get(
        services: &Arc<AppServices>,
        collection: &str,
        account_id: &str,
        params: &[(&str, &str)],
    ) -> (StatusCode, Value) {
        let query: HashMap<String, String> = params
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let state = State(services.clone());
        let path = Path(account_id.to_string());
        let query = Query(query);
        let response = match collection {
            "outbox" => get_actor_outbox(state, path, query).await,
            "followers" => get_actor_followers(state, path, query).await,
            "following" => get_actor_following(state, path, query).await,
            other => panic!("unknown collection {other}"),
        };
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(Value::Null)
        };
        (status, body)
    }

    /// Starts at `?page=true` and follows `next` links until none is returned.
    /// Asserts every `next` has the documented `{coll}?page=true&cursor=` form.
    async fn slice11_walk(
        services: &Arc<AppServices>,
        collection: &str,
        account_id: &str,
    ) -> Vec<Value> {
        let coll_url = format!("{SLICE11_BASE}/users/{account_id}/{collection}");
        let prefix = format!("{coll_url}?page=true&cursor=");
        let (status, first) =
            slice11_get(services, collection, account_id, &[("page", "true")]).await;
        assert_eq!(status, StatusCode::OK, "first page of {collection}");
        let mut pages = Vec::new();
        let mut current = first;
        loop {
            assert!(pages.len() < 200, "paging of {collection} did not terminate");
            let next = current
                .get("next")
                .and_then(Value::as_str)
                .map(str::to_string);
            pages.push(current);
            let Some(next) = next else { break };
            assert!(
                next.starts_with(&prefix),
                "next has unexpected form: {next}"
            );
            let cursor = next[prefix.len()..].to_string();
            assert!(!cursor.is_empty(), "next cursor must not be empty");
            let (status, page) = slice11_get(
                services,
                collection,
                account_id,
                &[("page", "true"), ("cursor", cursor.as_str())],
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{collection} page with cursor");
            current = page;
        }
        pages
    }

    fn slice11_local(services: &AppServices, handle: &str) -> Account {
        services
            .accounts
            .create_account(CreateAccountInput::local(handle), None)
            .expect("create local account")
    }

    /// Storage double: delegates everything to the real store, but the two
    /// collection reads fail with a storage error (Slice 11 500 test).
    struct Slice11FailingFollows {
        inner: Arc<dyn FollowStore>,
    }

    impl FollowStore for Slice11FailingFollows {
        fn follow(
            &self,
            follower_id: &str,
            followee_id: &str,
            with_replies: bool,
            notify: Option<FollowNotify>,
            jobs: Vec<NewJob>,
        ) -> Result<Follow, PortError> {
            self.inner
                .follow(follower_id, followee_id, with_replies, notify, jobs)
        }
        fn unfollow(
            &self,
            follower_id: &str,
            followee_id: &str,
            jobs: Vec<NewJob>,
        ) -> Result<(), PortError> {
            self.inner.unfollow(follower_id, followee_id, jobs)
        }
        fn is_following(&self, follower_id: &str, followee_id: &str) -> Result<bool, PortError> {
            self.inner.is_following(follower_id, followee_id)
        }
        fn get_follow(
            &self,
            follower_id: &str,
            followee_id: &str,
        ) -> Result<Option<Follow>, PortError> {
            self.inner.get_follow(follower_id, followee_id)
        }
        fn followers_of(
            &self,
            _account_id: &str,
            _cursor: Option<&str>,
            _limit: usize,
        ) -> Result<Vec<Follow>, PortError> {
            Err(PortError::StorageUnavailable(
                "injected follow-store failure".to_string(),
            ))
        }
        fn following_of(
            &self,
            _account_id: &str,
            _cursor: Option<&str>,
            _limit: usize,
        ) -> Result<Vec<Follow>, PortError> {
            Err(PortError::StorageUnavailable(
                "injected follow-store failure".to_string(),
            ))
        }
    }

    // (a) 250 followers page through completely: no duplicates, none missing,
    // last page has no `next`, and the index totalItems equals the counter.
    #[tokio::test]
    async fn slice11_followers_250_page_through_completely() {
        let services = Arc::new(test_services("s11-followers"));
        let target = slice11_local(&services, "target");
        let mut expected: Vec<String> = Vec::new();
        for i in 0..250 {
            let fan = slice11_local(&services, &format!("fan{i:03}"));
            services
                .follows
                .follow(&fan.id, &target.id, false, None, Vec::new())
                .expect("follow");
            expected.push(fan.actor_url(SLICE11_BASE));
        }
        let counter = services
            .accounts
            .find_by_id(&target.id)
            .expect("lookup")
            .expect("target exists")
            .followers_count;
        assert_eq!(counter, 250);

        let coll_url = format!("{SLICE11_BASE}/users/{}/followers", target.id);
        let (status, index) =
            slice11_get(&services, "followers", &target.id, &[]).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(index["type"].as_str(), Some("OrderedCollection"));
        assert_eq!(index["id"].as_str(), Some(coll_url.as_str()));
        assert_eq!(index["totalItems"].as_u64(), Some(counter));
        assert_eq!(
            index["first"].as_str(),
            Some(format!("{coll_url}?page=true").as_str())
        );
        assert!(index.get("orderedItems").is_none());
        assert!(index.get("next").is_none());

        let pages = slice11_walk(&services, "followers", &target.id).await;
        assert_eq!(pages.len(), 7, "250 items at 40 per page is 7 pages");
        let mut seen: Vec<String> = Vec::new();
        for (i, page) in pages.iter().enumerate() {
            assert_eq!(page["type"].as_str(), Some("OrderedCollectionPage"));
            assert_eq!(page["partOf"].as_str(), Some(coll_url.as_str()));
            assert_eq!(page["totalItems"].as_u64(), Some(counter));
            let items = page["orderedItems"].as_array().expect("orderedItems array");
            if i + 1 < pages.len() {
                assert_eq!(items.len(), 40, "full page {i}");
                assert!(page.get("next").is_some(), "page {i} must carry next");
            } else {
                assert_eq!(items.len(), 10, "last page holds the remainder");
                assert!(page.get("next").is_none(), "last page must not carry next");
            }
            assert!(page.get("prev").is_none(), "prev is never emitted");
            for item in items {
                seen.push(item.as_str().expect("actor uri string").to_string());
            }
        }
        let unique: HashSet<&String> = seen.iter().collect();
        assert_eq!(seen.len(), 250, "no duplicates across pages");
        assert_eq!(unique.len(), 250, "no duplicates across pages");
        let mut expected_sorted = expected.clone();
        expected_sorted.sort();
        let mut seen_sorted = seen.clone();
        seen_sorted.sort();
        assert_eq!(seen_sorted, expected_sorted, "none missing across pages");
    }

    // (b) A storage error in the follow store returns 500, not an empty
    // collection.
    #[tokio::test]
    async fn slice11_follow_store_failure_returns_500() {
        let mut services = test_services("s11-500");
        let target = slice11_local(&services, "victim");
        let fan = slice11_local(&services, "fan");
        services
            .follows
            .follow(&fan.id, &target.id, false, None, Vec::new())
            .expect("follow");
        let failing: Arc<dyn FollowStore> = Arc::new(Slice11FailingFollows {
            inner: services.follows.clone(),
        });
        services.follows = failing;
        let services = Arc::new(services);

        for collection in ["followers", "following"] {
            let (status, _) =
                slice11_get(&services, collection, &target.id, &[("page", "true")]).await;
            assert_eq!(
                status,
                StatusCode::INTERNAL_SERVER_ERROR,
                "{collection} page must surface the storage error"
            );
        }
    }

    // (b2) §§1–2 parity: any non-`public` followers/following visibility
    // denies the ActivityPub collections (index and page alike) with 403 —
    // no per-requester exception.
    #[tokio::test]
    async fn ff_visibility_non_public_collections_are_403() {
        let services = Arc::new(test_services("s11-ffvis"));
        let target = slice11_local(&services, "shy");
        let fan = slice11_local(&services, "fan");
        services
            .follows
            .follow(&fan.id, &target.id, false, None, Vec::new())
            .expect("follow");

        for visibility in [FollowVisibility::Followers, FollowVisibility::Private] {
            for list in ["followers", "following"] {
                services
                    .accounts
                    .update_account(
                        &target.id,
                        UpdateAccountPatch {
                            followers_visibility: Some(visibility),
                            following_visibility: Some(visibility),
                            ..Default::default()
                        },
                        Vec::new(),
                    )
                    .expect("set visibility");
                for params in
                    [Vec::new(), vec![("page", "true")]]
                {
                    let (status, _) =
                        slice11_get(&services, list, &target.id, &params).await;
                    assert_eq!(
                        status,
                        StatusCode::FORBIDDEN,
                        "{list} {visibility:?} {params:?} must be 403"
                    );
                }
            }
        }

        // Back to public: both collections read again.
        services
            .accounts
            .update_account(
                &target.id,
                UpdateAccountPatch {
                    followers_visibility: Some(FollowVisibility::Public),
                    following_visibility: Some(FollowVisibility::Public),
                    ..Default::default()
                },
                Vec::new(),
            )
            .expect("set public");
        for (list, params) in [
            ("followers", Vec::new()),
            ("followers", vec![("page", "true")]),
            ("following", Vec::new()),
            ("following", vec![("page", "true")]),
        ] {
            let (status, _) = slice11_get(&services, list, &target.id, &params).await;
            assert_eq!(status, StatusCode::OK, "{list} {params:?} must read again");
        }
    }

    // (c) Outbox: more than one page of public notes, with Followers-only
    // notes interleaved. Public notes page exactly once; hidden ones never leak.
    #[tokio::test]
    async fn slice11_outbox_pages_public_notes_skipping_non_public() {
        let services = Arc::new(test_services("s11-outbox"));
        let author = slice11_local(&services, "poster");
        let mut public_ids: Vec<String> = Vec::new();
        let mut hidden_ids: Vec<String> = Vec::new();
        for i in 0..100 {
            let mut input = CreateNoteInput::simple_public(author.id.clone(), format!("note {i}"));
            if i % 3 == 0 {
                input.visibility = Visibility::Followers;
                hidden_ids.push(
                    services
                        .notes
                        .create_note(input, Vec::new())
                        .expect("create hidden note")
                        .id,
                );
            } else {
                public_ids.push(
                    services
                        .notes
                        .create_note(input, Vec::new())
                        .expect("create public note")
                        .id,
                );
            }
        }
        assert_eq!(public_ids.len(), 66);
        assert_eq!(hidden_ids.len(), 34);

        let coll_url = format!("{SLICE11_BASE}/users/{}/outbox", author.id);
        let (status, index) = slice11_get(&services, "outbox", &author.id, &[]).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(index["type"].as_str(), Some("OrderedCollection"));
        assert!(index.get("totalItems").is_none(), "outbox totalItems is omitted");

        let pages = slice11_walk(&services, "outbox", &author.id).await;
        assert_eq!(pages.len(), 2, "66 public notes at 40 per page is 2 pages");
        assert!(pages[1].get("next").is_none());
        assert!(pages[0].get("totalItems").is_none());
        assert_eq!(pages[0]["partOf"].as_str(), Some(coll_url.as_str()));

        let rendered: Vec<String> = pages
            .iter()
            .flat_map(|p| {
                p["orderedItems"]
                    .as_array()
                    .expect("orderedItems array")
                    .iter()
                    .map(|v| v.to_string())
            })
            .collect();
        assert_eq!(rendered.len(), 66, "every public note exactly once");
        for id in &public_ids {
            let hits = rendered.iter().filter(|r| r.contains(id.as_str())).count();
            assert_eq!(hits, 1, "public note {id} must appear exactly once");
        }
        for id in &hidden_ids {
            assert!(
                !rendered.iter().any(|r| r.contains(id.as_str())),
                "followers-only note {id} leaked into the outbox"
            );
        }
    }

    // (d) An invalid cursor is a 400 on every paged collection.
    #[tokio::test]
    async fn slice11_invalid_cursor_returns_400() {
        let services = Arc::new(test_services("s11-cursor"));
        let account = slice11_local(&services, "cursorvictim");
        // "!!!" and "a+b/c=" are not URL-safe unpadded base64; "" decodes to
        // nothing; "_w" decodes to 0xFF, which is not UTF-8.
        for collection in ["followers", "following", "outbox"] {
            for bad in ["!!!", "a+b/c=", "", "_w"] {
                let (status, _) = slice11_get(
                    &services,
                    collection,
                    &account.id,
                    &[("page", "true"), ("cursor", bad)],
                )
                .await;
                assert_eq!(
                    status,
                    StatusCode::BAD_REQUEST,
                    "{collection} with cursor {bad:?} must be 400"
                );
            }
        }
    }

    // (e) Following pages completely across three pages (40 + 40 + 5).
    #[tokio::test]
    async fn slice11_following_pages_completely() {
        let services = Arc::new(test_services("s11-following"));
        let reader = slice11_local(&services, "reader");
        let mut expected: Vec<String> = Vec::new();
        for i in 0..85 {
            let star = slice11_local(&services, &format!("star{i:03}"));
            services
                .follows
                .follow(&reader.id, &star.id, false, None, Vec::new())
                .expect("follow");
            expected.push(star.actor_url(SLICE11_BASE));
        }
        let (status, index) = slice11_get(&services, "following", &reader.id, &[]).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(index["totalItems"].as_u64(), Some(85));

        let pages = slice11_walk(&services, "following", &reader.id).await;
        assert_eq!(pages.len(), 3);
        let mut seen: Vec<String> = Vec::new();
        for (i, page) in pages.iter().enumerate() {
            let items = page["orderedItems"].as_array().expect("orderedItems array");
            let expected_len = if i < 2 { 40 } else { 5 };
            assert_eq!(items.len(), expected_len, "page {i} length");
            assert_eq!(page.get("next").is_some(), i < 2, "next only before the last page");
            for item in items {
                seen.push(item.as_str().expect("actor uri string").to_string());
            }
        }
        let mut expected_sorted = expected.clone();
        expected_sorted.sort();
        let mut seen_sorted = seen.clone();
        seen_sorted.sort();
        assert_eq!(seen_sorted, expected_sorted, "no duplicates, none missing");
    }
}
