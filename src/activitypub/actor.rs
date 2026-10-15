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
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::activitypub::format_millis;
use crate::activitypub::objects::{
    AP_CONTENT_TYPE, ap_context, is_ap_accept, render_create_or_announce_for_note,
};
use crate::activitypub::webfinger::extract_host;
use crate::application::commands::AppServices;
use crate::application::ports::{PortError, TimelineCursor};
use crate::domain::account::Account;
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

/// `GET /users/{id}/outbox` — ActivityPub `OrderedCollection` / `OrderedCollectionPage`
/// of the actor's public/home non-local-only notes wrapped in `Create` or `Announce`.
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

    let notes = match services
        .notes
        .notes_by_author(&account.id, &TimelineCursor::with_limit(50))
    {
        Ok(n) => n,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let viewer = ViewerContext::anonymous();
    let visible_notes: Vec<_> = notes
        .into_iter()
        .filter(|n| {
            !n.local_only
                && matches!(n.visibility, Visibility::Public | Visibility::Home)
                && is_visible(n, &viewer)
        })
        .collect();

    if params.get("page").map(|v| v == "true").unwrap_or(false) {
        let mut items = Vec::with_capacity(visible_notes.len());
        for note in &visible_notes {
            if let Ok(act) = render_create_or_announce_for_note(&services, note) {
                items.push(act);
            }
        }
        let page = json!({
            "@context": ap_context(),
            "id": format!("{outbox_url}?page=true"),
            "type": "OrderedCollectionPage",
            "partOf": outbox_url,
            "totalItems": visible_notes.len(),
            "orderedItems": items,
        });
        ap_json_response(StatusCode::OK, &page, None)
    } else {
        let collection = json!({
            "@context": ap_context(),
            "id": outbox_url,
            "type": "OrderedCollection",
            "totalItems": visible_notes.len(),
            "first": format!("{outbox_url}?page=true"),
        });
        ap_json_response(StatusCode::OK, &collection, None)
    }
}

/// `GET /users/{id}/followers` — ActivityPub `OrderedCollection` of follower actor URIs.
pub async fn get_actor_followers(
    State(services): State<Arc<AppServices>>,
    Path(id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let account = match find_visible_local_user(&services, &id) {
        Ok(a) => a,
        Err(code) => return code.into_response(),
    };
    let base = services.public_url.trim_end_matches('/');
    let coll_url = format!("{base}/users/{}/followers", account.id);
    let follows = services
        .follows
        .followers_of(&account.id, None, 200)
        .unwrap_or_default();
    let mut uris = Vec::with_capacity(follows.len());
    for f in &follows {
        if let Ok(Some(acc)) = services.accounts.find_by_id(&f.follower_id) {
            uris.push(Value::String(acc.actor_url(base)));
        }
    }
    if params.get("page").map(|v| v == "true").unwrap_or(false) {
        let page = json!({
            "@context": ap_context(),
            "id": format!("{coll_url}?page=true"),
            "type": "OrderedCollectionPage",
            "partOf": coll_url,
            "totalItems": account.followers_count,
            "orderedItems": uris,
        });
        ap_json_response(StatusCode::OK, &page, None)
    } else {
        let collection = json!({
            "@context": ap_context(),
            "id": coll_url,
            "type": "OrderedCollection",
            "totalItems": account.followers_count,
            "first": format!("{coll_url}?page=true"),
        });
        ap_json_response(StatusCode::OK, &collection, None)
    }
}

/// `GET /users/{id}/following` — ActivityPub `OrderedCollection` of followee actor URIs.
pub async fn get_actor_following(
    State(services): State<Arc<AppServices>>,
    Path(id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let account = match find_visible_local_user(&services, &id) {
        Ok(a) => a,
        Err(code) => return code.into_response(),
    };
    let base = services.public_url.trim_end_matches('/');
    let coll_url = format!("{base}/users/{}/following", account.id);
    let follows = services
        .follows
        .following_of(&account.id, None, 200)
        .unwrap_or_default();
    let mut uris = Vec::with_capacity(follows.len());
    for f in &follows {
        if let Ok(Some(acc)) = services.accounts.find_by_id(&f.followee_id) {
            uris.push(Value::String(acc.actor_url(base)));
        }
    }
    if params.get("page").map(|v| v == "true").unwrap_or(false) {
        let page = json!({
            "@context": ap_context(),
            "id": format!("{coll_url}?page=true"),
            "type": "OrderedCollectionPage",
            "partOf": coll_url,
            "totalItems": account.following_count,
            "orderedItems": uris,
        });
        ap_json_response(StatusCode::OK, &page, None)
    } else {
        let collection = json!({
            "@context": ap_context(),
            "id": coll_url,
            "type": "OrderedCollection",
            "totalItems": account.following_count,
            "first": format!("{coll_url}?page=true"),
        });
        ap_json_response(StatusCode::OK, &collection, None)
    }
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
    };
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
            reactions: store.clone(),
            jobs: store.clone(),
            media: store.clone(),
            blobs,
            notifications: store.clone(),
            auth: store.clone(),
            streaming: Arc::new(NoopStreamingSink),
            dedup: store.clone() as Arc<dyn ActivityDedup>,
            federation_http: SafeHttpClient::new(SafeHttpConfig {
                allow_private_ips: true,
                ..Default::default()
            }),
            public_url: "https://pinetree.local".to_string(),
            max_file_size: 10 * 1024 * 1024,
            preserved_usernames: Vec::new(),
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
        }
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
}
