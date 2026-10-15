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
    let (_, host) =
        parse_url_host(&uri).ok_or_else(|| format!("invalid actor id URL: {uri}"))?;

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
    let (_, inbox_host) =
        parse_url_host(&inbox).ok_or_else(|| format!("invalid inbox URL: {inbox}"))?;
    if inbox_host != host {
        return Err(format!(
            "actor inbox host {inbox_host} does not match actor host {host}"
        ));
    }

    let shared_inbox = obj
        .get("endpoints")
        .and_then(Value::as_object)
        .and_then(|ep| ep.get("sharedInbox"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned);

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
