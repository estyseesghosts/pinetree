//! Local actor documents: the `Person`/`Service`/`Application` renderer,
//! the standalone public-key object, and the `GET /users/{id}`,
//! `GET /@{acct}`, and `GET /users/{id}/publickey` handlers.

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};

use crate::activitypub::format_millis;
use crate::activitypub::objects::{AP_CONTENT_TYPE, ap_context, is_ap_accept};
use crate::activitypub::webfinger::extract_host;
use crate::application::commands::AppServices;
use crate::application::ports::PortError;
use crate::domain::account::Account;

const CACHE_ACTOR: &str = "public, max-age=180";

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
        if let Some(uris) = account
            .also_known_as
            .as_deref()
            .filter(|uris| !uris.is_empty())
        {
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

pub(crate) fn ap_json_response(
    status: StatusCode,
    body: &Value,
    cache: Option<&'static str>,
) -> Response {
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

pub(crate) fn find_visible_local_user(
    services: &AppServices,
    id: &str,
) -> Result<Account, StatusCode> {
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
