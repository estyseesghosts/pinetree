//! WebFinger resource resolution (JRD + XRD) and the
//! `GET /.well-known/webfinger` handler.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::Response,
};
use serde_json::{Value, json};

use crate::application::commands::AppServices;
use crate::domain::account::Account;

use super::responses::{
    CONTENT_TYPE_JRD, CONTENT_TYPE_XRD, base_url, error_response, extract_host, json_response,
    text_response, xml_escape,
};

const CACHE_WEBFINGER: &str = "public, max-age=180";

fn account_visible(account: &Account) -> bool {
    account.is_local() && !account.is_suspended && !account.is_deleted
}

fn find_local_account(
    services: &AppServices,
    username: &str,
) -> Result<Option<Account>, StatusCode> {
    let username = username.trim();
    if username.is_empty() {
        return Ok(None);
    }
    let lower = username.to_ascii_lowercase();
    if let Some(acc) = services
        .accounts
        .find_by_username_and_host(&lower, None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    {
        return Ok(Some(acc));
    }
    if lower == "system.actor" || lower == "instance.actor" {
        if let Some(acc) = services
            .accounts
            .find_by_username_and_host("system_actor", None)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        {
            return Ok(Some(acc));
        }
    }
    Ok(None)
}

/// Resolve a WebFinger `resource` to a visible local account.
///
/// Accepts (case-insensitively) `{base}/users/{id}`, `{base}/@{username}`,
/// `acct:{user}@{host}`, `{user}@{host}`, `acct:{user}`, and `{user}`.
/// A foreign `@{host}` is `422` (matching Misskey/Sharkey
/// `WellKnownServerService.ts`); unknown, remote, suspended, or deleted
/// accounts are `404`.
pub fn resolve_webfinger_resource(
    services: &AppServices,
    resource: &str,
) -> Result<Account, StatusCode> {
    let base = base_url(&services.public_url).to_string();
    let base_lower = base.to_ascii_lowercase();
    let host = extract_host(&base);
    let resource = resource.trim();
    if resource.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }

    // Optional `acct:` scheme (case-insensitive).
    let mut candidate = resource;
    if let Some(prefix) = candidate.get(.."acct:".len()) {
        if prefix.eq_ignore_ascii_case("acct:") {
            candidate = candidate["acct:".len()..].trim();
            if candidate.is_empty() {
                return Err(StatusCode::NOT_FOUND);
            }
        }
    }

    let candidate_lower = candidate.to_ascii_lowercase();
    let users_prefix = format!("{base_lower}/users/");
    let profile_prefix = format!("{base_lower}/@");
    let found = if candidate_lower
        .strip_prefix(users_prefix.as_str())
        .map(|_| &candidate[users_prefix.len()..])
        .is_some()
    {
        // `{base}/users/{id}`: the id keeps its given case (aidx ids are
        // lowercase, so this also matches a lowercased lookup).
        let rest = &candidate[users_prefix.len()..];
        let id = rest.split(['/', '?', '#']).next().unwrap_or("");
        if id.is_empty() {
            return Err(StatusCode::NOT_FOUND);
        }
        services
            .accounts
            .find_by_id(id)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    } else if let Some(rest) = candidate_lower
        .strip_prefix(profile_prefix.as_str())
        .map(|_| &candidate[profile_prefix.len()..])
    {
        // `{base}/@{username}`.
        let username = rest.split(['/', '?', '#']).next().unwrap_or("");
        find_local_account(services, username)?
    } else {
        // `acct:{user}@{h}`, `{user}@{h}`, `acct:{user}`, `{user}`, plus
        // the `system.actor` / `instance.actor` service accounts (resolved
        // through the same local username lookup when present).
        let bare = candidate.trim().trim_start_matches('@');
        if bare.is_empty() {
            return Err(StatusCode::NOT_FOUND);
        }
        if let Some((user, h)) = bare.split_once('@') {
            if h.contains('@') || user.trim().is_empty() || h.trim().is_empty() {
                return Err(StatusCode::NOT_FOUND);
            }
            if h.to_ascii_lowercase() != host.to_ascii_lowercase() {
                return Err(StatusCode::UNPROCESSABLE_ENTITY);
            }
            find_local_account(services, user.trim().trim_start_matches('@'))?
        } else {
            find_local_account(services, bare)?
        }
    };

    match found {
        Some(account) if account_visible(&account) => Ok(account),
        _ => Err(StatusCode::NOT_FOUND),
    }
}

/// JRD WebFinger document for a local account.
pub fn render_webfinger_jrd(account: &Account, public_url: &str) -> Value {
    let base = base_url(public_url);
    let host = extract_host(base);
    let actor = account.actor_url(base);
    json!({
        "subject": format!("acct:{}@{}", account.username, host),
        "aliases": [
            format!("{base}/@{}", account.username),
            actor.clone(),
        ],
        "links": [
            {
                "rel": "self",
                "type": "application/activity+json",
                "href": actor,
            },
            {
                "rel": "http://webfinger.net/rel/profile-page",
                "type": "text/html",
                "href": format!("{base}/@{}", account.username),
            },
            {
                "rel": "http://ostatus.org/schema/1.0/subscribe",
                "template": format!("{base}/authorize-follow?acct={{uri}}"),
            },
        ],
    })
}

/// XRD WebFinger document for a local account.
pub fn render_webfinger_xrd(account: &Account, public_url: &str) -> String {
    let base = base_url(public_url);
    let host = extract_host(base);
    let actor = account.actor_url(base);
    let subject = xml_escape(&format!("acct:{}@{host}", account.username));
    let profile_page = xml_escape(&format!("{base}/@{}", account.username));
    let actor_escaped = xml_escape(&actor);
    let subscribe = xml_escape(&format!("{base}/authorize-follow?acct={{uri}}"));
    let mut out = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><XRD xmlns=\"http://docs.oasis-open.org/ns/xri/xrd-1.0\">",
    );
    out.push_str(&format!("<Subject>{subject}</Subject>"));
    out.push_str(&format!(
        "<Link rel=\"self\" type=\"application/activity+json\" href=\"{actor_escaped}\"/>"
    ));
    out.push_str(&format!(
        "<Link rel=\"http://webfinger.net/rel/profile-page\" type=\"text/html\" href=\"{profile_page}\"/>"
    ));
    out.push_str(&format!(
        "<Link rel=\"http://ostatus.org/schema/1.0/subscribe\" template=\"{subscribe}\"/>"
    ));
    for alias in [format!("{base}/@{}", account.username), actor] {
        out.push_str(&format!("<Alias>{}</Alias>", xml_escape(&alias)));
    }
    out.push_str("</XRD>");
    out
}

/// True when the `Accept` header explicitly prefers `application/xrd+xml`
/// over the JRD/JSON types (a bare `*/*` stays on JRD).
fn wants_xrd(accept: Option<&str>) -> bool {
    let header = match accept {
        Some(value) if !value.trim().is_empty() => value,
        _ => return false,
    };
    let quality = |wanted: &str| -> f32 {
        let mut best: f32 = 0.0;
        for part in header.split(',') {
            let mut segments = part.split(';');
            let mime = segments.next().unwrap_or("").trim().to_ascii_lowercase();
            if mime != wanted {
                continue;
            }
            let mut q: f32 = 1.0;
            for param in segments {
                let param = param.trim();
                if let Some(value) = param
                    .strip_prefix("q=")
                    .or_else(|| param.strip_prefix("Q="))
                {
                    q = value.trim().parse().unwrap_or(1.0);
                }
            }
            best = best.max(q);
        }
        best
    };
    let xrd = quality("application/xrd+xml");
    xrd > 0.0 && xrd > quality("application/jrd+json") && xrd > quality("application/json")
}

/// `GET /.well-known/webfinger?resource=...`.
pub async fn webfinger(
    State(services): State<Arc<AppServices>>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    let resource = params.get("resource").map(String::as_str).unwrap_or("");
    if resource.trim().is_empty() {
        return error_response(StatusCode::BAD_REQUEST);
    }
    let account = match resolve_webfinger_resource(&services, resource) {
        Ok(account) => account,
        Err(status) => return error_response(status),
    };
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok());
    if wants_xrd(accept) {
        let mut response = text_response(
            StatusCode::OK,
            CONTENT_TYPE_XRD,
            render_webfinger_xrd(&account, &services.public_url),
        );
        response
            .headers_mut()
            .insert(header::VARY, HeaderValue::from_static("Accept"));
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static(CACHE_WEBFINGER),
        );
        response
    } else {
        let mut response = json_response(
            StatusCode::OK,
            CONTENT_TYPE_JRD,
            &render_webfinger_jrd(&account, &services.public_url),
        );
        response
            .headers_mut()
            .insert(header::VARY, HeaderValue::from_static("Accept"));
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static(CACHE_WEBFINGER),
        );
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activitypub::webfinger::memory_mock_extra::{alice, test_state};

    use axum::body::to_bytes;
    use axum::extract::{Query, State};

    #[test]
    fn all_resource_forms_resolve_to_canonical_actor_url() {
        let (state, mem) = test_state("https://example.local");
        let account = alice("aid00000000000001");
        mem.insert_account(account.clone());
        let services = &state;
        let canonical = "https://example.local/users/aid00000000000001";

        let forms = [
            "acct:alice@example.local".to_string(),
            "alice@example.local".to_string(),
            "ACCT:ALICE@EXAMPLE.LOCAL".to_string(),
            "https://example.local/users/aid00000000000001".to_string(),
            "https://example.local/@alice".to_string(),
            "alice".to_string(),
            "acct:alice".to_string(),
        ];
        for resource in forms {
            let resolved = resolve_webfinger_resource(services, &resource)
                .unwrap_or_else(|_| panic!("expected {resource} to resolve"));
            assert_eq!(
                resolved.actor_url(&services.public_url),
                canonical,
                "{resource}"
            );
        }

        // JRD aliases point at the canonical actor URL; no node hostname leaks.
        let jrd = render_webfinger_jrd(&account, &services.public_url);
        assert_eq!(jrd["subject"], "acct:alice@example.local");
        let aliases: Vec<String> =
            serde_json::from_value(jrd["aliases"].clone()).expect("aliases array");
        assert!(aliases.contains(&canonical.to_string()));
        let self_link = jrd["links"]
            .as_array()
            .unwrap()
            .iter()
            .find(|link| link["rel"] == "self")
            .expect("self link");
        assert_eq!(self_link["href"], canonical);
    }

    #[test]
    fn system_actor_resolves_when_present() {
        let (state, mem) = test_state("https://example.local");
        let mut service = alice("aid00000000000009");
        service.username = "system.actor".to_string();
        service.username_lower = "system.actor".to_string();
        mem.insert_account(service);
        let resolved = resolve_webfinger_resource(&state, "acct:system.actor@example.local")
            .expect("system.actor resolves");
        assert_eq!(
            resolved.actor_url(&state.public_url),
            "https://example.local/users/aid00000000000009"
        );
    }

    #[test]
    fn wrong_host_is_unprocessable() {
        let (state, mem) = test_state("https://example.local");
        mem.insert_account(alice("aid00000000000001"));
        assert_eq!(
            resolve_webfinger_resource(&state, "acct:alice@other.example").unwrap_err(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            resolve_webfinger_resource(&state, "alice@other.example").unwrap_err(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
    }

    #[test]
    fn unknown_suspended_deleted_or_remote_are_not_found() {
        let (state, mem) = test_state("https://example.local");
        mem.insert_account(alice("aid00000000000001"));

        assert_eq!(
            resolve_webfinger_resource(&state, "acct:bob@example.local").unwrap_err(),
            StatusCode::NOT_FOUND
        );

        let mut suspended = alice("aid00000000000002");
        suspended.username = "suspended".to_string();
        suspended.username_lower = "suspended".to_string();
        suspended.is_suspended = true;
        mem.insert_account(suspended);
        assert_eq!(
            resolve_webfinger_resource(&state, "acct:suspended@example.local").unwrap_err(),
            StatusCode::NOT_FOUND
        );

        let mut deleted = alice("aid00000000000003");
        deleted.username = "deleted".to_string();
        deleted.username_lower = "deleted".to_string();
        deleted.is_deleted = true;
        mem.insert_account(deleted);
        assert_eq!(
            resolve_webfinger_resource(&state, "acct:deleted@example.local").unwrap_err(),
            StatusCode::NOT_FOUND
        );

        let mut remote = alice("aid00000000000004");
        remote.username = "remote".to_string();
        remote.username_lower = "remote".to_string();
        remote.host = Some("remote.example".to_string());
        mem.insert_account(remote);
        // Remote accounts share no username namespace with local webfinger.
        assert_eq!(
            resolve_webfinger_resource(&state, "acct:remote@example.local").unwrap_err(),
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn content_negotiation_prefers_explicit_xrd() {
        assert!(!wants_xrd(None));
        assert!(!wants_xrd(Some("")));
        assert!(!wants_xrd(Some("*/*")));
        assert!(!wants_xrd(Some("application/jrd+json")));
        assert!(wants_xrd(Some("application/xrd+xml")));
        assert!(wants_xrd(Some(
            "application/xrd+xml, application/jrd+json;q=0.5"
        )));
        assert!(!wants_xrd(Some(
            "application/jrd+json, application/xrd+xml;q=0.5"
        )));
        assert!(!wants_xrd(Some("application/xrd+xml;q=0")));
    }

    #[tokio::test]
    async fn webfinger_handler_defaults_to_jrd_and_serves_xrd_on_demand() {
        let (state, mem) = test_state("https://example.local");
        mem.insert_account(alice("aid00000000000001"));

        let mut params = HashMap::new();
        params.insert(
            "resource".to_string(),
            "acct:alice@example.local".to_string(),
        );

        // Default: JRD.
        let response = webfinger(
            State(state.clone()),
            Query(params.clone()),
            HeaderMap::new(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], CONTENT_TYPE_JRD);
        assert_eq!(response.headers()["vary"], "Accept");
        assert_eq!(response.headers()["access-control-allow-origin"], "*");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let jrd: Value = serde_json::from_slice(&body).expect("valid JRD JSON");
        assert_eq!(jrd["subject"], "acct:alice@example.local");

        // Explicit XRD preference: XML.
        let mut accept = HeaderMap::new();
        accept.insert(
            header::ACCEPT,
            HeaderValue::from_static("application/xrd+xml"),
        );
        let response = webfinger(State(state.clone()), Query(params.clone()), accept).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], CONTENT_TYPE_XRD);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let xml = String::from_utf8(body.to_vec()).expect("utf-8 xml");
        assert!(xml.contains("<XRD"));
        assert!(xml.contains("https://example.local/users/aid00000000000001"));

        // Missing resource: 400. Unknown user: 404. Wrong host: 422.
        let response = webfinger(
            State(state.clone()),
            Query(HashMap::new()),
            HeaderMap::new(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let mut unknown = HashMap::new();
        unknown.insert(
            "resource".to_string(),
            "acct:nobody@example.local".to_string(),
        );
        let response = webfinger(State(state.clone()), Query(unknown), HeaderMap::new()).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let mut foreign = HashMap::new();
        foreign.insert(
            "resource".to_string(),
            "acct:alice@other.example".to_string(),
        );
        let response = webfinger(State(state), Query(foreign), HeaderMap::new()).await;
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[test]
    fn xrd_escapes_xml_specials() {
        let (state, _) = test_state("https://example.local");
        // Usernames cannot contain `&`, so exercise the escaper through a
        // hand-mutated account instead of store round-tripping.
        let mut tricky = alice("aid00000000000001");
        tricky.username = "alice&bob".to_string();
        let xml = render_webfinger_xrd(&tricky, &state.public_url);
        assert!(xml.contains("acct:alice&amp;bob@example.local"));
        assert!(!xml.contains("alice&bob"));
    }
}
