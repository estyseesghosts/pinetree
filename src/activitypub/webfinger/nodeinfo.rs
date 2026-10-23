//! NodeInfo 1.x/2.x documents (`/.well-known/nodeinfo`,
//! `/nodeinfo/2.0`, `/nodeinfo/2.1`) matching Misskey
//! `NodeinfoServerService.ts`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use axum::{
    extract::State,
    http::{HeaderValue, StatusCode, header},
    response::Response,
};
use serde_json::{Value, json};

use crate::activitypub::PACKER_VERSION;
use crate::application::commands::AppServices;

use super::responses::{
    CONTENT_TYPE_NODEINFO_2_0, CONTENT_TYPE_NODEINFO_2_1, base_url, json_response,
};

const CACHE_NODEINFO: &str = "public, max-age=300";

/// Server-side memo for the rendered NodeInfo usage document (Wave 4
/// liveness): the endpoint is unauthenticated, so every scrape would
/// otherwise pay a render per request. Entries expire after
/// [`NODEINFO_CACHE_TTL_MS`]; versions render independently.
const NODEINFO_CACHE_TTL_MS: i64 = 60_000;

fn nodeinfo_cache() -> &'static Mutex<HashMap<(String, String), (Value, i64)>> {
    static CACHE: OnceLock<Mutex<HashMap<(String, String), (Value, i64)>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// `/.well-known/nodeinfo` link document.
pub fn render_well_known_nodeinfo(public_url: &str) -> Value {
    let base = base_url(public_url);
    json!({
        "links": [
            {
                "rel": "http://nodeinfo.diaspora.software/ns/schema/2.1",
                "href": format!("{base}/nodeinfo/2.1"),
            },
            {
                "rel": "http://nodeinfo.diaspora.software/ns/schema/2.0",
                "href": format!("{base}/nodeinfo/2.0"),
            },
        ]
    })
}

/// NodeInfo 2.0 / 2.1 document matching Misskey `NodeinfoServerService.ts`.
///
/// Wave 4 liveness: both usage numbers come from maintained O(1) counters
/// (`count_local_accounts`, `count_local_notes`) — the old code decoded up
/// to 10_000 account rows plus 10_000 note rows on every unauthenticated
/// scrape. Errors read as 0 (same lenient posture as the old
/// `unwrap_or_default` scans).
pub fn render_nodeinfo(services: &AppServices, version: &str) -> Value {
    let local_users = services.accounts.count_local_accounts().unwrap_or(0);
    let local_posts = services.notes.count_local_notes().unwrap_or(0);

    let software = if version == "2.1" {
        json!({
            "name": "misskey",
            "version": PACKER_VERSION,
            "homepage": "https://misskey-hub.net",
            "repository": null,
        })
    } else {
        json!({
            "name": "misskey",
            "version": PACKER_VERSION,
        })
    };

    json!({
        "version": version,
        "software": software,
        "protocols": ["activitypub"],
        "services": {
            "inbound": [],
            "outbound": ["atom1.0", "rss2.0"],
        },
        "openRegistrations": true,
        "usage": {
            "users": {
                "total": local_users,
                "activeHalfyear": null,
                "activeMonth": null,
            },
            "localPosts": local_posts,
            "localComments": 0,
        },
        "metadata": {
            "nodeName": "Pinetree",
            "nodeDescription": null,
            "maintainer": { "name": null, "email": null },
            "langs": ["en"],
            "tosUrl": null,
            "repositoryUrl": null,
            "feedbackUrl": null,
            "disableRegistration": false,
            "disableLocalTimeline": false,
            "disableGlobalTimeline": false,
            "emailRequiredForSignup": false,
            "enableHcaptcha": false,
            "enableRecaptcha": false,
            "enableMcaptcha": false,
            "enableTurnstile": false,
            "maxNoteTextLength": 3000,
            "enableEmail": false,
            "enableServiceWorker": true,
            "proxyAccountName": null,
            "themeColor": "#86b300",
        },
    })
}

/// `GET /.well-known/nodeinfo`.
pub async fn well_known_nodeinfo(State(services): State<Arc<AppServices>>) -> Response {
    let mut response = json_response(
        StatusCode::OK,
        "application/json; charset=utf-8",
        &render_well_known_nodeinfo(&services.public_url),
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(CACHE_NODEINFO),
    );
    response
}

/// `GET /nodeinfo/2.0`.
pub async fn nodeinfo_2_0(State(services): State<Arc<AppServices>>) -> Response {
    let mut response = json_response(
        StatusCode::OK,
        CONTENT_TYPE_NODEINFO_2_0,
        &cached_nodeinfo(&services, "2.0"),
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(CACHE_NODEINFO),
    );
    response
}

/// `GET /nodeinfo/2.1`.
pub async fn nodeinfo_2_1(State(services): State<Arc<AppServices>>) -> Response {
    let mut response = json_response(
        StatusCode::OK,
        CONTENT_TYPE_NODEINFO_2_1,
        &cached_nodeinfo(&services, "2.1"),
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(CACHE_NODEINFO),
    );
    response
}

/// Briefly-cached [`render_nodeinfo`] (Wave 4 liveness): the endpoint is
/// unauthenticated, so scrapes share one render per
/// [`NODEINFO_CACHE_TTL_MS`] window instead of rendering per request. The
/// cache is keyed by `(public_url, version)` and holds at most a handful of
/// entries; a poisoned or contended lock degrades to a fresh render.
pub fn cached_nodeinfo(services: &AppServices, version: &str) -> Value {
    let key = (services.public_url.clone(), version.to_string());
    let now = services.clock.now_ms();
    if let Ok(cache) = nodeinfo_cache().lock() {
        if let Some((doc, expires)) = cache.get(&key) {
            if *expires > now {
                return doc.clone();
            }
        }
    }
    let doc = render_nodeinfo(services, version);
    if let Ok(mut cache) = nodeinfo_cache().lock() {
        cache.insert(key, (doc.clone(), now.saturating_add(NODEINFO_CACHE_TTL_MS)));
        while cache.len() > 8 {
            let Some(first) = cache.keys().next().cloned() else {
                break;
            };
            cache.remove(&first);
        }
    }
    doc
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activitypub::webfinger::memory_mock_extra::{alice, local_note, test_state};

    use axum::body::to_bytes;
    use axum::extract::State;
    use serde_json::json;
    use std::collections::HashSet;

    #[tokio::test]
    async fn nodeinfo_shapes_and_counts() {
        let (state, mem) = test_state("https://example.local");
        mem.insert_account(alice("aid00000000000001"));
        let mut remote = alice("aid00000000000002");
        remote.username = "bob".to_string();
        remote.username_lower = "bob".to_string();
        remote.host = Some("remote.example".to_string());
        mem.insert_account(remote);
        mem.insert_note(local_note("note1", "aid00000000000001"));
        mem.insert_note(local_note("note2", "aid00000000000001"));
        let mut remote_note = local_note("note3", "aid00000000000002");
        remote_note.user_host = Some("remote.example".to_string());
        mem.insert_note(remote_note);

        let response = well_known_nodeinfo(State(state.clone())).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let doc: Value = serde_json::from_slice(&body).unwrap();
        let rels: HashSet<String> = doc["links"]
            .as_array()
            .unwrap()
            .iter()
            .map(|link| link["rel"].as_str().unwrap().to_string())
            .collect();
        assert!(rels.contains("http://nodeinfo.diaspora.software/ns/schema/2.0"));
        assert!(rels.contains("http://nodeinfo.diaspora.software/ns/schema/2.1"));

        let response = nodeinfo_2_0(State(state.clone())).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["content-type"],
            CONTENT_TYPE_NODEINFO_2_0
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let doc: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(doc["version"], "2.0");
        assert_eq!(
            doc["software"],
            json!({"name": "misskey", "version": PACKER_VERSION})
        );
        assert_eq!(doc["protocols"], json!(["activitypub"]));
        assert_eq!(doc["services"]["outbound"], json!(["atom1.0", "rss2.0"]));
        assert_eq!(doc["openRegistrations"], true);
        // Only local users and local notes are counted.
        assert_eq!(doc["usage"]["users"]["total"], 1);
        assert_eq!(doc["usage"]["localPosts"], 2);
        assert_eq!(doc["usage"]["localComments"], 0);
        assert_eq!(doc["metadata"]["nodeName"], "Pinetree");
        assert_eq!(doc["metadata"]["maxNoteTextLength"], 3000);

        let response = nodeinfo_2_1(State(state.clone())).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["content-type"],
            CONTENT_TYPE_NODEINFO_2_1
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let doc: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(doc["version"], "2.1");
        assert_eq!(doc["software"]["name"], "misskey");
        assert_eq!(doc["software"]["version"], PACKER_VERSION);
        assert_eq!(doc["protocols"], json!(["activitypub"]));
        assert_eq!(doc["services"]["outbound"], json!(["atom1.0", "rss2.0"]));
        assert_eq!(doc["openRegistrations"], true);
        assert_eq!(doc["usage"]["users"]["total"], 1);
        assert_eq!(doc["usage"]["localPosts"], 2);
        assert_eq!(doc["usage"]["localComments"], 0);
        assert_eq!(doc["metadata"]["nodeName"], "Pinetree");
        assert_eq!(doc["metadata"]["maxNoteTextLength"], 3000);

        // 2.1 carries the extra software link fields.
        let doc21 = render_nodeinfo(&state, "2.1");
        assert_eq!(doc21["software"]["homepage"], "https://misskey-hub.net");
        assert!(doc21["software"].get("repository").is_some());
    }
}
