//! NodeInfo 1.x/2.x documents (`/.well-known/nodeinfo`,
//! `/nodeinfo/2.0`, `/nodeinfo/2.1`) matching Misskey
//! `NodeinfoServerService.ts`.

use std::sync::Arc;

use axum::{
    extract::State,
    http::{HeaderValue, StatusCode, header},
    response::Response,
};
use serde_json::{Value, json};

use crate::activitypub::PACKER_VERSION;
use crate::application::commands::AppServices;
use crate::application::ports::TimelineCursor;

use super::responses::{
    CONTENT_TYPE_NODEINFO_2_0, CONTENT_TYPE_NODEINFO_2_1, base_url, json_response,
};

const CACHE_NODEINFO: &str = "public, max-age=300";

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
pub fn render_nodeinfo(services: &AppServices, version: &str) -> Value {
    let local_users = services
        .accounts
        .search_accounts("", 0, 10_000)
        .unwrap_or_default()
        .into_iter()
        .filter(|account| account.is_local())
        .count();
    let local_posts = services
        .notes
        .list_all_notes(&TimelineCursor::with_limit(10_000))
        .unwrap_or_default()
        .into_iter()
        .filter(|note| note.user_host.is_none())
        .count();

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
        &render_nodeinfo(&services, "2.0"),
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
        &render_nodeinfo(&services, "2.1"),
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(CACHE_NODEINFO),
    );
    response
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
