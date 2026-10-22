//! `host-meta` (XRD + JRD) documents and handlers.

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, response::Response};
use serde_json::{Value, json};

use crate::application::commands::AppServices;

use super::responses::{
    CONTENT_TYPE_JRD, CONTENT_TYPE_XRD, base_url, json_response, text_response,
};

/// XRD host-meta document pointing at this node's WebFinger template.
pub fn render_host_meta_xrd(public_url: &str) -> String {
    let base = base_url(public_url);
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><XRD xmlns=\"http://docs.oasis-open.org/ns/xri/xrd-1.0\"><Link rel=\"lrdd\" type=\"application/xrd+xml\" template=\"{base}/.well-known/webfinger?resource={{uri}}\"/></XRD>"
    )
}

/// JRD host-meta document pointing at this node's WebFinger template.
pub fn render_host_meta_jrd(public_url: &str) -> Value {
    let base = base_url(public_url);
    json!({
        "links": [
            {
                "rel": "lrdd",
                "type": "application/jrd+json",
                "template": format!("{base}/.well-known/webfinger?resource={{uri}}"),
            }
        ]
    })
}

/// `GET /.well-known/host-meta` (XRD).
pub async fn host_meta_xrd(State(services): State<Arc<AppServices>>) -> Response {
    text_response(
        StatusCode::OK,
        CONTENT_TYPE_XRD,
        render_host_meta_xrd(&services.public_url),
    )
}

/// `GET /.well-known/host-meta.json` (JRD).
pub async fn host_meta_json(State(services): State<Arc<AppServices>>) -> Response {
    json_response(
        StatusCode::OK,
        CONTENT_TYPE_JRD,
        &render_host_meta_jrd(&services.public_url),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activitypub::webfinger::memory_mock_extra::test_state;

    use axum::body::to_bytes;

    #[tokio::test]
    async fn host_meta_shapes() {
        let (state, _) = test_state("https://example.local");

        let response = host_meta_xrd(State(state.clone())).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], CONTENT_TYPE_XRD);
        assert_eq!(response.headers()["access-control-allow-origin"], "*");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let xml = String::from_utf8(body.to_vec()).unwrap();
        assert!(
            xml.contains("template=\"https://example.local/.well-known/webfinger?resource={uri}\"")
        );

        let response = host_meta_json(State(state.clone())).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], CONTENT_TYPE_JRD);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let jrd: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            jrd["links"][0]["template"],
            "https://example.local/.well-known/webfinger?resource={uri}"
        );
    }
}
