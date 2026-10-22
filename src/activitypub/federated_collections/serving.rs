//! Shared serving helpers for federated collections: the ActivityPub
//! JSON response envelope and the global `federateClips`/`federatePages`
//! instance switches (default OFF ⇒ zero protocol footprint).

use axum::{
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::Value;

use crate::application::commands::AppServices;

pub(crate) fn ap_response(status: StatusCode, body: &Value) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/activity+json; charset=utf-8"),
    );
    let payload = serde_json::to_string(body).unwrap_or_else(|_| "{}".to_string());
    (status, headers, payload).into_response()
}

/// Whether the global federation switch for `kind` is on (`clips` /
/// `pages`; default OFF ⇒ zero protocol footprint).
pub fn federation_switch_on(services: &AppServices, kind: &str) -> bool {
    match services.instance_settings.get() {
        Ok(settings) => match kind {
            "clips" => settings.federate_clips,
            "pages" => settings.federate_pages,
            _ => false,
        },
        Err(_) => false,
    }
}
