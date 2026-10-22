//! Shared WebFinger/host-meta/NodeInfo plumbing: content types, the
//! `extract_host` helper, and the small response constructors every
//! handler in this family uses.

use axum::{
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::Value;

/// JRD content type for WebFinger and `host-meta.json`.
pub const CONTENT_TYPE_JRD: &str = "application/jrd+json; charset=utf-8";
/// XRD content type for `host-meta` and WebFinger XML.
pub const CONTENT_TYPE_XRD: &str = "application/xrd+xml; charset=utf-8";
/// NodeInfo 2.0 document content type.
pub const CONTENT_TYPE_NODEINFO_2_0: &str =
    "application/json; profile=\"http://nodeinfo.diaspora.software/ns/schema/2.0#\"; charset=utf-8";
/// NodeInfo 2.1 document content type.
pub const CONTENT_TYPE_NODEINFO_2_1: &str =
    "application/json; profile=\"http://nodeinfo.diaspora.software/ns/schema/2.1#\"; charset=utf-8";

/// Lowercased `host[:port]` portion of a `public_url` such as
/// `https://example.local` or `https://example.local:3000/base`.
pub fn extract_host(public_url: &str) -> String {
    let without_scheme = match public_url.split_once("://") {
        Some((_, rest)) => rest,
        None => public_url,
    };
    let host = without_scheme.split('/').next().unwrap_or("");
    host.trim_end_matches(':').to_ascii_lowercase()
}

pub(crate) fn xml_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(ch),
        }
    }
    out
}

pub(crate) fn base_url(public_url: &str) -> &str {
    public_url.trim_end_matches('/')
}

fn cors_headers(headers: &mut HeaderMap) {
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
}

pub(crate) fn text_response(
    status: StatusCode,
    content_type: &'static str,
    body: String,
) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    cors_headers(&mut headers);
    (status, headers, body).into_response()
}

pub(crate) fn json_response(
    status: StatusCode,
    content_type: &'static str,
    body: &Value,
) -> Response {
    text_response(
        status,
        content_type,
        serde_json::to_string(body).unwrap_or_else(|_| "{}".to_string()),
    )
}

pub(crate) fn error_response(status: StatusCode) -> Response {
    text_response(status, CONTENT_TYPE_JRD, String::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_host_handles_ports_and_paths() {
        assert_eq!(extract_host("https://example.local"), "example.local");
        assert_eq!(extract_host("https://example.local/"), "example.local");
        assert_eq!(
            extract_host("https://Example.LOCAL:3000/base"),
            "example.local:3000"
        );
    }
}
