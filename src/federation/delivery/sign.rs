//! Signed ActivityPub GET/POST helpers (moved from `src/federation/delivery.rs`).
//! Safe federation HTTP client, SSRF protection, and WebFinger + signed-GET
//! remote actor/note resolver (Phase 2 Slice C1).
//!
//! Enforces:
//! - Scheme check (`http` or `https` only)
//! - SSRF / private-IP rejection (loopback, RFC1918, link-local, CGNAT,
//!   multicast, unspecified, IPv6 unique-local/link-local/loopback, plus
//!   IPv4-mapped IPv6 addresses) unless explicitly allowed by an allowlist
//!   or `allow_private_ips` flag for local federation testing.
//!   `allow_private_ips` is an SSRF exemption ONLY: it never affects TLS
//!   certificate verification, which is always enforced on HTTPS requests.
//! - Connect and read timeouts (default 10s), an overall deadline for the
//!   plain-HTTP path (default 20s), and a bounded DNS lookup (default 5s)
//! - Typed [`SendError`]s: permanent kinds (SSRF, invalid URL, NXDOMAIN) are
//!   never retried; transient kinds (timeouts, 5xx/429 at the retry layer)
//!   are retried with backoff by the delivery worker
//! - Redirect limit (default ≤ 3 hops, rejecting `https` → `http` downgrades
//!   and re-validating every redirect target against SSRF rules)
//! - Response size limit (default 64 KiB for ActivityPub JSON, configurable
//!   cap for media downloads).

use chrono::Utc;
use serde_json::Value;

use crate::activitypub::objects::AP_CONTENT_TYPE;
use crate::activitypub::signatures::sign_request_headers;
use crate::application::commands::AppServices;

use super::client::{SafeHttpClient, SafeHttpResponse};
use super::errors::{MAX_AP_JSON_BYTES, SendError, SendErrorKind};
use super::url::parse_http_url;

/// Format a millisecond timestamp as an HTTP `Date` IMF-fixdate string
/// (`"Wed, 14 Oct 2026 12:00:00 GMT"`).
pub fn format_http_date(ms: i64) -> String {
    let dt = chrono::DateTime::<Utc>::from_timestamp_millis(ms).unwrap_or_else(Utc::now);
    dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

/// Perform an HTTP-signed ActivityPub `GET` using the local `system_actor` key.
pub fn signed_get_json(
    services: &AppServices,
    client: &SafeHttpClient,
    url: &str,
    accept: &str,
) -> Result<Value, String> {
    let parsed = parse_http_url(url)?;
    let (sys_account, sys_kp) = services
        .ensure_system_actor()
        .map_err(|e| format!("ensure_system_actor: {e}"))?;
    let base = services.public_url.trim_end_matches('/');
    let key_id = format!("{}/users/{}#main-key", base, sys_account.id);
    let date = format_http_date(services.clock.now_ms());
    let sig = sign_request_headers(
        "GET",
        &parsed.path_and_query,
        &parsed.host_header,
        &date,
        None,
        &key_id,
        &sys_kp.private_key_pem,
    )?;

    let headers = [
        ("Host", parsed.host_header.as_str()),
        ("Date", sig.date.as_str()),
        ("Signature", sig.signature.as_str()),
        ("Accept", accept),
    ];
    let resp = client.send("GET", url, &headers, None, MAX_AP_JSON_BYTES)?;
    if !(200..300).contains(&resp.status) {
        return Err(format!("GET {url} returned HTTP {}", resp.status));
    }
    serde_json::from_slice(&resp.body).map_err(|e| format!("parse JSON from {url}: {e}"))
}

/// Perform an HTTP-signed ActivityPub `POST` of a JSON activity to a remote inbox.
pub fn signed_post_activity(
    services: &AppServices,
    client: &SafeHttpClient,
    sender_account_id: &str,
    inbox_url: &str,
    body_bytes: &[u8],
) -> Result<SafeHttpResponse, SendError> {
    let parsed =
        parse_http_url(inbox_url).map_err(|e| SendError::new(SendErrorKind::InvalidUrl, e))?;
    let kp = services
        .ensure_actor_keypair(sender_account_id)
        .map_err(|e| {
            SendError::new(
                SendErrorKind::Internal,
                format!("ensure_actor_keypair: {e}"),
            )
        })?;
    let base = services.public_url.trim_end_matches('/');
    let key_id = format!("{base}/users/{sender_account_id}#main-key");
    let date = format_http_date(services.clock.now_ms());
    let sig = sign_request_headers(
        "POST",
        &parsed.path_and_query,
        &parsed.host_header,
        &date,
        Some(body_bytes),
        &key_id,
        &kp.private_key_pem,
    )
    .map_err(|e| SendError::new(SendErrorKind::Internal, e))?;
    let digest = sig.digest.unwrap_or_default();
    let headers = [
        ("Host", parsed.host_header.as_str()),
        ("Date", sig.date.as_str()),
        ("Digest", digest.as_str()),
        ("Signature", sig.signature.as_str()),
        ("Content-Type", AP_CONTENT_TYPE),
        ("Accept", "application/activity+json, application/ld+json"),
    ];
    client.send(
        "POST",
        inbox_url,
        &headers,
        Some(body_bytes),
        MAX_AP_JSON_BYTES,
    )
}
