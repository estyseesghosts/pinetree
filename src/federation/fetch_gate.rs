//! Generic authenticated-fetch gate (Phase 0 of the federated
//! clips + pages handoff; second customer: private communities,
//! `docs/community-planning.md` — "private fetch requires
//! follower-instance signature").
//!
//! The gate does two things and nothing else:
//!
//! 1. **Authenticate the fetcher** ([`authenticate_fetch_viewer`]): verify
//!    the HTTP Signature on an inbound GET and resolve the signing key to
//!    its owner actor URI (same key-lookup path as the inbox: local cache,
//!    fetch on miss, one refresh on rotation). This step never rejects by
//!    itself — missing/unparsable signatures, unresolvable keys, and bad
//!    signatures all collapse to an *anonymous* viewer so
//!    existence-hiding resources can 404 the fetcher without confirming
//!    the object exists.
//! 2. **Map (viewer, resource decision) to serve/403/404**
//!    ([`decide_fetch`]): the resource supplies a [`FetchDecision`] from
//!    its own authorization callback (clip tiers, page tiers, community
//!    membership — see e.g. `AppServices::authorize_clip_fetch`). The gate
//!    maps `Serve → Ok`, `Forbidden` from an *identified* viewer → 403,
//!    `Forbidden` from an *anonymous* viewer → 404 (existence
//!    unconfirmed), and `Hidden` → 404 always.
//!
//! No object-type logic lives below the callback: the gate never inspects
//! what kind of object is being fetched. Resource callbacks are pure
//! functions over `(resource row, switches, Option<actor_uri>)`. Community
//! membership checks (reader checks + signed `is_follower` probes) call
//! this same interface later.

use axum::http::{HeaderMap, StatusCode};

use crate::activitypub::signatures::{parse_signature_header, verify_http_signature};
use crate::application::commands::AppServices;
use crate::federation::delivery::resolve_remote_public_key;
use crate::federation::inbox::MAX_CLOCK_SKEW_SECS;

/// The resource's authorization answer for one fetch, computed by the
/// resource's own callback above this gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchDecision {
    /// Serve the object.
    Serve,
    /// The fetcher is identified but not authorized. The gate reports 403
    /// to identified fetchers and 404 to anonymous ones (so strangers can
    /// never distinguish "forbidden" from "does not exist").
    Forbidden,
    /// The object is not remotely servable at all (local-only, unknown or
    /// deleted). Always 404, regardless of who asks.
    Hidden,
}

/// Who is fetching: a verified actor URI, or nobody (unsigned, unparsable,
/// unresolvable, or mis-signed requests all land here).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchViewer {
    pub actor_uri: Option<String>,
}

impl FetchViewer {
    pub fn anonymous() -> Self {
        Self { actor_uri: None }
    }

    pub fn identified(actor_uri: String) -> Self {
        Self {
            actor_uri: Some(actor_uri),
        }
    }

    pub fn is_anonymous(&self) -> bool {
        self.actor_uri.is_none()
    }
}

/// Verify the HTTP Signature on an inbound GET and resolve the signer.
/// `path_and_query` is the origin-form request target (e.g. `/clips/{id}`
/// plus query string). Any failure — no header, unparsable header,
/// unresolvable key (even after refresh), bad signature — yields an
/// anonymous viewer; the resource callback + [`decide_fetch`] turn that
/// into 404 for non-public objects.
pub fn authenticate_fetch_viewer(
    services: &AppServices,
    path_and_query: &str,
    headers: &HeaderMap,
) -> FetchViewer {
    let raw = headers
        .get("signature")
        .and_then(|v| v.to_str().ok())
        .or_else(|| {
            headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
        });
    let Some(raw) = raw else {
        return FetchViewer::anonymous();
    };
    let parsed = match parse_signature_header(raw) {
        Ok(p) => p,
        Err(_) => return FetchViewer::anonymous(),
    };
    let key_id = parsed.key_id.clone();
    let now_ms = services.clock.now_ms();
    let client = &services.federation_http;
    let verify = |pem: &str| {
        verify_http_signature(
            "GET",
            path_and_query,
            headers,
            None,
            pem,
            Some(now_ms),
            MAX_CLOCK_SKEW_SECS,
        )
    };
    // Same lookup discipline as the inbox: cached key, fetch on miss, one
    // refresh on rotation. Every failure collapses to anonymous (never a
    // 401/403 here — existence-hiding is the resource's decision).
    let resolved = resolve_remote_public_key(services, client, &key_id, false);
    match resolved {
        Ok((owner, pem)) => match verify(&pem) {
            Ok(_) => FetchViewer::identified(owner),
            Err(_) => {
                match resolve_remote_public_key(services, client, &key_id, true) {
                    Ok((fresh_owner, fresh_pem)) => match verify(&fresh_pem) {
                        Ok(_) => FetchViewer::identified(fresh_owner),
                        Err(_) => FetchViewer::anonymous(),
                    },
                    Err(_) => FetchViewer::anonymous(),
                }
            }
        },
        Err(_) => FetchViewer::anonymous(),
    }
}

/// Map the resource's [`FetchDecision`] to the fetch outcome: `Ok(())`
/// means "serve the object", `Err(status)` is the status to return
/// otherwise. The `Forbidden`-from-anonymous collapse to 404 is generic
/// existence-hiding policy, not object-type logic.
pub fn decide_fetch(
    viewer: &FetchViewer,
    decision: FetchDecision,
) -> Result<(), StatusCode> {
    match decision {
        FetchDecision::Serve => Ok(()),
        FetchDecision::Forbidden if !viewer.is_anonymous() => {
            Err(StatusCode::FORBIDDEN)
        }
        FetchDecision::Forbidden => Err(StatusCode::NOT_FOUND),
        FetchDecision::Hidden => Err(StatusCode::NOT_FOUND),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activitypub::signatures::{
        generate_rsa_keypair_2048, private_key_to_pkcs8_pem, public_key_to_spki_pem,
        sign_request_headers,
    };

    fn test_services() -> AppServices {
        crate::application::commands::tests::test_services("fetch-gate")
    }

    /// HTTP date for `now_ms` (RFC 2822; `parse_http_date` accepts it).
    fn http_date(now_ms: i64) -> String {
        chrono::DateTime::from_timestamp_millis(now_ms)
            .unwrap_or_default()
            .to_rfc2822()
    }

    /// Build a `HeaderMap` for a signed GET (mirrors the outbound
    /// signed-GET shape in `delivery.rs`).
    fn signed_get_headers(
        path: &str,
        host: &str,
        key_id: &str,
        private_key_pem: &str,
        date: &str,
    ) -> HeaderMap {
        let signed =
            sign_request_headers("GET", path, host, date, None, key_id, private_key_pem)
                .unwrap();
        let mut map = HeaderMap::new();
        map.insert("date", date.parse().unwrap());
        map.insert("host", host.parse().unwrap());
        map.insert("signature", signed.signature.parse().unwrap());
        map
    }

    /// Decision matrix: {public, unlisted-capability, approved-remote,
    /// stranger, unsigned} × {serve, 403, 404}. Uses a stub authorizer so
    /// the gate is tested with zero clip/page/community assumptions.
    fn stub_authorize(kind: &str, viewer: &FetchViewer) -> FetchDecision {
        match kind {
            // Public object: everyone served, even anonymous.
            "public" => FetchDecision::Serve,
            // Unlisted capability object: served to any bearer.
            "unlisted" => FetchDecision::Serve,
            // Approved-remote object: only the listed approver.
            "approved" => match viewer.actor_uri.as_deref() {
                Some("https://remote.example/users/approver") => FetchDecision::Serve,
                _ => FetchDecision::Forbidden,
            },
            // Local-only object: never served.
            "local" => FetchDecision::Hidden,
            _ => FetchDecision::Hidden,
        }
    }

    #[test]
    fn unsigned_viewer_is_anonymous_and_hidden_is_404() {
        let services = test_services();
        let viewer = authenticate_fetch_viewer(&services, "/x/1", &HeaderMap::new());
        assert!(viewer.is_anonymous());
        assert_eq!(
            decide_fetch(&viewer, stub_authorize("local", &viewer)),
            Err(StatusCode::NOT_FOUND)
        );
        // Public objects still serve anonymous fetchers.
        assert_eq!(decide_fetch(&viewer, stub_authorize("public", &viewer)), Ok(()));
    }

    #[test]
    fn anonymous_forbidden_collapses_to_404() {
        let viewer = FetchViewer::anonymous();
        // An approved-remote object refuses the stranger-with-no-signature
        // with 404 (existence unconfirmed), not 403.
        assert_eq!(
            decide_fetch(&viewer, stub_authorize("approved", &viewer)),
            Err(StatusCode::NOT_FOUND)
        );
    }

    #[test]
    fn identified_stranger_gets_403_on_gated_object() {
        let viewer = FetchViewer::identified("https://remote.example/users/stranger".to_string());
        assert_eq!(
            decide_fetch(&viewer, stub_authorize("approved", &viewer)),
            Err(StatusCode::FORBIDDEN)
        );
        // Approver is served.
        let approver = FetchViewer::identified("https://remote.example/users/approver".to_string());
        assert_eq!(
            decide_fetch(&approver, stub_authorize("approved", &approver)),
            Ok(())
        );
    }

    #[test]
    fn signed_get_with_cached_key_identifies_fetcher() {
        let services = test_services();
        // Cache a remote key, then sign a GET with the matching private
        // key: the full verify + key-lookup path must identify the actor.
        let (priv_key, pub_key) = generate_rsa_keypair_2048().unwrap();
        let priv_pem = private_key_to_pkcs8_pem(&priv_key);
        let pub_pem = public_key_to_spki_pem(&pub_key);
        let actor_uri = "https://remote.example/users/approver";
        let key_id = format!("{actor_uri}#main-key");
        services
            .cache_remote_public_key(&key_id, actor_uri, None, &pub_pem)
            .unwrap();
        let now_ms = services.clock.now_ms();
        let headers = signed_get_headers("/x/1", "example.local", &key_id, &priv_pem, &http_date(now_ms));
        let viewer = authenticate_fetch_viewer(&services, "/x/1", &headers);
        assert_eq!(viewer.actor_uri.as_deref(), Some(actor_uri));
        assert_eq!(
            decide_fetch(&viewer, stub_authorize("approved", &viewer)),
            Ok(())
        );
    }

    #[test]
    fn signed_get_with_wrong_path_is_anonymous() {
        let services = test_services();
        let (priv_key, pub_key) = generate_rsa_keypair_2048().unwrap();
        let priv_pem = private_key_to_pkcs8_pem(&priv_key);
        let pub_pem = public_key_to_spki_pem(&pub_key);
        let actor_uri = "https://remote.example/users/signer2";
        let key_id = format!("{actor_uri}#main-key");
        services
            .cache_remote_public_key(&key_id, actor_uri, None, &pub_pem)
            .unwrap();
        let now_ms = services.clock.now_ms();
        // Signed for /x/1, fetched as /x/2: the signature must not verify
        // (fail closed → anonymous → gated objects 404).
        let headers = signed_get_headers("/x/1", "example.local", &key_id, &priv_pem, &http_date(now_ms));
        let viewer = authenticate_fetch_viewer(&services, "/x/2", &headers);
        assert!(viewer.is_anonymous());
        assert_eq!(
            decide_fetch(&viewer, stub_authorize("approved", &viewer)),
            Err(StatusCode::NOT_FOUND)
        );
    }

    #[test]
    fn malformed_signature_header_is_anonymous() {
        let services = test_services();
        let mut headers = HeaderMap::new();
        headers.insert("signature", "not-a-valid-signature".parse().unwrap());
        let viewer = authenticate_fetch_viewer(&services, "/x/1", &headers);
        assert!(viewer.is_anonymous());
    }
}
