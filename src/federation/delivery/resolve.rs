//! Remote actor/note/stamp resolution via signed fetch
//! (moved from `src/federation/delivery.rs`).
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

use serde_json::Value;

use crate::activitypub::actor::{ParsedRemoteActor, parse_remote_actor, parse_url_host};
use crate::activitypub::objects::{ParsedRemoteNote, parse_remote_note};
use crate::activitypub::signatures::actor_uri_from_key_id;
use crate::application::commands::AppServices;
use crate::application::ports::{CreateAccountInput, PortError, UpdateAccountPatch};
use crate::domain::account::Account;

use super::client::SafeHttpClient;
use super::errors::MAX_AP_JSON_BYTES;
use super::sign::signed_get_json;
use super::url::is_ip_literal_host;

/// Discover a remote user's canonical actor URI via WebFinger
/// (`/.well-known/webfinger?resource=acct:{username}@{host}`).
pub fn webfinger_discover_actor_uri(
    client: &SafeHttpClient,
    username: &str,
    host: &str,
    scheme_override: Option<&str>,
) -> Result<String, String> {
    let resource = format!("acct:{}@{}", username.trim(), host.trim());
    let encoded_resource = resource.replace('@', "%40").replace(':', "%3A");
    let schemes: Vec<&str> = match scheme_override {
        Some(s) => vec![s],
        // IP-literal hosts (common in test rigs and LAN deployments, often
        // with a port) never serve public TLS: go straight to `http` instead
        // of burning a full timeout on a doomed `https` attempt.
        None if is_ip_literal_host(host) => vec!["http"],
        None => vec!["https", "http"],
    };
    let mut last_err = String::new();
    for scheme in schemes {
        let wf_url = format!(
            "{scheme}://{}/.well-known/webfinger?resource={encoded_resource}",
            host.trim()
        );
        let headers = [(
            "Accept",
            "application/jrd+json, application/json;q=0.9, application/xrd+xml;q=0.8",
        )];
        match client.send("GET", &wf_url, &headers, None, MAX_AP_JSON_BYTES) {
            Ok(resp) if (200..300).contains(&resp.status) => {
                if let Ok(jrd) = serde_json::from_slice::<Value>(&resp.body) {
                    if let Some(links) = jrd.get("links").and_then(Value::as_array) {
                        for link in links {
                            let rel = link.get("rel").and_then(Value::as_str).unwrap_or("");
                            let typ = link.get("type").and_then(Value::as_str).unwrap_or("");
                            if rel == "self"
                                && (typ.contains("activity+json") || typ.contains("ld+json"))
                            {
                                if let Some(href) = link.get("href").and_then(Value::as_str) {
                                    return Ok(href.trim().to_string());
                                }
                            }
                        }
                    }
                }
                last_err = format!("WebFinger JRD at {wf_url} had no ActivityPub self link");
            }
            Ok(resp) => {
                last_err = format!("WebFinger {wf_url} returned HTTP {}", resp.status);
            }
            Err(e) => {
                last_err = e.to_string();
            }
        }
    }
    Err(last_err)
}

/// Fetch a remote ActivityPub Actor by URI, verify its origin host, and
/// upsert/cache the remote [`Account`] and its public key in storage.
pub fn fetch_and_upsert_remote_actor(
    services: &AppServices,
    client: &SafeHttpClient,
    actor_uri: &str,
) -> Result<(Account, ParsedRemoteActor), PortError> {
    let doc = signed_get_json(
        services,
        client,
        actor_uri,
        "application/activity+json, application/ld+json;q=0.9, application/json;q=0.8",
    )
    .map_err(PortError::Validation)?;
    upsert_remote_actor_from_doc(services, actor_uri, &doc)
}

/// Upsert a remote [`Account`] and cache its public key from an already-fetched
/// Actor JSON document, enforcing same-origin authority between `expected_uri`
/// and `doc["id"]`.
pub fn upsert_remote_actor_from_doc(
    services: &AppServices,
    expected_uri: &str,
    doc: &Value,
) -> Result<(Account, ParsedRemoteActor), PortError> {
    let parsed = parse_remote_actor(doc).map_err(PortError::Validation)?;
    let (_, expected_host) = parse_url_host(expected_uri)
        .ok_or_else(|| PortError::Validation(format!("invalid expected URI: {expected_uri}")))?;
    if parsed.host != expected_host {
        return Err(PortError::Validation(format!(
            "actor id host {} does not match requested URI host {}",
            parsed.host, expected_host
        )));
    }

    let description = parsed
        .summary_mfm
        .clone()
        .or_else(|| {
            parsed
                .summary_html
                .as_deref()
                .map(crate::activitypub::objects::sanitize_html_to_mfm)
        })
        .filter(|s| !s.is_empty());

    let account = if let Some(existing) = services.accounts.find_by_uri(&parsed.uri)?.or(services
        .accounts
        .find_by_username_and_host(&parsed.username, Some(&parsed.host))?)
    {
        // Batch P: refresh the move-handshake columns from the document
        // (`updatePerson` parity). `alsoKnownAs` overwrites only when the
        // document carries the field; `movedTo` is set-only (patch has no
        // clear — an account never un-moves).
        let mut patch = UpdateAccountPatch {
            display_name: Some(parsed.display_name.clone()),
            description: Some(description),
            location: Some(parsed.location.clone()),
            birthday: None,
            avatar_id: None,
            banner_id: None,
            is_locked: Some(parsed.manually_approves_followers),
            is_bot: Some(parsed.is_bot()),
            is_cat: Some(parsed.is_cat),
            is_explorable: Some(parsed.discoverable),
            ..Default::default()
        };
        if parsed.also_known_as.is_some() {
            patch.also_known_as = Some(parsed.also_known_as.clone());
        }
        if let Some(moved) = parsed.moved_to_uri.clone() {
            patch.moved_to_uri = Some(moved);
            patch.moved_at_ms = Some(services.clock.now_ms());
        }
        services
            .accounts
            .update_account(&existing.id, patch, Vec::new())?
    } else {
        services.accounts.create_account(
            CreateAccountInput {
                handle: parsed.username.clone(),
                host: Some(parsed.host.clone()),
                display_name: parsed.display_name.clone(),
                description,
                location: parsed.location.clone(),
                birthday: None,
                uri: Some(parsed.uri.clone()),
                inbox: Some(parsed.inbox.clone()),
                shared_inbox: parsed.shared_inbox.clone(),
                is_bot: parsed.is_bot(),
                is_cat: parsed.is_cat,
                is_locked: parsed.manually_approves_followers,
                preserved_usernames: Vec::new(),
            },
            None,
        )?
    };
    // Batch P: seed the move-handshake columns on first sight (same fields
    // as the refresh path above; one extra txn only when present). Rebind:
    // callers read the handshake columns off the returned row, and the
    // pre-seed struct does not have them.
    let mut account = account;
    if parsed.also_known_as.is_some() || parsed.moved_to_uri.is_some() {
        let mut patch = UpdateAccountPatch {
            ..Default::default()
        };
        if parsed.also_known_as.is_some() {
            patch.also_known_as = Some(parsed.also_known_as.clone());
        }
        if let Some(moved) = parsed.moved_to_uri.clone() {
            patch.moved_to_uri = Some(moved);
            patch.moved_at_ms = Some(services.clock.now_ms());
        }
        account = services
            .accounts
            .update_account(&account.id, patch, Vec::new())?;
    }

    // Slice 3 (H5): run domain validation on the upsert path (both the
    // freshly created and the previously stored account) before caching any
    // key, so overlong or otherwise illegal inbox/URI/username values from
    // the document are refused even if parsing ever let them through.
    // (`PortError` is an alias for `DomainError`, so `?` preserves the
    // `Validation` variant the slice asks to map to.)
    account.validate()?;

    // Slice 2 (C1): cache only the key id the actor document actually
    // advertises. The old speculative `{actor}/publickey` alias wrote cache
    // rows for key ids the actor never claimed, widening the poisoning
    // surface; lookups for any other key-id shape refetch instead.
    services.cache_remote_public_key(
        &parsed.public_key_id,
        &parsed.uri,
        Some(account.id.clone()),
        &parsed.public_key_pem,
    )?;

    // WP8b remote-emoji metadata ingest (actor documents, no fetch).
    // Idempotent upserts; only storage faults propagate.
    if !parsed.emojis.is_empty() {
        services.ingest_remote_emoji_tags(&parsed.host, &parsed.emojis)?;
    }

    Ok((account, parsed))
}

/// Resolve a signer `keyId` to `(owner_actor_uri, public_key_pem)`,
/// consulting the local cache first and fetching the remote actor document
/// on miss (or when `force_refresh` is true). The returned URI is the key's
/// owner: callers must compare the activity actor against it, never against
/// a string derived from the keyId.
pub fn resolve_remote_public_key(
    services: &AppServices,
    client: &SafeHttpClient,
    key_id: &str,
    force_refresh: bool,
) -> Result<(String, String), PortError> {
    if !force_refresh {
        if let Some(cached) = services.auth.get_remote_public_key(key_id)? {
            return Ok((cached.actor_uri, cached.public_key_pem));
        }
    }
    let actor_uri = actor_uri_from_key_id(key_id);
    let (_account, parsed) = fetch_and_upsert_remote_actor(services, client, &actor_uri)?;
    // Slice 2 (C1): the fetched document must actually advertise the
    // requested key id. Without this, a keyId naming one actor could
    // resolve to a key owned by another.
    if parsed.public_key_id.trim() != key_id.trim() {
        return Err(PortError::Validation(format!(
            "fetched publicKey id {} does not match requested key id {key_id}",
            parsed.public_key_id
        )));
    }
    Ok((parsed.uri, parsed.public_key_pem))
}

/// Fetch a FEP-044f `QuoteAuthorization` stamp document by IRI (signed
/// GET). Returns the raw JSON; verification (type, fields, authority) is
/// the caller's job via `quotes::verify_stamp_for_quote`. Transport or
/// parse failures are transient: the caller maps them to `Unknown`, never
/// to rejection or revocation.
pub fn fetch_quote_stamp_doc(
    services: &AppServices,
    client: &SafeHttpClient,
    stamp_uri: &str,
) -> Result<Value, PortError> {
    let doc = signed_get_json(
        services,
        client,
        stamp_uri,
        "application/activity+json, application/ld+json;q=0.9, application/json;q=0.8",
    )
    .map_err(|e| PortError::Validation(format!("fetch stamp: {e}")))?;
    if !doc.is_object() {
        return Err(PortError::Validation(
            "stamp document is not a JSON object".to_string(),
        ));
    }
    Ok(doc)
}

/// Fetch and parse a remote ActivityPub `Note` by URI, verifying that its
/// `attributedTo` actor shares the same origin host as `note_uri`.
pub fn fetch_remote_note(
    services: &AppServices,
    client: &SafeHttpClient,
    note_uri: &str,
) -> Result<ParsedRemoteNote, PortError> {
    let doc = signed_get_json(
        services,
        client,
        note_uri,
        "application/activity+json, application/ld+json;q=0.9, application/json;q=0.8",
    )
    .map_err(PortError::Validation)?;
    let obj = if doc.get("type").and_then(Value::as_str) == Some("Create") {
        doc.get("object").cloned().unwrap_or(doc)
    } else {
        doc
    };
    let parsed = parse_remote_note(&obj).map_err(PortError::Validation)?;
    let (_, note_host) = parse_url_host(&parsed.uri)
        .ok_or_else(|| PortError::Validation("invalid remote note URI".to_string()))?;
    let (_, req_host) = parse_url_host(note_uri)
        .ok_or_else(|| PortError::Validation("invalid requested note URI".to_string()))?;
    let (_, author_host) = parse_url_host(&parsed.attributed_to)
        .ok_or_else(|| PortError::Validation("invalid remote note attributedTo URI".to_string()))?;
    if note_host != req_host || note_host != author_host {
        return Err(PortError::Validation(format!(
            "remote note origin mismatch: req={req_host}, note={note_host}, author={author_host}"
        )));
    }
    Ok(parsed)
}
