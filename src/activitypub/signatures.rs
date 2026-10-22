//! Slice A3: RSA-2048 keys, PEM codecs, PKCS#1 v1.5 `rsa-sha256`,
//! `Digest` header helpers, and legacy HTTP Signatures (draft-cavage).
//!
//! RSA math, key generation, and PEM/DER codecs come from the maintained
//! RustCrypto `rsa` crate (pure Rust); base64 comes from the `base64` crate.
//! This module keeps the HTTP-signature layer plus thin crate-backed wrappers
//! with the pre-Slice-6 public surface (`sha2` for hashing, `rand` for key
//! generation, `axum::http` for headers, `chrono` for `Date` skew checks).

use axum::http::{HeaderMap, HeaderName, HeaderValue};
use base64::Engine as _;
use rsa::pkcs1::{DecodeRsaPrivateKey, DecodeRsaPublicKey, EncodeRsaPrivateKey, LineEnding};
use rsa::pkcs1v15::{Signature, SigningKey, VerifyingKey};
use rsa::pkcs8::{DecodePrivateKey, EncodePrivateKey};
use spki::{DecodePublicKey, EncodePublicKey};
use sha2::{Digest as _, Sha256, Sha512};
use signature::Verifier;
use std::str::FromStr;

use crate::config::Rfc9421Mode;
use std::sync::atomic::{AtomicU8, Ordering};

/// RSA public key (RustCrypto `rsa` crate type, re-exported so existing
/// `signatures::RsaPublicKey` import paths keep working).
pub use rsa::RsaPublicKey;
/// RSA private key (RustCrypto `rsa` crate type, re-exported so existing
/// `signatures::RsaPrivateKey` import paths keep working).
pub use rsa::RsaPrivateKey;

// ---------------------------------------------------------------------------
// RSA key generation (via the `rsa` crate)
// ---------------------------------------------------------------------------

/// Generate a random RSA keypair of `bits` bits (default 2048 via
/// [`generate_rsa_keypair_2048`]). Returns `(private_key, public_key)`.
pub fn generate_rsa_keypair(bits: usize) -> Result<(RsaPrivateKey, RsaPublicKey), String> {
    let priv_key = RsaPrivateKey::new(&mut rand_core::OsRng, bits)
        .map_err(|e| format!("RSA key generation failed: {e}"))?;
    let pub_key = RsaPublicKey::from(&priv_key);
    Ok((priv_key, pub_key))
}

/// Generate a random 2048-bit RSA keypair.
pub fn generate_rsa_keypair_2048() -> Result<(RsaPrivateKey, RsaPublicKey), String> {
    generate_rsa_keypair(2048)
}

// ---------------------------------------------------------------------------
// Base64 (RFC 4648 standard alphabet, via the `base64` crate)
// ---------------------------------------------------------------------------

/// Standard-alphabet base64 encoding (with padding).
pub fn base64_encode(data: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(data)
}

/// Standard-alphabet base64 decoding (with padding). ASCII whitespace is
/// ignored for backwards compatibility with the pre-Slice-6 codec.
pub fn base64_decode(s: &str) -> Result<Vec<u8>, String> {
    let clean: String = s.chars().filter(|c| !c.is_whitespace()).collect();
    base64::engine::general_purpose::STANDARD
        .decode(clean.as_str())
        .map_err(|e| format!("invalid base64: {e}"))
}

// ---------------------------------------------------------------------------
// PEM encoding / decoding (via `rsa`'s pkcs1/pkcs8/spki support)
// ---------------------------------------------------------------------------

/// Encode a public key as `-----BEGIN PUBLIC KEY-----` (SPKI) PEM, the form
/// Misskey (`publicKeyPem`) and Node `crypto` expect, with LF line endings.
pub fn public_key_to_spki_pem(key: &RsaPublicKey) -> String {
    key.to_public_key_pem(LineEnding::LF)
        .expect("in-memory RSA public key always encodes as SPKI PEM")
        .as_str()
        .to_owned()
}

/// Encode a private key as `-----BEGIN PRIVATE KEY-----` (PKCS#8) PEM.
pub fn private_key_to_pkcs8_pem(key: &RsaPrivateKey) -> String {
    key.to_pkcs8_pem(LineEnding::LF)
        .expect("in-memory RSA private key always encodes as PKCS#8 PEM")
        .as_str()
        .to_owned()
}

/// Encode a private key as `-----BEGIN RSA PRIVATE KEY-----` (PKCS#1) PEM.
pub fn private_key_to_pkcs1_pem(key: &RsaPrivateKey) -> String {
    key.to_pkcs1_pem(LineEnding::LF)
        .expect("in-memory RSA private key always encodes as PKCS#1 PEM")
        .as_str()
        .to_owned()
}

/// Parse a `PUBLIC KEY` (SPKI) or `RSA PUBLIC KEY` (raw PKCS#1) PEM.
pub fn parse_public_key_pem(pem: &str) -> Result<RsaPublicKey, String> {
    // Check the longer `RSA PUBLIC KEY` label first: it contains `PUBLIC KEY`
    // as a substring.
    if pem.contains("RSA PUBLIC KEY") {
        RsaPublicKey::from_pkcs1_pem(pem)
            .map_err(|e| format!("parse PKCS#1 RSA public key PEM: {e}"))
    } else if pem.contains("PUBLIC KEY") {
        RsaPublicKey::from_public_key_pem(pem)
            .map_err(|e| format!("parse SPKI public key PEM: {e}"))
    } else {
        Err("unsupported public key PEM (expected PUBLIC KEY or RSA PUBLIC KEY)".to_string())
    }
}

/// Parse a `PRIVATE KEY` (PKCS#8) or `RSA PRIVATE KEY` (PKCS#1) PEM.
pub fn parse_private_key_pem(pem: &str) -> Result<RsaPrivateKey, String> {
    // Check the longer `RSA PRIVATE KEY` label first: it contains
    // `PRIVATE KEY` as a substring.
    if pem.contains("RSA PRIVATE KEY") {
        RsaPrivateKey::from_pkcs1_pem(pem)
            .map_err(|e| format!("parse PKCS#1 RSA private key PEM: {e}"))
    } else if pem.contains("PRIVATE KEY") {
        RsaPrivateKey::from_pkcs8_pem(pem)
            .map_err(|e| format!("parse PKCS#8 private key PEM: {e}"))
    } else {
        Err("unsupported private key PEM (expected PRIVATE KEY or RSA PRIVATE KEY)".to_string())
    }
}

// ---------------------------------------------------------------------------
// PKCS#1 v1.5 rsa-sha256 sign & verify (via `rsa::pkcs1v15`)
// ---------------------------------------------------------------------------

/// Sign `message` with PKCS#1 v1.5 + SHA-256. Returns the raw signature bytes.
///
/// Slice 12 (M6, decision D-f(a)): signing runs on `ring` (constant-time
/// RSA with blinding) because RUSTSEC-2023-0071 (Marvin timing attack) is
/// unpatched in the `rsa` crate. The `rsa` crate still parses the stored
/// PKCS#1/PKCS#8 PEM (so existing keys load unchanged) and verifies; its
/// private-key operation is never used. `ring` requires >= 2048-bit keys.
pub fn rsa_sha256_sign(private_key_pem: &str, message: &[u8]) -> Result<Vec<u8>, String> {
    let key = parse_private_key_pem(private_key_pem)?;
    let der = key
        .to_pkcs1_der()
        .map_err(|e| format!("encode PKCS#1 DER: {e}"))?;
    let pair = ring::signature::RsaKeyPair::from_der(der.as_bytes())
        .map_err(|e| format!("load RSA key into ring (needs >= 2048 bits): {e}"))?;
    let mut sig = vec![0u8; pair.public().modulus_len()];
    pair.sign(
        &ring::signature::RSA_PKCS1_SHA256,
        &ring::rand::SystemRandom::new(),
        message,
        &mut sig,
    )
    .map_err(|e| format!("ring RSA sign: {e}"))?;
    Ok(sig)
}

/// Sign `message` and return the base64-encoded signature.
pub fn rsa_sha256_sign_base64(private_key_pem: &str, message: &[u8]) -> Result<String, String> {
    Ok(base64_encode(&rsa_sha256_sign(
        private_key_pem,
        message,
    )?))
}

/// Verify a PKCS#1 v1.5 + SHA-256 signature. Returns `Ok(false)` for a
/// well-formed but non-matching signature; `Err` for undecodable keys.
pub fn rsa_sha256_verify(
    public_key_pem: &str,
    message: &[u8],
    signature: &[u8],
) -> Result<bool, String> {
    let key = parse_public_key_pem(public_key_pem)?;
    let verifying_key = VerifyingKey::<Sha256>::new(key);
    let Ok(sig) = Signature::try_from(signature) else {
        return Ok(false);
    };
    Ok(verifying_key.verify(message, &sig).is_ok())
}

// ---------------------------------------------------------------------------
// Digest header (RFC 9530 style "SHA-256=<base64>" used by Misskey/Sharkey)
// ---------------------------------------------------------------------------

/// Compute the `Digest` header value `"SHA-256=<base64(sha256(body))>"`.
pub fn compute_digest_header(body: &[u8]) -> String {
    format!("SHA-256={}", base64_encode(Sha256::digest(body).as_slice()))
}

/// Verify a `Digest` header value against `body`. The algorithm name match is
/// case-insensitive and other comma-separated digests are ignored.
pub fn verify_digest_header(header_value: &str, body: &[u8]) -> bool {
    let expected = Sha256::digest(body);
    for part in header_value.split(',') {
        let part = part.trim();
        let Some((algo, value)) = part.split_once('=') else {
            continue;
        };
        if !algo.trim().eq_ignore_ascii_case("SHA-256") {
            continue;
        }
        let value = value.trim().trim_matches('"');
        let Ok(raw) = base64_decode(value) else {
            continue;
        };
        if raw.as_slice() == expected.as_slice() {
            return true;
        }
    }
    false
}

// ---------------------------------------------------------------------------
// Legacy HTTP Signatures (draft-cavage-http-signatures)
// ---------------------------------------------------------------------------

/// Headers produced by [`sign_request_headers`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignedRequestHeaders {
    pub date: String,
    pub digest: Option<String>,
    /// Full value for the `Signature` header.
    pub signature: String,
}

/// Parsed `Signature` header (or `Authorization: Signature ...`) value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedSignature {
    pub key_id: String,
    pub algorithm: String,
    pub headers: Vec<String>,
    pub signature: Vec<u8>,
    /// `created` parameter, the value of the `(created)` pseudo-header.
    pub created: Option<String>,
    /// `expires` parameter, the value of the `(expires)` pseudo-header.
    pub expires: Option<String>,
}

/// Sign an outgoing request the way Misskey/Sharkey `ApRequestService` does:
/// POST (or any call with `body`) signs
/// `(request-target) date host digest`; GET (no body) signs
/// `(request-target) date host`.
pub fn sign_request_headers(
    method: &str,
    path_and_query: &str,
    host: &str,
    date: &str,
    body: Option<&[u8]>,
    key_id: &str,
    private_key_pem: &str,
) -> Result<SignedRequestHeaders, String> {
    let mut lines = vec![
        format!("(request-target): {} {}", method.to_lowercase(), path_and_query),
        format!("date: {date}"),
        format!("host: {host}"),
    ];
    let mut header_names = "(request-target) date host".to_string();
    let digest = if let Some(b) = body {
        let d = compute_digest_header(b);
        lines.push(format!("digest: {d}"));
        header_names.push_str(" digest");
        Some(d)
    } else {
        None
    };
    let signing_string = lines.join("\n");
    let sig_b64 = rsa_sha256_sign_base64(private_key_pem, signing_string.as_bytes())?;
    let signature = format!(
        "keyId=\"{key_id}\",algorithm=\"rsa-sha256\",headers=\"{header_names}\",signature=\"{sig_b64}\""
    );
    Ok(SignedRequestHeaders {
        date: date.to_string(),
        digest,
        signature,
    })
}

/// Parse a `Signature` header value (also accepts an
/// `Authorization: Signature ...` value).
pub fn parse_signature_header(header_value: &str) -> Result<ParsedSignature, String> {
    let mut v = header_value.trim();
    // Tolerate an `Authorization: Signature ...` value.
    if v.len() >= 9 && v[..9].eq_ignore_ascii_case("signature")
        && (v.len() == 9 || v.as_bytes()[9].is_ascii_whitespace())
    {
        v = v[9..].trim_start();
    }
    let mut key_id: Option<String> = None;
    let mut algorithm = "rsa-sha256".to_string();
    let mut headers: Option<Vec<String>> = None;
    let mut signature: Option<Vec<u8>> = None;
    let mut created: Option<String> = None;
    let mut expires: Option<String> = None;
    for part in split_quoted_commas(v) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (k, val) = part
            .split_once('=')
            .ok_or_else(|| format!("malformed signature param: {part}"))?;
        let val = val.trim().trim_matches('"').to_string();
        match k.trim().to_lowercase().as_str() {
            "keyid" => key_id = Some(val),
            "algorithm" => algorithm = val.to_lowercase(),
            "headers" => {
                headers = Some(val.split_whitespace().map(|s| s.to_string()).collect());
            }
            "signature" => signature = Some(base64_decode(&val)?),
            "created" => created = Some(val),
            "expires" => expires = Some(val),
            _ => {}
        }
    }
    if algorithm != "rsa-sha256" && algorithm != "hs2019" {
        return Err(format!("unsupported signature algorithm: {algorithm}"));
    }
    Ok(ParsedSignature {
        key_id: key_id.ok_or("signature missing keyId")?,
        algorithm,
        headers: headers.unwrap_or_else(|| vec!["date".to_string()]),
        signature: signature.ok_or("signature missing signature")?,
        created,
        expires,
    })
}

fn split_quoted_commas(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    for c in s.chars() {
        match c {
            '"' => {
                in_quotes = !in_quotes;
                cur.push(c);
            }
            ',' if !in_quotes => {
                parts.push(std::mem::take(&mut cur));
            }
            _ => cur.push(c),
        }
    }
    parts.push(cur);
    parts
}

/// Find the first legacy draft-cavanaugh signature value across the
/// `Signature` and `Authorization` headers: `None` when neither is present,
/// otherwise the first value that parses (an unparsable `Signature` header
/// falls through to `Authorization`, so dual-sign senders may carry the
/// RFC 9421 dictionary in one slot and the legacy value in the other; when
/// nothing parses, the last parse error is returned).
pub fn parse_first_legacy_signature(headers: &HeaderMap) -> Option<Result<ParsedSignature, String>> {
    let mut error: Option<String> = None;
    for name in ["signature", "authorization"] {
        if let Some(raw) = headers.get(name).and_then(|v| v.to_str().ok()) {
            match parse_signature_header(raw) {
                Ok(parsed) => return Some(Ok(parsed)),
                Err(err) => error = Some(err),
            }
        }
    }
    error.map(Err)
}

/// Map `{actor}#main-key` and `{actor}/publickey` back to `{actor}`.
///
/// Slice 2 (C1): this is only a fetch locator (turning a keyId into the
/// actor document to download). It must never be used as authority: inbox
/// authentication compares `activity.actor` against the key's cached owner,
/// and the fetched document's `publicKey.id` must equal the requested keyId.
pub fn actor_uri_from_key_id(key_id: &str) -> String {
    if let Some((actor, _)) = key_id.split_once('#') {
        return actor.to_string();
    }
    if let Some(actor) = key_id.strip_suffix("/publickey") {
        return actor.to_string();
    }
    key_id.to_string()
}

fn header_value(headers: &HeaderMap, name: &str) -> Result<String, String> {
    let hn = HeaderName::from_str(&name.to_lowercase())
        .map_err(|_| format!("invalid header name: {name}"))?;
    let v = headers
        .get(&hn)
        .ok_or_else(|| format!("missing header: {name}"))?;
    v.to_str()
        .map(|s| s.to_string())
        .map_err(|_| format!("non-visible header value: {name}"))
}

/// Build the signing string: lowercase `name: value` lines joined by `\n`,
/// with `(request-target): {method} {path_and_query}`.
pub fn build_signing_string(
    method: &str,
    path_and_query: &str,
    headers: &HeaderMap,
    signed_header_names: &[String],
    created: Option<&str>,
    expires: Option<&str>,
) -> Result<String, String> {
    let mut lines = Vec::with_capacity(signed_header_names.len());
    for name in signed_header_names {
        if name.eq_ignore_ascii_case("(request-target)") {
            lines.push(format!(
                "(request-target): {} {}",
                method.to_lowercase(),
                path_and_query
            ));
        } else if name.eq_ignore_ascii_case("(created)") {
            // Draft HTTP Signatures pseudo-header: the signature's `created` parameter.
            let created = created.ok_or("signed headers use (created) but the signature has no created parameter")?;
            lines.push(format!("(created): {created}"));
        } else if name.eq_ignore_ascii_case("(expires)") {
            let expires = expires.ok_or("signed headers use (expires) but the signature has no expires parameter")?;
            lines.push(format!("(expires): {expires}"));
        } else {
            lines.push(format!("{}: {}", name.to_lowercase(), header_value(headers, name)?));
        }
    }
    Ok(lines.join("\n"))
}

/// Parse an HTTP date: RFC 2822 first, then RFC 7231 IMF-fixdate.
/// Returns milliseconds since the Unix epoch.
pub fn parse_http_date(s: &str) -> Result<i64, String> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc2822(s) {
        return Ok(dt.timestamp_millis());
    }
    if let Ok(naive) =
        chrono::NaiveDateTime::parse_from_str(s, "%a, %d %b %Y %H:%M:%S GMT")
    {
        return Ok(naive.and_utc().timestamp_millis());
    }
    Err(format!("unparseable Date header: {s}"))
}

/// Verify an incoming signed request.
///
/// Requires `(request-target)`, `host` and `date` in the signed header list
/// (plus `digest` when `body` is present), validates the `Digest` header
/// against `body`, checks `Date` skew when `now_ms` is `Some`, and verifies
/// the RSA-SHA256 signature over the rebuilt signing string.
pub fn verify_http_signature(
    method: &str,
    path_and_query: &str,
    headers: &HeaderMap,
    body: Option<&[u8]>,
    public_key_pem: &str,
    now_ms: Option<i64>,
    max_clock_skew_secs: i64,
) -> Result<ParsedSignature, String> {
    let parsed = parse_first_legacy_signature(headers)
        .ok_or("missing Signature header")??;
    let names: Vec<String> = parsed.headers.iter().map(|h| h.to_lowercase()).collect();
    for required in ["(request-target)", "host", "date"] {
        if !names.iter().any(|h| h == required) {
            return Err(format!("signed headers missing required {required}"));
        }
    }
    if body.is_some() && !names.iter().any(|h| h == "digest") {
        return Err("signed headers missing required digest".to_string());
    }
    if let Some(b) = body {
        let digest = header_value(headers, "digest")?;
        if !verify_digest_header(&digest, b) {
            return Err("Digest header does not match body".to_string());
        }
    }
    let date_str = header_value(headers, "date")?;
    let date_ms = parse_http_date(&date_str)?;
    if let Some(now) = now_ms {
        if (now - date_ms).abs() > max_clock_skew_secs * 1000 {
            return Err("Date header outside allowed clock skew".to_string());
        }
    }
    let signing_string = build_signing_string(
        method,
        path_and_query,
        headers,
        &parsed.headers,
        parsed.created.as_deref(),
        parsed.expires.as_deref(),
    )?;
    if !rsa_sha256_verify(public_key_pem, signing_string.as_bytes(), &parsed.signature)? {
        return Err("HTTP signature verification failed".to_string());
    }
    Ok(parsed)
}

// ---------------------------------------------------------------------------
// RFC 9421 HTTP signatures (httpbis) + inbound mode gating (Batch E)
// ---------------------------------------------------------------------------
//
// Supported profile (documented contract; `enforce` requires exactly this):
//
// * Transport: `Signature-Input` dictionary with exactly the parsed first
//   member used for verification (extra members are ignored, never trusted;
//   duplicate labels are rejected), plus the matching `Signature`
//   dictionary entry (`sig1=:base64:`). A leading `Signature ` prefix (as in
//   `Authorization: Signature ...`) is tolerated on the `Signature` value.
// * Covered components: `@method`, `@authority` and (`@path` or
//   `@target-uri`) are REQUIRED. `@scheme` and `@query` are supported when
//   covered. Any other `@`-component is rejected. Any other covered name is
//   treated as a plain header field (e.g. `date`, `host`, `digest`,
//   `content-digest`) and must be present. Covered components must not carry
//   parameters (`;key`, `;bs`, ... are rejected).
// * Body integrity: when a body is present, `content-digest` or `digest`
//   must be covered and must verify against the body (`Content-Digest`
//   supports `sha-256`/`sha-512`; legacy `Digest` reuses
//   [`verify_digest_header`]).
// * Algorithms: `rsa-v1_5-sha256` only (`rsa-sha256` accepted as an alias
//   for the same PKCS#1 v1.5 + SHA-256 operation this crate implements).
//   Every other `alg` (including `hmac-sha256`, `ed25519`, `ecdsa-*`,
//   `rsa-pss-sha512`) is rejected. An absent `alg` is assumed to be
//   `rsa-v1_5-sha256` (documented assumption: this stack only holds RSA
//   keys, and the key is resolved through the existing keyid-to-owner
//   lookup before verification).
// * Freshness: `created` (integer seconds) is REQUIRED and must be within
//   `max_clock_skew_secs` of `now_ms` when a clock is supplied. `expires`,
//   when present, must not be past (`now <= expires + skew`). `nonce`, when
//   present, is parsed and ignored: inbox replays are already covered by the
//   activity-id dedup gate, and the fetch gate never serves side effects.
// * Malformed input (bad quoting, duplicate covered components, duplicate
//   parameter keys, empty covered lists, non-integer timestamps) is
//   rejected, never skipped.
//
// Mode plumbing: the configured [`Rfc9421Mode`] reaches the inbox and fetch
// gate through the process-wide [`rfc9421_mode`]/[`set_rfc9421_mode`] cell
// below (default `permissive`, matching `Config::default()`). This is
// interim plumbing forced by batch file ownership (`src/config.rs` and
// `src/main.rs` belong to another batch, and `AppServices` cannot gain a
// field without breaking sibling-owned literal constructors): the parent
// wires one line in `main.rs` after config load,
// `set_rfc9421_mode(cfg.federation.rfc9421_mode)`, and may later move the
// mode into `AppServices`. The pure decision function [`apply_sig_mode`]
// takes the mode as a parameter so every mode combination is hermetically
// testable without the global.

/// Process-wide effective inbound signature mode (see module docs).
static INBOUND_RFC9421_MODE: AtomicU8 = AtomicU8::new(0);

/// Read the effective inbound signature mode (default `permissive`).
pub fn rfc9421_mode() -> Rfc9421Mode {
    match INBOUND_RFC9421_MODE.load(Ordering::SeqCst) {
        1 => Rfc9421Mode::Enforce,
        _ => Rfc9421Mode::Permissive,
    }
}

/// Set the effective inbound signature mode (startup wiring + tests).
pub fn set_rfc9421_mode(mode: Rfc9421Mode) {
    INBOUND_RFC9421_MODE.store(
        match mode {
            Rfc9421Mode::Enforce => 1,
            Rfc9421Mode::Permissive => 0,
        },
        Ordering::SeqCst,
    );
}

/// Scheme implied by a configured `public_url` (for `@scheme`/`@target-uri`
/// reconstruction). Anything that is not explicit `http://` is `https://`.
pub fn scheme_of_public_url(public_url: &str) -> &'static str {
    if public_url.trim_start().starts_with("http://") {
        "http"
    } else {
        "https"
    }
}

/// True when the request carries a legacy draft-cavanaugh signature value
/// (a `Signature`/`Authorization` header mentioning `keyId`). Advisory
/// presence heuristic only: parsing decides acceptance.
pub fn has_legacy_signature(headers: &HeaderMap) -> bool {
    for name in ["signature", "authorization"] {
        if let Some(value) = headers.get(name).and_then(|v| v.to_str().ok())
            && value.to_ascii_lowercase().contains("keyid")
        {
            return true;
        }
    }
    false
}

/// Outcome of one signature-scheme verification attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VerifyOutcome {
    /// Cryptographically valid; carries the key owner's actor URI.
    Verified(String),
    /// Absent-unusable or invalid; carries a log-safe detail string.
    Failed(String),
}

/// Pure mode gate combining the legacy and RFC 9421 attempt outcomes.
///
/// * `permissive`: accept when EITHER scheme verifies (dual-sign interop);
///   when both verify they must agree on the owner (actor-mismatch fails
///   closed). RFC 9421 fields, when present, are always verified — never
///   merely logged — but a valid legacy signature still authenticates.
/// * `enforce`: require a valid RFC 9421 signature; legacy-only, unsigned,
///   and RFC 9421-invalid requests are rejected even with a valid legacy
///   signature. A legacy signature that verifies for a DIFFERENT owner
///   than the RFC 9421 signature also fails closed.
pub fn apply_sig_mode(
    mode: Rfc9421Mode,
    legacy: Option<VerifyOutcome>,
    rfc9421: Option<VerifyOutcome>,
) -> Result<String, String> {
    match mode {
        Rfc9421Mode::Permissive => match (legacy, rfc9421) {
            (Some(VerifyOutcome::Verified(a)), Some(VerifyOutcome::Verified(b))) => {
                if a == b {
                    Ok(a)
                } else {
                    Err(format!(
                        "legacy and RFC 9421 signatures disagree on owner ({a:?} vs {b:?})"
                    ))
                }
            }
            (Some(VerifyOutcome::Verified(a)), _) => Ok(a),
            (_, Some(VerifyOutcome::Verified(b))) => Ok(b),
            (Some(VerifyOutcome::Failed(a)), Some(VerifyOutcome::Failed(b))) => {
                Err(format!("no valid signature (legacy: {a}; rfc9421: {b})"))
            }
            (Some(VerifyOutcome::Failed(a)), None) => Err(a),
            (None, Some(VerifyOutcome::Failed(b))) => Err(b),
            (None, None) => Err("no signature".to_string()),
        },
        Rfc9421Mode::Enforce => match (legacy, rfc9421) {
            (Some(VerifyOutcome::Verified(a)), Some(VerifyOutcome::Verified(b))) => {
                if a == b {
                    Ok(b)
                } else {
                    Err(format!(
                        "legacy and RFC 9421 signatures disagree on owner ({a:?} vs {b:?})"
                    ))
                }
            }
            (_, Some(VerifyOutcome::Verified(b))) => Ok(b),
            (_, Some(VerifyOutcome::Failed(detail))) => Err(detail),
            (_, None) => Err(
                "RFC 9421 signature required (federation.rfc9421_mode=enforce)".to_string(),
            ),
        },
    }
}

// ---------------------------------------------------------------------------
// Content-Digest (RFC 9421 §3.3.4, sf-dictionary of byte sequences)
// ---------------------------------------------------------------------------

/// Compute the `Content-Digest` header value `sha-256=:<base64>:`.
pub fn compute_content_digest_header(body: &[u8]) -> String {
    format!("sha-256=:{}:", base64_encode(Sha256::digest(body).as_slice()))
}

fn parse_content_digest_entries(header_value: &str) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    for part in header_value.split(',') {
        let part = part.trim();
        let Some((algo, value)) = part.split_once('=') else {
            continue;
        };
        let value = value.trim();
        if !(value.len() >= 2 && value.starts_with(':') && value.ends_with(':')) {
            continue;
        }
        let Ok(raw) = base64_decode(&value[1..value.len() - 1]) else {
            continue;
        };
        out.push((algo.trim().to_lowercase(), raw));
    }
    out
}

/// Verify a `Content-Digest` header value against `body`. `sha-256` entries
/// win over `sha-512`; any matching entry of the preferred present
/// algorithm verifies, a present-but-mismatching preferred algorithm fails,
/// and unknown-only/malformed values fail.
pub fn verify_content_digest_header(header_value: &str, body: &[u8]) -> bool {
    let entries = parse_content_digest_entries(header_value);
    for algo in ["sha-256", "sha-512"] {
        let mut seen = false;
        for (name, raw) in &entries {
            if name == algo {
                seen = true;
                let expected = if algo == "sha-256" {
                    Sha256::digest(body).to_vec()
                } else {
                    Sha512::digest(body).to_vec()
                };
                if raw == &expected {
                    return true;
                }
            }
        }
        if seen {
            return false;
        }
    }
    false
}

// ---------------------------------------------------------------------------
// Structured-Fields subset parser (Signature-Input / Signature dictionaries)
// ---------------------------------------------------------------------------

/// A parsed signature-parameter value (supported Structured Fields subset).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SigParamValue {
    Str(String),
    Int(i64),
    Token(String),
    Bytes(Vec<u8>),
    Bool(bool),
}

fn param_value_to_sf(value: &SigParamValue) -> String {
    match value {
        SigParamValue::Str(s) => {
            format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
        }
        SigParamValue::Int(i) => i.to_string(),
        SigParamValue::Token(t) => t.clone(),
        SigParamValue::Bytes(b) => format!(":{}:", base64_encode(b)),
        SigParamValue::Bool(true) => "?1".to_string(),
        SigParamValue::Bool(false) => "?0".to_string(),
    }
}

/// Parsed RFC 9421 `Signature-Input` entry (first dictionary member).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedRfc9421Signature {
    /// Dictionary label matching the `Signature` header entry.
    pub label: String,
    /// Key identifier used to resolve the verification key.
    pub key_id: String,
    /// Effective algorithm (`rsa-v1_5-sha256`; `rsa-sha256` is aliased).
    pub algorithm: String,
    /// `created` timestamp (seconds since epoch).
    pub created: Option<i64>,
    /// `expires` timestamp (seconds since epoch), when sent.
    pub expires: Option<i64>,
    /// Covered components in received order.
    pub covered: Vec<String>,
    /// All signature parameters in received order.
    pub params: Vec<(String, SigParamValue)>,
}

struct SfCursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> SfCursor<'a> {
    fn new(s: &'a str) -> Self {
        Self {
            bytes: s.as_bytes(),
            pos: 0,
        }
    }

    fn eof(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn bump(&mut self) {
        self.pos += 1;
    }

    fn skip_ows(&mut self) {
        while matches!(self.peek(), Some(b' ') | Some(b'\t')) {
            self.bump();
        }
    }

    fn expect_byte(&mut self, want: u8, what: &str) -> Result<(), String> {
        match self.peek() {
            Some(got) if got == want => {
                self.bump();
                Ok(())
            }
            _ => Err(format!("malformed Signature-Input: expected {what}")),
        }
    }

    /// Dictionary key / parameter name: `lcalpha *( lcalpha / DIGIT /
    /// "_" / "-" / "." / "*" )`.
    fn parse_key(&mut self) -> Result<String, String> {
        let start = self.pos;
        match self.peek() {
            Some(b'a'..=b'z') => self.bump(),
            _ => return Err("malformed Signature-Input: expected lowercase key".to_string()),
        }
        while matches!(
            self.peek(),
            Some(b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' | b'.' | b'*')
        ) {
            self.bump();
        }
        String::from_utf8(self.bytes[start..self.pos].to_vec())
            .map_err(|_| "malformed Signature-Input: non-UTF8 key".to_string())
    }

    /// `sf-string`: `"` with `\`-escapes for `"` and `\` only.
    fn parse_string(&mut self) -> Result<String, String> {
        self.expect_byte(b'"', "`\"`")?;
        let mut out = String::new();
        loop {
            match self.peek() {
                None => return Err("malformed Signature-Input: unterminated string".to_string()),
                Some(b'"') => {
                    self.bump();
                    return Ok(out);
                }
                Some(b'\\') => {
                    self.bump();
                    match self.peek() {
                        Some(b'"') => out.push('"'),
                        Some(b'\\') => out.push('\\'),
                        _ => {
                            return Err(
                                "malformed Signature-Input: bad string escape".to_string()
                            );
                        }
                    }
                    self.bump();
                }
                Some(c) => {
                    if c < 0x20 || c == 0x7f {
                        return Err(
                            "malformed Signature-Input: control character in string".to_string()
                        );
                    }
                    out.push(c as char);
                    self.bump();
                }
            }
        }
    }

    /// `sf-binary`: `:base64:` (strict alphabet, no whitespace inside).
    fn parse_binary(&mut self) -> Result<Vec<u8>, String> {
        self.expect_byte(b':', "`:`")?;
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c == b':' {
                break;
            }
            if !matches!(c, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'+' | b'/' | b'=') {
                return Err("malformed Signature-Input: bad byte-sequence character".to_string());
            }
            self.bump();
        }
        self.expect_byte(b':', "closing `:`")?;
        let raw = std::str::from_utf8(&self.bytes[start..self.pos - 1])
            .map_err(|_| "malformed Signature-Input: non-UTF8 byte sequence".to_string())?;
        base64_decode(raw).map_err(|e| format!("malformed Signature-Input: {e}"))
    }

    /// Integer or decimal number; decimals are outside the supported subset.
    fn parse_number(&mut self) -> Result<SigParamValue, String> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.bump();
        }
        let digits_start = self.pos;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.bump();
        }
        if self.pos == digits_start {
            return Err("malformed Signature-Input: expected number".to_string());
        }
        if self.peek() == Some(b'.') {
            return Err("malformed Signature-Input: decimals are not supported".to_string());
        }
        std::str::from_utf8(&self.bytes[start..self.pos])
            .map_err(|_| "malformed Signature-Input: non-UTF8 number".to_string())?
            .parse::<i64>()
            .map(SigParamValue::Int)
            .map_err(|_| "malformed Signature-Input: integer out of range".to_string())
    }

    /// Token: `ALPHA / "*"` followed by `tchar / ":" / "/"` characters.
    fn parse_token(&mut self) -> Result<String, String> {
        let start = self.pos;
        match self.peek() {
            Some(b'A'..=b'Z' | b'a'..=b'z' | b'*') => self.bump(),
            _ => return Err("malformed Signature-Input: expected token".to_string()),
        }
        while let Some(c) = self.peek() {
            if matches!(c, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b':' | b'/')
                || matches!(
                    c,
                    b'!' | b'#' | b'$' | b'%' | b'&' | b'\'' | b'*' | b'+' | b'-' | b'.' | b'^'
                        | b'_' | b'`' | b'|' | b'~'
                )
            {
                self.bump();
            } else {
                break;
            }
        }
        String::from_utf8(self.bytes[start..self.pos].to_vec())
            .map_err(|_| "malformed Signature-Input: non-UTF8 token".to_string())
    }

    fn parse_param_value(&mut self) -> Result<SigParamValue, String> {
        match self.peek() {
            Some(b'"') => self.parse_string().map(SigParamValue::Str),
            Some(b':') => self.parse_binary().map(SigParamValue::Bytes),
            Some(b'?') => {
                self.bump();
                match self.peek() {
                    Some(b'1') => {
                        self.bump();
                        Ok(SigParamValue::Bool(true))
                    }
                    Some(b'0') => {
                        self.bump();
                        Ok(SigParamValue::Bool(false))
                    }
                    _ => Err("malformed Signature-Input: bad boolean".to_string()),
                }
            }
            Some(b'0'..=b'9' | b'-') => self.parse_number(),
            Some(b'(') => Err(
                "malformed Signature-Input: inner-list parameter values are not supported"
                    .to_string(),
            ),
            Some(_) => self.parse_token().map(SigParamValue::Token),
            None => Err("malformed Signature-Input: truncated parameter value".to_string()),
        }
    }
}

/// Parse an RFC 9421 `Signature-Input` dictionary value. The FIRST member is
/// returned; extra members are parsed (labels must be unique) and ignored.
/// Duplicate covered components, component parameters, empty covered lists,
/// missing `keyid`, and unsupported `alg` values are all errors.
pub fn parse_signature_input(header_value: &str) -> Result<ParsedRfc9421Signature, String> {
    let mut cursor = SfCursor::new(header_value);
    let mut members: Vec<(String, Vec<String>, Vec<(String, SigParamValue)>)> = Vec::new();
    loop {
        cursor.skip_ows();
        if cursor.eof() {
            break;
        }
        if !members.is_empty() {
            cursor.expect_byte(b',', "`,`")?;
            cursor.skip_ows();
            if cursor.eof() {
                return Err("malformed Signature-Input: trailing comma".to_string());
            }
        }
        let label = cursor.parse_key()?;
        cursor.skip_ows();
        cursor.expect_byte(b'=', "`=`")?;
        cursor.skip_ows();
        cursor.expect_byte(b'(', "`(`")?;
        let mut covered = Vec::new();
        loop {
            cursor.skip_ows();
            if cursor.peek() == Some(b')') {
                cursor.bump();
                break;
            }
            if cursor.eof() {
                return Err("malformed Signature-Input: unterminated covered list".to_string());
            }
            let item = cursor.parse_string()?;
            cursor.skip_ows();
            if cursor.peek() == Some(b';') {
                return Err(format!(
                    "unsupported parameters on covered component {item:?}"
                ));
            }
            covered.push(item);
        }
        let mut params: Vec<(String, SigParamValue)> = Vec::new();
        loop {
            cursor.skip_ows();
            if cursor.peek() != Some(b';') {
                break;
            }
            cursor.bump();
            cursor.skip_ows();
            let key = cursor.parse_key()?;
            if params.iter().any(|(k, _)| k == &key) {
                return Err(format!("duplicate Signature-Input parameter: {key}"));
            }
            cursor.skip_ows();
            let value = if cursor.peek() == Some(b'=') {
                cursor.bump();
                cursor.skip_ows();
                cursor.parse_param_value()?
            } else {
                SigParamValue::Bool(true)
            };
            params.push((key, value));
        }
        members.push((label, covered, params));
    }
    if members.is_empty() {
        return Err("malformed Signature-Input: empty dictionary".to_string());
    }
    let mut seen_labels: Vec<&str> = Vec::with_capacity(members.len());
    for (label, _, _) in &members {
        if seen_labels.contains(&label.as_str()) {
            return Err(format!("duplicate Signature-Input label: {label}"));
        }
        seen_labels.push(label.as_str());
    }
    let (label, covered, params) = &members[0];
    if covered.is_empty() {
        return Err("malformed Signature-Input: no covered components".to_string());
    }
    let mut seen: Vec<&str> = Vec::with_capacity(covered.len());
    for component in covered {
        if seen.contains(&component.as_str()) {
            return Err(format!("duplicate covered component: {component}"));
        }
        seen.push(component.as_str());
    }
    let key_id = params
        .iter()
        .find(|(k, _)| k == "keyid")
        .and_then(|(_, v)| match v {
            SigParamValue::Str(s) if !s.trim().is_empty() => Some(s.clone()),
            _ => None,
        })
        .ok_or("Signature-Input missing keyid".to_string())?;
    let mut algorithm = "rsa-v1_5-sha256".to_string();
    if let Some((_, value)) = params.iter().find(|(k, _)| k == "alg") {
        let name = match value {
            SigParamValue::Str(s) | SigParamValue::Token(s) => s.to_lowercase(),
            _ => return Err("malformed Signature-Input: non-string alg".to_string()),
        };
        if name == "rsa-v1_5-sha256" || name == "rsa-sha256" {
            algorithm = "rsa-v1_5-sha256".to_string();
        } else {
            return Err(format!("unsupported signature algorithm: {name}"));
        }
    }
    let int_param = |name: &str| -> Result<Option<i64>, String> {
        match params.iter().find(|(k, _)| k == name) {
            None => Ok(None),
            Some((_, SigParamValue::Int(i))) => Ok(Some(*i)),
            Some(_) => Err(format!("malformed Signature-Input: non-integer {name}")),
        }
    };
    Ok(ParsedRfc9421Signature {
        label: label.clone(),
        key_id,
        algorithm,
        created: int_param("created")?,
        expires: int_param("expires")?,
        covered: covered.clone(),
        params: params.clone(),
    })
}

/// Parse an RFC 9421 `Signature` dictionary value and return the raw
/// signature bytes for `label`. A leading `Signature ` prefix (as in an
/// `Authorization: Signature ...` value) is tolerated.
pub fn parse_http_signature_value(header_value: &str, label: &str) -> Result<Vec<u8>, String> {
    let mut value = header_value.trim();
    if value.len() >= 9
        && value[..9].eq_ignore_ascii_case("signature")
        && (value.len() == 9 || value.as_bytes()[9].is_ascii_whitespace())
    {
        value = value[9..].trim_start();
    }
    let mut cursor = SfCursor::new(value);
    let mut found: Option<Vec<u8>> = None;
    let mut labels: Vec<String> = Vec::new();
    loop {
        cursor.skip_ows();
        if cursor.eof() {
            break;
        }
        if !labels.is_empty() {
            cursor.expect_byte(b',', "`,`")?;
            cursor.skip_ows();
            if cursor.eof() {
                return Err("malformed Signature: trailing comma".to_string());
            }
        }
        let entry_label = cursor.parse_key()?;
        if labels.contains(&entry_label) {
            return Err(format!("duplicate Signature label: {entry_label}"));
        }
        labels.push(entry_label.clone());
        cursor.skip_ows();
        cursor.expect_byte(b'=', "`=`")?;
        cursor.skip_ows();
        let bytes = cursor.parse_binary()?;
        // Unknown trailing parameters on a Signature entry are not part of
        // the supported profile; the dictionary shape has none.
        cursor.skip_ows();
        if cursor.peek() == Some(b';') {
            return Err("malformed Signature: entry parameters are not supported".to_string());
        }
        if entry_label == label {
            found = Some(bytes);
        }
    }
    found.ok_or_else(|| format!("Signature entry {label:?} not present"))
}

// ---------------------------------------------------------------------------
// Signature base construction + verification
// ---------------------------------------------------------------------------

fn rfc9421_covered_value(
    component: &str,
    method: &str,
    path_and_query: &str,
    scheme: &str,
    authority: &str,
    headers: &HeaderMap,
) -> Result<String, String> {
    match component {
        "@method" => Ok(method.to_uppercase()),
        "@authority" => Ok(authority.to_string()),
        "@scheme" => Ok(scheme.to_lowercase()),
        "@target-uri" => Ok(format!(
            "{}://{}{}",
            scheme.to_lowercase(),
            authority,
            path_and_query
        )),
        "@path" => Ok(path_and_query.to_string()),
        "@query" => Ok(path_and_query
            .split_once('?')
            .map(|(_, query)| format!("?{query}"))
            .unwrap_or_default()),
        "@request-response" | "@status" => Err(format!(
            "covered component {component} is response-only"
        )),
        _ if component.starts_with('@') => Err(format!("unsupported covered component: {component}")),
        _ => {
            let hn = HeaderName::from_str(&component.to_lowercase())
                .map_err(|_| format!("invalid covered header name: {component}"))?;
            let value = headers
                .get(&hn)
                .ok_or_else(|| format!("covered header missing: {component}"))?;
            value
                .to_str()
                .map(|s| s.trim().to_string())
                .map_err(|_| format!("non-visible covered header value: {component}"))
        }
    }
}

/// Build the RFC 9421 §2.5 signature base: one `"name: value"` line per
/// covered component plus the `"@signature-params"` line (canonical
/// serialization preserving received order). Enforces the required covered
/// set: `@method`, `@authority`, and (`@path` or `@target-uri`).
pub fn build_rfc9421_signature_base(
    method: &str,
    path_and_query: &str,
    scheme: &str,
    authority: &str,
    headers: &HeaderMap,
    parsed: &ParsedRfc9421Signature,
) -> Result<String, String> {
    if parsed.covered.is_empty() {
        return Err("RFC 9421 signature covers no components".to_string());
    }
    let mut seen: Vec<String> = Vec::with_capacity(parsed.covered.len());
    for component in &parsed.covered {
        let key = if component.starts_with('@') {
            component.clone()
        } else {
            component.to_lowercase()
        };
        if seen.contains(&key) {
            return Err(format!("duplicate covered component: {component}"));
        }
        seen.push(key);
    }
    let has = |name: &str| seen.iter().any(|s| s == name);
    for required in ["@method", "@authority"] {
        if !has(required) {
            return Err(format!("RFC 9421 signature missing required {required}"));
        }
    }
    if !has("@path") && !has("@target-uri") {
        return Err("RFC 9421 signature must cover @path or @target-uri".to_string());
    }
    let mut lines = Vec::with_capacity(parsed.covered.len() + 1);
    for component in &parsed.covered {
        let value = rfc9421_covered_value(
            component,
            method,
            path_and_query,
            scheme,
            authority,
            headers,
        )?;
        lines.push(format!("{component}: {value}"));
    }
    let mut inner = String::from("(");
    for (i, component) in parsed.covered.iter().enumerate() {
        if i > 0 {
            inner.push(' ');
        }
        inner.push_str(&format!("\"{}\"", component.replace('\\', "\\\\").replace('"', "\\\"")));
    }
    inner.push(')');
    for (key, value) in &parsed.params {
        inner.push(';');
        inner.push_str(key);
        if *value != SigParamValue::Bool(true) {
            inner.push('=');
            inner.push_str(&param_value_to_sf(value));
        }
    }
    lines.push(format!("\"@signature-params\": {inner}"));
    Ok(lines.join("\n"))
}

/// Find the RFC 9421 signature bytes for `label`, looking in the
/// `Signature` header first and then `Authorization` (dual-sign senders may
/// carry the legacy value in either slot).
fn find_rfc9421_signature(headers: &HeaderMap, label: &str) -> Result<Vec<u8>, String> {
    let mut detail = "missing Signature header".to_string();
    for name in ["signature", "authorization"] {
        if let Some(raw) = headers.get(name).and_then(|v| v.to_str().ok()) {
            match parse_http_signature_value(raw, label) {
                Ok(bytes) => return Ok(bytes),
                Err(err) => detail = err,
            }
        }
    }
    Err(detail)
}

/// Verify an incoming RFC 9421 signed request under the supported profile
/// (see module docs). Returns the parsed `Signature-Input` entry (including
/// the `keyid` the caller resolves) on success.
pub fn verify_rfc9421_signature(
    method: &str,
    path_and_query: &str,
    scheme: &str,
    headers: &HeaderMap,
    body: Option<&[u8]>,
    public_key_pem: &str,
    now_ms: Option<i64>,
    max_clock_skew_secs: i64,
) -> Result<ParsedRfc9421Signature, String> {
    let input_raw = headers
        .get("signature-input")
        .and_then(|v| v.to_str().ok())
        .ok_or("missing Signature-Input header")?;
    let parsed = parse_signature_input(input_raw)?;
    let created = parsed
        .created
        .ok_or("Signature-Input missing created".to_string())?;
    if let Some(now) = now_ms {
        let now_secs = now.div_euclid(1000);
        if (now_secs - created).abs() > max_clock_skew_secs {
            return Err("Signature-Input created outside allowed clock skew".to_string());
        }
        if let Some(expires) = parsed.expires
            && now_secs > expires + max_clock_skew_secs
        {
            return Err("RFC 9421 signature expired".to_string());
        }
    }
    let authority = headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .ok_or("missing Host header")?;
    let authority = authority.trim().to_lowercase();
    if authority.is_empty() {
        return Err("missing Host header".to_string());
    }
    if let Some(body) = body {
        let names: Vec<String> = parsed.covered.iter().map(|c| c.to_lowercase()).collect();
        let wants_content_digest = names.iter().any(|n| n == "content-digest");
        let wants_digest = names.iter().any(|n| n == "digest");
        if !wants_content_digest && !wants_digest {
            return Err(
                "RFC 9421 signature covers no digest with a body present".to_string(),
            );
        }
        if wants_content_digest {
            let value = header_value(headers, "content-digest")?;
            if !verify_content_digest_header(&value, body) {
                return Err("Content-Digest header does not match body".to_string());
            }
        }
        if wants_digest {
            let value = header_value(headers, "digest")?;
            if !verify_digest_header(&value, body) {
                return Err("Digest header does not match body".to_string());
            }
        }
    }
    let base =
        build_rfc9421_signature_base(method, path_and_query, scheme, &authority, headers, &parsed)?;
    let signature = find_rfc9421_signature(headers, &parsed.label)?;
    if !rsa_sha256_verify(public_key_pem, base.as_bytes(), &signature)? {
        return Err("RFC 9421 signature verification failed".to_string());
    }
    Ok(parsed)
}

// ---------------------------------------------------------------------------
// RFC 9421 request signer (tests + future outbound use; mirrors
// `sign_request_headers` for the legacy scheme)
// ---------------------------------------------------------------------------

/// Headers produced by [`sign_rfc9421_headers`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rfc9421SignedHeaders {
    /// Full value for the `Content-Digest` header (`None` for bodyless requests).
    pub content_digest: Option<String>,
    /// Full value for the `Signature-Input` header.
    pub signature_input: String,
    /// Full value for the `Signature` header.
    pub signature: String,
}

/// Sign an outgoing request under this crate's RFC 9421 profile: covers
/// `@method`, `@authority`, `@path` (plus `content-digest` when `body` is
/// present) with `created`, `keyid`, `alg="rsa-v1_5-sha256"` parameters.
pub fn sign_rfc9421_headers(
    method: &str,
    path_and_query: &str,
    scheme: &str,
    authority: &str,
    body: Option<&[u8]>,
    key_id: &str,
    private_key_pem: &str,
    created_secs: i64,
) -> Result<Rfc9421SignedHeaders, String> {
    let authority = authority.trim().to_lowercase();
    if authority.is_empty() {
        return Err("RFC 9421 signing needs a non-empty authority".to_string());
    }
    let mut covered = vec!["@method", "@authority", "@path"];
    let content_digest = body.map(compute_content_digest_header);
    if body.is_some() {
        covered.push("content-digest");
    }
    let mut lines = Vec::with_capacity(covered.len() + 1);
    for component in &covered {
        let value = match *component {
            "@method" => method.to_uppercase(),
            "@authority" => authority.clone(),
            "@path" => path_and_query.to_string(),
            _ => content_digest.clone().unwrap_or_default(),
        };
        lines.push(format!("{component}: {value}"));
    }
    let mut inner = String::from("(");
    for (i, component) in covered.iter().enumerate() {
        if i > 0 {
            inner.push(' ');
        }
        inner.push_str(&format!("\"{component}\""));
    }
    let escaped_key = key_id.replace('\\', "\\\\").replace('"', "\\\"");
    inner.push_str(&format!(
        ");created={created_secs};keyid=\"{escaped_key}\";alg=\"rsa-v1_5-sha256\""
    ));
    lines.push(format!("\"@signature-params\": {inner}"));
    let base = lines.join("\n");
    let signature = format!("sig1=:{}:", rsa_sha256_sign_base64(private_key_pem, base.as_bytes())?);
    Ok(Rfc9421SignedHeaders {
        content_digest,
        signature_input: format!("sig1={inner}"),
        signature,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::pkcs1::EncodeRsaPublicKey;

    /// ring refuses < 2048-bit keys, so signing tests share one cached
    /// 2048-bit keypair instead of generating a key per test.
    fn test_keypair() -> (RsaPrivateKey, RsaPublicKey) {
        static KEY: std::sync::OnceLock<(RsaPrivateKey, RsaPublicKey)> =
            std::sync::OnceLock::new();
        KEY.get_or_init(|| generate_rsa_keypair_2048().unwrap()).clone()
    }

    /// Crate-backed equivalent of the old textbook-fixture DER roundtrip:
    /// every PEM encoding parses back to the same key, and mismatched labels
    /// are rejected.
    #[test]
    fn key_pem_roundtrip_all_encodings() {
        let (priv_key, pub_key) = test_keypair();
        // PKCS#8 private roundtrip.
        let pkcs8_pem = private_key_to_pkcs8_pem(&priv_key);
        assert!(pkcs8_pem.starts_with("-----BEGIN PRIVATE KEY-----\n"));
        let priv_back = parse_private_key_pem(&pkcs8_pem).unwrap();
        assert_eq!(private_key_to_pkcs8_pem(&priv_back), pkcs8_pem);
        // PKCS#1 private parses to the same key.
        let pkcs1_pem = private_key_to_pkcs1_pem(&priv_key);
        assert!(pkcs1_pem.starts_with("-----BEGIN RSA PRIVATE KEY-----\n"));
        let priv_from_pkcs1 = parse_private_key_pem(&pkcs1_pem).unwrap();
        assert_eq!(private_key_to_pkcs8_pem(&priv_from_pkcs1), pkcs8_pem);
        // SPKI public roundtrip.
        let spki_pem = public_key_to_spki_pem(&pub_key);
        assert!(spki_pem.starts_with("-----BEGIN PUBLIC KEY-----\n"));
        let pub_back = parse_public_key_pem(&spki_pem).unwrap();
        assert_eq!(public_key_to_spki_pem(&pub_back), spki_pem);
        // Raw PKCS#1 public label parses to the same key.
        let raw_pub_pem = pub_back
            .to_pkcs1_pem(LineEnding::LF)
            .expect("PKCS#1 public PEM encoding");
        assert!(raw_pub_pem.starts_with("-----BEGIN RSA PUBLIC KEY-----\n"));
        let pub_from_pkcs1 = parse_public_key_pem(&raw_pub_pem).unwrap();
        assert_eq!(public_key_to_spki_pem(&pub_from_pkcs1), spki_pem);
        // PEM wrappers reject the wrong key kind.
        assert!(parse_public_key_pem(&pkcs8_pem).is_err());
        assert!(parse_private_key_pem(&spki_pem).is_err());
    }

    /// Regression coverage for the old hand-rolled CRT signing path (which
    /// underflowed for keys with `q > p` whenever `m2 > m1 + p`, failing a
    /// few percent of signatures). CRT now lives inside the `rsa` crate;
    /// this test keeps the coverage: fresh keys each sign/verify 200
    /// messages without failure.
    #[test]
    fn sign_verify_with_q_larger_than_p() {
        for _ in 0..1 {
            let (priv_key, pub_key) = test_keypair();
            let priv_pem = private_key_to_pkcs8_pem(&priv_key);
            let pub_pem = public_key_to_spki_pem(&pub_key);
            for i in 0..200 {
                let msg = format!("message {i}");
                let sig =
                    rsa_sha256_sign(&priv_pem, msg.as_bytes()).expect("signing must not fail");
                assert!(rsa_sha256_verify(&pub_pem, msg.as_bytes(), &sig).unwrap());
            }
        }
    }

    #[test]
    fn keygen_1024_pem_roundtrip_and_sign_verify() {
        let (priv_key, pub_key) = test_keypair();
        let priv_pem = private_key_to_pkcs8_pem(&priv_key);
        let pub_pem = public_key_to_spki_pem(&pub_key);
        let priv_back = parse_private_key_pem(&priv_pem).unwrap();
        let pub_back = parse_public_key_pem(&pub_pem).unwrap();
        // `rsa` key types carry no `PartialEq`; compare canonical PEMs.
        assert_eq!(private_key_to_pkcs8_pem(&priv_back), priv_pem);
        assert_eq!(public_key_to_spki_pem(&pub_back), pub_pem);
        let msg = b"hello federation";
        let sig = rsa_sha256_sign(&priv_pem, msg).unwrap();
        assert_eq!(sig.len(), 256);
        assert!(rsa_sha256_verify(&pub_pem, msg, &sig).unwrap());
        // Tampered message / signature / wrong key all fail.
        assert!(!rsa_sha256_verify(&pub_pem, b"hello federatioN", &sig).unwrap());
        let mut bad_sig = sig.clone();
        bad_sig[10] ^= 0x01;
        assert!(!rsa_sha256_verify(&pub_pem, msg, &bad_sig).unwrap());
        assert!(!rsa_sha256_verify(&pub_pem, msg, &sig[..64]).unwrap());
        let (_, other_pub) = generate_rsa_keypair(1024).unwrap();
        let other_pem = public_key_to_spki_pem(&other_pub);
        assert!(!rsa_sha256_verify(&other_pem, msg, &sig).unwrap());
        // Base64 helper agrees.
        assert_eq!(rsa_sha256_sign_base64(&priv_pem, msg).unwrap(), base64_encode(&sig));
    }

    #[test]
    fn base64_known_answers() {
        assert_eq!(base64_encode(b"hello"), "aGVsbG8=");
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_decode("aGVsbG8=").unwrap(), b"hello");
        assert_eq!(base64_decode("aGVs\nbG8=").unwrap(), b"hello");
        assert_eq!(base64_decode("").unwrap(), b"");
        assert!(base64_decode("aGVsbG8").is_err());
        assert!(base64_decode("aGVsbG8!").is_err());
        // Roundtrip incl. all byte values.
        let all: Vec<u8> = (0..=255).collect();
        assert_eq!(base64_decode(&base64_encode(&all)).unwrap(), all);
    }

    #[test]
    fn digest_header_roundtrip() {
        let body = br#"{"type":"Create"}"#;
        let header = compute_digest_header(body);
        assert!(header.starts_with("SHA-256="));
        assert!(verify_digest_header(&header, body));
        assert!(!verify_digest_header(&header, b"tampered"));
        // Case-insensitive algorithm + extra digests tolerated.
        assert!(verify_digest_header(
            &format!("md5=AAAA, {} ", header.replace("SHA-256", "sha-256")),
            body
        ));
        assert!(!verify_digest_header("SHA-256=AAAA", body));
        assert!(!verify_digest_header("garbage", body));
    }

    fn test_headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (k, v) in pairs {
            map.insert(
                HeaderName::from_str(k).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        map
    }

    fn date_fixture() -> (&'static str, i64) {
        // 2026-10-14 is a Wednesday; toUTCString form (IMF-fixdate).
        let s = "Wed, 14 Oct 2026 12:00:00 GMT";
        (s, parse_http_date(s).unwrap())
    }

    #[test]
    fn http_get_sign_and_verify() {
        let (priv_key, pub_key) = test_keypair();
        let priv_pem = private_key_to_pkcs8_pem(&priv_key);
        let pub_pem = public_key_to_spki_pem(&pub_key);
        let (date, now) = date_fixture();
        let key_id = "https://example.local/users/123#main-key";
        let signed = sign_request_headers(
            "GET",
            "/users/123",
            "example.local",
            date,
            None,
            key_id,
            &priv_pem,
        )
        .unwrap();
        assert!(signed.digest.is_none());
        assert!(signed.signature.contains("headers=\"(request-target) date host\""));
        let headers = test_headers(&[
            ("date", date),
            ("host", "example.local"),
            ("signature", &signed.signature),
        ]);
        let parsed = verify_http_signature(
            "get",
            "/users/123",
            &headers,
            None,
            &pub_pem,
            Some(now + 60_000),
            300,
        )
        .unwrap();
        assert_eq!(parsed.key_id, key_id);
        assert_eq!(actor_uri_from_key_id(&parsed.key_id), "https://example.local/users/123");
        // Tampered path fails.
        assert!(verify_http_signature(
            "get",
            "/users/124",
            &headers,
            None,
            &pub_pem,
            Some(now),
            300
        )
        .is_err());
        // Tampered host header value fails.
        let bad = test_headers(&[
            ("date", date),
            ("host", "evil.example"),
            ("signature", &signed.signature),
        ]);
        assert!(verify_http_signature("get", "/users/123", &bad, None, &pub_pem, Some(now), 300).is_err());
        // Wrong public key fails.
        let (_, other) = generate_rsa_keypair(1024).unwrap();
        let other_pem = public_key_to_spki_pem(&other);
        assert!(verify_http_signature(
            "get",
            "/users/123",
            &headers,
            None,
            &other_pem,
            Some(now),
            300
        )
        .is_err());
        // Expired Date fails; missing skew check passes.
        assert!(verify_http_signature(
            "get",
            "/users/123",
            &headers,
            None,
            &pub_pem,
            Some(now + 3600_000),
            300
        )
        .is_err());
        assert!(verify_http_signature("get", "/users/123", &headers, None, &pub_pem, None, 300).is_ok());
    }

    #[test]
    fn http_post_sign_and_verify() {
        let (priv_key, pub_key) = test_keypair();
        let priv_pem = private_key_to_pkcs8_pem(&priv_key);
        let pub_pem = public_key_to_spki_pem(&pub_key);
        let (date, now) = date_fixture();
        let body = br#"{"type":"Follow"}"#;
        let key_id = "https://example.local/users/123/publickey";
        let signed = sign_request_headers(
            "POST",
            "/users/456/inbox",
            "example.local",
            date,
            Some(body),
            key_id,
            &priv_pem,
        )
        .unwrap();
        let digest = signed.digest.clone().unwrap();
        assert!(signed.signature.contains("headers=\"(request-target) date host digest\""));
        assert!(verify_digest_header(&digest, body));
        let headers = test_headers(&[
            ("date", date),
            ("host", "example.local"),
            ("digest", &digest),
            ("signature", &signed.signature),
        ]);
        let parsed = verify_http_signature(
            "POST",
            "/users/456/inbox",
            &headers,
            Some(body),
            &pub_pem,
            Some(now),
            300,
        )
        .unwrap();
        assert_eq!(actor_uri_from_key_id(&parsed.key_id), "https://example.local/users/123");
        // Tampered body: Digest mismatch fails.
        let tampered = br#"{"type":"Follow","tampered":true}"#;
        assert!(verify_http_signature(
            "POST",
            "/users/456/inbox",
            &headers,
            Some(tampered),
            &pub_pem,
            Some(now),
            300
        )
        .is_err());
        // GET-style signature (no digest covered) rejected for POST with body.
        let get_signed = sign_request_headers(
            "POST",
            "/users/456/inbox",
            "example.local",
            date,
            None,
            key_id,
            &priv_pem,
        )
        .unwrap();
        let get_headers = test_headers(&[
            ("date", date),
            ("host", "example.local"),
            ("signature", &get_signed.signature),
        ]);
        assert!(verify_http_signature(
            "POST",
            "/users/456/inbox",
            &get_headers,
            Some(body),
            &pub_pem,
            Some(now),
            300
        )
        .is_err());
    }

    /// Birchtree (activitypub_federation) signs a `(created)` pseudo-header, with the
    /// `created` Signature parameter as its value. Verification must accept it.
    #[test]
    fn http_post_created_pseudo_header_verifies() {
        let (priv_key, pub_key) = test_keypair();
        let priv_pem = private_key_to_pkcs8_pem(&priv_key);
        let pub_pem = public_key_to_spki_pem(&pub_key);
        let (date, now) = date_fixture();
        let body = br#"{"type":"Follow"}"#;
        let digest = compute_digest_header(body);
        let created = (now / 1000).to_string();
        let names: Vec<String> = ["(request-target)", "(created)", "host", "date", "digest"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let signed_headers = test_headers(&[("date", date), ("host", "example.local"), ("digest", &digest)]);
        let signing = build_signing_string(
            "POST",
            "/users/456/inbox",
            &signed_headers,
            &names,
            Some(&created),
            None,
        )
        .unwrap();
        assert!(signing.contains(&format!("(created): {created}")));
        let sig = rsa_sha256_sign_base64(&priv_pem, signing.as_bytes()).unwrap();
        let sig_header = format!(
            "keyId=\"https://example.local/users/123#main-key\",algorithm=\"hs2019\",created={created},headers=\"(request-target) (created) host date digest\",signature=\"{sig}\""
        );
        let headers = test_headers(&[
            ("date", date),
            ("host", "example.local"),
            ("digest", &digest),
            ("signature", &sig_header),
        ]);
        let parsed = verify_http_signature(
            "POST",
            "/users/456/inbox",
            &headers,
            Some(body),
            &pub_pem,
            Some(now),
            300,
        )
        .unwrap();
        assert_eq!(parsed.created.as_deref(), Some(created.as_str()));
        // A (created) header without a created parameter cannot be built.
        assert!(build_signing_string("POST", "/x", &signed_headers, &names, None, None).is_err());
    }

    #[test]
    fn signature_header_parsing_variants() {
        let (priv_key, _) = test_keypair();
        let priv_pem = private_key_to_pkcs8_pem(&priv_key);
        let signed = sign_request_headers(
            "GET",
            "/x",
            "example.local",
            "Wed, 14 Oct 2026 12:00:00 GMT",
            None,
            "https://example.local/users/1#main-key",
            &priv_pem,
        )
        .unwrap();
        // Raw Signature value.
        let p = parse_signature_header(&signed.signature).unwrap();
        assert_eq!(p.algorithm, "rsa-sha256");
        assert_eq!(p.headers, vec!["(request-target)", "date", "host"]);
        // Authorization: Signature ... prefix.
        let p2 = parse_signature_header(&format!("Signature {}", signed.signature)).unwrap();
        assert_eq!(p2, p);
        // hs2019 accepted, unknown rejected.
        assert!(parse_signature_header("keyId=\"a\",algorithm=\"hs2019\",signature=\"AAA=\"").is_ok());
        assert!(parse_signature_header("keyId=\"a\",algorithm=\"hmac-sha256\",signature=\"AAA=\"").is_err());
        // headers omitted defaults to date.
        let p3 = parse_signature_header("keyId=\"a\",signature=\"AAA=\"").unwrap();
        assert_eq!(p3.headers, vec!["date"]);
        assert!(parse_signature_header("algorithm=\"rsa-sha256\",signature=\"AAA=\"").is_err());
        // actor URIs.
        assert_eq!(
            actor_uri_from_key_id("https://example.local/users/123#main-key"),
            "https://example.local/users/123"
        );
        assert_eq!(
            actor_uri_from_key_id("https://example.local/users/123/publickey"),
            "https://example.local/users/123"
        );
    }

    #[test]
    fn verify_rejects_missing_required_signed_headers() {
        let (priv_key, pub_key) = test_keypair();
        let priv_pem = private_key_to_pkcs8_pem(&priv_key);
        let pub_pem = public_key_to_spki_pem(&pub_key);
        let (date, now) = date_fixture();
        // Craft a signature covering only (request-target) + date.
        let signing_string = format!("(request-target): get /users/123\ndate: {date}");
        let sig = rsa_sha256_sign_base64(&priv_pem, signing_string.as_bytes()).unwrap();
        let value = format!(
            "keyId=\"https://example.local/users/1#main-key\",algorithm=\"rsa-sha256\",headers=\"(request-target) date\",signature=\"{sig}\""
        );
        let headers = test_headers(&[
            ("date", date),
            ("host", "example.local"),
            ("signature", &value),
        ]);
        assert!(verify_http_signature("get", "/users/123", &headers, None, &pub_pem, Some(now), 300).is_err());
        // Unparseable Date fails (all required headers signed, so the
        // failure comes from date parsing, not the header gate).
        let full = sign_request_headers(
            "GET",
            "/users/123",
            "example.local",
            date,
            None,
            "https://example.local/users/1#main-key",
            &priv_pem,
        )
        .unwrap();
        let bad_date = test_headers(&[
            ("date", "not a date"),
            ("host", "example.local"),
            ("signature", &full.signature),
        ]);
        assert!(verify_http_signature("get", "/users/123", &bad_date, None, &pub_pem, Some(now), 300).is_err());
    }

    #[test]
    fn node_crypto_rsa2048_fixture_interop() {
        let node_pub_pem = "-----BEGIN PUBLIC KEY-----\n\
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtB8BOMPCFjO+KBuZFVM+\n\
UlZ98+5yiaTRFJYTMzNHtP+J1jphiTe6VRLLTY92mVM+p8ziIz7hklh7ta/Y+7Hr\n\
EfOksEZG+7dvSopGJf9YBXmCeYNGOjAEjYLhx+I88CQsw3Xvu/nnzXJDOQe07hWm\n\
vF7E1AyimnOVi3bjQNrqyCQ35U9hDJZFUAfc8oHeYjqdu7N/jy0VYt+U+vCQod8a\n\
z+Fufip7bv/hSnVBLOUb61zd44ksDf1rpiA3Q617oYh5/18U5jycaTbxeWU/iNfM\n\
HVN0N9bZ8OpLCRlVWl4RN5uYhxbjrovKswWWlHX8kC/7nkp3NXHo6VY5FEVtocVH\n\
eQIDAQAB\n\
-----END PUBLIC KEY-----\n";
        let node_priv_pem = "-----BEGIN PRIVATE KEY-----\n\
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC0HwE4w8IWM74o\n\
G5kVUz5SVn3z7nKJpNEUlhMzM0e0/4nWOmGJN7pVEstNj3aZUz6nzOIjPuGSWHu1\n\
r9j7sesR86SwRkb7t29KikYl/1gFeYJ5g0Y6MASNguHH4jzwJCzDde+7+efNckM5\n\
B7TuFaa8XsTUDKKac5WLduNA2urIJDflT2EMlkVQB9zygd5iOp27s3+PLRVi35T6\n\
8JCh3xrP4W5+Kntu/+FKdUEs5RvrXN3jiSwN/WumIDdDrXuhiHn/XxTmPJxpNvF5\n\
ZT+I18wdU3Q31tnw6ksJGVVaXhE3m5iHFuOui8qzBZaUdfyQL/ueSnc1cejpVjkU\n\
RW2hxUd5AgMBAAECggEASXhMG0deJ4g7jEiVjKlDxnoZLiKY/aQI1IjSlBPar+17\n\
37I4jqv4iTL9UkmuaqQV7oQXyaCgif0lmNp9e0z7QXtMNcq0sYAFGTaFnoIk+Taz\n\
DbWKM7naYY59YfbaPGnFCIVWoCozbw2odd664lJajINrbcMpoC59VUHNOGsTUUcl\n\
Zu6XJb41vJR984iBWVl3H4IBQ0fq8omwSdDfnVesTUFAI5+UWbVsFSkR2GxeafeW\n\
wCcxb/HZGmHdZA0r+wcE5DHlvKWEuNyTLeaL+5Ls4i4tF1u5M4VObR6p8pOXCuDr\n\
bhlrMC29ppcytPA6gp3rfYjQB3KGSCez85QOZEoiMQKBgQDfFkkAYRDJXOFmiKMI\n\
5Es9iEKNDRtnJGZHsY7EUaY0KjwARnf0B107GOcpDP/rAvDcE1AgrH8WWp2U/F7c\n\
Tn0ivFoC71mcgqDW8CpxWm6gFfJxIDcQHab4pCePZTSokCGvQHkQ8SCF9CiYPF8e\n\
N36OYIfCcqzw10S61wdoneWpywKBgQDOsfNbnEM4MqP3slJ4xfZEMJiCmn9A0XLN\n\
FKlg64WwZYuPf0tVGLUsFk7v2bxKjPdIB/CN0k/bmBgbTsCVNaxN2JQgCWVN1Lkn\n\
RK74TDk1U54AoEEg5wFDboKxwila4O+QzEv9Kvc4wKfP6m3UKwcsjCHOJ8IeKNdr\n\
g6/sV7h7SwKBgQDMcmLyuwIkWD+ltSEc/APMX30GQXXUn7IwkBPT7TQRaIdWhT5X\n\
YQxbJaZro6NCII7WUcjEOPG7bc4vBN52rmPWGqGvQOVyHvECKlXrTkJF6fcnH6v2\n\
1AX6po4ux0RsGrMUJlUNZr40pR8y7VkKmzcbinHAjVHa4p31US1T/ijE4wKBgBsK\n\
kOoFUY4Cogz/2WZPj95v9SPxDoDuZfSKpXY2mfDv+Ag4ASUK7HjuGjmq5MVkF93m\n\
nAQR5by280BvVyauNPKiSQ6/6+74y5hllfaKEln1rJAsZxjk3/dwd8zFuB9Zmj35\n\
XF+sxVc5bTr4UpIVZwTe+HU+IllLwpMgUV28RDhtAoGBAKGCggYfQ1Q54vmljH1c\n\
me9QQFofDzsoq8Gj1xuhDFFNmHqW1z0DQkkf1gdKgx9E/uE4ceXTLE9z313cof8v\n\
mzqBLgLPPqB2yu9VRXgr43i6mJ3jiW6NK+tPIj7U7GNdGRd8LAmjP0W/8S6CFoYk\n\
/Xx9/OtP1rd1WHkDZbdd3gi9\n\
-----END PRIVATE KEY-----\n";
        let msg = b"(request-target): post /inbox\ndate: Wed, 14 Oct 2026 12:00:00 GMT\nhost: example.local\ndigest: SHA-256=47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=";
        let expected_sig = "HsFrxxUwBWUhCpsXx9sn2QIFZin4n1iGmxhpTX/VBfjg9wZaykhLUf4W2Y1oAGGiGPiri4AqpZ+EDqg5AobkK5ygwrbuqViCr/+7tK6RC02FAJKV+5OfQpq4OGAK9WoFm6HJ6pylZZuTHXi84fVv/u1mvQIXLGWSJ1wZL9cEE+LXlqCB4uk5ZKDZQpUfcy3djn26gTXzQRaeYSzxhyNzOmv7v6eXAu5X+Tte3Vbtj3le1UUTDhbT2MpnCuefFfRXra5tjVOMi0UtBsa97HaxgDgAmPCx6vjVtVJPgXjaQVV2bTsfXKwFVKyKLyusPTkxvTTuMaTX0lixrg8xhb09XA==";

        let our_sig = rsa_sha256_sign_base64(node_priv_pem, msg).unwrap();
        assert_eq!(our_sig, expected_sig);
        let sig_bytes = base64_decode(expected_sig).unwrap();
        assert!(rsa_sha256_verify(node_pub_pem, msg, &sig_bytes).unwrap());
    }

    /// Slice 12 (D-f(a)): the ring-backed signer produces exactly the bytes
    /// the `rsa` crate's (deterministic) PKCS#1 v1.5 signer would, and
    /// both stored PEM encodings (PKCS#1, PKCS#8) load and sign.
    #[test]
    fn slice12_ring_signature_matches_rsa_crate_and_both_pem_forms_load() {
        use rsa::pkcs1v15::SigningKey;
        use signature::{SignatureEncoding, Signer};
        let (priv_key, pub_key) = test_keypair();
        let msg = b"slice12 fixture message";
        let expected = SigningKey::<Sha256>::new(priv_key.clone())
            .sign(msg)
            .to_bytes()
            .as_ref()
            .to_vec();
        let pub_pem = public_key_to_spki_pem(&pub_key);
        for pem in [
            private_key_to_pkcs8_pem(&priv_key),
            private_key_to_pkcs1_pem(&priv_key),
        ] {
            let sig = rsa_sha256_sign(&pem, msg).expect("ring signing");
            assert_eq!(sig, expected, "PKCS#1 v1.5 is deterministic");
            assert!(rsa_sha256_verify(&pub_pem, msg, &sig).unwrap());
        }
    }

    /// ring rejects undersized keys; the error is explicit, not a panic.
    #[test]
    fn slice12_undersized_key_is_a_clean_error() {
        let (small, _) = generate_rsa_keypair(1024).unwrap();
        let err = rsa_sha256_sign(&private_key_to_pkcs8_pem(&small), b"x").unwrap_err();
        assert!(err.contains("2048"), "{err}");
    }

    // ------------------------------------------------------------------
    // Batch E: RFC 9421 verifier + mode gate additions (existing tests
    // above are untouched).
    // ------------------------------------------------------------------

    /// Second 2048-bit signer without paying RSA keygen in every test: the
    /// static Node-crypto fixture PEMs (see
    /// `node_crypto_rsa2048_fixture_interop`).
    fn second_signer_pems() -> (&'static str, &'static str) {
        let pub_pem = "-----BEGIN PUBLIC KEY-----\n\
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtB8BOMPCFjO+KBuZFVM+\n\
UlZ98+5yiaTRFJYTMzNHtP+J1jphiTe6VRLLTY92mVM+p8ziIz7hklh7ta/Y+7Hr\n\
EfOksEZG+7dvSopGJf9YBXmCeYNGOjAEjYLhx+I88CQsw3Xvu/nnzXJDOQe07hWm\n\
vF7E1AyimnOVi3bjQNrqyCQ35U9hDJZFUAfc8oHeYjqdu7N/jy0VYt+U+vCQod8a\n\
z+Fufip7bv/hSnVBLOUb61zd44ksDf1rpiA3Q617oYh5/18U5jycaTbxeWU/iNfM\n\
HVN0N9bZ8OpLCRlVWl4RN5uYhxbjrovKswWWlHX8kC/7nkp3NXHo6VY5FEVtocVH\n\
eQIDAQAB\n\
-----END PUBLIC KEY-----\n";
        let priv_pem = "-----BEGIN PRIVATE KEY-----\n\
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC0HwE4w8IWM74o\n\
G5kVUz5SVn3z7nKJpNEUlhMzM0e0/4nWOmGJN7pVEstNj3aZUz6nzOIjPuGSWHu1\n\
r9j7sesR86SwRkb7t29KikYl/1gFeYJ5g0Y6MASNguHH4jzwJCzDde+7+efNckM5\n\
B7TuFaa8XsTUDKKac5WLduNA2urIJDflT2EMlkVQB9zygd5iOp27s3+PLRVi35T6\n\
8JCh3xrP4W5+Kntu/+FKdUEs5RvrXN3jiSwN/WumIDdDrXuhiHn/XxTmPJxpNvF5\n\
ZT+I18wdU3Q31tnw6ksJGVVaXhE3m5iHFuOui8qzBZaUdfyQL/ueSnc1cejpVjkU\n\
RW2hxUd5AgMBAAECggEASXhMG0deJ4g7jEiVjKlDxnoZLiKY/aQI1IjSlBPar+17\n\
37I4jqv4iTL9UkmuaqQV7oQXyaCgif0lmNp9e0z7QXtMNcq0sYAFGTaFnoIk+Taz\n\
DbWKM7naYY59YfbaPGnFCIVWoCozbw2odd664lJajINrbcMpoC59VUHNOGsTUUcl\n\
Zu6XJb41vJR984iBWVl3H4IBQ0fq8omwSdDfnVesTUFAI5+UWbVsFSkR2GxeafeW\n\
wCcxb/HZGmHdZA0r+wcE5DHlvKWEuNyTLeaL+5Ls4i4tF1u5M4VObR6p8pOXCuDr\n\
bhlrMC29ppcytPA6gp3rfYjQB3KGSCez85QOZEoiMQKBgQDfFkkAYRDJXOFmiKMI\n\
5Es9iEKNDRtnJGZHsY7EUaY0KjwARnf0B107GOcpDP/rAvDcE1AgrH8WWp2U/F7c\n\
Tn0ivFoC71mcgqDW8CpxWm6gFfJxIDcQHab4pCePZTSokCGvQHkQ8SCF9CiYPF8e\n\
N36OYIfCcqzw10S61wdoneWpywKBgQDOsfNbnEM4MqP3slJ4xfZEMJiCmn9A0XLN\n\
FKlg64WwZYuPf0tVGLUsFk7v2bxKjPdIB/CN0k/bmBgbTsCVNaxN2JQgCWVN1Lkn\n\
RK74TDk1U54AoEEg5wFDboKxwila4O+QzEv9Kvc4wKfP6m3UKwcsjCHOJ8IeKNdr\n\
g6/sV7h7SwKBgQDMcmLyuwIkWD+ltSEc/APMX30GQXXUn7IwkBPT7TQRaIdWhT5X\n\
YQxbJaZro6NCII7WUcjEOPG7bc4vBN52rmPWGqGvQOVyHvECKlXrTkJF6fcnH6v2\n\
1AX6po4ux0RsGrMUJlUNZr40pR8y7VkKmzcbinHAjVHa4p31US1T/ijE4wKBgBsK\n\
kOoFUY4Cogz/2WZPj95v9SPxDoDuZfSKpXY2mfDv+Ag4ASUK7HjuGjmq5MVkF93m\n\
nAQR5by280BvVyauNPKiSQ6/6+74y5hllfaKEln1rJAsZxjk3/dwd8zFuB9Zmj35\n\
XF+sxVc5bTr4UpIVZwTe+HU+IllLwpMgUV28RDhtAoGBAKGCggYfQ1Q54vmljH1c\n\
me9QQFofDzsoq8Gj1xuhDFFNmHqW1z0DQkkf1gdKgx9E/uE4ceXTLE9z313cof8v\n\
mzqBLgLPPqB2yu9VRXgr43i6mJ3jiW6NK+tPIj7U7GNdGRd8LAmjP0W/8S6CFoYk\n\
/Xx9/OtP1rd1WHkDZbdd3gi9\n\
-----END PRIVATE KEY-----\n";
        (priv_pem, pub_pem)
    }

    fn rfc9421_post_fixture() -> (String, String, Vec<u8>, String, i64) {
        let (priv_key, pub_key) = test_keypair();
        let priv_pem = private_key_to_pkcs8_pem(&priv_key);
        let pub_pem = public_key_to_spki_pem(&pub_key);
        let body = br#"{"type":"Follow"}"#.to_vec();
        let key_id = "https://example.local/users/123#main-key".to_string();
        let (_, now_ms) = date_fixture();
        (priv_pem, pub_pem, body, key_id, now_ms)
    }

    fn rfc9421_headers_for(
        signed: &Rfc9421SignedHeaders,
        host: &str,
        date: &str,
    ) -> HeaderMap {
        let mut map = test_headers(&[
            ("date", date),
            ("host", host),
            ("signature-input", &signed.signature_input),
            ("signature", &signed.signature),
        ]);
        if let Some(digest) = signed.content_digest.as_deref() {
            map.insert(
                HeaderName::from_str("content-digest").unwrap(),
                HeaderValue::from_str(digest).unwrap(),
            );
        }
        map
    }

    #[test]
    fn rfc9421_post_sign_and_verify() {
        let (priv_pem, pub_pem, body, key_id, now_ms) = rfc9421_post_fixture();
        let (date, _) = date_fixture();
        let created = now_ms.div_euclid(1000);
        let signed = sign_rfc9421_headers(
            "POST",
            "/users/456/inbox",
            "https",
            "example.local",
            Some(&body),
            &key_id,
            &priv_pem,
            created,
        )
        .unwrap();
        assert!(signed.content_digest.as_deref().unwrap().starts_with("sha-256=:"));
        let headers = rfc9421_headers_for(&signed, "example.local", date);
        let parsed = verify_rfc9421_signature(
            "POST",
            "/users/456/inbox",
            "https",
            &headers,
            Some(&body),
            &pub_pem,
            Some(now_ms),
            300,
        )
        .unwrap();
        assert_eq!(parsed.key_id, key_id);
        assert_eq!(parsed.label, "sig1");
        assert_eq!(parsed.created, Some(created));
        assert_eq!(parsed.algorithm, "rsa-v1_5-sha256");
        // Tampered path / host fail.
        assert!(verify_rfc9421_signature(
            "POST",
            "/users/999/inbox",
            "https",
            &headers,
            Some(&body),
            &pub_pem,
            Some(now_ms),
            300
        )
        .is_err());
        let bad_host = rfc9421_headers_for(&signed, "evil.example", date);
        assert!(verify_rfc9421_signature(
            "POST",
            "/users/456/inbox",
            "https",
            &bad_host,
            Some(&body),
            &pub_pem,
            Some(now_ms),
            300
        )
        .is_err());
        // Wrong key fails.
        let (_, other_pub) = second_signer_pems();
        assert!(verify_rfc9421_signature(
            "POST",
            "/users/456/inbox",
            "https",
            &headers,
            Some(&body),
            other_pub,
            Some(now_ms),
            300
        )
        .is_err());
        // Stale created fails; absent clock check passes.
        assert!(verify_rfc9421_signature(
            "POST",
            "/users/456/inbox",
            "https",
            &headers,
            Some(&body),
            &pub_pem,
            Some(now_ms + 3600_000),
            300
        )
        .is_err());
        assert!(verify_rfc9421_signature(
            "POST",
            "/users/456/inbox",
            "https",
            &headers,
            Some(&body),
            &pub_pem,
            None,
            300
        )
        .is_ok());
    }

    #[test]
    fn rfc9421_get_sign_and_verify() {
        let (priv_pem, pub_pem, _, key_id, now_ms) = rfc9421_post_fixture();
        let (date, _) = date_fixture();
        let created = now_ms.div_euclid(1000);
        let signed = sign_rfc9421_headers(
            "GET",
            "/users/123",
            "https",
            "example.local",
            None,
            &key_id,
            &priv_pem,
            created,
        )
        .unwrap();
        assert!(signed.content_digest.is_none());
        let headers = rfc9421_headers_for(&signed, "example.local", date);
        verify_rfc9421_signature(
            "GET",
            "/users/123",
            "https",
            &headers,
            None,
            &pub_pem,
            Some(now_ms),
            300,
        )
        .unwrap();
        // A body submitted against a bodyless signature fails (no digest covered).
        assert!(verify_rfc9421_signature(
            "GET",
            "/users/123",
            "https",
            &headers,
            Some(b"{}"),
            &pub_pem,
            Some(now_ms),
            300
        )
        .is_err());
    }

    #[test]
    fn rfc9421_tampered_body_and_signature_rejected() {
        let (priv_pem, pub_pem, body, key_id, now_ms) = rfc9421_post_fixture();
        let (date, _) = date_fixture();
        let created = now_ms.div_euclid(1000);
        let signed = sign_rfc9421_headers(
            "POST",
            "/users/456/inbox",
            "https",
            "example.local",
            Some(&body),
            &key_id,
            &priv_pem,
            created,
        )
        .unwrap();
        let headers = rfc9421_headers_for(&signed, "example.local", date);
        // Tampered body: Content-Digest mismatch fails before crypto.
        assert!(verify_rfc9421_signature(
            "POST",
            "/users/456/inbox",
            "https",
            &headers,
            Some(br#"{"type":"Follow","tampered":true}"#),
            &pub_pem,
            Some(now_ms),
            300
        )
        .is_err());
        // Flipped signature bit fails.
        let mut raw = base64_decode(
            signed
                .signature
                .trim_start_matches("sig1=:")
                .trim_end_matches(':'),
        )
        .unwrap();
        raw[10] ^= 0x01;
        let bad_sig = format!("sig1=:{}:", base64_encode(&raw));
        let mut bad = headers.clone();
        bad.insert("signature", bad_sig.parse().unwrap());
        assert!(verify_rfc9421_signature(
            "POST",
            "/users/456/inbox",
            "https",
            &bad,
            Some(&body),
            &pub_pem,
            Some(now_ms),
            300
        )
        .is_err());
    }

    #[test]
    fn rfc9421_key_rotation() {
        // Key A signs; verification with A passes and with B fails. After
        // rotation (re-sign with B), verification with B passes.
        let (priv_a, pub_a) = test_keypair();
        let (priv_pem_b, pub_pem_b) = second_signer_pems();
        let pub_pem_a = public_key_to_spki_pem(&pub_a);
        let priv_pem_a = private_key_to_pkcs8_pem(&priv_a);
        let (_, now_ms) = date_fixture();
        let (date, _) = date_fixture();
        let created = now_ms.div_euclid(1000);
        let body = br#"{"type":"Follow"}"#;
        let key_id = "https://example.local/users/123#main-key";
        let signed_a = sign_rfc9421_headers(
            "POST",
            "/inbox",
            "https",
            "example.local",
            Some(body),
            key_id,
            &priv_pem_a,
            created,
        )
        .unwrap();
        let headers_a = rfc9421_headers_for(&signed_a, "example.local", date);
        assert!(verify_rfc9421_signature(
            "POST",
            "/inbox",
            "https",
            &headers_a,
            Some(body),
            &pub_pem_a,
            Some(now_ms),
            300
        )
        .is_ok());
        assert!(verify_rfc9421_signature(
            "POST",
            "/inbox",
            "https",
            &headers_a,
            Some(body),
            pub_pem_b,
            Some(now_ms),
            300
        )
        .is_err());
        let signed_b = sign_rfc9421_headers(
            "POST",
            "/inbox",
            "https",
            "example.local",
            Some(body),
            key_id,
            priv_pem_b,
            created,
        )
        .unwrap();
        let headers_b = rfc9421_headers_for(&signed_b, "example.local", date);
        assert!(verify_rfc9421_signature(
            "POST",
            "/inbox",
            "https",
            &headers_b,
            Some(body),
            pub_pem_b,
            Some(now_ms),
            300
        )
        .is_ok());
    }

    #[test]
    fn rfc9421_malformed_inputs_rejected() {
        // Signature-Input values that must all fail to parse.
        let bad_inputs = [
            "",
            "not-a-dictionary",
            "sig1=",
            "sig1=()",
            "sig1=();created=1",
            "sig1=(\"@method\" \"@authority\");created=notanumber",
            "sig1=(\"@method\" \"@authority\");created=\"123\"",
            "sig1=(\"@method\" \"@authority\");created=1.5",
            "sig1=(\"@method\" \"@authority\" \"@method\");created=1;keyid=\"k\"",
            "sig1=(\"@method\" \"@authority\";key \"@path\");created=1;keyid=\"k\"",
            "sig1=(\"@method\" \"@authority\");created=1;created=2;keyid=\"k\"",
            "sig1=(\"@method\" \"@authority\");created=1;keyid=\"k\",sig1=(\"@method\");created=1",
            "a=(\"@method\" \"@authority\");created=1;keyid=\"k\",b=(\"@method\" \"@authority\");created=1;keyid=\"k\",b=(\"@method\" \"@authority\");created=1;keyid=\"k\"",
            "sig1=(\"@method\" \"@authority\");created=1;keyid=\"k\";alg=\"hmac-sha256\"",
            "sig1=(\"@method\" \"@authority\");created=1;keyid=\"k\";alg=\"ed25519\"",
            "sig1=(\"@method\" \"@authority\");created=1;keyid=\"k\";alg=\"rsa-pss-sha512\"",
            "sig1=(\"@method\" \"@authority\");created=1;keyid=\"\"",
            "sig1=(\"@method\" \"@authority\");created=1,",
            "SIG1=(\"@method\" \"@authority\");created=1;keyid=\"k\"",
        ];
        for bad in bad_inputs {
            assert!(parse_signature_input(bad).is_err(), "accepted: {bad}");
        }
        // `created` is optional at parse time (verification requires it);
        // exotic-but-well-formed covered sets parse and fail at base build.
        let no_created =
            parse_signature_input("sig1=(\"@method\" \"@authority\" \"@path\");keyid=\"k\"")
                .unwrap();
        assert_eq!(no_created.created, None);
        let sparse =
            parse_signature_input("sig1=(\"@method\");created=1;keyid=\"k\"").unwrap();
        assert_eq!(sparse.covered, vec!["@method".to_string()]);
        let (date, _) = date_fixture();
        let dummy = test_headers(&[("date", date), ("host", "example.local")]);
        assert!(build_rfc9421_signature_base(
            "GET",
            "/x",
            "https",
            "example.local",
            &dummy,
            &sparse
        )
        .is_err());
        for bad_covered in ["@status", "@unknown-component", "@Method"] {
            let parsed = parse_signature_input(&format!(
                "sig1=(\"@method\" \"@authority\" \"@path\" \"{bad_covered}\");created=1;keyid=\"k\""
            ))
            .unwrap();
            assert!(
                build_rfc9421_signature_base(
                    "GET",
                    "/x",
                    "https",
                    "example.local",
                    &dummy,
                    &parsed
                )
                .is_err(),
                "built base for {bad_covered}"
            );
        }
        // Well-formed parses with expected fields.
        let parsed = parse_signature_input(
            "sig1=(\"@method\" \"@authority\" \"@path\" \"content-digest\");created=1700000000;keyid=\"https://x/users/1#k\"",
        )
        .unwrap();
        assert_eq!(parsed.label, "sig1");
        assert_eq!(parsed.created, Some(1_700_000_000));
        assert_eq!(parsed.expires, None);
        // hs2019-style alias and token alg parse; unknown algs do not.
        let aliased = parse_signature_input(
            "sig1=(\"@method\" \"@authority\" \"@path\");created=1;keyid=\"k\";alg=\"rsa-sha256\"",
        )
        .unwrap();
        assert_eq!(aliased.algorithm, "rsa-v1_5-sha256");
        // Signature dictionary values that must all fail.
        assert!(parse_http_signature_value("sig1=:AAA", "sig1").is_err());
        assert!(parse_http_signature_value("sig1=\"AAA=\"", "sig1").is_err());
        assert!(parse_http_signature_value("other=:QUJD:", "sig1").is_err());
        assert!(parse_http_signature_value("sig1=:QUJD:,sig1=:QUJD:", "sig1").is_err());
        assert_eq!(
            parse_http_signature_value("sig1=:QUJD:,other=:REVG:", "sig1").unwrap(),
            b"ABC"
        );
        // Leading `Signature ` prefix tolerated (Authorization shape).
        assert_eq!(
            parse_http_signature_value("Signature sig1=:QUJD:", "sig1").unwrap(),
            b"ABC"
        );
    }

    #[test]
    fn rfc9421_verify_rejects_missing_created_and_required() {
        let (priv_pem, pub_pem, body, key_id, now_ms) = rfc9421_post_fixture();
        let (date, _) = date_fixture();
        let created = now_ms.div_euclid(1000);
        let signed = sign_rfc9421_headers(
            "POST",
            "/inbox",
            "https",
            "example.local",
            Some(&body),
            &key_id,
            &priv_pem,
            created,
        )
        .unwrap();
        // Strip `created`: parse succeeds but verification names the gap.
        let mut headers = rfc9421_headers_for(&signed, "example.local", date);
        let stripped = signed
            .signature_input
            .replacen(&format!(";created={created}"), "", 1);
        assert_ne!(stripped, signed.signature_input);
        headers.insert("signature-input", stripped.parse().unwrap());
        let err = verify_rfc9421_signature(
            "POST",
            "/inbox",
            "https",
            &headers,
            Some(&body),
            &pub_pem,
            Some(now_ms),
            300,
        )
        .unwrap_err();
        assert!(err.contains("created"), "{err}");
        // Drop `@authority` from the covered set (keeping a valid digest so
        // the check reaches base construction): the required set is named.
        let mut headers = rfc9421_headers_for(&signed, "example.local", date);
        let shrunk = signed.signature_input.replacen(
            "(\"@method\" \"@authority\" \"@path\" \"content-digest\")",
            "(\"@method\" \"@path\" \"content-digest\")",
            1,
        );
        assert_ne!(shrunk, signed.signature_input);
        headers.insert("signature-input", shrunk.parse().unwrap());
        let err = verify_rfc9421_signature(
            "POST",
            "/inbox",
            "https",
            &headers,
            Some(&body),
            &pub_pem,
            Some(now_ms),
            300,
        )
        .unwrap_err();
        assert!(err.contains("@authority"), "{err}");
    }

    #[test]
    fn rfc9421_timestamp_edges_rejected() {
        let (priv_pem, pub_pem, body, key_id, now_ms) = rfc9421_post_fixture();
        let (date, _) = date_fixture();
        let now_secs = now_ms.div_euclid(1000);
        // Future created beyond skew fails.
        let future = sign_rfc9421_headers(
            "POST",
            "/inbox",
            "https",
            "example.local",
            Some(&body),
            &key_id,
            &priv_pem,
            now_secs + 3600,
        )
        .unwrap();
        let headers = rfc9421_headers_for(&future, "example.local", date);
        assert!(verify_rfc9421_signature(
            "POST",
            "/inbox",
            "https",
            &headers,
            Some(&body),
            &pub_pem,
            Some(now_ms),
            300
        )
        .is_err());
        // Expired `expires` fails even with a fresh `created`.
        let signed = sign_rfc9421_headers(
            "POST",
            "/inbox",
            "https",
            "example.local",
            Some(&body),
            &key_id,
            &priv_pem,
            now_secs,
        )
        .unwrap();
        let mut headers = rfc9421_headers_for(&signed, "example.local", date);
        let expired_input = signed
            .signature_input
            .replace(";alg=", &format!(";expires={};alg=", now_secs - 3600));
        headers.insert("signature-input", expired_input.parse().unwrap());
        assert!(verify_rfc9421_signature(
            "POST",
            "/inbox",
            "https",
            &headers,
            Some(&body),
            &pub_pem,
            Some(now_ms),
            300
        )
        .is_err());
    }

    #[test]
    fn content_digest_roundtrip() {
        let body = br#"{"type":"Create"}"#;
        let header = compute_content_digest_header(body);
        assert!(header.starts_with("sha-256=:"));
        assert!(verify_content_digest_header(&header, body));
        assert!(!verify_content_digest_header(&header, b"tampered"));
        // sha-512 entries verify; unknown-only and malformed values fail.
        let sha512 = format!("sha-512=:{}:", base64_encode(Sha256::digest(body).as_slice()));
        assert!(!verify_content_digest_header(&sha512, body));
        let real512 = format!("sha-512=:{}:", base64_encode(Sha512::digest(body).as_slice()));
        assert!(verify_content_digest_header(&real512, body));
        assert!(verify_content_digest_header(&format!("md5=:AAAA:, {header}"), body));
        assert!(!verify_content_digest_header("sha-256=:AAAA:", body));
        assert!(!verify_content_digest_header("garbage", body));
        assert!(!verify_content_digest_header("", body));
    }

    #[test]
    fn apply_sig_mode_matrix() {
        use crate::config::Rfc9421Mode;
        let alice = VerifyOutcome::Verified("https://x/users/alice".to_string());
        let bob = VerifyOutcome::Verified("https://x/users/bob".to_string());
        let bad_l = VerifyOutcome::Failed("legacy bad".to_string());
        let bad_r = VerifyOutcome::Failed("rfc9421 bad".to_string());
        // Permissive: either valid signature authenticates.
        assert_eq!(
            apply_sig_mode(Rfc9421Mode::Permissive, Some(alice.clone()), None),
            Ok("https://x/users/alice".to_string())
        );
        assert_eq!(
            apply_sig_mode(Rfc9421Mode::Permissive, None, Some(alice.clone())),
            Ok("https://x/users/alice".to_string())
        );
        assert_eq!(
            apply_sig_mode(Rfc9421Mode::Permissive, Some(alice.clone()), Some(alice.clone())),
            Ok("https://x/users/alice".to_string())
        );
        assert_eq!(
            apply_sig_mode(Rfc9421Mode::Permissive, Some(bad_l.clone()), Some(alice.clone())),
            Ok("https://x/users/alice".to_string())
        );
        assert_eq!(
            apply_sig_mode(Rfc9421Mode::Permissive, Some(alice.clone()), Some(bad_r.clone())),
            Ok("https://x/users/alice".to_string())
        );
        // Permissive: dual-signer disagreement fails closed.
        assert!(apply_sig_mode(Rfc9421Mode::Permissive, Some(alice.clone()), Some(bob.clone())).is_err());
        assert!(apply_sig_mode(Rfc9421Mode::Permissive, Some(bad_l.clone()), Some(bad_r.clone())).is_err());
        assert!(apply_sig_mode(Rfc9421Mode::Permissive, Some(bad_l.clone()), None).is_err());
        assert!(apply_sig_mode(Rfc9421Mode::Permissive, None, None).is_err());
        // Enforce: RFC 9421 required; legacy-only rejected.
        assert_eq!(
            apply_sig_mode(Rfc9421Mode::Enforce, None, Some(alice.clone())),
            Ok("https://x/users/alice".to_string())
        );
        assert_eq!(
            apply_sig_mode(Rfc9421Mode::Enforce, Some(alice.clone()), Some(alice.clone())),
            Ok("https://x/users/alice".to_string())
        );
        assert!(apply_sig_mode(Rfc9421Mode::Enforce, Some(alice.clone()), None).is_err());
        assert!(apply_sig_mode(Rfc9421Mode::Enforce, None, None).is_err());
        assert!(apply_sig_mode(Rfc9421Mode::Enforce, Some(bad_l.clone()), Some(bad_r.clone())).is_err());
        assert!(apply_sig_mode(Rfc9421Mode::Enforce, Some(alice.clone()), Some(bad_r.clone())).is_err());
        assert!(apply_sig_mode(Rfc9421Mode::Enforce, Some(alice.clone()), Some(bob.clone())).is_err());
    }

    #[test]
    fn legacy_presence_and_scheme_helpers() {
        let (priv_key, _) = test_keypair();
        let priv_pem = private_key_to_pkcs8_pem(&priv_key);
        let signed = sign_request_headers(
            "GET",
            "/x",
            "example.local",
            "Wed, 14 Oct 2026 12:00:00 GMT",
            None,
            "https://example.local/users/1#main-key",
            &priv_pem,
        )
        .unwrap();
        assert!(has_legacy_signature(&test_headers(&[("signature", &signed.signature)])));
        assert!(!has_legacy_signature(&test_headers(&[("signature", "sig1=:QUJD:")])));
        assert!(!has_legacy_signature(&HeaderMap::new()));
        assert_eq!(scheme_of_public_url("https://example.local"), "https");
        assert_eq!(scheme_of_public_url("http://127.0.0.1:3000/"), "http");
    }
}
