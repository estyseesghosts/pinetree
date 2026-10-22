//! RFC 9421 HTTP signatures (httpbis): signature-base construction,
//! inbound verification, and the outbound request signer.
//!
//! Supported profile (documented contract; `enforce` requires exactly this):
//!
//! * Transport: `Signature-Input` dictionary with exactly the parsed first
//!   member used for verification (extra members are ignored, never trusted;
//!   duplicate labels are rejected), plus the matching `Signature`
//!   dictionary entry (`sig1=:base64:`). A leading `Signature ` prefix (as in
//!   `Authorization: Signature ...`) is tolerated on the `Signature` value.
//! * Covered components: `@method`, `@authority` and (`@path` or
//!   `@target-uri`) are REQUIRED. `@scheme` and `@query` are supported when
//!   covered. Any other `@`-component is rejected. Any other covered name is
//!   treated as a plain header field (e.g. `date`, `host`, `digest`,
//!   `content-digest`) and must be present. Covered components must not carry
//!   parameters (`;key`, `;bs`, ... are rejected).
//! * Body integrity: when a body is present, `content-digest` or `digest`
//!   must be covered and must verify against the body (`Content-Digest`
//!   supports `sha-256`/`sha-512`; legacy `Digest` reuses
//!   [`crate::activitypub::signatures::digest::verify_digest_header`]).
//! * Algorithms: `rsa-v1_5-sha256` only (`rsa-sha256` accepted as an alias
//!   for the same PKCS#1 v1.5 + SHA-256 operation this crate implements).
//!   Every other `alg` (including `hmac-sha256`, `ed25519`, `ecdsa-*`,
//!   `rsa-pss-sha512`) is rejected. An absent `alg` is assumed to be
//!   `rsa-v1_5-sha256` (documented assumption: this stack only holds RSA
//!   keys, and the key is resolved through the existing keyid-to-owner
//!   lookup before verification).
//! * Freshness: `created` (integer seconds) is REQUIRED and must be within
//!   `max_clock_skew_secs` of `now_ms` when a clock is supplied. `expires`,
//!   when present, must not be past (`now <= expires + skew`). `nonce`, when
//!   present, is parsed and ignored: inbox replays are already covered by the
//!   activity-id dedup gate, and the fetch gate never serves side effects.
//! * Malformed input (bad quoting, duplicate covered components, duplicate
//!   parameter keys, empty covered lists, non-integer timestamps) is
//!   rejected, never skipped.

use std::str::FromStr;

use axum::http::{HeaderMap, HeaderName};

use super::digest::{verify_content_digest_header, verify_digest_header};
use super::keys::rsa_sha256_verify;
use super::legacy::header_value;
use super::rfc9421_parser::{
    ParsedRfc9421Signature, SigParamValue, param_value_to_sf, parse_http_signature_value,
    parse_signature_input,
};

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
        "@request-response" | "@status" => {
            Err(format!("covered component {component} is response-only"))
        }
        _ if component.starts_with('@') => {
            Err(format!("unsupported covered component: {component}"))
        }
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
        inner.push_str(&format!(
            "\"{}\"",
            component.replace('\\', "\\\\").replace('"', "\\\"")
        ));
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
        // Overflow-safe freshness check: `created`/`expires` are
        // attacker-controlled i64s, so `checked_sub` + `checked_abs` +
        // `checked_add` fail closed (Err) instead of panicking on
        // `created = i64::MIN` or wrapping on `expires = i64::MAX`.
        let skew_ok = now_secs
            .checked_sub(created)
            .and_then(|delta| delta.checked_abs())
            .is_some_and(|delta| delta <= max_clock_skew_secs);
        if !skew_ok {
            return Err("Signature-Input created outside allowed clock skew".to_string());
        }
        if let Some(expires) = parsed.expires {
            let not_expired = expires
                .checked_add(max_clock_skew_secs)
                .is_some_and(|limit| now_secs <= limit);
            if !not_expired {
                return Err("RFC 9421 signature expired".to_string());
            }
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
            return Err("RFC 9421 signature covers no digest with a body present".to_string());
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activitypub::signatures::keys::{
        RsaPrivateKey, RsaPublicKey, base64_decode, base64_encode, generate_rsa_keypair_2048,
        private_key_to_pkcs8_pem, public_key_to_spki_pem,
    };
    use crate::activitypub::signatures::legacy::parse_http_date;
    use crate::activitypub::signatures::rfc9421_sign::{
        Rfc9421SignedHeaders, sign_rfc9421_headers,
    };
    use axum::http::{HeaderMap, HeaderName, HeaderValue};

    use std::str::FromStr;

    /// ring refuses < 2048-bit keys, so signing tests share one cached
    /// 2048-bit keypair instead of generating a key per test.
    fn test_keypair() -> (RsaPrivateKey, RsaPublicKey) {
        static KEY: std::sync::OnceLock<(RsaPrivateKey, RsaPublicKey)> = std::sync::OnceLock::new();
        KEY.get_or_init(|| generate_rsa_keypair_2048().unwrap())
            .clone()
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

    fn rfc9421_headers_for(signed: &Rfc9421SignedHeaders, host: &str, date: &str) -> HeaderMap {
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
        assert!(
            signed
                .content_digest
                .as_deref()
                .unwrap()
                .starts_with("sha-256=:")
        );
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
        assert!(
            verify_rfc9421_signature(
                "POST",
                "/users/999/inbox",
                "https",
                &headers,
                Some(&body),
                &pub_pem,
                Some(now_ms),
                300
            )
            .is_err()
        );
        let bad_host = rfc9421_headers_for(&signed, "evil.example", date);
        assert!(
            verify_rfc9421_signature(
                "POST",
                "/users/456/inbox",
                "https",
                &bad_host,
                Some(&body),
                &pub_pem,
                Some(now_ms),
                300
            )
            .is_err()
        );
        // Wrong key fails.
        let (_, other_pub) = second_signer_pems();
        assert!(
            verify_rfc9421_signature(
                "POST",
                "/users/456/inbox",
                "https",
                &headers,
                Some(&body),
                other_pub,
                Some(now_ms),
                300
            )
            .is_err()
        );
        // Stale created fails; absent clock check passes.
        assert!(
            verify_rfc9421_signature(
                "POST",
                "/users/456/inbox",
                "https",
                &headers,
                Some(&body),
                &pub_pem,
                Some(now_ms + 3600_000),
                300
            )
            .is_err()
        );
        assert!(
            verify_rfc9421_signature(
                "POST",
                "/users/456/inbox",
                "https",
                &headers,
                Some(&body),
                &pub_pem,
                None,
                300
            )
            .is_ok()
        );
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
        assert!(
            verify_rfc9421_signature(
                "GET",
                "/users/123",
                "https",
                &headers,
                Some(b"{}"),
                &pub_pem,
                Some(now_ms),
                300
            )
            .is_err()
        );
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
        assert!(
            verify_rfc9421_signature(
                "POST",
                "/users/456/inbox",
                "https",
                &headers,
                Some(br#"{"type":"Follow","tampered":true}"#),
                &pub_pem,
                Some(now_ms),
                300
            )
            .is_err()
        );
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
        assert!(
            verify_rfc9421_signature(
                "POST",
                "/users/456/inbox",
                "https",
                &bad,
                Some(&body),
                &pub_pem,
                Some(now_ms),
                300
            )
            .is_err()
        );
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
        assert!(
            verify_rfc9421_signature(
                "POST",
                "/inbox",
                "https",
                &headers_a,
                Some(body),
                &pub_pem_a,
                Some(now_ms),
                300
            )
            .is_ok()
        );
        assert!(
            verify_rfc9421_signature(
                "POST",
                "/inbox",
                "https",
                &headers_a,
                Some(body),
                pub_pem_b,
                Some(now_ms),
                300
            )
            .is_err()
        );
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
        assert!(
            verify_rfc9421_signature(
                "POST",
                "/inbox",
                "https",
                &headers_b,
                Some(body),
                pub_pem_b,
                Some(now_ms),
                300
            )
            .is_ok()
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
        assert!(
            verify_rfc9421_signature(
                "POST",
                "/inbox",
                "https",
                &headers,
                Some(&body),
                &pub_pem,
                Some(now_ms),
                300
            )
            .is_err()
        );
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
        assert!(
            verify_rfc9421_signature(
                "POST",
                "/inbox",
                "https",
                &headers,
                Some(&body),
                &pub_pem,
                Some(now_ms),
                300
            )
            .is_err()
        );
    }
}
