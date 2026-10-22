//! Legacy HTTP Signatures (draft-cavage-http-signatures): the Misskey /
//! Sharkey `(request-target) date host [digest]` profile — signing,
//! `Signature`-header parsing, signing-string construction, and inbound
//! verification.

use std::str::FromStr;

use axum::http::{HeaderMap, HeaderName};

use super::digest::{compute_digest_header, verify_digest_header};
use super::keys::{base64_decode, rsa_sha256_sign_base64, rsa_sha256_verify};

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
        format!(
            "(request-target): {} {}",
            method.to_lowercase(),
            path_and_query
        ),
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
    if v.len() >= 9
        && v[..9].eq_ignore_ascii_case("signature")
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
pub fn parse_first_legacy_signature(
    headers: &HeaderMap,
) -> Option<Result<ParsedSignature, String>> {
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

pub(crate) fn header_value(headers: &HeaderMap, name: &str) -> Result<String, String> {
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
            let created = created
                .ok_or("signed headers use (created) but the signature has no created parameter")?;
            lines.push(format!("(created): {created}"));
        } else if name.eq_ignore_ascii_case("(expires)") {
            let expires = expires
                .ok_or("signed headers use (expires) but the signature has no expires parameter")?;
            lines.push(format!("(expires): {expires}"));
        } else {
            lines.push(format!(
                "{}: {}",
                name.to_lowercase(),
                header_value(headers, name)?
            ));
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
    if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(s, "%a, %d %b %Y %H:%M:%S GMT") {
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
    let parsed = parse_first_legacy_signature(headers).ok_or("missing Signature header")??;
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
        // Overflow-safe skew check: `date_ms` derives from the
        // attacker-controlled `Date` header, so `checked_sub` +
        // `checked_abs` (+ `checked_mul` on the configured skew) fail
        // closed (Err) instead of panicking on extreme values.
        let skew_ok = now
            .checked_sub(date_ms)
            .and_then(|delta| delta.checked_abs())
            .zip(max_clock_skew_secs.checked_mul(1000))
            .is_some_and(|(delta, skew_ms)| delta <= skew_ms);
        if !skew_ok {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activitypub::signatures::keys::{
        RsaPrivateKey, RsaPublicKey, generate_rsa_keypair, generate_rsa_keypair_2048,
        private_key_to_pkcs8_pem, public_key_to_spki_pem, rsa_sha256_sign_base64,
    };
    use crate::activitypub::signatures::sig_mode::scheme_of_public_url;
    use axum::http::HeaderValue;

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
        assert!(
            signed
                .signature
                .contains("headers=\"(request-target) date host\"")
        );
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
        assert_eq!(
            actor_uri_from_key_id(&parsed.key_id),
            "https://example.local/users/123"
        );
        // Tampered path fails.
        assert!(
            verify_http_signature(
                "get",
                "/users/124",
                &headers,
                None,
                &pub_pem,
                Some(now),
                300
            )
            .is_err()
        );
        // Tampered host header value fails.
        let bad = test_headers(&[
            ("date", date),
            ("host", "evil.example"),
            ("signature", &signed.signature),
        ]);
        assert!(
            verify_http_signature("get", "/users/123", &bad, None, &pub_pem, Some(now), 300)
                .is_err()
        );
        // Wrong public key fails.
        let (_, other) = generate_rsa_keypair(1024).unwrap();
        let other_pem = public_key_to_spki_pem(&other);
        assert!(
            verify_http_signature(
                "get",
                "/users/123",
                &headers,
                None,
                &other_pem,
                Some(now),
                300
            )
            .is_err()
        );
        // Expired Date fails; missing skew check passes.
        assert!(
            verify_http_signature(
                "get",
                "/users/123",
                &headers,
                None,
                &pub_pem,
                Some(now + 3600_000),
                300
            )
            .is_err()
        );
        assert!(
            verify_http_signature("get", "/users/123", &headers, None, &pub_pem, None, 300).is_ok()
        );
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
        assert!(
            signed
                .signature
                .contains("headers=\"(request-target) date host digest\"")
        );
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
        assert_eq!(
            actor_uri_from_key_id(&parsed.key_id),
            "https://example.local/users/123"
        );
        // Tampered body: Digest mismatch fails.
        let tampered = br#"{"type":"Follow","tampered":true}"#;
        assert!(
            verify_http_signature(
                "POST",
                "/users/456/inbox",
                &headers,
                Some(tampered),
                &pub_pem,
                Some(now),
                300
            )
            .is_err()
        );
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
        assert!(
            verify_http_signature(
                "POST",
                "/users/456/inbox",
                &get_headers,
                Some(body),
                &pub_pem,
                Some(now),
                300
            )
            .is_err()
        );
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
        let signed_headers = test_headers(&[
            ("date", date),
            ("host", "example.local"),
            ("digest", &digest),
        ]);
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
        assert!(
            parse_signature_header("keyId=\"a\",algorithm=\"hs2019\",signature=\"AAA=\"").is_ok()
        );
        assert!(
            parse_signature_header("keyId=\"a\",algorithm=\"hmac-sha256\",signature=\"AAA=\"")
                .is_err()
        );
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
        assert!(
            verify_http_signature(
                "get",
                "/users/123",
                &headers,
                None,
                &pub_pem,
                Some(now),
                300
            )
            .is_err()
        );
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
        assert!(
            verify_http_signature(
                "get",
                "/users/123",
                &bad_date,
                None,
                &pub_pem,
                Some(now),
                300
            )
            .is_err()
        );
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
        assert!(has_legacy_signature(&test_headers(&[(
            "signature",
            &signed.signature
        )])));
        assert!(!has_legacy_signature(&test_headers(&[(
            "signature",
            "sig1=:QUJD:"
        )])));
        assert!(!has_legacy_signature(&HeaderMap::new()));
        assert_eq!(scheme_of_public_url("https://example.local"), "https");
        assert_eq!(scheme_of_public_url("http://127.0.0.1:3000/"), "http");
    }
}
