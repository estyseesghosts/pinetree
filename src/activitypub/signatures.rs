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
use sha2::{Digest as _, Sha256};
use signature::{SignatureEncoding, Signer, Verifier};
use std::str::FromStr;

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
pub fn rsa_sha256_sign(private_key_pem: &str, message: &[u8]) -> Result<Vec<u8>, String> {
    let key = parse_private_key_pem(private_key_pem)?;
    let signing_key = SigningKey::<Sha256>::new(key);
    let sig: Signature = signing_key.sign(message);
    Ok(sig.to_bytes().as_ref().to_vec())
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
) -> Result<String, String> {
    let mut lines = Vec::with_capacity(signed_header_names.len());
    for name in signed_header_names {
        if name.eq_ignore_ascii_case("(request-target)") {
            lines.push(format!(
                "(request-target): {} {}",
                method.to_lowercase(),
                path_and_query
            ));
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
    let raw = headers
        .get("signature")
        .and_then(|v| v.to_str().ok())
        .or_else(|| {
            headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
        })
        .ok_or("missing Signature header")?;
    let parsed = parse_signature_header(raw)?;
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
    let signing_string = build_signing_string(method, path_and_query, headers, &parsed.headers)?;
    if !rsa_sha256_verify(public_key_pem, signing_string.as_bytes(), &parsed.signature)? {
        return Err("HTTP signature verification failed".to_string());
    }
    Ok(parsed)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::pkcs1::EncodeRsaPublicKey;

    /// Crate-backed equivalent of the old textbook-fixture DER roundtrip:
    /// every PEM encoding parses back to the same key, and mismatched labels
    /// are rejected.
    #[test]
    fn key_pem_roundtrip_all_encodings() {
        let (priv_key, pub_key) = generate_rsa_keypair(1024).unwrap();
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
        for _ in 0..3 {
            let (priv_key, pub_key) = generate_rsa_keypair(1024).unwrap();
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
        let (priv_key, pub_key) = generate_rsa_keypair(1024).unwrap();
        let priv_pem = private_key_to_pkcs8_pem(&priv_key);
        let pub_pem = public_key_to_spki_pem(&pub_key);
        let priv_back = parse_private_key_pem(&priv_pem).unwrap();
        let pub_back = parse_public_key_pem(&pub_pem).unwrap();
        // `rsa` key types carry no `PartialEq`; compare canonical PEMs.
        assert_eq!(private_key_to_pkcs8_pem(&priv_back), priv_pem);
        assert_eq!(public_key_to_spki_pem(&pub_back), pub_pem);
        let msg = b"hello federation";
        let sig = rsa_sha256_sign(&priv_pem, msg).unwrap();
        assert_eq!(sig.len(), 128);
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
        let (priv_key, pub_key) = generate_rsa_keypair(1024).unwrap();
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
        let (priv_key, pub_key) = generate_rsa_keypair(1024).unwrap();
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

    #[test]
    fn signature_header_parsing_variants() {
        let (priv_key, _) = generate_rsa_keypair(1024).unwrap();
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
        let (priv_key, pub_key) = generate_rsa_keypair(1024).unwrap();
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
}
