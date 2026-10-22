//! `Digest` and `Content-Digest` header helpers (RFC 9530 style
//! `"SHA-256=<base64>"` used by Misskey/Sharkey, and RFC 9421 §3.3.4
//! `Content-Digest`).

use sha2::{Digest as _, Sha256, Sha512};

use super::keys::{base64_decode, base64_encode};

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
// Content-Digest (RFC 9421 §3.3.4, sf-dictionary of byte sequences)
// ---------------------------------------------------------------------------

/// Compute the `Content-Digest` header value `sha-256=:<base64>:`.
pub fn compute_content_digest_header(body: &[u8]) -> String {
    format!(
        "sha-256=:{}:",
        base64_encode(Sha256::digest(body).as_slice())
    )
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

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn content_digest_roundtrip() {
        let body = br#"{"type":"Create"}"#;
        let header = compute_content_digest_header(body);
        assert!(header.starts_with("sha-256=:"));
        assert!(verify_content_digest_header(&header, body));
        assert!(!verify_content_digest_header(&header, b"tampered"));
        // sha-512 entries verify; unknown-only and malformed values fail.
        let sha512 = format!(
            "sha-512=:{}:",
            base64_encode(Sha256::digest(body).as_slice())
        );
        assert!(!verify_content_digest_header(&sha512, body));
        let real512 = format!(
            "sha-512=:{}:",
            base64_encode(Sha512::digest(body).as_slice())
        );
        assert!(verify_content_digest_header(&real512, body));
        assert!(verify_content_digest_header(
            &format!("md5=:AAAA:, {header}"),
            body
        ));
        assert!(!verify_content_digest_header("sha-256=:AAAA:", body));
        assert!(!verify_content_digest_header("garbage", body));
        assert!(!verify_content_digest_header("", body));
    }
}
