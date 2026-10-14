//! Media ingestion and content-type sniffing helpers (Phase 2 Slice C2).

use md5::{Digest, Md5};

/// Compute the lowercase MD5 hex digest of `bytes`.
pub fn md5_hex(bytes: &[u8]) -> String {
    let mut hasher = Md5::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Sniff the MIME content type from magic bytes, falling back to `fallback`
/// (stripping `; charset=...` parameters) or `"application/octet-stream"`.
pub fn sniff_mime(bytes: &[u8], fallback: Option<&str>) -> String {
    if bytes.starts_with(&[0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A]) {
        return "image/png".to_string();
    }
    if bytes.starts_with(&[0xFF, 0xD8, 0xFF]) {
        return "image/jpeg".to_string();
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return "image/gif".to_string();
    }
    if bytes.len() >= 12
        && bytes.starts_with(b"RIFF")
        && bytes.get(8..12) == Some(b"WEBP".as_slice())
    {
        return "image/webp".to_string();
    }
    if bytes.starts_with(b"%PDF") {
        return "application/pdf".to_string();
    }
    if bytes.len() >= 12 && bytes.get(4..8) == Some(b"ftyp".as_slice()) {
        return "video/mp4".to_string();
    }
    if let Some(fb) = fallback {
        let base = fb.split(';').next().unwrap_or(fb).trim();
        if !base.is_empty() && base != "application/octet-stream" {
            return base.to_ascii_lowercase();
        }
    }
    if std::str::from_utf8(bytes).is_ok() {
        return "text/plain".to_string();
    }
    "application/octet-stream".to_string()
}

/// Sanitize an untrusted remote filename so it obeys `MediaMeta::validate`
/// (`1..=256` chars, no NUL, `/`, or `\\`).
pub fn sanitize_filename(raw: Option<&str>, fallback_ext: &str) -> String {
    let candidate = raw
        .unwrap_or("")
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or("")
        .replace(['\0', '/', '\\'], "_");
    let trimmed = candidate.trim();
    if trimmed.is_empty() {
        format!("remote.{fallback_ext}")
    } else {
        trimmed.chars().take(200).collect()
    }
}
