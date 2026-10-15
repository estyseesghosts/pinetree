//! Media ingestion and content-type sniffing helpers (Phase 2 Slice C2).

use md5::{Digest, Md5};

/// Compute the lowercase MD5 hex digest of `bytes`.
pub fn md5_hex(bytes: &[u8]) -> String {
    let mut hasher = Md5::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Compute the lowercase SHA-256 hex digest of `bytes` (Phase 2.6 slice 8,
/// decision D6: the `BlobStore` content address). MD5 stays only as the
/// client-visible `MediaMeta.hash` / packer `md5` and the per-owner dedup key.
pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Sniffing lives in exactly one place: [`crate::domain::media::sniff_mime`]
/// (Phase 2.6 slice 7). The fallback-accepting duplicate that used to live
/// here was deleted: declared and remote types may never introduce a MIME
/// type outside the sniffed allowlist.

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
