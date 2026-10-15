use super::AppServices;

use crate::application::ports::{InsertMediaInput, PortError};
use crate::domain::media::{MediaMeta, MediaProperties};

impl AppServices {
    #[allow(clippy::too_many_arguments)]
    pub fn upload_media(
        &self,
        owner_id: &str,
        name: String,
        mime: Option<String>,
        bytes: &[u8],
        comment: Option<String>,
        is_sensitive: bool,
        force: bool,
    ) -> Result<MediaMeta, PortError> {
        self.accounts.get_owner(owner_id)?;
        if bytes.len() as u64 > self.max_file_size {
            return Err(PortError::Validation(
                "MAX_FILE_SIZE_EXCEEDED".to_string(),
            ));
        }
        let hash = md5_hex(bytes);
        if !force {
            if let Some(existing) = self.media.find_by_owner_and_hash(owner_id, &hash)? {
                return Ok(existing);
            }
        }
        let sniffed = sniff_mime(bytes, mime.as_deref());
        let media_id = self.ids.generate();
        let url = format!("{}/files/{media_id}", self.public_url.trim_end_matches('/'));
        self.blobs.put(&hash, bytes)?;
        self.media.insert_metadata(InsertMediaInput {
            id: Some(media_id),
            owner_id: Some(owner_id.to_string()),
            name,
            mime: sniffed,
            size_bytes: bytes.len() as u64,
            hash,
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive,
            is_link: false,
            url,
            thumbnail_url: None,
            comment,
            max_file_size: self.max_file_size,
        })
    }
}

fn md5_hex(bytes: &[u8]) -> String {
    use md5::{Digest, Md5};
    let mut hasher = Md5::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Sniff the content type from magic bytes, falling back to the caller
/// provided `mime` or `application/octet-stream`.
fn sniff_mime(bytes: &[u8], fallback: Option<&str>) -> String {
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
    if std::str::from_utf8(bytes).is_ok() {
        return "text/plain".to_string();
    }
    fallback.unwrap_or("application/octet-stream").to_string()
}
