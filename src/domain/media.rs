//! Media metadata: pure data + validation (no I/O, no bytes).

use crate::domain::id::DomainError;

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MediaProperties {
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub orientation: Option<u8>,
    pub avg_color: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MediaMeta {
    pub id: String,
    pub owner_id: Option<String>,
    pub name: String,
    pub mime: String,
    pub size_bytes: u64,
    pub hash: String,
    pub blurhash: Option<String>,
    pub properties: MediaProperties,
    pub is_sensitive: bool,
    pub is_link: bool,
    pub url: String,
    pub thumbnail_url: Option<String>,
    pub comment: Option<String>,
}

impl MediaMeta {
    /// `max_file_size` is instance configuration; exceeding it surfaces
    /// `MAX_FILE_SIZE_EXCEEDED` at the application layer.
    pub fn validate(&self, max_file_size: u64) -> Result<(), DomainError> {
        let name_len = self.name.chars().count();
        if name_len == 0 || name_len > 256 {
            return Err(DomainError::Validation(
                "name must be 1..=256 chars".to_string(),
            ));
        }
        if self.name.contains('\0') || self.name.contains('/') || self.name.contains('\\') {
            return Err(DomainError::Validation(
                "name must not contain NUL, / or \\".to_string(),
            ));
        }
        let mime_len = self.mime.chars().count();
        if mime_len == 0 || mime_len > 128 {
            return Err(DomainError::Validation(
                "mime must be 1..=128 chars".to_string(),
            ));
        }
        if self.size_bytes > max_file_size {
            return Err(DomainError::Validation(
                "size_bytes exceeds max_file_size".to_string(),
            ));
        }
        // Hashes are hex/alnum text; raw binary blob bytes are rejected here.
        if self.hash.is_empty()
            || self.hash.len() > 128
            || !self.hash.bytes().all(|b| b.is_ascii_alphanumeric())
        {
            return Err(DomainError::Validation(
                "hash must be 1..=128 ASCII alphanumeric chars".to_string(),
            ));
        }
        if let Some(blurhash) = &self.blurhash {
            if blurhash.chars().count() > 128 {
                return Err(DomainError::Validation(
                    "blurhash exceeds 128 chars".to_string(),
                ));
            }
        }
        let url_len = self.url.chars().count();
        if url_len == 0 || url_len > 1024 {
            return Err(DomainError::Validation(
                "url must be 1..=1024 chars".to_string(),
            ));
        }
        if let Some(thumbnail_url) = &self.thumbnail_url {
            if thumbnail_url.chars().count() > 512 {
                return Err(DomainError::Validation(
                    "thumbnail_url exceeds 512 chars".to_string(),
                ));
            }
        }
        if let Some(comment) = &self.comment {
            if comment.chars().count() > 512 {
                return Err(DomainError::Validation(
                    "comment exceeds 512 chars".to_string(),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX_FILE_SIZE: u64 = 1024;

    fn valid_media() -> MediaMeta {
        MediaMeta {
            id: "mid00000000000001".to_string(),
            owner_id: Some("aid00000000000001".to_string()),
            name: "photo.png".to_string(),
            mime: "image/png".to_string(),
            size_bytes: 512,
            hash: "d41d8cd98f00b204e9800998ecf8427e".to_string(),
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive: false,
            is_link: false,
            url: "https://example.local/files/mid00000000000001".to_string(),
            thumbnail_url: None,
            comment: None,
        }
    }

    #[test]
    fn valid_media_passes() {
        assert!(valid_media().validate(MAX_FILE_SIZE).is_ok());
    }

    #[test]
    fn name_bounds() {
        let mut m = valid_media();
        m.name = "n".repeat(256);
        assert!(m.validate(MAX_FILE_SIZE).is_ok());
        m.name = "n".repeat(257);
        assert!(m.validate(MAX_FILE_SIZE).is_err());
        for bad in ["", "a\0b", "a/b", "a\\b"] {
            m.name = bad.to_string();
            assert!(m.validate(MAX_FILE_SIZE).is_err(), "expected err: {bad:?}");
        }
    }

    #[test]
    fn mime_and_size_bounds() {
        let mut m = valid_media();
        m.mime = String::new();
        assert!(m.validate(MAX_FILE_SIZE).is_err());
        m.mime = "m".repeat(129);
        assert!(m.validate(MAX_FILE_SIZE).is_err());
        m.mime = "image/png".to_string();
        m.size_bytes = MAX_FILE_SIZE;
        assert!(m.validate(MAX_FILE_SIZE).is_ok());
        m.size_bytes = MAX_FILE_SIZE + 1;
        assert!(m.validate(MAX_FILE_SIZE).is_err());
    }

    #[test]
    fn hash_rejects_binary() {
        let mut m = valid_media();
        assert!(m.validate(MAX_FILE_SIZE).is_ok());
        m.hash = String::new();
        assert!(m.validate(MAX_FILE_SIZE).is_err());
        m.hash = "h".repeat(129);
        assert!(m.validate(MAX_FILE_SIZE).is_err());
        // Raw binary/blob bytes are not valid hashes.
        m.hash = "\u{0}\u{1}\u{2}binary".to_string();
        assert!(m.validate(MAX_FILE_SIZE).is_err());
        m.hash = "not a hash!".to_string();
        assert!(m.validate(MAX_FILE_SIZE).is_err());
    }

    #[test]
    fn url_comment_bounds() {
        let mut m = valid_media();
        m.url = "u".repeat(1024);
        assert!(m.validate(MAX_FILE_SIZE).is_ok());
        m.url = "u".repeat(1025);
        assert!(m.validate(MAX_FILE_SIZE).is_err());
        m.url = String::new();
        assert!(m.validate(MAX_FILE_SIZE).is_err());

        let mut m = valid_media();
        m.thumbnail_url = Some("t".repeat(513));
        assert!(m.validate(MAX_FILE_SIZE).is_err());
        m.thumbnail_url = Some("t".repeat(512));
        m.comment = Some("c".repeat(513));
        assert!(m.validate(MAX_FILE_SIZE).is_err());
        m.comment = Some("c".repeat(512));
        m.blurhash = Some("b".repeat(129));
        assert!(m.validate(MAX_FILE_SIZE).is_err());
        m.blurhash = Some("b".repeat(128));
        assert!(m.validate(MAX_FILE_SIZE).is_ok());
    }
}
