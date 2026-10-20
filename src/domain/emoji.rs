//! Custom emoji (Batch N WS1 + WP8b; Misskey `MiEmoji` parity).
//!
//! Local emoji live in [`CustomEmoji`] rows (shortcode unique
//! case-insensitively, media bytes in the blob store via the drive `file`
//! row referenced by `media_id`). Remote emoji are metadata-only
//! [`RemoteEmoji`] rows (unique per host + lowercased shortcode), ingested
//! from AP `Emoji` tags at note-ingest time; image bytes are fetched lazily
//! by a bounded job only after a user opts in via `showRemoteEmojis`.

use crate::domain::id::DomainError;

fn invalid(msg: String) -> DomainError {
    DomainError::Validation(msg)
}

/// Misskey `admin/emoji/add.ts` shortcode rule (`/^[a-zA-Z0-9_]+$/`).
pub fn validate_shortcode(shortcode: &str) -> Result<(), DomainError> {
    if shortcode.is_empty() || shortcode.chars().count() > 128 {
        return Err(invalid("emoji shortcode must be 1..=128 chars".to_string()));
    }
    if !shortcode
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(invalid(
            "emoji shortcode must match ^[a-zA-Z0-9_]+$".to_string(),
        ));
    }
    Ok(())
}

/// One local custom emoji (Misskey `MiEmoji`, host always null).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CustomEmoji {
    pub id: String,
    pub shortcode: String,
    pub category: Option<String>,
    /// Drive file row holding the image bytes (`admin/emoji/add` `fileId`).
    pub media_id: Option<String>,
    pub aliases: Vec<String>,
    pub license: Option<String>,
    pub is_sensitive: bool,
    pub local_only: bool,
    /// Role ids allowed to use this emoji as a reaction (stored, never
    /// enforced — pinetree has no custom-role subsystem).
    pub role_ids: Vec<String>,
    pub created_at_ms: i64,
}

impl CustomEmoji {
    pub fn validate(&self) -> Result<(), DomainError> {
        validate_shortcode(&self.shortcode)?;
        if self.id.trim().is_empty() {
            return Err(invalid("emoji id must be non-empty".to_string()));
        }
        if let Some(category) = self.category.as_deref() {
            if category.chars().count() > 128 {
                return Err(invalid("emoji category must be <= 128 chars".to_string()));
            }
        }
        if self.aliases.len() > 32 {
            return Err(invalid("emoji aliases must be <= 32 entries".to_string()));
        }
        if let Some(license) = self.license.as_deref() {
            if license.chars().count() > 1024 {
                return Err(invalid("emoji license must be <= 1024 chars".to_string()));
            }
        }
        Ok(())
    }
}

/// One remote custom-emoji metadata row (WP8b opt-in).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RemoteEmoji {
    /// Lowercased host the emoji was seen on.
    pub host: String,
    /// Lowercased shortcode on that host.
    pub shortcode: String,
    /// Last-seen `icon.url` (packed until a local copy is cached).
    pub remote_url: String,
    pub first_seen_ms: i64,
    /// Drive file row holding lazily fetched bytes (set by the fetch job;
    /// `None` until fetched). Packing prefers it once present.
    pub media_id: Option<String>,
}

impl RemoteEmoji {
    pub fn new(
        host: &str,
        shortcode: &str,
        remote_url: &str,
        now_ms: i64,
    ) -> Result<Self, DomainError> {
        let host = host.trim().to_ascii_lowercase();
        let shortcode = shortcode.trim().to_ascii_lowercase();
        if host.is_empty() || host.chars().count() > 256 {
            return Err(invalid("remote emoji host must be 1..=256 chars".to_string()));
        }
        validate_shortcode(&shortcode)?;
        let remote_url = remote_url.trim().to_string();
        if !(remote_url.starts_with("https://") || remote_url.starts_with("http://")) {
            return Err(invalid("remote emoji url must be http(s)".to_string()));
        }
        Ok(Self {
            host,
            shortcode,
            remote_url,
            first_seen_ms: now_ms,
            media_id: None,
        })
    }
}

/// Parse one AP `Emoji` tag (`{type: 'Emoji', name: ':short:', icon: {url}}`
/// — Misskey `extractEmojis` + `renderEmoji` shape). Returns
/// `(shortcode, remote_url)` with surrounding colons stripped. Icon urls may
/// be a bare string, an object, or a single-element array (same leniency as
/// the attachment parser).
pub fn parse_emoji_tag(item: &serde_json::Value) -> Option<(String, String)> {
    let m = item.as_object()?;
    if m.get("type").and_then(|v| v.as_str()) != Some("Emoji") {
        return None;
    }
    let name = m.get("name").and_then(|v| v.as_str())?.trim();
    let shortcode = name.trim_matches(':').trim().to_string();
    if shortcode.is_empty() || validate_shortcode(&shortcode).is_err() {
        return None;
    }
    let url = m
        .get("icon")
        .and_then(|icon| match icon {
            // Upstream `isEmoji` requires a non-array icon object with
            // `url`; pinetree additionally accepts a bare-string icon (some
            // implementations emit it). Arrays stay rejected like upstream.
            serde_json::Value::String(s) => Some(s.as_str()),
            serde_json::Value::Object(o) => o.get("url").and_then(|v| v.as_str()),
            _ => None,
        })
        .map(str::trim)
        .filter(|s| !s.is_empty())?;
    Some((shortcode, url.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shortcode_rule_matches_upstream() {
        assert!(validate_shortcode("blobcat").is_ok());
        assert!(validate_shortcode("ABC_123").is_ok());
        assert!(validate_shortcode("").is_err());
        assert!(validate_shortcode("has-dash").is_err());
        assert!(validate_shortcode("has space").is_err());
        assert!(validate_shortcode("has.dot").is_err());
    }

    #[test]
    fn emoji_tag_parser_shapes() {
        let tag = serde_json::json!({
            "type": "Emoji",
            "name": ":blobcat:",
            "icon": { "type": "Image", "url": "https://r.example/e/blobcat.png" },
        });
        assert_eq!(
            parse_emoji_tag(&tag),
            Some(("blobcat".to_string(), "https://r.example/e/blobcat.png".to_string()))
        );
        // Bare-string icon and array-icon shapes.
        let bare = serde_json::json!({"type": "Emoji", "name": ":x:", "icon": "https://r.example/x.png"});
        assert_eq!(
            parse_emoji_tag(&bare),
            Some(("x".to_string(), "https://r.example/x.png".to_string()))
        );
        // Array icons fail upstream's `isEmoji` shape — skipped.
        let arr = serde_json::json!({"type": "Emoji", "name": ":y:", "icon": [{"url": "https://r.example/y.png"}]});
        assert!(parse_emoji_tag(&arr).is_none());
        // Non-emoji tags and invalid shortcodes are skipped.
        assert!(parse_emoji_tag(&serde_json::json!({"type": "Hashtag", "name": "#x"})).is_none());
        assert!(parse_emoji_tag(&serde_json::json!({"type": "Emoji", "name": ":a-b:", "icon": {"url": "https://r.example/a.png"}})).is_none());
    }

    #[test]
    fn remote_emoji_normalizes() {
        let r = RemoteEmoji::new("R.Example", "BlobCat", "https://r.example/e.png", 1).unwrap();
        assert_eq!(r.host, "r.example");
        assert_eq!(r.shortcode, "blobcat");
        assert!(RemoteEmoji::new("r.example", "x", "ftp://r.example/x", 1).is_err());
    }
}
