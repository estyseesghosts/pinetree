//! Pages (Misskey `MiPage`/`MiPageLike` parity + pinetree federation).
//!
//! Upstream shape (`models/Page.ts`): `title` (`varchar(256)`, required),
//! `name` slug (1–256 chars, `pageNameSchema`, unique per `(userId, name)`),
//! `summary` (nullable `varchar(256)`), `alignCenter`, `hideTitleWhenPinned`
//! (default false), `font` (`'serif' | 'sans-serif'`), `userId`,
//! `eyeCatchingImageId` (nullable), `content` (block array), `variables`,
//! **`script`** (AiScript, ≤16384, executes client-side upstream),
//! `visibility` (dead legacy — `PageService.create()` hardcodes
//! `'public'`, no API param sets it), `visibleUserIds`, `likedCount`.
//! `PageLike`: unique `(userId, pageId)`.
//!
//! Federation (DIVERGENCE — Misskey pages have zero AP surface): pages
//! federate as **`Article`** (`url` = page URL, `source` carries content
//! blocks + variables for pinetree↔pinetree fidelity). **`script` is NEVER
//! serialized into any federated field, structurally** — the Article
//! serializer has no code path that emits it. Inbound `Article`s keep the
//! existing note-pipeline path (Misskey parity); remote pages render as
//! long-form notes, never execute anything.

use crate::domain::collection::CollectionTier;
use crate::domain::id::DomainError;
use crate::domain::note::Visibility;

/// Page title bound (upstream `varchar(256)` column).
pub const PAGE_TITLE_MAX: usize = 256;
/// Page slug bound (upstream `pageNameSchema`, 1–256 chars).
pub const PAGE_NAME_MAX: usize = 256;
/// Page summary bound (upstream `varchar(256)`, nullable).
pub const PAGE_SUMMARY_MAX: usize = 256;
/// Page script bound (upstream `varchar(16384)`).
pub const PAGE_SCRIPT_MAX: usize = 16384;

/// Reserved characters rejected by upstream `pageNameSchema`:
/// whitespace/control (`\x00–\x20`) plus `: / ? # [ ] @ ! $ & ' ( ) * + , ; = \ %`.
fn slug_char_allowed(c: char) -> bool {
    if c <= '\u{20}' || c == '\u{7f}' {
        return false;
    }
    !matches!(
        c,
        ':' | '/'
            | '?'
            | '#'
            | '['
            | ']'
            | '@'
            | '!'
            | '$'
            | '&'
            | '\''
            | '('
            | ')'
            | '*'
            | '+'
            | ','
            | ';'
            | '='
            | '\\'
            | '%'
    )
}

/// Upstream `pageNameSchema` parity: 1–256 chars, no reserved characters.
pub fn validate_page_name(name: &str) -> Result<(), DomainError> {
    let n = name.chars().count();
    if n == 0 || n > PAGE_NAME_MAX {
        return Err(DomainError::Validation(format!(
            "page name must be 1..={PAGE_NAME_MAX} chars"
        )));
    }
    if !name.chars().all(slug_char_allowed) {
        return Err(DomainError::Validation(
            "page name contains reserved characters".to_string(),
        ));
    }
    Ok(())
}

/// One page (upstream `MiPage` + federation columns).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Page {
    pub id: String,
    pub owner_id: String,
    pub title: String,
    pub name: String,
    pub summary: Option<String>,
    pub content: serde_json::Value,
    pub variables: serde_json::Value,
    /// AiScript source. Local-only, never federated, never executed
    /// server-side (pinetree has no AiScript engine).
    pub script: String,
    pub eye_catching_image_id: Option<String>,
    pub font: PageFont,
    pub align_center: bool,
    pub hide_title_when_pinned: bool,
    /// Federation privacy tier. Local pages default `Listed` (upstream
    /// pages are world-readable); remote pages materialized from public
    /// Articles land `Listed`, others are never materialized (they keep
    /// the note path, which enforces visibility).
    pub tier: CollectionTier,
    pub federate: bool,
    /// Actor URIs allowed to fetch when `tier == RemoteApproved`.
    pub approved_actor_uris: Vec<String>,
    /// Canonical remote URI for pages materialized from inbound `Article`
    /// activities (`None` = locally authored). Keyed by a dedicated index
    /// for update/like/delete correlation. `#[serde(default)]` keeps rows
    /// written before this field decoding as local.
    #[serde(default)]
    pub uri: Option<String>,
    /// Original `attachment` entries of a materialized remote `Article`
    /// (http(s) URLs only, filtered on ingest). Re-emitted verbatim when
    /// we serve the page — local `eye_catching_image_id` stays `None` for
    /// remote pages (no inline remote-media fetch). `#[serde(default)]`
    /// for pre-field rows.
    #[serde(default)]
    pub remote_attachments: Vec<serde_json::Value>,
    pub liked_count: i64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    /// Read gate for the page body (reuses the note [`Visibility`] enum).
    /// Local creates default `Public` (upstream parity — world-readable).
    /// `#[serde(default)]` keeps old rows decoding as public.
    #[serde(default)]
    pub visibility: Visibility,
    /// Explicit reader ids when `visibility == Specified`.
    #[serde(default)]
    pub visible_user_ids: Vec<String>,
    /// Origin timestamps for materialized remote `Article`s (Phase 2/3:
    /// served in place of row times). `None` = pre-field rows / local.
    #[serde(default)]
    pub remote_published_ms: Option<i64>,
    #[serde(default)]
    pub remote_updated_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PageFont {
    Serif,
    SansSerif,
}

impl PageFont {
    pub fn as_str(&self) -> &'static str {
        match self {
            PageFont::Serif => "serif",
            PageFont::SansSerif => "sans-serif",
        }
    }

    pub fn parse(s: &str) -> Option<PageFont> {
        match s {
            "serif" => Some(PageFont::Serif),
            "sans-serif" => Some(PageFont::SansSerif),
            _ => None,
        }
    }
}

impl Default for PageFont {
    fn default() -> Self {
        PageFont::SansSerif
    }
}

impl Page {
    pub fn validate_title(title: &str) -> Result<(), DomainError> {
        let n = title.chars().count();
        if n == 0 || n > PAGE_TITLE_MAX {
            return Err(DomainError::Validation(format!(
                "page title must be 1..={PAGE_TITLE_MAX} chars"
            )));
        }
        Ok(())
    }

    pub fn validate_summary(summary: Option<&str>) -> Result<(), DomainError> {
        if let Some(s) = summary {
            if s.chars().count() > PAGE_SUMMARY_MAX {
                return Err(DomainError::Validation(format!(
                    "page summary must be <={PAGE_SUMMARY_MAX} chars"
                )));
            }
        }
        Ok(())
    }

    pub fn validate_script(script: &str) -> Result<(), DomainError> {
        if script.chars().count() > PAGE_SCRIPT_MAX {
            return Err(DomainError::Validation(format!(
                "page script must be <={PAGE_SCRIPT_MAX} chars"
            )));
        }
        Ok(())
    }

    /// Content blocks must be a JSON array; elements must be objects with a
    /// string `type` (block shapes are validated leniently — unknown block
    /// types are stored and skipped at render, never executed).
    pub fn validate_content(content: &serde_json::Value) -> Result<(), DomainError> {
        let arr = content
            .as_array()
            .ok_or_else(|| DomainError::Validation("page content must be an array".to_string()))?;
        for block in arr {
            let obj = block.as_object().ok_or_else(|| {
                DomainError::Validation("page content blocks must be objects".to_string())
            })?;
            if !obj.get("type").and_then(|t| t.as_str()).is_some() {
                return Err(DomainError::Validation(
                    "page content blocks require a string type".to_string(),
                ));
            }
        }
        Ok(())
    }
}

/// One page like (upstream `MiPageLike`, unique on `(user, page)`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PageLike {
    pub id: String,
    pub user_id: String,
    pub page_id: String,
    pub created_at_ms: i64,
}

/// Read gate for one page row: owner always passes, moderators bypass
/// (consistent with `pages/delete`), otherwise the note-style visibility
/// rule applies (Public = all, Followers = follower edge, Specified =
/// listed ids; Home collapses to Public — pages have no timelines).
pub fn page_visible_to(
    page: &Page,
    viewer_id: Option<&str>,
    is_moderator: bool,
    follows_owner: bool,
) -> bool {
    if let Some(v) = viewer_id {
        if v == page.owner_id {
            return true;
        }
    }
    if is_moderator {
        return true;
    }
    match page.visibility {
        Visibility::Public | Visibility::Home => true,
        Visibility::Followers => viewer_id.is_some_and(|_| follows_owner),
        Visibility::Specified => {
            viewer_id.is_some_and(|v| page.visible_user_ids.iter().any(|id| id == v))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slug_rules() {
        assert!(validate_page_name("hello-world_1").is_ok());
        assert!(validate_page_name("").is_err());
        assert!(validate_page_name(&"n".repeat(256)).is_ok());
        assert!(validate_page_name(&"n".repeat(257)).is_err());
        for bad in [
            "a b", "a/b", "a?b", "a#b", "a@b", "a:b", "a%b", "a\\b", "a,b", "a;b",
        ] {
            assert!(validate_page_name(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn font_roundtrip() {
        assert_eq!(PageFont::parse("serif"), Some(PageFont::Serif));
        assert_eq!(PageFont::parse("sans-serif"), Some(PageFont::SansSerif));
        assert_eq!(PageFont::parse("comic"), None);
        assert_eq!(PageFont::default(), PageFont::SansSerif);
    }
}
