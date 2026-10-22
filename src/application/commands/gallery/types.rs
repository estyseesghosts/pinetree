use crate::application::ports::PortError;

/// Simple id cursor (`since_id` exclusive lower bound, `until_id`
/// exclusive upper bound, newest-first, `limit` clamped to 1..=100).
/// (Relocated from the removed `channels` module, which was its only
/// other user; gallery lists are the remaining consumer.)
#[derive(Debug, Clone, Default)]
pub struct IdCursor {
    pub since_id: Option<String>,
    pub until_id: Option<String>,
    pub limit: usize,
}

impl IdCursor {
    pub fn with_limit(limit: usize) -> Self {
        Self {
            since_id: None,
            until_id: None,
            limit: limit.clamp(1, 100),
        }
    }
}
/// Gallery post title bound (pinetree-fixed).
pub const GALLERY_TITLE_MAX: usize = 128;
/// Gallery post description bound (pinetree-fixed).
pub const GALLERY_DESCRIPTION_MAX: usize = 2048;
/// Gallery files per post (mirrors the note 16-file bound).
pub const GALLERY_FILES_MAX: usize = 16;
/// Flash title bound (pinetree-fixed).
pub const FLASH_TITLE_MAX: usize = 128;
/// Flash summary bound (pinetree-fixed).
pub const FLASH_SUMMARY_MAX: usize = 2048;
/// Flash script bound (Play scripts are code; generous cap, pinetree-fixed).
pub const FLASH_SCRIPT_MAX: usize = 100_000;
/// `popular` recency window.
pub const GALLERY_POPULAR_WINDOW_MS: i64 = 30 * 24 * 60 * 60 * 1000;
/// One gallery post (upstream `MiGalleryPost` local columns).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GalleryPost {
    pub id: String,
    pub owner_id: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub file_ids: Vec<String>,
    pub is_sensitive: bool,
    pub liked_count: u64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// One gallery like (unique on `(post_id, user_id)`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct GalleryLike {
    pub id: String,
    pub post_id: String,
    pub user_id: String,
    pub created_at_ms: i64,
}

/// One Play page (upstream `MiFlash` local columns).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Flash {
    pub id: String,
    pub owner_id: String,
    pub title: String,
    pub summary: Option<String>,
    pub script: String,
    pub visibility: FlashVisibility,
    pub liked_count: u64,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

/// Play visibility (`public` / `home`, upstream `FlashVisibility`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FlashVisibility {
    Public,
    Home,
}

impl FlashVisibility {
    pub fn parse(value: &str) -> Result<Self, PortError> {
        match value {
            "public" => Ok(Self::Public),
            "home" => Ok(Self::Home),
            _ => Err(PortError::Validation(
                "INVALID_PARAM visibility must be public or home".to_string(),
            )),
        }
    }
}

/// One flash like (unique on `(flash_id, user_id)`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FlashLike {
    pub id: String,
    pub flash_id: String,
    pub user_id: String,
    pub created_at_ms: i64,
}

pub fn validate_gallery_post(
    title: Option<&str>,
    description: Option<&str>,
    file_ids: &[String],
) -> Result<(), PortError> {
    if let Some(t) = title {
        if t.chars().count() > GALLERY_TITLE_MAX {
            return Err(PortError::Validation(format!(
                "gallery title must be <={GALLERY_TITLE_MAX} chars"
            )));
        }
    }
    if let Some(d) = description {
        if d.chars().count() > GALLERY_DESCRIPTION_MAX {
            return Err(PortError::Validation(format!(
                "gallery description must be <={GALLERY_DESCRIPTION_MAX} chars"
            )));
        }
    }
    if file_ids.is_empty() {
        return Err(PortError::Validation(
            "FILE_REQUIRED gallery posts need at least one file".to_string(),
        ));
    }
    if file_ids.len() > GALLERY_FILES_MAX {
        return Err(PortError::Validation(format!(
            "gallery posts hold at most {GALLERY_FILES_MAX} files"
        )));
    }
    let mut seen = std::collections::HashSet::new();
    for file_id in file_ids {
        if !seen.insert(file_id) {
            return Err(PortError::Validation(
                "INVALID_PARAM duplicate fileIds".to_string(),
            ));
        }
    }
    Ok(())
}

pub fn validate_flash(title: &str, summary: Option<&str>, script: &str) -> Result<(), PortError> {
    let title = title.trim();
    if title.is_empty() || title.chars().count() > FLASH_TITLE_MAX {
        return Err(PortError::Validation(format!(
            "flash title must be 1..={FLASH_TITLE_MAX} chars"
        )));
    }
    if let Some(s) = summary {
        if s.chars().count() > FLASH_SUMMARY_MAX {
            return Err(PortError::Validation(format!(
                "flash summary must be <={FLASH_SUMMARY_MAX} chars"
            )));
        }
    }
    if script.is_empty() || script.chars().count() > FLASH_SCRIPT_MAX {
        return Err(PortError::Validation(format!(
            "flash script must be 1..={FLASH_SCRIPT_MAX} chars"
        )));
    }
    Ok(())
}
