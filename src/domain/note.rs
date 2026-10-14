//! Note entity: pure data + validation (no I/O).

use std::collections::BTreeMap;

use crate::domain::id::DomainError;

fn invalid(msg: String) -> DomainError {
    DomainError::Validation(msg)
}

pub const MAX_NOTE_TEXT_CHARS: usize = 3000;
pub const MAX_CW_API_CHARS: usize = 100;
pub const MAX_CW_STORAGE_CHARS: usize = 512;
pub const MAX_FILE_IDS: usize = 16;
pub const MAX_TAG_CHARS: usize = 128;
pub const MAX_EMOJI_CHARS: usize = 128;

/// Least → most restrictive: `Public < Home < Followers < Specified`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum Visibility {
    Public,
    Home,
    Followers,
    Specified,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ReactionAcceptance {
    LikeOnly,
    LikeOnlyForRemote,
    NonSensitiveOnly,
    NonSensitiveOnlyForLocalLikeOnlyForRemote,
}

impl Default for Visibility {
    fn default() -> Self {
        Self::Public
    }
}

fn default_mentioned_remote_users() -> String {
    "[]".to_string()
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Note {
    pub id: String,
    pub author_id: String,
    pub text: Option<String>,
    pub cw: Option<String>,
    pub visibility: Visibility,
    pub visible_user_ids: Vec<String>,
    pub local_only: bool,
    pub reply_id: Option<String>,
    pub renote_id: Option<String>,
    pub reply_user_id: Option<String>,
    pub file_ids: Vec<String>,
    pub mentions: Vec<String>,
    #[serde(default = "default_mentioned_remote_users")]
    pub mentioned_remote_users: String,
    pub tags: Vec<String>,
    pub emojis: Vec<String>,
    pub reactions: BTreeMap<String, u64>,
    pub renote_count: u64,
    pub replies_count: u64,
    pub reaction_acceptance: Option<ReactionAcceptance>,
    pub uri: Option<String>,
    pub url: Option<String>,
    pub user_host: Option<String>,
    #[serde(default)]
    pub is_deleted: bool,
}

impl Note {
    /// API-create bounds: `text` 1..=3000 chars, `cw` 1..=100 chars.
    pub fn validate_api_create(&self) -> Result<(), DomainError> {
        if let Some(cw) = &self.cw {
            let n = cw.chars().count();
            if n == 0 || n > MAX_CW_API_CHARS {
                return Err(invalid(format!(
                    "cw must be 1..={MAX_CW_API_CHARS} chars for API create"
                )));
            }
        }
        self.validate_common()
    }

    /// Storage bounds: same as API create except `cw` carries up to 512 chars.
    pub fn validate_storage(&self) -> Result<(), DomainError> {
        if let Some(cw) = &self.cw {
            if cw.chars().count() > MAX_CW_STORAGE_CHARS {
                return Err(invalid(format!(
                    "cw exceeds {MAX_CW_STORAGE_CHARS} chars"
                )));
            }
        }
        self.validate_common()
    }

    fn validate_common(&self) -> Result<(), DomainError> {
        match &self.text {
            Some(text) => {
                let n = text.chars().count();
                if n == 0 || n > MAX_NOTE_TEXT_CHARS {
                    return Err(invalid(format!(
                        "text must be 1..={MAX_NOTE_TEXT_CHARS} chars"
                    )));
                }
                if self.renote_id.is_none()
                    && self.file_ids.is_empty()
                    && text.trim().is_empty()
                {
                    return Err(invalid(
                        "text must be non-blank without renote or files".to_string(),
                    ));
                }
            }
            None => {
                if self.renote_id.is_none() && self.file_ids.is_empty() {
                    return Err(invalid(
                        "text is required without renote or files".to_string(),
                    ));
                }
            }
        }
        if self.visibility == Visibility::Specified && self.visible_user_ids.is_empty() {
            return Err(invalid(
                "specified notes require non-empty visible_user_ids".to_string(),
            ));
        }
        if self.file_ids.len() > MAX_FILE_IDS {
            return Err(invalid(format!(
                "file_ids exceeds {MAX_FILE_IDS} items"
            )));
        }
        {
            let mut seen = std::collections::HashSet::new();
            if !self.file_ids.iter().all(|id| seen.insert(id)) {
                return Err(invalid("file_ids must be unique".to_string()));
            }
        }
        for tag in &self.tags {
            if tag.chars().count() > MAX_TAG_CHARS {
                return Err(invalid(format!(
                    "tag exceeds {MAX_TAG_CHARS} chars"
                )));
            }
        }
        for emoji in &self.emojis {
            if emoji.chars().count() > MAX_EMOJI_CHARS {
                return Err(invalid(format!(
                    "emoji name exceeds {MAX_EMOJI_CHARS} chars"
                )));
            }
        }
        for (label, value) in [("uri", &self.uri), ("url", &self.url)] {
            if let Some(v) = value {
                if v.chars().count() > 512 {
                    return Err(invalid(format!("{label} exceeds 512 chars")));
                }
            }
        }
        if let Some(user_host) = &self.user_host {
            if user_host.chars().count() > 128 {
                return Err(invalid("user_host exceeds 128 chars".to_string()));
            }
        }
        Ok(())
    }
}

/// Extracts `@username` local usernames from text.
pub fn extract_mentions(text: &str) -> Vec<String> {
    extract_prefixed(text, '@', true)
}

/// Extracts both local `@username` and remote `@username@host` mentions from text.
pub fn extract_all_mentions(text: &str) -> Vec<(String, Option<String>)> {
    let chars: Vec<char> = text.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let boundary_ok =
            i == 0 || !(chars[i - 1].is_alphanumeric() || chars[i - 1] == '_');
        if chars[i] == '@' && boundary_ok {
            let mut j = i + 1;
            while j < chars.len()
                && (chars[j].is_ascii_alphanumeric()
                    || chars[j] == '_'
                    || chars[j] == '.'
                    || chars[j] == '-')
            {
                j += 1;
            }
            if j > i + 1 {
                let username: String = chars[i + 1..j].iter().collect();
                if j < chars.len() && chars[j] == '@' {
                    let mut k = j + 1;
                    while k < chars.len()
                        && (chars[k].is_ascii_alphanumeric()
                            || chars[k] == '.'
                            || chars[k] == '-'
                            || chars[k] == ':')
                    {
                        k += 1;
                    }
                    while k > j + 1 && chars[k - 1] == '.' {
                        k -= 1;
                    }
                    if k > j + 1 {
                        let host: String = chars[j + 1..k].iter().collect();
                        out.push((username, Some(host)));
                        i = k;
                        continue;
                    }
                }
                if username.len() <= 20
                    && username
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_')
                {
                    out.push((username, None));
                }
                i = j;
                continue;
            }
        }
        i += 1;
    }
    out
}

/// Extracts `#tag` values (each <= 128 chars) from text.
pub fn extract_hashtags(text: &str) -> Vec<String> {
    extract_prefixed(text, '#', false)
}

fn extract_prefixed(text: &str, prefix: char, ascii_only: bool) -> Vec<String> {
    let chars: Vec<char> = text.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let boundary_ok =
            i == 0 || !(chars[i - 1].is_alphanumeric() || chars[i - 1] == '_');
        if chars[i] == prefix && boundary_ok {
            let mut j = i + 1;
            while j < chars.len()
                && (chars[j] == '_'
                    || (if ascii_only {
                        chars[j].is_ascii_alphanumeric()
                    } else {
                        chars[j].is_alphanumeric()
                    }))
            {
                j += 1;
            }
            let value: String = chars[i + 1..j].iter().collect();
            let n = value.chars().count();
            let max = if prefix == '#' { 128 } else { 20 };
            if n >= 1 && n <= max {
                out.push(value);
            }
            i = j;
        } else {
            i += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_note() -> Note {
        Note {
            id: "nid00000000000001".to_string(),
            author_id: "aid00000000000001".to_string(),
            text: Some("hello".to_string()),
            cw: None,
            visibility: Visibility::Public,
            visible_user_ids: Vec::new(),
            local_only: false,
            reply_id: None,
            renote_id: None,
            reply_user_id: None,
            file_ids: Vec::new(),
            mentions: Vec::new(),
            mentioned_remote_users: "[]".to_string(),
            tags: Vec::new(),
            emojis: Vec::new(),
            reactions: BTreeMap::new(),
            renote_count: 0,
            replies_count: 0,
            reaction_acceptance: None,
            uri: None,
            url: None,
            user_host: None,
            is_deleted: false,
        }
    }

    #[test]
    fn visibility_ordering() {
        assert!(Visibility::Public < Visibility::Home);
        assert!(Visibility::Home < Visibility::Followers);
        assert!(Visibility::Followers < Visibility::Specified);
    }

    #[test]
    fn valid_note_passes_both_validators() {
        let n = valid_note();
        assert!(n.validate_api_create().is_ok());
        assert!(n.validate_storage().is_ok());
    }

    #[test]
    fn text_bounds() {
        let mut n = valid_note();
        n.text = Some("t".repeat(3000));
        assert!(n.validate_api_create().is_ok());

        n.text = Some("t".repeat(3001));
        assert!(n.validate_api_create().is_err());
        assert!(n.validate_storage().is_err());

        // Blank text without renote/files is rejected; None without them too.
        n.text = Some("   ".to_string());
        assert!(n.validate_api_create().is_err());
        n.text = None;
        assert!(n.validate_api_create().is_err());

        // Blank text rides along on a renote; files stand in for text.
        n.text = Some("   ".to_string());
        n.renote_id = Some("nid00000000000002".to_string());
        assert!(n.validate_api_create().is_ok());
        n.renote_id = None;
        n.text = None;
        n.file_ids = vec!["mid00000000000001".to_string()];
        assert!(n.validate_api_create().is_ok());

        // Empty string text is never valid.
        n.text = Some(String::new());
        assert!(n.validate_api_create().is_err());
    }

    #[test]
    fn cw_bounds() {
        let mut n = valid_note();
        n.cw = Some("c".repeat(100));
        assert!(n.validate_api_create().is_ok());
        n.cw = Some("c".repeat(101));
        assert!(n.validate_api_create().is_err());
        // Storage carries up to 512.
        assert!(n.validate_storage().is_ok());
        n.cw = Some("c".repeat(512));
        assert!(n.validate_storage().is_ok());
        n.cw = Some("c".repeat(513));
        assert!(n.validate_storage().is_err());
        // Empty cw is rejected at API create.
        n.cw = Some(String::new());
        assert!(n.validate_api_create().is_err());
    }

    #[test]
    fn specified_requires_visible_user_ids() {
        let mut n = valid_note();
        n.visibility = Visibility::Specified;
        assert!(n.validate_api_create().is_err());
        n.visible_user_ids = vec!["aid00000000000009".to_string()];
        assert!(n.validate_api_create().is_ok());
    }

    #[test]
    fn file_id_bounds() {
        let mut n = valid_note();
        n.text = None;
        n.file_ids = (0..16).map(|i| format!("mid{i:012}")).collect();
        assert!(n.validate_api_create().is_ok());
        n.file_ids.push("mid00000000000099".to_string());
        assert!(n.validate_api_create().is_err());

        n.file_ids = vec!["mid1".to_string(), "mid1".to_string()];
        assert!(n.validate_api_create().is_err());
    }

    #[test]
    fn tag_emoji_uri_bounds() {
        let mut n = valid_note();
        n.tags = vec!["t".repeat(128)];
        n.emojis = vec!["e".repeat(128)];
        assert!(n.validate_api_create().is_ok());
        n.tags = vec!["t".repeat(129)];
        assert!(n.validate_api_create().is_err());
        n.tags = Vec::new();
        n.emojis = vec!["e".repeat(129)];
        assert!(n.validate_api_create().is_err());

        let mut n = valid_note();
        n.uri = Some("u".repeat(513));
        assert!(n.validate_api_create().is_err());
        n.uri = None;
        n.user_host = Some("h".repeat(129));
        assert!(n.validate_api_create().is_err());
    }

    #[test]
    fn extract_helpers() {
        assert_eq!(
            extract_mentions("hello @alice and @bob_1!"),
            vec!["alice".to_string(), "bob_1".to_string()]
        );
        assert_eq!(extract_mentions("no mentions here"), Vec::<String>::new());
        assert_eq!(extract_mentions("mail a@b alone"), Vec::<String>::new());
        assert_eq!(
            extract_hashtags("#rust #hello_world done"),
            vec!["rust".to_string(), "hello_world".to_string()]
        );
        assert_eq!(
            extract_hashtags(&format!("#ok #{}", "t".repeat(129))),
            vec!["ok".to_string()]
        );
    }
}
