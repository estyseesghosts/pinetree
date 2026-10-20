//! Antennas (Batch N WS4; guide WP15; Misskey `AntennaService` parity).
//!
//! Antennas are rules collecting matching notes into a personal feed. The
//! match order below copies `AntennaService.checkHitAntenna` (`:116-209`)
//! with two documented divergences (manifest §29): `home` matches the
//! owner's own notes + notes from followed accounts (upstream `home` is a
//! TODO matching everything), and there is no 7-day auto-deactivation
//! (instance cap instead).

use crate::domain::id::DomainError;
use crate::domain::note::Visibility;

fn invalid(msg: String) -> DomainError {
    DomainError::Validation(msg)
}

/// Antenna note source (upstream `src` enum + `user_list_id`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AntennaSrc {
    Home,
    All,
    Users,
    #[serde(rename = "users_blacklist")]
    UsersBlacklist,
    List,
}

impl AntennaSrc {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "home" => Some(Self::Home),
            "all" => Some(Self::All),
            "users" => Some(Self::Users),
            "users_blacklist" => Some(Self::UsersBlacklist),
            "list" => Some(Self::List),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Home => "home",
            Self::All => "all",
            Self::Users => "users",
            Self::UsersBlacklist => "users_blacklist",
            Self::List => "list",
        }
    }
}

/// One antenna rule (upstream `MiAntenna` + `antennas/create` params).
/// `exclude_notes_in_sensitive_channel` is accepted and stored but has no
/// effect (no channels exist).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Antenna {
    pub id: String,
    pub owner_id: String,
    pub name: String,
    pub src: AntennaSrc,
    pub user_list_id: Option<String>,
    /// Outer OR, inner AND keyword groups.
    pub keywords: Vec<Vec<String>>,
    pub exclude_keywords: Vec<Vec<String>>,
    /// Lowercased `user[@host]` accts.
    pub users: Vec<String>,
    pub case_sensitive: bool,
    pub with_replies: bool,
    pub with_file: bool,
    pub local_only: bool,
    pub exclude_bots: bool,
    pub exclude_notes_in_sensitive_channel: bool,
    pub is_active: bool,
    pub last_used_at_ms: i64,
    pub created_at_ms: i64,
}

impl Antenna {
    /// Misskey `create.ts` param rules: name 1..=100; `src=list` requires a
    /// list id; at least one non-empty keyword group overall
    /// (`EMPTY_KEYWORD`).
    pub fn validate(&self) -> Result<(), DomainError> {
        let n = self.name.chars().count();
        if n == 0 || n > 100 {
            return Err(invalid("antenna name must be 1..=100 chars".to_string()));
        }
        if self.id.trim().is_empty() || self.owner_id.trim().is_empty() {
            return Err(invalid("antenna requires id and owner".to_string()));
        }
        if self.src == AntennaSrc::List
            && self.user_list_id.as_deref().map(str::trim).unwrap_or("").is_empty()
        {
            return Err(invalid("antenna src=list requires user_list_id".to_string()));
        }
        if !has_keyword(&self.keywords) && !has_keyword(&self.exclude_keywords) {
            return Err(invalid("EMPTY_KEYWORD".to_string()));
        }
        Ok(())
    }
}

/// True when at least one keyword group holds a non-blank entry (empties
/// are stripped at match time). Upstream `create.ts` EMPTY_KEYWORD rule.
pub fn has_keyword(groups: &[Vec<String>]) -> bool {
    groups.iter().any(|g| g.iter().any(|k| !k.trim().is_empty()))
}

/// Default antenna cap per instance (upstream `antennaLimit: 5`,
/// `RoleService.ts:99` parity; enforced + advertised).
pub const DEFAULT_ANTENNA_LIMIT: usize = 5;

/// Feed cap per antenna (newest 200 entries; upstream `push` parity).
pub const ANTENNA_NOTES_CAP: usize = 200;

/// Resolved note view the matcher runs on (application layer assembles it:
/// author record + owner relationship lookups stay out of the domain).
pub struct AntennaNoteView<'a> {
    pub note: &'a crate::domain::note::Note,
    /// Lowercased `username` or `username@host` of the note author.
    pub author_acct: String,
    pub author_is_bot: bool,
    pub author_is_local: bool,
    /// True when the owner authored the note.
    pub owner_is_author: bool,
    /// True when the owner follows the note author.
    pub owner_follows_author: bool,
    /// True when the note author is in `visibleUserIds` (specified notes) —
    /// equivalently, whether the owner may see it via the specified path.
    pub owner_in_visible_ids: bool,
}

/// Upstream `checkHitAntenna` order (`AntennaService.ts:116-209`):
/// sensitive-channel/bots → localOnly → withReplies → visibility →
/// src → keywords → excludeKeywords → withFile. `case_sensitive` toggles
/// case handling; empty strings are stripped; null text+cw never matches.
pub fn antenna_matches(antenna: &Antenna, view: &AntennaNoteView) -> bool {
    let note = view.note;
    // (1) bots / sensitive channels (no channels exist: second half no-op).
    if antenna.exclude_bots && view.author_is_bot {
        return false;
    }
    // (2) localOnly drops remote authors.
    if antenna.local_only && !view.author_is_local {
        return false;
    }
    // (3) withReplies=false drops every reply, including self-replies.
    if !antenna.with_replies && note.is_reply() {
        return false;
    }
    // (4) visibility: specified requires owner==author or owner in
    // visibleUserIds; followers requires owner-follows-author.
    match note.visibility {
        Visibility::Specified => {
            if !view.owner_is_author && !view.owner_in_visible_ids {
                return false;
            }
        }
        Visibility::Followers => {
            if !view.owner_is_author && !view.owner_follows_author {
                return false;
            }
        }
        Visibility::Public | Visibility::Home => {}
    }
    // (5) source. DIVERGENCE `home`: owner's own notes + notes from
    // followed accounts (upstream `home` is a TODO matching everything).
    // `list` has no membership index (WP16 pending): matches nothing —
    // the endpoint refuses `src=list` outright (documented).
    match antenna.src {
        AntennaSrc::All => {}
        AntennaSrc::Home => {
            if !view.owner_is_author && !view.owner_follows_author {
                return false;
            }
        }
        AntennaSrc::Users => {
            if !antenna.users.iter().any(|u| u == &view.author_acct) {
                return false;
            }
        }
        AntennaSrc::UsersBlacklist => {
            if antenna.users.iter().any(|u| u == &view.author_acct) {
                return false;
            }
        }
        AntennaSrc::List => return false,
    }
    // (6) keywords: OR of AND-groups over `text + "\n" + cw`.
    let haystack = format!(
        "{}\n{}",
        note.text.as_deref().unwrap_or(""),
        note.cw.as_deref().unwrap_or("")
    );
    if !matches_keywords(&antenna.keywords, &haystack, antenna.case_sensitive) {
        return false;
    }
    // (7) excludeKeywords: any matching group drops the note. An empty
    // outer list excludes nothing (upstream: exclusion only runs when the
    // list is non-empty).
    if !antenna.exclude_keywords.is_empty()
        && matches_keywords(&antenna.exclude_keywords, &haystack, antenna.case_sensitive)
    {
        return false;
    }
    // (8) withFile drops notes with no attachments.
    if antenna.with_file && note.file_ids.is_empty() {
        return false;
    }
    true
}

/// OR-of-AND substring match. Empty strings are stripped; an emptied group
/// never matches; a group matches when every entry hits. An empty outer
/// list matches everything (upstream: no constraint). Null text+cw (empty
/// haystack after the join) only matches an empty outer list.
fn matches_keywords(groups: &[Vec<String>], haystack: &str, case_sensitive: bool) -> bool {
    if groups.is_empty() {
        return true;
    }
    let hay = if case_sensitive {
        haystack.to_string()
    } else {
        haystack.to_lowercase()
    };
    groups.iter().any(|group| {
        let terms: Vec<String> = group
            .iter()
            .map(|k| if case_sensitive { k.clone() } else { k.to_lowercase() })
            .filter(|k| !k.trim().is_empty())
            .collect();
        !terms.is_empty() && terms.iter().all(|t| hay.contains(t))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn note(id: &str, author: &str, text: Option<&str>) -> crate::domain::note::Note {
        crate::domain::note::Note {
            id: id.to_string(),
            author_id: author.to_string(),
            text: text.map(str::to_string),
            cw: None,
            visibility: Visibility::Public,
            visible_user_ids: Vec::new(),
            local_only: false,
            reply_id: None,
            renote_id: None,
            reply_user_id: None,
            file_ids: Vec::new(),
            mentions: Vec::new(),
            mentioned_remote_users: String::new(),
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
            reply_parent_uri: None,
            quote: None,
            updated_at_ms: None,
            process_errors: None,
        }
    }

    fn antenna() -> Antenna {
        Antenna {
            id: "a1".to_string(),
            owner_id: "owner".to_string(),
            name: "n".to_string(),
            src: AntennaSrc::All,
            user_list_id: None,
            keywords: Vec::new(),
            exclude_keywords: Vec::new(),
            users: Vec::new(),
            case_sensitive: false,
            with_replies: true,
            with_file: false,
            local_only: false,
            exclude_bots: false,
            exclude_notes_in_sensitive_channel: false,
            is_active: true,
            last_used_at_ms: 0,
            created_at_ms: 0,
        }
    }

    fn view<'a>(note: &'a crate::domain::note::Note, author_acct: &str) -> AntennaNoteView<'a> {
        AntennaNoteView {
            note,
            author_acct: author_acct.to_string(),
            author_is_bot: false,
            author_is_local: true,
            owner_is_author: false,
            owner_follows_author: false,
            owner_in_visible_ids: false,
        }
    }

    #[test]
    fn src_all_with_no_keywords_matches_everything() {
        let a = antenna();
        let n = note("n1", "u1", Some("hello"));
        assert!(antenna_matches(&a, &view(&n, "u1")));
        let empty = note("n2", "u1", None);
        assert!(antenna_matches(&a, &view(&empty, "u1")));
    }

    #[test]
    fn keyword_and_or_and_case_rules() {
        let mut a = antenna();
        a.keywords = vec![vec!["foo".to_string(), "bar".to_string()], vec!["baz".to_string()]];
        let both = note("n1", "u1", Some("foo and bar"));
        assert!(antenna_matches(&a, &view(&both, "u1")));
        let second = note("n2", "u1", Some("a BAZ here"));
        assert!(antenna_matches(&a, &view(&second, "u1")));
        let neither = note("n3", "u1", Some("foo only"));
        assert!(!antenna_matches(&a, &view(&neither, "u1")));
        // Case-sensitive: BAZ no longer matches baz.
        a.case_sensitive = true;
        assert!(!antenna_matches(&a, &view(&second, "u1")));
        // Empty strings strip; an emptied group never matches.
        a.case_sensitive = false;
        a.keywords = vec![vec!["  ".to_string()]];
        assert!(!antenna_matches(&a, &view(&both, "u1")));
        // excludeKeywords drop.
        a.keywords = Vec::new();
        a.exclude_keywords = vec![vec!["spam".to_string()]];
        let spam = note("n4", "u1", Some("total spam post"));
        assert!(!antenna_matches(&a, &view(&spam, "u1")));
        assert!(antenna_matches(&a, &view(&both, "u1")));
    }

    #[test]
    fn cw_joins_the_haystack() {
        let mut a = antenna();
        a.keywords = vec![vec!["warn".to_string()]];
        let mut n = note("n1", "u1", Some("body"));
        n.cw = Some("WARN inside".to_string());
        assert!(antenna_matches(&a, &view(&n, "u1")));
    }

    #[test]
    fn visibility_and_source_gates() {
        let mut a = antenna();
        // specified: owner in visible ids passes.
        let mut n = note("n1", "u1", Some("x"));
        n.visibility = Visibility::Specified;
        let mut v = view(&n, "u1");
        assert!(!antenna_matches(&a, &v));
        v.owner_in_visible_ids = true;
        assert!(antenna_matches(&a, &v));
        // followers: follower passes.
        n.visibility = Visibility::Followers;
        let mut v = view(&n, "u1");
        assert!(!antenna_matches(&a, &v));
        v.owner_follows_author = true;
        assert!(antenna_matches(&a, &v));
        // home divergence: self + followed only.
        a.src = AntennaSrc::Home;
        let mut v = view(&n, "u1");
        assert!(!antenna_matches(&a, &v));
        v.owner_follows_author = true;
        assert!(antenna_matches(&a, &v));
        v.owner_follows_author = false;
        v.owner_is_author = true;
        assert!(antenna_matches(&a, &v));
        // users / blacklist (back to public visibility for the src gates).
        n.visibility = Visibility::Public;
        a.src = AntennaSrc::Users;
        a.users = vec!["u1".to_string()];
        assert!(antenna_matches(&a, &view(&n, "u1")));
        assert!(!antenna_matches(&a, &view(&n, "u2")));
        a.src = AntennaSrc::UsersBlacklist;
        assert!(!antenna_matches(&a, &view(&n, "u1")));
        assert!(antenna_matches(&a, &view(&n, "u2")));
        // list matches nothing (WP16 pending).
        a.src = AntennaSrc::List;
        assert!(!antenna_matches(&a, &view(&n, "u1")));
    }

    #[test]
    fn replies_files_bots_locality() {
        let mut a = antenna();
        a.with_replies = false;
        let mut n = note("n1", "u1", Some("x"));
        n.reply_id = Some("p".to_string());
        assert!(!antenna_matches(&a, &view(&n, "u1")));
        a.with_replies = true;
        a.with_file = true;
        assert!(!antenna_matches(&a, &view(&n, "u1")));
        n.file_ids = vec!["f1".to_string()];
        assert!(antenna_matches(&a, &view(&n, "u1")));
        a.local_only = true;
        let mut v = view(&n, "u1");
        v.author_is_local = false;
        assert!(!antenna_matches(&a, &v));
        v.author_is_local = true;
        v.author_is_bot = true;
        a.exclude_bots = true;
        assert!(!antenna_matches(&a, &v));
    }

    #[test]
    fn validation_rules() {
        let mut a = antenna();
        assert!(a.validate().is_err(), "no keywords is EMPTY_KEYWORD");
        a.keywords = vec![vec!["x".to_string()]];
        assert!(a.validate().is_ok());
        a.name = "".to_string();
        assert!(a.validate().is_err());
        a.name = "n".to_string();
        a.src = AntennaSrc::List;
        assert!(a.validate().is_err(), "list needs a list id");
        a.user_list_id = Some("l1".to_string());
        assert!(a.validate().is_ok());
    }
}
