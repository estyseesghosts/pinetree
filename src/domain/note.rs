//! Note entity: pure data + validation (no I/O).

use std::collections::BTreeMap;

use crate::domain::id::DomainError;
use crate::domain::quote::QuoteReference;

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
    /// Unresolved ActivityPub reply parent (`docs/replies.md` Slice B): the
    /// canonical `inReplyTo` URI of a remotely authored reply whose parent
    /// was not cached at ingestion time. `None` once the parent resolves to
    /// `reply_id` (or when there is no parent). `#[serde(default)]` keeps
    /// rows written before this field existed decoding as resolved/roots
    /// (migration by default). Internal only: never packed into the
    /// Misskey-compatible API or rendered into ActivityPub documents. While
    /// set, ordinary reads withhold the note (fail-closed); see
    /// `domain::visibility::is_visible`.
    #[serde(default)]
    pub reply_parent_uri: Option<String>,
    /// Structured quote relationship. `None` for ordinary notes and pure
    /// renotes; `Some` marks a quote (which still carries `renote_id` for
    /// Misskey API compatibility). `#[serde(default)]` keeps rows written
    /// before quotes existed decoding as non-quotes (migration by default).
    #[serde(default)]
    pub quote: Option<QuoteReference>,
    /// Last edit time as unix millis (`None` = never edited). Set on every
    /// effective `notes/edit` (§8); packed as upstream `updatedAt`
    /// (ISO string or null). `#[serde(default)]` keeps rows written before
    /// §8 decoding as never-edited (migration by default).
    #[serde(default)]
    pub updated_at_ms: Option<i64>,
    /// Internal edit/strip diagnostics (Sharkey `processErrors` parity):
    /// entries are translation keys such as `quoteUnavailable` (set when a
    /// §3 strip downgrades a quote to a plain note). Packed on the Note
    /// shape; `None` packs as null. `#[serde(default)]` keeps older rows
    /// decoding without diagnostics (migration by default).
    #[serde(default)]
    pub process_errors: Option<Vec<String>>,
}

impl Note {
    /// True when this note is a quote (own commentary + embedded target),
    /// as opposed to an ordinary note or a pure renote.
    pub fn is_quote(&self) -> bool {
        self.quote.is_some()
    }

    /// True when this note is a reply in the threading sense: either linked
    /// to a resolved parent (`reply_id`) or waiting on an unresolved remote
    /// parent (`reply_parent_uri`). Read filters that exclude replies (e.g.
    /// `withReplies = false`) must use this, not `reply_id` alone, so
    /// unresolved replies are not misrepresented as roots. Direct-child
    /// queries (`notes/replies`) keep matching `reply_id` only: pending
    /// notes appear there only after linking.
    pub fn is_reply(&self) -> bool {
        self.reply_id.is_some() || self.reply_parent_uri.is_some()
    }

    /// True while the reply parent is unresolved: the note carries a pending
    /// `inReplyTo` URI but no linked `reply_id` yet.
    pub fn has_pending_reply_parent(&self) -> bool {
        self.reply_parent_uri.is_some()
    }

    /// API-create bounds: `text` 1..=3000 chars, `cw` 1..=100 chars.
    pub fn validate_api_create(&self) -> Result<(), DomainError> {
        self.validate_api_create_with_poll(false)
    }

    /// API-create validation with poll presence (Misskey `notes/create.ts`
    /// parity: text is required only when the note carries no renote, no
    /// files, and no poll).
    pub fn validate_api_create_with_poll(&self, has_poll: bool) -> Result<(), DomainError> {
        if let Some(cw) = &self.cw {
            let n = cw.chars().count();
            if n == 0 || n > MAX_CW_API_CHARS {
                return Err(invalid(format!(
                    "cw must be 1..={MAX_CW_API_CHARS} chars for API create"
                )));
            }
        }
        self.validate_common(has_poll)
    }

    /// Storage bounds: same as API create except `cw` carries up to 512 chars.
    pub fn validate_storage(&self) -> Result<(), DomainError> {
        self.validate_storage_with_poll(false)
    }

    /// Storage validation with poll presence (same text rule as the API
    /// path: a poll stands in for text).
    pub fn validate_storage_with_poll(&self, has_poll: bool) -> Result<(), DomainError> {
        if let Some(cw) = &self.cw {
            if cw.chars().count() > MAX_CW_STORAGE_CHARS {
                return Err(invalid(format!(
                    "cw exceeds {MAX_CW_STORAGE_CHARS} chars"
                )));
            }
        }
        self.validate_common(has_poll)
    }

    fn validate_common(&self, has_poll: bool) -> Result<(), DomainError> {
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
                    && !has_poll
                    && text.trim().is_empty()
                {
                    return Err(invalid(
                        "text must be non-blank without renote, files, or poll".to_string(),
                    ));
                }
            }
            None => {
                if self.renote_id.is_none() && self.file_ids.is_empty() && !has_poll {
                    return Err(invalid(
                        "text is required without renote, files, or poll".to_string(),
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
        if let Some(pending) = &self.reply_parent_uri {
            if pending.chars().count() > 2048 {
                return Err(invalid("reply_parent_uri exceeds 2048 chars".to_string()));
            }
            // A stored note is either linked (reply_id) or pending (URI),
            // never both: the link operation sets one while clearing the
            // other atomically.
            if self.reply_id.is_some() {
                return Err(invalid(
                    "reply has both a resolved parent and a pending parent URI".to_string(),
                ));
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

/// Sharkey `isQuote` parity (`NoteCreateService.isQuote`, synced with
/// `misc/is-renote.ts`): a renote carrying any of these is a quote, a pure
/// renote otherwise. `text`/`cw` count by presence (not blankness — the
/// caller trims later); `has_reply` is a resolved reply link (an unresolved
/// pending parent carries no relationship yet); `has_poll` is a poll
/// definition; `file_count` is attached files.
pub fn is_quote_content(
    text: Option<&str>,
    cw: Option<&str>,
    has_reply: bool,
    has_poll: bool,
    file_count: usize,
) -> bool {
    text.is_some() || cw.is_some() || has_reply || has_poll || file_count > 0
}

/// One `note_edits` version-history row (§8; Sharkey `MiNoteEdit` parity).
/// A row is written per *effective* edit (same-note no-ops write nothing).
/// `old_date_ms` is the note's previous `updated_at_ms` (the creation time
/// derived from the note id on the first edit); `updated_at_ms` is the
/// edit time. `file_ids` mirrors upstream (always unset on rows pinetree
/// writes — file changes are detected but their ids are not versioned).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NoteEdit {
    pub id: String,
    pub note_id: String,
    pub old_text: Option<String>,
    pub new_text: Option<String>,
    pub cw: Option<String>,
    #[serde(default)]
    pub file_ids: Option<Vec<String>>,
    pub old_date_ms: i64,
    pub updated_at_ms: i64,
}

impl NoteEdit {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.id.trim().is_empty() {
            return Err(invalid("note edit id is required".to_string()));
        }
        if self.note_id.trim().is_empty() {
            return Err(invalid("note edit note_id is required".to_string()));
        }
        Ok(())
    }
}

/// Edit permission (WP6 domain layering: Misskey `notes/edit` parity).
/// Only the author may edit, and only a live note. Deleted notes and
/// foreign editors are rejected before any storage write.
pub fn can_edit_note(note: &Note, editor_id: &str) -> Result<(), DomainError> {
    if note.is_deleted {
        return Err(invalid("NO_SUCH_NOTE".to_string()));
    }
    if note.author_id.as_str() != editor_id {
        return Err(invalid("YOU_ARE_NOT_THE_AUTHOR".to_string()));
    }
    Ok(())
}

/// Delete permission (WP6): the author may always delete a live note;
/// moderators (admin-implied) may delete anyone's live note. Deleted notes
/// are `NotFound(NO_SUCH_NOTE)` so deletes stay idempotent-safe at the op
/// layer (the op maps repeat deletes to success after this check).
pub fn can_delete_note(
    note: &Note,
    requester_id: &str,
    requester_is_moderator: bool,
) -> Result<(), DomainError> {
    if note.is_deleted {
        return Err(DomainError::NotFound("NO_SUCH_NOTE".to_string()));
    }
    if note.author_id.as_str() == requester_id || requester_is_moderator {
        Ok(())
    } else {
        Err(invalid("YOU_ARE_NOT_THE_AUTHOR".to_string()))
    }
}

/// Tombstone transition (WP6): a live note may tombstone; an already-deleted
/// note may not transition again (the op treats the `NotFound` as a
/// converged no-op for idempotent replays).
pub fn tombstone_transition(note: &Note) -> Result<(), DomainError> {
    if note.is_deleted {
        return Err(DomainError::NotFound("NO_SUCH_NOTE".to_string()));
    }
    Ok(())
}

/// MFM sanitize policy (WP6 domain layering: domain decides,
/// `activitypub/objects.rs` only renders). Strips the dangerous subset of
/// Misskey MFM/HTML that must never reach federation output or stored HTML:
/// `<script…>…</script>` blocks, inline event-handler attributes
/// (`on*=…`), and `javascript:`/`data:text/html` URL schemes. Plain MFM
/// (`**bold**`, `@mentions`, `#tags`, `:emoji:`, links) passes through
/// unchanged. Idempotent: sanitizing twice converges.
pub fn sanitize_mfm(input: &str) -> String {
    // Strip <script ...>...</script> (case-insensitive, spanning lines).
    let mut out = strip_script_blocks(input);
    // Strip inline event handlers: ` onxxx=` / ` ONXXX =`.
    out = strip_event_handlers(&out);
    // Neutralize dangerous URL schemes (case-insensitive).
    out = neutralize_scheme_case_insensitive(&out, "javascript");
    out = neutralize_scheme_case_insensitive(&out, "data:text/html");
    // Null bytes never survive (AP JSON + HTML parsers disagree on them).
    out = out.replace('\0', "");
    out
}

fn strip_script_blocks(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    loop {
        let lower = rest.to_lowercase();
        let Some(start) = lower.find("<script") else {
            out.push_str(rest);
            break;
        };
        out.push_str(rest.get(..start).unwrap_or(""));
        let after_open = rest.get(start..).unwrap_or("");
        let lower_after = after_open.to_lowercase();
        if let Some(end) = lower_after.find("</script>") {
            rest = after_open.get(end + "</script>".len()..).unwrap_or("");
        } else {
            // Unterminated <script: drop to end (fail-closed).
            break;
        }
    }
    out
}

fn strip_event_handlers(s: &str) -> String {
    // Remove ` on<name> = "..." | '...' | bare` occurrences. Operates on the
    // raw string so both HTML and MFM `$[x ...]` shapes are covered.
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        if bytes[i] == b' ' || bytes[i] == b'\t' || bytes[i] == b'\n' {
            let mut j = i + 1;
            if j + 2 <= s.len() && (s[j..].starts_with("on") || s[j..].starts_with("ON")) {
                // `on` must be followed by a handler name char.
                let mut k = j + 2;
                while k < s.len() && (bytes[k].is_ascii_alphanumeric() || bytes[k] == b'_') {
                    k += 1;
                }
                if k > j + 2 {
                    let mut m = k;
                    while m < s.len() && (bytes[m] == b' ' || bytes[m] == b'\t') {
                        m += 1;
                    }
                    if m < s.len() && bytes[m] == b'=' {
                        m += 1;
                        while m < s.len() && (bytes[m] == b' ' || bytes[m] == b'\t') {
                            m += 1;
                        }
                        if m < s.len() && (bytes[m] == b'"' || bytes[m] == b'\'') {
                            let q = bytes[m];
                            m += 1;
                            while m < s.len() && bytes[m] != q {
                                m += 1;
                            }
                            if m < s.len() {
                                m += 1;
                            }
                        } else {
                            while m < s.len()
                                && !bytes[m].is_ascii_whitespace()
                                && bytes[m] != b'>'
                            {
                                m += 1;
                            }
                        }
                        i = m;
                        continue;
                    }
                }
            }
        }
        let Some(rest) = s.get(i..) else { break };
        let Some(ch) = rest.chars().next() else { break };
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

fn neutralize_scheme_case_insensitive(s: &str, scheme: &str) -> String {
    // Replace every case-variant of `{scheme}:` with `blocked:`.
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    loop {
        let lower = rest.to_lowercase();
        let Some(pos) = lower.find(&format!("{scheme}:")) else {
            out.push_str(rest);
            break;
        };
        out.push_str(rest.get(..pos).unwrap_or(""));
        out.push_str("blocked:");
        rest = rest.get(pos + scheme.len() + 1..).unwrap_or("");
    }
    out
}

/// Hashtag-trend backing (WP6): count tag occurrences across a batch of
/// notes (each note contributes at most one per distinct tag, deleted notes
/// excluded). Adapters share this so local trends and federation-tag
/// ingestion rank identically.
pub fn aggregate_hashtag_counts<'a>(
    notes: impl IntoIterator<Item = &'a Note>,
) -> std::collections::BTreeMap<String, u64> {
    let mut counts = std::collections::BTreeMap::new();
    for note in notes {
        if note.is_deleted {
            continue;
        }
        let mut seen = std::collections::HashSet::new();
        for tag in &note.tags {
            let normalized = tag.to_lowercase();
            if seen.insert(normalized.clone()) {
                *counts.entry(normalized).or_insert(0) += 1;
            }
        }
    }
    counts
}

/// Rank trends: count-descending, name-ascending tiebreak, truncated to
/// `limit`. `limit == 0` means no truncation.
pub fn rank_hashtag_trends(
    counts: &std::collections::BTreeMap<String, u64>,
    limit: usize,
) -> Vec<(String, u64)> {
    let mut ranked: Vec<(String, u64)> =
        counts.iter().map(|(k, v)| (k.clone(), *v)).collect();
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    if limit > 0 && ranked.len() > limit {
        ranked.truncate(limit);
    }
    ranked
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
            reply_parent_uri: None,
            quote: None,
            updated_at_ms: None,
            process_errors: None,
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
    fn poll_stands_in_for_text() {
        let mut n = valid_note();
        n.text = None;
        assert!(n.validate_api_create().is_err());
        assert!(n.validate_api_create_with_poll(true).is_ok());
        assert!(n.validate_storage_with_poll(true).is_ok());
        n.text = Some("   ".to_string());
        assert!(n.validate_api_create().is_err());
        assert!(n.validate_api_create_with_poll(true).is_ok());
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

    /// Slice B: `is_reply` covers resolved and pending replies, but a root
    /// is neither.
    #[test]
    fn is_reply_covers_resolved_and_pending() {
        let root = valid_note();
        assert!(!root.is_reply());
        assert!(!root.has_pending_reply_parent());

        let mut resolved = valid_note();
        resolved.reply_id = Some("nid00000000000002".to_string());
        assert!(resolved.is_reply());
        assert!(!resolved.has_pending_reply_parent());

        let mut pending = valid_note();
        pending.reply_parent_uri = Some("https://remote.example/notes/1".to_string());
        assert!(pending.is_reply());
        assert!(pending.has_pending_reply_parent());
    }

    /// Slice B: a stored note is either linked or pending, never both.
    #[test]
    fn resolved_and_pending_parent_conflict() {
        let mut n = valid_note();
        n.reply_id = Some("nid00000000000002".to_string());
        n.reply_parent_uri = Some("https://remote.example/notes/1".to_string());
        assert!(n.validate_api_create().is_err());
        assert!(n.validate_storage().is_err());

        // Pending alone validates; overlong pending URIs do not.
        n.reply_id = None;
        assert!(n.validate_storage().is_ok());
        n.reply_parent_uri = Some(format!("https://remote.example/{}", "x".repeat(2048)));
        assert!(n.validate_storage().is_err());
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

    #[test]
    fn edit_requires_live_owner_note() {
        let mut n = valid_note();
        assert!(can_edit_note(&n, "aid00000000000001").is_ok());
        assert!(can_edit_note(&n, "aid00000000000009").is_err());
        n.is_deleted = true;
        assert!(can_edit_note(&n, "aid00000000000001").is_err());
    }

    #[test]
    fn delete_allows_owner_or_moderator_on_live_notes() {
        let mut n = valid_note();
        assert!(can_delete_note(&n, "aid00000000000001", false).is_ok());
        assert!(can_delete_note(&n, "aid00000000000009", true).is_ok());
        assert!(can_delete_note(&n, "aid00000000000009", false).is_err());
        n.is_deleted = true;
        assert!(can_delete_note(&n, "aid00000000000001", false).is_err());
        assert!(tombstone_transition(&valid_note()).is_ok());
        assert!(tombstone_transition(&n).is_err());
    }

    #[test]
    fn sanitize_mfm_strips_dangerous_subset_only() {
        // Plain MFM passes through.
        let plain = "**bold** @alice #tag :emoji: https://example.local/x";
        assert_eq!(sanitize_mfm(plain), plain);
        // Script blocks drop (fail-closed on unterminated).
        assert_eq!(sanitize_mfm("a<script>alert(1)</script>b"), "ab");
        assert_eq!(sanitize_mfm("a<SCRIPT SRC=x>evil</SCRIPT>b"), "ab");
        assert_eq!(sanitize_mfm("a<script>alert(1)"), "a");
        // Event handlers drop; other attributes stay.
        assert_eq!(
            sanitize_mfm("<b title=\"hi\" onclick=\"evil()\">x</b>"),
            "<b title=\"hi\">x</b>"
        );
        // Dangerous schemes neutralize (any case).
        assert_eq!(
            sanitize_mfm("[x](JAVASCRIPT:alert(1))"),
            "[x](blocked:alert(1))"
        );
        assert_eq!(sanitize_mfm("![x](data:text/html,evil)"), "![x](blocked:,evil)");
        // Null bytes never survive; sanitize is idempotent.
        let once = sanitize_mfm("a\0<script>x</script>b");
        assert!(!once.contains('\0'));
        assert_eq!(sanitize_mfm(&once), once);
    }

    #[test]
    fn hashtag_trend_counts_and_ranks() {
        let mut a = valid_note();
        a.tags = vec!["Rust".to_string(), "rust".to_string(), "async".to_string()];
        let mut b = valid_note();
        b.tags = vec!["rust".to_string()];
        let mut deleted = valid_note();
        deleted.is_deleted = true;
        deleted.tags = vec!["rust".to_string()];
        let counts = aggregate_hashtag_counts([&a, &b, &deleted]);
        // Per-note dedup: `a` contributes one `rust` despite two spellings.
        assert_eq!(counts.get("rust"), Some(&2));
        assert_eq!(counts.get("async"), Some(&1));
        let ranked = rank_hashtag_trends(&counts, 1);
        assert_eq!(ranked, vec![("rust".to_string(), 2)]);
        let ranked_all = rank_hashtag_trends(&counts, 0);
        assert_eq!(
            ranked_all,
            vec![("rust".to_string(), 2), ("async".to_string(), 1)]
        );
    }
}
