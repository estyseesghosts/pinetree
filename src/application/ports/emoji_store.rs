use super::job_queue::NewJob;
use super::note_store::TimelineCursor;
use crate::domain::id::PortError;
use crate::domain::moderation::ModerationLogEntry;

/// Custom + remote emoji (Batch N WS1 + WP8b; Misskey `MiEmoji` parity:
/// local rows unique on case-insensitive shortcode, remote rows unique on
/// (lower host, lower shortcode)).
pub trait EmojiStore: Send + Sync {
    fn create_emoji(
        &self,
        emoji: crate::domain::emoji::CustomEmoji,
        log: ModerationLogEntry,
        jobs: Vec<NewJob>,
    ) -> Result<crate::domain::emoji::CustomEmoji, PortError>;
    fn update_emoji(
        &self,
        emoji: crate::domain::emoji::CustomEmoji,
        log: ModerationLogEntry,
        jobs: Vec<NewJob>,
    ) -> Result<crate::domain::emoji::CustomEmoji, PortError>;
    fn delete_emoji(
        &self,
        id: &str,
        log: ModerationLogEntry,
        jobs: Vec<NewJob>,
    ) -> Result<bool, PortError>;
    fn get_emoji(&self, id: &str) -> Result<Option<crate::domain::emoji::CustomEmoji>, PortError>;
    fn get_emoji_by_name(
        &self,
        shortcode: &str,
    ) -> Result<Option<crate::domain::emoji::CustomEmoji>, PortError>;
    /// Local emoji, cursor-paged (`limit` default 10, 1..=100), newest-first,
    /// with optional case-insensitive substring `query` over
    /// name/aliases/category or exact `:name:` tokens (upstream list parity).
    fn list_emojis(
        &self,
        query: Option<&str>,
        cursor: &TimelineCursor,
    ) -> Result<Vec<crate::domain::emoji::CustomEmoji>, PortError>;
    /// Every local emoji (pack path; single-node scan, bounded by the admin
    /// list — no per-note queries).
    fn all_local_emojis(&self) -> Result<Vec<crate::domain::emoji::CustomEmoji>, PortError>;
    /// Metadata-only remote-emoji upsert (AP `Emoji` tag ingest; no fetch).
    /// Updates `remote_url` when the (host, shortcode) row exists, inserts
    /// otherwise. Never touches `media_id`.
    fn upsert_remote_emoji(
        &self,
        emoji: crate::domain::emoji::RemoteEmoji,
    ) -> Result<crate::domain::emoji::RemoteEmoji, PortError>;
    fn get_remote_emoji(
        &self,
        host: &str,
        shortcode: &str,
    ) -> Result<Option<crate::domain::emoji::RemoteEmoji>, PortError>;
    /// Every known remote-emoji metadata row (lazy-fetch fan-out source).
    fn all_remote_emojis(&self) -> Result<Vec<crate::domain::emoji::RemoteEmoji>, PortError>;
    /// Remote emoji, newest-first by `first_seen_ms`. `host`: `None` lists
    /// every remote row (upstream `list-remote` with `host: null`); `Some(h)`
    /// restricts to that host (case-insensitive exact). `query` is the same
    /// name-or-`:name:` filter as [`EmojiStore::list_emojis`] (remote rows
    /// carry no aliases/category, so it matches the shortcode). `limit`
    /// (default 10, 1..=100) and `since_date`/`until_date` apply;
    /// `since_id`/`until_id` are accepted and ignored (remote rows have no
    /// time-ordered ids — documented DIVERGENCE from upstream pagination).
    fn list_remote_emojis(
        &self,
        query: Option<&str>,
        host: Option<&str>,
        cursor: &TimelineCursor,
    ) -> Result<Vec<crate::domain::emoji::RemoteEmoji>, PortError>;
    /// Record the lazily fetched local copy (`media_id`) for one remote row.
    fn set_remote_emoji_media(
        &self,
        host: &str,
        shortcode: &str,
        media_id: &str,
    ) -> Result<(), PortError>;
}
