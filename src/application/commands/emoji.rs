//! Custom emoji admin management + remote-emoji opt-in (Batch N WS1 + WP8b).
//!
//! Reference pins: Misskey `2026.10.1-alpha.0` (`./misskey`).
//! `emojis` (public, `{}`), `emoji {name}`, `admin/emoji/add|update|delete|
//! list` (role `canManageCustomEmoji`, kind `write:admin:emoji` /
//! `read:admin:emoji`). Bulk ops, `import-zip`, `copy`, `list-remote` stay
//! out (guide §8); `export-custom-emojis` has no dedicated endpoint in this
//! pin (parity plan §13) and stays out.
//!
//! Every mutation commits state + moderation-log row (+ any jobs) in one
//! storage transaction. Image bytes stay in the blob store behind the drive
//! `media_id`; the emoji row only references the file.

use super::AppServices;

use crate::application::ports::{NewJob, PortError, TimelineCursor};
use crate::domain::emoji::{validate_shortcode, CustomEmoji, RemoteEmoji};
use crate::domain::moderation::{ModerationAction, ModerationLogEntry};

/// Cap on AP `Emoji` tags ingested per object (WP8b; stated in the session
/// log). Metadata rows are tiny; the cap bounds one object's ingest work.
pub const REMOTE_EMOJI_TAGS_PER_OBJECT: usize = 32;

/// Cap on lazy-fetch jobs enqueued by one opt-in toggle (WP8b; stated in the
/// session log). Keeps the toggle transaction bounded; remaining rows fetch
/// on a later toggle or stay remote-URL packed.
pub const REMOTE_EMOJI_FETCH_PER_TOGGLE: usize = 200;

/// Largest remote-emoji image the fetch job accepts (WP8b §2 rule 6
/// size cap; stated in the session log).
pub const REMOTE_EMOJI_MAX_BYTES: usize = 2 * 1024 * 1024;

impl AppServices {
    fn emoji_log(
        &self,
        moderator_id: &str,
        action: ModerationAction,
        target: Option<&str>,
        details: serde_json::Value,
    ) -> Result<ModerationLogEntry, PortError> {
        ModerationLogEntry::new(
            self.ids.generate(),
            Some(moderator_id.to_string()),
            action,
            target.map(str::to_string),
            details,
            self.now_ms(),
        )
        .map_err(|e| PortError::Validation(format!("{e:?}")))
    }

    /// `admin/emoji/add`: resolve `file_id` (any existing drive file — the
    /// admin curates instance emoji), require an `image/*` MIME (upstream
    /// `FILE_TYPE_IMAGE` parity), enforce the shortcode rule + case-insensitive
    /// uniqueness (`DUPLICATE_NAME` via the store).
    #[allow(clippy::too_many_arguments)]
    pub fn create_emoji(
        &self,
        moderator_id: &str,
        name: &str,
        file_id: &str,
        category: Option<String>,
        aliases: Vec<String>,
        license: Option<String>,
        is_sensitive: bool,
        local_only: bool,
        role_ids: Vec<String>,
    ) -> Result<CustomEmoji, PortError> {
        validate_shortcode(name).map_err(|e| PortError::Validation(format!("{e:?}")))?;
        let meta = match self.media.get(file_id)? {
            Some(m) => m,
            None => return Err(PortError::NotFound("NO_SUCH_FILE".to_string())),
        };
        if !meta.mime.starts_with("image/") {
            return Err(PortError::Validation("UNSUPPORTED_FILE_TYPE".to_string()));
        }
        let emoji = CustomEmoji {
            id: self.ids.generate(),
            shortcode: name.to_string(),
            category: category.filter(|c| !c.is_empty()),
            media_id: Some(meta.id.clone()),
            aliases,
            license: license.filter(|l| !l.is_empty()),
            is_sensitive,
            local_only,
            role_ids,
            created_at_ms: self.now_ms(),
        };
        emoji
            .validate()
            .map_err(|e| PortError::Validation(format!("{e:?}")))?;
        let log = self.emoji_log(
            moderator_id,
            ModerationAction::AddCustomEmoji,
            None,
            serde_json::json!({ "emojiId": emoji.id, "emojiName": emoji.shortcode }),
        )?;
        self.emojis.create_emoji(emoji, log, Vec::new())
    }

    /// `admin/emoji/update`: address by `id` xor `name` (the endpoint resolves
    /// the row first). Only the supplied fields move; `category`/`license`
    /// accept explicit null to reset (upstream `null`-reset parity).
    #[allow(clippy::too_many_arguments)]
    pub fn update_emoji(
        &self,
        moderator_id: &str,
        id: Option<&str>,
        name: Option<&str>,
        file_id: Option<&str>,
        category: Option<Option<String>>,
        aliases: Option<Vec<String>>,
        license: Option<Option<String>>,
        is_sensitive: Option<bool>,
        local_only: Option<bool>,
        role_ids: Option<Vec<String>>,
    ) -> Result<CustomEmoji, PortError> {
        let existing = match (id, name) {
            (Some(id), _) => self.emojis.get_emoji(id)?,
            (None, Some(name)) => self.emojis.get_emoji_by_name(name)?,
            (None, None) => None,
        };
        let Some(mut emoji) = existing else {
            return Err(PortError::NotFound("NO_SUCH_EMOJI".to_string()));
        };
        // Upstream renames when id AND a different name are given
        // (`doNameUpdate`); the store's unique index enforces
        // `SAME_NAME_EMOJI_EXISTS`.
        if let (Some(_), Some(new_name)) = (id, name) {
            if new_name != emoji.shortcode {
                validate_shortcode(new_name).map_err(|e| PortError::Validation(format!("{e:?}")))?;
                emoji.shortcode = new_name.to_string();
            }
        }
        if let Some(file_id) = file_id {
            let meta = match self.media.get(file_id)? {
                Some(m) => m,
                None => return Err(PortError::NotFound("NO_SUCH_FILE".to_string())),
            };
            if !meta.mime.starts_with("image/") {
                return Err(PortError::Validation("UNSUPPORTED_FILE_TYPE".to_string()));
            }
            emoji.media_id = Some(meta.id.clone());
        }
        if let Some(category) = category {
            emoji.category = category.filter(|c| !c.is_empty());
        }
        if let Some(aliases) = aliases {
            emoji.aliases = aliases;
        }
        if let Some(license) = license {
            emoji.license = license.filter(|l| !l.is_empty());
        }
        if let Some(v) = is_sensitive {
            emoji.is_sensitive = v;
        }
        if let Some(v) = local_only {
            emoji.local_only = v;
        }
        if let Some(v) = role_ids {
            emoji.role_ids = v;
        }
        emoji
            .validate()
            .map_err(|e| PortError::Validation(format!("{e:?}")))?;
        let log = self.emoji_log(
            moderator_id,
            ModerationAction::UpdateCustomEmoji,
            None,
            serde_json::json!({ "emojiId": emoji.id, "emojiName": emoji.shortcode }),
        )?;
        match self.emojis.update_emoji(emoji, log, Vec::new()) {
            // The store names the residual race; surface it verbatim.
            Err(PortError::Conflict(code)) => Err(PortError::Conflict(code)),
            other => other,
        }
    }

    /// `admin/emoji/delete` by `id`. Unknown ids are `NO_SUCH_EMOJI`
    /// (upstream parity).
    pub fn delete_emoji(&self, moderator_id: &str, id: &str) -> Result<(), PortError> {
        if self.emojis.get_emoji(id)?.is_none() {
            return Err(PortError::NotFound("NO_SUCH_EMOJI".to_string()));
        }
        let log = self.emoji_log(
            moderator_id,
            ModerationAction::DeleteCustomEmoji,
            None,
            serde_json::json!({ "emojiId": id }),
        )?;
        self.emojis.delete_emoji(id, log, Vec::new())?;
        Ok(())
    }

    /// Ingest AP `Emoji` tag metadata for one remote host (WP8b): upsert the
    /// given pre-parsed `(shortcode, remote_url)` pairs (already capped by
    /// the parser), fetch nothing. Runs after the inbound drop checks and
    /// around the note commit (idempotent metadata-only upserts; a replay
    /// re-upserts harmlessly). Never touches `media_id`.
    pub fn ingest_remote_emoji_tags(
        &self,
        host: &str,
        tags: &[(String, String)],
    ) -> Result<usize, PortError> {
        let host = host.trim().to_ascii_lowercase();
        if host.is_empty() {
            return Ok(0);
        }
        let mut kept = 0usize;
        for (shortcode, url) in tags {
            // SSRF discipline at ingest: only http(s) icon URLs are stored
            // (the domain constructor enforces it); anything else is skipped.
            let Ok(row) = RemoteEmoji::new(&host, shortcode, url, self.now_ms()) else {
                continue;
            };
            self.emojis.upsert_remote_emoji(row)?;
            kept += 1;
        }
        Ok(kept)
    }

    /// Run one `RemoteEmojiFetch` job (WP8b lazy fetch): download the
    /// remote icon through the SSRF-guarded fetcher, require an `image/*`
    /// content type and the size cap, store the bytes + a drive metadata
    /// row, and record the local copy on the remote row. Converges quietly
    /// when the row is gone/already cached or the fetch is unusable (the
    /// packed URL keeps pointing at the remote host); only storage faults
    /// propagate (job retries).
    pub fn fetch_remote_emoji(&self, host: &str, shortcode: &str) -> Result<(), PortError> {
        let Some(row) = self.emojis.get_remote_emoji(host, shortcode)? else {
            return Ok(());
        };
        if row.media_id.is_some() {
            return Ok(());
        }
        let resp = match self.federation_http.send(
            "GET",
            &row.remote_url,
            &[],
            None,
            REMOTE_EMOJI_MAX_BYTES,
        ) {
            Ok(resp) => resp,
            Err(_) => return Ok(()),
        };
        if resp.status != 200 {
            return Ok(());
        }
        if resp.body.len() > REMOTE_EMOJI_MAX_BYTES {
            return Ok(());
        }
        // Content-type gate (upstream `FILE_TYPE_IMAGE` parity) plus the
        // shared sniffer (same rule as uploads: never store `text/html`).
        let content_type = resp.header("content-type").unwrap_or("").to_ascii_lowercase();
        if !content_type.starts_with("image/") {
            return Ok(());
        }
        let sniffed = crate::domain::media::sniff_mime(&resp.body);
        if !sniffed.starts_with("image/") {
            return Ok(());
        }
        let media_id = self.ids.generate();
        let url = format!("{}/files/{media_id}", self.public_url.trim_end_matches('/'));
        self.blobs
            .put(&crate::media::ingest::sha256_hex(&resp.body), &resp.body)?;
        self.media.insert_metadata(crate::application::ports::InsertMediaInput {
            id: Some(media_id.clone()),
            owner_id: None,
            name: shortcode.to_string(),
            mime: sniffed,
            size_bytes: resp.body.len() as u64,
            hash: crate::media::ingest::md5_hex(&resp.body),
            blob_key: crate::media::ingest::sha256_hex(&resp.body),
            blurhash: None,
            properties: crate::domain::media::MediaProperties::default(),
            is_sensitive: false,
            is_link: false,
            url,
            thumbnail_url: None,
            comment: None,
            source_url: Some(row.remote_url.clone()),
            max_file_size: self.max_file_size,
        })?;
        self.emojis.set_remote_emoji_media(&row.host, &row.shortcode, &media_id)?;
        Ok(())
    }

    /// Enqueue one bounded `RemoteEmojiFetch` job per known remote-emoji row
    /// lacking a cached copy (WP8b toggle-on fan-out). Bounded by
    /// [`REMOTE_EMOJI_FETCH_PER_TOGGLE`]; the toggle transaction stays small.
    pub fn enqueue_remote_emoji_fetches(&self) -> Result<usize, PortError> {
        let mut jobs = Vec::new();
        for row in self.emojis.all_remote_emojis()? {
            if row.media_id.is_some() {
                continue;
            }
            if jobs.len() >= REMOTE_EMOJI_FETCH_PER_TOGGLE {
                break;
            }
            jobs.push(NewJob::new_json(
                crate::application::ports::JobKind::RemoteEmojiFetch,
                &crate::application::ports::RemoteEmojiFetchPayload {
                    host: row.host.clone(),
                    shortcode: row.shortcode.clone(),
                },
                format!("remote-emoji:{}:{}:{}", row.host, row.shortcode, self.ids.generate()),
                self.now_ms(),
            )?);
            if jobs.len() >= crate::application::ports::MAX_JOBS_PER_TXN {
                break;
            }
        }
        let n = jobs.len();
        if n > 0 {
            self.jobs.enqueue_jobs(jobs)?;
        }
        Ok(n)
    }
}
