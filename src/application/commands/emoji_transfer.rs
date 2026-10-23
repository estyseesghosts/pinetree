//! Custom-emoji export/import jobs (emoji-completion workstream).
//!
//! Owned by the transfer subagent: `handle_custom_emoji_transfer` is the
//! worker entry point for `JobKind::ExportCustomEmojis` (build the instance
//! zip off-request, store it in the caller's drive, fan out the
//! `exportCompleted` Notify follow-up) and `JobKind::ImportCustomEmojis`
//! (validate the caller's drive-file zip and apply its rows through the
//! interactive `create_emoji` op).
//!
//! Ground truth: Misskey `2026.10.1-alpha.0` (`./misskey`, read-only),
//! `exportCustomEmojis` / `importCustomEmojis` queue processors and the
//! `GlobalEventService` broadcast shapes.
//!
//! Upstream export parity: zips ALL local emojis — `meta.json`
//! (`{metaVersion: 2, host, exportedAt, emojis: [{fileName, downloaded,
//! emoji}]}`) plus one file per emoji. Rows whose image bytes are
//! unreachable keep their meta row with `downloaded: false` and contribute
//! no file entry; rows with invalid names (`^[a-zA-Z0-9_]+$`) are skipped
//! entirely. The archive uses stored (level-0) compression, lands in the
//! caller's drive, and completion fans out an `exportCompleted`
//! notification with `exportedEntity: "customEmoji"` verbatim.
//!
//! Upstream import parity: ONLY entries referenced by `meta.json` are
//! extracted, one at a time; per record the existing local same-name row is
//! DELETEd then re-added (name collisions replace). Only `downloaded`
//! records apply. Missing `meta.json` fails the job.
//!
//! Hardening (documented tightenings of upstream, which trusts the archive):
//! total zip bytes cap 32 MiB, entry-count cap 500, per-entry uncompressed
//! size cap 32 MiB checked via `ZipFile::size()` BEFORE extraction,
//! cumulative uncompressed cap 64 MiB (zip-bomb ratio guard),
//! `enclosed_name()` traversal guard, `meta.json` cap 16 MiB (upstream caps
//! it at 64 MiB), fileName must match
//! `^[a-zA-Z0-9_]+(\.[a-zA-Z0-9]+)*$`, emoji names must match
//! `^[a-zA-Z0-9_]+$` (the domain shortcode rule), and every extracted row is
//! sniffed for `image/*` before it is stored.
//!
//! DIVERGENCES from upstream (all stated here):
//! - The export zip is built IN MEMORY (no temp files). The build is bounded
//!   by the admin-curated emoji table: each image is at most `max_file_size`
//!   and each meta row is bounded by the validated emoji lengths
//!   (shortcode <= 128, category <= 128, aliases <= 32, license <= 1024).
//!   Upstream builds in memory too (`adm-zip`), so the bound matches.
//! - The export is content-deduped (`upload_media(force = false)`, unlike
//!   upstream `DriveService.addFile` `force: true`): a crashed-and-retried
//!   export converges on one drive file instead of orphaning a duplicate,
//!   and the Notify follow-up key (`export-custom-emojis:{file_id}`) is
//!   deterministic per converged file.
//! - Upstream's import processor adds rows with no moderator. Pinetree's
//!   `create_emoji`/`delete_emoji` ops always write a moderation-log row,
//!   so the importing user is recorded as the moderator of every applied
//!   row (visible in `admin/show-moderation-logs`).
//! - Archive-level problems (oversized total, too many entries,
//!   unreadable zip, missing/invalid `meta.json`, oversized meta) fail the
//!   job (quarantine — they can never succeed on retry). Per-row problems
//!   (unresolvable entry, bad fileName/emoji name, `downloaded: false`,
//!   non-image bytes, oversize drive write, name conflict races) skip the
//!   row with a log line and count it — the same per-row try/catch
//!   philosophy as the §13 profile imports. Gone users/files converge
//!   quietly (`Ok(None)`), like upstream's early return.

use std::io::{Read, Write};

use crate::application::events::NotifyJobPayload;
use crate::application::ports::{CustomEmojiTransferPayload, JobKind, NewJob, PortError};
use crate::domain::notification::NotificationKind;

use super::AppServices;

/// Largest accepted transfer zip, in bytes (hardening cap; upstream has no
/// total cap on the import side). Unreachable in practice through the drive
/// (drive files are at most `max_file_size`) — defense in depth.
pub const TRANSFER_MAX_ZIP_BYTES: u64 = 32 * 1024 * 1024;
/// Largest accepted entry count (zip-bomb guard; upstream has none).
pub const TRANSFER_MAX_ENTRIES: usize = 500;
/// Largest accepted single entry, uncompressed bytes, checked via
/// `ZipFile::size()` BEFORE any extraction (upstream has none).
pub const TRANSFER_MAX_ENTRY_BYTES: u64 = 32 * 1024 * 1024;
/// Largest accepted cumulative uncompressed total across extracted entries
/// (compression-ratio bomb guard; upstream has none).
pub const TRANSFER_MAX_TOTAL_UNCOMPRESSED_BYTES: u64 = 64 * 1024 * 1024;
/// Largest accepted `meta.json`, in bytes (documented tightening of
/// upstream's 64 MiB cap).
pub const TRANSFER_MAX_META_BYTES: u64 = 16 * 1024 * 1024;
/// Longest accepted zip entry name (drive names cap at 200; keep this below
/// that so imported names always fit the drive).
pub const TRANSFER_MAX_FILE_NAME_CHARS: usize = 128;

/// Worker entry point (see module docs). Returns the `Notify` follow-up for
/// the dispatcher to commit atomically with the job's completion, or `None`
/// when there is nothing to follow up (gone user/file converges quietly).
pub fn handle_custom_emoji_transfer(
    services: &AppServices,
    kind: &JobKind,
    payload: &[u8],
) -> Result<Option<NewJob>, PortError> {
    let parsed: CustomEmojiTransferPayload = serde_json::from_slice(payload).map_err(|e| {
        PortError::Validation(format!("custom emoji transfer payload invalid: {e}"))
    })?;
    match kind {
        JobKind::ExportCustomEmojis => export_custom_emojis(services, &parsed),
        JobKind::ImportCustomEmojis => {
            import_custom_emojis(services, &parsed)?;
            Ok(None)
        }
        _ => Err(PortError::Validation(
            "custom emoji transfer handler called for wrong job kind".to_string(),
        )),
    }
}

/// Run one `ExportCustomEmojis` job: zip every local emoji, store the zip
/// in the caller's drive, and return the `exportCompleted` Notify follow-up
/// (`exportedEntity: "customEmoji"` verbatim). A gone user converges
/// quietly (`Ok(None)`).
fn export_custom_emojis(
    services: &AppServices,
    payload: &CustomEmojiTransferPayload,
) -> Result<Option<NewJob>, PortError> {
    let Some(user) = services.accounts.find_by_id(&payload.user_id)? else {
        return Ok(None);
    };
    let now_ms = services.clock.now_ms();
    let mut rows = services.emojis.all_local_emojis()?;
    rows.sort_by(|a, b| a.shortcode.cmp(&b.shortcode));

    let mut meta_rows = Vec::with_capacity(rows.len());
    // `(file_name, bytes)` pairs in meta-row order; every image is at most
    // `max_file_size` (drive invariant), so the in-memory build stays
    // bounded by the admin-curated table (see module docs).
    let mut files: Vec<(String, Vec<u8>)> = Vec::new();
    for emoji in &rows {
        // Invalid names are skipped entirely (upstream parity); the store
        // enforces the rule, so this only fires on legacy rows.
        if crate::domain::emoji::validate_shortcode(&emoji.shortcode).is_err() {
            continue;
        }
        // Resolve the image bytes through the media row + blob store (same
        // path as `fetch_remote_emoji`). Unreachable bytes keep the meta
        // row with `downloaded: false` and contribute no file entry.
        let mut downloaded = false;
        let mut file_name = format!("{}.png", emoji.shortcode);
        let mut image: Option<Vec<u8>> = None;
        if let Some(media_id) = emoji.media_id.as_deref() {
            if let Some(meta) = services.media.get(media_id)? {
                file_name = format!("{}.{}", emoji.shortcode, ext_for_mime(&meta.mime));
                if let Some(bytes) = services.blobs.get(&meta.blob_key)? {
                    downloaded = true;
                    image = Some(bytes);
                }
            }
        }
        // `EmojiDetailed` pack (upstream `emoji` row parity); the packer
        // falls back to the canonical `/emojis/:name` URL when the drive
        // row is gone, so unreachable rows still serialize.
        let packed = super::emoji::emoji_detailed_value(emoji, services);
        meta_rows.push(serde_json::json!({
            "fileName": file_name,
            "downloaded": downloaded,
            "emoji": packed,
        }));
        if let Some(bytes) = image {
            files.push((file_name, bytes));
        }
    }

    let meta_doc = serde_json::json!({
        "metaVersion": 2,
        "host": public_host(services),
        "exportedAt": crate::activitypub::format_millis(now_ms),
        "emojis": meta_rows,
    });
    let meta_bytes = serde_json::to_vec(&meta_doc).map_err(|e| {
        PortError::StorageUnavailable(format!("emoji export meta serialize failed: {e}"))
    })?;
    // Stored (level-0) compression — upstream zip parity.
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    writer
        .start_file("meta.json", options)
        .map_err(|e| PortError::StorageUnavailable(format!("emoji export zip failed: {e}")))?;
    writer
        .write_all(&meta_bytes)
        .map_err(|e| PortError::StorageUnavailable(format!("emoji export zip failed: {e}")))?;
    for (name, bytes) in &files {
        writer
            .start_file(name, options)
            .map_err(|e| PortError::StorageUnavailable(format!("emoji export zip failed: {e}")))?;
        writer
            .write_all(bytes)
            .map_err(|e| PortError::StorageUnavailable(format!("emoji export zip failed: {e}")))?;
    }
    let zip_bytes = writer
        .finish()
        .map_err(|e| PortError::StorageUnavailable(format!("emoji export zip failed: {e}")))?
        .into_inner();

    let dt: chrono::DateTime<chrono::Utc> =
        chrono::DateTime::from_timestamp_millis(now_ms).unwrap_or_default();
    let name = format!("custom-emojis-{}.zip", dt.format("%Y-%m-%d-%H-%M-%S"));
    // Content-deduped (`force == false`, unlike upstream `force: true`):
    // a crashed-and-retried export converges on one file, and the Notify
    // key below stays deterministic per converged file.
    let file = services.upload_media(&user.id, name, &zip_bytes, None, false, false)?;
    let notify = NotifyJobPayload {
        notifiee_id: user.id.clone(),
        notifier_id: user.id.clone(),
        kind: NotificationKind::ExportCompleted,
        note_id: None,
        reaction: None,
        file_id: Some(file.id.clone()),
        exported_entity: Some("customEmoji".to_string()),
        clip_id: None,
        page_id: None,
    };
    Ok(Some(NewJob::new_json(
        JobKind::Notify,
        &notify,
        format!("export-custom-emojis:{}", file.id),
        now_ms,
    )?))
}

/// One parsed `meta.json` emoji row (unknown fields ignored — forward
/// compatible with upstream's row shape).
#[derive(Debug, serde::Deserialize)]
struct ImportEmojiRow {
    #[serde(rename = "fileName")]
    file_name: String,
    #[serde(default)]
    downloaded: bool,
    emoji: serde_json::Value,
}

/// Run one `ImportCustomEmojis` job: validate the caller's drive-file zip
/// and apply its `meta.json` rows through `delete_emoji` + `create_emoji`
/// (existing local same-name rows are replaced). Gone users/files converge
/// quietly; archive-level problems fail the job; per-row problems skip the
/// row (see module docs).
fn import_custom_emojis(
    services: &AppServices,
    payload: &CustomEmojiTransferPayload,
) -> Result<(), PortError> {
    let Some(user) = services.accounts.find_by_id(&payload.user_id)? else {
        return Ok(());
    };
    let Some(file_id) = payload.file_id.as_deref() else {
        return Err(PortError::Validation(
            "import payload missing fileId".to_string(),
        ));
    };
    let Some(meta) = services.media.get(file_id)? else {
        return Ok(());
    };
    if meta.owner_id.as_deref() != Some(user.id.as_str()) {
        return Ok(());
    }
    let Some(bytes) = services.blobs.get(&meta.blob_key)? else {
        return Ok(());
    };
    if bytes.len() as u64 > TRANSFER_MAX_ZIP_BYTES {
        return Err(PortError::Validation("ZIP_TOO_LARGE".to_string()));
    }
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(&bytes[..]))
        .map_err(|_| PortError::Validation("INVALID_ZIP".to_string()))?;
    if archive.len() > TRANSFER_MAX_ENTRIES {
        return Err(PortError::Validation("TOO_MANY_ENTRIES".to_string()));
    }

    // Locate + read `meta.json` (missing meta fails the job — upstream
    // parity). The size is checked BEFORE extraction (hardening cap).
    let meta_index = (0..archive.len()).find(|&i| {
        archive
            .name_for_index(i)
            .is_some_and(|name| name == "meta.json")
    });
    let Some(meta_index) = meta_index else {
        return Err(PortError::Validation("META_JSON_MISSING".to_string()));
    };
    let meta_text = {
        let mut entry = archive
            .by_index(meta_index)
            .map_err(|_| PortError::Validation("INVALID_ZIP".to_string()))?;
        if entry.size() > TRANSFER_MAX_META_BYTES {
            return Err(PortError::Validation("META_JSON_TOO_LARGE".to_string()));
        }
        let mut text = Vec::new();
        entry
            .read_to_end(&mut text)
            .map_err(|_| PortError::Validation("INVALID_ZIP".to_string()))?;
        text
    };
    let meta_doc: serde_json::Value = serde_json::from_slice(&meta_text)
        .map_err(|_| PortError::Validation("INVALID_META_JSON".to_string()))?;
    let Some(meta_rows) = meta_doc.get("emojis").and_then(|v| v.as_array()) else {
        return Err(PortError::Validation("INVALID_META_JSON".to_string()));
    };

    // Index entry names once (borrow-friendly): only meta-referenced
    // entries are ever extracted (upstream parity).
    let mut names: Vec<String> = Vec::with_capacity(archive.len());
    for i in 0..archive.len() {
        match archive.name_for_index(i) {
            Some(name) => names.push(name.to_string()),
            None => return Err(PortError::Validation("INVALID_ZIP".to_string())),
        }
    }

    let mut applied = 0usize;
    let mut skipped = 0usize;
    let mut uncompressed_total: u64 = 0;
    for entry in meta_rows {
        let row: ImportEmojiRow = match serde_json::from_value(entry.clone()) {
            Ok(row) => row,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };
        // Only `downloaded` records apply (upstream parity).
        if !row.downloaded {
            skipped += 1;
            continue;
        }
        if !valid_import_file_name(&row.file_name) {
            eprintln!(
                "import-custom-emojis: skipping bad fileName {:?}",
                row.file_name
            );
            skipped += 1;
            continue;
        }
        let Some(name) = row
            .emoji
            .get("name")
            .and_then(|v| v.as_str())
            .map(str::to_string)
        else {
            skipped += 1;
            continue;
        };
        // Emoji names follow the domain shortcode rule (upstream
        // `^[a-zA-Z0-9_]+$` parity, enforced by `validate_shortcode`).
        if crate::domain::emoji::validate_shortcode(&name).is_err() {
            eprintln!("import-custom-emojis: skipping bad emoji name {name:?}");
            skipped += 1;
            continue;
        }
        // ONLY meta-referenced entries are extracted, one at a time
        // (upstream parity). Traversal and size guards run BEFORE any
        // bytes are read.
        let Some(index) = names.iter().position(|n| n == &row.file_name) else {
            eprintln!(
                "import-custom-emojis: skipping {name:?}: archive has no {:?}",
                row.file_name
            );
            skipped += 1;
            continue;
        };
        let mut data = Vec::new();
        {
            let mut file = archive
                .by_index(index)
                .map_err(|_| PortError::Validation("INVALID_ZIP".to_string()))?;
            // Traversal guard (defense in depth — the fileName rule above
            // already forbids `/`, so this only fires on hostile archives
            // whose names alias a referenced one).
            if file.enclosed_name().is_none() {
                eprintln!("import-custom-emojis: skipping {name:?}: unenclosed entry name");
                skipped += 1;
                continue;
            }
            if file.size() > TRANSFER_MAX_ENTRY_BYTES {
                eprintln!(
                    "import-custom-emojis: skipping {name:?}: entry too large ({} bytes)",
                    file.size()
                );
                skipped += 1;
                continue;
            }
            uncompressed_total = uncompressed_total.saturating_add(file.size());
            if uncompressed_total > TRANSFER_MAX_TOTAL_UNCOMPRESSED_BYTES {
                return Err(PortError::Validation("ZIP_BOMB".to_string()));
            }
            file.read_to_end(&mut data)
                .map_err(|_| PortError::Validation("INVALID_ZIP".to_string()))?;
        }
        // Image gate (upstream `FILE_TYPE_IMAGE` parity, via the shared
        // sniffer — never store `text/html`).
        if !crate::domain::media::sniff_mime(&data).starts_with("image/") {
            eprintln!("import-custom-emojis: skipping {name:?}: not an image");
            skipped += 1;
            continue;
        }
        // Each added file goes through the drive (`force == false`:
        // content-deduped, so a retried job converges instead of
        // orphaning duplicates). Oversize-for-drive rows skip (the drive
        // cap is per-instance and smaller than the zip caps).
        let drive_file =
            match services.upload_media(&user.id, row.file_name.clone(), &data, None, false, false)
            {
                Ok(file) => file,
                Err(e @ PortError::StorageUnavailable(_)) => return Err(e),
                Err(e) => {
                    eprintln!("import-custom-emojis: skipping {name:?}: drive store failed: {e:?}");
                    skipped += 1;
                    continue;
                }
            };
        // Delete-then-add (upstream parity: name collisions replace).
        // The importing user acts as moderator for both rows (DIVERGENCE:
        // upstream adds with no moderator; pinetree always logs — see
        // module docs).
        if let Some(existing) = services.emojis.get_emoji_by_name(&name)? {
            match services.delete_emoji(&user.id, &existing.id) {
                Ok(()) => {}
                // Raced away concurrently: proceed to the add.
                Err(PortError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        let (category, aliases, license, is_sensitive, local_only, role_ids) =
            import_emoji_params(&row.emoji);
        match services.create_emoji(
            &user.id,
            &name,
            &drive_file.id,
            category,
            aliases,
            license,
            is_sensitive,
            local_only,
            role_ids,
        ) {
            Ok(_) => applied += 1,
            // Storage faults retry the whole job (already-applied rows
            // converge on conflicts on replay — delete-then-add is
            // idempotent).
            Err(e @ PortError::StorageUnavailable(_)) => return Err(e),
            Err(e) => {
                eprintln!("import-custom-emojis: skipping {name:?}: create failed: {e:?}");
                skipped += 1;
            }
        }
    }
    eprintln!(
        "import-custom-emojis for {}: applied={applied} skipped={skipped}",
        user.id,
    );
    Ok(())
}

/// Upstream `fileName` rule (`^[a-zA-Z0-9_]+(\.[a-zA-Z0-9]+)*$`, verbatim):
/// no slashes, so no traversal can pass this check.
fn valid_import_file_name(name: &str) -> bool {
    if name.is_empty() || name.chars().count() > TRANSFER_MAX_FILE_NAME_CHARS {
        return false;
    }
    let mut parts = name.split('.');
    let Some(stem) = parts.next() else {
        return false;
    };
    if stem.is_empty() || !stem.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        return false;
    }
    for ext in parts {
        if ext.is_empty() || !ext.bytes().all(|b| b.is_ascii_alphanumeric()) {
            return false;
        }
    }
    true
}

/// Tolerant per-row emoji params from the `meta.json` `emoji` object
/// (missing keys fall back to the `admin/emoji/add` defaults; overlong or
/// mistyped values fall through to the op, which rejects the row and it is
/// skipped — never a batch failure).
fn import_emoji_params(
    value: &serde_json::Value,
) -> (
    Option<String>,
    Vec<String>,
    Option<String>,
    bool,
    bool,
    Vec<String>,
) {
    let category = value
        .get("category")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let aliases = value
        .get("aliases")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|v| v.as_str())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    let license = value
        .get("license")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let is_sensitive = value
        .get("isSensitive")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let local_only = value
        .get("localOnly")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let role_ids = value
        .get("roleIdsThatCanBeUsedThisEmojiAsReaction")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|v| v.as_str())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    (
        category,
        aliases,
        license,
        is_sensitive,
        local_only,
        role_ids,
    )
}

/// Drive filename extension for a stored MIME (same table as
/// `upload_media_from_url`; unknown types land as `.bin`).
fn ext_for_mime(mime: &str) -> &'static str {
    match mime {
        "image/png" => "png",
        "image/jpeg" => "jpg",
        "image/gif" => "gif",
        "image/webp" => "webp",
        "image/avif" => "avif",
        "video/mp4" => "mp4",
        "video/webm" => "webm",
        _ => "bin",
    }
}

/// Host part of `public_url` (`https://example.local` → `example.local`),
/// lowercased (same rule as the §13 exporter's `self_host`).
fn public_host(services: &AppServices) -> String {
    let url = services.public_url.trim();
    let after_scheme = url.split("://").nth(1).unwrap_or(url);
    after_scheme
        .split('/')
        .next()
        .unwrap_or(after_scheme)
        .to_ascii_lowercase()
}

impl AppServices {
    /// Enqueue one `ExportCustomEmojis` job for `user_id` (any
    /// authenticated caller — self-service like the `i/export-*` family).
    pub fn enqueue_custom_emoji_export(&self, user_id: &str) -> Result<(), PortError> {
        self.accounts.get_owner(user_id)?;
        let now_ms = self.clock.now_ms();
        self.jobs.enqueue_jobs(vec![NewJob::new_json(
            JobKind::ExportCustomEmojis,
            &CustomEmojiTransferPayload {
                user_id: user_id.to_string(),
                file_id: None,
            },
            format!("export-custom-emojis:{user_id}:{now_ms}"),
            now_ms,
        )?])?;
        Ok(())
    }

    /// Endpoint guard for `admin/emoji/import-zip`: the file must exist AND
    /// belong to the caller (`NO_SUCH_FILE`), must be non-empty
    /// (`EMPTY_FILE`), and must fit the transfer zip cap (`TOO_BIG_FILE` —
    /// 32 MiB, not the §13 64 KiB cap, which zips legitimately exceed).
    /// Mapped with `map_import_file_error` at the endpoint.
    pub fn check_custom_emoji_import_file(
        &self,
        user_id: &str,
        file_id: &str,
    ) -> Result<crate::domain::media::MediaMeta, PortError> {
        self.accounts.get_owner(user_id)?;
        let meta = match self.media.get(file_id)? {
            Some(meta) if meta.owner_id.as_deref() == Some(user_id) => meta,
            _ => return Err(PortError::NotFound("NO_SUCH_FILE".to_string())),
        };
        if meta.size_bytes == 0 {
            return Err(PortError::Validation("EMPTY_FILE".to_string()));
        }
        if meta.size_bytes > TRANSFER_MAX_ZIP_BYTES {
            return Err(PortError::Validation("TOO_BIG_FILE".to_string()));
        }
        Ok(meta)
    }

    /// Enqueue one `ImportCustomEmojis` job for `user_id` (endpoint path;
    /// guards ran in [`Self::check_custom_emoji_import_file`] first).
    pub fn enqueue_custom_emoji_import(
        &self,
        user_id: &str,
        file_id: &str,
    ) -> Result<(), PortError> {
        self.accounts.get_owner(user_id)?;
        let now_ms = self.clock.now_ms();
        self.jobs.enqueue_jobs(vec![NewJob::new_json(
            JobKind::ImportCustomEmojis,
            &CustomEmojiTransferPayload {
                user_id: user_id.to_string(),
                file_id: Some(file_id.to_string()),
            },
            format!("import-custom-emojis:{user_id}:{now_ms}"),
            now_ms,
        )?])?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn import_file_names_match_upstream_rule() {
        assert!(valid_import_file_name("blobcat.png"));
        assert!(valid_import_file_name("ABC_123"));
        assert!(valid_import_file_name("a.b.c"));
        assert!(!valid_import_file_name(""));
        assert!(!valid_import_file_name("../evil.png"));
        assert!(!valid_import_file_name("a/b.png"));
        assert!(!valid_import_file_name("has-dash.png"));
        assert!(!valid_import_file_name("has space.png"));
        assert!(!valid_import_file_name(".png"));
        assert!(!valid_import_file_name("a."));
        assert!(!valid_import_file_name("a..png"));
        // Extension segments are alphanumeric only (no underscore).
        assert!(!valid_import_file_name("a.p_ng"));
        assert!(valid_import_file_name("a_.png"));
    }
}
