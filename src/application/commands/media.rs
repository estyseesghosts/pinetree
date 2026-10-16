use super::AppServices;

use crate::application::ports::{InsertMediaInput, PortError, TimelineCursor};
use crate::domain::media::{sniff_mime, MediaMeta, MediaProperties};
use crate::media::ingest::{md5_hex, sha256_hex};

/// Misskey `DriveFileEntityService.validateFileName` parity: non-blank,
/// at most 200 chars, no path separators or parent traversal.
fn validate_file_name(name: &str) -> Result<(), PortError> {
    if name.trim().is_empty()
        || name.chars().count() > 200
        || name.contains('\\')
        || name.contains('/')
        || name.contains("..")
    {
        return Err(PortError::Validation("INVALID_FILE_NAME".to_string()));
    }
    Ok(())
}

impl AppServices {
    #[allow(clippy::too_many_arguments)]
    pub fn upload_media(
        &self,
        owner_id: &str,
        name: String,
        bytes: &[u8],
        comment: Option<String>,
        is_sensitive: bool,
        force: bool,
    ) -> Result<MediaMeta, PortError> {
        self.accounts.get_owner(owner_id)?;
        if bytes.len() as u64 > self.max_file_size {
            return Err(PortError::Validation(
                "MAX_FILE_SIZE_EXCEEDED".to_string(),
            ));
        }
        // Slice 8 (D6): both hashes are computed on upload. The MD5 stays
        // the per-owner dedup key (same bytes → same md5 → existing row)
        // and the client-visible packer `md5`; the SHA-256 is the blob
        // content address.
        let md5 = md5_hex(bytes);
        if !force {
            if let Some(existing) = self.media.find_by_owner_and_hash(owner_id, &md5)? {
                return Ok(existing);
            }
        }
        // Slice 7 (H2): the stored MIME comes from the shared sniffer only.
        // The caller-supplied content type is deliberately ignored so a
        // local upload can never store `text/html` (or svg/xml).
        let sniffed = sniff_mime(bytes);
        let media_id = self.ids.generate();
        let url = format!("{}/files/{media_id}", self.public_url.trim_end_matches('/'));
        let blob_key = sha256_hex(bytes);
        self.blobs.put(&blob_key, bytes)?;
        self.media.insert_metadata(InsertMediaInput {
            id: Some(media_id),
            owner_id: Some(owner_id.to_string()),
            name,
            mime: sniffed,
            size_bytes: bytes.len() as u64,
            hash: md5,
            blob_key,
            blurhash: None,
            properties: MediaProperties::default(),
            is_sensitive,
            is_link: false,
            url,
            thumbnail_url: None,
            comment,
            source_url: None,
            max_file_size: self.max_file_size,
        })
    }

    /// Attach one prepared remote-cache row to `note_id` (Phase 2.6 slice 7,
    /// decision D-d). Thin application op over the atomic
    /// `NoteStore::attach_remote_media`: insert + `file_ids` append commit
    /// together, idempotent per (`source_url`, note).
    pub fn attach_remote_media_to_note(
        &self,
        note_id: &str,
        input: InsertMediaInput,
    ) -> Result<MediaMeta, PortError> {
        self.notes.attach_remote_media(note_id, input, Vec::new())
    }

    /// Owner-scoped file read (Phase 3 `drive/files/show`). Anything that is
    /// not the caller's own row — missing, unowned, or a remote-cache row —
    /// is `NO_SUCH_FILE` (profile §3).
    pub fn get_media_for(
        &self,
        caller_id: &str,
        file_id: &str,
    ) -> Result<MediaMeta, PortError> {
        self.accounts.get_owner(caller_id)?;
        match self.media.get(file_id)? {
            Some(meta) if meta.owner_id.as_deref() == Some(caller_id) => Ok(meta),
            _ => Err(PortError::NotFound("NO_SUCH_FILE".to_string())),
        }
    }

    /// Update a file's metadata (Phase 3 `drive/files/update`). Bytes never
    /// change: the row is re-inserted under the same id (`insert_metadata`
    /// upserts), so only `name`/`isSensitive`/`comment` move.
    pub fn update_media(
        &self,
        caller_id: &str,
        file_id: &str,
        name: Option<String>,
        is_sensitive: Option<bool>,
        comment: Option<Option<String>>,
    ) -> Result<MediaMeta, PortError> {
        let mut meta = self.get_media_for(caller_id, file_id)?;
        if let Some(name) = name {
            validate_file_name(&name)?;
            meta.name = name;
        }
        if let Some(is_sensitive) = is_sensitive {
            meta.is_sensitive = is_sensitive;
        }
        if let Some(comment) = comment {
            meta.comment = comment;
        }
        self.media.insert_metadata(InsertMediaInput {
            id: Some(meta.id.clone()),
            owner_id: meta.owner_id.clone(),
            name: meta.name.clone(),
            mime: meta.mime.clone(),
            size_bytes: meta.size_bytes,
            hash: meta.hash.clone(),
            blob_key: meta.blob_key.clone(),
            blurhash: meta.blurhash.clone(),
            properties: meta.properties.clone(),
            is_sensitive: meta.is_sensitive,
            is_link: meta.is_link,
            url: meta.url.clone(),
            thumbnail_url: meta.thumbnail_url.clone(),
            comment: meta.comment.clone(),
            source_url: meta.source_url.clone(),
            max_file_size: self.max_file_size,
        })
    }

    /// Delete a file's metadata row and its blob bytes (Phase 3
    /// `drive/files/delete`). Owner-scoped like [`Self::get_media_for`].
    pub fn delete_media(&self, caller_id: &str, file_id: &str) -> Result<(), PortError> {
        let meta = self.get_media_for(caller_id, file_id)?;
        self.media.delete_metadata(&meta.id)?;
        self.blobs.delete(&meta.blob_key)?;
        Ok(())
    }

    /// Owner's files with `drive/files` list filtering (Phase 3): optional
    /// MIME prefix (`type_prefix`, e.g. `"image/"`), the shared id/date
    /// cursor, and Misskey `sort` (default newest-first; note the Misskey
    /// inversion: `+createdAt` is newest-first, `-createdAt` oldest-first).
    pub fn list_drive_files(
        &self,
        owner_id: &str,
        cursor: &TimelineCursor,
        type_prefix: Option<&str>,
        sort: Option<&str>,
    ) -> Result<Vec<MediaMeta>, PortError> {
        self.accounts.get_owner(owner_id)?;
        let mut files = self.media.list_by_owner(owner_id)?;
        files.retain(|meta| {
            if let Some(prefix) = type_prefix {
                if !meta.mime.starts_with(prefix) {
                    return false;
                }
            }
            if let Some(since_id) = cursor.since_id.as_deref() {
                if meta.id.as_str() <= since_id {
                    return false;
                }
            }
            if let Some(until_id) = cursor.until_id.as_deref() {
                if meta.id.as_str() >= until_id {
                    return false;
                }
            }
            if cursor.since_date.is_some() || cursor.until_date.is_some() {
                if let Ok(ts) = self.ids.parse_time(&meta.id) {
                    if cursor.since_date.is_some_and(|s| ts <= s) {
                        return false;
                    }
                    if cursor.until_date.is_some_and(|u| ts >= u) {
                        return false;
                    }
                }
            }
            true
        });
        files.sort_by(|a, b| b.id.cmp(&a.id));
        match sort {
            None | Some("-createdAt") => {}
            Some("+createdAt") => files.sort_by(|a, b| b.id.cmp(&a.id)),
            Some("-name") => files.sort_by(|a, b| a.name.cmp(&b.name)),
            Some("+name") => files.sort_by(|a, b| b.name.cmp(&a.name)),
            Some("-size") => files.sort_by(|a, b| a.size_bytes.cmp(&b.size_bytes)),
            Some("+size") => files.sort_by(|a, b| b.size_bytes.cmp(&a.size_bytes)),
            // Any other value is rejected by the endpoint before this runs.
            Some(_) => {}
        }
        files.truncate(cursor.limit);
        Ok(files)
    }
}
