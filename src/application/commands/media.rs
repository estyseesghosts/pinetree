use super::AppServices;

use crate::application::ports::{CacheStats, InsertMediaInput, PortError, TimelineCursor};
use crate::domain::media::{MediaMeta, MediaProperties, sniff_mime};
use crate::media::ingest::{md5_hex, sanitize_filename, sha256_hex};
use crate::media::remote_cache::validate_remote_media_url;
use crate::media::storage::{acquire_blob_key, acquire_media_op};

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
            return Err(PortError::Validation("MAX_FILE_SIZE_EXCEEDED".to_string()));
        }
        // Slice 8 (D6): both hashes are computed on upload. The MD5 stays
        // the per-owner dedup key (same bytes → same md5 → existing row)
        // and the client-visible packer `md5`; the SHA-256 is the blob
        // content address.
        let md5 = md5_hex(bytes);
        if !force {
            // Dedup hit is free: no quota is charged for an existing row.
            if let Some(existing) = self.media.find_by_owner_and_hash(owner_id, &md5)? {
                return Ok(existing);
            }
        }
        // Best-effort pre-check so the common over-quota case fails BEFORE
        // the durable `blobs.put` below (a breach after `put` would leave
        // orphan bytes for the reconciler to sweep). The in-transaction
        // check inside `insert_metadata_with_durable_quota` is authoritative
        // — this read-then-write gap can only over-admit, never over-reject
        // relative to it, and the atomic method still returns
        // `DRIVE_CAPACITY_EXCEEDED` on a lost race.
        let size_bytes = bytes.len() as u64;
        let used = self.media.durable_bytes(owner_id)?;
        if used.saturating_add(size_bytes) > self.drive_capacity_per_user_bytes {
            return Err(PortError::Validation("DRIVE_CAPACITY_EXCEEDED".to_string()));
        }
        // Slice 7 (H2): the stored MIME comes from the shared sniffer only.
        // The caller-supplied content type is deliberately ignored so a
        // local upload can never store `text/html` (or svg/xml).
        let sniffed = sniff_mime(bytes);
        let media_id = self.ids.generate();
        let url = format!("{}/files/{media_id}", self.public_url.trim_end_matches('/'));
        let blob_key = sha256_hex(bytes);
        // Whole-transition hold (Phase 1 P0): the shared lifecycle gate plus
        // the per-blob-key lock serialize this upload against concurrent
        // deletes, evictions, fetches, and the exclusive orphan sweep. The
        // inner `blobs.put` re-acquire nests safely (counted no-op). Bytes
        // land before the metadata transaction; a crash between them leaves
        // orphan bytes the exclusive sweep collects — never a dangling row.
        let _lifecycle = acquire_media_op("upload_media")?;
        let _key_lock = acquire_blob_key(&blob_key)?;
        self.blobs.put(&blob_key, bytes)?;
        // Quota-checked insert: breach maps to
        // `Validation("DRIVE_CAPACITY_EXCEEDED")` (raised by the store; the
        // pre-check above only avoids creating orphan bytes first).
        // The just-written blob bytes use the durable pool (`self.blobs`,
        // never `cache_blobs`, so remote-cache eviction can never touch
        // local uploads).
        self.media.insert_metadata_with_durable_quota(
            InsertMediaInput {
                id: Some(media_id),
                owner_id: Some(owner_id.to_string()),
                name,
                mime: sniffed,
                size_bytes,
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
            },
            self.drive_capacity_per_user_bytes,
        )
    }

    /// Fetch `url` and store it as the caller's drive file (Batch O WP13
    /// `drive/files/upload-from-url`). Reuses the §2-rule-6 fetcher
    /// (`federation_http`: SSRF guard, timeouts, redirect cap, size cap)
    /// and the existing `upload_media` dedup path, so `force` keeps its
    /// exact local meaning. DIVERGENCES from upstream: the fetch runs
    /// synchronously in-request and the packed file is returned (upstream
    /// returns void + async `urlUploadFinished`); `content-disposition`
    /// filenames are ignored, so the name always comes from the URL leaf.
    pub fn upload_media_from_url(
        &self,
        owner_id: &str,
        url: &str,
        comment: Option<String>,
        is_sensitive: bool,
        force: bool,
    ) -> Result<MediaMeta, PortError> {
        self.accounts.get_owner(owner_id)?;
        validate_remote_media_url(url, &self.federation_http)?;
        let max_bytes = usize::try_from(self.max_file_size).unwrap_or(16 * 1024 * 1024);
        let resp = self
            .federation_http
            .send("GET", url, &[], None, max_bytes)
            .map_err(|e| PortError::Validation(e.to_string()))?;
        if !(200..300).contains(&resp.status) {
            return Err(PortError::Validation(format!(
                "upload-from-url GET returned HTTP {}",
                resp.status
            )));
        }
        if resp.body.is_empty() {
            return Err(PortError::Validation(
                "upload-from-url body is empty".to_string(),
            ));
        }
        if resp.body.len() as u64 > self.max_file_size {
            return Err(PortError::Validation("MAX_FILE_SIZE_EXCEEDED".to_string()));
        }
        let mime = sniff_mime(&resp.body);
        let ext = match mime.as_str() {
            "image/png" => "png",
            "image/jpeg" => "jpg",
            "image/gif" => "gif",
            "image/webp" => "webp",
            "image/avif" => "avif",
            "video/mp4" => "mp4",
            "video/webm" => "webm",
            _ => "bin",
        };
        let url_leaf = url
            .split('?')
            .next()
            .and_then(|p| p.rsplit('/').next())
            .filter(|s| !s.is_empty());
        let name = sanitize_filename(url_leaf, ext);
        // Upstream `DriveService.uploadFromUrl` nulls the comment when it
        // equals the derived name.
        let comment = match comment {
            Some(c) if c == name => None,
            other => other,
        };
        self.upload_media(owner_id, name, &resp.body, comment, is_sensitive, force)
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
    /// is `NO_SUCH_FILE` (profile §3). Grandfathered over-quota reads are
    /// unaffected: no quota check applies on reads.
    pub fn get_media_for(&self, caller_id: &str, file_id: &str) -> Result<MediaMeta, PortError> {
        self.accounts.get_owner(caller_id)?;
        match self.media.get(file_id)? {
            Some(meta) if meta.owner_id.as_deref() == Some(caller_id) => Ok(meta),
            _ => Err(PortError::NotFound("NO_SUCH_FILE".to_string())),
        }
    }

    /// Update a file's metadata (Phase 3 `drive/files/update`). Bytes never
    /// change: the row is re-inserted under the same id (`insert_metadata`
    /// upserts), so only `name`/`isSensitive`/`comment` move. Routed through
    /// the quota-checked insert like the upload path; the byte size is
    /// identical so a correct backend treats the upsert as charge-neutral
    /// (the default impl is a plain upsert with no quota state).
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
        // Bytes never change here, but the upsert still adjusts refcounts
        // transactionally: hold the transition locks for consistency with
        // the upload and delete paths.
        let _lifecycle = acquire_media_op("update_media")?;
        let _key_lock = acquire_blob_key(&meta.blob_key)?;
        self.media.insert_metadata_with_durable_quota(
            InsertMediaInput {
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
            },
            self.drive_capacity_per_user_bytes,
        )
    }

    /// Delete a file's metadata row and — only when no other metadata row
    /// references the same bytes — its durable blob bytes (Phase 3
    /// `drive/files/delete`, plan §2 shared-blob fix). Owner-scoped like
    /// [`Self::get_media_for`]. The in-transaction
    /// `delete_media_record` removes the row and decrements the refcount
    /// plus the owner's durable bytes atomically; the physical `delete`
    /// happens under the same lifecycle gate + per-blob-key serialization
    /// only after rechecking the authoritative refcount, so a concurrent
    /// same-content upload that committed between the record delete and the
    /// byte delete keeps its bytes (deferred GC). Cross-owner identical
    /// bytes (and byte-identical remote rows, which share SHA-256 keys)
    /// survive a sharer's delete. A crash between the record delete and the
    /// blob delete leaves orphan bytes that the reconciler sweeps — never a
    /// dangling reference.
    pub fn delete_media(&self, caller_id: &str, file_id: &str) -> Result<(), PortError> {
        let meta = self.get_media_for(caller_id, file_id)?;
        // Whole-transition hold: serializes against concurrent uploads of
        // identical bytes and against the exclusive orphan sweep.
        let _lifecycle = acquire_media_op("delete_media")?;
        let _key_lock = acquire_blob_key(&meta.blob_key)?;
        match self.media.delete_media_record(&meta.id)? {
            Some((removed, remaining)) if remaining > 0 => {
                debug_assert_eq!(removed.blob_key, meta.blob_key);
                Ok(())
            }
            Some((removed, _)) => {
                // Last reference gone at record-delete time: recheck under
                // the same key serialization before touching bytes (a
                // concurrent upload may have recreated a reference after
                // our transaction committed). Retried deletes after a crash
                // between record-delete and blob-delete converge here too.
                debug_assert_eq!(removed.blob_key, meta.blob_key);
                let _ = self.delete_durable_bytes_if_unreferenced(&removed.blob_key)?;
                Ok(())
            }
            // Already gone between the owner check and the record delete
            // (concurrent delete): converge to success without touching
            // bytes another row may still reference.
            None => Ok(()),
        }
    }

    /// Deferred-GC byte delete: remove the durable object at `blob_key` only
    /// when the authoritative reference count is zero. Holds the shared
    /// lifecycle gate plus the per-blob-key lock, so the check and the delete
    /// are atomic with respect to concurrent uploads, deletes, refetches,
    /// and the exclusive orphan sweep. Returns `true` when no live reference
    /// remains (bytes deleted when present, idempotent when absent) and
    /// `false` when a live row still requires the bytes. Split from
    /// [`Self::delete_media`] so crash-recovery and reconciliation paths can
    /// reuse the exact recheck; nested calls from holders are safe
    /// (reentrant counted no-ops).
    pub fn delete_durable_bytes_if_unreferenced(&self, blob_key: &str) -> Result<bool, PortError> {
        let _lifecycle = acquire_media_op("delete_unreferenced_bytes")?;
        let _key_lock = acquire_blob_key(blob_key)?;
        self.delete_durable_bytes_if_unreferenced_under_guard(blob_key)
    }

    /// [`Self::delete_durable_bytes_if_unreferenced`] without lock
    /// acquisition. The caller must hold the shared lifecycle gate and the
    /// per-blob-key lock for `blob_key`.
    pub fn delete_durable_bytes_if_unreferenced_under_guard(
        &self,
        blob_key: &str,
    ) -> Result<bool, PortError> {
        if self.media.blob_refcount(blob_key)? == 0 {
            self.blobs.delete(blob_key)?;
            Ok(true)
        } else {
            Ok(false)
        }
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

    /// Remote-cache observability snapshot (plan §5.6): totals, per-class
    /// breakdown, in-flight bytes, hits/misses, bytes fetched, evictions and
    /// failed origins — without shell access. Pure passthrough to the media
    /// store's counters.
    pub fn get_cache_stats(&self) -> Result<CacheStats, PortError> {
        self.media.cache_stats()
    }
}
