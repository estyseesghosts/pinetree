use super::AppServices;

use crate::application::ports::{InsertMediaInput, PortError};
use crate::domain::media::{sniff_mime, MediaMeta, MediaProperties};
use crate::media::ingest::{md5_hex, sha256_hex};

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
}
