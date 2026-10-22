use super::fetch::{DEFAULT_VARIANT, fetch_and_index_remote_media};
use crate::activitypub::objects::ParsedAttachment;
use crate::application::commands::AppServices;
use crate::application::ports::{
    InsertMediaInput, JobKind, NewJob, PortError, RemoteMediaAttachment, RemoteMediaFetchPayload,
};
use crate::config::RemoteCacheMode;
use crate::domain::note::MAX_FILE_IDS;

/// Maximum attachments carried per note (Misskey parity; matches the
/// `Note::file_ids` domain bound). Longer lists are truncated, keeping
/// document order. Applied both when the inbox builds the fetch job and
/// defensively when the worker runs it.
pub const MAX_REMOTE_ATTACHMENTS_PER_NOTE: usize = MAX_FILE_IDS;

/// Cumulative fetch budget per note per job run (64 MiB). Attachment sizes
/// are unknown before fetching, so the worker stops starting new fetches
/// once the budget is exhausted; the first attachment is always attempted
/// so a single large file still caches. Per-file `max_file_size` still
/// applies to every fetch.
pub const MAX_REMOTE_MEDIA_TOTAL_BYTES_PER_NOTE: u64 = 64 * 1024 * 1024;

impl From<&ParsedAttachment> for RemoteMediaAttachment {
    fn from(att: &ParsedAttachment) -> Self {
        Self {
            url: att.url.clone(),
            media_type: att.media_type.clone(),
            name: att.name.clone(),
            is_sensitive: att.is_sensitive,
            blurhash: att.blurhash.clone(),
            width: att.width,
            height: att.height,
        }
    }
}

/// Build the durable fetch job for a note's attachments, or `None` when
/// there is nothing to fetch. Truncates to
/// [`MAX_REMOTE_ATTACHMENTS_PER_NOTE`]. The payload's `note_id` is the
/// `"__PENDING_NOTE_ID__"` placeholder: it is substituted with the
/// committed note id inside the `record_note_activity` transaction, so the
/// job is enqueued atomically with the note (a replay enqueues nothing).
pub fn build_remote_media_fetch_job(
    services: &AppServices,
    activity_id: &str,
    attachments: &[ParsedAttachment],
) -> Result<Option<NewJob>, PortError> {
    if attachments.is_empty() {
        return Ok(None);
    }
    let kept: Vec<RemoteMediaAttachment> = attachments
        .iter()
        .take(MAX_REMOTE_ATTACHMENTS_PER_NOTE)
        .map(RemoteMediaAttachment::from)
        .collect();
    let payload = RemoteMediaFetchPayload {
        note_id: "__PENDING_NOTE_ID__".to_string(),
        attachments: kept,
    };
    let job = NewJob::new_json(
        JobKind::RemoteMediaFetch,
        &payload,
        format!("remote-media:{activity_id}"),
        services.clock.now_ms(),
    )?;
    Ok(Some(job))
}

/// Worker entry point for a `RemoteMediaFetch` job: fetch every attachment
/// and attach the results to the note. Best-effort per attachment
/// (unreachable/oversize/invalid URLs are skipped); storage faults
/// propagate so the worker retries the whole job, which re-skips the URLs
/// it already attached. Returns `Ok` when there is nothing left to do,
/// including when the note was deleted meanwhile.
///
/// Warming path: skipped entirely while the cache mode is `Disabled`.
/// Relay exclusion (plan §5.5) is enforced by the caller that builds the
/// job, not here — this function fetches exactly what the payload lists.
pub fn process_remote_media_fetch(
    services: &AppServices,
    payload: &RemoteMediaFetchPayload,
) -> Result<(), PortError> {
    if services.remote_cache_mode == RemoteCacheMode::Disabled {
        return Ok(());
    }
    let mut total_bytes: u64 = 0;
    let mut attempted: usize = 0;
    for attachment in payload
        .attachments
        .iter()
        .take(MAX_REMOTE_ATTACHMENTS_PER_NOTE)
    {
        let note = match services.notes.get_note(&payload.note_id)? {
            Some(note) => note,
            None => return Ok(()),
        };
        // Idempotency pre-check: already attached for this URL → no fetch.
        if !attachment.url.trim().is_empty() {
            let existing = services.media.find_by_source_url(attachment.url.trim())?;
            if existing.iter().any(|m| note.file_ids.contains(&m.id)) {
                continue;
            }
        }
        // Per-note byte budget (first attachment always attempted).
        if attempted > 0 && total_bytes >= MAX_REMOTE_MEDIA_TOTAL_BYTES_PER_NOTE {
            break;
        }
        match fetch_and_attach_remote_media(services, &payload.note_id, attachment) {
            Ok(Some(size)) => {
                total_bytes = total_bytes.saturating_add(size);
                attempted += 1;
            }
            // Skipped (note gone, duplicate, over budget): nothing to do.
            Ok(None) => {}
            // Best-effort: a bad attachment never fails the job.
            Err(PortError::Validation(_)) => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Fetch one attachment into the cache pool and attach a per-note row to
/// the note. Returns the attached size, or `None` when the fetch was
/// legitimately skipped. `Validation` means "skip this attachment"; any
/// other error is a storage fault the caller retries.
fn fetch_and_attach_remote_media(
    services: &AppServices,
    note_id: &str,
    attachment: &RemoteMediaAttachment,
) -> Result<Option<u64>, PortError> {
    if services.notes.get_note(note_id)?.is_none() {
        return Ok(None);
    }
    let comment = attachment
        .name
        .as_deref()
        .map(|c| c.chars().take(512).collect());
    let canonical = fetch_and_index_remote_media(
        services,
        &services.federation_http,
        attachment.url.trim(),
        DEFAULT_VARIANT,
        None,
        attachment.name.as_deref(),
        comment,
        attachment.is_sensitive,
        attachment.blurhash.clone(),
        attachment.width,
        attachment.height,
    )?;
    // Per-note row sharing the canonical bytes (new id + note-local URL,
    // same blob key; the backend refcounts the key). No cache accounting
    // here: the bytes were already counted once by the fetch core. The
    // whole-transition hold serializes this insert against concurrent
    // deletes, evictions, and the exclusive sweep for the same key.
    let _lifecycle = crate::media::storage::acquire_media_op("fetch_attach")?;
    let _key_lock = crate::media::storage::acquire_blob_key(&canonical.blob_key)?;
    let media_id = services.ids.generate();
    let local_url = format!(
        "{}/files/{media_id}",
        services.public_url.trim_end_matches('/')
    );
    let input = InsertMediaInput {
        id: Some(media_id),
        owner_id: None,
        name: canonical.name.clone(),
        mime: canonical.mime.clone(),
        size_bytes: canonical.size_bytes,
        hash: canonical.hash.clone(),
        blob_key: canonical.blob_key.clone(),
        blurhash: canonical.blurhash.clone(),
        properties: canonical.properties.clone(),
        is_sensitive: canonical.is_sensitive,
        is_link: true,
        url: local_url,
        thumbnail_url: None,
        comment: canonical.comment.clone(),
        source_url: Some(attachment.url.clone()),
        max_file_size: services.max_file_size,
    };
    match services.attach_remote_media_to_note(note_id, input) {
        Ok(meta) => Ok(Some(meta.size_bytes)),
        // The note was deleted between the check above and the attach:
        // drop the fetch (the canonical row + bytes stay cached for
        // re-fetch; nothing references them from this note).
        Err(PortError::NotFound(_)) => Ok(None),
        // The note cannot take more files: stop, do not fail the job.
        Err(PortError::Validation(msg)) if msg == "TOO_MANY_ATTACHMENTS" => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::super::support::{attachment_for, test_services};
    use super::*;
    use crate::application::ports::{JobKind, RemoteMediaFetchPayload};

    #[test]
    fn remote_media_fetch_job_truncates_to_16() {
        let services = test_services("slice7-truncate", 4096);
        assert!(
            build_remote_media_fetch_job(&services, "act:empty", &[])
                .unwrap()
                .is_none()
        );
        let attachments: Vec<ParsedAttachment> = (0..20)
            .map(|i| attachment_for(&format!("http://remote.invalid/{i}.png"), Some("image/png")))
            .collect();
        let job = build_remote_media_fetch_job(&services, "act:big", &attachments)
            .unwrap()
            .expect("non-empty attachments build a job");
        assert_eq!(job.kind, JobKind::RemoteMediaFetch);
        let payload: RemoteMediaFetchPayload =
            serde_json::from_slice(&job.payload).expect("payload deserializes");
        assert_eq!(
            payload.attachments.len(),
            MAX_REMOTE_ATTACHMENTS_PER_NOTE,
            ">16 attachments are truncated, keeping document order"
        );
        assert_eq!(payload.attachments[0].url, "http://remote.invalid/0.png");
        assert_eq!(payload.attachments[15].url, "http://remote.invalid/15.png");
    }
}
