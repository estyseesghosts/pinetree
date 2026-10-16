//! Remote media URL validation, downloading, and caching (Phase 2 Slice C2).
//!
//! Validates remote attachment URLs (scheme `http`/`https`, SSRF check,
//! size limit, MIME sniffing) before fetching/caching; stores metadata in
//! `MediaStore` (`owner_id = None`, `is_link = true`) and raw bytes in
//! `BlobStore` outside the database.
//!
//! Phase 2.6 slice 7 (H2+C3, decision D-d): the inbox no longer fetches
//! attachments inline. It enqueues one durable [`JobKind::RemoteMediaFetch`]
//! job per note (atomically with `record_note_activity`); the worker runs
//! [`process_remote_media_fetch`], which is idempotent per (remote URL,
//! note) and best-effort per attachment.

use crate::activitypub::objects::ParsedAttachment;
use crate::application::commands::AppServices;
use crate::application::ports::{
    InsertMediaInput, JobKind, NewJob, PortError, RemoteMediaAttachment, RemoteMediaFetchPayload,
};
use crate::domain::media::{sniff_mime, MediaMeta, MediaProperties};
use crate::domain::note::MAX_FILE_IDS;
use crate::federation::delivery::{SafeHttpClient, parse_http_url, resolve_and_validate_url};
use crate::media::ingest::{md5_hex, sha256_hex, sanitize_filename};

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

/// Validate a remote media URL before any network fetch occurs.
pub fn validate_remote_media_url(
    url: &str,
    client: &SafeHttpClient,
) -> Result<(), PortError> {
    let parsed = parse_http_url(url).map_err(PortError::Validation)?;
    if parsed.scheme != "http" && parsed.scheme != "https" {
        return Err(PortError::Validation(format!(
            "unsupported remote media scheme: {}",
            parsed.scheme
        )));
    }
    if url.chars().count() > 1024 {
        return Err(PortError::Validation(
            "remote media URL exceeds 1024 chars".to_string(),
        ));
    }
    resolve_and_validate_url(url, &client.config)
        .map_err(|e| PortError::Validation(e.to_string()))?;
    Ok(())
}

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
pub fn process_remote_media_fetch(
    services: &AppServices,
    payload: &RemoteMediaFetchPayload,
) -> Result<(), PortError> {
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

/// Fetch one attachment and attach it to the note. Returns the attached
/// size, or `None` when the fetch was legitimately skipped. `Validation`
/// means "skip this attachment"; any other error is a storage fault the
/// caller retries.
fn fetch_and_attach_remote_media(
    services: &AppServices,
    note_id: &str,
    attachment: &RemoteMediaAttachment,
) -> Result<Option<u64>, PortError> {
    if services.notes.get_note(note_id)?.is_none() {
        return Ok(None);
    }
    let parsed = ParsedAttachment {
        url: attachment.url.clone(),
        media_type: attachment.media_type.clone(),
        name: attachment.name.clone(),
        is_sensitive: attachment.is_sensitive,
        blurhash: attachment.blurhash.clone(),
        width: attachment.width,
        height: attachment.height,
    };
    let prepared = download_and_sniff(services, &services.federation_http, &parsed)?;
    services.blobs.put(&prepared.blob_key, &prepared.bytes)?;
    let media_id = services.ids.generate();
    let local_url = format!(
        "{}/files/{media_id}",
        services.public_url.trim_end_matches('/')
    );
    let input = InsertMediaInput {
        id: Some(media_id),
        owner_id: None,
        name: prepared.name,
        mime: prepared.mime,
        size_bytes: prepared.bytes.len() as u64,
        hash: prepared.hash,
        blob_key: prepared.blob_key,
        blurhash: prepared.blurhash,
        properties: prepared.properties,
        is_sensitive: prepared.is_sensitive,
        is_link: true,
        url: local_url,
        thumbnail_url: None,
        comment: prepared.comment,
        source_url: Some(parsed.url.clone()),
        max_file_size: services.max_file_size,
    };
    match services.attach_remote_media_to_note(note_id, input) {
        Ok(meta) => Ok(Some(meta.size_bytes)),
        // The note was deleted between the check above and the attach:
        // drop the fetch (the blob bytes are inert without a meta row).
        Err(PortError::NotFound(_)) => Ok(None),
        // The note cannot take more files: stop, do not fail the job.
        Err(PortError::Validation(msg)) if msg == "TOO_MANY_ATTACHMENTS" => Ok(None),
        Err(e) => Err(e),
    }
}

/// Fetched bytes plus the sniffed metadata, with no store writes yet.
struct PreparedRemoteMedia {
    bytes: Vec<u8>,
    mime: String,
    name: String,
    comment: Option<String>,
    /// MD5: stays the client-visible `MediaMeta.hash` (packer `md5`).
    hash: String,
    /// SHA-256 hex: the `BlobStore` content address (slice 8).
    blob_key: String,
    blurhash: Option<String>,
    properties: MediaProperties,
    is_sensitive: bool,
}

/// Validate, fetch, and sniff a remote attachment. The stored MIME comes
/// from the shared sniffer ONLY: the declared `mediaType` and the response
/// `Content-Type` are deliberately ignored (slice 7, H2), so neither can
/// introduce `text/html`, `image/svg+xml` or any other active type.
fn download_and_sniff(
    services: &AppServices,
    client: &SafeHttpClient,
    attachment: &ParsedAttachment,
) -> Result<PreparedRemoteMedia, PortError> {
    validate_remote_media_url(&attachment.url, client)?;

    let max_bytes = usize::try_from(services.max_file_size).unwrap_or(16 * 1024 * 1024);
    let resp = client
        .send("GET", &attachment.url, &[], None, max_bytes)
        .map_err(|e| PortError::Validation(e.to_string()))?;
    if !(200..300).contains(&resp.status) {
        return Err(PortError::Validation(format!(
            "remote media GET {} returned HTTP {}",
            attachment.url, resp.status
        )));
    }
    if resp.body.is_empty() {
        return Err(PortError::Validation(
            "remote media body is empty".to_string(),
        ));
    }
    if resp.body.len() as u64 > services.max_file_size {
        return Err(PortError::Validation(
            "MAX_FILE_SIZE_EXCEEDED".to_string(),
        ));
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
        "video/mpeg" => "mpg",
        "audio/mpeg" => "mp3",
        "audio/ogg" => "ogg",
        "audio/wav" => "wav",
        "text/plain" => "txt",
        _ => "bin",
    };

    let url_leaf = parse_http_url(&attachment.url)
        .ok()
        .and_then(|u| {
            u.path_and_query
                .split('?')
                .next()
                .and_then(|p| p.rsplit('/').next())
                .filter(|s| !s.is_empty())
                .map(ToOwned::to_owned)
        });
    let name = sanitize_filename(
        attachment.name.as_deref().or(url_leaf.as_deref()),
        ext,
    );
    let comment = attachment
        .name
        .as_deref()
        .map(|c| c.chars().take(512).collect());

    let hash = md5_hex(&resp.body);
    let blob_key = sha256_hex(&resp.body);
    Ok(PreparedRemoteMedia {
        bytes: resp.body,
        mime,
        name,
        comment,
        hash,
        blob_key,
        blurhash: attachment.blurhash.clone(),
        properties: MediaProperties {
            width: attachment.width,
            height: attachment.height,
            orientation: None,
            avg_color: None,
        },
        is_sensitive: attachment.is_sensitive,
    })
}

/// Validate, fetch, and cache a remote attachment into `BlobStore` +
/// `MediaStore` (standalone, without attaching to a note).
///
/// The raw bytes are written to `BlobStore` (filesystem) and never stored inside
/// the database transaction (`MediaStore` only stores `MediaMeta`).
pub fn fetch_and_cache_remote_media(
    services: &AppServices,
    client: &SafeHttpClient,
    attachment: &ParsedAttachment,
) -> Result<MediaMeta, PortError> {
    let prepared = download_and_sniff(services, client, attachment)?;
    services.blobs.put(&prepared.blob_key, &prepared.bytes)?;

    let media_id = services.ids.generate();
    let local_url = format!(
        "{}/files/{media_id}",
        services.public_url.trim_end_matches('/')
    );
    services.media.insert_metadata(InsertMediaInput {
        id: Some(media_id),
        owner_id: None,
        name: prepared.name,
        mime: prepared.mime,
        size_bytes: prepared.bytes.len() as u64,
        hash: prepared.hash,
        blob_key: prepared.blob_key,
        blurhash: prepared.blurhash,
        properties: prepared.properties,
        is_sensitive: prepared.is_sensitive,
        is_link: true,
        url: local_url,
        thumbnail_url: None,
        comment: prepared.comment,
        source_url: Some(attachment.url.clone()),
        max_file_size: services.max_file_size,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::ports::{ActivityDedup, NoopStreamingSink};
    use crate::domain::id::{AidxGenerator, Clock, IdGenerator, ManualClock};
    use crate::federation::delivery::SafeHttpConfig;
    use crate::media::storage::FsBlobStore;
    use crate::persistence::redb::RedbStore;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn test_services(prefix: &str, max_file_size: u64) -> AppServices {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pinetree-media-cache-{}-{prefix}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(1_760_400_000_000));
        let ids: Arc<dyn IdGenerator> = Arc::new(AidxGenerator::new(clock.clone()));
        let store = RedbStore::open(dir.join("test.redb"), clock.clone(), ids.clone()).unwrap();
        let blobs = Arc::new(FsBlobStore::new(dir.join("blobs")).unwrap());
        AppServices {
            clock,
            ids,
            accounts: store.clone(),
            notes: store.clone(),
            follows: store.clone(),
            blocks: store.clone(),
            mutes: store.clone(),
            reactions: store.clone(),
            polls: store.clone(),
            jobs: store.clone(),
            media: store.clone(),
            blobs,
            notifications: store.clone(),
            auth: store.clone(),
            streaming: Arc::new(NoopStreamingSink),
            dedup: store.clone() as Arc<dyn ActivityDedup>,
            federation_http: SafeHttpClient::new(SafeHttpConfig {
                allow_private_ips: true,
                ..Default::default()
            }),
            public_url: "https://pinetree.local".to_string(),
            max_file_size,
            preserved_usernames: Vec::new(),
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
            note_draft_limit: crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT,
            scheduled_note_limit: crate::domain::draft::DEFAULT_SCHEDULED_NOTE_LIMIT,
            pin_limit: crate::domain::pinning::DEFAULT_PIN_LIMIT,
        }
    }

    #[test]
    fn remote_media_ssrf_and_fetch_cache_roundtrip() {
        let services = test_services("cache", 4096);
        let strict_client = SafeHttpClient::new(SafeHttpConfig {
            allow_private_ips: false,
            ..Default::default()
        });
        // Private IP rejected before any network fetch.
        let bad_att = ParsedAttachment {
            url: "http://127.0.0.1:9999/secret.png".to_string(),
            media_type: Some("image/png".to_string()),
            name: Some("secret.png".to_string()),
            is_sensitive: false,
            blurhash: None,
            width: None,
            height: None,
        };
        assert!(fetch_and_cache_remote_media(&services, &strict_client, &bad_att).is_err());

        // Start a local test HTTP server serving a valid PNG and an oversized payload.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let png_bytes = vec![
            0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 1, 2, 3, 4, 5, 6, 7, 8,
        ];
        let png_clone = png_bytes.clone();
        let handle = std::thread::spawn(move || {
            for _ in 0..2 {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                let mut buf = [0u8; 1024];
                let n = stream.read(&mut buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                if req.starts_with("GET /pic.png") {
                    let hdr = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: image/png\r\nContent-Length: {}\r\n\r\n",
                        png_clone.len()
                    );
                    let _ = stream.write_all(hdr.as_bytes());
                    let _ = stream.write_all(&png_clone);
                } else {
                    let big = vec![0x89; 8192];
                    let hdr = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\n\r\n",
                        big.len()
                    );
                    let _ = stream.write_all(hdr.as_bytes());
                    let _ = stream.write_all(&big);
                }
            }
        });

        let local_client = SafeHttpClient::new(SafeHttpConfig {
            allow_private_ips: true,
            ..Default::default()
        });
        let good_att = ParsedAttachment {
            url: format!("http://{addr}/pic.png"),
            media_type: Some("image/png".to_string()),
            name: Some("Alt text".to_string()),
            is_sensitive: true,
            blurhash: None,
            width: Some(64),
            height: Some(64),
        };
        let meta = fetch_and_cache_remote_media(&services, &local_client, &good_att).unwrap();
        assert_eq!(meta.owner_id, None);
        assert!(meta.is_link);
        assert!(meta.is_sensitive);
        assert_eq!(meta.mime, "image/png");
        assert_eq!(meta.size_bytes, png_bytes.len() as u64);
        // Slice 8 (D6): the blob lives under the SHA-256 key while the
        // meta keeps the md5; both are the real digests of the fetched bytes.
        assert_eq!(meta.hash, md5_hex(&png_bytes));
        assert_eq!(meta.blob_key, sha256_hex(&png_bytes));
        assert_ne!(meta.blob_key, meta.hash);
        assert_eq!(
            services.blobs.get(&meta.blob_key).unwrap().unwrap(),
            png_bytes
        );

        let big_att = ParsedAttachment {
            url: format!("http://{addr}/big.bin"),
            media_type: None,
            name: None,
            is_sensitive: false,
            blurhash: None,
            width: None,
            height: None,
        };
        assert!(fetch_and_cache_remote_media(&services, &local_client, &big_att).is_err());
        drop(handle);
    }

    // --- Slice 7 (H2+C3) regression tests (written before the fix) ---

    /// Serve one canned body per path; used by the slice-7 tests.
    fn serve_bodies(bodies: Vec<(String, String, Vec<u8>)>) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for _ in 0..bodies.len() + 4 {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                let mut buf = [0u8; 2048];
                let n = stream.read(&mut buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).into_owned();
                let path = req
                    .lines()
                    .next()
                    .and_then(|l| l.split_whitespace().nth(1))
                    .unwrap_or("/")
                    .to_string();
                let found = bodies.iter().find(|(p, _, _)| *p == path);
                match found {
                    Some((_, content_type, body)) => {
                        let hdr = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        );
                        let _ = stream.write_all(hdr.as_bytes());
                        let _ = stream.write_all(body);
                    }
                    None => {
                        let _ = stream.write_all(
                            b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        );
                    }
                }
                let _ = stream.flush();
            }
        });
        addr
    }

    fn attachment_for(url: &str, media_type: Option<&str>) -> ParsedAttachment {
        ParsedAttachment {
            url: url.to_string(),
            media_type: media_type.map(ToOwned::to_owned),
            name: Some("remote file".to_string()),
            is_sensitive: false,
            blurhash: None,
            width: None,
            height: None,
        }
    }

    #[test]
    fn remote_html_and_svg_attachments_stored_as_octet_stream() {
        // Declared `mediaType` (and the response header) must never win:
        // HTML served as `text/html` and declared `image/png` still stores
        // as octet-stream; an SVG served/declared as svg still does.
        let services = test_services("slice7-html-svg", 4096);
        let html = b"<!DOCTYPE html><html><body><script>alert(1)</script></body></html>".to_vec();
        let svg = b"<svg xmlns=\"http://www.w3.org/2000/svg\"><script>alert(1)</script></svg>"
            .to_vec();
        let addr = serve_bodies(vec![
            ("/evil.html".to_string(), "text/html".to_string(), html),
            (
                "/pic.svg".to_string(),
                "image/svg+xml".to_string(),
                svg,
            ),
        ]);
        let local_client = SafeHttpClient::new(SafeHttpConfig {
            allow_private_ips: true,
            ..Default::default()
        });

        let html_att = attachment_for(&format!("http://{addr}/evil.html"), Some("image/png"));
        let meta = fetch_and_cache_remote_media(&services, &local_client, &html_att).unwrap();
        assert_eq!(meta.mime, "application/octet-stream");
        assert_ne!(meta.mime, "text/html");

        let svg_att =
            attachment_for(&format!("http://{addr}/pic.svg"), Some("image/svg+xml"));
        let meta = fetch_and_cache_remote_media(&services, &local_client, &svg_att).unwrap();
        assert_eq!(meta.mime, "application/octet-stream");
        assert_ne!(meta.mime, "image/svg+xml");
    }

    #[test]
    fn remote_media_fetch_job_truncates_to_16() {
        let services = test_services("slice7-truncate", 4096);
        assert!(
            build_remote_media_fetch_job(&services, "act:empty", &[])
                .unwrap()
                .is_none()
        );
        let attachments: Vec<ParsedAttachment> = (0..20)
            .map(|i| {
                attachment_for(&format!("http://remote.invalid/{i}.png"), Some("image/png"))
            })
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
        assert_eq!(
            payload.attachments[15].url,
            "http://remote.invalid/15.png"
        );
    }
}
