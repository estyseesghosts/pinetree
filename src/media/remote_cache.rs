//! Remote media URL validation, downloading, and caching (Phase 2 Slice C2).
//!
//! Validates remote attachment URLs (scheme `http`/`https`, SSRF check,
//! size limit, MIME sniffing) before fetching/caching; stores metadata in
//! `MediaStore` (`owner_id = None`, `is_link = true`) and raw bytes in the
//! remote-cache `BlobStore` pool (`services.cache_blobs`, never the durable
//! `services.blobs` pool) outside the database.
//!
//! Phase 2.6 slice 7 (H2+C3, decision D-d): the inbox no longer fetches
//! attachments inline. It enqueues one durable [`JobKind::RemoteMediaFetch`]
//! job per note (atomically with `record_note_activity`); the worker runs
//! [`process_remote_media_fetch`], which is idempotent per (remote URL,
//! note) and best-effort per attachment.
//!
//! Remote-media cache plan (`docs/remote-media-cache-plan.md` §§4–5):
//! the default is fetch-on-first-media-request ([`fetch_on_first_request`],
//! used by the `GET /files/:id` handler when cache bytes are absent); the
//! job system remains for opt-in warming. One global byte budget plus
//! per-class caps bound the cache; eviction drops bytes only (metadata and
//! the `(source_url, variant)` index row stay, so bytes re-fetch while the
//! source lives). In-flight reservations (not post-hoc totals) keep
//! concurrent fetches under the cap.

use std::collections::HashSet;

use crate::activitypub::objects::ParsedAttachment;
use crate::application::commands::AppServices;
use crate::application::ports::{
    CacheStats, InsertMediaInput, JobKind, MediaClass, NewJob, PortError, RemoteCacheRecord,
    RemoteMediaAttachment, RemoteMediaFetchPayload,
};
use crate::config::RemoteCacheMode;
use crate::domain::media::{sniff_mime, MediaMeta, MediaProperties};
use crate::domain::note::MAX_FILE_IDS;
use crate::federation::delivery::{parse_http_url, resolve_and_validate_url, SafeHttpClient};
use crate::media::fetch_stream::stream_response_to_store;
use crate::media::ingest::{md5_hex, sha256_hex, sanitize_filename};

/// Variant key for the original bytes (what the `GET /files/:id` handler
/// asks for and what the fetch job caches).
pub const DEFAULT_VARIANT: &str = "original";
/// Variant key for avatar/emoji proxies (callers pass this with
/// `force_class = Some(MediaClass::Avatar)`).
pub const AVATAR_VARIANT: &str = "avatar";

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

/// Bound on eviction rounds per [`ensure_cache_budget`] call: the loop
/// re-reads accounting after every evicted candidate, so a backend whose
/// counters never move (or candidates that never shrink the total) cannot
/// spin forever — the fetch then fails closed at the reservation.
const MAX_EVICTION_ROUNDS: usize = 64;

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
    // here: the bytes were already counted once by the fetch core.
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

/// Fetch-on-first-request (plan §4.4): the `GET /files/:id` handler calls
/// this when a remote row's bytes are absent from the cache pool. Fresh
/// index hit + bytes present → touch + serve, no network. Hit with evicted
/// (or otherwise missing) bytes, or a miss → re-fetch under the global
/// reservation, stream to disk, import into the cache pool, upsert metadata
/// + index, and account. Fail-closed: `Disabled` mode and an unreservable
/// budget both surface as `Validation` (the caller serves its fallback).
pub fn fetch_on_first_request(
    services: &AppServices,
    source_url: &str,
    variant: &str,
) -> Result<MediaMeta, PortError> {
    fetch_and_index_remote_media(
        services,
        &services.federation_http,
        source_url,
        variant,
        None,
        None,
        None,
        false,
        None,
        None,
        None,
    )
}

/// Fetch-on-first-request with an explicit class override (avatar/emoji
/// proxies: `variant = AVATAR_VARIANT`, `force_class =
/// Some(MediaClass::Avatar)`). Same semantics as
/// [`fetch_on_first_request`]; the override only decides which class cap
/// and TTL govern the row — the bytes still sniff their own MIME.
pub fn fetch_on_first_request_as(
    services: &AppServices,
    source_url: &str,
    variant: &str,
    force_class: Option<MediaClass>,
) -> Result<MediaMeta, PortError> {
    fetch_and_index_remote_media(
        services,
        &services.federation_http,
        source_url,
        variant,
        force_class,
        None,
        None,
        false,
        None,
        None,
        None,
    )
}

/// Shared fetch → cache-pool → index core behind [`fetch_on_first_request`],
/// [`fetch_on_first_request_as`], the fetch job, and
/// [`fetch_and_cache_remote_media`]. Never touches the durable pool.
#[allow(clippy::too_many_arguments)]
fn fetch_and_index_remote_media(
    services: &AppServices,
    client: &SafeHttpClient,
    source_url: &str,
    variant: &str,
    force_class: Option<MediaClass>,
    name_hint: Option<&str>,
    comment: Option<String>,
    is_sensitive: bool,
    blurhash: Option<String>,
    width: Option<u32>,
    height: Option<u32>,
) -> Result<MediaMeta, PortError> {
    // Fail closed when the cache is disabled.
    if services.remote_cache_mode == RemoteCacheMode::Disabled {
        return Err(PortError::Validation(
            "REMOTE_CACHE_DISABLED".to_string(),
        ));
    }
    // Freshness lookup: hit + bytes present → touch + hit++ + serve.
    // Hit with evicted (or otherwise missing) bytes, or metadata lost
    // meanwhile → fall through and re-fetch (miss path).
    if let Some(rec) = services
        .media
        .remote_cache_lookup(source_url, variant)?
    {
        if services.cache_blobs.byte_len(&rec.blob_key)?.is_some() {
            services
                .media
                .remote_cache_touch(source_url, variant, services.clock.now_ms())?;
            services
                .media
                .cache_accounting_apply(0, rec.class, 0, 1, 0, 0, 0, 0)?;
            if let Some(meta) = services.media.get(&rec.media_id)? {
                return Ok(meta);
            }
        }
    }
    // Miss path: validate before holding any reservation (cheap skip).
    validate_remote_media_url(source_url, client)?;

    // In-flight reservation under the global cap. The true size is unknown
    // before the fetch, so reserve `max_file_size` first and true-up after
    // the real size is known (release `max - actual`, account `actual`).
    // Fail closed when the budget cannot be reserved — even after
    // `ensure_cache_budget` evicted everything it could.
    let max_bytes = services.max_file_size;
    let provisional = force_class.unwrap_or(MediaClass::Preview);
    ensure_cache_budget(services, max_bytes, provisional)?;
    if !services.media.reserve_cache_bytes(max_bytes)? {
        services
            .media
            .cache_accounting_apply(0, provisional, 0, 0, 1, 0, 0, 0)?;
        return Err(PortError::Validation(
            "CACHE_CAPACITY_EXCEEDED".to_string(),
        ));
    }

    let attachment = ParsedAttachment {
        url: source_url.to_string(),
        media_type: None,
        name: name_hint.map(ToOwned::to_owned),
        is_sensitive,
        blurhash,
        width,
        height,
    };
    let prepared = match download_and_sniff(services, client, &attachment) {
        Ok(prepared) => prepared,
        Err(e) => {
            // Post-reservation fetch failure = failed origin fetch.
            let _ = services.media.release_cache_reservation(max_bytes);
            let _ = services
                .media
                .cache_accounting_apply(0, provisional, 0, 0, 0, 0, 0, 1);
            return Err(e);
        }
    };
    let actual = prepared.bytes.len() as u64;

    // Stage to disk under the cap, then import into the cache pool.
    // (`SafeHttpClient` still buffers the body; the disk stage + cap
    // re-enforcement + `import_file` are real, the end-to-end streaming
    // transport is a follow-up.) The staging directory is a process-temp
    // fallback: a pool-owned staging dir needs a `BlobStore` tmp-dir
    // accessor the trait does not expose yet.
    let staged = match (|| -> Result<
        crate::media::fetch_stream::StreamFetchResult,
        PortError,
    > {
        let tmp_dir = std::env::temp_dir().join("pinetree-remote-cache");
        let (tmp_path, staged) =
            stream_response_to_store(&prepared.bytes[..], max_bytes, &tmp_dir)?;
        if staged.sha256 != prepared.blob_key || staged.bytes_written != actual {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(PortError::StorageUnavailable(
                "staged remote-media bytes failed hash check".to_string(),
            ));
        }
        // `BlobStore::import_file` dispatches to the filesystem rename
        // path on `FsBlobStore`; the staged file is removed either way
        // (post-rename the remove is a harmless miss).
        let out = services
            .cache_blobs
            .import_file(&prepared.blob_key, &tmp_path);
        let _ = std::fs::remove_file(&tmp_path);
        out?;
        Ok(staged)
    })() {
        Ok(staged) => staged,
        Err(e) => {
            let _ = services.media.release_cache_reservation(max_bytes);
            return Err(e);
        }
    };
    let class = force_class.unwrap_or_else(|| MediaClass::classify_mime(&staged.mime));

    // Metadata + index + accounting. These are separate store calls (no
    // cross-table transaction on the trait): a crash between them heals
    // via the reconciler (dangling meta without index re-fetches on next
    // lookup miss; production backends should co-locate them).
    let stored = (|| -> Result<MediaMeta, PortError> {
        let media_id = services.ids.generate();
        let local_url = format!(
            "{}/files/{media_id}",
            services.public_url.trim_end_matches('/')
        );
        let meta = services.media.insert_metadata(InsertMediaInput {
            id: Some(media_id.clone()),
            owner_id: None,
            name: prepared.name.clone(),
            mime: staged.mime.clone(),
            size_bytes: actual,
            hash: staged.md5.clone(),
            blob_key: prepared.blob_key.clone(),
            blurhash: prepared.blurhash.clone(),
            properties: prepared.properties.clone(),
            is_sensitive: prepared.is_sensitive,
            is_link: true,
            url: local_url,
            thumbnail_url: None,
            comment: comment.or(prepared.comment.clone()),
            source_url: Some(source_url.to_string()),
            max_file_size: services.max_file_size,
        })?;
        let now_ms = services.clock.now_ms();
        services.media.remote_cache_insert(&RemoteCacheRecord {
            source_url: source_url.to_string(),
            variant: variant.to_string(),
            media_id: media_id.clone(),
            blob_key: prepared.blob_key.clone(),
            size_bytes: actual,
            class,
            fetched_at_ms: now_ms,
            last_access_ms: now_ms,
            access_count: 1,
        })?;
        services.media.cache_accounting_apply(
            actual as i64,
            class,
            actual as i64,
            0,
            1,
            actual,
            0,
            0,
        )?;
        Ok(meta)
    })();
    // True-up the reservation on success; release it whole on failure
    // (storage fault — the caller retries and the lookup converges).
    match stored {
        Ok(meta) => {
            let _ = services
                .media
                .release_cache_reservation(max_bytes.saturating_sub(actual));
            Ok(meta)
        }
        Err(e) => {
            let _ = services.media.release_cache_reservation(max_bytes);
            Err(e)
        }
    }
}

/// Ensure `need_bytes` for `class` fit under the global cap and the class
/// cap with borrowing (plan §5): a class may borrow unused global room, so
/// eviction only triggers while `total + need > max`, or while the class
/// exceeds its cap *plus* the currently unused global slack. Candidates
/// come LRU-first from the store; TTL-stale rows (by `fetched_at_ms`
/// against the class TTL) evict first. Each eviction deletes cache-pool
/// bytes ONLY — metadata rows and the `(source_url, variant)` index row
/// stay, so the next request re-fetches while the source lives — then
/// accounting drops the bytes and counts the eviction. Terminates after
/// [`MAX_EVICTION_ROUNDS`] re-reads; whatever still does not fit fails
/// closed at the reservation.
pub fn ensure_cache_budget(
    services: &AppServices,
    need_bytes: u64,
    class: MediaClass,
) -> Result<(), PortError> {
    let max = services.remote_cache_max_bytes;
    let cap = class_cap_bytes(services, class);
    let ttl_ms = ttl_days_for(services, class) as i64 * 86_400_000;
    let now_ms = services.clock.now_ms();
    let mut evicted_keys: HashSet<String> = HashSet::new();
    for _ in 0..MAX_EVICTION_ROUNDS {
        let stats = services.media.cache_stats()?;
        let slack = max.saturating_sub(stats.total_bytes);
        let allowance = cap.saturating_add(slack);
        let over_global = stats.total_bytes.saturating_add(need_bytes) > max;
        let over_class =
            class_bytes_in(&stats, class).saturating_add(need_bytes) > allowance;
        if !over_global && !over_class {
            return Ok(());
        }
        let candidates = services.media.remote_cache_evict_candidates(8)?;
        if candidates.is_empty() {
            break;
        }
        // TTL-stale first (stable: LRU order kept within each group).
        let mut ordered = candidates;
        ordered.sort_by_key(|rec| {
            let age = now_ms.saturating_sub(rec.fetched_at_ms);
            i64::from(age <= ttl_ms)
        });
        let mut progressed = false;
        for rec in &ordered {
            if !evicted_keys.insert(rec.blob_key.clone()) {
                continue;
            }
            // Bytes ONLY: metadata + index bookkeeping stay for re-fetch.
            services.cache_blobs.delete(&rec.blob_key)?;
            services.media.cache_accounting_apply(
                -(rec.size_bytes as i64),
                rec.class,
                -(rec.size_bytes as i64),
                0,
                0,
                0,
                1,
                0,
            )?;
            progressed = true;
            break; // re-read accounting before the next eviction.
        }
        if !progressed {
            break;
        }
    }
    Ok(())
}

/// Per-class cap in bytes (AudioVideo caches on borrowed global room only —
/// plan §5 gives it a 0 MiB persistent allocation: stream/fetch on demand).
fn class_cap_bytes(services: &AppServices, class: MediaClass) -> u64 {
    match class {
        MediaClass::Avatar => services.remote_cache_avatar_bytes,
        MediaClass::Preview => services.remote_cache_preview_bytes,
        MediaClass::Full => services.remote_cache_full_bytes,
        MediaClass::AudioVideo => 0,
    }
}

/// Class bytes as exposed by [`CacheStats`] (AudioVideo has no dedicated
/// counter — it is governed by the global cap alone).
fn class_bytes_in(stats: &CacheStats, class: MediaClass) -> u64 {
    match class {
        MediaClass::Avatar => stats.avatar_bytes,
        MediaClass::Preview => stats.preview_bytes,
        MediaClass::Full => stats.full_bytes,
        MediaClass::AudioVideo => 0,
    }
}

/// Per-class TTL in days (AudioVideo TTL 0: always eviction-eligible first).
fn ttl_days_for(services: &AppServices, class: MediaClass) -> u64 {
    match class {
        MediaClass::Avatar => services.remote_cache_avatar_ttl_days,
        MediaClass::Preview => services.remote_cache_preview_ttl_days,
        MediaClass::Full => services.remote_cache_full_ttl_days,
        MediaClass::AudioVideo => 0,
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

/// Validate, fetch, and cache a remote attachment into the cache pool +
/// `MediaStore` (standalone, without attaching to a note).
///
/// The raw bytes are staged to disk and imported into `cache_blobs` (never
/// the durable pool); `MediaStore` only stores `MediaMeta` plus the
/// `(source_url, "original")` index row. Prefer [`fetch_on_first_request`]
/// for serving; this stays for callers that need the row eagerly (and for
/// the tests below).
pub fn fetch_and_cache_remote_media(
    services: &AppServices,
    client: &SafeHttpClient,
    attachment: &ParsedAttachment,
) -> Result<MediaMeta, PortError> {
    fetch_and_index_remote_media(
        services,
        client,
        &attachment.url,
        DEFAULT_VARIANT,
        None,
        attachment.name.as_deref(),
        attachment
            .name
            .as_deref()
            .map(|c| c.chars().take(512).collect()),
        attachment.is_sensitive,
        attachment.blurhash.clone(),
        attachment.width,
        attachment.height,
    )
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
        let cache_blobs = Arc::new(FsBlobStore::new(dir.join("blobs_cache")).unwrap());
        AppServices {
            clock,
            ids,
            accounts: store.clone(),
            notes: store.clone(),
            follows: store.clone(),
            blocks: store.clone(),
            mutes: store.clone(),
            reactions: store.clone(),
            favorites: store.clone(),
            emojis: store.clone(),
            antennas: store.clone(),
            antenna_notes: store.clone(),
            polls: store.clone(),
            jobs: store.clone(),
            media: store.clone(),
            blobs,
            cache_blobs,
            notifications: store.clone(),
            auth: store.clone(),
            streaming: Arc::new(NoopStreamingSink),
            dedup: store.clone() as Arc<dyn ActivityDedup>,
            host_caps: store.clone(),
            moderation: store.clone(),
            instance_settings: store.clone(),
            invites: store.clone(),
            abuse_reports: store.clone(),
            lists: store.clone(),
            signin: store.clone(),
            password_resets: store.clone(),
            registry: store.clone(),
            clips: store.clone(),
            pages: store.clone(),
            folders: store.clone(),
            system_webhooks: store.clone(),
            user_webhooks: store.clone(),
            abuse_recipients: store.clone(),
            memos: store.clone(),
            thread_mutes: store.clone(),
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
            default_like: None,
            drive_capacity_per_user_bytes: 1 << 30,
            remote_cache_max_bytes: 512 << 20,
            remote_cache_avatar_bytes: 128 << 20,
            remote_cache_preview_bytes: 256 << 20,
            remote_cache_full_bytes: 128 << 20,
            remote_cache_mode: RemoteCacheMode::OnDemand,
            remote_cache_avatar_ttl_days: 30,
            remote_cache_preview_ttl_days: 7,
            remote_cache_full_ttl_days: 3,
            remote_cache_warming_enabled: false,
        }
    }

    #[test]
    fn fetch_on_first_request_disabled_fails_closed() {
        // Mode gate fires before any store or network I/O, so this holds
        // regardless of backend quota/index implementations.
        let mut services = test_services("disabled-mode", 4096);
        services.remote_cache_mode = RemoteCacheMode::Disabled;
        let err = fetch_on_first_request(&services, "http://remote.invalid/a.png", "original")
            .expect_err("disabled cache must fail closed");
        assert!(
            matches!(err, PortError::Validation(ref m) if m == "REMOTE_CACHE_DISABLED"),
            "unexpected: {err:?}"
        );
    }

    #[test]
    fn cache_class_caps_and_ttls_match_plan() {
        // Plan §5 starting values (also the Config defaults).
        assert_eq!(MediaClass::classify_mime("image/png"), MediaClass::Preview);
        assert_eq!(MediaClass::classify_mime("video/mp4"), MediaClass::AudioVideo);
        assert_eq!(MediaClass::classify_mime("audio/mpeg"), MediaClass::AudioVideo);
        assert_eq!(MediaClass::classify_mime("text/plain"), MediaClass::Full);
        assert_eq!(MediaClass::classify_mime("application/octet-stream"), MediaClass::Full);
        let services = test_services("class-caps", 4096);
        assert_eq!(class_cap_bytes(&services, MediaClass::Avatar), 128 << 20);
        assert_eq!(class_cap_bytes(&services, MediaClass::Preview), 256 << 20);
        assert_eq!(class_cap_bytes(&services, MediaClass::Full), 128 << 20);
        assert_eq!(class_cap_bytes(&services, MediaClass::AudioVideo), 0);
        assert_eq!(ttl_days_for(&services, MediaClass::Avatar), 30);
        assert_eq!(ttl_days_for(&services, MediaClass::Preview), 7);
        assert_eq!(ttl_days_for(&services, MediaClass::Full), 3);
        assert_eq!(ttl_days_for(&services, MediaClass::AudioVideo), 0);
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
            for _ in 0..3 {
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
        // Slice 8 (D6, written before the fix): the blob lives under the
        // SHA-256 key while the meta keeps the md5; both are the real
        // digests of the fetched bytes. Remote bytes live in the cache
        // pool, never the durable pool.
        assert_eq!(meta.hash, md5_hex(&png_bytes));
        assert_eq!(meta.blob_key, sha256_hex(&png_bytes));
        assert_ne!(meta.blob_key, meta.hash);
        assert_eq!(
            services.cache_blobs.get(&meta.blob_key).unwrap().unwrap(),
            png_bytes
        );
        assert_eq!(services.blobs.get(&meta.blob_key).unwrap(), None);

        // On-demand second request for the same URL hits the index: no
        // second fetch (the server only served 3 connections; oversize
        // fetch below would be the 3rd) and the same row comes back.
        let again =
            fetch_on_first_request(&services, &format!("http://{addr}/pic.png"), "original")
                .unwrap();
        assert_eq!(again.id, meta.id);

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
