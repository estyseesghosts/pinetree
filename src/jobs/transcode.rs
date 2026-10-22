//! Post-commit transcode job (gaps WP1).
//!
//! The upload path commits the `MediaMeta` row + a `TranscodeMedia` job
//! atomically (proposed parent wiring: `drive/files/create` +
//! `upload_media_from_url`); the worker runs ffmpeg outside any transaction
//! via [`run_transcode_job`] and commits the result last.
//!
//! Crash safety: ffmpeg writes to temp files outside the blob store. The
//! derived bytes are stored under their own SHA-256 content key, read back
//! and verified, and the row is re-committed under the same id pointing at
//! the new bytes last. A crash before commit leaves the original row
//! untouched (original bytes stay served) and a bounded re-run converges:
//! ffmpeg sniffs content, so re-transcoding already-derived bytes is still
//! valid output.
//!
//! Idempotency: a missing row is a no-op ([`TranscodeOutcome::Noop`]), as is
//! a row already in target shape with committed dims. Storage faults
//! (`get`/`put`) return `Err` so the job retries under its bounded budget
//! and then quarantines; deterministic input failures (bad decode, ffmpeg
//! error, wall-time/output-size/memory breach) report
//! [`TranscodeOutcome::FailedDeterministic`], which the API surfaces as
//! `422` once the parent wires the `MediaState::Failed` commit (until then
//! the proposed worker arm quarantines the job with the reason, which is
//! equally terminal for the row).
//!
//! Content-addressed adaptation: pinetree's `BlobStore` keys bytes by SHA-256
//! (identical bytes share one key), so the derived blob cannot overwrite the
//! original in place. It is stored under its own key and the row is
//! re-committed (same id) pointing at the new key. The old bytes stay stored
//! while any row references them (cross-owner dedup sharers keep working);
//! bytes referenced by no row are reconciler sweep candidates. Quota note:
//! the interim commit uses the plain `insert_metadata` upsert (no quota
//! state machine runs on it); the proposed atomic
//! `MediaStore::commit_transcoded_media` (see the WP1 final report) will
//! adjust durable owner bytes saturatingly. Derived outputs are capped at 64
//! MiB by the ffmpeg run itself, so interim over-counting is bounded.

use crate::application::ports::{BlobStore, InsertMediaInput, MediaStore, PortError};
use crate::domain::media::MediaMeta;
use crate::media::ingest::{md5_hex, sha256_hex};
use crate::media::processing::{self, PREVIEW_BOUND};
use crate::media::transcode::{
    self, TranscodeConfig, TranscodeError, TranscodePlan, derived_mime, plan_for_mime,
    probe_derived, probe_has_audio, probe_video, transcode_bytes, transcode_still_with_preview,
    validate_toolchain,
};

/// Payload of the post-commit transcode job, enqueued under
/// `JobKind::TranscodeMedia` (proposed parent addition) and dispatched by
/// the job worker. Jobs carry ids only — never file content, per the 64 KiB
/// cap.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TranscodeJobPayload {
    pub file_id: String,
}

impl TranscodeJobPayload {
    pub fn new(file_id: impl Into<String>) -> Self {
        Self {
            file_id: file_id.into(),
        }
    }

    /// Deterministic idempotency key: one transcode job per file id, so
    /// re-enqueues converge instead of stacking duplicate ffmpeg runs.
    pub fn idempotency_key(&self) -> String {
        format!("transcode-media:{}", self.file_id)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, PortError> {
        serde_json::to_vec(self)
            .map_err(|e| PortError::Validation(format!("transcode payload serialize failed: {e}")))
    }

    /// Extract the file id from a job payload (`Err` when absent: a
    /// programmer error, surfaced loudly rather than silently skipped).
    pub fn file_id_from_bytes(payload: &[u8]) -> Result<String, PortError> {
        let parsed: TranscodeJobPayload = serde_json::from_slice(payload)
            .map_err(|e| PortError::Validation(format!("transcode-media payload invalid: {e}")))?;
        if parsed.file_id.trim().is_empty() {
            return Err(PortError::Validation(
                "transcode job: missing file_id".to_string(),
            ));
        }
        Ok(parsed.file_id)
    }
}

/// How a transcode job run ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TranscodeOutcome {
    /// Missing row, or a row already converged (nothing to do). The job
    /// completes.
    Noop,
    /// Derived bytes committed and the row re-pointed (see [`ReadyCommit`]).
    /// The job completes.
    Ready(ReadyCommit),
    /// Deterministic input failure (bad decode, ffmpeg error, wall-time /
    /// output-size / memory breach). The API surfaces this as `422`; the
    /// worker must record it terminally (proposed `MediaState::Failed`
    /// commit; interim: quarantine with the reason).
    FailedDeterministic(String),
}

/// What a [`TranscodeOutcome::Ready`] run committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadyCommit {
    /// `file_id` of the re-committed row.
    pub file_id: String,
    /// MIME of the derived bytes (`image/webp` / `video/webm`).
    pub mime: String,
    /// SHA-256 content key of the derived bytes.
    pub blob_key: String,
    pub size_bytes: u64,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub duration_ms: Option<u64>,
    /// `true` when the `small` preview pass also produced WebP bytes.
    pub preview_committed: bool,
}

/// Execute a transcode job: finish processing staged bytes and commit the
/// result (idempotent).
///
/// Contract: a missing row, or a row already in target shape with committed
/// dims, is a no-op [`TranscodeOutcome::Noop`]. Rows that need no transcode
/// take the legacy pure-Rust path. Transcoded rows run ffmpeg outside any
/// transaction, store the derived blob under its content key, and re-commit
/// the row (same id) pointing at the new bytes, with meta read from the
/// *derived* bytes. Deterministic failures report
/// [`TranscodeOutcome::FailedDeterministic`]; only missing/unreadable
/// storage (and a vanishing toolchain) return `Err` so the job retries
/// under its bounded budget and then quarantines.
pub fn run_transcode_job(
    media: &dyn MediaStore,
    blobs: &dyn BlobStore,
    file_id: &str,
    config: &TranscodeConfig,
) -> Result<TranscodeOutcome, PortError> {
    let Some(meta) = media.get(file_id)? else {
        return Ok(TranscodeOutcome::Noop);
    };
    if is_converged(&meta) {
        return Ok(TranscodeOutcome::Noop);
    }
    match plan_for_mime(&meta.mime) {
        TranscodePlan::Passthrough => commit_legacy(media, blobs, &meta),
        plan => {
            if !config.enabled {
                // `--no-transcode` routes around this module: the upload path
                // never enqueues. A stray job is a programmer error — loud,
                // retryable (quarantines past the budget), never a silent
                // passthrough of the original.
                return Err(PortError::StorageUnavailable(
                    "transcode job ran while transcoding is disabled".to_string(),
                ));
            }
            if let Err(detail) = validate_toolchain(&config.ffmpeg_path) {
                // Startup validation should have caught this; a binary that
                // vanishes at runtime is environmental → retry, then
                // quarantine. Never fall back to serving the original.
                return Err(PortError::StorageUnavailable(format!(
                    "ffmpeg toolchain unavailable: {detail}"
                )));
            }
            run_transcode_plan(media, blobs, &meta, plan, config)
        }
    }
}

/// True when the row needs no work: audio rows never change, and rows
/// already in target shape (`image/webp`, `video/webm`) with committed dims
/// are the output of a previous run — re-running ffmpeg would only burn CPU
/// to produce the same bytes.
fn is_converged(meta: &MediaMeta) -> bool {
    if meta.mime.starts_with("audio/") {
        return true;
    }
    if matches!(meta.mime.as_str(), "image/webp" | "video/webm") && meta.properties.width.is_some()
    {
        return true;
    }
    false
}

/// Legacy pure-Rust finish for rows that need no transcode: parse dims from
/// the stored bytes and re-commit the row when anything changed.
fn commit_legacy(
    media: &dyn MediaStore,
    blobs: &dyn BlobStore,
    meta: &MediaMeta,
) -> Result<TranscodeOutcome, PortError> {
    let bytes = match read_staged_bytes(blobs, meta)? {
        Some(bytes) => bytes,
        // Staged original missing: deterministic — there is nothing to
        // process and nothing that a retry could heal (the put happened
        // before the row commit on the upload path).
        None => {
            return Ok(TranscodeOutcome::FailedDeterministic(format!(
                "no staged bytes for file {}",
                meta.id
            )));
        }
    };
    match processing::process(&meta.mime, &bytes) {
        Ok(processed) => {
            let thumbnail_url = match meta.mime.as_str() {
                // `small` preview parity on the legacy path: the derived
                // shape *is* WebP, so the servable preview is the file
                // itself (there is no thumbnail serving route yet; see the
                // WP1 report).
                mime if mime.starts_with("image/") => Some(meta.url.clone()),
                _ => meta.thumbnail_url.clone(),
            };
            if processed.width == meta.properties.width
                && processed.height == meta.properties.height
                && thumbnail_url == meta.thumbnail_url
            {
                return Ok(TranscodeOutcome::Noop);
            }
            let mut next = meta.clone();
            next.properties.width = processed.width;
            next.properties.height = processed.height;
            next.thumbnail_url = thumbnail_url;
            repoint_row(media, &next)?;
            Ok(TranscodeOutcome::Ready(ReadyCommit {
                file_id: meta.id.clone(),
                mime: meta.mime.clone(),
                blob_key: meta.blob_key.clone(),
                size_bytes: meta.size_bytes,
                width: processed.width,
                height: processed.height,
                duration_ms: processed.duration_ms,
                preview_committed: false,
            }))
        }
        Err(err) => {
            let reason = match &err {
                processing::ProcessError::Unidentifiable(detail) => {
                    format!("File type of uploaded media could not be verified: {detail}")
                }
                processing::ProcessError::Other(detail) => {
                    format!("Error processing thumbnail for uploaded media: {detail}")
                }
            };
            Ok(TranscodeOutcome::FailedDeterministic(reason))
        }
    }
}

/// Run the ffmpeg plan for `meta` and commit the derived result.
fn run_transcode_plan(
    media: &dyn MediaStore,
    blobs: &dyn BlobStore,
    meta: &MediaMeta,
    plan: TranscodePlan,
    config: &TranscodeConfig,
) -> Result<TranscodeOutcome, PortError> {
    let Some(bytes) = read_staged_bytes(blobs, meta)? else {
        return Err(PortError::StorageUnavailable(format!(
            "staged original missing for file {}",
            meta.id
        )));
    };
    // Video probes the original first: VP9 + Opus-or-silent within the size
    // limit keeps its codecs — byte-identical when already WebM,
    // stream-copied into WebM otherwise.
    if plan == TranscodePlan::VideoToWebm
        && let Some(probe) = probe_video(&bytes, config)
    {
        if probe.container_is_webm() {
            return commit_passthrough_video(media, meta, &probe);
        }
        return run_remux_to_webm(media, blobs, meta, &bytes, config);
    }
    let with_audio = plan == TranscodePlan::VideoToWebm && probe_has_audio(&bytes, config);
    // Demuxer hint only (ffmpeg sniffs content): real extensions keep ffmpeg
    // from mis-probing BOM-less stills, and bound the temp-file namespace.
    let input_ext = match meta.mime.as_str() {
        "image/heic" | "image/heif" => "heic",
        "image/tiff" | "image/x-tiff" => "tif",
        "image/avif" => "avif",
        _ => match plan {
            TranscodePlan::ImageToWebp => "img",
            TranscodePlan::GifToWebm => "gif",
            TranscodePlan::VideoToWebm | TranscodePlan::VideoRemux => "vid",
            TranscodePlan::Passthrough => "bin",
        },
    };
    if plan == TranscodePlan::ImageToWebp {
        match transcode_still_with_preview(&bytes, input_ext, PREVIEW_BOUND, config) {
            Ok((derived, preview)) => {
                commit_derived(media, blobs, meta, &derived, Some(&preview), plan, config)
            }
            Err(err) => map_transcode_error(meta, err),
        }
    } else {
        match transcode_bytes(&bytes, input_ext, plan, with_audio, config) {
            Ok((derived, _stats)) => {
                commit_derived(media, blobs, meta, &derived, None, plan, config)
            }
            Err(err) => map_transcode_error(meta, err),
        }
    }
}

/// Remux VP9 + Opus-or-silent bytes from a non-WebM container into WebM
/// with stream copy (no re-encode), then commit exactly like a transcode:
/// the row points at the remuxed bytes, meta comes from the remuxed bytes.
/// A remux failure is deterministic input → `FailedDeterministic`, never a
/// quiet keep of the non-WebM bytes.
fn run_remux_to_webm(
    media: &dyn MediaStore,
    blobs: &dyn BlobStore,
    meta: &MediaMeta,
    bytes: &[u8],
    config: &TranscodeConfig,
) -> Result<TranscodeOutcome, PortError> {
    match transcode_bytes(bytes, "vid", TranscodePlan::VideoRemux, false, config) {
        Ok((derived, _stats)) => commit_derived(
            media,
            blobs,
            meta,
            &derived,
            None,
            TranscodePlan::VideoToWebm,
            config,
        ),
        Err(err) => map_transcode_error(meta, err),
    }
}

/// Environmental ffmpeg failures retry; deterministic ones fail the row.
fn map_transcode_error(
    meta: &MediaMeta,
    err: TranscodeError,
) -> Result<TranscodeOutcome, PortError> {
    match err {
        TranscodeError::Spawn(detail) | TranscodeError::Io(detail) => {
            Err(PortError::StorageUnavailable(format!(
                "transcode of file {} failed transiently: {detail}",
                meta.id
            )))
        }
        other => Ok(TranscodeOutcome::FailedDeterministic(format!(
            "transcode failed: {other}"
        ))),
    }
}

/// Commit a passthrough video: original bytes stay, the row is re-committed
/// with probe dims when the probe could read them (honest `None`
/// otherwise).
fn commit_passthrough_video(
    media: &dyn MediaStore,
    meta: &MediaMeta,
    probe: &transcode::Probe,
) -> Result<TranscodeOutcome, PortError> {
    if meta.properties.width == probe.width
        && meta.properties.height == probe.height
        && meta.thumbnail_url == Some(meta.url.clone())
    {
        return Ok(TranscodeOutcome::Noop);
    }
    let mut next = meta.clone();
    next.properties.width = probe.width;
    next.properties.height = probe.height;
    next.thumbnail_url = Some(meta.url.clone());
    repoint_row(media, &next)?;
    Ok(TranscodeOutcome::Ready(ReadyCommit {
        file_id: meta.id.clone(),
        mime: meta.mime.clone(),
        blob_key: meta.blob_key.clone(),
        size_bytes: meta.size_bytes,
        width: probe.width,
        height: probe.height,
        duration_ms: probe.duration_ms,
        preview_committed: false,
    }))
}

/// Commit transcoded derived bytes: verify them, store them under their
/// content key, and re-commit the row (same id) pointing at the new bytes
/// with meta read from the *derived* bytes.
fn commit_derived(
    media: &dyn MediaStore,
    blobs: &dyn BlobStore,
    meta: &MediaMeta,
    derived: &[u8],
    preview: Option<&[u8]>,
    plan: TranscodePlan,
    config: &TranscodeConfig,
) -> Result<TranscodeOutcome, PortError> {
    if derived.is_empty() {
        return Ok(TranscodeOutcome::FailedDeterministic(
            "transcode failed: ffmpeg succeeded but wrote an empty output".to_string(),
        ));
    }
    // 1. Meta comes from the DERIVED bytes, never the original: dimensions
    //    must match the stored pixels.
    let (width, height, duration_ms) = match plan {
        TranscodePlan::ImageToWebp => match processing::process("image/webp", derived) {
            Ok(processed) => (processed.width, processed.height, None),
            Err(err) => {
                return Ok(TranscodeOutcome::FailedDeterministic(format!(
                    "File type of uploaded media could not be verified: \
                     ffmpeg output undecodable: {err}"
                )));
            }
        },
        TranscodePlan::GifToWebm | TranscodePlan::VideoToWebm | TranscodePlan::VideoRemux => {
            probe_derived(derived, config)
        }
        TranscodePlan::Passthrough => return Ok(TranscodeOutcome::Noop),
    };
    // 2. Store the derived blob under its content key and verify the
    //    read-back before it can ever be served.
    let blob_key = sha256_hex(derived);
    blobs.put(&blob_key, derived)?;
    let staged = blobs.get(&blob_key)?.ok_or_else(|| {
        PortError::StorageUnavailable(format!("stored derived bytes missing for file {}", meta.id))
    })?;
    if staged.len() != derived.len() {
        return Err(PortError::StorageUnavailable(format!(
            "staged derived bytes failed read-back for file {}",
            meta.id
        )));
    }
    // 3. Store the `small` preview (WebP) when the still pass produced one.
    //    There is no thumbnail serving route yet, so the wire
    //    `thumbnailUrl` points at the file itself (servable, and WebP);
    //    the preview bytes stay content-addressed for the future thumbnail
    //    route (unreferenced bytes are reconciler sweep candidates).
    if let Some(preview) = preview {
        let preview_key = sha256_hex(preview);
        blobs.put(&preview_key, preview)?;
    }
    // 4. Re-commit the row last, pointing at the derived bytes. The wire
    //    type follows the derived blob: GIF → `video/webm`; stills →
    //    `image/webp`; video → `video/webm`. (`gifv` never.)
    let mut next = meta.clone();
    next.mime = derived_mime(plan).to_string();
    next.size_bytes = derived.len() as u64;
    next.hash = md5_hex(derived);
    next.blob_key = blob_key.clone();
    next.properties.width = width;
    next.properties.height = height;
    next.thumbnail_url = Some(meta.url.clone());
    repoint_row(media, &next)?;
    Ok(TranscodeOutcome::Ready(ReadyCommit {
        file_id: meta.id.clone(),
        mime: next.mime,
        blob_key,
        size_bytes: next.size_bytes,
        width,
        height,
        duration_ms,
        preview_committed: preview.is_some(),
    }))
}

/// Re-commit a drive-file row under its existing id (bytes never change
/// here — only metadata moves). Quota note: the plain `insert_metadata`
/// upsert is charge-neutral for same-size rewrites on correct backends (see
/// `update_media`); size-changing rewrites need the proposed atomic
/// `commit_transcoded_media` — see the WP1 final report.
fn repoint_row(media: &dyn MediaStore, next: &MediaMeta) -> Result<(), PortError> {
    media.insert_metadata(InsertMediaInput {
        id: Some(next.id.clone()),
        owner_id: next.owner_id.clone(),
        name: next.name.clone(),
        mime: next.mime.clone(),
        size_bytes: next.size_bytes,
        hash: next.hash.clone(),
        blob_key: next.blob_key.clone(),
        blurhash: next.blurhash.clone(),
        properties: next.properties.clone(),
        is_sensitive: next.is_sensitive,
        is_link: next.is_link,
        url: next.url.clone(),
        thumbnail_url: next.thumbnail_url.clone(),
        comment: next.comment.clone(),
        source_url: next.source_url.clone(),
        max_file_size: u64::MAX,
    })?;
    Ok(())
}

/// Read the row's staged original bytes. `Ok(None)` (missing, corrupt, or
/// torn sidecar) is distinct from `Err` (the store itself failed).
pub fn read_staged_bytes(
    blobs: &dyn BlobStore,
    meta: &MediaMeta,
) -> Result<Option<Vec<u8>>, PortError> {
    blobs.get(&meta.blob_key)
}
