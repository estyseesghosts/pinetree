//! WP1 gaps test: ffmpeg transcode ported from birchtree, adapted to
//! Misskey DriveFile (`MediaMeta`: `url`, `thumbnail_url`, `properties`).
//!
//! Split from the units living next to the code (`media::transcode`,
//! `media::processing`): this file pins the WP1 contract end to end —
//! decision table, probe parsing, argv shape, toolchain gate, the durable
//! job handler's outcomes, and (tool-chain gated) a real ffmpeg run.
//!
//! Tests that need a working `ffmpeg` binary skip loudly (`eprintln! +
//! return`) when `validate_toolchain("ffmpeg")` fails, so the suite stays
//! green on boxes without the full toolchain (e.g. Fedora, whose build
//! disables software h264/hevc decoders) while still enforcing the gate
//! shape. `transcode_bytes`-level tests only need a spawning `ffmpeg`.

use std::collections::HashMap;
use std::process::Command;
use std::sync::RwLock;

use pinetree::application::ports::{
    BlobStore, InsertMediaInput, MediaStore, PortError, TimelineCursor,
};
use pinetree::domain::media::{MediaMeta, MediaProperties};
use pinetree::jobs::transcode::{TranscodeJobPayload, TranscodeOutcome, run_transcode_job};
use pinetree::media::ingest::{md5_hex, sha256_hex};
use pinetree::media::transcode::{
    DEFAULT_MAX_FILE_SIZE, Probe, TranscodeConfig, TranscodeError, TranscodePlan, build_argv,
    derived_extension, derived_mime, needs_transcode, parse_duration_ms, parse_probe_stderr,
    plan_for_mime, probe_is_passthrough, transcode_bytes, validate_toolchain,
};

// ---------------------------------------------------------------------------
// In-memory fakes (implement only what the handler touches; every other
// MediaStore method keeps its trait default).
// ---------------------------------------------------------------------------

#[derive(Default)]
struct FakeMedia {
    rows: RwLock<HashMap<String, MediaMeta>>,
    fail_next: RwLock<bool>,
}

impl FakeMedia {
    fn fail_next(&self) {
        *self.fail_next.write().unwrap() = true;
    }
    fn take_fail(&self) -> bool {
        std::mem::replace(&mut *self.fail_next.write().unwrap(), false)
    }
}

impl MediaStore for FakeMedia {
    fn insert_metadata(&self, input: InsertMediaInput) -> Result<MediaMeta, PortError> {
        if self.take_fail() {
            return Err(PortError::StorageUnavailable("boom".to_string()));
        }
        let id = input.id.clone().unwrap_or_else(|| "auto".to_string());
        let meta = MediaMeta {
            id: id.clone(),
            owner_id: input.owner_id,
            name: input.name,
            mime: input.mime,
            size_bytes: input.size_bytes,
            hash: input.hash,
            blob_key: input.blob_key,
            blurhash: input.blurhash,
            properties: input.properties,
            is_sensitive: input.is_sensitive,
            is_link: input.is_link,
            url: input.url,
            thumbnail_url: input.thumbnail_url,
            comment: input.comment,
            source_url: input.source_url,
            variant: "original".to_string(),
            media_class: "preview".to_string(),
            evicted: false,
        };
        self.rows.write().unwrap().insert(id, meta.clone());
        Ok(meta)
    }
    fn get(&self, id: &str) -> Result<Option<MediaMeta>, PortError> {
        if self.take_fail() {
            return Err(PortError::StorageUnavailable("boom".to_string()));
        }
        Ok(self.rows.read().unwrap().get(id).cloned())
    }
    fn find_by_owner_and_hash(
        &self,
        _owner_id: &str,
        _hash: &str,
    ) -> Result<Option<MediaMeta>, PortError> {
        Ok(None)
    }
    fn find_by_source_url(&self, _source_url: &str) -> Result<Vec<MediaMeta>, PortError> {
        Ok(Vec::new())
    }
    fn delete_metadata(&self, id: &str) -> Result<(), PortError> {
        self.rows.write().unwrap().remove(id);
        Ok(())
    }
    fn list_by_owner(&self, _owner_id: &str) -> Result<Vec<MediaMeta>, PortError> {
        Ok(Vec::new())
    }
    fn list_all_media(&self, _cursor: &TimelineCursor) -> Result<Vec<MediaMeta>, PortError> {
        Ok(Vec::new())
    }
}

#[derive(Default)]
struct FakeBlobs {
    bytes: RwLock<HashMap<String, Vec<u8>>>,
    fail_get: RwLock<bool>,
}

impl BlobStore for FakeBlobs {
    fn put(&self, hash: &str, bytes: &[u8]) -> Result<(), PortError> {
        self.bytes
            .write()
            .unwrap()
            .insert(hash.to_string(), bytes.to_vec());
        Ok(())
    }
    fn get(&self, hash: &str) -> Result<Option<Vec<u8>>, PortError> {
        if *self.fail_get.read().unwrap() {
            return Err(PortError::StorageUnavailable("get boom".to_string()));
        }
        Ok(self.bytes.read().unwrap().get(hash).cloned())
    }
    fn delete(&self, hash: &str) -> Result<(), PortError> {
        self.bytes.write().unwrap().remove(hash);
        Ok(())
    }
}

fn drive_row(mime: &str, bytes: &[u8], blobs: &FakeBlobs) -> MediaMeta {
    let blob_key = sha256_hex(bytes);
    blobs.put(&blob_key, bytes).unwrap();
    MediaMeta {
        id: "file01".to_string(),
        owner_id: Some("owner1".to_string()),
        name: "upload.bin".to_string(),
        mime: mime.to_string(),
        size_bytes: bytes.len() as u64,
        hash: md5_hex(bytes),
        blob_key,
        blurhash: None,
        properties: MediaProperties::default(),
        is_sensitive: false,
        is_link: false,
        url: "https://example.invalid/files/file01".to_string(),
        thumbnail_url: None,
        comment: None,
        source_url: None,
        variant: "original".to_string(),
        media_class: "preview".to_string(),
        evicted: false,
    }
}

fn insert(media: &FakeMedia, meta: &MediaMeta) {
    media
        .insert_metadata(InsertMediaInput {
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
            max_file_size: u64::MAX,
        })
        .unwrap();
}

fn test_config(ffmpeg_path: &str, enabled: bool) -> TranscodeConfig {
    TranscodeConfig {
        enabled,
        ffmpeg_path: ffmpeg_path.to_string(),
        ..TranscodeConfig::default()
    }
}

// ---------------------------------------------------------------------------
// Decision table (gaps WP1 spec).
// ---------------------------------------------------------------------------

#[test]
fn gaps_transcode_decision_table() {
    use TranscodePlan::{GifToWebm, ImageToWebp, Passthrough, VideoToWebm};
    // GIF (animated or not) → WebM/VP9.
    assert_eq!(plan_for_mime("image/gif"), GifToWebm);
    // Stills → WebP (WebP itself passes through).
    for mime in [
        "image/jpeg",
        "image/png",
        "image/bmp",
        "image/heic",
        "image/heif",
        "image/tiff",
        "image/x-tiff",
        "image/avif",
    ] {
        assert_eq!(plan_for_mime(mime), ImageToWebp, "{mime}");
        assert!(needs_transcode(mime), "{mime}");
    }
    assert_eq!(plan_for_mime("image/webp"), Passthrough);
    // Video → WebM/VP9 + Opus.
    assert_eq!(plan_for_mime("video/mp4"), VideoToWebm);
    assert_eq!(plan_for_mime("video/webm"), VideoToWebm);
    // Audio passthrough.
    for mime in ["audio/ogg", "audio/mpeg", "audio/wav", "audio/flac"] {
        assert_eq!(plan_for_mime(mime), Passthrough, "{mime}");
        assert!(!needs_transcode(mime), "{mime}");
    }
    assert!(!needs_transcode("image/webp"));
    assert!(needs_transcode("image/gif"));
    // Derived shapes are always servable containers.
    assert_eq!(derived_extension(ImageToWebp), "webp");
    assert_eq!(derived_extension(GifToWebm), "webm");
    assert_eq!(derived_mime(ImageToWebp), "image/webp");
    assert_eq!(derived_mime(GifToWebm), "video/webm");
    assert_eq!(derived_mime(VideoToWebm), "video/webm");
}

#[test]
fn gaps_transcode_passthrough_bound_is_codec_based() {
    let base = Probe {
        video_codec: Some("vp9".to_string()),
        audio_codec: Some("opus".to_string()),
        container: Some("matroska,webm".to_string()),
        width: Some(640),
        height: Some(480),
        duration_ms: Some(2_500),
    };
    assert!(probe_is_passthrough(&base, 1024, DEFAULT_MAX_FILE_SIZE));
    assert!(base.container_is_webm());
    // Codec bound ignores the container (remux covers the rest).
    let mp4 = Probe {
        container: Some("mov,mp4,m4a,3gp,3g2,mj2".to_string()),
        ..base.clone()
    };
    assert!(probe_is_passthrough(&mp4, 1024, DEFAULT_MAX_FILE_SIZE));
    assert!(!mp4.container_is_webm());
    // Silent is fine; h264/aac/oversize transcode.
    assert!(probe_is_passthrough(
        &Probe {
            audio_codec: None,
            ..base.clone()
        },
        1024,
        DEFAULT_MAX_FILE_SIZE
    ));
    assert!(!probe_is_passthrough(
        &Probe {
            video_codec: Some("h264".to_string()),
            ..base.clone()
        },
        1024,
        DEFAULT_MAX_FILE_SIZE
    ));
    assert!(!probe_is_passthrough(
        &base,
        DEFAULT_MAX_FILE_SIZE + 1,
        DEFAULT_MAX_FILE_SIZE
    ));
}

#[test]
fn gaps_transcode_probe_parses_ffmpeg_stderr() {
    let stderr = "ffmpeg version 8.1.3 Copyright (c)\n\
         Input #0, matroska,webm, from 'in.webm':\n\
         Duration: 00:00:02.50, start: 0.000000, bitrate: 100 kb/s\n\
         Stream #0:0: Video: vp9 (Profile 0), yuv420p(tv), 640x480, 25 fps\n\
         Stream #0:1(eng): Audio: opus, 48000 Hz, stereo, fltp\n";
    let probe = parse_probe_stderr(stderr);
    assert_eq!(probe.video_codec.as_deref(), Some("vp9"));
    assert_eq!(probe.audio_codec.as_deref(), Some("opus"));
    assert!(probe.container_is_webm());
    assert_eq!((probe.width, probe.height), (Some(640), Some(480)));
    assert_eq!(probe.duration_ms, Some(2_500));
    assert_eq!(parse_probe_stderr(""), Probe::default());
    assert_eq!(
        parse_duration_ms("01:02:03.456, bitrate: 1"),
        Some(3_723_456)
    );
    assert_eq!(parse_duration_ms("N/A,"), None);
}

#[test]
fn gaps_transcode_argv_contract() {
    let cfg = TranscodeConfig::default();
    let input = std::path::Path::new("/tmp/in.gif");
    let output = std::path::Path::new("/tmp/out.webm");
    // Every plan: `-map_metadata 0` (never strip), `-i` precedes input,
    // output is the final argument.
    for (plan, with_audio) in [
        (TranscodePlan::ImageToWebp, false),
        (TranscodePlan::GifToWebm, false),
        (TranscodePlan::VideoToWebm, true),
        (TranscodePlan::VideoToWebm, false),
        (TranscodePlan::VideoRemux, true),
    ] {
        let argv = build_argv("ffmpeg", input, output, plan, with_audio, &cfg);
        assert!(argv.contains(&"-map_metadata".to_string()), "{plan:?}");
        assert!(argv.contains(&"0".to_string()), "{plan:?}");
        let i = argv.iter().position(|a| a == "-i").expect("has -i");
        assert_eq!(argv[i + 1], "/tmp/in.gif");
        assert_eq!(argv.last().map(String::as_str), Some("/tmp/out.webm"));
    }
    // Quality pins (gaps WP1): WebP q80, VP9 crf30, Opus 96k.
    let argv = build_argv(
        "ffmpeg",
        input,
        output,
        TranscodePlan::VideoToWebm,
        true,
        &cfg,
    );
    let joined = argv.join(" ");
    assert!(joined.contains("libvpx-vp9"), "{joined}");
    assert!(joined.contains("libopus"), "{joined}");
    assert!(joined.contains("30"), "{joined}");
    assert!(joined.contains("96k"), "{joined}");
    // Remux never re-encodes.
    let argv = build_argv(
        "ffmpeg",
        input,
        output,
        TranscodePlan::VideoRemux,
        true,
        &cfg,
    );
    let joined = argv.join(" ");
    assert!(joined.contains("-c copy"), "{joined}");
    assert!(!joined.contains("libvpx"), "{joined}");
}

// ---------------------------------------------------------------------------
// Toolchain gate.
// ---------------------------------------------------------------------------

#[test]
fn gaps_transcode_toolchain_gate() {
    // Missing binary is a startup error, never a silent passthrough.
    let err = validate_toolchain("/nonexistent/pinetree-ffmpeg-xyz").expect_err("missing");
    assert!(err.contains("could not be executed"), "{err}");
    // Disabled transcoding validates without touching the binary.
    let disabled = test_config("/nonexistent/pinetree-ffmpeg-xyz", false);
    assert!(!disabled.validate().expect("disabled").enabled);
    // Range checks run before the toolchain probe.
    assert!(
        TranscodeConfig {
            webp_quality: 101,
            ..TranscodeConfig::default()
        }
        .validate()
        .is_err()
    );
    // Spawn failure surfaces as retryable (job retries, then quarantines).
    let err = transcode_bytes(
        b"GIF89a",
        "gif",
        TranscodePlan::GifToWebm,
        false,
        &test_config("/nonexistent/pinetree-ffmpeg-xyz", true),
    )
    .expect_err("missing binary must fail");
    assert!(matches!(err, TranscodeError::Spawn(_)), "{err}");
}

// ---------------------------------------------------------------------------
// Job handler outcomes (no ffmpeg needed).
// ---------------------------------------------------------------------------

#[test]
fn gaps_transcode_job_payload_round_trip() {
    let p = TranscodeJobPayload::new("file01");
    assert_eq!(p.idempotency_key(), "transcode-media:file01");
    let bytes = p.to_bytes().unwrap();
    assert_eq!(
        TranscodeJobPayload::file_id_from_bytes(&bytes).unwrap(),
        "file01"
    );
    assert!(TranscodeJobPayload::file_id_from_bytes(b"{}").is_err());
    assert!(TranscodeJobPayload::file_id_from_bytes(b"not json").is_err());
}

#[test]
fn gaps_transcode_missing_row_is_noop() {
    let media = FakeMedia::default();
    let blobs = FakeBlobs::default();
    let out = run_transcode_job(&media, &blobs, "nope", &test_config("ffmpeg", true)).unwrap();
    assert!(matches!(out, TranscodeOutcome::Noop));
}

#[test]
fn gaps_transcode_audio_row_is_noop() {
    let media = FakeMedia::default();
    let blobs = FakeBlobs::default();
    insert(&media, &drive_row("audio/ogg", b"OggS fake audio", &blobs));
    let out = run_transcode_job(&media, &blobs, "file01", &test_config("ffmpeg", true)).unwrap();
    assert!(matches!(out, TranscodeOutcome::Noop));
}

#[test]
fn gaps_transcode_webp_row_commits_dims_then_converges() {
    let media = FakeMedia::default();
    let blobs = FakeBlobs::default();
    // Minimal VP8X webp: RIFF/WEBP + VP8X + 24-bit (canvas-1) 639x479.
    let mut bytes = b"RIFF".to_vec();
    bytes.extend_from_slice(&22u32.to_le_bytes());
    bytes.extend_from_slice(b"WEBPVP8X");
    bytes.extend_from_slice(&[0; 8]);
    bytes.extend_from_slice(&[0x7F, 0x02, 0x00, 0xDF, 0x01, 0x00]);
    insert(&media, &drive_row("image/webp", &bytes, &blobs));
    let out = run_transcode_job(&media, &blobs, "file01", &test_config("ffmpeg", true)).unwrap();
    match out {
        TranscodeOutcome::Ready(commit) => {
            assert_eq!(commit.mime, "image/webp");
            assert_eq!((commit.width, commit.height), (Some(640), Some(480)));
            let row = media.get("file01").unwrap().unwrap();
            assert_eq!(
                (row.properties.width, row.properties.height),
                (Some(640), Some(480))
            );
            // Servable preview: the file itself (no thumbnail route yet).
            assert_eq!(row.thumbnail_url, Some(row.url.clone()));
        }
        other => panic!("expected Ready, got {other:?}"),
    }
    // Second run converges without work.
    let out = run_transcode_job(&media, &blobs, "file01", &test_config("ffmpeg", true)).unwrap();
    assert!(matches!(out, TranscodeOutcome::Noop));
}

#[test]
fn gaps_transcode_undecodable_is_deterministic_failure() {
    let media = FakeMedia::default();
    let blobs = FakeBlobs::default();
    insert(&media, &drive_row("image/webp", b"RIFF\x00broken", &blobs));
    let out = run_transcode_job(&media, &blobs, "file01", &test_config("ffmpeg", true)).unwrap();
    match out {
        TranscodeOutcome::FailedDeterministic(reason) => {
            assert!(reason.contains("could not be verified"), "{reason}");
        }
        other => panic!("expected FailedDeterministic, got {other:?}"),
    }
    // No dims fabricated on the row.
    let row = media.get("file01").unwrap().unwrap();
    assert_eq!(row.properties.width, None);
}

#[test]
fn gaps_transcode_disabled_toolchain_is_loud_error() {
    let media = FakeMedia::default();
    let blobs = FakeBlobs::default();
    insert(
        &media,
        &drive_row("image/gif", b"GIF89a\x01\x00\x01\x00\x00\x00\x00;", &blobs),
    );
    // `--no-transcode` routes around the module; a stray job errors loudly.
    let err =
        run_transcode_job(&media, &blobs, "file01", &test_config("ffmpeg", false)).unwrap_err();
    assert!(matches!(err, PortError::StorageUnavailable(_)), "{err:?}");
}

#[test]
fn gaps_transcode_missing_binary_leaves_original_served() {
    let media = FakeMedia::default();
    let blobs = FakeBlobs::default();
    let original = b"GIF89a\x01\x00\x01\x00\x00\x00\x00;".to_vec();
    insert(&media, &drive_row("image/gif", &original, &blobs));
    // Enabled but the binary is bogus → toolchain check fails → retryable.
    let err = run_transcode_job(
        &media,
        &blobs,
        "file01",
        &test_config("/nonexistent/pinetree-ffmpeg-xyz", true),
    )
    .unwrap_err();
    assert!(matches!(err, PortError::StorageUnavailable(_)), "{err:?}");
    // Original row untouched: nothing partial, original bytes still served.
    let row = media.get("file01").unwrap().unwrap();
    assert_eq!(row.mime, "image/gif");
    assert_eq!(blobs.get(&row.blob_key).unwrap().unwrap(), original);
}

#[test]
fn gaps_transcode_storage_fault_retries() {
    let media = FakeMedia::default();
    let blobs = FakeBlobs::default();
    media.fail_next();
    let err =
        run_transcode_job(&media, &blobs, "file01", &test_config("ffmpeg", true)).unwrap_err();
    assert!(matches!(err, PortError::StorageUnavailable(_)), "{err:?}");
}

#[test]
fn gaps_transcode_missing_bytes_is_deterministic() {
    let media = FakeMedia::default();
    let blobs = FakeBlobs::default();
    // Row whose blob key has no bytes: deterministic, never a retry loop.
    let mut meta = drive_row("image/webp", b"RIFFdummy", &blobs);
    meta.blob_key = "00".repeat(32);
    insert(&media, &meta);
    let out = run_transcode_job(&media, &blobs, "file01", &test_config("ffmpeg", true)).unwrap();
    assert!(
        matches!(out, TranscodeOutcome::FailedDeterministic(_)),
        "{out:?}"
    );
}

// ---------------------------------------------------------------------------
// Real-ffmpeg runs (gated: skip loudly without the toolchain).
// ---------------------------------------------------------------------------

/// Generate test input locally with lavfi (no network, no fixtures).
/// Returns the working ffmpeg binary plus the PNG bytes.
fn lavfi_png(size: &str) -> Option<(String, Vec<u8>)> {
    for cand in ["ffmpeg", "/usr/sbin/ffmpeg", "/usr/bin/ffmpeg"] {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("gaps-wp1-{}.png", std::process::id()));
        let status = Command::new(cand)
            .args([
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-f",
                "lavfi",
                "-i",
                &format!("testsrc=size={size}:rate=1:duration=0.2"),
                "-frames:v",
                "1",
                &path.to_string_lossy(),
            ])
            .status();
        if status.is_ok_and(|s| s.success()) {
            let bytes = std::fs::read(&path).ok();
            let _ = std::fs::remove_file(&path);
            if let Some(bytes) = bytes {
                if bytes.starts_with(&[0x89, b'P', b'N', b'G']) {
                    return Some((cand.to_string(), bytes));
                }
            }
        }
    }
    None
}

#[test]
fn gaps_transcode_png_to_webp_with_real_ffmpeg() {
    let Some((bin, input_bytes)) = lavfi_png("64x48") else {
        eprintln!("SKIP gaps_transcode_png_to_webp_with_real_ffmpeg: no ffmpeg on PATH");
        return;
    };
    let config = test_config(&bin, true);
    let (derived, stats) = transcode_bytes(
        &input_bytes,
        "png",
        TranscodePlan::ImageToWebp,
        false,
        &config,
    )
    .expect("png->webp");
    assert!(derived.starts_with(b"RIFF"), "must be WebP");
    assert_eq!(&derived[8..12], b"WEBP");
    assert_eq!(stats.output_bytes, derived.len() as u64);
    // `-map_metadata 0` never strips: the command carries it (checked in
    // argv unit tests); here the derived dims honestly parse.
    let processed =
        pinetree::media::processing::process("image/webp", &derived).expect("parse derived");
    assert_eq!((processed.width, processed.height), (Some(64), Some(48)));
}

#[test]
fn gaps_transcode_full_job_with_real_toolchain() {
    // Full `run_transcode_job` (toolchain validation included): runs only
    // where the box carries every required encoder/decoder.
    if validate_toolchain("ffmpeg").is_err() {
        eprintln!(
            "SKIP gaps_transcode_full_job_with_real_toolchain: incomplete toolchain on this box"
        );
        return;
    }
    let Some((_bin, input_bytes)) = lavfi_png("64x48") else {
        eprintln!("SKIP gaps_transcode_full_job_with_real_toolchain: no ffmpeg on PATH");
        return;
    };
    let media = FakeMedia::default();
    let blobs = FakeBlobs::default();
    insert(&media, &drive_row("image/png", &input_bytes, &blobs));
    let config = test_config("ffmpeg", true);
    match run_transcode_job(&media, &blobs, "file01", &config).unwrap() {
        TranscodeOutcome::Ready(commit) => {
            assert_eq!(commit.mime, "image/webp");
            assert!(commit.preview_committed);
            assert_eq!((commit.width, commit.height), (Some(64), Some(48)));
            let row = media.get("file01").unwrap().unwrap();
            assert_eq!(row.mime, "image/webp");
            assert_eq!(row.size_bytes, commit.size_bytes);
            assert_eq!(
                blobs.get(&row.blob_key).unwrap().unwrap().len() as u64,
                row.size_bytes
            );
            // Re-run converges: target shape + dims → Noop.
            let again = run_transcode_job(&media, &blobs, "file01", &config).unwrap();
            assert!(matches!(again, TranscodeOutcome::Noop), "{again:?}");
        }
        other => panic!("expected Ready, got {other:?}"),
    }
}
