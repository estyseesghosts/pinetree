//! Media transcoding (gaps WP1): ffmpeg-backed post-commit job, ported from
//! `birchtree/src/media/transcode.rs` and adapted to pinetree's Misskey
//! `DriveFile` model (`MediaMeta`: `url`, `thumbnail_url`, `properties`).
//!
//! Decision table (gaps WP1):
//!
//! - GIF (animated or not) → WebM/VP9, wire type `video` (`gifv` never).
//! - Video → WebM/VP9 + Opus (D3 passthrough bound is codec-based: VP9 video
//!   with Opus audio or no audio within the upload size limit keeps its
//!   codecs — byte-identical when already WebM, stream-copied (`-c copy`, no
//!   re-encode) into WebM otherwise, so served bytes are always WebM).
//! - Stills (JPEG, PNG, BMP, HEIC/HEIF, TIFF, AVIF) → WebP (WebP passes
//!   through; the `small` preview is WebP too).
//! - Audio is not transcoded and keeps the legacy processing path.
//! - Metadata is never stripped: every ffmpeg command carries
//!   `-map_metadata 0` and no strip filter. Orientation is kept as a tag,
//!   never applied to pixels (clients apply it).
//!
//! Architecture rules (gaps WP1):
//!
//! - No ffmpeg or outbound I/O inside a request transaction. Transcoding is a
//!   post-commit durable job: the upload commits metadata + job atomically,
//!   the job runs ffmpeg outside any transaction, stages the derived blob,
//!   and commits the result last.
//! - Crash safety: ffmpeg writes to a temp file outside the blob store; the
//!   derived bytes are written to the content-addressed [`BlobStore`] under
//!   their SHA-256 key, read back and verified, and the row is re-committed
//!   under the same id pointing at the new bytes last. A crash before commit
//!   leaves the original row untouched (original bytes stay served; the
//!   orphan derived bytes are content-addressed garbage the reconciler can
//!   sweep) and a bounded re-run converges: ffmpeg sniffs content, so
//!   re-transcoding already-derived bytes is still valid output.
//! - Idempotency: a missing row is a no-op. Rows already in target shape
//!   converge through the same code path (re-transcode of derived bytes is
//!   valid). Storage faults (`get`/`put`) return `Err` so the job retries up
//!   to its budget and then quarantines; deterministic input failures (bad
//!   decode, ffmpeg error, wall-time/output-size/memory breach) report
//!   `Failed`, which the API surfaces as `422`.
//! - A missing or non-working ffmpeg binary is a startup configuration error
//!   (see [`validate_toolchain`]), never a silent passthrough. When
//!   transcoding is enabled, a row that needs transcode and has no working
//!   toolchain fails loudly instead of serving the original.
//!
//! Limits: wall-time, output-size and RSS caps per job ([`TranscodeLimits`]).
//! Quality settings: WebP `-quality 80`; VP9 constrained quality
//! `-crf 30 -b:v 0` with `-cpu-used 2 -row-mt 1`; Opus `-b:a 96k` with source
//! channel layout.
//!
//! Adaptation notes vs birchtree (whose blob store is keyed by `file_key`
//! with deterministic `{dir}/derived.{ext}` staging siblings):
//!
//! - pinetree's [`BlobStore`] is content-addressed (SHA-256 key ⇒ bytes), so
//!   "replace the original blob (no pointer swap)" is impossible: identical
//!   bytes share one key and `put` skips same-length writes. The derived blob
//!   is therefore stored under its own SHA-256 key and the row is
//!   re-committed (same id) pointing at the new key — a row-level pointer
//!   swap instead of a byte-level overwrite. The old bytes stay stored while
//!   any row (e.g. a cross-owner dedup sharer) references them; bytes left
//!   referenced by no row are reconciler sweep candidates.
//! - pinetree has no `MediaState` column yet (`pending`/`processing`/`ready`/
//!   `failed`); the durable commit method is proposed to the parent (see the
//!   WP1 final report). Until it lands, the job handler in
//!   [`crate::jobs::transcode`] reports
//!   [`TranscodeOutcome`](crate::jobs::transcode::TranscodeOutcome) and the
//!   terminal `Failed` state is carried by the quarantined job record.
//!
//! [`BlobStore`]: crate::application::ports::BlobStore

use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Configuration.
// ---------------------------------------------------------------------------

/// Default upload size cap: 16 MiB (matches `Config::max_file_size` only when
/// the operator leaves both at default; the live bound is copied from config
/// at startup via the proposed `Config::transcode_config`).
pub const DEFAULT_MAX_FILE_SIZE: u64 = 16 * 1024 * 1024;

/// Default per-job derived-output cap: 64 MiB.
pub const DEFAULT_MAX_OUTPUT_BYTES: u64 = 64 * 1024 * 1024;

/// Default per-job wall-time: 120 s.
pub const DEFAULT_WALL_TIME_SECS: u64 = 120;

/// Default per-job resident-memory cap: 1 GiB.
pub const DEFAULT_MAX_MEMORY_BYTES: u64 = 1024 * 1024 * 1024;

/// Encoders the startup check requires (`ffmpeg -encoders` names).
pub const REQUIRED_ENCODERS: &[&str] = &["libvpx-vp9", "libopus", "libwebp"];

/// Decoders the startup check requires (`ffmpeg -decoders` names): every
/// accepted still/video/audio input named by gaps WP1, plus TIFF and HEVC
/// (HEIC/HEIF readiness).
pub const REQUIRED_DECODERS: &[&str] = &[
    "png", "mjpeg", "gif", "bmp", "webp", "tiff", "h264", "hevc", "mpeg4", "vp8", "vp9",
    "aac", "opus", "vorbis", "mp3", "flac",
];

/// Transcoding configuration, built from config at startup (see the WP1
/// proposal for `Config`: CLI/file knobs for quality (`webp_quality`,
/// `vp9_crf`, `vp9_cpu_used`, `opus_bitrate_kbps`), caps (`max_file_size`,
/// `max_output_bytes`, `max_memory_bytes`, wall time) and the toolchain
/// path).
#[derive(Debug, Clone)]
pub struct TranscodeConfig {
    /// Master switch. Validation enforces the toolchain only when set; the
    /// upload-path hook routes around this module entirely when unset.
    pub enabled: bool,
    /// ffmpeg binary (absolute path or `PATH` lookup, e.g. `"ffmpeg"`).
    pub ffmpeg_path: String,
    /// libwebp `-quality` (0–100).
    pub webp_quality: u8,
    /// VP9 `-crf` (0–63, constrained-quality mode with `-b:v 0`).
    pub vp9_crf: u8,
    /// VP9 `-cpu-used` (0–5; higher is faster and slightly worse).
    pub vp9_cpu_used: u8,
    /// Opus `-b:a` in kbit/s.
    pub opus_bitrate_kbps: u32,
    /// Per-job wall-time in seconds.
    pub wall_time_secs: u64,
    /// Per-job derived-output cap in bytes.
    pub max_output_bytes: u64,
    /// Per-job resident-memory (RSS) cap in bytes (enforced by polling
    /// `/proc/<pid>/status` on Linux; best-effort elsewhere).
    pub max_memory_bytes: u64,
    /// Upload size cap in bytes (VP9/WebM passthrough bound).
    pub max_file_size: u64,
}

impl Default for TranscodeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            ffmpeg_path: "ffmpeg".to_string(),
            webp_quality: 80,
            vp9_crf: 30,
            vp9_cpu_used: 2,
            opus_bitrate_kbps: 96,
            wall_time_secs: DEFAULT_WALL_TIME_SECS,
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
            max_memory_bytes: DEFAULT_MAX_MEMORY_BYTES,
            max_file_size: DEFAULT_MAX_FILE_SIZE,
        }
    }
}

impl TranscodeConfig {
    /// Per-job enforcement caps.
    pub fn limits(&self) -> TranscodeLimits {
        TranscodeLimits {
            wall_time: Duration::from_secs(self.wall_time_secs.max(1)),
            max_output_bytes: self.max_output_bytes,
            max_memory_bytes: self.max_memory_bytes,
        }
    }

    /// Validate the configuration. When transcoding is enabled this runs the
    /// toolchain check ([`validate_toolchain`]): a missing or non-working
    /// ffmpeg is a startup error, never a silent passthrough. Returns the
    /// probed toolchain on success.
    pub fn validate(&self) -> std::result::Result<ToolchainInfo, String> {
        if self.webp_quality > 100 {
            return Err(format!("webp_quality {} out of range 0-100", self.webp_quality));
        }
        if self.vp9_crf > 63 {
            return Err(format!("vp9_crf {} out of range 0-63", self.vp9_crf));
        }
        if self.vp9_cpu_used > 5 {
            return Err(format!(
                "vp9_cpu_used {} out of range 0-5",
                self.vp9_cpu_used
            ));
        }
        if self.opus_bitrate_kbps == 0 {
            return Err("opus_bitrate_kbps must be > 0".to_string());
        }
        if self.wall_time_secs == 0 {
            return Err("transcode wall-time must be > 0".to_string());
        }
        if self.max_output_bytes == 0 {
            return Err("transcode max_output_bytes must be > 0".to_string());
        }
        if self.max_memory_bytes == 0 {
            return Err("transcode max_memory_bytes must be > 0".to_string());
        }
        if self.max_file_size == 0 {
            return Err("max_file_size must be > 0".to_string());
        }
        if !self.enabled {
            return Ok(ToolchainInfo::disabled());
        }
        validate_toolchain(&self.ffmpeg_path)
    }
}

/// Per-job enforcement caps.
#[derive(Debug, Clone, Copy)]
pub struct TranscodeLimits {
    pub wall_time: Duration,
    pub max_output_bytes: u64,
    pub max_memory_bytes: u64,
}

// ---------------------------------------------------------------------------
// Startup validation.
// ---------------------------------------------------------------------------

/// A working ffmpeg toolchain (probed at startup).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolchainInfo {
    pub path: String,
    /// First line of `ffmpeg -version` (e.g. `ffmpeg version 7.1 ...`).
    pub version: String,
    /// False when transcoding is disabled (no binary was probed).
    pub enabled: bool,
}

impl ToolchainInfo {
    fn disabled() -> Self {
        Self {
            path: String::new(),
            version: String::new(),
            enabled: false,
        }
    }
}

/// Check that the ffmpeg binary runs and carries the VP9/Opus/WebP encoders
/// plus every decoder the accepted inputs need. Any missing component is an
/// `Err` listing it: startup must fail rather than silently pass media
/// through.
pub fn validate_toolchain(path: &str) -> std::result::Result<ToolchainInfo, String> {
    if path.trim().is_empty() {
        return Err("ffmpeg path must not be empty".to_string());
    }
    let version_out = Command::new(path)
        .arg("-hide_banner")
        .arg("-version")
        .output()
        .map_err(|err| format!("ffmpeg binary {path:?} could not be executed: {err}"))?;
    if !version_out.status.success() {
        return Err(format!(
            "ffmpeg binary {path:?} exited with status {}",
            version_out.status
        ));
    }
    let version = String::from_utf8_lossy(&version_out.stdout)
        .lines()
        .next()
        .unwrap_or("")
        .trim()
        .chars()
        .take(120)
        .collect::<String>();
    if !version.contains("ffmpeg version") {
        return Err(format!(
            "ffmpeg binary {path:?} did not report an ffmpeg version line"
        ));
    }
    let encoders = list_names(path, "-encoders")
        .map_err(|err| format!("ffmpeg {path:?} -encoders failed: {err}"))?;
    let mut missing: Vec<String> = REQUIRED_ENCODERS
        .iter()
        .filter(|name| !encoders.iter().any(|have| have.as_str() == **name))
        .map(|name| format!("encoder {name}"))
        .collect();
    let decoders = list_names(path, "-decoders")
        .map_err(|err| format!("ffmpeg {path:?} -decoders failed: {err}"))?;
    missing.extend(
        REQUIRED_DECODERS
            .iter()
            .filter(|name| !decoders.iter().any(|have| have.as_str() == **name))
            .map(|name| format!("decoder {name}")),
    );
    if !missing.is_empty() {
        return Err(format!(
            "ffmpeg binary {path:?} is missing required components: {}",
            missing.join(", ")
        ));
    }
    Ok(ToolchainInfo {
        path: path.to_string(),
        version,
        enabled: true,
    })
}

/// Parse an `ffmpeg -encoders` / `-decoders` listing into component names
/// (the second whitespace-separated field of each entry line).
fn list_names(path: &str, flag: &str) -> std::result::Result<Vec<String>, String> {
    let out = Command::new(path)
        .arg("-hide_banner")
        .arg(flag)
        .output()
        .map_err(|err| err.to_string())?;
    if !out.status.success() {
        return Err(format!("exit status {}", out.status));
    }
    Ok(String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            fields.next()?;
            fields.next().map(str::to_string)
        })
        .collect())
}

// ---------------------------------------------------------------------------
// Transcode decision.
// ---------------------------------------------------------------------------

/// What a stored upload needs. The video codec bound (VP9 + Opus-or-silent
/// within the size limit) is decided at job time by probing the bytes
/// ([`probe_video`]); WebM keeps its bytes, other containers remux. The
/// static plan only says the target shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TranscodePlan {
    /// Already in target shape, or a class transcoding never touches
    /// (audio, unknown): keep the legacy processing path.
    Passthrough,
    /// Still image → WebP (wire type stays `image`).
    ImageToWebp,
    /// GIF (animated or not) → WebM/VP9 (wire type becomes `video`).
    GifToWebm,
    /// Video → WebM/VP9 + Opus (wire type stays `video`). At job time the
    /// bytes are probed first: VP9 + Opus-or-silent within the size limit
    /// keeps its codecs — byte-identical when already WebM, stream-copied
    /// (`-c copy`, no re-encode) into WebM otherwise.
    VideoToWebm,
    /// Job-time refinement of [`TranscodePlan::VideoToWebm`], never returned
    /// by [`plan_for_mime`]: codecs are already VP9 + Opus-or-silent but the
    /// container is not WebM, so remux with stream copy (no re-encode, no
    /// quality loss) into WebM.
    VideoRemux,
}

/// Static plan for a sniffed content type. Probe-dependent passthrough (VP9
/// WebM) still reports [`TranscodePlan::VideoToWebm`]; the job probes the
/// bytes before deciding.
pub fn plan_for_mime(mime_type: &str) -> TranscodePlan {
    match mime_type {
        "image/gif" => TranscodePlan::GifToWebm,
        // Stills → WebP. `image/x-tiff` is the legacy alias for TIFF (the
        // sniffer reports canonical `image/tiff`); both spellings map here
        // so a row carrying either one still converges to WebP.
        "image/jpeg" | "image/png" | "image/bmp" | "image/heic" | "image/heif"
        | "image/tiff" | "image/x-tiff" | "image/avif" => TranscodePlan::ImageToWebp,
        "image/webp" => TranscodePlan::Passthrough,
        mime if mime.starts_with("video/") => TranscodePlan::VideoToWebm,
        _ => TranscodePlan::Passthrough,
    }
}

/// True when the upload needs an ffmpeg transcode (as opposed to the legacy
/// pure-Rust path). A GIF is sniffed `image/*` but still defers.
pub fn needs_transcode(mime_type: &str) -> bool {
    !matches!(plan_for_mime(mime_type), TranscodePlan::Passthrough)
}

/// Canonical extension of a plan's derived blob.
pub fn derived_extension(plan: TranscodePlan) -> &'static str {
    match plan {
        TranscodePlan::ImageToWebp | TranscodePlan::Passthrough => "webp",
        TranscodePlan::GifToWebm | TranscodePlan::VideoToWebm | TranscodePlan::VideoRemux => "webm",
    }
}

/// MIME type of a plan's derived blob (what the re-committed row carries).
pub fn derived_mime(plan: TranscodePlan) -> &'static str {
    match plan {
        TranscodePlan::ImageToWebp | TranscodePlan::Passthrough => "image/webp",
        TranscodePlan::GifToWebm | TranscodePlan::VideoToWebm | TranscodePlan::VideoRemux => {
            "video/webm"
        }
    }
}

// ---------------------------------------------------------------------------
// Stream probing (`ffmpeg -i` stderr parsing).
// ---------------------------------------------------------------------------

/// Stream facts parsed from `ffmpeg -i` stderr. `None` fields are honest
/// unknowns, never fabricated.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Probe {
    /// First video stream codec (`vp9`, `h264`, ...).
    pub video_codec: Option<String>,
    /// First audio stream codec (`opus`, `aac`, ...).
    pub audio_codec: Option<String>,
    /// Container format list from the `Input #0, ...` line
    /// (`matroska,webm`, `mov,mp4,m4a,3gp,3g2,mj2`, ...).
    pub container: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub duration_ms: Option<u64>,
}

impl Probe {
    /// True when the probed container is WebM (`Input #0` format list
    /// contains `webm`). An unknown container is never WebM.
    pub fn container_is_webm(&self) -> bool {
        self.container
            .as_deref()
            .is_some_and(|formats| formats.split(',').any(|f| f.trim() == "webm"))
    }
}

/// Codec/size bound for video passthrough: VP9 video, Opus-or-silent audio,
/// within the upload size limit. Positive identification only — an
/// inconclusive probe transcodes (never a silent passthrough of unknown
/// bytes). The container is decided separately: WebM keeps its bytes (see
/// [`Probe::container_is_webm`]), any other container is remuxed with
/// stream-copy into WebM (no re-encode).
pub fn probe_is_passthrough(probe: &Probe, file_size: u64, max_file_size: u64) -> bool {
    file_size <= max_file_size
        && probe.video_codec.as_deref() == Some("vp9")
        && matches!(probe.audio_codec.as_deref(), None | Some("opus"))
}

/// Parse `ffmpeg -hide_banner -i <input>` stderr.
pub fn parse_probe_stderr(stderr: &str) -> Probe {
    let mut probe = Probe::default();
    for line in stderr.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("Duration:") {
            probe.duration_ms = parse_duration_ms(rest);
        }
        if let Some(rest) = line.strip_prefix("Input #") {
            // `Input #0, matroska,webm, from 'in.webm':` → `matroska,webm`.
            if probe.container.is_none()
                && let Some(formats) = line_formats(rest)
            {
                probe.container = Some(formats);
            }
            continue;
        }
        if !line.contains("Stream #") {
            continue;
        }
        if line.contains("Video:") && probe.video_codec.is_none() {
            probe.video_codec = codec_after(line, "Video:");
            if probe.width.is_none() {
                let (width, height) = parse_dims_after(line, "Video:");
                probe.width = width;
                probe.height = height;
            }
        } else if line.contains("Audio:") && probe.audio_codec.is_none() {
            probe.audio_codec = codec_after(line, "Audio:");
        }
    }
    probe
}

/// `Input #0, <formats>, from ...` → lowercase `<formats>`.
fn line_formats(rest: &str) -> Option<String> {
    let (_, tail) = rest.split_once(',')?;
    let (head, _) = tail.rsplit_once(", from").or_else(|| tail.rsplit_once(" from"))?;
    let formats = head.trim();
    if formats.is_empty() {
        return None;
    }
    Some(formats.to_lowercase())
}

/// Codec token after a `Video:`/`Audio:` marker (`vp9 (Profile 0), ...` →
/// `vp9`; lowercased).
fn codec_after(line: &str, marker: &str) -> Option<String> {
    let rest = line.split(marker).nth(1)?;
    let token = rest
        .trim()
        .split([',', ' ', '('])
        .next()?
        .trim()
        .to_lowercase();
    (!token.is_empty()).then_some(token)
}

/// First `WIDTHxHEIGHT` pair after the marker.
fn parse_dims_after(line: &str, marker: &str) -> (Option<u32>, Option<u32>) {
    let rest = line.split(marker).nth(1).unwrap_or("");
    let bytes = rest.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i].is_ascii_digit() {
            let start = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            if i < bytes.len() && bytes[i] == b'x' {
                if let Ok(width) = rest[start..i].parse::<u32>() {
                    let mut j = i + 1;
                    let height_start = j;
                    while j < bytes.len() && bytes[j].is_ascii_digit() {
                        j += 1;
                    }
                    if j > height_start
                        && let Ok(height) = rest[height_start..j].parse::<u32>()
                        && width > 0
                        && height > 0
                    {
                        return (Some(width), Some(height));
                    }
                }
            }
        } else {
            i += 1;
        }
    }
    (None, None)
}

/// `HH:MM:SS.ms` (ffmpeg `Duration:` field) → milliseconds. `N/A` → `None`.
pub fn parse_duration_ms(field: &str) -> Option<u64> {
    let token = field.trim().split([',', ' ']).next()?.trim();
    if token == "N/A" {
        return None;
    }
    let (hms, ms) = match token.split_once('.') {
        Some((hms, ms)) => (hms, ms),
        None => (token, "0"),
    };
    let mut parts = hms.split(':');
    let hours: u64 = parts.next()?.parse().ok()?;
    let minutes: u64 = parts.next()?.parse().ok()?;
    let seconds: u64 = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    let millis: u64 = format!("{ms:0<3}")
        .chars()
        .take(3)
        .collect::<String>()
        .parse()
        .ok()?;
    Some(hours * 3_600_000 + minutes * 60_000 + seconds * 1_000 + millis)
}

/// Probe a file with `<ffmpeg> -hide_banner -i <input>` and parse stderr.
/// The exit status is ignored (plain `-i` prints the facts either way). A
/// spawn failure is an `Err`, and the caller must treat it as "not provably
/// passthrough" → transcode. The input is always a local temp file, so the
/// probe cannot block on the network.
pub fn probe_file(ffmpeg_path: &str, input: &Path) -> std::result::Result<Probe, String> {
    let out = Command::new(ffmpeg_path)
        .arg("-hide_banner")
        .arg("-i")
        .arg(input)
        .output()
        .map_err(|err| format!("probe spawn failed: {err}"))?;
    let stderr = String::from_utf8_lossy(&out.stderr);
    Ok(parse_probe_stderr(&stderr))
}

// ---------------------------------------------------------------------------
// ffmpeg command construction.
// ---------------------------------------------------------------------------

/// Build the ffmpeg argv for a plan. Contract (relied on by tests):
/// `-i` immediately precedes the input path, and the output path is the
/// final argument. Every command carries `-map_metadata 0` and no
/// strip/normalize filter: metadata (including EXIF and the orientation tag)
/// rides along where the target format holds it.
pub fn build_argv(
    ffmpeg_path: &str,
    input: &Path,
    output: &Path,
    plan: TranscodePlan,
    with_audio: bool,
    config: &TranscodeConfig,
) -> Vec<String> {
    let mut argv = vec![
        ffmpeg_path.to_string(),
        "-hide_banner".to_string(),
        "-loglevel".to_string(),
        "error".to_string(),
        "-y".to_string(),
        "-i".to_string(),
        input.to_string_lossy().into_owned(),
        "-map_metadata".to_string(),
        "0".to_string(),
    ];
    match plan {
        TranscodePlan::ImageToWebp | TranscodePlan::Passthrough => {
            argv.extend([
                "-c:v".to_string(),
                "libwebp".to_string(),
                "-quality".to_string(),
                config.webp_quality.to_string(),
                "-compression_level".to_string(),
                "4".to_string(),
            ]);
        }
        TranscodePlan::GifToWebm => {
            argv.extend([
                "-an".to_string(),
                "-c:v".to_string(),
                "libvpx-vp9".to_string(),
                "-crf".to_string(),
                config.vp9_crf.to_string(),
                "-b:v".to_string(),
                "0".to_string(),
                "-pix_fmt".to_string(),
                // GIF transparency survives as WebM alpha.
                "yuva420p".to_string(),
                "-cpu-used".to_string(),
                config.vp9_cpu_used.to_string(),
                "-row-mt".to_string(),
                "1".to_string(),
            ]);
        }
        TranscodePlan::VideoToWebm => {
            argv.extend([
                "-map".to_string(),
                "0:v:0".to_string(),
                "-c:v".to_string(),
                "libvpx-vp9".to_string(),
                "-crf".to_string(),
                config.vp9_crf.to_string(),
                "-b:v".to_string(),
                "0".to_string(),
                "-pix_fmt".to_string(),
                "yuv420p".to_string(),
                "-cpu-used".to_string(),
                config.vp9_cpu_used.to_string(),
                "-row-mt".to_string(),
                "1".to_string(),
            ]);
            if with_audio {
                argv.extend([
                    "-map".to_string(),
                    "0:a:0".to_string(),
                    "-c:a".to_string(),
                    "libopus".to_string(),
                    "-b:a".to_string(),
                    format!("{}k", config.opus_bitrate_kbps),
                ]);
            } else {
                argv.push("-an".to_string());
            }
        }
        TranscodePlan::VideoRemux => {
            // Stream copy into the output container (`.webm` via
            // `derived_extension`): no re-encode, no quality loss, metadata
            // rides along with `-map_metadata 0`. `with_audio` is ignored —
            // every stream is copied.
            argv.extend([
                "-map".to_string(),
                "0".to_string(),
                "-c".to_string(),
                "copy".to_string(),
            ]);
        }
    }
    argv.push(output.to_string_lossy().into_owned());
    argv
}

/// Build the ffmpeg argv for the `small` still-image preview: the derived
/// WebP bytes scaled to fit inside a `bound`×`bound` box, encoded WebP at
/// the configured quality. Same `-map_metadata 0` contract as the main
/// command.
pub fn build_preview_argv(
    ffmpeg_path: &str,
    input: &Path,
    output: &Path,
    bound: u32,
    config: &TranscodeConfig,
) -> Vec<String> {
    let argv = vec![
        ffmpeg_path.to_string(),
        "-hide_banner".to_string(),
        "-loglevel".to_string(),
        "error".to_string(),
        "-y".to_string(),
        "-i".to_string(),
        input.to_string_lossy().into_owned(),
        "-map_metadata".to_string(),
        "0".to_string(),
        "-vf".to_string(),
        format!("scale='min({bound},iw)':'min({bound},ih)':eval=frame"),
        "-c:v".to_string(),
        "libwebp".to_string(),
        "-quality".to_string(),
        config.webp_quality.to_string(),
        "-compression_level".to_string(),
        "4".to_string(),
        output.to_string_lossy().into_owned(),
    ];
    argv
}

// ---------------------------------------------------------------------------
// Bounded ffmpeg execution.
// ---------------------------------------------------------------------------

/// How an ffmpeg run ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TranscodeError {
    /// The binary could not be spawned (environmental → job retries).
    Spawn(String),
    /// I/O around temp files or pipes (environmental → job retries).
    Io(String),
    /// ffmpeg exited non-zero with deterministic input (terminal → `Failed`).
    FfmpegFailed { code: String, stderr_tail: String },
    /// Wall-time breach (terminal → `Failed`).
    Timeout { wall_ms: u64 },
    /// Output-size breach, during or after the run (terminal → `Failed`).
    OutputTooLarge { bytes: u64, limit: u64 },
    /// RSS cap breach (terminal → `Failed`).
    MemoryExceeded { bytes: u64, limit: u64 },
}

impl std::fmt::Display for TranscodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TranscodeError::Spawn(detail) => write!(f, "ffmpeg could not be spawned: {detail}"),
            TranscodeError::Io(detail) => write!(f, "transcode I/O failed: {detail}"),
            TranscodeError::FfmpegFailed { code, stderr_tail } => {
                write!(f, "ffmpeg exited {code}: {stderr_tail}")
            }
            TranscodeError::Timeout { wall_ms } => {
                write!(f, "transcode exceeded its wall-time ({wall_ms} ms)")
            }
            TranscodeError::OutputTooLarge { bytes, limit } => {
                write!(f, "transcode output {bytes} bytes exceeded the {limit}-byte limit")
            }
            TranscodeError::MemoryExceeded { bytes, limit } => {
                write!(f, "transcode RSS {bytes} bytes exceeded the {limit}-byte limit")
            }
        }
    }
}

impl std::error::Error for TranscodeError {}

/// Stats of a successful run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunStats {
    pub output_bytes: u64,
    pub wall_ms: u64,
}

/// Keep the last `cap` bytes (ffmpeg diagnostics tail for failure reasons).
fn stderr_tail(stderr: &str, cap: usize) -> String {
    const ELLIPSIS: &str = "…";
    if stderr.len() <= cap {
        return stderr.to_string();
    }
    let cut = stderr.len() - cap;
    let mut start = cut;
    while start < stderr.len() && !stderr.is_char_boundary(start) {
        start += 1;
    }
    format!("{ELLIPSIS}{}", &stderr[start..])
}

/// Run `argv` with wall-time/output-size/RSS enforcement. `watch_path` is the
/// file ffmpeg writes (polled for the size cap, deleted on failure).
/// The output file must not exist on entry; on success it holds the derived
/// blob.
fn run_bounded(
    argv: &[String],
    watch_path: Option<&Path>,
    limits: &TranscodeLimits,
) -> std::result::Result<RunStats, TranscodeError> {
    if argv.is_empty() {
        return Err(TranscodeError::Spawn("empty argv".to_string()));
    }
    let mut child = Command::new(&argv[0])
        .args(&argv[1..])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| TranscodeError::Spawn(err.to_string()))?;
    let pid = child.id();
    let start = Instant::now();
    let breach = loop {
        if let Some(_status) = child
            .try_wait()
            .map_err(|err| TranscodeError::Io(format!("wait for ffmpeg: {err}")))?
        {
            break None;
        }
        let wall_ms = start.elapsed().as_millis() as u64;
        if start.elapsed() > limits.wall_time {
            break Some(TranscodeError::Timeout { wall_ms });
        }
        if let Some(path) = watch_path
            && let Ok(meta) = std::fs::metadata(path)
            && meta.len() > limits.max_output_bytes
        {
            break Some(TranscodeError::OutputTooLarge {
                bytes: meta.len(),
                limit: limits.max_output_bytes,
            });
        }
        #[cfg(target_os = "linux")]
        if let Some(rss) = rss_bytes(pid)
            && rss > limits.max_memory_bytes
        {
            break Some(TranscodeError::MemoryExceeded {
                bytes: rss,
                limit: limits.max_memory_bytes,
            });
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    if let Some(err) = breach {
        let _ = child.kill();
        let _ = child.wait();
        if let Some(path) = watch_path {
            let _ = std::fs::remove_file(path);
        }
        return Err(err);
    }
    let wall_ms = start.elapsed().as_millis() as u64;
    // The child has exited; drain the (small: `-loglevel error`) stderr pipe.
    let mut stderr_bytes = Vec::new();
    if let Some(mut piped) = child.stderr.take()
        && let Err(err) = piped.read_to_end(&mut stderr_bytes)
    {
        return Err(TranscodeError::Io(format!("read ffmpeg stderr: {err}")));
    }
    let status = child
        .wait()
        .map_err(|err| TranscodeError::Io(format!("reap ffmpeg: {err}")))?;
    if !status.success() {
        if let Some(path) = watch_path {
            let _ = std::fs::remove_file(path);
        }
        return Err(TranscodeError::FfmpegFailed {
            code: status.code().map(|c| c.to_string()).unwrap_or_else(|| "signal".to_string()),
            stderr_tail: stderr_tail(&String::from_utf8_lossy(&stderr_bytes), 2048),
        });
    }
    if let Some(path) = watch_path {
        let len = std::fs::metadata(path)
            .map(|meta| meta.len())
            .map_err(|err| TranscodeError::Io(format!("stat transcode output: {err}")))?;
        if len == 0 {
            let _ = std::fs::remove_file(path);
            return Err(TranscodeError::FfmpegFailed {
                code: "0".to_string(),
                stderr_tail: "ffmpeg succeeded but wrote an empty output".to_string(),
            });
        }
        if len > limits.max_output_bytes {
            let _ = std::fs::remove_file(path);
            return Err(TranscodeError::OutputTooLarge {
                bytes: len,
                limit: limits.max_output_bytes,
            });
        }
        Ok(RunStats {
            output_bytes: len,
            wall_ms,
        })
    } else {
        Ok(RunStats {
            output_bytes: 0,
            wall_ms,
        })
    }
}

/// Current RSS of `pid` in bytes (`None` when unreadable). Linux only; other
/// platforms skip RSS enforcement (wall-time and output-size still apply).
#[cfg(target_os = "linux")]
fn rss_bytes(pid: u32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Temp files.
// ---------------------------------------------------------------------------

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Filename prefix of transcode temp files (input/probe/output). The
/// orphan sweeper ([`sweep_tmp_files`]) keys on this prefix.
pub const TEMP_PREFIX: &str = "pinetree-transcode-";

/// A fresh temp path that never collides within or across processes.
pub(crate) fn temp_path(prefix: &str, ext: &str) -> PathBuf {
    let n = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "{TEMP_PREFIX}{prefix}-{}-{n}.{ext}",
        std::process::id()
    ))
}

/// Deletes its paths on drop (best-effort): a crash-safe temp-file guard.
struct TempGuard {
    paths: Vec<PathBuf>,
}

impl TempGuard {
    fn new(paths: Vec<PathBuf>) -> Self {
        Self { paths }
    }
}

impl Drop for TempGuard {
    fn drop(&mut self) {
        for path in &self.paths {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Maximum `/tmp` orphans removed by one [`sweep_tmp_files`] pass: the sweep
/// is bounded so a crowded temp dir cannot stall startup.
pub const TMP_SWEEP_CAP: usize = 1_000;

/// Minimum age of a `/tmp` file the sweeper removes: a running transcode
/// holds its temp files for seconds at most (wall-time cap 120 s default),
/// so one hour of grace never touches a live job while still converging
/// `SIGKILL` orphans within an operator-tolerable window.
pub const TMP_SWEEP_GRACE_MS: u64 = 3_600_000;

/// Remove stale transcode temp files (`{TEMP_PREFIX}*`) from `dir`.
///
/// A `SIGKILL` between ffmpeg spawn and `TempGuard` drop orphans the input /
/// probe / output files permanently (no guard runs on `SIGKILL`), so the
/// server calls this once at startup. Only files whose mtime is at least
/// `older_than_ms` old are removed, so a concurrently running transcode's
/// fresh files are never touched; entries whose age cannot be read are kept
/// for the next pass, never fatal. At most `cap` files (clamped to
/// [`TMP_SWEEP_CAP`]) are removed per call, so a crowded temp dir cannot
/// stall startup. Returns the number of files removed.
pub fn sweep_tmp_files(dir: &Path, older_than_ms: u64, cap: usize) -> usize {
    let cap = cap.min(TMP_SWEEP_CAP);
    let entries = std::fs::read_dir(dir)
        .map(|read| read.collect::<Vec<_>>())
        .unwrap_or_default();
    let mut removed = 0usize;
    for entry in entries {
        if removed >= cap {
            break;
        }
        let Ok(entry) = entry else { continue };
        if !entry.file_name().to_string_lossy().starts_with(TEMP_PREFIX) {
            continue;
        }
        let age_ms: Option<u64> = entry
            .metadata()
            .and_then(|meta| meta.modified())
            .ok()
            .and_then(|modified| modified.elapsed().ok())
            .and_then(|age| u64::try_from(age.as_millis()).ok());
        match age_ms {
            Some(age) if age >= older_than_ms => {
                if std::fs::remove_file(entry.path()).is_ok() {
                    removed += 1;
                }
            }
            _ => continue,
        }
    }
    removed
}

/// Transcode `input_bytes` (extension `input_ext`, a hint for ffmpeg's
/// demuxer only) into the plan's derived bytes.
pub fn transcode_bytes(
    input_bytes: &[u8],
    input_ext: &str,
    plan: TranscodePlan,
    with_audio: bool,
    config: &TranscodeConfig,
) -> std::result::Result<(Vec<u8>, RunStats), TranscodeError> {
    let input_path = temp_path("in", input_ext);
    let output_path = temp_path("out", derived_extension(plan));
    let _guard = TempGuard::new(vec![input_path.clone(), output_path.clone()]);
    std::fs::write(&input_path, input_bytes)
        .map_err(|err| TranscodeError::Io(format!("write transcode input: {err}")))?;
    let argv = build_argv(
        &config.ffmpeg_path,
        &input_path,
        &output_path,
        plan,
        with_audio,
        config,
    );
    let run_stats = run_bounded(&argv, Some(&output_path), &config.limits())?;
    let derived = std::fs::read(&output_path)
        .map_err(|err| TranscodeError::Io(format!("read transcode output: {err}")))?;
    let stats = RunStats {
        output_bytes: derived.len() as u64,
        wall_ms: run_stats.wall_ms,
    };
    Ok((derived, stats))
}

/// Transcode `input_bytes` and then render the `small` still-image preview
/// (derived WebP scaled to fit inside a `bound`×`bound` box, WebP at the
/// configured quality). Returns `(derived, preview)`. A preview-pass
/// failure maps to the same [`TranscodeError`] taxonomy as the main pass
/// (environmental → retry; deterministic → `Failed`).
pub fn transcode_still_with_preview(
    input_bytes: &[u8],
    input_ext: &str,
    bound: u32,
    config: &TranscodeConfig,
) -> std::result::Result<(Vec<u8>, Vec<u8>), TranscodeError> {
    let (derived, _) =
        transcode_bytes(input_bytes, input_ext, TranscodePlan::ImageToWebp, false, config)?;
    let input_path = temp_path("prev-in", "webp");
    let output_path = temp_path("prev-out", "webp");
    let _guard = TempGuard::new(vec![input_path.clone(), output_path.clone()]);
    std::fs::write(&input_path, &derived)
        .map_err(|err| TranscodeError::Io(format!("write preview input: {err}")))?;
    let argv = build_preview_argv(&config.ffmpeg_path, &input_path, &output_path, bound, config);
    run_bounded(&argv, Some(&output_path), &config.limits())?;
    let preview = std::fs::read(&output_path)
        .map_err(|err| TranscodeError::Io(format!("read preview output: {err}")))?;
    Ok((derived, preview))
}

/// Probe `bytes` for the D3 codec bound. Returns the probe when the codecs
/// are already VP9 + Opus-or-silent within the size limit (`None` otherwise,
/// including any probe failure → the job transcodes). The caller splits
/// WebM (byte passthrough) from other containers (stream-copy remux).
pub fn probe_video(bytes: &[u8], config: &TranscodeConfig) -> Option<Probe> {
    let input_path = temp_path("probe", "vid");
    let _guard = TempGuard::new(vec![input_path.clone()]);
    std::fs::write(&input_path, bytes).ok()?;
    let probe = probe_file(&config.ffmpeg_path, &input_path).ok()?;
    probe_is_passthrough(&probe, bytes.len() as u64, config.max_file_size).then_some(probe)
}

/// Whether the original carries an audio stream (decides the `-map 0:a:0`
/// vs `-an` video command). A seen audio stream maps it; silence (or an
/// unreadable probe, conservatively treated as "maybe audio") maps the audio
/// stream too — ffmpeg then errors deterministically on a truly missing
/// stream, which commits `Failed` loudly, instead of `-an` silently dropping
/// real audio.
pub fn probe_has_audio(bytes: &[u8], config: &TranscodeConfig) -> bool {
    let input_path = temp_path("probe", "vid");
    let _guard = TempGuard::new(vec![input_path.clone()]);
    if std::fs::write(&input_path, bytes).is_err() {
        return true;
    }
    probe_file(&config.ffmpeg_path, &input_path)
        .map(|probe| probe.audio_codec.is_some())
        .unwrap_or(true)
}

/// Probe derived video bytes for commit meta (dims + duration when known).
pub fn probe_derived(
    derived: &[u8],
    config: &TranscodeConfig,
) -> (Option<u32>, Option<u32>, Option<u64>) {
    let output_path = temp_path("derived", "webm");
    let _guard = TempGuard::new(vec![output_path.clone()]);
    if std::fs::write(&output_path, derived).is_err() {
        return (None, None, None);
    }
    match probe_file(&config.ffmpeg_path, &output_path) {
        Ok(probe) => (probe.width, probe.height, probe.duration_ms),
        Err(_) => (None, None, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_for(path: &str) -> TranscodeConfig {
        TranscodeConfig {
            ffmpeg_path: path.to_string(),
            ..TranscodeConfig::default()
        }
    }

    #[test]
    fn decision_table_covers_every_input_class() {
        use TranscodePlan::{GifToWebm, ImageToWebp, Passthrough, VideoToWebm};
        assert_eq!(plan_for_mime("image/jpeg"), ImageToWebp);
        assert_eq!(plan_for_mime("image/png"), ImageToWebp);
        assert_eq!(plan_for_mime("image/bmp"), ImageToWebp);
        assert_eq!(plan_for_mime("image/heic"), ImageToWebp);
        assert_eq!(plan_for_mime("image/heif"), ImageToWebp);
        assert_eq!(plan_for_mime("image/tiff"), ImageToWebp);
        assert_eq!(plan_for_mime("image/x-tiff"), ImageToWebp);
        assert_eq!(plan_for_mime("image/avif"), ImageToWebp);
        assert_eq!(plan_for_mime("image/gif"), GifToWebm);
        assert_eq!(plan_for_mime("image/webp"), Passthrough);
        assert_eq!(plan_for_mime("video/mp4"), VideoToWebm);
        assert_eq!(plan_for_mime("video/webm"), VideoToWebm);
        // Audio and unknown types never transcode.
        assert_eq!(plan_for_mime("audio/ogg"), Passthrough);
        assert_eq!(plan_for_mime("audio/mpeg"), Passthrough);
        assert_eq!(plan_for_mime("application/pdf"), Passthrough);
        assert!(needs_transcode("image/gif"));
        assert!(needs_transcode("image/jpeg"));
        assert!(needs_transcode("image/heic"));
        assert!(needs_transcode("image/heif"));
        assert!(needs_transcode("image/tiff"));
        assert!(needs_transcode("image/avif"));
        assert!(needs_transcode("video/mp4"));
        assert!(!needs_transcode("image/webp"));
        assert!(!needs_transcode("audio/ogg"));
        assert_eq!(derived_extension(ImageToWebp), "webp");
        assert_eq!(derived_extension(GifToWebm), "webm");
        assert_eq!(derived_extension(VideoToWebm), "webm");
        assert_eq!(derived_mime(ImageToWebp), "image/webp");
        assert_eq!(derived_mime(GifToWebm), "video/webm");
        assert_eq!(derived_mime(VideoToWebm), "video/webm");
    }

    #[test]
    fn passthrough_bound_needs_vp9_opus_or_silent_within_size() {
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
        // The codec bound ignores the container: VP9+Opus in MP4 passes the
        // codec check (the job remuxes it into WebM with stream copy).
        let mp4 = Probe {
            container: Some("mov,mp4,m4a,3gp,3g2,mj2".to_string()),
            ..base.clone()
        };
        assert!(probe_is_passthrough(&mp4, 1024, DEFAULT_MAX_FILE_SIZE));
        assert!(!mp4.container_is_webm());
        // Silent is fine.
        let silent = Probe {
            audio_codec: None,
            ..base.clone()
        };
        assert!(probe_is_passthrough(&silent, 1024, DEFAULT_MAX_FILE_SIZE));
        // Non-VP9 video, non-Opus audio, and oversize files transcode.
        for probe in [
            Probe {
                video_codec: Some("h264".to_string()),
                ..base.clone()
            },
            Probe {
                audio_codec: Some("aac".to_string()),
                ..base.clone()
            },
            Probe {
                audio_codec: Some("mp3".to_string()),
                ..base.clone()
            },
            Probe {
                video_codec: None,
                ..base.clone()
            },
        ] {
            assert!(!probe_is_passthrough(&probe, 1024, DEFAULT_MAX_FILE_SIZE));
        }
        assert!(!probe_is_passthrough(&base, DEFAULT_MAX_FILE_SIZE + 1, DEFAULT_MAX_FILE_SIZE));
    }

    #[test]
    fn probe_parses_ffmpeg_i_stderr() {
        let stderr = "ffmpeg version 7.1 Copyright (c)\n\
             Input #0, matroska,webm, from 'in.webm':\n\
             Duration: 00:00:02.50, start: 0.000000, bitrate: 100 kb/s\n\
             Stream #0:0: Video: vp9 (Profile 0), yuv420p(tv, progressive), 640x480 [SAR 1:1 DAR 4:3], 25 fps\n\
             Stream #0:1(eng): Audio: opus, 48000 Hz, stereo, fltp\n";
        let probe = parse_probe_stderr(stderr);
        assert_eq!(probe.video_codec.as_deref(), Some("vp9"));
        assert_eq!(probe.audio_codec.as_deref(), Some("opus"));
        assert_eq!(probe.container.as_deref(), Some("matroska,webm"));
        assert!(probe.container_is_webm());
        assert_eq!((probe.width, probe.height), (Some(640), Some(480)));
        assert_eq!(probe.duration_ms, Some(2_500));
        // MP4 container: same codecs, not WebM.
        let mp4 = "Input #0, mov,mp4,m4a,3gp,3g2,mj2, from 'in.mp4':\n\
             Stream #0:0: Video: vp9 (Profile 0), yuv420p, 1280x720\n\
             Stream #0:1: Audio: opus, 48000 Hz, stereo\n";
        let probe = parse_probe_stderr(mp4);
        assert_eq!(probe.container.as_deref(), Some("mov,mp4,m4a,3gp,3g2,mj2"));
        assert!(!probe.container_is_webm());
        assert!(probe_is_passthrough(&probe, 1024, DEFAULT_MAX_FILE_SIZE));
        // Silent input: no audio stream, no duration.
        let silent = "  Stream #0:0: Video: h264, yuv420p, 1280x720\n";
        let probe = parse_probe_stderr(silent);
        assert_eq!(probe.video_codec.as_deref(), Some("h264"));
        assert_eq!(probe.audio_codec, None);
        assert_eq!(probe.container, None);
        assert!(!probe.container_is_webm());
        assert_eq!((probe.width, probe.height), (Some(1280), Some(720)));
        assert_eq!(probe.duration_ms, None);
        // Garbage parses to honest unknowns.
        assert_eq!(parse_probe_stderr(""), Probe::default());
    }

    #[test]
    fn remux_argv_copies_streams_without_reencode() {
        let config = TranscodeConfig::default();
        let argv = build_argv(
            "ffmpeg",
            std::path::Path::new("in.vid"),
            std::path::Path::new("out.webm"),
            TranscodePlan::VideoRemux,
            true,
            &config,
        );
        let joined = argv.join(" ");
        assert!(joined.contains("-map 0"), "{joined}");
        assert!(joined.contains("-c copy"), "{joined}");
        assert!(!joined.contains("libvpx"), "{joined}");
        assert!(!joined.contains("libopus"), "{joined}");
        assert!(joined.ends_with("out.webm"));
        assert_eq!(derived_extension(TranscodePlan::VideoRemux), "webm");
    }

    #[test]
    fn duration_parses_hh_mm_ss_ms() {
        assert_eq!(parse_duration_ms("00:00:02.50, start: 0"), Some(2_500));
        assert_eq!(parse_duration_ms("01:02:03.456, bitrate: 1"), Some(3_723_456));
        assert_eq!(parse_duration_ms("00:00:00.00,"), Some(0));
        assert_eq!(parse_duration_ms("N/A,"), None);
        assert_eq!(parse_duration_ms("garbage"), None);
    }

    #[test]
    fn argv_carries_metadata_and_quality_with_output_last() {
        let cfg = TranscodeConfig::default();
        let input = Path::new("/tmp/in.gif");
        let output = Path::new("/tmp/out.webm");
        for (plan, with_audio, video, audio) in [
            (TranscodePlan::ImageToWebp, false, "libwebp", ""),
            (TranscodePlan::GifToWebm, false, "libvpx-vp9", ""),
            (TranscodePlan::VideoToWebm, true, "libvpx-vp9", "libopus"),
            (TranscodePlan::VideoToWebm, false, "libvpx-vp9", ""),
        ] {
            let argv = build_argv("ffmpeg", input, output, plan, with_audio, &cfg);
            assert!(argv.contains(&"-map_metadata".to_string()), "{plan:?}");
            assert!(argv.contains(&"0".to_string()), "{plan:?}");
            assert!(
                !argv.iter().any(|arg| arg == "-map_metadata -1"),
                "{plan:?}"
            );
            assert!(argv.contains(&video.to_string()), "{plan:?}");
            if !audio.is_empty() {
                assert!(argv.contains(&audio.to_string()), "{plan:?}");
            }
            // Fake-ffmpeg harness contract: `-i` precedes the input, the
            // output is the final argument.
            let i = argv.iter().position(|a| a == "-i").expect("has -i");
            assert_eq!(argv[i + 1], "/tmp/in.gif");
            assert_eq!(argv.last().map(String::as_str), Some("/tmp/out.webm"));
        }
        // Quality settings are the recorded WP1 defaults.
        let argv = build_argv(
            "ffmpeg",
            input,
            output,
            TranscodePlan::ImageToWebp,
            false,
            &cfg,
        );
        assert!(argv.contains(&"80".to_string()));
        let argv = build_argv(
            "ffmpeg",
            input,
            output,
            TranscodePlan::VideoToWebm,
            true,
            &cfg,
        );
        assert!(argv.contains(&"30".to_string()));
        assert!(argv.contains(&"96k".to_string()));
    }

    #[test]
    fn missing_binary_is_a_startup_error() {
        let err = validate_toolchain("/nonexistent/pinetree-ffmpeg-xyz").expect_err("missing");
        assert!(err.contains("could not be executed"), "{err}");
        // Disabled transcoding validates without touching the binary.
        let disabled = TranscodeConfig {
            enabled: false,
            ffmpeg_path: "/nonexistent/pinetree-ffmpeg-xyz".to_string(),
            ..TranscodeConfig::default()
        };
        assert!(!disabled.validate().expect("disabled").enabled);
        // Nonsense knob values are startup errors too.
        let bad = TranscodeConfig {
            webp_quality: 101,
            ..TranscodeConfig::default()
        };
        assert!(bad.validate().is_err());
    }

    #[test]
    fn default_config_validates_ranges_without_a_binary() {
        // Range checks run before the toolchain probe.
        let bad = TranscodeConfig {
            wall_time_secs: 0,
            ..config_for("ffmpeg")
        };
        assert!(bad.validate().is_err());
    }

    #[test]
    fn transcode_bytes_reports_spawn_for_missing_binary() {
        let err = transcode_bytes(
            b"GIF89a",
            "gif",
            TranscodePlan::GifToWebm,
            false,
            &config_for("/nonexistent/pinetree-ffmpeg-xyz"),
        )
        .expect_err("missing binary must fail");
        assert!(matches!(err, TranscodeError::Spawn(_)), "{err}");
    }
}
