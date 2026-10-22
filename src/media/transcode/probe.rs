use super::run::TempGuard;
use super::{TranscodeConfig, temp_path};
use std::path::Path;
use std::process::Command;

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
        "image/jpeg" | "image/png" | "image/bmp" | "image/heic" | "image/heif" | "image/tiff"
        | "image/x-tiff" | "image/avif" => TranscodePlan::ImageToWebp,
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
    let (head, _) = tail
        .rsplit_once(", from")
        .or_else(|| tail.rsplit_once(" from"))?;
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
    use super::super::DEFAULT_MAX_FILE_SIZE;
    use super::*;

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
        assert!(!probe_is_passthrough(
            &base,
            DEFAULT_MAX_FILE_SIZE + 1,
            DEFAULT_MAX_FILE_SIZE
        ));
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
    fn duration_parses_hh_mm_ss_ms() {
        assert_eq!(parse_duration_ms("00:00:02.50, start: 0"), Some(2_500));
        assert_eq!(
            parse_duration_ms("01:02:03.456, bitrate: 1"),
            Some(3_723_456)
        );
        assert_eq!(parse_duration_ms("00:00:00.00,"), Some(0));
        assert_eq!(parse_duration_ms("N/A,"), None);
        assert_eq!(parse_duration_ms("garbage"), None);
    }
}
