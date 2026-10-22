use std::process::Command;
use std::time::Duration;

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
    "png", "mjpeg", "gif", "bmp", "webp", "tiff", "h264", "hevc", "mpeg4", "vp8", "vp9", "aac",
    "opus", "vorbis", "mp3", "flac",
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
            // Batch B policy (matches `TranscodeSection::default`): a new
            // install must start without FFmpeg, so the runtime default is
            // also disabled. `{}`-JSON must not opt into transcoding.
            enabled: false,
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

    /// Validate the static knob ranges (no environment access: safe in
    /// `Config::validate()` and unit tests). The environment-dependent
    /// toolchain probe is [`TranscodeConfig::validate`], run once at
    /// startup by the main gate.
    pub fn validate_ranges(&self) -> std::result::Result<(), String> {
        if self.webp_quality > 100 {
            return Err(format!(
                "webp_quality {} out of range 0-100",
                self.webp_quality
            ));
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
        Ok(())
    }

    /// Validate the configuration. Runs [`TranscodeConfig::validate_ranges`]
    /// first, then — when transcoding is enabled — the toolchain check
    /// ([`validate_toolchain`]): a missing or non-working ffmpeg is a
    /// startup error, never a silent passthrough. Returns the probed
    /// toolchain on success. Run once at startup (the main gate), never
    /// from `Config::validate()`.
    pub fn validate(&self) -> std::result::Result<ToolchainInfo, String> {
        self.validate_ranges()?;
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

#[cfg(test)]
pub(crate) fn config_for(path: &str) -> TranscodeConfig {
    TranscodeConfig {
        ffmpeg_path: path.to_string(),
        ..TranscodeConfig::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
