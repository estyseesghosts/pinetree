use super::{
    TranscodeConfig, TranscodeLimits, TranscodePlan, build_argv, build_preview_argv,
    derived_extension,
};
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

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
                write!(
                    f,
                    "transcode output {bytes} bytes exceeded the {limit}-byte limit"
                )
            }
            TranscodeError::MemoryExceeded { bytes, limit } => {
                write!(
                    f,
                    "transcode RSS {bytes} bytes exceeded the {limit}-byte limit"
                )
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
    let _pid = child.id();
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
            code: status
                .code()
                .map(|c| c.to_string())
                .unwrap_or_else(|| "signal".to_string()),
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
pub(crate) struct TempGuard {
    paths: Vec<PathBuf>,
}

impl TempGuard {
    pub(crate) fn new(paths: Vec<PathBuf>) -> Self {
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
    let (derived, _) = transcode_bytes(
        input_bytes,
        input_ext,
        TranscodePlan::ImageToWebp,
        false,
        config,
    )?;
    let input_path = temp_path("prev-in", "webp");
    let output_path = temp_path("prev-out", "webp");
    let _guard = TempGuard::new(vec![input_path.clone(), output_path.clone()]);
    std::fs::write(&input_path, &derived)
        .map_err(|err| TranscodeError::Io(format!("write preview input: {err}")))?;
    let argv = build_preview_argv(
        &config.ffmpeg_path,
        &input_path,
        &output_path,
        bound,
        config,
    );
    run_bounded(&argv, Some(&output_path), &config.limits())?;
    let preview = std::fs::read(&output_path)
        .map_err(|err| TranscodeError::Io(format!("read preview output: {err}")))?;
    Ok((derived, preview))
}

#[cfg(test)]
mod tests {
    use super::super::config::config_for;
    use super::*;

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
