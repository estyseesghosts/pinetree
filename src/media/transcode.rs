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

mod argv;
mod config;
mod probe;
mod run;

pub use argv::{build_argv, build_preview_argv};
pub use config::{
    DEFAULT_MAX_FILE_SIZE, DEFAULT_MAX_MEMORY_BYTES, DEFAULT_MAX_OUTPUT_BYTES,
    DEFAULT_WALL_TIME_SECS, REQUIRED_DECODERS, REQUIRED_ENCODERS, ToolchainInfo, TranscodeConfig,
    TranscodeLimits, validate_toolchain,
};
pub use probe::{
    Probe, TranscodePlan, derived_extension, derived_mime, needs_transcode, parse_duration_ms,
    parse_probe_stderr, plan_for_mime, probe_derived, probe_file, probe_has_audio,
    probe_is_passthrough, probe_video,
};
pub(crate) use run::temp_path;
pub use run::{
    RunStats, TEMP_PREFIX, TMP_SWEEP_CAP, TMP_SWEEP_GRACE_MS, TranscodeError, sweep_tmp_files,
    transcode_bytes, transcode_still_with_preview,
};
