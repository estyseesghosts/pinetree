use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;

use crate::media::transcode::TranscodeConfig;
use crate::services::email::SmtpConfig;

/// Deployment mode (Phase 4 owns `Cluster`; selecting it is a startup
/// error until then).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeploymentMode {
    Single,
    Cluster,
}

impl Default for DeploymentMode {
    fn default() -> Self {
        DeploymentMode::Single
    }
}

/// Remote-media cache operating mode (`docs/remote-media-cache-plan.md`
/// §5). `Disabled` never fetches remote media, `OnDemand` fetches on first
/// media request, `Warm` also runs background warming jobs (opt-in,
/// relay-excluded).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteCacheMode {
    Disabled,
    #[default]
    OnDemand,
    Warm,
}
/// How the inbox authenticates HTTP signatures (`federation`
/// section, `rfc9421_mode` key; gaps WP5 tightener knob).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Rfc9421Mode {
    /// Log RFC 9421 (`Signature-Input`) fields and verify the legacy
    /// draft-cavanaugh `Signature` header (Mastodon 4.3+ dual-sign
    /// migration interop; see `federation::inbox::has_rfc9421_headers`).
    #[default]
    Permissive,
    /// Require a valid RFC 9421 signature (rejects requests that only
    /// carry the legacy header).
    Enforce,
}

/// Federation settings (nested `federation` section of the JSON config).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FederationSection {
    /// RFC 9421 HTTP signature handling (`"permissive"` default, `"enforce"`).
    pub rfc9421_mode: Rfc9421Mode,
}

/// Transcode settings (nested `transcode` section of the JSON config).
///
/// Mirrors [`crate::media::transcode::TranscodeConfig`] field for field;
/// [`TranscodeSection::to_runtime`] converts without loss, and
/// [`Config::validate`] runs the converted value through
/// [`TranscodeConfig::validate_ranges`] (static checks only; the
/// toolchain probe runs once at startup in the main gate).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TranscodeSection {
    /// Master switch. Validation probes the toolchain only when set;
    /// `--no-transcode` (or `false` here) routes the upload path around
    /// ffmpeg entirely.
    pub enabled: bool,
    /// ffmpeg binary (absolute path or `PATH` lookup, e.g. `"ffmpeg"`).
    pub ffmpeg_path: String,
    /// libwebp `-quality` (0-100).
    pub webp_quality: u8,
    /// VP9 `-crf` (0-63, constrained-quality mode with `-b:v 0`).
    pub vp9_crf: u8,
    /// VP9 `-cpu-used` (0-5; higher is faster and slightly worse).
    pub vp9_cpu_used: u8,
    /// Opus `-b:a` in kbit/s.
    pub opus_bitrate_kbps: u32,
    /// Per-job wall-time in seconds.
    pub wall_time_secs: u64,
    /// Per-job derived-output cap in bytes.
    pub max_output_bytes: u64,
    /// Per-job resident-memory (RSS) cap in bytes.
    pub max_memory_bytes: u64,
    /// Upload size cap in bytes (VP9/WebM passthrough bound).
    pub max_file_size: u64,
}

impl Default for TranscodeSection {
    fn default() -> Self {
        Self {
            enabled: true,
            ffmpeg_path: "ffmpeg".to_string(),
            webp_quality: 80,
            vp9_crf: 30,
            vp9_cpu_used: 2,
            opus_bitrate_kbps: 96,
            wall_time_secs: crate::media::transcode::DEFAULT_WALL_TIME_SECS,
            max_output_bytes: crate::media::transcode::DEFAULT_MAX_OUTPUT_BYTES,
            max_memory_bytes: crate::media::transcode::DEFAULT_MAX_MEMORY_BYTES,
            max_file_size: crate::media::transcode::DEFAULT_MAX_FILE_SIZE,
        }
    }
}

impl TranscodeSection {
    /// Convert to the runtime [`TranscodeConfig`] the startup gate and
    /// the job worker consume (field-for-field copy, no loss).
    pub fn to_runtime(&self) -> TranscodeConfig {
        TranscodeConfig {
            enabled: self.enabled,
            ffmpeg_path: self.ffmpeg_path.clone(),
            webp_quality: self.webp_quality,
            vp9_crf: self.vp9_crf,
            vp9_cpu_used: self.vp9_cpu_used,
            opus_bitrate_kbps: self.opus_bitrate_kbps,
            wall_time_secs: self.wall_time_secs,
            max_output_bytes: self.max_output_bytes,
            max_memory_bytes: self.max_memory_bytes,
            max_file_size: self.max_file_size,
        }
    }
}

fn default_smtp_port() -> u16 {
    25
}

/// Server configuration. Validated before any services are built.
///
/// JSON keys match the field names. Every key is optional in the file:
/// absent keys take the [`Default`] values below. Unknown keys are
/// rejected (typo safety). `--config=PATH` sets the base; CLI flags
/// override file values; `validate()` runs once over the merged result.
///
/// No `env:` indirection exists here (unlike birchtree): pinetree's
/// config carries no secrets, so there is nothing to keep out of the file.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Public base URL used for actor/object URIs (must be http/https).
    pub public_url: String,
    pub mode: DeploymentMode,
    pub data_dir: PathBuf,
    pub listen: SocketAddr,
    /// Maximum media upload size in bytes.
    pub max_file_size: u64,
    /// Local usernames that can never register (upstream `preservedUsernames`).
    pub preserved_usernames: Vec<String>,
    /// Word patterns rejected in display names / profile text.
    pub prohibited_words: Vec<String>,
    /// Words that force the sensitive flag on notes carrying them.
    pub sensitive_words: Vec<String>,
    /// Reverse proxies whose `X-Forwarded-For` is honored for client IPs
    /// (slice 9, H4). Empty by default: the TCP peer is always the client.
    pub trusted_proxies: Vec<IpAddr>,
    /// Maximum note drafts per account (Misskey `RoleService`
    /// `noteDraftLimit` default 10; §5).
    pub note_draft_limit: usize,
    /// Maximum actually-scheduled drafts per account (Misskey `RoleService`
    /// `scheduledNoteLimit` default 1; §5).
    pub scheduled_note_limit: usize,
    /// Maximum pinned notes per account (Misskey `RoleService` `pinLimit`
    /// default 5; §7).
    pub pin_limit: usize,
    /// Instance-wide default like emoji (Sharkey/Catodon `defaultLike`
    /// parity): the emoji `notes/like` and reaction-less
    /// `notes/reactions/create` resolve to when the caller passes no
    /// `override`/`reaction` and the reactor has no `defaultReaction`.
    /// `None` (default) means the bare heart ❤. Any unicode emoji or
    /// local custom `:name:` works; unknown/custom-gated names still fall
    /// back to ❤ through the reaction gate at use time.
    pub default_like: Option<String>,
    /// Durable (local-upload) bytes each user may hold (1 GiB default).
    /// Enforced in-txn at `upload_media`/`upload-from-url`; excess fails
    /// with `DRIVE_CAPACITY_EXCEEDED`. Over-quota rows predating the cap
    /// stay readable/deletable (grandfathered).
    pub drive_capacity_per_user_bytes: u64,
    /// Global remote-cache budget (512 MiB default) covering all cached
    /// remote bytes plus in-flight reservations.
    pub remote_cache_max_bytes: u64,
    /// Reserved class budgets inside the global cap (borrowing allowed):
    /// avatars/emoji 128 MiB, previews 256 MiB, full originals 128 MiB.
    pub remote_cache_avatar_bytes: u64,
    pub remote_cache_preview_bytes: u64,
    pub remote_cache_full_bytes: u64,
    /// Fetch policy: `on_demand` (default) fetches on first media request,
    /// `disabled` never fetches, `warm` also runs background warming jobs.
    pub remote_cache_mode: RemoteCacheMode,
    /// Per-class TTLs (days) driving LRU+TTL eviction preference.
    pub remote_cache_avatar_ttl_days: u64,
    pub remote_cache_preview_ttl_days: u64,
    pub remote_cache_full_ttl_days: u64,
    /// Opt-in background warming (relay-excluded). Off by default.
    pub remote_cache_warming_enabled: bool,
    /// Durable pool root override (`docs/database.md`): local uploads live
    /// here and are never evicted. `None` (default) resolves to
    /// `<data_dir>/blobs_durable`.
    pub durable_dir: Option<PathBuf>,
    /// Cache pool root override: remote fetches live here under the
    /// global + per-class caps. `None` (default) resolves to
    /// `<data_dir>/blobs_cache`. Must resolve distinct from `durable_dir`.
    pub cache_dir: Option<PathBuf>,
    /// SMTP relay host for outbound email (gaps WP3; shape in
    /// `services::email::SmtpConfig`). Empty (default) leaves the email
    /// transport dormant; set with `smtp_from` to enable it.
    pub smtp_host: String,
    /// SMTP relay port (default 25). Must be non-zero.
    #[serde(default = "default_smtp_port")]
    pub smtp_port: u16,
    /// SMTP username (optional; empty means no authentication).
    /// Must be set together with `smtp_password`.
    pub smtp_username: String,
    /// SMTP password (optional; empty means no authentication).
    /// Must be set together with `smtp_username`.
    pub smtp_password: String,
    /// Envelope + header `From` address for outbound email. Required
    /// together with `smtp_host` to enable the transport.
    pub smtp_from: String,
    /// When true, signup requires a well-shaped email address
    /// (`services::email::signup_email_gate` parity).
    pub email_required_for_signup: bool,
    /// Transcode settings (nested `transcode` section; gaps WP1).
    pub transcode: TranscodeSection,
    /// Federation settings (nested `federation` section; gaps WP5).
    pub federation: FederationSection,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            public_url: "https://example.local".to_string(),
            mode: DeploymentMode::Single,
            data_dir: PathBuf::from("./data"),
            listen: "127.0.0.1:3000".parse().expect("valid default socket addr"),
            max_file_size: 262_144_000, // 250 MiB
            preserved_usernames: vec![
                "admin".to_string(),
                "root".to_string(),
                "system".to_string(),
                "i".to_string(),
            ],
            prohibited_words: Vec::new(),
            sensitive_words: Vec::new(),
            trusted_proxies: Vec::new(),
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
            durable_dir: None,
            cache_dir: None,
            smtp_host: String::new(),
            smtp_port: default_smtp_port(),
            smtp_username: String::new(),
            smtp_password: String::new(),
            smtp_from: String::new(),
            email_required_for_signup: false,
            transcode: TranscodeSection::default(),
            federation: FederationSection::default(),
        }
    }
}

impl Config {
    /// Validate configuration. Returns a human-readable error string.
    pub fn validate(&self) -> Result<(), String> {
        if self.mode == DeploymentMode::Cluster {
            return Err("cluster mode is not implemented (Phase 4)".to_string());
        }
        let url = self.public_url.trim_end_matches('/');
        if !url.starts_with("http://") && !url.starts_with("https://") {
            return Err("public_url must start with http:// or https://".to_string());
        }
        let host = self.public_host();
        if host.is_empty() {
            return Err("public_url must include a host".to_string());
        }
        if self.data_dir.as_os_str().is_empty() {
            return Err("data_dir must not be empty".to_string());
        }
        if self.max_file_size == 0 {
            return Err("max_file_size must be > 0".to_string());
        }
        if self.note_draft_limit == 0 {
            return Err("note_draft_limit must be > 0".to_string());
        }
        if self.scheduled_note_limit == 0 {
            return Err("scheduled_note_limit must be > 0".to_string());
        }
        if self.pin_limit == 0 {
            return Err("pin_limit must be > 0".to_string());
        }
        if let Some(like) = self.default_like.as_deref() {
            let n = like.chars().count();
            if n == 0 || n > 260 {
                return Err("default_like must be 1..=260 chars".to_string());
            }
        }
        if self.drive_capacity_per_user_bytes == 0 {
            return Err("drive_capacity_per_user_bytes must be > 0".to_string());
        }
        if self.remote_cache_max_bytes == 0 {
            return Err("remote_cache_max_bytes must be > 0".to_string());
        }
        for (name, v) in [
            ("remote_cache_avatar_bytes", self.remote_cache_avatar_bytes),
            ("remote_cache_preview_bytes", self.remote_cache_preview_bytes),
            ("remote_cache_full_bytes", self.remote_cache_full_bytes),
        ] {
            if v == 0 {
                return Err(format!("{name} must be > 0"));
            }
            if v > self.remote_cache_max_bytes {
                return Err(format!("{name} must be <= remote_cache_max_bytes"));
            }
        }
        for (name, v) in [
            ("remote_cache_avatar_ttl_days", self.remote_cache_avatar_ttl_days),
            ("remote_cache_preview_ttl_days", self.remote_cache_preview_ttl_days),
            ("remote_cache_full_ttl_days", self.remote_cache_full_ttl_days),
        ] {
            if v == 0 {
                return Err(format!("{name} must be > 0"));
            }
        }
        // Reserved class budgets live inside the global cap — individually
        // (above) and as a sum, so the classes can never over-subscribe it.
        let reserved = self
            .remote_cache_avatar_bytes
            .saturating_add(self.remote_cache_preview_bytes)
            .saturating_add(self.remote_cache_full_bytes);
        if reserved > self.remote_cache_max_bytes {
            return Err(format!(
                "reserved class budgets ({reserved}) must fit in remote_cache_max_bytes ({})",
                self.remote_cache_max_bytes
            ));
        }
        // The two byte pools must be distinct directories: sharing one root
        // would let cache eviction eat durable uploads.
        if self.durable_pool_dir() == self.cache_pool_dir() {
            return Err("durable_dir and cache_dir must be distinct".to_string());
        }
        // Outbound email (gaps WP3; flattened `smtp_*` keys): the port is
        // always non-zero (a zero port means an explicit bad value — the
        // default is 25), and the all-or-nothing shape plus the `from`
        // address run through the sender's own validator so the file and
        // the transport can never disagree.
        if self.smtp_port == 0 {
            return Err("smtp_port must be non-zero".to_string());
        }
        self.smtp_config().validate()?;
        // Transcode knobs (gaps WP1): `Config::validate()` enforces the
        // static ranges only — the environment-dependent ffmpeg toolchain
        // probe runs once at startup in the main gate (a missing/broken
        // binary is a startup error, never a silent passthrough), so a
        // default config stays valid on machines without the toolchain.
        self.transcode
            .to_runtime()
            .validate_ranges()
            .map_err(|err| format!("transcode: {err}"))?;
        Ok(())
    }

    /// Load a JSON config file over the defaults. Unknown keys, unreadable
    /// files and malformed JSON are hard errors naming the path. CLI flags
    /// override afterwards; `validate()` runs once over the merged result.
    pub fn load_from_file(path: &std::path::Path) -> Result<Self, String> {
        let bytes = std::fs::read(path)
            .map_err(|err| format!("--config {}: {err}", path.display()))?;
        serde_json::from_slice(&bytes)
            .map_err(|err| format!("--config {}: {err}", path.display()))
    }

    pub fn normalized_public_url(&self) -> String {
        self.public_url.trim_end_matches('/').to_string()
    }

    /// Durable pool root: explicit `durable_dir` or `<data_dir>/blobs_durable`.
    pub fn durable_pool_dir(&self) -> PathBuf {
        self.durable_dir
            .clone()
            .unwrap_or_else(|| self.data_dir.join("blobs_durable"))
    }

    /// Cache pool root: explicit `cache_dir` or `<data_dir>/blobs_cache`.
    pub fn cache_pool_dir(&self) -> PathBuf {
        self.cache_dir
            .clone()
            .unwrap_or_else(|| self.data_dir.join("blobs_cache"))
    }

    /// Build the [`SmtpConfig`] mirror of the flattened `smtp_*` fields.
    /// The worker hands this to the email sender; [`Config::validate`]
    /// runs the same value through [`SmtpConfig::validate`] so the file
    /// and the transport can never disagree.
    pub fn smtp_config(&self) -> SmtpConfig {
        SmtpConfig {
            smtp_host: self.smtp_host.clone(),
            smtp_port: self.smtp_port,
            smtp_username: self.smtp_username.clone(),
            smtp_password: self.smtp_password.clone(),
            smtp_from: self.smtp_from.clone(),
            email_required_for_signup: self.email_required_for_signup,
        }
    }

    pub fn public_host(&self) -> String {
        let stripped = self
            .public_url
            .trim_end_matches('/')
            .strip_prefix("https://")
            .or_else(|| self.public_url.trim_end_matches('/').strip_prefix("http://"))
            .unwrap_or("");
        stripped.split('/').next().unwrap_or("").to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        assert!(Config::default().validate().is_ok());
    }

    #[test]
    fn rejects_unknown_keys() {
        // Typo safety: `deny_unknown_fields` fails the whole file.
        let raw = r#"{"public_url": "https://example.local", "pubic_url": "x"}"#;
        assert!(serde_json::from_str::<Config>(raw).is_err());
    }

    #[test]
    fn missing_keys_take_defaults() {
        let cfg: Config = serde_json::from_str("{}").expect("empty object parses");
        assert_eq!(cfg.public_url, Config::default().public_url);
        assert_eq!(cfg.listen, Config::default().listen);
        assert_eq!(cfg.note_draft_limit, crate::domain::draft::DEFAULT_NOTE_DRAFT_LIMIT);
        assert!(cfg.trusted_proxies.is_empty());
        // Email transport dormant by default (empty host/from, port 25,
        // signup gate off); transcode on with the runtime defaults;
        // federation signatures permissive.
        assert!(cfg.smtp_host.is_empty());
        assert_eq!(cfg.smtp_port, 25);
        assert!(cfg.smtp_username.is_empty());
        assert!(cfg.smtp_password.is_empty());
        assert!(cfg.smtp_from.is_empty());
        assert!(!cfg.email_required_for_signup);
        assert!(!cfg.smtp_config().is_configured());
        assert!(cfg.transcode.enabled);
        assert_eq!(cfg.transcode.ffmpeg_path, "ffmpeg");
        assert_eq!(cfg.transcode.webp_quality, 80);
        assert_eq!(cfg.transcode.vp9_crf, 30);
        assert_eq!(cfg.transcode.vp9_cpu_used, 2);
        assert_eq!(cfg.transcode.opus_bitrate_kbps, 96);
        assert_eq!(
            cfg.transcode.wall_time_secs,
            crate::media::transcode::DEFAULT_WALL_TIME_SECS
        );
        assert_eq!(
            cfg.transcode.max_output_bytes,
            crate::media::transcode::DEFAULT_MAX_OUTPUT_BYTES
        );
        assert_eq!(
            cfg.transcode.max_memory_bytes,
            crate::media::transcode::DEFAULT_MAX_MEMORY_BYTES
        );
        assert_eq!(
            cfg.transcode.max_file_size,
            crate::media::transcode::DEFAULT_MAX_FILE_SIZE
        );
        assert_eq!(cfg.federation.rfc9421_mode, Rfc9421Mode::Permissive);
        // Storage overhaul defaults: 1 GiB durable quota, 512 MiB global
        // cache (128/256/128 reserved), on-demand, 30/7/3-day TTLs.
        assert_eq!(cfg.drive_capacity_per_user_bytes, 1 << 30);
        assert_eq!(cfg.remote_cache_max_bytes, 512 << 20);
        assert_eq!(cfg.remote_cache_avatar_bytes, 128 << 20);
        assert_eq!(cfg.remote_cache_preview_bytes, 256 << 20);
        assert_eq!(cfg.remote_cache_full_bytes, 128 << 20);
        assert_eq!(cfg.remote_cache_mode, RemoteCacheMode::OnDemand);
        assert_eq!(cfg.remote_cache_avatar_ttl_days, 30);
        assert_eq!(cfg.remote_cache_preview_ttl_days, 7);
        assert_eq!(cfg.remote_cache_full_ttl_days, 3);
        assert!(!cfg.remote_cache_warming_enabled);
    }

    #[test]
    fn smtp_keys_round_trip() {
        let dir = std::env::temp_dir().join(format!(
            "pinetree-config-smtp-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("pinetree.json");
        let body = r#"{
            "smtp_host": "relay.local",
            "smtp_port": 2525,
            "smtp_username": "pinetree",
            "smtp_password": "secret",
            "smtp_from": "noreply@example.com",
            "email_required_for_signup": true,
            "transcode": {"enabled": false}
        }"#;
        std::fs::write(&path, body).unwrap();
        let cfg = Config::load_from_file(&path).expect("load");
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.smtp_host, "relay.local");
        assert_eq!(cfg.smtp_port, 2525);
        assert_eq!(cfg.smtp_config().smtp_from, "noreply@example.com");
        assert!(cfg.smtp_config().is_configured());
        assert!(cfg.email_required_for_signup);
        // Serde round-trip keeps the keys (typo safety works both ways).
        let json = serde_json::to_value(&cfg).expect("serialize");
        assert_eq!(json["smtp_host"], "relay.local");
        assert_eq!(json["smtp_port"], 2525);
        assert_eq!(json["email_required_for_signup"], true);
        let back: Config = serde_json::from_value(json).expect("deserialize");
        assert_eq!(back.smtp_config(), cfg.smtp_config());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn smtp_keys_reject_bad_values() {
        // Disabled transcode keeps these assertions hermetic (no ffmpeg
        // probe); the toolchain gate has its own test below.
        let dormant = || TranscodeSection { enabled: false, ..Default::default() };
        // Zero port is always rejected (the default is 25, so zero means
        // an explicit bad value).
        let zero_port = Config {
            smtp_port: 0,
            transcode: dormant(),
            ..Config::default()
        };
        assert!(zero_port.validate().is_err());
        // Half-filled transport (host without from) is rejected.
        let half = Config {
            smtp_host: "relay.local".to_string(),
            transcode: dormant(),
            ..Config::default()
        };
        assert!(half.validate().is_err());
        // Bad `from` shape is rejected via the sender's validator.
        let bad_from = Config {
            smtp_host: "relay.local".to_string(),
            smtp_from: "not-an-email".to_string(),
            transcode: dormant(),
            ..Config::default()
        };
        assert!(bad_from.validate().is_err());
        // Split auth (username without password) is rejected.
        let split_auth = Config {
            smtp_host: "relay.local".to_string(),
            smtp_from: "noreply@example.com".to_string(),
            smtp_username: "pinetree".to_string(),
            transcode: dormant(),
            ..Config::default()
        };
        assert!(split_auth.validate().is_err());
        // Unknown SMTP keys stay hard errors.
        assert!(serde_json::from_str::<Config>(r#"{"smtp_hosts": "x"}"#).is_err());
        // A fully-configured transport validates.
        let full = Config {
            smtp_host: "relay.local".to_string(),
            smtp_from: "noreply@example.com".to_string(),
            transcode: dormant(),
            ..Config::default()
        };
        assert!(full.validate().is_ok());
    }

    #[test]
    fn transcode_section_defaults_match_runtime() {
        // The section default converts field-for-field to the runtime
        // default (single source of truth for quality/cap values).
        let runtime = TranscodeSection::default().to_runtime();
        let expected = crate::media::transcode::TranscodeConfig::default();
        assert_eq!(runtime.enabled, expected.enabled);
        assert_eq!(runtime.ffmpeg_path, expected.ffmpeg_path);
        assert_eq!(runtime.webp_quality, expected.webp_quality);
        assert_eq!(runtime.vp9_crf, expected.vp9_crf);
        assert_eq!(runtime.vp9_cpu_used, expected.vp9_cpu_used);
        assert_eq!(runtime.opus_bitrate_kbps, expected.opus_bitrate_kbps);
        assert_eq!(runtime.wall_time_secs, expected.wall_time_secs);
        assert_eq!(runtime.max_output_bytes, expected.max_output_bytes);
        assert_eq!(runtime.max_memory_bytes, expected.max_memory_bytes);
        assert_eq!(runtime.max_file_size, expected.max_file_size);
    }

    #[test]
    fn transcode_keys_round_trip() {
        // Disabled so no test machine needs an ffmpeg binary for this.
        let raw = r#"{
            "transcode": {
                "enabled": false,
                "ffmpeg_path": "/opt/bin/ffmpeg",
                "webp_quality": 90,
                "vp9_crf": 32,
                "vp9_cpu_used": 3,
                "opus_bitrate_kbps": 128,
                "wall_time_secs": 60,
                "max_output_bytes": 33554432,
                "max_memory_bytes": 536870912,
                "max_file_size": 16777216
            }
        }"#;
        let cfg: Config = serde_json::from_str(raw).expect("parse");
        assert!(cfg.validate().is_ok());
        assert!(!cfg.transcode.enabled);
        assert_eq!(cfg.transcode.ffmpeg_path, "/opt/bin/ffmpeg");
        assert_eq!(cfg.transcode.webp_quality, 90);
        assert_eq!(cfg.transcode.opus_bitrate_kbps, 128);
        let runtime = cfg.transcode.to_runtime();
        assert!(!runtime.enabled);
        assert_eq!(runtime.vp9_crf, 32);
        // Serde round-trip keeps the nested keys.
        let json = serde_json::to_value(&cfg).expect("serialize");
        assert_eq!(json["transcode"]["webp_quality"], 90);
        assert_eq!(json["transcode"]["enabled"], false);
        let back: Config = serde_json::from_value(json).expect("deserialize");
        assert_eq!(back.transcode, cfg.transcode);
        // Unknown nested keys stay hard errors.
        assert!(serde_json::from_str::<Config>(
            r#"{"transcode": {"webp_qualit": 90}}"#
        )
        .is_err());
    }

    #[test]
    fn transcode_keys_reject_bad_values() {
        // Every range knob rejects zero/out-of-range without probing any
        // binary (disabled, so validation stays hermetic).
        for section in [
            TranscodeSection { webp_quality: 101, enabled: false, ..Default::default() },
            TranscodeSection { vp9_crf: 64, enabled: false, ..Default::default() },
            TranscodeSection { vp9_cpu_used: 6, enabled: false, ..Default::default() },
            TranscodeSection { opus_bitrate_kbps: 0, enabled: false, ..Default::default() },
            TranscodeSection { wall_time_secs: 0, enabled: false, ..Default::default() },
            TranscodeSection { max_output_bytes: 0, enabled: false, ..Default::default() },
            TranscodeSection { max_memory_bytes: 0, enabled: false, ..Default::default() },
            TranscodeSection { max_file_size: 0, enabled: false, ..Default::default() },
        ] {
            let cfg = Config { transcode: section, ..Config::default() };
            assert!(cfg.validate().is_err());
        }
        // Enabled with an unexecutable binary fails the STARTUP probe
        // (`TranscodeConfig::validate`), not `Config::validate()` (which
        // stays hermetic so defaults validate anywhere). No binary runs
        // for a missing path — the spawn error is the gate.
        let missing_toolchain = Config {
            transcode: TranscodeSection {
                ffmpeg_path: "/nonexistent/pinetree-ffmpeg-xyz".to_string(),
                ..Default::default()
            },
            ..Config::default()
        };
        assert!(missing_toolchain.validate().is_ok());
        assert!(missing_toolchain.transcode.to_runtime().validate().is_err());
        // Disabled with an unexecutable path validates (no probe).
        let disabled_toolchain = Config {
            transcode: TranscodeSection {
                enabled: false,
                ffmpeg_path: "/nonexistent/pinetree-ffmpeg-xyz".to_string(),
                ..Default::default()
            },
            ..Config::default()
        };
        assert!(disabled_toolchain.validate().is_ok());
    }

    #[test]
    fn federation_keys_round_trip() {
        let cfg: Config = serde_json::from_str("{}").expect("empty object parses");
        assert_eq!(cfg.federation.rfc9421_mode, Rfc9421Mode::Permissive);
        let enforced: Config = serde_json::from_str(
            r#"{"federation": {"rfc9421_mode": "enforce"}, "transcode": {"enabled": false}}"#,
        )
        .expect("parse");
        assert_eq!(enforced.federation.rfc9421_mode, Rfc9421Mode::Enforce);
        assert!(enforced.validate().is_ok());
        // Serde round-trip keeps the nested key.
        let json = serde_json::to_value(&enforced).expect("serialize");
        assert_eq!(json["federation"]["rfc9421_mode"], "enforce");
        let back: Config = serde_json::from_value(json).expect("deserialize");
        assert_eq!(back.federation, enforced.federation);
        // Unknown modes and unknown nested keys stay hard errors.
        assert!(serde_json::from_str::<Config>(
            r#"{"federation": {"rfc9421_mode": "strict"}}"#
        )
        .is_err());
        assert!(serde_json::from_str::<Config>(
            r#"{"federation": {"rfc9421_modes": "enforce"}}"#
        )
        .is_err());
    }

    #[test]
    fn file_round_trip() {
        let dir = std::env::temp_dir().join(format!(
            "pinetree-config-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("pinetree.json");
        let body = r#"{
            "public_url": "https://social.example.com/",
            "listen": "0.0.0.0:3000",
            "max_file_size": 1048576,
            "trusted_proxies": ["10.0.0.1"],
            "note_draft_limit": 20,
            "scheduled_note_limit": 2,
            "pin_limit": 8,
            "preserved_usernames": ["admin"],
            "prohibited_words": ["spam"],
            "sensitive_words": ["nsfw"],
            "default_like": "👍"
        }"#;
        std::fs::write(&path, body).unwrap();
        let cfg = Config::load_from_file(&path).expect("load");
        assert_eq!(cfg.public_url, "https://social.example.com/");
        assert_eq!(cfg.normalized_public_url(), "https://social.example.com");
        assert_eq!(cfg.listen.to_string(), "0.0.0.0:3000");
        assert_eq!(cfg.max_file_size, 1048576);
        assert_eq!(cfg.trusted_proxies, vec!["10.0.0.1".parse::<IpAddr>().unwrap()]);
        assert_eq!(cfg.note_draft_limit, 20);
        assert_eq!(cfg.scheduled_note_limit, 2);
        assert_eq!(cfg.pin_limit, 8);
        assert_eq!(cfg.preserved_usernames, vec!["admin".to_string()]);
        assert_eq!(cfg.default_like.as_deref(), Some("👍"));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn default_like_bounds() {
        assert!(Config { default_like: None, ..Config::default() }.validate().is_ok());
        assert!(Config { default_like: Some("👍".to_string()), ..Config::default() }.validate().is_ok());
        assert!(Config { default_like: Some(":blobcat:".to_string()), ..Config::default() }.validate().is_ok());
        assert!(Config { default_like: Some(String::new()), ..Config::default() }.validate().is_err());
        assert!(Config { default_like: Some("r".repeat(261)), ..Config::default() }.validate().is_err());
    }

    #[test]
    fn storage_keys_round_trip() {
        let dir = std::env::temp_dir().join(format!(
            "pinetree-config-storage-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("pinetree.json");
        let body = r#"{
            "drive_capacity_per_user_bytes": 1073741824,
            "remote_cache_max_bytes": 536870912,
            "remote_cache_avatar_bytes": 134217728,
            "remote_cache_preview_bytes": 268435456,
            "remote_cache_full_bytes": 134217728,
            "remote_cache_mode": "warm",
            "remote_cache_avatar_ttl_days": 30,
            "remote_cache_preview_ttl_days": 7,
            "remote_cache_full_ttl_days": 3,
            "remote_cache_warming_enabled": true
        }"#;
        std::fs::write(&path, body).unwrap();
        let cfg = Config::load_from_file(&path).expect("load");
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.drive_capacity_per_user_bytes, 1 << 30);
        assert_eq!(cfg.remote_cache_mode, RemoteCacheMode::Warm);
        assert!(cfg.remote_cache_warming_enabled);
        // Serde round-trip keeps the keys (typo safety works both ways).
        let json = serde_json::to_value(&cfg).expect("serialize");
        assert_eq!(json["remote_cache_mode"], "warm");
        assert_eq!(json["remote_cache_max_bytes"], 536870912);
        let back: Config = serde_json::from_value(json).expect("deserialize");
        assert_eq!(back.remote_cache_mode, RemoteCacheMode::Warm);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn storage_keys_reject_bad_values() {
        for cfg in [
            Config { drive_capacity_per_user_bytes: 0, ..Config::default() },
            Config { remote_cache_max_bytes: 0, ..Config::default() },
            Config { remote_cache_avatar_bytes: 0, ..Config::default() },
            Config { remote_cache_preview_bytes: 0, ..Config::default() },
            Config { remote_cache_full_bytes: 0, ..Config::default() },
            // Reserved class budgets live inside the global cap.
            Config { remote_cache_avatar_bytes: (512 << 20) + 1, ..Config::default() },
            Config { remote_cache_avatar_ttl_days: 0, ..Config::default() },
            Config { remote_cache_preview_ttl_days: 0, ..Config::default() },
            Config { remote_cache_full_ttl_days: 0, ..Config::default() },
        ] {
            assert!(cfg.validate().is_err(), "{cfg:?}");
        }
        // Unknown storage keys stay hard errors.
        assert!(serde_json::from_str::<Config>(
            r#"{"remote_cache_max_byte": 1}"#
        )
        .is_err());
        // Mode values are snake_case.
        let cfg: Config =
            serde_json::from_str(r#"{"remote_cache_mode": "on_demand"}"#).unwrap();
        assert_eq!(cfg.remote_cache_mode, RemoteCacheMode::OnDemand);
        assert!(serde_json::from_str::<Config>(r#"{"remote_cache_mode": "eager"}"#).is_err());
    }

    #[test]
    fn storage_pool_dirs_default_distinct_and_sum_guard() {
        let cfg = Config::default();
        assert_eq!(cfg.durable_pool_dir(), cfg.data_dir.join("blobs_durable"));
        assert_eq!(cfg.cache_pool_dir(), cfg.data_dir.join("blobs_cache"));
        assert!(cfg.validate().is_ok());
        // Sum of reserved class budgets over the global cap is rejected,
        // even when each class fits individually.
        let over = Config {
            remote_cache_max_bytes: 512 << 20,
            remote_cache_avatar_bytes: 256 << 20,
            remote_cache_preview_bytes: 256 << 20,
            remote_cache_full_bytes: 256 << 20,
            ..Config::default()
        };
        assert!(over.validate().is_err());
        // Explicit pool dirs must resolve distinct.
        let same = Config {
            durable_dir: Some(PathBuf::from("/tmp/pool")),
            cache_dir: Some(PathBuf::from("/tmp/pool")),
            ..Config::default()
        };
        assert!(same.validate().is_err());
        let split = Config {
            durable_dir: Some(PathBuf::from("/tmp/durable")),
            cache_dir: Some(PathBuf::from("/tmp/cache")),
            ..Config::default()
        };
        assert!(split.validate().is_ok());
        assert_eq!(split.durable_pool_dir(), PathBuf::from("/tmp/durable"));
        assert_eq!(split.cache_pool_dir(), PathBuf::from("/tmp/cache"));
    }

    #[test]
    fn load_missing_file_names_path() {
        let err = Config::load_from_file(std::path::Path::new("/nonexistent/pinetree.json"))
            .expect_err("missing file errors");
        assert!(err.contains("/nonexistent/pinetree.json"), "{err}");
    }

    #[test]
    fn rejects_bad_values() {
        for cfg in [
            Config { public_url: "ftp://example.local".into(), ..Config::default() },
            Config { public_url: "https://".into(), ..Config::default() },
            Config { max_file_size: 0, ..Config::default() },
            Config { note_draft_limit: 0, ..Config::default() },
            Config { scheduled_note_limit: 0, ..Config::default() },
            Config { pin_limit: 0, ..Config::default() },
            Config { mode: DeploymentMode::Cluster, ..Config::default() },
        ] {
            assert!(cfg.validate().is_err(), "{cfg:?}");
        }
        let empty_dir = Config { data_dir: PathBuf::from(""), ..Config::default() };
        assert!(empty_dir.validate().is_err());
    }
}
