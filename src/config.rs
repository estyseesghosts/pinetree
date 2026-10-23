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
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FederationSection {
    /// RFC 9421 HTTP signature handling (`"permissive"` default, `"enforce"`).
    pub rfc9421_mode: Rfc9421Mode,
    /// Hard maximum of concurrent in-flight DNS resolver calls/threads
    /// (default 32). Saturation fails fast with a transient resource-busy
    /// error instead of spawning another thread.
    #[serde(default = "default_dns_max_in_flight")]
    pub dns_max_in_flight: usize,
    /// SSRF exemption for outbound federation HTTP (default `false`).
    /// `true` skips private/reserved-IP rejection for federation delivery
    /// ONLY (local federation testing against loopback/LAN peers); TLS
    /// verification stays enforced and the webhook delivery path ignores
    /// this flag (always `false` there). Prefer the shipped default; the
    /// deprecated `PINETREE_ALLOW_PRIVATE_IPS=1` env override maps here.
    #[serde(default)]
    pub allow_private_ips: bool,
    /// Extra CA bundle for outbound federation HTTPS (default `None`).
    /// Appended as an additional trust anchor — the platform default store
    /// is never replaced. The startup gate logs the file's SHA-256
    /// fingerprint and refuses to boot when the file is missing or
    /// unreadable (permanent, not a transient retry). Prefer the CLI
    /// `--federation-extra-ca=PATH`, which writes this same field.
    #[serde(default)]
    pub federation_extra_ca: Option<PathBuf>,
    /// Federation debug lines (accept/reject, delivery). Default `false`.
    /// Non-production toggle: honored only in debug builds
    /// (`#[cfg(debug_assertions)]`); `validate()` rejects `true` in release
    /// builds. Prefer the CLI `--debug-federation`, which writes this same
    /// field. The deprecated `PINETREE_DEBUG_FEDERATION=1` env override
    /// maps here.
    #[serde(default)]
    pub debug_federation: bool,
    /// Opt-in bounded repair for pre-Slice-B historical remote notes whose
    /// original `inReplyTo` was discarded (`docs/replies.md` §6.1).
    /// `None` (default) disables the repair (no network I/O at startup);
    /// `Some(n)` re-fetches at most `n` of the newest stored notes' origin
    /// documents and marks the ones that still declare a parent.
    /// Sequential, restartable, safe to re-run. The deprecated
    /// `PINETREE_REPLY_REPAIR_LIMIT=<1..=5000>` env override maps here.
    #[serde(default)]
    pub reply_repair_limit: Option<usize>,
}

/// Default DNS resolver pool size (mirrors
/// `federation::delivery::DEFAULT_MAX_DNS_IN_FLIGHT` without a dependency
/// from the config crate surface).
fn default_dns_max_in_flight() -> usize {
    32
}

/// Maximum accepted DNS pool size (operator sanity bound; the resolver
/// itself enforces the hard thread bound at whatever value is set).
const MAX_DNS_MAX_IN_FLIGHT: usize = 256;

impl Default for FederationSection {
    fn default() -> Self {
        Self {
            rfc9421_mode: Rfc9421Mode::default(),
            dns_max_in_flight: default_dns_max_in_flight(),
            allow_private_ips: false,
            federation_extra_ca: None,
            debug_federation: false,
            reply_repair_limit: None,
        }
    }
}

/// Diagnostic detail + log shape (nested `diagnostics` section of the JSON
/// config; Batch H). `detailed_admin_status: false` (default) means the
/// expensive/private status fields are unavailable; enabling it does NOT
/// remove the admin-authentication requirement. `structured_logging: true`
/// (default) emits stable `key=value` event lines; `false` emits only
/// `level=` + `event=` (see `crate::observability`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiagnosticsSection {
    /// Gate the expensive/detailed admin status fields. Default `false`.
    pub detailed_admin_status: bool,
    /// Emit structured `key=value` event lines. Default `true`.
    pub structured_logging: bool,
}

impl Default for DiagnosticsSection {
    /// Batch H default: detail off, structured lines on.
    fn default() -> Self {
        Self {
            detailed_admin_status: false,
            structured_logging: true,
        }
    }
}

/// Backup output policy (nested `backup` section of the JSON config;
/// Batch H wires the Batch F engine). `dir: null` (default) resolves to
/// `<data_dir>/backups`; `max_bundle_bytes` (default 64 GiB, mirroring
/// `backup::bundle::MAX_BUNDLE_BYTES`) bounds one staging bundle.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BackupSection {
    /// Backup bundle + status directory override. `None` (default)
    /// resolves to `<data_dir>/backups`.
    pub dir: Option<PathBuf>,
    /// Per-bundle staging cap in bytes (default 64 GiB). Must be `> 0` and
    /// within the immutable hard ceiling
    /// (`backup::HARD_BUNDLE_CEILING_BYTES`); the enforced value is the
    /// configured cap, never silently the ceiling.
    pub max_bundle_bytes: u64,
    /// Backup rotation: keep at most this many completed bundles (`None`
    /// default = keep all). When set, must be `>= 1`; pruning deletes only
    /// older completed bundles and never the newest completed one, and a
    /// failed backup never deletes anything. No age-based deletion is
    /// implemented (retention is count/bytes only).
    pub retention_max_count: Option<usize>,
    /// Backup rotation: keep completed bundles within this total byte
    /// budget (`None` default = unbounded). When set, must be `>= 1`;
    /// pruning deletes the oldest completed bundles first and never the
    /// newest completed one.
    pub retention_max_bytes: Option<u64>,
}

fn default_backup_max_bundle_bytes() -> u64 {
    64 << 30
}

impl Default for BackupSection {
    fn default() -> Self {
        Self {
            dir: None,
            max_bundle_bytes: default_backup_max_bundle_bytes(),
            retention_max_count: None,
            retention_max_bytes: None,
        }
    }
}

/// Fixed-window rate-limiter memory bounds (nested `rate_limit` section
/// of the JSON config; Batch D). All three mirror the hardcoded
/// documented constants in
/// `crate::api::misskey::response` (`MAX_RATE_LIMIT_BUCKETS`,
/// `MAX_AUTH_FAILURE_BUCKETS`, `PRUNE_INTERVAL_MS`): 32 768 ordinary
/// buckets, 32 768 auth-failure buckets, one expiry scan per 60 000 ms
/// of clock time. Every entry is a few short strings plus integers, so
/// a full map stays on the order of megabytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RateLimitSection {
    /// Cap on ordinary `(key, bucket)` window entries. Must be `> 0`.
    pub max_buckets: usize,
    /// Cap on `(key, bucket)` auth-failure entries. Must be `> 0`.
    pub max_auth_failures: usize,
    /// At most one full expiry scan per map per this interval of clock
    /// time (ms). Must be `> 0`.
    pub prune_interval_ms: i64,
}

impl Default for RateLimitSection {
    fn default() -> Self {
        Self {
            max_buckets: 32_768,
            max_auth_failures: 32_768,
            prune_interval_ms: 60_000,
        }
    }
}

/// Concurrent-upload slot bounds (nested `uploads` section of the JSON
/// config; Batch D + Slice 6 H1). Defaults mirror the hardcoded
/// documented constants in `crate::api::misskey::response`
/// (`MAX_CONCURRENT_UPLOADS_PER_IP`, `MAX_CONCURRENT_UPLOADS_GLOBAL`,
/// `MAX_UPLOAD_SLOT_IPS`): 2 concurrent uploads per IP, 16 globally,
/// at most 4 096 distinct per-IP slot entries retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct UploadsSection {
    /// Concurrent uploads per client IP. Must be `> 0`.
    pub per_ip: usize,
    /// Global concurrent-upload cap. Must be `> 0`.
    pub global: usize,
    /// Distinct per-IP slot entries retained. Must be `> 0`.
    pub max_ips: usize,
}

impl Default for UploadsSection {
    fn default() -> Self {
        Self {
            per_ip: 2,
            global: 16,
            max_ips: 4_096,
        }
    }
}

/// Unsafe test-only HTTP opt-in (nested `unsafe` section of the JSON
/// config; Batch B). The Rust field uses a raw identifier (`r#unsafe`,
/// since `unsafe` is a keyword); the JSON key is exactly `"unsafe"`,
/// matching the field name per the Configuration rule in `AGENTS.md`.
///
/// Default is `false`: production `public_url` values must be `https://`.
/// When `true`, `http://` `public_url` values are accepted **only** for
/// local/test hosts ([`PublicHostClass`]): loopback IP literals, `localhost`
/// / `*.localhost`, RFC 1918 private-IP literals, and the RFC 2606 reserved
/// `.test` suffix used by the local bench rigs. Arbitrary public HTTP origins
/// stay rejected. This is JSON-controlled only (plus the per-invocation CLI
/// override, like every other setting); there is no environment-only toggle.
///
/// This is deliberately SEPARATE from the outbound federation SSRF control
/// (`SafeHttpConfig::allow_private_ips`, fed via `PINETREE_ALLOW_PRIVATE_IPS`
/// in test rigs): that flag exempts outbound fetch address checks, while this
/// flag exempts the inbound canonical-origin scheme check. Do not conflate
/// them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct UnsafeSection {
    /// Allow `http://` `public_url` for local/test hosts only. `false`
    /// (default) rejects every `http://` `public_url` at validation.
    pub allow_local_http_for_testing: bool,
}

/// Host class of the configured `public_url` (Batch B). Used by
/// [`Config::validate`] to gate `http://` origins and by the startup warning
/// in `main` (class only, never secrets).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicHostClass {
    /// `127.0.0.0/8` or `::1` literal.
    Loopback,
    /// `localhost` or `*.localhost`.
    LocalhostName,
    /// RFC 1918 private IPv4 literal (`10/8`, `172.16/12`, `192.168/16`).
    PrivateLan,
    /// RFC 2606 reserved `.test` suffix (local bench/interop rigs; can never
    /// collide with a public origin).
    TestDomain,
    /// Anything else (public IPs, public DNS, unparseable literals).
    Public,
}

impl PublicHostClass {
    /// Short stable label for logs (no hostnames, no secrets).
    pub fn as_str(&self) -> &'static str {
        match self {
            PublicHostClass::Loopback => "loopback",
            PublicHostClass::LocalhostName => "localhost",
            PublicHostClass::PrivateLan => "private-lan",
            PublicHostClass::TestDomain => "test-domain",
            PublicHostClass::Public => "public",
        }
    }

    /// True for the host classes the unsafe local-HTTP flag covers.
    pub fn is_local_test(&self) -> bool {
        !matches!(self, PublicHostClass::Public)
    }
}

/// Strip a `:port` suffix from a `public_host()` value for classification.
/// Bracketed IPv6 (`[::1]:3000`) unwraps to `::1`; a bare IPv6 literal
/// (several colons, no brackets) is returned whole; other single-colon
/// values split at the last colon.
fn host_without_port(hostport: &str) -> &str {
    let hostport = hostport.trim();
    if let Some(rest) = hostport.strip_prefix('[') {
        if let Some(end) = rest.find(']') {
            return rest[..end].trim();
        }
        return hostport;
    }
    if hostport.chars().filter(|c| *c == ':').count() == 1
        && let Some((host, _)) = hostport.rsplit_once(':')
    {
        return host.trim();
    }
    hostport
}

/// Classify a bare (port-stripped) `public_url` host. DNS names are never
/// resolved here: only IP literals, `localhost` names and the reserved
/// `.test` suffix count as local/test; everything else is `Public`
/// (conservative — e.g. IPv6 ULAs and link-local literals are `Public`).
fn classify_host(bare: &str) -> PublicHostClass {
    let lower = bare.trim().trim_end_matches('.').to_lowercase();
    if lower == "localhost" || lower.ends_with(".localhost") {
        return PublicHostClass::LocalhostName;
    }
    if lower.len() > ".test".len() && lower.ends_with(".test") {
        return PublicHostClass::TestDomain;
    }
    if let Ok(addr) = lower.parse::<IpAddr>() {
        if addr.is_loopback() {
            return PublicHostClass::Loopback;
        }
        if let IpAddr::V4(v4) = addr
            && v4.is_private()
        {
            return PublicHostClass::PrivateLan;
        }
        return PublicHostClass::Public;
    }
    PublicHostClass::Public
}
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
            // Batch B policy (flipped Batch H): a new install must start
            // without FFmpeg, so the section default is disabled. `{}`-JSON
            // must not opt into transcoding; enabling it is explicit
            // (`transcode.enabled: true` plus a working toolchain, probed
            // once at startup). `Config::default()` pins the same value
            // below so both defaults agree.
            enabled: false,
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
    /// Unsafe test-only HTTP opt-in (nested `unsafe` section; Batch B).
    /// Rust field name uses a raw identifier so the JSON key is exactly
    /// `"unsafe"`. Default disables every `http://` `public_url`.
    pub r#unsafe: UnsafeSection,
    /// Diagnostics detail + log shape (nested `diagnostics` section;
    /// Batch H). Unknown keys rejected; see [`DiagnosticsSection`].
    pub diagnostics: DiagnosticsSection,
    /// Backup output policy (nested `backup` section; Batch H). Unknown
    /// keys rejected; see [`BackupSection`].
    pub backup: BackupSection,
    /// Rate-limiter memory bounds (nested `rate_limit` section; Batch D).
    /// Unknown keys rejected; see [`RateLimitSection`].
    pub rate_limit: RateLimitSection,
    /// Concurrent-upload slot bounds (nested `uploads` section; Batch D).
    /// Unknown keys rejected; see [`UploadsSection`].
    pub uploads: UploadsSection,
    /// Opt-in live-redb integrity check at open (LOW-6). Default `false`
    /// (boot stays fast): when `true`, the open path runs the same
    /// `check_integrity` the restore path uses and refuses to boot on
    /// failure. Prefer the CLI `--storage-integrity-check-at-open`, which
    /// writes this same field.
    #[serde(default)]
    pub storage_integrity_check_at_open: bool,
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
            // Batch B policy (section default flipped Batch H):
            // `TranscodeSection::default()` carries `enabled: false`, so
            // `{}`-JSON never opts into transcoding. The explicit field
            // here keeps the Config-level default obvious; the shipped
            // Docker default pins this explicitly too.
            transcode: TranscodeSection {
                enabled: false,
                ..TranscodeSection::default()
            },
            federation: FederationSection::default(),
            r#unsafe: UnsafeSection::default(),
            diagnostics: DiagnosticsSection::default(),
            backup: BackupSection::default(),
            rate_limit: RateLimitSection::default(),
            uploads: UploadsSection::default(),
            storage_integrity_check_at_open: false,
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
        let is_http = url.starts_with("http://");
        if !is_http && !url.starts_with("https://") {
            return Err("public_url must start with http:// or https://".to_string());
        }
        let host = self.public_host();
        if host.is_empty() {
            return Err("public_url must include a host".to_string());
        }
        // Batch B forcing function: the shipped Docker default carries an
        // RFC 2606 `.invalid` host so a fresh checkout refuses to start until
        // the operator sets a canonical URL. `.invalid` can never resolve
        // publicly, so this rejects no real deployment.
        let bare = host_without_port(&host).to_lowercase();
        if bare == "invalid" || bare.ends_with(".invalid") {
            return Err(format!(
                "public_url host {host:?} is a placeholder: set public_url to your canonical https:// URL"
            ));
        }
        // Batch B unsafe-HTTP gate: `http://` origins need the explicit JSON
        // opt-in, and even then only for local/test host classes. The
        // outbound federation SSRF control (`allow_private_ips`) is separate
        // and does not affect this check.
        if is_http && !self.r#unsafe.allow_local_http_for_testing {
            return Err(
                "public_url uses http:// but unsafe.allow_local_http_for_testing is false: \
                 use an https:// public_url, or set {\"unsafe\": \
                 {\"allow_local_http_for_testing\": true}} for a local/test host only"
                    .to_string(),
            );
        }
        if is_http {
            let class = self.public_host_class();
            if !class.is_local_test() {
                return Err(format!(
                    "public_url uses http:// for a non-local host (class: {}): \
                     unsafe.allow_local_http_for_testing covers loopback, localhost, \
                     private-LAN literals and .test names only — serve public origins over https://",
                    class.as_str()
                ));
            }
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
            (
                "remote_cache_preview_bytes",
                self.remote_cache_preview_bytes,
            ),
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
            (
                "remote_cache_avatar_ttl_days",
                self.remote_cache_avatar_ttl_days,
            ),
            (
                "remote_cache_preview_ttl_days",
                self.remote_cache_preview_ttl_days,
            ),
            (
                "remote_cache_full_ttl_days",
                self.remote_cache_full_ttl_days,
            ),
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
        // Backup output policy (Batch H): the staging cap must be
        // positive and within the immutable hard ceiling, and an explicit
        // dir must be a non-empty path. The cap is enforced at
        // bundle-creation and restore time against the same policy object;
        // the config value lets operators tighten it per install. Retention
        // bounds only shrink storage when explicitly set (`None` = keep).
        if self.backup.max_bundle_bytes == 0
            || self.backup.max_bundle_bytes > crate::backup::HARD_BUNDLE_CEILING_BYTES
        {
            return Err(
                "backup.max_bundle_bytes must be within the hard bundle ceiling".to_string(),
            );
        }
        if self.backup.retention_max_count == Some(0) {
            return Err("backup.retention_max_count must be >= 1".to_string());
        }
        if self.backup.retention_max_bytes == Some(0) {
            return Err("backup.retention_max_bytes must be >= 1".to_string());
        }
        // Federation DNS pool (Phase 4): a hard thread bound, always >= 1.
        if self.federation.dns_max_in_flight == 0
            || self.federation.dns_max_in_flight > MAX_DNS_MAX_IN_FLIGHT
        {
            return Err("federation.dns_max_in_flight must be within 1..=256".to_string());
        }
        // Federation SSRF posture (Wave 5): the exemption is explicit and
        // off by default; the webhook path ignores it (always strict).
        // An empty extra-CA path is a config error, not a silent skip.
        if self
            .federation
            .federation_extra_ca
            .as_deref()
            .is_some_and(|p| p.as_os_str().is_empty())
        {
            return Err("federation.federation_extra_ca must not be empty".to_string());
        }
        // Non-production toggle: only debug builds may enable it, so a
        // release binary can never ship with federation debug logging on
        // by configuration.
        if self.federation.debug_federation && !cfg!(debug_assertions) {
            return Err("federation.debug_federation is a debug-build toggle only".to_string());
        }
        // Opt-in reply repair bound (Wave 5): 1..=5000 when set.
        if self
            .federation
            .reply_repair_limit
            .is_some_and(|n| n == 0 || n > 5000)
        {
            return Err("federation.reply_repair_limit must be within 1..=5000".to_string());
        }
        if let Some(dir) = self.backup.dir.as_deref()
            && dir.as_os_str().is_empty()
        {
            return Err("backup.dir must not be empty".to_string());
        }
        // Batch D bounds (JSON-configurable; defaults mirror the
        // documented constants in `api::misskey::response`). Every bound
        // must be positive so a zero silently disabling throttling or
        // pruning fails closed at startup.
        if self.rate_limit.max_buckets == 0 {
            return Err("rate_limit.max_buckets must be > 0".to_string());
        }
        if self.rate_limit.max_auth_failures == 0 {
            return Err("rate_limit.max_auth_failures must be > 0".to_string());
        }
        if self.rate_limit.prune_interval_ms <= 0 {
            return Err("rate_limit.prune_interval_ms must be > 0".to_string());
        }
        if self.uploads.per_ip == 0 {
            return Err("uploads.per_ip must be > 0".to_string());
        }
        if self.uploads.global == 0 {
            return Err("uploads.global must be > 0".to_string());
        }
        if self.uploads.max_ips == 0 {
            return Err("uploads.max_ips must be > 0".to_string());
        }
        Ok(())
    }

    /// Load a JSON config file over the defaults. Unknown keys, unreadable
    /// files and malformed JSON are hard errors naming the path. CLI flags
    /// override afterwards; `validate()` runs once over the merged result.
    pub fn load_from_file(path: &std::path::Path) -> Result<Self, String> {
        let bytes =
            std::fs::read(path).map_err(|err| format!("--config {}: {err}", path.display()))?;
        serde_json::from_slice(&bytes).map_err(|err| format!("--config {}: {err}", path.display()))
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

    /// Backup bundle + status directory: explicit `backup.dir` or
    /// `<data_dir>/backups` (Batch H; mirrors
    /// `backup::DEFAULT_BACKUP_DIR_NAME`).
    pub fn backup_dir(&self) -> PathBuf {
        self.backup
            .dir
            .clone()
            .unwrap_or_else(|| self.data_dir.join(crate::backup::DEFAULT_BACKUP_DIR_NAME))
    }

    /// Safe config fingerprint for the detailed admin surface (Batch H):
    /// mode/feature flags, pool caps, and host *class* — never secrets
    /// (`smtp_password`, tokens, keys), never addresses, never whole
    /// secrets-bearing sections. Counts, not contents, for name lists.
    pub fn config_summary(&self) -> serde_json::Value {
        // Mode enums serialize through their serde impls so the fingerprint
        // uses the same wire names as the config file (`on_demand`, ...).
        let mode = serde_json::to_value(&self.mode).unwrap_or(serde_json::Value::Null);
        let cache_mode =
            serde_json::to_value(&self.remote_cache_mode).unwrap_or(serde_json::Value::Null);
        let sig_mode =
            serde_json::to_value(&self.federation.rfc9421_mode).unwrap_or(serde_json::Value::Null);
        serde_json::json!({
            "mode": mode,
            "max_file_size": self.max_file_size,
            "preserved_usernames": self.preserved_usernames.len(),
            "prohibited_words": self.prohibited_words.len(),
            "sensitive_words": self.sensitive_words.len(),
            "trusted_proxies": self.trusted_proxies.len(),
            "note_draft_limit": self.note_draft_limit,
            "scheduled_note_limit": self.scheduled_note_limit,
            "pin_limit": self.pin_limit,
            "default_like_set": self.default_like.is_some(),
            "drive_capacity_per_user_bytes": self.drive_capacity_per_user_bytes,
            "remote_cache_max_bytes": self.remote_cache_max_bytes,
            "remote_cache_mode": cache_mode,
            "remote_cache_warming_enabled": self.remote_cache_warming_enabled,
            "transcode_enabled": self.transcode.enabled,
            "transcode_ffmpeg_path_set": !self.transcode.ffmpeg_path.is_empty(),
            "rfc9421_mode": sig_mode,
            "dns_max_in_flight": self.federation.dns_max_in_flight,
            "allow_local_http_for_testing": self.r#unsafe.allow_local_http_for_testing,
            "public_host_class": self.public_host_class().as_str(),
            "detailed_admin_status": self.diagnostics.detailed_admin_status,
            "structured_logging": self.diagnostics.structured_logging,
            "backup_max_bundle_bytes": self.backup.max_bundle_bytes,
            "backup_retention_max_count": self.backup.retention_max_count,
            "backup_retention_max_bytes": self.backup.retention_max_bytes,
            "backup_dir_configured": self.backup.dir.is_some(),
            "rate_limit_max_buckets": self.rate_limit.max_buckets,
            "rate_limit_max_auth_failures": self.rate_limit.max_auth_failures,
            "rate_limit_prune_interval_ms": self.rate_limit.prune_interval_ms,
            "uploads_per_ip": self.uploads.per_ip,
            "uploads_global": self.uploads.global,
            "uploads_max_ips": self.uploads.max_ips,
            "email_configured": self.smtp_config().is_configured(),
            "email_required_for_signup": self.email_required_for_signup,
        })
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
            .or_else(|| {
                self.public_url
                    .trim_end_matches('/')
                    .strip_prefix("http://")
            })
            .unwrap_or("");
        stripped.split('/').next().unwrap_or("").to_string()
    }

    /// Host class of the configured `public_url` (port-stripped; see
    /// [`PublicHostClass`]). Used by [`Config::validate`] and the unsafe-HTTP
    /// startup warning. Reports the class only, never secrets.
    pub fn public_host_class(&self) -> PublicHostClass {
        classify_host(host_without_port(&self.public_host()))
    }
}

#[cfg(test)]
mod tests;
