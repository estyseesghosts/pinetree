//! Structured operational logging with redaction (`prod-ready.md` §9 H3).
//!
//! The server logs with plain `eprintln!` (there is no tracing
//! dependency), so every critical-path line follows one stable shape:
//!
//! ```text
//! [pinetree] level=<error|warn|info|debug> event=<dotted.name> key=value...
//! ```
//!
//! Event names are stable (`http.request.done`, `backup.requested`, ...)
//! so operators can grep one release to the next. Request correlation,
//! job, delivery, media, and backup ids travel as ordinary `key=value`
//! fields; [`log_event`] redacts every sensitive value by key before it
//! can reach stderr (see [`is_sensitive_key`]).
//!
//! Redaction rules (fail closed):
//!
//! * `Authorization`, cookies, access tokens, passwords, SMTP credentials,
//!   private key material, request bodies, and personal data (`email`,
//!   `ip`, ...) are replaced with `[redacted]`.
//! * Free-form detail strings are truncated ([`MAX_DETAIL_CHARS]) and never
//!   carry remote response bodies — callers log [`error_class`] + a short
//!   cause instead.
//! * There is intentionally no helper that formats a whole `Config`: use
//!   [`crate::config::Config::config_summary`] (safe subset) for
//!   fingerprints.
//!
//! The opt-in federation debug workflow (`--debug-federation` /
//! `PINETREE_DEBUG_FEDERATION=1`, `[fed-debug]` lines) is preserved during
//! migration and stays completely separate from this surface.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Environment variable that enables federation debug logging (any non-empty
/// affirmative value; see [`env_enables_federation_debug`]).
pub const DEBUG_FEDERATION_ENV: &str = "PINETREE_DEBUG_FEDERATION";

/// Upper bound for one free-form detail value in a structured line.
/// Longer values are truncated; remote bodies must never be logged whole.
pub const MAX_DETAIL_CHARS: usize = 500;

/// Cap on the bounded recent-error ring ([`recent_errors`]).
pub const MAX_RECENT_ERRORS: usize = 64;

/// Cap on distinct error-category counters (churn safety).
pub const MAX_ERROR_CATEGORIES: usize = 128;

static FEDERATION_DEBUG: AtomicBool = AtomicBool::new(false);
static STRUCTURED_LOGGING: AtomicBool = AtomicBool::new(true);

/// Log severity for [`log_event`]. Normal operation logs at `info`;
/// `debug` lines require explicit opt-in at the call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
}

impl LogLevel {
    /// Stable lowercase label used in the `level=` field.
    pub fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Error => "error",
            LogLevel::Warn => "warn",
            LogLevel::Info => "info",
            LogLevel::Debug => "debug",
        }
    }
}

pub fn init() {}

/// Turn federation debug lines on or off for the whole process. Called once from
/// `main` after reading `--debug-federation` and the environment.
pub fn set_federation_debug(on: bool) {
    FEDERATION_DEBUG.store(on, Ordering::Relaxed);
}

pub fn federation_debug() -> bool {
    FEDERATION_DEBUG.load(Ordering::Relaxed)
}

/// Whether an environment value enables federation debug. Unset, empty, and any
/// value other than `1`/`true`/`yes`/`on` (case-insensitive) mean off.
pub fn env_enables_federation_debug(value: Option<&str>) -> bool {
    matches!(
        value.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

/// Publish the configured `diagnostics.structured_logging` value
/// process-wide (called once from `main`; tests default to `true`).
pub fn set_structured_logging(on: bool) {
    STRUCTURED_LOGGING.store(on, Ordering::Relaxed);
}

/// Whether structured `key=value` lines are enabled. When off,
/// [`log_event`] emits only `level=` + `event=` (no fields).
pub fn structured_logging() -> bool {
    STRUCTURED_LOGGING.load(Ordering::Relaxed)
}

/// True when `key` names a secret or personal value that must never reach
/// logs: `Authorization`/cookies/tokens, passwords, SMTP credentials,
/// private key material, request bodies, and personal data. Multi-word
/// needles match case-insensitively by substring; bare `ip` and bare
/// `key` match only as standalone tokens (split on non-alphanumeric
/// characters) so operational `*_id` fields (`request_id`, `job_id`,
/// `backup_id`, `media_id`) and `destination` stay visible.
pub fn is_sensitive_key(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    if [
        "authorization",
        "cookie",
        "set-cookie",
        "token",
        "password",
        "passwd",
        "secret",
        "credential",
        "private_key",
        "private-key",
        "privatekey",
        "smtp",
        "api_key",
        "apikey",
        "access_key",
        "email",
        "phone",
        "ip_address",
        "client_ip",
        "body",
        "payload",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
    {
        return true;
    }
    for token in lower.split(|c: char| !c.is_ascii_alphanumeric()) {
        if token == "ip" || token == "key" {
            return true;
        }
    }
    false
}

/// Redact one `key=value` field value: sensitive keys become `[redacted]`,
/// anything else is truncated to [`MAX_DETAIL_CHARS`]. Untrusted strings
/// are returned as-is here; [`log_event`] debug-formats them (`{:?}`) so
/// control characters cannot forge log lines.
pub fn redact_value(key: &str, value: &str) -> String {
    if is_sensitive_key(key) {
        return "[redacted]".to_string();
    }
    value.chars().take(MAX_DETAIL_CHARS).collect()
}

/// Stable error class for a storage/application failure, derived from the
/// error's `Display` prefix (`validation error`, `conflict`, `not found`,
/// `storage unavailable` — see `DomainError`). Lets operators troubleshoot
/// from the class + a short cause without copying arbitrary remote
/// response bodies into logs.
pub fn error_class(message: &str) -> &'static str {
    let lower = message.to_ascii_lowercase();
    if lower.starts_with("storage unavailable") || lower.contains("storage unavailable") {
        "storage-unavailable"
    } else if lower.starts_with("validation error") || lower.starts_with("invalid") {
        "validation"
    } else if lower.starts_with("conflict") {
        "conflict"
    } else if lower.starts_with("not found") {
        "not-found"
    } else if lower.contains("timeout") || lower.contains("timed out") {
        "timeout"
    } else if lower.contains("unauthorized") || lower.contains("forbidden") {
        "auth"
    } else {
        "internal"
    }
}

/// One structured log line: `[pinetree] level=<l> event=<e> key=value...`.
/// Values are redacted by key (see [`redact_value`]) and debug-formatted
/// so untrusted text cannot forge lines. When [`structured_logging`] is
/// off, only `level=` + `event=` are emitted (no fields).
pub fn log_event(level: LogLevel, event: &str, fields: &[(&str, &str)]) {
    let ts_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    if structured_logging() {
        let mut line = format!(
            "[pinetree] ts={ts_ms} level={} event={event}",
            level.as_str()
        );
        for (key, value) in fields {
            line.push(' ');
            line.push_str(key);
            line.push('=');
            line.push_str(&format!("{:?}", redact_value(key, value)));
        }
        eprintln!("{line}");
    } else {
        eprintln!(
            "[pinetree] ts={ts_ms} level={} event={event}",
            level.as_str()
        );
    }
}

/// Mint a request correlation id (uuid v4, hex). Cheap enough for one per
/// HTTP request; ids travel in `request_id=` fields and the
/// `x-request-id` response header.
pub fn new_request_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Structured start line for one HTTP request (`http.request.start`).
/// `method` + `path` only — never query strings, headers, or bodies.
pub fn log_request_start(request_id: &str, method: &str, path: &str) {
    log_event(
        LogLevel::Debug,
        "http.request.start",
        &[
            ("request_id", request_id),
            ("method", method),
            ("path", path),
        ],
    );
}

/// Structured completion line (`http.request.done`) with status + duration.
pub fn log_request_done(request_id: &str, method: &str, path: &str, status: u16, elapsed_ms: u64) {
    let status_text = status.to_string();
    let elapsed_text = elapsed_ms.to_string();
    log_event(
        LogLevel::Info,
        "http.request.done",
        &[
            ("request_id", request_id),
            ("method", method),
            ("path", path),
            ("status", status_text.as_str()),
            ("elapsed_ms", elapsed_text.as_str()),
        ],
    );
}

/// One bounded recent-error sample: millisecond timestamp + redacted
/// category only (never messages, bodies, or key material).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorSample {
    /// `SystemTime` millis when the error was recorded.
    pub recorded_ms: u64,
    /// Redacted dotted category (`job.delivery`, `backup.create`, ...).
    pub category: String,
}

struct ErrorLedger {
    recent: VecDeque<ErrorSample>,
    counts: HashMap<String, u64>,
}

fn error_ledger() -> &'static Mutex<ErrorLedger> {
    static LEDGER: std::sync::OnceLock<Mutex<ErrorLedger>> = std::sync::OnceLock::new();
    LEDGER.get_or_init(|| {
        Mutex::new(ErrorLedger {
            recent: VecDeque::new(),
            counts: HashMap::new(),
        })
    })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Record one internal error by redacted category. Bounded: the recent
/// ring keeps [`MAX_RECENT_ERRORS`] samples and the counter map keeps
/// [`MAX_ERROR_CATEGORIES`] categories (further categories fold into
/// `other`). Never pass secrets, bodies, or untrusted text here.
pub fn record_error(category: &str) {
    let category = category.chars().take(96).collect::<String>();
    let mut ledger = error_ledger()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    ledger.recent.push_back(ErrorSample {
        recorded_ms: now_ms(),
        category: category.clone(),
    });
    while ledger.recent.len() > MAX_RECENT_ERRORS {
        ledger.recent.pop_front();
    }
    if ledger.counts.len() >= MAX_ERROR_CATEGORIES && !ledger.counts.contains_key(&category) {
        *ledger.counts.entry("other".to_string()).or_insert(0) += 1;
    } else {
        *ledger.counts.entry(category).or_insert(0) += 1;
    }
}

/// Bounded snapshot of recent internal errors (oldest first) for the
/// detailed admin surface. Categories only — no messages or secrets.
pub fn recent_errors() -> Vec<ErrorSample> {
    error_ledger()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .recent
        .iter()
        .cloned()
        .collect()
}

/// Per-category error counts (bounded key space; see [`record_error`]).
pub fn error_counts() -> HashMap<String, u64> {
    error_ledger()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .counts
        .clone()
}

#[doc(hidden)]
pub fn emit_federation_debug(args: std::fmt::Arguments<'_>) {
    let ts_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    eprintln!("[fed-debug] ts={ts_ms} {args}");
}

/// `fed_debug!("in  reject reason={}", why)` writes one `[fed-debug]` line when
/// federation debug is on. Arguments are not evaluated when it is off.
#[macro_export]
macro_rules! fed_debug {
    ($($arg:tt)*) => {
        if $crate::observability::federation_debug() {
            $crate::observability::emit_federation_debug(format_args!($($arg)*));
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_value_parsing() {
        assert!(env_enables_federation_debug(Some("1")));
        assert!(env_enables_federation_debug(Some(" TRUE ")));
        assert!(env_enables_federation_debug(Some("yes")));
        assert!(env_enables_federation_debug(Some("on")));
        assert!(!env_enables_federation_debug(None));
        assert!(!env_enables_federation_debug(Some("")));
        assert!(!env_enables_federation_debug(Some("0")));
        assert!(!env_enables_federation_debug(Some("off")));
        assert!(!env_enables_federation_debug(Some("debug")));
    }

    #[test]
    fn switch_gates_the_macro() {
        set_federation_debug(false);
        assert!(!federation_debug());
        set_federation_debug(true);
        assert!(federation_debug());
        // Exercises the macro path; output goes to stderr.
        crate::fed_debug!("test line key={}", 1);
        set_federation_debug(false);
    }

    #[test]
    fn sensitive_keys_redact() {
        for key in [
            "Authorization",
            "authorization",
            "Cookie",
            "set-cookie",
            "token",
            "access_token",
            "password",
            "smtp_password",
            "smtp_username",
            "secret",
            "credential",
            "private_key",
            "private-key",
            "email",
            "request_body",
            "body",
            "ip",
            "IP",
            "key",
            "KEY",
            "client-ip",
            "api.key",
        ] {
            assert!(is_sensitive_key(key), "{key} must be treated as sensitive");
            assert_eq!(redact_value(key, "hunter2-super-secret"), "[redacted]");
        }
        // Bare-word boundary: substrings inside other words stay visible.
        for key in ["stripe", "monkey", "turkey", "skip"] {
            assert!(!is_sensitive_key(key), "{key} must stay visible");
        }
        // Operational ids stay visible so logs remain correlatable.
        for key in [
            "request_id",
            "job_id",
            "event",
            "method",
            "path",
            "status",
            "elapsed_ms",
            "backup_id",
            "media_id",
            "destination",
        ] {
            assert!(!is_sensitive_key(key), "{key} must stay visible");
            assert_eq!(redact_value(key, "bk-123"), "bk-123");
        }
    }

    #[test]
    fn remote_body_never_logged_verbatim() {
        // Batch H3: remote response bodies must never reach `log_event`
        // verbatim — callers log `error_class` + a short cause instead.
        let remote_body = "REMOTE-BODY-SENTINEL-9f3a<html>evil-payload</html>";
        // Bodies are sensitive: even a direct `body`/`payload` field
        // redacts instead of echoing the remote text.
        assert_eq!(redact_value("body", remote_body), "[redacted]");
        assert_eq!(redact_value("payload", remote_body), "[redacted]");
        // The sanctioned path is `error_class`: the class label never
        // contains the remote text verbatim.
        let class = error_class(remote_body);
        assert!(
            !class.contains(remote_body),
            "error class must not echo remote body"
        );
        assert!(
            !class.contains("evil-payload"),
            "error class must not leak remote fragment"
        );
        // And the class label itself is a small fixed set, never the body.
        assert!(
            [
                "storage-unavailable",
                "validation",
                "conflict",
                "not-found",
                "timeout",
                "auth",
                "internal"
            ]
            .contains(&class),
            "unexpected error class {class}"
        );
    }

    #[test]
    fn freeform_values_truncate() {
        let long = "x".repeat(MAX_DETAIL_CHARS + 100);
        assert_eq!(
            redact_value("cause", &long).chars().count(),
            MAX_DETAIL_CHARS
        );
    }

    #[test]
    fn error_classes_cover_port_variants() {
        assert_eq!(
            error_class("storage unavailable: lock poisoned"),
            "storage-unavailable"
        );
        assert_eq!(error_class("validation error: bad cursor"), "validation");
        assert_eq!(error_class("conflict: duplicate"), "conflict");
        assert_eq!(error_class("not found: no row"), "not-found");
        assert_eq!(error_class("fetch timed out after 5s"), "timeout");
        assert_eq!(error_class("something else entirely"), "internal");
    }

    #[test]
    fn structured_toggle_is_process_wide() {
        let prev = structured_logging();
        set_structured_logging(false);
        assert!(!structured_logging());
        log_event(LogLevel::Info, "test.toggle", &[("token", "abc")]);
        set_structured_logging(true);
        assert!(structured_logging());
        log_event(LogLevel::Info, "test.toggle", &[("request_id", "r-1")]);
        set_structured_logging(prev);
    }

    #[test]
    fn request_ids_are_unique() {
        assert_ne!(new_request_id(), new_request_id());
    }
}
