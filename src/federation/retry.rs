//! Outbound federation retry classification, exponential backoff, and
//! per-destination failure tracking (Phase 2 Slice D3; Phase 2.6 Slice 10).

use std::collections::HashMap;
use std::sync::Mutex;

use crate::application::ports::NewJob;
use crate::federation::delivery::parse_http_url;

/// Upper bound on any honored `Retry-After` delay (1 hour).
pub const MAX_RETRY_AFTER_MS: i64 = 3_600_000;

/// Base of the per-destination cooldown applied after a transient failure.
pub const DESTINATION_BACKOFF_BASE_MS: i64 = 1_000;

/// Outcome of one delivery step (one inbox POST, or one fan-out job).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryOutcome {
    /// `2xx` HTTP response: delivery succeeded.
    Delivered,
    /// Permanent failure (`3xx` on POST, other `4xx`, SSRF block, invalid URL,
    /// DNS NXDOMAIN): do not retry.
    PermanentFailure(String),
    /// Transient failure (`5xx`, `408`, `429`, timeouts, connect/TLS/IO
    /// errors). `retry_after_ms` is an absolute earliest-retry time taken from
    /// `Retry-After` (capped at [`MAX_RETRY_AFTER_MS`]); the worker retries no
    /// earlier than `max(its own backoff, retry_after_ms)`.
    TransientFailure {
        reason: String,
        retry_after_ms: Option<i64>,
    },
    /// The destination host is in cooldown: nothing was sent and no attempt
    /// was counted. The job is rescheduled at `until_ms`.
    Deferred { until_ms: i64 },
    /// Fan-out (activity job) finished with per-inbox follow-up jobs that the
    /// worker must enqueue and settle atomically with the activity job.
    Split { followups: Vec<NewJob> },
}

/// Classify the final HTTP response from a remote inbox to a signed POST.
///
/// `retry_after` is the raw `Retry-After` header value, if any, and
/// `now_ms` anchors delta-seconds and HTTP-date forms.
pub fn classify_inbox_status(
    status: u16,
    reason: &str,
    retry_after: Option<&str>,
    now_ms: i64,
) -> DeliveryOutcome {
    match status {
        200..=299 => DeliveryOutcome::Delivered,
        408 | 429 | 500..=599 => DeliveryOutcome::TransientFailure {
            reason: format!("HTTP {status}: {reason}"),
            retry_after_ms: retry_after.and_then(|v| parse_retry_after_ms(v, now_ms)),
        },
        // Redirects are never followed for POST delivery, so a 3xx is a
        // permanent answer from this inbox. Other 4xx are permanent too.
        300..=499 => DeliveryOutcome::PermanentFailure(format!("HTTP {status}: {reason}")),
        _ => DeliveryOutcome::TransientFailure {
            reason: format!("HTTP {status}: {reason}"),
            retry_after_ms: None,
        },
    }
}

/// Parse a `Retry-After` header (delta-seconds or an HTTP-date) into an
/// absolute retry time in ms. The delay is capped at [`MAX_RETRY_AFTER_MS`];
/// a date in the past means "retry now". Unparseable values yield `None`.
pub fn parse_retry_after_ms(value: &str, now_ms: i64) -> Option<i64> {
    let v = value.trim();
    if v.is_empty() {
        return None;
    }
    let delay_ms = if v.bytes().all(|b| b.is_ascii_digit()) {
        let secs: u64 = v.parse().ok()?;
        i64::try_from(secs.min(3_600)).ok()?.saturating_mul(1_000)
    } else {
        let when = chrono::DateTime::parse_from_rfc2822(v)
            .ok()?
            .timestamp_millis();
        when.saturating_sub(now_ms).max(0)
    };
    Some(now_ms.saturating_add(delay_ms.min(MAX_RETRY_AFTER_MS)))
}

/// Compute exponential backoff in milliseconds (`base_backoff_ms * 2^min(attempts, 10)`).
pub fn compute_backoff_ms(base_backoff_ms: i64, attempts: u32) -> i64 {
    let shift = attempts.min(10);
    base_backoff_ms.max(1).saturating_mul(1_i64 << shift)
}

#[derive(Debug, Clone, Default)]
pub struct DestinationHealth {
    pub consecutive_failures: u32,
    pub last_failure_ms: Option<i64>,
    pub cooldown_until_ms: Option<i64>,
}

/// Per-destination failure tracker so a dead remote host backs off without
/// stalling deliveries to healthy hosts.
#[derive(Debug, Default)]
pub struct DestinationTracker {
    hosts: Mutex<HashMap<String, DestinationHealth>>,
}

impl DestinationTracker {
    pub fn new() -> Self {
        Self::default()
    }

    fn host_key(inbox_url: &str) -> String {
        parse_http_url(inbox_url)
            .map(|u| u.host_header)
            .unwrap_or_else(|_| inbox_url.to_ascii_lowercase())
    }

    pub fn record_success(&self, inbox_url: &str) {
        let key = Self::host_key(inbox_url);
        let mut guard = self.hosts.lock().unwrap_or_else(|e| e.into_inner());
        guard.remove(&key);
    }

    pub fn record_failure(&self, inbox_url: &str, now_ms: i64, base_backoff_ms: i64) -> i64 {
        let key = Self::host_key(inbox_url);
        let mut guard = self.hosts.lock().unwrap_or_else(|e| e.into_inner());
        let entry = guard.entry(key).or_default();
        entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
        entry.last_failure_ms = Some(now_ms);
        let backoff = compute_backoff_ms(
            base_backoff_ms,
            entry.consecutive_failures.saturating_sub(1),
        );
        let until = now_ms.saturating_add(backoff);
        entry.cooldown_until_ms = Some(until);
        until
    }

    pub fn health(&self, inbox_url: &str) -> DestinationHealth {
        let key = Self::host_key(inbox_url);
        let guard = self.hosts.lock().unwrap_or_else(|e| e.into_inner());
        guard.get(&key).cloned().unwrap_or_default()
    }

    /// Cooldown end for `inbox_url` when it is still in the future at `now_ms`.
    pub fn cooldown_until(&self, inbox_url: &str, now_ms: i64) -> Option<i64> {
        self.health(inbox_url)
            .cooldown_until_ms
            .filter(|until| *until > now_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_classification_and_backoff() {
        assert_eq!(
            classify_inbox_status(202, "Accepted", None, 0),
            DeliveryOutcome::Delivered
        );
        assert!(matches!(
            classify_inbox_status(500, "Internal Server Error", None, 0),
            DeliveryOutcome::TransientFailure { .. }
        ));
        assert!(matches!(
            classify_inbox_status(429, "Too Many Requests", None, 0),
            DeliveryOutcome::TransientFailure { .. }
        ));
        assert!(matches!(
            classify_inbox_status(400, "Bad Request", None, 0),
            DeliveryOutcome::PermanentFailure(_)
        ));
        assert_eq!(compute_backoff_ms(1000, 0), 1000);
        assert_eq!(compute_backoff_ms(1000, 3), 8000);
    }

    #[test]
    fn slice10_post_redirect_is_permanent() {
        for status in [301u16, 302, 303, 307, 308] {
            assert!(
                matches!(
                    classify_inbox_status(status, "moved", None, 0),
                    DeliveryOutcome::PermanentFailure(_)
                ),
                "3xx {status} on POST must be permanent"
            );
        }
    }

    #[test]
    fn slice10_timeouts_5xx_and_429_are_transient() {
        for status in [408u16, 429, 500, 502, 503, 599] {
            assert!(
                matches!(
                    classify_inbox_status(status, "x", None, 0),
                    DeliveryOutcome::TransientFailure { .. }
                ),
                "{status} must be transient"
            );
        }
        for status in [400u16, 401, 403, 404, 410, 422] {
            assert!(
                matches!(
                    classify_inbox_status(status, "x", None, 0),
                    DeliveryOutcome::PermanentFailure(_)
                ),
                "{status} must be permanent"
            );
        }
    }

    #[test]
    fn slice10_retry_after_delta_seconds_date_and_cap() {
        let now = 1_700_000_000_000_i64;
        assert_eq!(parse_retry_after_ms("120", now), Some(now + 120_000));
        assert_eq!(parse_retry_after_ms(" 0 ", now), Some(now));
        // Capped at one hour, whatever the server asks for.
        assert_eq!(
            parse_retry_after_ms("999999999", now),
            Some(now + MAX_RETRY_AFTER_MS)
        );
        // HTTP-date form (IMF-fixdate), one minute in the future.
        let date = chrono::DateTime::from_timestamp_millis(now + 60_000)
            .expect("timestamp")
            .to_rfc2822();
        assert_eq!(parse_retry_after_ms(&date, now), Some(now + 60_000));
        // A date in the past means retry now.
        let past = chrono::DateTime::from_timestamp_millis(now - 60_000)
            .expect("timestamp")
            .to_rfc2822();
        assert_eq!(parse_retry_after_ms(&past, now), Some(now));
        assert_eq!(parse_retry_after_ms("soon", now), None);
        assert_eq!(parse_retry_after_ms("", now), None);
        assert_eq!(parse_retry_after_ms("-5", now), None);
    }

    #[test]
    fn slice10_transient_outcome_carries_retry_after() {
        let now = 1_700_000_000_000_i64;
        assert_eq!(
            classify_inbox_status(503, "busy", Some("30"), now),
            DeliveryOutcome::TransientFailure {
                reason: "HTTP 503: busy".to_string(),
                retry_after_ms: Some(now + 30_000),
            }
        );
        assert_eq!(
            classify_inbox_status(503, "busy", None, now),
            DeliveryOutcome::TransientFailure {
                reason: "HTTP 503: busy".to_string(),
                retry_after_ms: None,
            }
        );
    }

    #[test]
    fn slice10_cooldown_is_reported_only_while_in_future() {
        let tracker = DestinationTracker::new();
        let url = "http://127.0.0.1:9/inbox";
        let now = 1_700_000_000_000_i64;
        assert_eq!(tracker.cooldown_until(url, now), None);
        let until = tracker.record_failure(url, now, DESTINATION_BACKOFF_BASE_MS);
        assert_eq!(until, now + DESTINATION_BACKOFF_BASE_MS);
        assert_eq!(tracker.cooldown_until(url, now), Some(until));
        assert_eq!(tracker.cooldown_until(url, until), None);
        tracker.record_success(url);
        assert_eq!(tracker.cooldown_until(url, now), None);
    }
}
