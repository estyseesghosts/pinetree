//! Outbound federation retry classification, exponential backoff, and
//! per-destination failure tracking (Phase 2 Slice D3).

use std::collections::HashMap;
use std::sync::Mutex;

use crate::federation::delivery:: parse_http_url;

/// Maximum retry attempts before a transient delivery job is dropped.
pub const MAX_DELIVERY_ATTEMPTS: u32 = 8;

/// Outcome classification for an outbound ActivityPub delivery attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryOutcome {
    /// `2xx` HTTP response: delivery succeeded.
    Delivered,
    /// Permanent client error (`4xx` other than `408` / `429`): do not retry.
    PermanentFailure(String),
    /// Transient error (`5xx`, `408`, `429`, DNS/socket timeout): schedule retry.
    TransientFailure(String),
}

/// Classify an HTTP status code from a remote inbox.
pub fn classify_http_status(status: u16, reason: &str) -> DeliveryOutcome {
    if (200..300).contains(&status) {
        DeliveryOutcome::Delivered
    } else if status == 408 || status == 429 || status >= 500 {
        DeliveryOutcome::TransientFailure(format!("HTTP {status}: {reason}"))
    } else if (400..500).contains(&status) {
        DeliveryOutcome::PermanentFailure(format!("HTTP {status}: {reason}"))
    } else {
        DeliveryOutcome::TransientFailure(format!("HTTP {status}: {reason}"))
    }
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
        let backoff = compute_backoff_ms(base_backoff_ms, entry.consecutive_failures.saturating_sub(1));
        let until = now_ms.saturating_add(backoff);
        entry.cooldown_until_ms = Some(until);
        until
    }

    pub fn health(&self, inbox_url: &str) -> DestinationHealth {
        let key = Self::host_key(inbox_url);
        let guard = self.hosts.lock().unwrap_or_else(|e| e.into_inner());
        guard.get(&key).cloned().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_classification_and_backoff() {
        assert_eq!(classify_http_status(202, "Accepted"), DeliveryOutcome::Delivered);
        assert!(matches!(
            classify_http_status(500, "Internal Server Error"),
            DeliveryOutcome::TransientFailure(_)
        ));
        assert!(matches!(
            classify_http_status(429, "Too Many Requests"),
            DeliveryOutcome::TransientFailure(_)
        ));
        assert!(matches!(
            classify_http_status(400, "Bad Request"),
            DeliveryOutcome::PermanentFailure(_)
        ));
        assert_eq!(compute_backoff_ms(1000, 0), 1000);
        assert_eq!(compute_backoff_ms(1000, 3), 8000);
    }
}
