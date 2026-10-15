//! Misskey API fixed-window rate limiting (Slice D1).
//!
//! Keyed by `(account_id_or_ip, endpoint_bucket)` with a window count
//! (`max_per_window` requests per `window_ms`) plus an optional
//! `min_interval_ms` between requests in the same bucket. Uses an
//! injected [`Clock`] so tests control time. Exceeding either limit
//! returns `Err(ApiError::rate_limit_exceeded(retry_secs))`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::errors::ApiError;
use crate::application::ports::Clock;

pub use super::request::check_allowed_fields;

const HOUR_MS: u64 = 3_600_000;

/// Configured limits for one endpoint bucket.
#[derive(Debug, Clone, Copy)]
pub struct BucketLimit {
    pub max_per_window: u64,
    pub window_ms: u64,
    pub min_interval_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct BucketState {
    window_start_ms: i64,
    count: u64,
    last_ms: Option<i64>,
}

/// Fixed-window rate limiter keyed by `(account_id_or_ip, endpoint_bucket)`.
pub struct RateLimiter {
    clock: Arc<dyn Clock>,
    limits: HashMap<String, BucketLimit>,
    state: Mutex<HashMap<(String, String), BucketState>>,
}

impl RateLimiter {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            clock,
            limits: HashMap::new(),
            state: Mutex::new(HashMap::new()),
        }
    }

    /// Misskey Phase 1 defaults (`docs/endpoint-manifest.md` §7 plus the
    /// `notes/reactions/delete` 60/h + 3 s and `drive/files/create` 120/h
    /// buckets from `docs/phase-1-plan.md`).
    pub fn with_misskey_defaults(clock: Arc<dyn Clock>) -> Self {
        Self::new(clock)
            .with_limit("i/update", 20, HOUR_MS, None)
            .with_limit("notes/create", 300, HOUR_MS, None)
            .with_limit("notes/delete", 300, HOUR_MS, Some(1_000))
            .with_limit("following/create", 100, HOUR_MS, None)
            .with_limit("following/delete", 100, HOUR_MS, None)
            .with_limit("notes/reactions/delete", 60, HOUR_MS, Some(3_000))
            .with_limit("drive/files/create", 120, HOUR_MS, None)
    }

    pub fn with_limit(
        mut self,
        bucket: impl Into<String>,
        max_per_window: u64,
        window_ms: u64,
        min_interval_ms: Option<u64>,
    ) -> Self {
        self.limits.insert(
            bucket.into(),
            BucketLimit {
                max_per_window,
                window_ms,
                min_interval_ms,
            },
        );
        self
    }

    /// Record one request from `account_id_or_ip` to `endpoint_bucket`.
    /// Buckets without a configured limit always pass.
    pub fn check(&self, account_id_or_ip: &str, endpoint_bucket: &str) -> Result<(), ApiError> {
        let limit = match self.limits.get(endpoint_bucket) {
            None => return Ok(()),
            Some(limit) => *limit,
        };
        let now = self.clock.now_ms();
        let key = (account_id_or_ip.to_string(), endpoint_bucket.to_string());
        let mut states = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut entry = states
            .get(&key)
            .copied()
            .unwrap_or(BucketState {
                window_start_ms: now,
                count: 0,
                last_ms: None,
            });
        // Roll the fixed window when it has elapsed.
        if now.saturating_sub(entry.window_start_ms) as u64 >= limit.window_ms {
            entry.window_start_ms = now;
            entry.count = 0;
        }
        // Short-term minimum interval first (Misskey `BRIEF_REQUEST_INTERVAL`).
        if let (Some(min), Some(last)) = (limit.min_interval_ms, entry.last_ms) {
            let elapsed = now.saturating_sub(last) as u64;
            if elapsed < min {
                return Err(ApiError::rate_limit_exceeded(ceil_secs(min - elapsed)));
            }
        }
        if entry.count >= limit.max_per_window {
            let reset_in =
                (entry.window_start_ms as u64).saturating_add(limit.window_ms) as i64 - now;
            return Err(ApiError::rate_limit_exceeded(ceil_secs(
                reset_in.max(0) as u64,
            )));
        }
        entry.count += 1;
        entry.last_ms = Some(now);
        states.insert(key, entry);
        Ok(())
    }
}

fn ceil_secs(ms: u64) -> u64 {
    ms.div_ceil(1000).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::id::ManualClock;

    fn limiter_at(now_ms: i64) -> (RateLimiter, Arc<ManualClock>) {
        let clock = Arc::new(ManualClock::new(now_ms));
        let limiter = RateLimiter::new(clock.clone()).with_limit("notes/create", 2, 60_000, None);
        (limiter, clock)
    }

    #[test]
    fn window_count_and_rollover() {
        let (limiter, clock) = limiter_at(0);
        assert!(limiter.check("alice", "notes/create").is_ok());
        assert!(limiter.check("alice", "notes/create").is_ok());
        let err = limiter.check("alice", "notes/create").unwrap_err();
        assert_eq!(err.code, "RATE_LIMIT_EXCEEDED");
        // Other actors and unconfigured buckets are unaffected.
        assert!(limiter.check("bob", "notes/create").is_ok());
        assert!(limiter.check("alice", "meta").is_ok());
        // Window rolls over after 60 s.
        clock.advance_ms(60_000);
        assert!(limiter.check("alice", "notes/create").is_ok());
    }

    #[test]
    fn min_interval_enforced() {
        let clock = Arc::new(ManualClock::new(0));
        let limiter =
            RateLimiter::new(clock.clone()).with_limit("notes/delete", 300, HOUR_MS, Some(1_000));
        assert!(limiter.check("alice", "notes/delete").is_ok());
        let err = limiter.check("alice", "notes/delete").unwrap_err();
        assert_eq!(err.code, "RATE_LIMIT_EXCEEDED");
        clock.advance_ms(1_000);
        assert!(limiter.check("alice", "notes/delete").is_ok());
    }
}
