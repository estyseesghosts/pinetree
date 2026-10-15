//! Misskey API fixed-window rate limiting (Slice D1).
//!
//! Keyed by `(account_id_or_ip, endpoint_bucket)` with a window count
//! (`max_per_window` requests per `window_ms`) plus an optional
//! `min_interval_ms` between requests in the same bucket. Uses an
//! injected [`Clock`] so tests control time. Exceeding either limit
//! returns `Err(ApiError::rate_limit_exceeded(retry_secs))`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

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

/// Slice 6 (H1, decision D-c): concurrent-upload slots for
/// `drive/files/create`. Uploads buffer up to `max_file_size` + 1 MiB in
/// memory, so one client must not stack unbounded concurrent uploads: at
/// most [`MAX_CONCURRENT_UPLOADS_PER_IP`] concurrent uploads per client IP
/// (keyed by the same [`client_ip`](super::endpoints::client_ip) string the
/// rate limiter falls back to), plus a [`MAX_CONCURRENT_UPLOADS_GLOBAL`]
/// global cap bounding worst-case transient memory on a single node. The
/// permit is held for the whole upload handler; exhaustion answers
/// Misskey-shaped `RATE_LIMIT_EXCEEDED` (429) with a short `Retry-After`
/// hint so clients back off instead of hot-looping.
pub const MAX_CONCURRENT_UPLOADS_PER_IP: usize = 2;
pub const MAX_CONCURRENT_UPLOADS_GLOBAL: usize = 16;
pub const UPLOAD_BUSY_RETRY_AFTER_SECS: u64 = 5;

/// Held for the duration of one upload handler; dropping it frees both
/// the per-IP and the global slot.
#[derive(Debug)]
pub struct UploadPermit {
    _global: OwnedSemaphorePermit,
    _ip: OwnedSemaphorePermit,
}

/// Concurrent-upload slots; see [`MAX_CONCURRENT_UPLOADS_PER_IP`].
pub struct UploadSlots {
    per_ip: usize,
    global: Arc<Semaphore>,
    per_ip_slots: Mutex<HashMap<String, Arc<Semaphore>>>,
}

impl UploadSlots {
    pub fn new(per_ip: usize, global: usize) -> Self {
        Self {
            per_ip,
            global: Arc::new(Semaphore::new(global)),
            per_ip_slots: Mutex::new(HashMap::new()),
        }
    }

    /// Production defaults: 2 concurrent uploads per IP, 16 globally.
    pub fn with_defaults() -> Self {
        Self::new(
            MAX_CONCURRENT_UPLOADS_PER_IP,
            MAX_CONCURRENT_UPLOADS_GLOBAL,
        )
    }

    /// Take one upload slot for `ip`, or fail 429 `RATE_LIMIT_EXCEEDED`.
    /// Non-blocking: never waits, so a saturated server sheds load fast.
    pub fn acquire(&self, ip: &str) -> Result<UploadPermit, ApiError> {
        let busy = || ApiError::rate_limit_exceeded(UPLOAD_BUSY_RETRY_AFTER_SECS);
        let slot = {
            let mut slots = self.per_ip_slots.lock().unwrap_or_else(|e| e.into_inner());
            slots
                .entry(ip.to_string())
                .or_insert_with(|| Arc::new(Semaphore::new(self.per_ip)))
                .clone()
        };
        let global = self.global.clone().try_acquire_owned().map_err(|_| busy())?;
        match slot.try_acquire_owned() {
            Ok(ip_permit) => Ok(UploadPermit {
                _global: global,
                _ip: ip_permit,
            }),
            Err(_) => Err(busy()),
        }
    }
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

    #[test]
    fn upload_slots_cap_per_ip_and_release_on_drop() {
        // Slice 6 (H1, decision D-c): at most `per_ip` concurrent uploads
        // per client IP; the permit releases when the handler drops it.
        let slots = UploadSlots::new(1, 10);
        let first = slots.acquire("1.1.1.1").expect("first slot");
        let err = slots.acquire("1.1.1.1").unwrap_err();
        assert_eq!(err.code, "RATE_LIMIT_EXCEEDED");
        assert_eq!(err.http_status, 429);
        // A different IP is unaffected by the first IP's usage.
        let _other = slots.acquire("2.2.2.2").expect("other ip slot");
        drop(first);
        let _again = slots.acquire("1.1.1.1").expect("slot after release");
    }

    #[test]
    fn upload_slots_global_cap() {
        // Slice 6 (H1, decision D-c): the global cap binds even when no
        // single IP is at its own cap.
        let slots = UploadSlots::new(10, 1);
        let _first = slots.acquire("1.1.1.1").expect("first slot");
        let err = slots.acquire("2.2.2.2").unwrap_err();
        assert_eq!(err.code, "RATE_LIMIT_EXCEEDED");
        assert_eq!(err.http_status, 429);
    }
}
