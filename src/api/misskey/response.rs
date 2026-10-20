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

const MINUTE_MS: u64 = 60_000;
const HOUR_MS: u64 = 3_600_000;

/// Slice 9 (H4): progressive delay after failed auth attempts. The delay
/// before the next attempt is `AUTH_BACKOFF_BASE_MS` after the first
/// failure in a window, doubling per further failure, capped at
/// [`AUTH_BACKOFF_CAP_MS`].
pub const AUTH_BACKOFF_BASE_MS: u64 = 1_000;
pub const AUTH_BACKOFF_CAP_MS: u64 = 60_000;

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

/// Failure-counted throttle for one auth bucket: a per-account limit and a
/// per-IP limit, each a fixed window of *failed* attempts.
#[derive(Debug, Clone, Copy)]
pub struct AuthLimit {
    pub account: BucketLimit,
    pub ip: BucketLimit,
}

#[derive(Debug, Clone, Copy)]
struct FailureState {
    window_start_ms: i64,
    failures: u64,
    last_failure_ms: i64,
}

/// Fixed-window rate limiter keyed by `(account_id_or_ip, endpoint_bucket)`.
pub struct RateLimiter {
    clock: Arc<dyn Clock>,
    limits: HashMap<String, BucketLimit>,
    state: Mutex<HashMap<(String, String), BucketState>>,
    auth_limits: HashMap<String, AuthLimit>,
    failures: Mutex<HashMap<(String, String), FailureState>>,
}

impl RateLimiter {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            clock,
            limits: HashMap::new(),
            state: Mutex::new(HashMap::new()),
            auth_limits: HashMap::new(),
            failures: Mutex::new(HashMap::new()),
        }
    }

    /// Misskey Phase 1 defaults (`docs/endpoint-manifest.md` §7 plus the
    /// `notes/reactions/delete` 60/h + 3 s and `drive/files/create` 120/h
    /// buckets from `docs/phase-1-plan.md`), plus the slice 9 auth buckets.
    pub fn with_misskey_defaults(clock: Arc<dyn Clock>) -> Self {
        Self::new(clock)
            .with_limit("i/update", 20, HOUR_MS, None)
            .with_limit("notes/create", 300, HOUR_MS, None)
            .with_limit("notes/edit", 300, HOUR_MS, None)
            .with_limit("notes/delete", 300, HOUR_MS, Some(1_000))
            .with_limit("following/create", 100, HOUR_MS, None)
            .with_limit("following/delete", 100, HOUR_MS, None)
            .with_limit("following/invalidate", 100, HOUR_MS, None)
            .with_limit("users/search-by-username-and-host", 60, HOUR_MS, None)
            .with_limit("ap/show", 60, HOUR_MS, None)
            .with_limit("ap/get", 30, HOUR_MS, None)
            // Batch O WP9 self-service + WP12 preview + WP13 drive extras.
            .with_limit("i/change-password", 10, HOUR_MS, None)
            .with_limit("i/regenerate-token", 10, HOUR_MS, None)
            .with_limit("i/apps", 60, HOUR_MS, None)
            .with_limit("i/authorized-apps", 60, HOUR_MS, None)
            .with_limit("i/signin-history", 60, HOUR_MS, None)
            .with_limit("url-preview", 60, HOUR_MS, None)
            .with_limit("drive/files/upload-from-url", 60, HOUR_MS, None)
            .with_limit("drive/files/find-by-hash", 60, HOUR_MS, None)
            // Upstream `users/lists/push.ts` 1h/30 rate bucket.
            .with_limit("users/lists/push", 30, HOUR_MS, None)
            // Federated clips + pages handoff (upstream buckets):
            // `clips/add-note` 20/h, `pages/create` 10/h, `pages/update`
            // 300/h.
            .with_limit("clips/add-note", 20, HOUR_MS, None)
            .with_limit("pages/create", 10, HOUR_MS, None)
            .with_limit("pages/update", 300, HOUR_MS, None)
            .with_limit("blocking/create", 20, HOUR_MS, None)
            .with_limit("blocking/delete", 100, HOUR_MS, None)
            .with_limit("mute/create", 20, HOUR_MS, None)
            .with_limit("renote-mute/create", 20, HOUR_MS, None)
            .with_limit("notes/reactions/delete", 60, HOUR_MS, Some(3_000))
            .with_limit("drive/files/create", 120, HOUR_MS, None)
            // Draft endpoints (§5; same 300/h cost class as `notes/*`).
            .with_limit("notes/drafts/create", 300, HOUR_MS, None)
            .with_limit("notes/drafts/update", 300, HOUR_MS, None)
            .with_limit("notes/drafts/delete", 300, HOUR_MS, None)
            // Profile import/export (§13; Misskey `secure: true` 1/hr
            // parity — each endpoint gets its own 1/hr bucket. Upstream
            // durations differ for three exports (`export-notes`,
            // `export-clips`, `export-favorites` are 1/day and
            // `export-user-lists` is 1/min); pinetree normalizes all 13 to
            // 1/hr, documented in the endpoint manifest §20.
            .with_limit("i/export-following", 1, HOUR_MS, None)
            .with_limit("i/export-blocking", 1, HOUR_MS, None)
            .with_limit("i/export-mute", 1, HOUR_MS, None)
            .with_limit("i/export-notes", 1, HOUR_MS, None)
            .with_limit("i/export-user-lists", 1, HOUR_MS, None)
            .with_limit("i/export-antennas", 1, HOUR_MS, None)
            .with_limit("i/export-clips", 1, HOUR_MS, None)
            .with_limit("i/export-favorites", 1, HOUR_MS, None)
            .with_limit("i/import-following", 1, HOUR_MS, None)
            .with_limit("i/import-blocking", 1, HOUR_MS, None)
            .with_limit("i/import-muting", 1, HOUR_MS, None)
            .with_limit("i/import-user-lists", 1, HOUR_MS, None)
            .with_limit("i/import-antennas", 1, HOUR_MS, None)
            // Custom-emoji export (upstream `export-custom-emojis.ts`
            // `1hour/1` parity). `admin/emoji/import-zip` has no upstream
            // bucket; it rides the default open bucket like the other
            // `admin/emoji/*` arms.
            .with_limit("export-custom-emojis", 1, HOUR_MS, None)
            // Every signup attempt from one IP counts (see `check`).
            .with_limit("auth/signup", 20, HOUR_MS, None)
            // MiAuth check polls: every request counts per IP.
            .with_limit("auth/miauth-check", 600, MINUTE_MS, None)
            // Failed signin attempts: 10 per account and 30 per IP per minute.
            .with_auth_limit("auth/signin", 10, 30, MINUTE_MS)
            .with_auth_limit("auth/miauth", 10, 30, MINUTE_MS)
            // Batch O WP9: wrong-password on self-service counts like a
            // failed signin (same 10/account + 30/IP per minute class).
            .with_auth_limit("auth/change-password", 10, 30, MINUTE_MS)
            .with_auth_limit("auth/regenerate-token", 10, 30, MINUTE_MS)
            // Batch P: email/move/translate/notification/discovery buckets
            // (upstream windows: update-email + request-reset-password 3/h,
            // i/move 5/day, fetch-external-resources 50/h, fetch-rss
            // 300/min, test-notification 10/min, notes/versions 10/5s).
            .with_limit("i/update-email", 3, HOUR_MS, None)
            .with_limit("request-reset-password", 3, HOUR_MS, None)
            .with_limit("i/move", 5, 86_400_000, None)
            .with_limit("fetch-external-resources", 50, HOUR_MS, None)
            .with_limit("fetch-rss", 300, MINUTE_MS, None)
            .with_limit("notifications/test-notification", 10, MINUTE_MS, None)
            .with_limit("notes/versions", 10, 5_000, None)
            // Batch Q Phase 1: upstream `i/notifications.ts:23-26` and
            // `i/notifications-grouped.ts:28-31` buckets (30 per 30 s).
            .with_limit("i/notifications", 30, 30_000, None)
            .with_limit("i/notifications-grouped", 30, 30_000, None)
            .with_auth_limit("auth/update-email", 10, 30, MINUTE_MS)
    }

    /// Failure-counted auth bucket: at most `max_per_account` failures per
    /// account key and `max_per_ip` per IP key within `window_ms`.
    pub fn with_auth_limit(
        mut self,
        bucket: impl Into<String>,
        max_per_account: u64,
        max_per_ip: u64,
        window_ms: u64,
    ) -> Self {
        let limit = |max_per_window| BucketLimit {
            max_per_window,
            window_ms,
            min_interval_ms: None,
        };
        self.auth_limits.insert(
            bucket.into(),
            AuthLimit {
                account: limit(max_per_account),
                ip: limit(max_per_ip),
            },
        );
        self
    }

    /// Refuse an auth attempt before any credential check. Only failures are
    /// counted, so a successful signin is never blocked by earlier successes.
    /// Blocked when either key is over its window cap or still inside its
    /// progressive delay. No lockout: every block expires with the window or
    /// the delay. Buckets without an auth limit always pass.
    pub fn check_auth_failures(
        &self,
        bucket: &str,
        account_key: &str,
        ip_key: &str,
    ) -> Result<(), ApiError> {
        let limit = match self.auth_limits.get(bucket) {
            None => return Ok(()),
            Some(limit) => *limit,
        };
        let now = self.clock.now_ms();
        let states = self.failures.lock().unwrap_or_else(|e| e.into_inner());
        let account = failure_retry_secs(
            states.get(&(account_key.to_string(), bucket.to_string())).copied(),
            limit.account,
            now,
        );
        let ip = failure_retry_secs(
            states.get(&(ip_key.to_string(), bucket.to_string())).copied(),
            limit.ip,
            now,
        );
        match account.into_iter().chain(ip).max() {
            Some(retry_secs) => Err(ApiError::rate_limit_exceeded(retry_secs)),
            None => Ok(()),
        }
    }

    /// Count one failed auth attempt against both the account and IP keys.
    pub fn record_auth_failure(&self, bucket: &str, account_key: &str, ip_key: &str) {
        let limit = match self.auth_limits.get(bucket) {
            None => return,
            Some(limit) => *limit,
        };
        let now = self.clock.now_ms();
        let mut states = self.failures.lock().unwrap_or_else(|e| e.into_inner());
        for (key, bucket_limit) in [(account_key, limit.account), (ip_key, limit.ip)] {
            let entry_key = (key.to_string(), bucket.to_string());
            let next = match states.get(&entry_key).copied() {
                Some(state)
                    if (now.saturating_sub(state.window_start_ms) as u64)
                        < bucket_limit.window_ms =>
                {
                    FailureState {
                        window_start_ms: state.window_start_ms,
                        failures: state.failures + 1,
                        last_failure_ms: now,
                    }
                }
                _ => FailureState {
                    window_start_ms: now,
                    failures: 1,
                    last_failure_ms: now,
                },
            };
            states.insert(entry_key, next);
        }
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

    /// Debug-only: multiplies every endpoint bucket's `max_per_window` by
    /// `factor`. Auth failure throttles and `min_interval_ms` are unchanged.
    /// Used by `--debug-weak-rate-limits` for load tests; never in production.
    pub fn with_weakened_limits(mut self, factor: u64) -> Self {
        for limit in self.limits.values_mut() {
            limit.max_per_window = limit.max_per_window.saturating_mul(factor);
        }
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

/// Progressive delay after `failures` (>= 1) failed attempts in a window.
fn auth_backoff_ms(failures: u64) -> u64 {
    let shift = failures.saturating_sub(1).min(16);
    (AUTH_BACKOFF_BASE_MS << shift).min(AUTH_BACKOFF_CAP_MS)
}

/// Retry hint in seconds for one failure-counted key, or `None` when the key
/// may attempt now: its window has expired, or it is under the cap and past
/// its progressive delay.
fn failure_retry_secs(state: Option<FailureState>, limit: BucketLimit, now: i64) -> Option<u64> {
    let state = state?;
    let window_end = state.window_start_ms.saturating_add(limit.window_ms as i64);
    if now >= window_end {
        return None;
    }
    if state.failures >= limit.max_per_window {
        return Some(ceil_secs((window_end - now) as u64));
    }
    let backoff = auth_backoff_ms(state.failures);
    let elapsed = now.saturating_sub(state.last_failure_ms).max(0) as u64;
    if elapsed < backoff {
        Some(ceil_secs(backoff - elapsed))
    } else {
        None
    }
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

    fn auth_limiter_at(now_ms: i64) -> (RateLimiter, Arc<ManualClock>) {
        let clock = Arc::new(ManualClock::new(now_ms));
        // 3 failures per account and 5 per IP within 60 s.
        let limiter =
            RateLimiter::new(clock.clone()).with_auth_limit("auth/signin", 3, 5, 60_000);
        (limiter, clock)
    }

    #[test]
    fn slice9_auth_failures_backoff_then_count_cap_then_window_expiry() {
        let (limiter, clock) = auth_limiter_at(0);
        let (acct, ip) = ("acct:alice", "ip:1.1.1.1");
        assert!(limiter.check_auth_failures("auth/signin", acct, ip).is_ok());
        // Failure 1 (t=0): progressive delay of 1 s before the next attempt.
        limiter.record_auth_failure("auth/signin", acct, ip);
        let err = limiter
            .check_auth_failures("auth/signin", acct, ip)
            .unwrap_err();
        assert_eq!(err.code, "RATE_LIMIT_EXCEEDED");
        assert_eq!(err.http_status, 429);
        clock.advance_ms(1_000);
        // Failure 2 (t=1 s): the delay doubles to 2 s.
        assert!(limiter.check_auth_failures("auth/signin", acct, ip).is_ok());
        limiter.record_auth_failure("auth/signin", acct, ip);
        assert!(limiter.check_auth_failures("auth/signin", acct, ip).is_err());
        clock.advance_ms(2_000);
        // Failure 3 (t=3 s) reaches the per-account cap of 3.
        assert!(limiter.check_auth_failures("auth/signin", acct, ip).is_ok());
        limiter.record_auth_failure("auth/signin", acct, ip);
        clock.advance_ms(30_000);
        assert!(
            limiter.check_auth_failures("auth/signin", acct, ip).is_err(),
            "count cap holds until the window ends"
        );
        // The window expires at 60 s: no lockout beyond the window.
        clock.advance_ms(30_000);
        assert!(limiter.check_auth_failures("auth/signin", acct, ip).is_ok());
    }

    #[test]
    fn slice9_many_accounts_from_one_ip_trip_the_ip_bucket() {
        let (limiter, clock) = auth_limiter_at(0);
        for n in 0..5 {
            limiter.record_auth_failure("auth/signin", &format!("acct:user{n}"), "ip:2.2.2.2");
        }
        // A fresh account from the same IP is refused although it has no
        // failures of its own.
        let err = limiter
            .check_auth_failures("auth/signin", "acct:fresh", "ip:2.2.2.2")
            .unwrap_err();
        assert_eq!(err.code, "RATE_LIMIT_EXCEEDED");
        // Other IPs are unaffected.
        assert!(limiter
            .check_auth_failures("auth/signin", "acct:fresh", "ip:3.3.3.3")
            .is_ok());
        clock.advance_ms(60_000);
        assert!(limiter
            .check_auth_failures("auth/signin", "acct:fresh", "ip:2.2.2.2")
            .is_ok());
    }
}
